from __future__ import annotations

import asyncio
import http.client
import socket
import traceback
from collections import defaultdict
from threading import Thread
from typing import TYPE_CHECKING, Union

import aiohttp.client_exceptions
from lxml import etree as etree_

from sdc11073.xml_types.addressing_types import HeaderInformationBlock
from .subscriptionmgr_base import SubscriptionsManagerBase, ActionBasedSubscription, RoundTripData
from .. import observableproperties
from ..pysoap.soapclient import HTTPReturnCodeError
from ..xml_types import eventing_types as evt_types
from ..xml_types.basetypes import MessageType

if TYPE_CHECKING:
    from ..definitions_base import BaseDefinitions
    from ..pysoap.msgfactory import MessageFactory
    from ..pysoap.soapclientpool import SoapClientPool
    from ..dispatch import RequestData


def _mk_dispatch_identifier(reference_parameters: list, path_suffix: str):
    # this is always our own reference parameter. We know that is has max. one element,
    # and the text is the identifier of the subscription
    if path_suffix == '':
        path_suffix = None
    if len(reference_parameters) > 0:
        return reference_parameters[0].text, path_suffix
    return None, path_suffix


class BicepsSubscriptionAsync(ActionBasedSubscription):
    """Async version of a single BICEPS subscription. It is used by BICEPSSubscriptionsManagerBaseAsync."""
    async def async_send_notification_report(self, body_node: etree_.Element, action: str):
        """

        :param body_node: The soap body node to be sent to the subscriber.
        :param action: the action string
        :return: None or an exception raises
        """
        if not self.is_valid:
            return
        addr = HeaderInformationBlock(addr_to=self.notify_to_address,
                                      action=action,
                                      addr_from=None,
                                      reference_parameters=self.notify_ref_params)
        message = self._mk_notification_message(addr, body_node)
        try:
            soap_client = self._get_soap_client()
            roundtrip_timer = observableproperties.SingleValueCollector(soap_client, 'roundtrip_time')

            await soap_client.async_post_message_to(self.notify_to_url.path, message,
                                                          msg=f'send_notification_report {action}')
            try:
                roundtrip_time = roundtrip_timer.result(0)
                self.last_roundtrip_times.append(roundtrip_time)
                self.max_roundtrip_time = max(self.max_roundtrip_time, roundtrip_time)
            except observableproperties.CollectTimeoutError:
                pass
            self.notify_errors = 0
            self._is_connection_error = False
        except HTTPReturnCodeError:
            self.notify_errors += 1
            raise
        except asyncio.exceptions.TimeoutError:
            self.notify_errors += 1
            self._is_connection_error = True
            raise
        except Exception:  # any other exception is handled as an unreachable location (disconnected)
            self.notify_errors += 1
            self._is_connection_error = True
            raise

    async def async_send_notification_end_message(self, code='SourceShuttingDown',
                                                  reason='Event source going off line.'):
        url = self.base_urls[0]
        my_addr = f'{url.scheme}:{url.netloc}/{url.path}'

        if not self.is_valid:
            return
        soap_client = self._get_soap_client()
        subscription_end = evt_types.SubscriptionEnd()
        subscription_end.SubscriptionManager.Address = my_addr
        subscription_end.SubscriptionManager.ReferenceParameters = self.reference_parameters
        subscription_end.Status = code
        subscription_end.add_reason(reason, 'en-US')
        inf = HeaderInformationBlock(action=subscription_end.action,
                                     addr_to=self.end_to_address or self.notify_to_address)
        message = self._msg_factory.mk_soap_message(inf, payload=subscription_end)
        try:
            url = self._end_to_url or self.notify_to_url
            self._logger.info('async send subscription end to {}, subscription = {}', url, self)
            return await soap_client.async_post_message_to(url.path, message,
                                                                   msg='send_notification_end_message')
        except aiohttp.client_exceptions.ClientConnectorError as ex:
            # it does not matter that we could not send the message - end is end ;)
            self._logger.info('exception async send subscription end to {}, subscription = {}', url, self)
            pass
        except Exception as ex:
            self._logger.error(traceback.format_exc())
        finally:
            self._is_closed = True


class AsyncioEventLoopThread(Thread):
    def __init__(self, name):
        super().__init__(name=name)
        self.daemon = True
        self.loop = asyncio.new_event_loop()
        self.running = False

    def run(self):
        self.running = True
        self.loop.run_forever()

    def run_coro(self, coro):
        return asyncio.run_coroutine_threadsafe(coro, loop=self.loop).result()

    def stop(self):
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.join()
        self.running = False


class BICEPSSubscriptionsManagerBaseAsync(SubscriptionsManagerBase):
    """This async version of a subscriptions manager sends a notification parallel to all subscribers
    by using the async functionality. First all notifications are sent, then all http responses are collected.
    This saves a lot of time compared to the synchronous version that sends the notification to the first subscriber,
    waits for the response, then sends the notification to the second subscriber, and so on."""

    def __init__(self,
                 sdc_definitions: BaseDefinitions,
                 msg_factory: MessageFactory,
                 soap_client_pool: SoapClientPool,
                 max_subscription_duration: [float, None] = None,
                 log_prefix: str = None,
                 ):
        super().__init__(sdc_definitions, msg_factory, soap_client_pool, max_subscription_duration, log_prefix)
        loop_member_name = 'async_loop_subscr_mgr'
        if not hasattr(soap_client_pool, loop_member_name):
            thr = AsyncioEventLoopThread(name=loop_member_name)
            setattr(soap_client_pool, loop_member_name, thr)
            thr.start()
        self._async_send_thread = getattr(soap_client_pool, loop_member_name)

    def _mk_subscription_instance(self, request_data: RequestData):
        subscribe_request = evt_types.Subscribe.from_node(request_data.message_data.p_msg.msg_node)
        accepted_encodings = request_data.http_header['Accept-Encoding']
        return BicepsSubscriptionAsync(self, subscribe_request, accepted_encodings, self.base_urls,
                                    self._max_subscription_duration,
                                    self._soap_client_pool,
                                    msg_factory=self._msg_factory,
                                    log_prefix=self._logger.log_prefix)

    async def _coro_send_to_subscribers(self, tasks):
        return await asyncio.gather(*tasks, return_exceptions=True)

    def _end_all_subscriptions(self, send_subscription_end: bool):
        tasks = []
        with self._subscriptions.lock:
            all_subscriptions = self._subscriptions.objects

            for subscription in all_subscriptions:
                self._logger.info('send subscription end, subscription = {}', subscription)
                tasks.append(subscription.async_send_notification_end_message())
            result = self._async_send_thread.run_coro(self._coro_send_to_subscribers(tasks))
            import time
            time.sleep(1)
            for counter, element in enumerate(result):
                if isinstance(element, Exception):
                    self._logger.warning(f'end_all_subscriptions {all_subscriptions[counter]} returned {element}')
            self._subscriptions.clear()


    def send_to_subscribers(self, payload: Union[MessageType, etree_.Element],
                            action: str,
                            mdib_version_group,
                            what):
        subscribers = self._get_subscriptions_for_action(action)
        if isinstance(payload, MessageType):
            nsh = self.sdc_definitions.data_model.ns_helper
            namespaces = [nsh.PM, nsh.MSG]
            namespaces.extend(payload.additional_namespaces)
            my_ns_map = nsh.partial_map(*namespaces)
            body_node = payload.as_etree_node(payload.NODETYPE, my_ns_map)
        else:
            body_node = payload

        self.sent_to_subscribers = (action, mdib_version_group, body_node)  # update observable
        tasks = []
        for subscriber in subscribers:
            tasks.append(self._async_send_notification_report(subscriber, body_node, action))

        if what:
            self._logger.debug('{}: sending report to {}', what, [s.notify_to_address for s in subscribers])
        result = self._async_send_thread.run_coro(self._coro_send_to_subscribers(tasks))
        for counter, element in enumerate(result):
            if isinstance(element, Exception):
                self._logger.warning(f'{what}: _send_to_subscribers {subscribers[counter]} returned {element}')

    async def _async_send_notification_report(self, subscription,
                                              body_node: etree_.Element,
                                              action: str):
        try:
            self._logger.debug(f'send notification report {action} to {subscription}')
            await subscription.async_send_notification_report(body_node, action)
            self._logger.debug(f' done: send notification report {action} to {subscription}')
        except aiohttp.client_exceptions.ClientConnectorError as ex:
            # this is an error related to the connection => log error and continue
            self._logger.error('could not send notification report {}: {} {}', action, str(ex), subscription)
            self._soap_client_pool.report_unreachable(subscription.notify_to_url.netloc)
        except HTTPReturnCodeError as ex:
            # this is an error related to the connection => log error and continue
            self._logger.error('could not send notification report {}: HTTP status= {}, reason={}, {}',
                               action, ex.status, ex.reason, subscription)
        except http.client.NotConnected as ex:
            # this is an error related to the connection => log error and continue
            self._logger.error('{subscription}:could not send notification report {}: {!r}:  subscr = {}',
                               action, ex, subscription)
            self._soap_client_pool.report_unreachable(subscription.notify_to_url.netloc)
        except socket.timeout as ex:
            # this is an error related to the connection => log error and continue
            self._logger.error('could not send notification report {} error= {!r}: {}', action, ex, subscription)
            self._soap_client_pool.report_unreachable(subscription.notify_to_url.netloc)
        except asyncio.exceptions.TimeoutError as ex:
            # this is an error related to the connection => log error and continue
            self._logger.error('could not send notification report {} error= {!r}: {}', action, ex, subscription)
        except aiohttp.client_exceptions.ClientConnectionError as ex:
            # this is an error related to the connection => log error and continue
            self._logger.error('could not send notification report {} error= {!r}: {}', action, ex, subscription)
            self._soap_client_pool.report_unreachable(subscription.notify_to_url.netloc)
        except etree_.DocumentInvalid as ex:
            # this is an error related to the document, it cannot be sent to any subscriber => re-raise
            self._logger.error('Invalid Document for action {}: {!r}\n{}', action, ex, etree_.tostring(body_node))
            raise
        except Exception:
            # this should never happen! => re-raise
            self._logger.error('could not send notification report {}, error {}: {}',
                               action, traceback.format_exc(), subscription)
            raise

    def get_subscription_round_trip_times(self):
        """Calculates round trip times based on last MAX_ROUNDTRIP_VALUES values.

        @return: a dictionary with key=(<notify_to_address>, (subscription_names)),
                value = _RoundTripData with members min, max, avg, abs_max, values
        """
        ret = {}
        with self._subscriptions.lock:
            for subscription in self._subscriptions.objects:
                if subscription.max_roundtrip_time > 0:
                    ret[(subscription.notify_to_address,
                         subscription.short_filter_names())] = subscription.get_roundtrip_stats()
        return ret

    def get_client_round_trip_times(self):
        """Calculates round trip times based on last MAX_ROUNDTRIP_VALUES values.

        @return: a dictionary with key=<notify_to_address>, value = _RoundTripData with members min, max, avg, abs_max, values
        """
        # first step: collect all round trip times of subscriptions, group them by notify_to_address
        tmp = defaultdict(list)
        ret = {}
        with self._subscriptions.lock:
            for subscription in self._subscriptions.objects:
                if subscription.max_roundtrip_time > 0:
                    tmp[subscription.notify_to_address].append(subscription.get_roundtrip_stats())
        for key, stats in tmp.items():
            all_values = [stat.values for stat in stats]
            ret[key] = RoundTripData(all_values, max([s.max for s in stats]), )
        return ret



class SubscriptionsManagerPathAsync(BICEPSSubscriptionsManagerBaseAsync):
    """This implementation uses path dispatching to identify subscriptions."""

    def _mk_subscription_instance(self, request_data: RequestData):
        subscription = super()._mk_subscription_instance(request_data)
        subscription.path_suffix = subscription.identifier_uuid.hex
        return subscription


class SubscriptionsManagerReferenceParamAsync(BICEPSSubscriptionsManagerBaseAsync):
    """This implementation uses reference parameters to identify subscriptions."""

    def _mk_subscription_instance(self, request_data: RequestData):
        subscription = super()._mk_subscription_instance(request_data)
        # add  a reference parameter
        subscription.set_reference_parameter()
        return subscription
