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
from .subscriptionmgr import SubscriptionsManagerBase, SubscriptionBase
from .. import observableproperties
from ..pysoap.soapclient import HTTPReturnCodeError
from ..xml_types import eventing_types as evt_types

if TYPE_CHECKING:
    from ..definitions_base import BaseDefinitions
    from ..pysoap.msgfactory import MessageFactory
    from .subscriptionmgr import SoapClientPool
    from ..dispatch import RequestData
    from ..xml_types.basetypes import MessageType

MAX_ROUNDTRIP_VALUES = 20


class _RoundTripData:
    def __init__(self, values, abs_max):
        if values:
            self.values = list(values)  # make a copy
            self.min = min(values)
            self.max = max(values)
            self.avg = sum(values) / len(values)
            self.abs_max = abs_max
        else:
            self.values = None
            self.min = None
            self.max = None
            self.avg = None
            self.abs_max = None

    def __repr__(self):
        return f'min={self.min:.4f} max={self.max:.4f} avg={self.avg:.4f} abs. max={self.abs_max:.4f}'


def _mk_dispatch_identifier(reference_parameters: list, path_suffix: str):
    # this is always our own reference parameter. We know that is has max. one element,
    # and the text is the identifier of the subscription
    if path_suffix == '':
        path_suffix = None
    if len(reference_parameters) > 0:
        return reference_parameters[0].text, path_suffix
    return None, path_suffix


class DevSubscriptionAsync(SubscriptionBase):

    async def async_send_notification_report(self, payload: MessageType, action):
        if not self.is_valid:
            return
        addr = HeaderInformationBlock(addr_to=self.notify_to_address,
                                      action=action,
                                      addr_from=None,
                                      reference_parameters=self.notify_ref_params)
        message = self._mk_notification_message(addr, payload)
        try:
            roundtrip_timer = observableproperties.SingleValueCollector(self._soap_client, 'roundtrip_time')

            await self._soap_client.async_post_message_to(self.notify_to_url.path, message,
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
        if self._soap_client is None:
            return
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
            result = await self._soap_client.async_post_message_to(url.path, message,
                                                                   msg='send_notification_end_message')
        except aiohttp.client_exceptions.ClientConnectorError:
            # it does not matter that we could not send the message - end is end ;)
            pass
        except Exception as ex:
            self._logger.error(traceback.format_exc())
        finally:
            self._is_closed = True


class AsyncioEventLoopThread(Thread):
    def __init__(self, *args, loop=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.daemon = True
        self.loop = loop or asyncio.new_event_loop()
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


class SubscriptionsManagerBaseAsync(SubscriptionsManagerBase):
    """This implementation uses ReferenceParameters to identify subscriptions."""
    DEFAULT_MAX_SUBSCR_DURATION = 7200  # max. possible duration of a subscription

    def __init__(self,
                 sdc_definitions: BaseDefinitions,
                 msg_factory: MessageFactory,
                 soap_client_pool: SoapClientPool,
                 max_subscription_duration: [float, None] = None,
                 log_prefix: str = None,
                 ):
        super().__init__(sdc_definitions, msg_factory, soap_client_pool, max_subscription_duration, log_prefix)
        self._async_send_thread = AsyncioEventLoopThread(name='async_send')
        self._async_send_thread.start()

    def _mk_subscription_instance(self, request_data: RequestData):
        subscribe_request = evt_types.Subscribe.from_node(request_data.message_data.p_msg.msg_node)
        accepted_encodings = request_data.http_header['Accept-Encoding']
        return DevSubscriptionAsync(subscribe_request, accepted_encodings, self.base_urls,
                                    self._max_subscription_duration,
                                    msg_factory=self._msg_factory,
                                    log_prefix=self._logger.log_prefix)

    async def _close_soap_client(self, soap_client):
        await soap_client.async_close()

    async def _coro_send_to_subscribers(self, tasks):
        return await asyncio.gather(*tasks, return_exceptions=True)

    def send_to_subscribers(self, payload: Union[MessageType, etree_._Element],
                            action: str,
                            mdib_version_group,
                            what):
        subscribers = self._get_subscriptions_for_action(action)
        if isinstance(payload, etree_._Element):
            body_node = payload
        else:
            nsh = self.sdc_definitions.data_model.ns_helper
            namespaces = [nsh.PM, nsh.MSG]
            namespaces.extend(payload.additional_namespaces)
            my_ns_map = nsh.partial_map(*namespaces)
            body_node = payload.as_etree_node(payload.NODETYPE, my_ns_map)

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
                                              body_node: etree_._Element,
                                              action: str):
        try:
            self._logger.debug(f'send notification report {action} to {subscription}')
            await subscription.async_send_notification_report(body_node, action)
            self._logger.debug(f' done: send notification report {action} to {subscription}')
        except aiohttp.client_exceptions.ClientConnectorError as ex:
            # this is an error related to the connection => log error and continue
            self._logger.error('could not send notification report: {} {}', str(ex), subscription)
        except HTTPReturnCodeError as ex:
            # this is an error related to the connection => log error and continue
            self._logger.error('could not send notification report: HTTP status= {}, reason={}, {}', ex.status,
                               ex.reason, subscription)
        except http.client.NotConnected as ex:
            # this is an error related to the connection => log error and continue
            self._logger.error('{subscription}:could not send notification report: {!r}:  subscr = {}', ex,
                               subscription)
        except socket.timeout as ex:
            # this is an error related to the connection => log error and continue
            self._logger.error('could not send notification report error= {!r}: {}', ex, subscription)
        except asyncio.exceptions.TimeoutError as ex:
            # this is an error related to the connection => log error and continue
            self._logger.error('could not send notification report error= {!r}: {}', ex, subscription)
        except aiohttp.client_exceptions.ClientConnectionError as ex:
            # this is an error related to the connection => log error and continue
            self._logger.error('could not send notification report error= {!r}: {}', ex, subscription)
        except etree_.DocumentInvalid as ex:
            # this is an error related to the document, it cannot be sent to any subscriber => re-raise
            self._logger.error('Invalid Document: {!r}\n{}', ex, etree_.tostring(body_node))
            raise
        except Exception as ex:
            # this should never happen! => re-raise
            self._logger.error('could not send notification report error= {!r}: {}', ex, subscription)
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
            ret[key] = _RoundTripData(all_values, max([s.max for s in stats]), )
        return ret


class SubscriptionsManagerPathAsync(SubscriptionsManagerBaseAsync):
    """This implementation uses path dispatching to identify subscriptions."""

    def _mk_subscription_instance(self, request_data: RequestData):
        subscription = super()._mk_subscription_instance(request_data)
        subscription.path_suffix = subscription.identifier_uuid.hex
        return subscription


class SubscriptionsManagerReferenceParamAsync(SubscriptionsManagerBaseAsync):
    """This implementation uses reference parameters to identify subscriptions."""

    def _mk_subscription_instance(self, request_data: RequestData):
        subscription = super()._mk_subscription_instance(request_data)
        # add  a reference parameter
        subscription.set_reference_parameter()
        return subscription
