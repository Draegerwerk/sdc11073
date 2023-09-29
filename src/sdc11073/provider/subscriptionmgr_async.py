from __future__ import annotations

import asyncio
import socket
import traceback
from collections import defaultdict
from threading import Thread
from typing import TYPE_CHECKING, Any

import aiohttp.client_exceptions
from lxml import etree as etree_

from sdc11073 import observableproperties
from sdc11073.etc import apply_map
from sdc11073.pysoap.soapclient import HTTPReturnCodeError
from sdc11073.xml_types import eventing_types as evt_types
from sdc11073.xml_types.addressing_types import HeaderInformationBlock
from sdc11073.xml_types.basetypes import MessageType

from .subscriptionmgr_base import ActionBasedSubscription, RoundTripData, SubscriptionsManagerBase

if TYPE_CHECKING:
    from collections.abc import Awaitable, Iterable

    from sdc11073.definitions_base import BaseDefinitions
    from sdc11073.dispatch import RequestData
    from sdc11073.mdib.mdibbase import MdibVersionGroup
    from sdc11073.pysoap.msgfactory import MessageFactory
    from sdc11073.pysoap.msgreader import ReceivedMessage
    from sdc11073.pysoap.soapclientpool import SoapClientPool
    from sdc11073 import xml_utils


def _mk_dispatch_identifier(reference_parameters: list, path_suffix: str) -> tuple[str | None, str]:
    # this is always our own reference parameter. We know that is has max. one element,
    # and the text is the identifier of the subscription
    if path_suffix == '':
        path_suffix = None
    if len(reference_parameters) > 0:
        return reference_parameters[0].text, path_suffix
    return None, path_suffix


class BicepsSubscriptionAsync(ActionBasedSubscription):
    """Async version of a single BICEPS subscription. It is used by BICEPSSubscriptionsManagerBaseAsync."""

    async def async_send_notification_report(self, body_node: xml_utils.LxmlElement, action: str):
        """Send notification to subscriber."""
        if not self.is_valid or self.unsubscribed_at is not None:
            return
        addr = HeaderInformationBlock(addr_to=self.notify_to_address,
                                      action=action,
                                      addr_from=None,
                                      reference_parameters=self.notify_ref_params)
        message = self._mk_notification_message(addr, body_node)
        try:
            soap_client = self._get_soap_client()
            roundtrip_timer = observableproperties.SingleValueCollector(soap_client, 'roundtrip_time')
            self._logger.debug('send_notification_report {}', action)  # noqa: PLE1205
            await soap_client.async_post_message_to(self.notify_to_url.path, message)
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

    async def async_send_notification_end_message(
            self,
            code: str = 'SourceShuttingDown',
            reason: str = 'Event source going off line.') -> ReceivedMessage | None:
        """Send notification end message to subscriber."""
        url = self.base_urls[0]
        my_addr = f'{url.scheme}:{url.netloc}/{url.path}'

        if not self.is_valid:
            return None
        soap_client = self._get_soap_client()
        subscription_end = evt_types.SubscriptionEnd()
        subscription_end.SubscriptionManager.Address = my_addr
        subscription_end.SubscriptionManager.ReferenceParameters = self.reference_parameters
        subscription_end.Status = code
        subscription_end.add_reason(reason, 'en-US')
        inf = HeaderInformationBlock(action=subscription_end.action,
                                     addr_to=self.end_to_address or self.notify_to_address,
                                     reference_parameters=self.end_to_ref_params or self.notify_ref_params)
        message = self._msg_factory.mk_soap_message(inf, payload=subscription_end)
        try:
            url = self._end_to_url or self.notify_to_url
            self._logger.info('async send subscription end to {}, subscription = {}', url, self)  # noqa: PLE1205
            return await soap_client.async_post_message_to(url.path, message)
        except aiohttp.client_exceptions.ClientConnectorError:
            # it does not matter that we could not send the message - end is end ;)
            self._logger.info('exception async send subscription end to {}, subscription = {}',  # noqa: PLE1205
                              url, self)
            pass
        except Exception:  # noqa: BLE001
            self._logger.error(traceback.format_exc())
        finally:
            self._is_closed = True


class AsyncioEventLoopThread(Thread):
    """Central event loop for provider."""

    def __init__(self, name: str):
        super().__init__(name=name)
        self.daemon = True
        self.loop = asyncio.new_event_loop()
        self.running = False

    def run(self):
        """Run method of thread."""
        self.running = True
        self.loop.run_forever()

    def run_coro(self, coro: Awaitable) -> Any:
        """Run threadsafe."""
        if not self.running:
            return None
        return asyncio.run_coroutine_threadsafe(coro, loop=self.loop).result()

    def stop(self):
        """Stop thread."""
        self.running = False
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.join()


class BICEPSSubscriptionsManagerBaseAsync(SubscriptionsManagerBase):
    """Async version of a subscriptions manager.

    It sends a notification parallel to all subscribers by using the async functionality.
    First all notifications are sent, then all http responses are collected.
    This saves a lot of time compared to the synchronous version that sends the notification to the first subscriber,
    waits for the response, then sends the notification to the second subscriber, and so on.
    """

    def __init__(self,
                 sdc_definitions: BaseDefinitions,
                 msg_factory: MessageFactory,
                 soap_client_pool: SoapClientPool,
                 max_subscription_duration: [float, None] = None,
                 log_prefix: str = None,
                 ):
        super().__init__(sdc_definitions, msg_factory, soap_client_pool, max_subscription_duration, log_prefix)
        if soap_client_pool.async_loop_subscr_mgr is None:
            thr = AsyncioEventLoopThread(name='async_loop_subscr_mgr')
            soap_client_pool.async_loop_subscr_mgr = thr
            thr.start()
        self._async_send_thread = soap_client_pool.async_loop_subscr_mgr

    def _mk_subscription_instance(self, request_data: RequestData) -> BicepsSubscriptionAsync:
        subscribe_request = evt_types.Subscribe.from_node(request_data.message_data.p_msg.msg_node)
        accepted_encodings = request_data.http_header['Accept-Encoding']
        return BicepsSubscriptionAsync(self, subscribe_request, accepted_encodings, self.base_urls,
                                       self._max_subscription_duration,
                                       self._soap_client_pool,
                                       msg_factory=self._msg_factory,
                                       log_prefix=self._logger.log_prefix)

    async def _coro_send_to_subscribers(self, tasks: Iterable[Awaitable]) -> tuple[BaseException | Any, ...]:
        return await asyncio.gather(*tasks, return_exceptions=True)

    def _end_all_subscriptions(self, send_subscription_end: bool):
        self._logger.info('end all subscriptions')
        tasks = []
        with self._subscriptions.lock:
            all_subscriptions = list(self._subscriptions.objects)
            if send_subscription_end:
                for subscription in all_subscriptions:
                    if subscription.is_valid and subscription.unsubscribed_at is None:
                        self._logger.info('send subscription end, subscription = {}', subscription)  # noqa: PLE1205
                        tasks.append(subscription.async_send_notification_end_message())
            if tasks:
                result = self._async_send_thread.run_coro(self._coro_send_to_subscribers(tasks))
                for counter, element in enumerate(result):
                    if isinstance(element, Exception):
                        self._logger.warning(  # noqa: PLE1205
                            'end_all_subscriptions {} returned {}', all_subscriptions[counter], element)

        apply_map(lambda subscription: subscription.close_by_subscription_manager(), self._subscriptions.objects)
        self._subscriptions.clear()

    def send_to_subscribers(self, payload: MessageType | xml_utils.LxmlElement,
                            action: str,
                            mdib_version_group: MdibVersionGroup):
        """Send payload to all subscribers."""
        with self._subscriptions.lock:
            if not self._async_send_thread.running:
                self._logger.info('could not send notifications, async send loop is not running.')
                return
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

            self._logger.debug('sending report %s to %r',
                               action, [s.notify_to_address for s in subscribers])
            result = self._async_send_thread.run_coro(self._coro_send_to_subscribers(tasks))
            if result is None:
                self._logger.info('could not send notifications, async send loop is not running.')
                return
            for counter, element in enumerate(result):
                if isinstance(element, Exception):
                    self._logger.warning(  # noqa: PLE1205
                        '{}: _send_to_subscribers {} returned {}', action, subscribers[counter], element)

    async def _async_send_notification_report(self, subscription: BicepsSubscriptionAsync,
                                              body_node: xml_utils.LxmlElement,
                                              action: str):
        try:
            self._logger.debug('send notification report {} to {}', action, subscription)  # noqa: PLE1205
            await subscription.async_send_notification_report(body_node, action)
            self._logger.debug(' done: send notification report {} to {}', action, subscription)  # noqa: PLE1205
        except HTTPReturnCodeError as ex:
            # this is an error related to the connection => log warning and continue
            self._logger.warning(  # noqa: PLE1205
                'could not send notification report {}: HTTP status= {}, reason={}, {}',
                action, ex.status, ex.reason, subscription)
        except (aiohttp.client_exceptions.ClientConnectionError,
                aiohttp.client_exceptions.ClientConnectorError,
                aiohttp.client_exceptions.ServerConnectionError,
                asyncio.exceptions.TimeoutError,
                socket.timeout) as ex:
            # this is an error related to the connection => log warning and continue
            self._logger.warning('could not send notification report {} warning= {!r}: {}',  # noqa: PLE1205
                               action, ex, subscription)
        except etree_.DocumentInvalid as ex:
            # this is an error related to the document, it cannot be sent to any subscriber => re-raise
            self._logger.warning('Invalid Document for action {}: {!r}\n{}',  # noqa: PLE1205
                               action, ex, etree_.tostring(body_node))
            raise
        except Exception:
            # this should never happen! => re-raise
            self._logger.error('could not send notification report {}, error {}: {}',  # noqa: PLE1205
                               action, traceback.format_exc(), subscription)
            raise

    def get_subscription_round_trip_times(self) -> dict[tuple[str, tuple[str]], RoundTripData]:
        """Calculate round trip times based on last MAX_ROUNDTRIP_VALUES values.

        :return: a dictionary with key=(<notify_to_address>, (subscription_names)),
                value = RoundTripData with members min, max, avg, abs_max, values
        """
        ret = {}
        with self._subscriptions.lock:
            for subscription in self._subscriptions.objects:
                if subscription.max_roundtrip_time > 0:
                    ret[(subscription.notify_to_address,
                         subscription.short_filter_names())] = subscription.get_roundtrip_stats()
        return ret

    def get_client_round_trip_times(self) -> dict[str, RoundTripData]:
        """Calculate round trip times based on last MAX_ROUNDTRIP_VALUES values.

        :return: a dictionary with key=<notify_to_address>,
        value = _RoundTripData with members min, max, avg, abs_max, values
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
            ret[key] = RoundTripData(all_values, max([s.max for s in stats]))
        return ret


class SubscriptionsManagerPathAsync(BICEPSSubscriptionsManagerBaseAsync):
    """Use path dispatching to identify subscriptions."""

    def _mk_subscription_instance(self, request_data: RequestData) -> BicepsSubscriptionAsync:
        subscription = super()._mk_subscription_instance(request_data)
        subscription.path_suffix = subscription.identifier_uuid.hex
        return subscription


class SubscriptionsManagerReferenceParamAsync(BICEPSSubscriptionsManagerBaseAsync):
    """Use reference parameters to identify subscriptions."""

    def _mk_subscription_instance(self, request_data: RequestData) -> BicepsSubscriptionAsync:
        subscription = super()._mk_subscription_instance(request_data)
        # add  a reference parameter
        subscription.set_reference_parameter()
        return subscription
