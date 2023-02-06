from __future__ import annotations

import asyncio
import http.client
import socket
import time
import traceback
import uuid
from collections import deque, defaultdict
from threading import Thread
from typing import TYPE_CHECKING
from urllib.parse import urlparse

import aiohttp.client_exceptions
from lxml import etree as etree_

from .subscriptionmgr import SubscriptionsManagerBase
from .. import isoduration
from .. import observableproperties
from ..addressing import ReferenceParameters, Address
from ..etc import short_filter_string
from ..pysoap.soapclient import HTTPReturnCodeError

if TYPE_CHECKING:
    from ..definitions_base import BaseDefinitions
    from ..pysoap.msgfactory import MessageFactoryDevice
    from .subscriptionmgr import SoapClientPool

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


def _mk_dispatch_identifier(reference_parameters: ReferenceParameters, path_suffix: str):
    # this is always our own reference parameter. We know that is has max. one element,
    # and the text is the identifier of the subscription
    if path_suffix == '':
        path_suffix = None
    if reference_parameters.has_parameters:
        return reference_parameters.parameters[0].text, path_suffix
    return None, path_suffix


class _DevSubscription:
    MAX_NOTIFY_ERRORS = 1
    IDENT_TAG = etree_.QName('http.local.com', 'MyDevIdentifier')

    def __init__(self, subscribe_request, base_urls, max_subscription_duration,
                 msg_factory):
        """

        :param subscribe_request:
        :param base_urls:
        :param max_subscription_duration:
        :param msg_factory:
        """
        self.mode = subscribe_request.mode
        self.base_urls = base_urls
        self._msg_factory = msg_factory
        self.notify_to_address = subscribe_request.notify_to_address
        self.notify_to_url = urlparse(subscribe_request.notify_to_address)
        self.notify_ref_params = subscribe_request.notify_ref_params  # reference parameters of other side
        self.end_to_address = subscribe_request.end_to_address
        if self.end_to_address is not None:
            self._end_to_url = urlparse(self.end_to_address)
        else:
            self._end_to_url = None
        self.end_to_ref_params = subscribe_request.end_to_ref_params
        self.identifier_uuid = uuid.uuid4()
        self.reference_parameters = ReferenceParameters(None)  # default: no reference parameters
        self.path_suffix = None  # used for path based dispatching

        self._max_subscription_duration = max_subscription_duration
        self._started = None
        self._expire_seconds = None
        self.renew(subscribe_request.expires)  # sets self._started and self._expire_seconds
        self.filters = subscribe_request.subscription_filters

        self._accepted_encodings = subscribe_request.accepted_encodings  # these encodings are accepted by other side
        self._soap_client = None

        self.notify_errors = 0
        self._is_closed = False
        self._is_connection_error = False
        self.last_roundtrip_times = deque(
            maxlen=MAX_ROUNDTRIP_VALUES)  # a list of last n roundtrip times for notifications
        self.max_roundtrip_time = 0

    def set_reference_parameter(self):
        """Create a ReferenceParameters instance with a reference parameter"""
        reference_parameter = etree_.Element(self.IDENT_TAG)
        reference_parameter.text = self.identifier_uuid.hex
        self.reference_parameters = ReferenceParameters([reference_parameter])

    def set_soap_client(self, soap_client):
        self._soap_client = soap_client

    def renew(self, expires):
        self._started = time.monotonic()
        if expires:
            self._expire_seconds = min(expires, self._max_subscription_duration)
        else:
            self._expire_seconds = self._max_subscription_duration

    @property
    def soap_client(self):
        return self._soap_client

    @property
    def remaining_seconds(self):
        duration = int(self._expire_seconds - (time.monotonic() - self._started))
        return 0 if duration < 0 else duration

    @property
    def expire_string(self):
        return isoduration.duration_string(self.remaining_seconds)

    @property
    def has_delivery_failure(self):
        return self.notify_errors >= self.MAX_NOTIFY_ERRORS

    @property
    def has_connection_error(self):
        return self._is_connection_error

    @property
    def is_valid(self):
        if self._is_closed:
            return False
        return self.remaining_seconds > 0 and not self.has_delivery_failure

    def matches(self, action):
        action = action.strip()  # just to be sure there are no spaces....
        for filter_string in self.filters:
            if filter_string.endswith(action):
                return True
        return False

    async def async_send_notification_report(self, msg_factory, body_node, action, doc_nsmap):
        if not self.is_valid:
            return
        addr = Address(addr_to=self.notify_to_address,
                       action=action,
                       addr_from=None,
                       reply_to=None,
                       fault_to=None,
                       reference_parameters=None)
        message = msg_factory.mk_notification_message(addr, body_node, self.notify_ref_params, doc_nsmap)
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
        message = self._msg_factory.mk_notification_end_message(self, my_addr, code, reason)
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

    def close(self):
        self._is_closed = True

    def is_closed(self):
        return self._is_closed

    def __repr__(self):
        try:
            if self.notify_ref_params.has_parameters:
                ref_ident = f' ReferenceParameters(other side)={[etree_.tostring(p) for p in self.notify_ref_params.parameters]}'
            else:
                ref_ident = ''
        except TypeError as ex:
            ref_ident = f' ident=<{ex}>'
        return f'Subscription(notify_to={self.notify_to_address}{ref_ident}, ' \
               f'my identifier={self.identifier_uuid.hex}, path suffix={self.path_suffix}, ' \
               f'expires={self.remaining_seconds}, filter={short_filter_string(self.filters)})'

    def get_roundtrip_stats(self):
        if len(self.last_roundtrip_times) > 0:
            return _RoundTripData(self.last_roundtrip_times, self.max_roundtrip_time)
        return _RoundTripData(None, None)

    def short_filter_names(self):
        return tuple([f.split('/')[-1] for f in self.filters])


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
                 msg_factory: MessageFactoryDevice,
                 soap_client_pool: SoapClientPool,
                 max_subscription_duration: [float, None] = None,
                 log_prefix: str = None,
                 ):
        super().__init__(sdc_definitions, msg_factory, soap_client_pool, max_subscription_duration)
        self._async_send_thread = AsyncioEventLoopThread(name='async_send')
        self._async_send_thread.start()

    def _mk_subscription_instance(self, subscribe_request):
        return _DevSubscription(subscribe_request, self.base_urls, self._max_subscription_duration,
                                msg_factory=self._msg_factory)

    async def _close_soap_client(self, soap_client):
        await soap_client.async_close()

    async def _coro_send_to_subscribers(self, tasks):
        return await asyncio.gather(*tasks, return_exceptions=True)

    def send_to_subscribers(self, body_node, action, mdib_version_group, nsmapper, what):
        subscribers = self._get_subscriptions_for_action(action)
        self.sent_to_subscribers = (action, mdib_version_group, body_node)  # update observable
        tasks = []
        for subscriber in subscribers:
            tasks.append(self._async_send_notification_report(
                subscriber, body_node, action,
                nsmapper.partial_map(nsmapper.PM, nsmapper.S12, nsmapper.WSA, nsmapper.WSE)))

        if what:
            self._logger.debug('{}: sending report to {}', what, [s.notify_to_address for s in subscribers])
        result = self._async_send_thread.run_coro(self._coro_send_to_subscribers(tasks))
        for counter, element in enumerate(result):
            if isinstance(element, Exception):
                self._logger.warning(f'{what}: _send_to_subscribers {subscribers[counter]} returned {element}')

    async def _async_send_notification_report(self, subscription, body_node, action, doc_nsmap):
        try:
            self._logger.debug(f'send notification report {action} to {subscription}')
            await subscription.async_send_notification_report(self._msg_factory, body_node, action, doc_nsmap)
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

    def _mk_subscription_instance(self, subscribe_request):
        subscription = super()._mk_subscription_instance(subscribe_request)
        subscription.path_suffix = subscription.identifier_uuid.hex
        return subscription


class SubscriptionsManagerReferenceParamAsync(SubscriptionsManagerBaseAsync):
    """This implementation uses reference parameters to identify subscriptions."""

    def _mk_subscription_instance(self, subscribe_request):
        subscription = super()._mk_subscription_instance(subscribe_request)
        # add  a reference parameter
        subscription.set_reference_parameter()
        return subscription
