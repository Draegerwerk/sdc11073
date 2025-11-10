"""base subscription manager classes."""

from __future__ import annotations

import http.client
import socket
import time
import uuid
from collections import deque
from threading import Thread
from typing import TYPE_CHECKING, Any, Protocol
from urllib.parse import urlparse

from lxml import etree

from sdc11073 import loghelper, multikey, observableproperties, xml_utils
from sdc11073.etc import apply_map
from sdc11073.pysoap.soapclient import HTTPReturnCodeError, SoapClientProtocol
from sdc11073.pysoap.soapenvelope import Fault, faultcodeEnum
from sdc11073.xml_types import eventing_types as evt_types
from sdc11073.xml_types import isoduration
from sdc11073.xml_types.addressing_types import HeaderInformationBlock
from sdc11073.xml_types.basetypes import MessageType

if TYPE_CHECKING:
    import urllib.parse
    from collections.abc import Sequence
    from urllib.parse import SplitResult

    from sdc11073.definitions_base import BaseDefinitions
    from sdc11073.dispatch import RequestData
    from sdc11073.mdib.mdibbase import MdibVersionGroup
    from sdc11073.pysoap.msgfactory import CreatedMessage, MessageFactory
    from sdc11073.pysoap.soapclientpool import SoapClientPool

MAX_ROUNDTRIP_VALUES = 20


class RoundTripData:
    """Roundtrip data."""

    def __init__(self, values: Sequence[Any] | None, abs_max: int | None):
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

    def __repr__(self) -> str:
        return f'min={self.min:.4f} max={self.max:.4f} avg={self.avg:.4f} absmax={self.abs_max:.4f}'


def _mk_dispatch_identifier(reference_parameters: list, path_suffix: str) -> tuple[str | None, str | None]:
    # this is always our own reference parameter. We know that is has max. one element,
    # and the text is the identifier of the subscription
    if path_suffix == '':
        path_suffix = None
    if len(reference_parameters) > 0:
        return reference_parameters[0].text, path_suffix
    return None, path_suffix


class SubscriptionBase:
    """Subscription base."""

    MAX_NOTIFY_ERRORS = 1
    IDENT_TAG = etree.QName('http.local.com', 'MyDevIdentifier')

    def __init__(  # noqa: PLR0913
        self,
        mgr,  # noqa: ANN001
        subscribe_request: evt_types.Subscribe,
        accepted_encodings: list[str],
        base_urls: list[SplitResult],
        max_subscription_duration: int,
        soap_client_pool: SoapClientPool,
        msg_factory: MessageFactory,
        log_prefix: str,
    ):
        """Subscription base.

        :param mgr: the parent subscription manager
        :param subscribe_request:
        :param base_urls:
        :param max_subscription_duration:
        :param soap_client_pool: where to get the soap client for sending data
        :param msg_factory:
        """
        self._mgr = mgr  # seems to be unused
        self._logger = loghelper.get_logger_adapter('sdc.device.subscrMgr', log_prefix)
        self.base_urls = base_urls
        self._msg_factory = msg_factory
        self._soap_client_pool = soap_client_pool
        self.filter_type = subscribe_request.Filter
        if self.filter_type is None:
            msg = f'No filter provided for {self.__class__.__name__}'
            raise ValueError(msg)

        self.mode = subscribe_request.Delivery.Mode
        self.notify_to_address = subscribe_request.Delivery.NotifyTo.Address
        self.notify_to_url = urlparse(self.notify_to_address)
        self.notify_ref_params = subscribe_request.Delivery.NotifyTo.ReferenceParameters
        self.end_to_address = None
        self.end_to_ref_params = []
        if subscribe_request.EndTo is not None:
            self.end_to_address = subscribe_request.EndTo.Address
            self.end_to_ref_params = subscribe_request.EndTo.ReferenceParameters
            if self.end_to_address is not None:
                self._end_to_url = urlparse(self.end_to_address)
            else:
                self._end_to_url = None

        self.identifier_uuid = uuid.uuid4()
        self.reference_parameters = []  # default: no reference parameters
        self.path_suffix = None  # used for path based dispatching

        self._max_subscription_duration = max_subscription_duration
        self._started = None
        self._expire_seconds = None
        self.renew(subscribe_request.Expires)  # sets self._started and self._expire_seconds
        self._accepted_encodings = accepted_encodings

        self.notify_errors = 0
        self._is_closed = False
        self._is_connection_error = False
        self.last_roundtrip_times = deque(
            maxlen=MAX_ROUNDTRIP_VALUES,
        )  # a list of last n roundtrip times for notifications
        self.max_roundtrip_time = 0
        self.unsubscribed_at: float | None = None  # for housekeeping

    def set_reference_parameter(self):
        """Create a ReferenceParameters instance with a reference parameter."""
        reference_parameter = etree.Element(self.IDENT_TAG)
        reference_parameter.text = self.identifier_uuid.hex
        self.reference_parameters.append(reference_parameter)

    def renew(self, expires: float | None):
        """Renew a subscription."""
        self._started = time.monotonic()
        if expires:
            self._expire_seconds = min(expires, self._max_subscription_duration)
        else:
            self._expire_seconds = self._max_subscription_duration

    def matches(self, what: Any) -> bool:  # noqa: ARG002
        """Check if "what" parameter matches the filter criteria.

        The FilterType of eventing standard is very generic, it also has "any" types.
        THe type of "what" is determined by the Dialect parameter of FilterType.
        :param what: something that identifies a filter criteria of a notification message.
        :return:
        """
        return False

    def _get_soap_client(self) -> SoapClientProtocol:
        return self._soap_client_pool.get_soap_client(self.notify_to_url.netloc, self._accepted_encodings, self)

    @property
    def remaining_seconds(self) -> float:
        """Get the remaining seconds until expiry."""
        # do not use int, because it would round down to 0 even if the subscription is still valid, e.g. 0.5 seconds
        duration = round(self._expire_seconds - (time.monotonic() - self._started), 2)
        return max(duration, 0)

    @property
    def expire_string(self) -> str:
        """Get a duration string until invalid subscription."""
        return isoduration.duration_string(self.remaining_seconds)

    @property
    def has_delivery_failure(self) -> bool:
        """Determine whether this subscription has a delivery failure or not."""
        return self.notify_errors >= self.MAX_NOTIFY_ERRORS

    @property
    def has_connection_error(self) -> bool:
        """Determine whether this subscription has a connection error or not."""
        return self._is_connection_error

    @property
    def is_valid(self) -> bool:
        """Determine whether the subscription is valid or not."""
        if self._is_closed:
            return False
        return self.remaining_seconds > 0 and not self.has_delivery_failure

    def send_notification_end_message(
        self,
        code: str = 'SourceShuttingDown',
        reason: str = 'Event source going off line.',
    ):
        """Send a notification end message."""
        url = self.base_urls[0]
        my_addr = f'{url.scheme}:{url.netloc}/{url.path}'

        if not self.is_valid:
            return
        subscription_end = evt_types.SubscriptionEnd()
        subscription_end.SubscriptionManager.Address = my_addr
        subscription_end.SubscriptionManager.ReferenceParameters = self.reference_parameters
        subscription_end.Status = code
        subscription_end.add_reason(reason, 'en-US')
        inf = HeaderInformationBlock(
            action=subscription_end.action,
            addr_to=self.end_to_address or self.notify_to_address,
            reference_parameters=self.end_to_ref_params or self.notify_ref_params,
        )
        message = self._msg_factory.mk_soap_message(inf, payload=subscription_end)

        try:
            url = self._end_to_url or self.notify_to_url
            soap_client = self._get_soap_client()
            soap_client.post_message_to(url.path, message, msg='send_notification_end_message')
            self.notify_errors = 0
            self._is_connection_error = False
            self._is_closed = True
        except Exception as ex:  # noqa: BLE001
            # it does not matter that we could not send the message - end is end ;)
            self._logger.info('could not send subscription end message, error = {}', ex)  # noqa: PLE1205

    def close_by_subscription_manager(self) -> None:
        """Close subscription."""
        self._logger.info(  # noqa: PLE1205
            'close subscription id={} to {}',
            self.identifier_uuid,
            self.notify_to_address,
        )
        self._is_closed = True
        self._soap_client_pool.forget_usr(self.notify_to_url.netloc, self)

    def is_closed(self) -> bool:
        """Determine whether the subscription is closed or not."""
        return self._is_closed

    def __repr__(self) -> str:
        try:
            ref_ident = '<none>' if self.notify_ref_params is None else str(self.notify_ref_params)
        except TypeError:
            ref_ident = '<unknown>'
        return (
            f'{self.__class__.__name__}(notify_to={self.notify_to_address} ident={ref_ident}, '
            f'my identifier={self.identifier_uuid.hex}, expires={self.remaining_seconds})'
        )

    def get_roundtrip_stats(self) -> RoundTripData:
        """Get rountrip stats."""
        if len(self.last_roundtrip_times) > 0:
            return RoundTripData(self.last_roundtrip_times, self.max_roundtrip_time)
        return RoundTripData(None, None)

    def _mk_notification_message(
        self,
        header_info: HeaderInformationBlock,
        body_node: xml_utils.LxmlElement,
    ) -> CreatedMessage:
        return self._msg_factory.mk_soap_message_etree_payload(header_info, body_node)


class ActionBasedSubscription(SubscriptionBase):
    """Subscription for specific actions.

    Actions are a space separated list of strings in FilterType.text.
    """

    def __init__(self, *args, **kwargs):  # noqa: ANN002, ANN003
        # split the filter sting into separate action strings and keep them
        self.actions_filter: list[str] = []
        self._short_filter_names: list[str] = []  # helper for shorter log entries
        super().__init__(*args, **kwargs)
        if self.filter_type is not None:
            self.actions_filter.extend(self.filter_type.text.split())
            self._short_filter_names = [f.split('/')[-1] for f in self.actions_filter]

    def matches(self, what: str) -> bool:
        """Get what matches what.

        :param what: this must be a string
        :return: True if argument matches one of the strings in self.actions_filter, else False
        """
        action = what.strip()  # just to be sure there are no spaces
        return any(filter_string.endswith(action) for filter_string in self.actions_filter)

    def short_filter_names(self) -> list[str]:
        """Short filter names."""
        return self._short_filter_names

    def __repr__(self) -> str:
        try:
            ref_ident = '<none>' if self.notify_ref_params is None else str(self.notify_ref_params)
        except TypeError:
            ref_ident = '<unknown>'
        return (
            f'{self.__class__.__name__}(notify_to={self.notify_to_address} ident={ref_ident}, '
            f'my identifier={self.identifier_uuid.hex}, expires={self.remaining_seconds}, '
            f'filter={self._short_filter_names})'
        )


class SubscriptionManagerProtocol(Protocol):
    """Methods of a subscription manager."""

    def set_base_urls(self, base_urls: Sequence[urllib.parse.SplitResult]):
        """Own address, it must be sent to subscribers in subscribe responses."""
        ...

    def on_subscribe_request(self, request_data: RequestData) -> CreatedMessage:
        """Handle a subscribe request."""
        ...

    def on_unsubscribe_request(self, request_data: RequestData) -> CreatedMessage:
        """Handle an unsubscribe request."""
        ...

    def on_get_status_request(self, request_data: RequestData) -> CreatedMessage:
        """Handle a get status request."""
        ...

    def on_renew_request(self, request_data: RequestData) -> CreatedMessage:
        """Handle a renewal request."""
        ...

    def stop_all(self, send_subscription_end: bool):
        """Call when a device is shut down. It can send subscription end messages to all subscribers."""
        ...


class SubscriptionsManagerBase:
    """Base class for subscription manager."""

    DEFAULT_MAX_SUBSCR_DURATION = 7200  # max. possible duration of a subscription
    # observable has tuple(action, mdib_version_group, body_node)
    sent_to_subscribers = observableproperties.ObservableProperty(fire_only_on_changed_value=False)

    def __init__(
        self,
        sdc_definitions: BaseDefinitions,
        msg_factory: MessageFactory,
        soap_client_pool: SoapClientPool,
        max_subscription_duration: float | None = None,
        log_prefix: str | None = None,
    ):
        self.sdc_definitions = sdc_definitions
        self._msg_factory = msg_factory
        self._soap_client_pool: SoapClientPool = soap_client_pool
        self._logger = loghelper.get_logger_adapter('sdc.device.subscrMgr', log_prefix)
        self._max_subscription_duration = max_subscription_duration or self.DEFAULT_MAX_SUBSCR_DURATION
        self._subscriptions = multikey.MultiKeyLookup()
        self._subscriptions.add_index(
            'dispatch_identifier',
            multikey.UIndexDefinition(lambda obj: _mk_dispatch_identifier(obj.reference_parameters, obj.path_suffix)),
        )
        self._subscriptions.add_index('identifier', multikey.UIndexDefinition(lambda obj: obj.identifier_uuid.hex))
        self._subscriptions.add_index('netloc', multikey.IndexDefinition(lambda obj: obj.notify_to_url.netloc))
        self.base_urls: Sequence[urllib.parse.SplitResult] | None = None
        self._housekeeping_thread = Thread(target=self._do_housekeeping, name='housekeeping', daemon=True)
        self._run_housekeeping_thread = False
        self._housekeeping_thread.start()

    def set_base_urls(self, base_urls: Sequence[urllib.parse.SplitResult]):
        """Set base url.

        A subscription manager must know its own address, it must be sent to subscribers in subscribe responses.
        """
        self.base_urls = base_urls

    def _mk_subscription_instance(self, request_data: RequestData) -> SubscriptionBase:
        raise NotImplementedError

    def on_subscribe_request(self, request_data: RequestData) -> CreatedMessage:
        """Handle subscribe request."""
        subscription = self._mk_subscription_instance(request_data)
        with self._subscriptions.lock:
            self._subscriptions.add_object(subscription)
        self._logger.info('new {}', subscription)  # noqa: PLE1205
        return self._mk_subscribe_response_message(request_data, subscription, self.base_urls)

    def _mk_subscribe_response_message(
        self,
        request_data: RequestData,
        subscription: SubscriptionBase,
        base_urls: Sequence[urllib.parse.SplitResult],
    ) -> CreatedMessage:
        subscribe_response = evt_types.SubscribeResponse()

        path = '/'.join(request_data.consumed_path_elements)
        path_suffix = '' if subscription.path_suffix is None else f'/{subscription.path_suffix}'
        subscription_address = f'{base_urls[0].scheme}://{base_urls[0].netloc}/{path}{path_suffix}'
        subscribe_response.SubscriptionManager.Address = subscription_address
        subscribe_response.SubscriptionManager.ReferenceParameters = subscription.reference_parameters
        subscribe_response.Expires = subscription.remaining_seconds
        return self._msg_factory.mk_reply_soap_message(request_data, subscribe_response)

    def on_unsubscribe_request(self, request_data: RequestData) -> CreatedMessage:
        """Handle unsubscribe request."""
        subscription: SubscriptionBase = self._get_subscription_for_request(request_data)
        nsh = self.sdc_definitions.data_model.ns_helper
        if subscription is None:
            fault = Fault()
            fault.Code.Value = faultcodeEnum.RECEIVER
            fault.set_sub_code(nsh.WSE.tag('InvalidMessage'))
            fault.add_reason_text('unknown Subscription identifier')
            response = self._msg_factory.mk_reply_soap_message(request_data, fault, ns_map=[nsh.WSE])
        else:
            unsubscribe_response = evt_types.UnsubscribeResponse()
            response = self._msg_factory.mk_reply_soap_message(request_data, unsubscribe_response)
            subscription.unsubscribed_at = time.time()  # allow housekeeping to delete it delayed.
        return response

    def on_get_status_request(self, request_data: RequestData) -> CreatedMessage:
        """Handle get status request."""
        data_model = self.sdc_definitions.data_model
        nsh = data_model.ns_helper

        self._logger.debug('on_get_status_request {}', lambda: request_data.message_data.p_msg.raw_data)  # noqa: PLE1205
        subscription: SubscriptionBase = self._get_subscription_for_request(request_data)
        if subscription is None:
            fault = Fault()
            fault.Code.Value = faultcodeEnum.RECEIVER
            fault.set_sub_code(nsh.WSE.tag('InvalidMessage'))
            fault.add_reason_text('unknown Subscription identifier')
            response = self._msg_factory.mk_reply_soap_message(request_data, fault)
        else:
            get_status_response = evt_types.GetStatusResponse()
            get_status_response.Expires = subscription.remaining_seconds
            response = self._msg_factory.mk_reply_soap_message(request_data, get_status_response)
        return response

    def on_renew_request(self, request_data: RequestData) -> CreatedMessage:
        """Handle renew request."""
        data_model = self.sdc_definitions.data_model
        nsh = data_model.ns_helper
        renew = evt_types.Renew.from_node(request_data.message_data.p_msg.msg_node)
        expires = renew.Expires
        subscription: SubscriptionBase = self._get_subscription_for_request(request_data)
        if subscription is None:
            fault = Fault()
            fault.Code.Value = faultcodeEnum.RECEIVER
            fault.set_sub_code(nsh.WSE.tag('InvalidMessage'))
            fault.add_reason_text('unknown Subscription identifier')
            response = self._msg_factory.mk_reply_soap_message(request_data, fault)
        else:
            subscription.renew(expires)
            renew_response = evt_types.RenewResponse()
            renew_response.Expires = subscription.remaining_seconds
            response = self._msg_factory.mk_reply_soap_message(request_data, renew_response)
        return response

    def stop_all(self, send_subscription_end: bool):
        """Stop all subscriptions."""
        self._logger.info('stop_all called')
        # stop housekeeping thread first to get it out of the way
        self._logger.debug('stop housekeeping thread')
        self._run_housekeeping_thread = False
        self._housekeeping_thread.join()
        self._logger.debug('housekeeping thread stopped')
        self._logger.debug('end all subscriptions')
        self._end_all_subscriptions(send_subscription_end)

    def _end_all_subscriptions(self, send_subscription_end: bool):
        # async variant has a different implementation!
        with self._subscriptions.lock:
            if send_subscription_end:
                tmp = [s for s in self._subscriptions.objects if s.unsubscribed_at is None]
                apply_map(lambda subscription: subscription.send_notification_end_message(), tmp)
            apply_map(lambda subscription: subscription.close_by_subscription_manager(), self._subscriptions.objects)
            self._subscriptions.clear()

    def _get_subscription_for_request(self, request_data: RequestData) -> SubscriptionBase:
        reference_parameters = request_data.message_data.p_msg.header_info_block.reference_parameters
        path_suffix = '/'.join(request_data.path_elements)  # not consumed path elements
        dispatch_identifier = _mk_dispatch_identifier(reference_parameters, path_suffix)
        with self._subscriptions.lock:
            subscription = self._subscriptions.dispatch_identifier.get_one(dispatch_identifier, allow_none=True)
        if subscription is None:
            self._logger.warning(  # noqa: PLE1205
                '{}: unknown Subscription identifier "{}" from {}',
                request_data.message_data.q_name,
                dispatch_identifier,
                request_data.peer_name,
            )
        return subscription

    def send_to_subscribers(
        self,
        payload: MessageType | xml_utils.LxmlElement,
        action: str,
        mdib_version_group: MdibVersionGroup,
    ):
        """Send a payload to the subscribers."""
        subscribers = self._get_subscriptions_for_action(action)
        nsh = self.sdc_definitions.data_model.ns_helper
        # convert to element tree only once for all subscribers
        if isinstance(payload, MessageType):
            namespaces = [nsh.PM, nsh.MSG]
            namespaces.extend(payload.additional_namespaces)
            my_ns_map = nsh.partial_map(*namespaces)
            body_node = payload.as_etree_node(payload.NODETYPE, my_ns_map)
        else:
            body_node = payload

        self.sent_to_subscribers = (action, mdib_version_group, body_node)  # update observable
        for subscriber in subscribers:
            self._logger.debug('{}: sending report to {}', action, subscriber.notify_to_address)  # noqa: PLE1205
            self._send_notification_report(subscriber, body_node, action)

    def _send_notification_report(self, subscription, body_node: xml_utils.LxmlElement, action: str):  # noqa: ANN001
        try:
            subscription.send_notification_report(body_node, action)
        except ConnectionRefusedError as ex:
            self._logger.error('could not send notification report: {!r}:  subscr = {}', ex, subscription)  # noqa: PLE1205, TRY400
        except HTTPReturnCodeError as ex:
            # this is an error related to the connection => log error and continue
            self._logger.error(  # noqa: PLE1205, TRY400
                'could not send notification report: HTTP status= {}, reason={}, {}',
                ex.status,
                ex.reason,
                subscription,
            )
        except http.client.NotConnected as ex:
            # this is an error related to the connection => log error and continue
            self._logger.error('could not send notification report: {!r}:  subscr = {}', ex, subscription)  # noqa: PLE1205, TRY400
        except socket.timeout as ex:
            # this is an error related to the connection => log error and continue
            self._logger.error('could not send notification report error= {!r}: {}', ex, subscription)  # noqa: PLE1205, TRY400
        except etree.DocumentInvalid as ex:
            # this is an error related to the document, it cannot be sent to any subscriber => re-raise
            self._logger.error('Invalid Document: {!r}\n{}', ex, etree.tostring(body_node))  # noqa: PLE1205, TRY400
            raise
        except Exception:
            # this should never happen! => re-raise
            self._logger.exception('could not send notification report for subscription: {}', subscription)  # noqa: PLE1205
            raise

    def _get_subscriptions_for_action(self, action: str) -> list[Any]:
        with self._subscriptions.lock:
            return [s for s in self._subscriptions.objects if s.matches(action)]

    def _do_housekeeping(self):
        """Remove expired or invalid subscriptions. Method is executed in a thread."""
        self._run_housekeeping_thread = True
        while self._run_housekeeping_thread:
            time.sleep(1)
            now = time.time()
            with self._subscriptions.lock:
                obsolete_subscriptions = [
                    s
                    for s in self._subscriptions.objects
                    if not s.is_valid or (s.unsubscribed_at is not None and now > s.unsubscribed_at + 1)
                ]

                for obsolete_subscription in obsolete_subscriptions:
                    if not obsolete_subscription.is_closed():
                        obsolete_subscription.close_by_subscription_manager()

                        self._subscriptions.remove_object(obsolete_subscription)
