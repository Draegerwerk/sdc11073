from __future__ import annotations

import http.client
import threading
import time
import traceback
import uuid
from typing import TYPE_CHECKING, Callable, Protocol
from urllib.parse import urlparse

from lxml import etree as etree_

from sdc11073 import loghelper
from sdc11073 import observableproperties as properties
from sdc11073.namespaces import EventingActions
from sdc11073.pysoap.soapclient import HTTPException, HTTPReturnCodeError
from sdc11073.pysoap.soapenvelope import SoapResponseError
from sdc11073.xml_types import eventing_types as evt_types
from sdc11073.xml_types.addressing_types import HeaderInformationBlock

from .request_handler_deferred import EmptyResponse

if TYPE_CHECKING:
    from sdc11073.definitions_base import AbstractDataModel
    from sdc11073.dispatch import RequestData
    from sdc11073.pysoap.msgfactory import CreatedMessage, MessageFactory
    from sdc11073.pysoap.msgreader import MessageReader
    from sdc11073.pysoap.soapclient import SoapClientProtocol
    from sdc11073.xml_types.eventing_types import FilterType
    from sdc11073.xml_types.mex_types import HostedServiceType
    from sdc11073 import xml_utils


class ConsumerSubscriptionProtocol(Protocol):
    """The interface of a subscription instance."""

    def subscribe(self, expires: int | float = 3600,  # seconds
                  any_elements: list | None = None,
                  any_attributes: dict | None = None) -> None:
        """Send a Subscribe message."""

    def renew(self, expires: int | float = 3600) -> float:
        """Send a Renew message."""

    def get_status(self) -> float:
        """Send a GetStatus message."""

    @property
    def remaining_subscription_seconds(self) -> float:
        """Return the time span until when the subscription will expire."""

    def on_notification(self, request_data: RequestData) -> CreatedMessage:
        """Process incoming notification."""

    def unsubscribe(self):
        """Send Unsubscribe message."""


class ConsumerSubscription:
    """ConsumerSubscription handles a subscription to an event source.

    It stores all key data of the subscription and can renew and unsubscribe this subscription.
    """

    notification_msg = properties.ObservableProperty()
    notification_data = properties.ObservableProperty()
    IDENT_TAG = etree_.QName('http.local.com', 'MyClIdentifier')
    is_subscribed = properties.ObservableProperty(False)

    def __init__(self, msg_factory: MessageFactory,  # noqa: PLR0913
                 data_model: AbstractDataModel,
                 get_soap_client_func: Callable[[str], SoapClientProtocol],
                 dpws_hosted: HostedServiceType,
                 filter_type: FilterType,
                 notification_url: str,
                 end_to_url: str,
                 log_prefix: str):
        """Construct a ConsumerSubscription."""
        self._msg_factory = msg_factory
        self._data_model = data_model
        self._get_soap_client_func = get_soap_client_func
        self._dpws_hosted = dpws_hosted
        self._filter_type = filter_type
        self._filter_text = filter_type.text
        self.notification_url = notification_url
        self.end_to_url = end_to_url
        self.is_subscribed = False
        self._is_subscribed_lock = threading.Lock()
        self.end_status = None  # if device sent a SubscriptionEnd message, this contains the status from the message
        self.end_reason = None  # if device sent a SubscriptionEnd message, this contains the reason from the message
        self.expires_at: float = 0.0
        self.requested_expires: int | float = 0
        self.granted_expires: int | float = 0

        self.notify_to_identifier: xml_utils.LxmlElement | None = None

        self.end_to_identifier: xml_utils.LxmlElement | None = None

        self._logger = loghelper.get_logger_adapter('sdc.client.subscr', log_prefix)
        self.event_counter = 0  # for display purpose, we count notifications
        # ToDo: check if there is more than one address. In that case a clever selection is needed
        self._hosted_service_address: str = self._dpws_hosted.EndpointReference[0].Address
        self._hosted_service_path = urlparse(self._hosted_service_address).path
        self._subscription_manager_path: str | None = None
        self.subscribe_response: evt_types.SubscribeResponse | None = None

    def subscribe(self, expires: int | float = 3600,
                  any_elements: list | None = None,
                  any_attributes: dict | None = None) -> None:
        """Send a subscribe request to the provider and handle the response."""
        self._logger.info('start subscription "{}"', self.short_filter_string)  # noqa: PLE1205
        self.event_counter = 0
        self.requested_expires = expires  # saved for later renewal, we will use the same interval
        subscribe_request = evt_types.Subscribe()
        # EndTo is an optional element
        if self.end_to_url is not None:
            subscribe_request.init_end_to()
            subscribe_request.EndTo.Address = self.end_to_url
            if self.end_to_identifier is not None:
                subscribe_request.EndTo.ReferenceParameters = [self.end_to_identifier]
        # DeliveryMode always is the same value
        subscribe_request.Delivery.Mode = f'{self._data_model.ns_helper.WSE.namespace}/DeliveryModes/Push'
        # NotifyTo
        subscribe_request.Delivery.NotifyTo.Address = self.notification_url
        if self.notify_to_identifier is not None:
            subscribe_request.Delivery.NotifyTo.ReferenceParameters = [self.notify_to_identifier]
        subscribe_request.Expires = expires
        subscribe_request.Filter = self._filter_type
        # make an elementtree from subscribe_request and add any_elements and any_attributes if present
        nsh = self._data_model.ns_helper
        nsmap = nsh.partial_map(nsh.WSE, nsh.MSG, nsh.PM)
        body_node = subscribe_request.as_etree_node(subscribe_request.NODETYPE, nsmap)
        if any_elements is not None:
            body_node.extend(any_elements)
        if any_attributes is not None:
            for name, value in any_attributes.items():
                body_node.set(name, value)
        inf = HeaderInformationBlock(action=EventingActions.Subscribe,
                                     addr_to=self._hosted_service_address)
        message = self._msg_factory.mk_soap_message_etree_payload(inf, body_node)
        msg = f'subscribe {self.short_filter_string}'
        try:
            soap_client = self._get_soap_client_func(self._hosted_service_address)
            message_data = soap_client.post_message_to(self._hosted_service_path, message, msg=msg)
            try:
                self.subscribe_response = evt_types.SubscribeResponse.from_node(message_data.p_msg.msg_node)
                self.is_subscribed = True
                subscription_manager_address = self.subscribe_response.SubscriptionManager.Address
                self._subscription_manager_path = urlparse(subscription_manager_address).path
                self.granted_expires = self.subscribe_response.Expires
                self.expires_at = time.time() + self.granted_expires
                self._logger.info('Subscribe was successful: expires at {}, address="{}"',  # noqa: PLE1205
                                  self.expires_at, self.subscribe_response.SubscriptionManager.Address)
            except AttributeError as ex:
                self._logger.error('Subscribe response has unexpected content: {}',  # noqa: PLE1205
                                   message_data.p_msg.raw_data)
                self.is_subscribed = False
                raise SoapResponseError(message_data.p_msg) from ex
        except HTTPReturnCodeError as ex:
            self._logger.error('could not subscribe: %r', ex)

    def renew(self, expires: int = 3600) -> float:
        """Send a Renew request to the provider and handle the response.

        :return: the remaining time of the subscription or 0.0, if the request was not successful
        """
        with self._is_subscribed_lock:
            if not self.is_subscribed:
                return 0.0
            renew = evt_types.Renew()
            renew.Expires = expires
            dev_reference_param = self.subscribe_response.SubscriptionManager.ReferenceParameters
            subscription_manager_address = self.subscribe_response.SubscriptionManager.Address
            inf = HeaderInformationBlock(action=renew.action,
                                         addr_to=subscription_manager_address,
                                         reference_parameters=dev_reference_param)
            message = self._msg_factory.mk_soap_message(inf, payload=renew)
            try:
                soap_client = self._get_soap_client_func(subscription_manager_address)
                message_data = soap_client.post_message_to(
                    self._subscription_manager_path, message, msg='renew')
                self._logger.debug('{}', message_data.p_msg.raw_data)  # noqa: PLE1205
            except HTTPReturnCodeError as ex:
                self.is_subscribed = False
                self._logger.error('could not renew: {}', ex)  # noqa: PLE1205
            except (http.client.HTTPException, ConnectionError) as ex:
                self._logger.warning('renew failed: {}', ex)  # noqa: PLE1205
                self.is_subscribed = False
            except Exception as ex:  # noqa: BLE001
                # log any other exception as error and consider subscription to be broken
                self._logger.error('Exception in renew: {}', ex)  # noqa: PLE1205
                self.is_subscribed = False
            else:
                renew_response = evt_types.RenewResponse.from_node(message_data.p_msg.msg_node)
                self.granted_expires = renew_response.Expires
                if self.granted_expires is not None:
                    self.expires_at = time.time() + self.granted_expires
                    return self.granted_expires
                self.is_subscribed = False
                self._logger.warning('renew failed: {}',  # noqa: PLE1205
                                     etree_.tostring(message_data.p_msg.body_node, pretty_print=True))
            return 0.0

    def unsubscribe(self):
        """Send an unsubscribe request to the provider and handle the response."""
        with self._is_subscribed_lock:
            if not self.is_subscribed:
                return
            request = evt_types.Unsubscribe()
            dev_reference_param = self.subscribe_response.SubscriptionManager.ReferenceParameters
            subscription_manager_address = self.subscribe_response.SubscriptionManager.Address
            inf = HeaderInformationBlock(action=request.action,
                                         addr_to=subscription_manager_address,
                                         reference_parameters=dev_reference_param)
            message = self._msg_factory.mk_soap_message(inf, payload=request)
            soap_client = self._get_soap_client_func(subscription_manager_address)
            received_message_data = soap_client.post_message_to(self._subscription_manager_path,
                                                                message, msg='unsubscribe')
            response_action = received_message_data.action
            # check response: response does not contain explicit status. If action== UnsubscribeResponse all is fine.
            if response_action == EventingActions.UnsubscribeResponse:
                self._logger.info(  # noqa: PLE1205
                    'unsubscribe: end of subscription {} was confirmed.', self.notification_url)
                self.is_subscribed = False
            else:
                self._logger.error(  # noqa: PLE1205
                    'unsubscribe: unexpected response action: {}', received_message_data.p_msg.raw_data)
                raise ValueError(f'unsubscribe: unexpected response action: {received_message_data.p_msg.raw_data}')

    def get_status(self) -> float:
        """Send a GetStatus Request to the device.

        :return: the remaining time of the subscription or 0, if the request was not successful
        """
        with self._is_subscribed_lock:
            if not self.is_subscribed:
                return 0.0
            request = evt_types.GetStatus()
            dev_reference_param = self.subscribe_response.SubscriptionManager.ReferenceParameters
            subscription_manager_address = self.subscribe_response.SubscriptionManager.Address
            inf = HeaderInformationBlock(action=request.action,
                                         addr_to=subscription_manager_address,
                                         reference_parameters=dev_reference_param)
            message = self._msg_factory.mk_soap_message(inf, payload=request)
            try:
                soap_client = self._get_soap_client_func(subscription_manager_address)
                message_data = soap_client.post_message_to(
                    self._subscription_manager_path,
                    message, msg='get_status')
            except HTTPReturnCodeError as ex:
                self.is_subscribed = False
                self._logger.error('could not get status: {}', ex)  # noqa: PLE1205
            except (http.client.HTTPException, ConnectionError) as ex:
                self.is_subscribed = False
                self._logger.warning(  # noqa: PLE1205
                    'get_status: Connection Error {} for subscription {}', ex, self._filter_text)
            except Exception as ex:  # noqa: BLE001
                # log any other exception as error and consider subscription to be broken
                self._logger.error('Exception in get_status: {}', ex)  # noqa: PLE1205
                self.is_subscribed = False
            else:
                get_status_response = evt_types.GetStatusResponse.from_node(message_data.p_msg.msg_node)
                expire_seconds = get_status_response.Expires
                if expire_seconds is None:
                    self._logger.warning(  # noqa: PLE1205
                        'get_status for {}: Could not find "Expires" node! get_status={} ', self._filter_text,
                        message_data.p_msg.raw_data)
                    raise SoapResponseError(message_data.p_msg)
                self._logger.debug('get_status for {}: Expires in {} seconds, counter = {}',  # noqa: PLE1205
                                   self._filter_text, expire_seconds, self.event_counter)
                return expire_seconds
            return 0.0

    @property
    def remaining_subscription_seconds(self) -> float:
        """Return remaining time."""
        return self.expires_at - time.time()

    def on_notification(self, request_data: RequestData) -> CreatedMessage:
        """Handle an incoming notification."""
        self.event_counter += 1
        self._logger.debug('received notification {} version= {}',  # noqa: PLE1205
                           request_data.message_data.action, request_data.message_data.mdib_version_group)
        self.notification_data = request_data.message_data
        self.notification_msg = request_data.message_data.p_msg
        return EmptyResponse()

    @property
    def short_filter_string(self) -> str:
        """Return a shorter version of the filter list with only the last elements of the actions."""
        if self._filter_text is not None and len(self._filter_text) > 0:
            return ', '.join(e.split('/')[-1] for e in self._filter_type.text.split())
        return '<unknown>'

    def __str__(self) -> str:
        if self._filter_text is not None:
            return (f'Subscription of "{self.short_filter_string}", is_subscribed={self.is_subscribed}, '
                    f'remaining time = {int(self.remaining_subscription_seconds)} sec., count={self.event_counter}')
        return ', '.join(str(a) for a in self._filter_type.any)


class ConsumerSubscriptionManagerProtocol(Protocol):
    """Factory for Subscription objects."""

    def __init__(self,  # noqa: PLR0913
                 msg_reader: MessageReader,
                 msg_factory: MessageFactory,
                 data_model: AbstractDataModel,
                 get_soap_client_func: Callable[[str], SoapClientProtocol],
                 notification_url: str,
                 end_to_url: str | None = None,
                 fixed_renew_interval: int | None = None,
                 log_prefix: str = ''):
        """Construct a ConsumerSubscriptionManager.

        :param msg_reader: xml -> internal representation
        :param msg_factory: internal representation -> xml
        :param notification_url: send to this address
        :param end_to_url: send to this address
        :param fixed_renew_interval: if set, renew is sent in this interval.
                                     if None, renew is sent when remaining time <= 50% of granted time
        :param log_prefix:
        """

    def start(self):
        """Start the subscription manager (typically starts a thread)."""

    def stop(self):
        """Stop the subscription manager."""

    def mk_subscription(self, dpws_hosted: HostedServiceType, filter_type: FilterType) -> ConsumerSubscriptionProtocol:
        """Create a subscription instance."""

    def on_subscription_end(self, request_data: RequestData) -> ConsumerSubscription | None:
        """Handle SubscriptionEnd message from provider."""

    def unsubscribe_all(self) -> bool:
        """Send Unsubscribe messages for all subscriptions."""


class ConsumerSubscriptionManager(threading.Thread,
                                  ConsumerSubscriptionManagerProtocol):  # derive from protocol to help typing.
    """Factory for Subscription objects.

    The thread periodically renews subscriptions. This tells the provider to keep the connection alive.
    Background info: providers can close a socket after a certain time without traffic.
    Periodic requests avoid the timeout

    This Implementation uses unique paths for notifications and SubscriptionEnd messages for each subscription.
    When the provider sends one of these messages, the unique url is enough to identify the corresponding
    subscription instance of the consumer. Reference parameters are not used.
    """

    def __init__(self, msg_reader: MessageReader,  # noqa: PLR0913
                 msg_factory: MessageFactory,
                 data_model: AbstractDataModel,
                 get_soap_client_func: Callable[[str], SoapClientProtocol],
                 notification_url: str,
                 end_to_url: str | None = None,
                 fixed_renew_interval: int | None = None,
                 log_prefix: str = ''):
        """Construct a ConsumerSubscriptionManager.

        :param msg_factory:
        :param notification_url:
        :param end_to_url:
        :param fixed_renew_interval: if set, renew is sent in this interval.
                                     if None, renew is sent when remaining time <= 50% of granted time
        :param log_prefix:
        """
        super().__init__(name=f'SubscriptionClient{log_prefix}')
        self.daemon = True
        self._msg_reader = msg_reader
        self._msg_factory = msg_factory
        self._data_model = data_model
        self._get_soap_client_func = get_soap_client_func
        self._renew_interval = fixed_renew_interval
        self.subscriptions: dict[str, ConsumerSubscription] = {}
        self._subscriptions_lock = threading.Lock()

        self._run = False
        self._notification_url = notification_url
        self._end_to_url = end_to_url or notification_url
        self._logger = loghelper.get_logger_adapter('sdc.client.subscrMgr', log_prefix)
        self.log_prefix = log_prefix
        self._counter = 1  # used to generate unique path for each subscription

    def stop(self):
        """Stop the thread."""
        self._run = False
        self.join(timeout=2)
        with self._subscriptions_lock:
            self.subscriptions.clear()

    def run(self):
        """Perform thread."""
        self._run = True
        try:
            if self._renew_interval is not None:
                self._fixed_renew_interval_loop()
            else:
                self._flexible_renew_interval_loop()
        finally:
            self._logger.info('terminating subscriptions check loop! self._run={}', self._run)  # noqa: PLE1205

    def _fixed_renew_interval_loop(self):
        """Renew subscriptions in a fixed period."""
        while self._run:
            try:
                for _ in range(self._renew_interval):
                    time.sleep(1)
                    if not self._run:
                        return
                with self._subscriptions_lock:
                    # copy list of subscriptions in order to release lock early
                    subscriptions = list(self.subscriptions.values())
                for subscription in subscriptions:
                    subscription.renew()
                for subscription in subscriptions:
                    self._logger.debug('{}', subscription)  # noqa: PLE1205
            except Exception:  # noqa: BLE001
                # catch all in order to keep thread running.
                self._logger.error('##### check loop: {}', traceback.format_exc())  # noqa: PLE1205

    def _flexible_renew_interval_loop(self):
        """Renew subscriptions when remaining time <= 50% of granted time."""
        while self._run:
            try:
                time.sleep(1)
                if not self._run:
                    return
                with self._subscriptions_lock:
                    # copy list of subscriptions in order to release lock early
                    subscriptions = list(self.subscriptions.values())
                for subscription in subscriptions:
                    # renew if remaining time is 50% of granted time or less.
                    if subscription.remaining_subscription_seconds <= subscription.granted_expires / 2:
                        subscription.renew()
                for subscription in subscriptions:
                    self._logger.debug('{}', subscription)  # noqa: PLE1205
            except Exception:  # noqa: BLE001
                # catch all in order to keep thread running.
                self._logger.error('##### check loop: {}', traceback.format_exc())  # noqa: PLE1205

    def mk_subscription(self, dpws_hosted: HostedServiceType, filter_type: FilterType) -> ConsumerSubscription:
        """Create a subscription instance."""
        sep = '' if self._notification_url.endswith('/') else '/'
        notification_url = f'{self._notification_url}{sep}subscr{self._counter}'
        sep = '' if self._end_to_url.endswith('/') else '/'
        end_to_url = f'{self._end_to_url}{sep}subscr{self._counter}_e'
        self._counter += 1
        subscription = ConsumerSubscription(self._msg_factory, self._data_model,
                                            self._get_soap_client_func,
                                            dpws_hosted, filter_type, notification_url,
                                            end_to_url, self.log_prefix)
        filter_ = filter_type.text
        with self._subscriptions_lock:
            self.subscriptions[filter_] = subscription
        return subscription

    def _find_subscription(self, request_data: RequestData,
                           log_prefix: str) -> ConsumerSubscription | None:
        for subscription in self.subscriptions.values():
            if subscription.end_to_url.endswith(request_data.current_path_element):
                return subscription
        self._logger.warning(  # noqa: PLE1205
            '{}: have no subscription for identifier = {}', log_prefix, request_data.current_path_element)
        return None

    def on_subscription_end(self, request_data: RequestData) -> ConsumerSubscription | None:
        """Handle SubscriptionEnd message from provider."""
        subscription_end = evt_types.SubscriptionEnd.from_node(request_data.message_data.p_msg.msg_node)
        info = f' status={subscription_end.Status} ' if subscription_end.Status else ''
        if len(subscription_end.Reason) > 0:
            if len(subscription_end.Reason) == 1:
                info += f' reason = {subscription_end.Reason[0]}'
            else:
                info += f' reasons = {subscription_end.Reason}'
        subscription = self._find_subscription(request_data,
                                               'on_subscription_end')
        if subscription is not None:
            self._logger.info('on_subscription_end: received Subscription End for {} {}',  # noqa: PLE1205
                              subscription.short_filter_string,
                              info)
            subscription.is_subscribed = False
            subscription.end_status = subscription_end.Status
            if len(subscription_end.Reason) > 0:
                subscription.end_reason = subscription_end.Reason[0]
            return subscription
        return None

    def unsubscribe_all(self) -> bool:
        """Send Unsubscribe messages for all subscriptions."""
        ret = True
        with self._subscriptions_lock:
            current_subscriptions = list(self.subscriptions.values())  # make a copy
            self.subscriptions.clear()
            for subscription in current_subscriptions:
                try:
                    subscription.unsubscribe()
                except HTTPException as ex:
                    self._logger.info('unsubscribe failed got HTTPException: {}', ex)  # noqa: PLE1205
                except Exception:  # noqa: BLE001
                    self._logger.error(  # noqa: PLE1205
                        'unsubscribe error: {}\n call stack:{} ', traceback.format_exc(), traceback.format_stack())
                    ret = False
        return ret


class ClientSubscriptionManagerReferenceParams(ConsumerSubscriptionManager):
    """Factory for Subscription objects. It uses reference parameters for identification of a subscription."""

    def mk_subscription(self, dpws_hosted: HostedServiceType, filter_type: FilterType) -> ConsumerSubscription:
        """Create a subscription instance."""
        subscription = ConsumerSubscription(self._msg_factory,
                                            self._data_model,
                                            self._get_soap_client_func,
                                            dpws_hosted, filter_type,
                                            self._notification_url,
                                            self._end_to_url, self.log_prefix)
        subscription.notify_to_identifier = etree_.Element(ConsumerSubscription.IDENT_TAG)
        subscription.notify_to_identifier.text = uuid.uuid4().urn
        subscription.end_to_identifier = etree_.Element(ConsumerSubscription.IDENT_TAG)
        subscription.end_to_identifier.text = uuid.uuid4().urn

        filter_ = filter_type.text
        with self._subscriptions_lock:
            self.subscriptions[filter_] = subscription
        return subscription

    def _find_subscription(self, request_data: RequestData,
                           log_prefix: str) -> ConsumerSubscription | None:
        subscr_ident_list = request_data.message_data.p_msg.header_info_block.reference_parameters
        if not subscr_ident_list:
            return None
        subscr_ident = subscr_ident_list[0]
        for subscription in self.subscriptions.values():
            if subscr_ident.text == subscription.end_to_identifier.text:
                return subscription
        self._logger.warning(  # noqa: PLE1205
            '{}}: have no subscription for identifier = {}', log_prefix, subscr_ident.text)
        return None
