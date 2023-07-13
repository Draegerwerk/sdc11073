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
    from sdc11073.pysoap.msgfactory import MessageFactory
    from sdc11073.pysoap.msgreader import MessageReader
    from sdc11073.pysoap.soapclient import SoapClientProtocol
    from sdc11073.pysoap.msgfactory import CreatedMessage
    from sdc11073.xml_types.eventing_types import FilterType
    from sdc11073.xml_types.mex_types import HostedServiceType


SUBSCRIPTION_CHECK_INTERVAL = 5  # seconds


class ConsumerSubscriptionProtocol(Protocol):
    """The interface of a subscription instance."""

    def subscribe(self, expire_minutes: int = 60,
                  any_elements: list | None = None,
                  any_attributes: dict | None = None) -> None:
        """Send a Subscribe message."""

    def renew(self, expire_minutes: int = 60) -> float:
        """Send a Renew message."""

    def get_status(self) -> float:
        """Send a GetStatus message."""

    def check_status(self, renew_limit: int | float):
        """Send GetStatus message and update internal data."""

    def check_status_renew(self):
        """Send Renew message and update internal data."""

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
        self.is_subscribed = False
        self.end_status = None  # if device sent a SubscriptionEnd message, this contains the status from the message
        self.end_reason = None  # if device sent a SubscriptionEnd message, this contains the reason from the message
        self.expire_at = None
        self.expire_minutes = None

        self.notification_url = notification_url
        self.notify_to_identifier = None

        self.end_to_url = end_to_url
        self.end_to_identifier = None

        self._logger = loghelper.get_logger_adapter('sdc.client.subscr', log_prefix)
        self.event_counter = 0  # for display purpose, we count notifications
        # ToDo: check if there is more than one address. In that case a clever selection is needed
        self._hosted_service_address: str = self._dpws_hosted.EndpointReference[0].Address
        self._hosted_service_path = urlparse(self._hosted_service_address).path
        self._subscription_manager_path: str | None = None
        self.subscribe_response: evt_types.SubscribeResponse | None = None

    def subscribe(self, expire_minutes: int = 60,
                  any_elements: list | None = None,
                  any_attributes: dict | None = None) -> None:
        """Send a subscribe request to the provider and handle the response."""
        self._logger.info('start subscription "{}"', self.short_filter_string)  # noqa: PLE1205
        self.event_counter = 0
        self.expire_minutes = expire_minutes  # saved for later renewal, we will use the same interval

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

        # Expires
        if expire_minutes is not None:
            subscribe_request.Expires = expire_minutes * 60

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

                self.expire_at = time.time() + self.subscribe_response.Expires
                self._logger.info('Subscribe was successful: expires at {}, address="{}"',  # noqa: PLE1205
                                  self.expire_at, self.subscribe_response.SubscriptionManager.Address)
            except AttributeError as ex:
                self._logger.error('Subscribe response has unexpected content: {}',  # noqa: PLE1205
                                   message_data.p_msg.raw_data)
                self.is_subscribed = False
                raise SoapResponseError(message_data.p_msg) from ex

        except HTTPReturnCodeError as ex:
            self._logger.error('could not subscribe: %r', ex)

    def renew(self, expire_minutes: int = 60) -> float:
        """Send a Renew request to the provider and handle the response.

        :return: the remaining time of the subscription or 0, if the request was not successful
        """
        if not self.is_subscribed:
            return 0.0
        renew = evt_types.Renew()
        renew.Expires = expire_minutes * 60
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
            expire_seconds = renew_response.Expires
            if expire_seconds is not None:
                self.expire_at = time.time() + expire_seconds
                return expire_seconds
            self.is_subscribed = False
            self._logger.warning('renew failed: {}',  # noqa: PLE1205
                              etree_.tostring(message_data.p_msg.body_node, pretty_print=True))
        return 0.0

    def unsubscribe(self):
        """Send an unsubscribe request to the provider and handle the response."""
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
        else:
            self._logger.error(  # noqa: PLE1205
                'unsubscribe: unexpected response action: {}', received_message_data.p_msg.raw_data)
            raise ValueError(f'unsubscribe: unexpected response action: {received_message_data.p_msg.raw_data}')

    def get_status(self) -> float:
        """Send a GetStatus Request to the device.

        :return: the remaining time of the subscription or 0, if the request was not successful
        """
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

    def check_status(self, renew_limit: int | float, time_diff_warn_limit: int | float = 10):
        """Call Get_status and update internal data.

        :param renew_limit: a value in seconds. If remaining duration of subscription is less than this value,
                            it renews the subscription.
        :param time_diff_warn_limit: if own calculated remaining time differs more from GetStatus result,
                 write a warning to log and correct own value.
        :return: None
        """
        if not self.is_subscribed:
            return

        remaining_time = self.get_status()
        if remaining_time is None:
            self.is_subscribed = False
            return
        if abs(remaining_time - self.remaining_subscription_seconds) > time_diff_warn_limit:
            self._logger.warning(
                'time delta between expected expire and reported expire  > 10 seconds. Will correct own expectation.')
            self.expire_at = time.time() + remaining_time

        if self.remaining_subscription_seconds < renew_limit:
            self._logger.info('renewing subscription')
            self.renew()

    def check_status_renew(self):
        """Call renew and update internal data.

        :return: None
        """
        if self.is_subscribed:
            self.renew()

    @property
    def remaining_subscription_seconds(self) -> float:
        """Return remaining time."""
        return self.expire_at - time.time()

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

    all_subscriptions_okay: bool

    def __init__(self, msg_reader: MessageReader,  # noqa: PLR0913
                 msg_factory: MessageFactory,
                 data_model: AbstractDataModel,
                 get_soap_client_func: Callable[[str], SoapClientProtocol],
                 notification_url: str,
                 end_to_url: str | None = None,
                 check_interval: int | None = None,
                 log_prefix: str = ''):
        """Construct an instance."""

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
                                  ConsumerSubscriptionManagerProtocol): # derive from protocol to help typing.
    """Factory for Subscription objects.

    The thread periodically renews subscriptions. This tells the provider to keep the connection alive.
    Background info: providers can close a socket after a certain time without traffic.
    Periodic requests avoid the timeout

    This Implementation uses unique paths for notifications and SubscriptionEnd messages for each subscription.
    When the provider sends one of these messages, the unique url is enough to identify the corresponding
    subscription instance of the consumer. Reference parameters are not used.
    """

    all_subscriptions_okay: bool = properties.ObservableProperty(True)  # a boolean
    keep_alive_with_renew = True  # enable as workaround if check status is not supported

    def __init__(self, msg_reader: MessageReader,  # noqa: PLR0913
                 msg_factory: MessageFactory,
                 data_model: AbstractDataModel,
                 get_soap_client_func: Callable[[str], SoapClientProtocol],
                 notification_url: str,
                 end_to_url: str | None = None,
                 check_interval: int | None = None,
                 log_prefix: str = ''):
        """Construct a ConsumerSubscriptionManager.

        :param msg_factory:
        :param notification_url:
        :param end_to_url:
        :param check_interval:
        :param log_prefix:
        """
        super().__init__(name=f'SubscriptionClient{log_prefix}')
        self.daemon = True
        self._msg_reader = msg_reader
        self._msg_factory = msg_factory
        self._data_model = data_model
        self._get_soap_client_func = get_soap_client_func
        self._check_interval = check_interval or SUBSCRIPTION_CHECK_INTERVAL
        self.subscriptions = {}
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
            while self._run:
                try:
                    for _ in range(self._check_interval):
                        time.sleep(1)
                        if not self._run:
                            return
                        # check if all subscriptions are okay
                        with self._subscriptions_lock:
                            not_okay = [s for s in self.subscriptions.values() if not s.is_subscribed]
                            self.all_subscriptions_okay = (len(not_okay) == 0)
                    with self._subscriptions_lock:
                        subscriptions = list(self.subscriptions.values())
                    for subscription in subscriptions:
                        if self.keep_alive_with_renew:
                            subscription.check_status_renew()
                        else:
                            subscription.check_status(renew_limit=self._check_interval * 5)
                    self._logger.debug('##### SubscriptionManager Interval ######')
                    for subscription in subscriptions:
                        self._logger.debug('{}', subscription)  # noqa: PLE1205
                except Exception:  # noqa: BLE001
                    # catch all in order to keep thread running.
                    self._logger.error('##### check loop: {}', traceback.format_exc())  # noqa: PLE1205
        finally:
            self._logger.info('terminating subscriptions check loop! self._run={}', self._run)  # noqa: PLE1205

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
