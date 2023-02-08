import http.client
import threading
import time
import traceback
import uuid
from typing import Optional, Union
from urllib.parse import urlparse, urlunparse, ParseResult

from lxml import etree as etree_

from sdc11073.pysoap.soapclient import HTTPReturnCodeError
from sdc11073.pysoap.soapenvelope import SoapResponseException
from .request_handler_deferred import EmptyResponse
from .. import etc
from .. import loghelper
from .. import observableproperties as properties
from ..namespaces import EventingActions
from ..namespaces import default_ns_helper as ns_hlp

SUBSCRIPTION_CHECK_INTERVAL = 5  # seconds


class ClSubscription:
    """ This class handles a subscription to an event source.
    It stores all key data of the subscription and can renew and unsubscribe this subscription."""
    notification_msg = properties.ObservableProperty()
    notification_data = properties.ObservableProperty()
    IDENT_TAG = etree_.QName('http.local.com', 'MyClIdentifier')

    def __init__(self, msg_factory, get_soap_client_func, dpws_hosted, actions, notification_url, end_to_url, log_prefix):
        """ A single Subscription
        """
        self._msg_factory = msg_factory
        self._get_soap_client_func = get_soap_client_func
        self._dpws_hosted = dpws_hosted
        self._actions = actions
        self._filter = ' '.join(actions)
        self.is_subscribed = False
        self.end_status = None  # if device sent a SubscriptionEnd message, this contains the status from the message
        self.end_reason = None  # if device sent a SubscriptionEnd message, this contains the reason from the message
        self.expire_at = None
        self.expire_minutes = None
        self.dev_reference_param = None

        self.notification_url = notification_url
        self.notify_to_identifier = None

        self.end_to_url = end_to_url
        self.end_to_identifier = None

        self._logger = loghelper.get_logger_adapter('sdc.client.subscr', log_prefix)
        self.event_counter = 0  # for display purpose, we count notifications
        # ToDo: check if there is more than one address. In that case a clever selection is needed
        self._hosted_service_address: str = self._dpws_hosted.endpoint_references[0].address
        self._hosted_service_path = urlparse(self._hosted_service_address).path
        self._subscription_manager_address: Optional[str] = None # manager address for this subscription, not known yet
        self._subscription_manager_path: Optional[str] = None

    def _handle_subscribe_response(self, subscribe_result):
        # Check content of response; raise Error if subscription was not successful
        self.dev_reference_param = subscribe_result.reference_param
        self.expire_at = time.time() + subscribe_result.expire_seconds
        self._subscription_manager_address = subscribe_result.subscription_manager_address
        self._subscription_manager_path = urlparse(self._subscription_manager_address).path
        self.is_subscribed = True
        self._logger.info('Subscribe was successful: expires at {}, address="{}"',
                          self.expire_at, self._subscription_manager_address)

    def subscribe(self, expire_minutes: int = 60,
                  any_elements: Optional[list] = None,
                  any_attributes: Optional[dict] = None):
        self._logger.info('### startSubscription "{}" ###', self._filter)
        self.event_counter = 0
        self.expire_minutes = expire_minutes  # saved for later renewal, we will use the same interval
        message = self._msg_factory.mk_subscribe_message(
            self._hosted_service_address, self.notification_url, self.notify_to_identifier,
            self.end_to_url, self.end_to_identifier, expire_minutes, self._filter,
            any_elements, any_attributes)

        msg = f'subscribe {self._filter}'
        try:
            soap_client = self._get_soap_client_func(self._hosted_service_address)
            message_data = soap_client.post_message_to(self._hosted_service_path, message, msg=msg)
            try:
                response_data = message_data.msg_reader.read_subscribe_response(message_data)
            except AttributeError as ex:
                self._logger.error('Subscribe response has unexpected content: {}',
                                   message_data.p_msg.raw_data)
                self.is_subscribed = False
                raise SoapResponseException(message_data.p_msg) from ex
            self._handle_subscribe_response(response_data)
        except HTTPReturnCodeError:
            self._logger.error(f'could not subscribe: {HTTPReturnCodeError}')

    def _mk_renew_message(self, expire_minutes):
        return self._msg_factory.mk_renew_message(
            self._subscription_manager_address,
            dev_reference_param=self.dev_reference_param, expire_minutes=expire_minutes)

    def renew(self, expire_minutes: int = 60) -> float:
        if not self.is_subscribed:
            return 0
        message = self._mk_renew_message(expire_minutes)
        try:
            soap_client = self._get_soap_client_func(self._subscription_manager_address)
            message_data = soap_client.post_message_to(
                self._subscription_manager_path, message, msg='renew')
            self._logger.debug('{}', message_data.p_msg.raw_data)
        except HTTPReturnCodeError as ex:
            self.is_subscribed = False
            self._logger.error('could not renew: {}', ex)
        except (http.client.HTTPException, ConnectionError) as ex:
            self._logger.warn('renew failed: {}', ex)
            self.is_subscribed = False
        except Exception as ex:
            self._logger.error('Exception in renew: {}', ex)
            self.is_subscribed = False
        else:
            expire_seconds = message_data.msg_reader.read_renew_response(message_data)
            if expire_seconds is not None:
                self.expire_at = time.time() + expire_seconds
                return expire_seconds
            self.is_subscribed = False
            self._logger.warn('renew failed: {}',
                              etree_.tostring(message_data.p_msg.body_node, pretty_print=True))
        return 0

    def unsubscribe(self):
        if not self.is_subscribed:
            return
        message = self._msg_factory.mk_unsubscribe_message(
            self._subscription_manager_address,
            dev_reference_param=self.dev_reference_param)

        soap_client = self._get_soap_client_func(self._subscription_manager_address)
        received_message_data = soap_client.post_message_to(self._subscription_manager_path,
                                                                             message, msg='unsubscribe')
        response_action = received_message_data.action
        # check response: response does not contain explicit status. If action== UnsubscribeResponse all is fine.
        if response_action == EventingActions.UnsubscribeResponse:
            self._logger.info('unsubscribe: end of subscription {} was confirmed.', self._filter)
        else:
            self._logger.error('unsubscribe: unexpected response action: {}', received_message_data.p_msg.raw_data)
            raise ValueError(f'unsubscribe: unexpected response action: {received_message_data.p_msg.raw_data}')

    def _mk_get_status_message(self):
        return self._msg_factory.mk_get_status_message(
            self._subscription_manager_address,
            dev_reference_param=self.dev_reference_param)

    def get_status(self) -> float:
        """ Sends a GetStatus Request to the device.
        @return: the remaining time of the subscription or None, if the request was not successful
        """
        message = self._mk_get_status_message()
        try:
            soap_client = self._get_soap_client_func(self._subscription_manager_address)
            message_data = soap_client.post_message_to(
                self._subscription_manager_path,
                message, msg='get_status')
        except HTTPReturnCodeError as ex:
            self.is_subscribed = False
            self._logger.error('could not get status: {}', ex)
        except (http.client.HTTPException, ConnectionError) as ex:
            self.is_subscribed = False
            self._logger.warn('get_status: Connection Error {} for subscription {}', ex, self._filter)
        except Exception as ex:
            self._logger.error('Exception in get_status: {}', ex)
            self.is_subscribed = False
        else:
            expire_seconds = message_data.msg_reader.read_get_status_response(message_data)
            if expire_seconds is None:
                self._logger.warn('get_status for {}: Could not find "Expires" node! get_status={} ', self._filter,
                                  message_data.p_msg.raw_data)
                raise SoapResponseException(message_data.p_msg)
            self._logger.debug('get_status for {}: Expires in {} seconds, counter = {}',
                               self._filter, expire_seconds, self.event_counter)
            return expire_seconds
        return 0.0

    def check_status(self, renew_limit: Union[int, float]):
        """ Calls get_status and updates internal data.
        :param renew_limit: a value in seconds. If remaining duration of subscription is less than this value, it renews the subscription.
        @return: None
        """
        if not self.is_subscribed:
            return

        remaining_time = self.get_status()
        if remaining_time is None:
            self.is_subscribed = False
            return
        if abs(remaining_time - self.remaining_subscription_seconds) > 10:
            self._logger.warn(
                'time delta between expected expire and reported expire  > 10 seconds. Will correct own expectation.')
            self.expire_at = time.time() + remaining_time

        if self.remaining_subscription_seconds < renew_limit:
            self._logger.info('renewing subscription')
            self.renew()

    def check_status_renew(self):
        """ Calls renew and updates internal data.
        @return: None
        """
        if self.is_subscribed:
            self.renew()

    @property
    def remaining_subscription_seconds(self):
        return self.expire_at - time.time()

    def on_notification(self, request_data):
        self.event_counter += 1
        self._logger.debug('received notification {} version= {}',
                           request_data.message_data.action, request_data.message_data.mdib_version_group)
        self.notification_data = request_data.message_data
        self.notification_msg = request_data.message_data.p_msg
        return EmptyResponse()

    @property
    def short_filter_string(self):
        return etc.short_filter_string(self._actions)

    def __str__(self):
        return f'Subscription of "{self.short_filter_string}", is_subscribed={self.is_subscribed}, ' \
               f'remaining time = {int(self.remaining_subscription_seconds)} sec., count={self.event_counter}'


class ClientSubscriptionManager(threading.Thread):
    """
     Factory for Subscription objects. It automatically renews expiring subscriptions.
    """
    all_subscriptions_okay = properties.ObservableProperty(True)  # a boolean
    keep_alive_with_renew = True  # enable as workaround if checkstatus is not supported

    def __init__(self, msg_reader, msg_factory, get_soap_client_func, notification_url, end_to_url=None, check_interval=None, log_prefix=''):
        """

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
        self._run = False
        self.join(timeout=2)
        with self._subscriptions_lock:
            self.subscriptions.clear()

    def run(self):
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
                        self._logger.debug('{}', subscription)
                except Exception:
                    self._logger.error('##### check loop: {}', traceback.format_exc())
        finally:
            self._logger.info('terminating subscriptions check loop! self._run={}', self._run)

    def mk_subscription(self, dpws_hosted, filters):
        sep = '' if self._notification_url.endswith('/') else '/'
        notification_url = f'{self._notification_url}{sep}subscr{self._counter}'
        sep = '' if self._end_to_url.endswith('/') else '/'
        end_to_url = f'{self._end_to_url}{sep}subscr{self._counter}_e'
        self._counter += 1
        subscription = ClSubscription(self._msg_factory, self._get_soap_client_func,
                                      dpws_hosted, filters, notification_url,
                                      end_to_url, self.log_prefix)
        filter_ = ' '.join(filters)
        with self._subscriptions_lock:
            self.subscriptions[filter_] = subscription
        return subscription

    def _find_subscription(self, request_data, reference_parameters, log_prefix):
        for subscription in self.subscriptions.values():
            if subscription.end_to_url.endswith(request_data.current):
                return subscription
        self._logger.warn('{}: have no subscription for identifier = {}', log_prefix, request_data.current)
        return None

    def on_subscription_end(self, request_data) -> [ClSubscription, None]:
        subscription_end_result = self._msg_reader.read_subscription_end_message(request_data.message_data)
        self._logger.info('on_subscription_end: received Subscription End {}', subscription_end_result)
        if subscription_end_result.status_list:
            info = f' status={subscription_end_result.status_list[0]} '
        else:
            info = ''
        if subscription_end_result.reason_list:
            if len(subscription_end_result.reason_list) == 1:
                info += f' reason = {subscription_end_result.reason_list[0]}'
            else:
                info += f' reasons = {subscription_end_result.reason_list}'
        subscription = self._find_subscription(request_data,
                                               subscription_end_result.reference_parameter_list,
                                               'on_subscription_end')
        if subscription is not None:
            self._logger.info('on_subscription_end: received Subscription End for {} {}',
                              subscription.short_filter_string,
                              info)
            subscription.is_subscribed = False
            if len(subscription_end_result.status_list) > 0:
                subscription.end_status = subscription_end_result.status_list[0]
            if len(subscription_end_result.reason_list) > 0:
                subscription.end_reason = subscription_end_result.reason_list[0]
            return subscription
        return None

    def unsubscribe_all(self) -> bool:
        ret = True
        with self._subscriptions_lock:
            current_subscriptions = list(self.subscriptions.values())  # make a copy
            self.subscriptions.clear()
            for subscription in current_subscriptions:
                try:
                    subscription.unsubscribe()
                except Exception:
                    self._logger.error('unsubscribe error: {}\n call stack:{} ', traceback.format_exc(),
                                       traceback.format_stack())
                    ret = False
        return ret


class ClientSubscriptionManagerReferenceParams(ClientSubscriptionManager):
    def mk_subscription(self, dpws_hosted, filters):
        subscription = ClSubscription(self._msg_factory, self._get_soap_client_func,
                                      dpws_hosted, filters,
                                      self._notification_url,
                                      self._end_to_url, self.log_prefix)
        subscription.notify_to_identifier = etree_.Element(ClSubscription.IDENT_TAG)
        subscription.notify_to_identifier.text = uuid.uuid4().urn
        subscription.end_to_identifier = etree_.Element(ClSubscription.IDENT_TAG)
        subscription.end_to_identifier.text = uuid.uuid4().urn

        filter_ = ' '.join(filters)
        with self._subscriptions_lock:
            self.subscriptions[filter_] = subscription
        return subscription

    def _find_subscription(self, request_data, reference_parameters, log_prefix):
        subscr_ident_list = request_data.message_data.p_msg.header_node.findall(ClSubscription.IDENT_TAG,
                                                                                namespaces=ns_hlp.ns_map)
        if not subscr_ident_list:
            return None
        subscr_ident = subscr_ident_list[0]
        for subscription in self.subscriptions.values():
            if subscr_ident.text == subscription.end_to_identifier.text:
                return subscription
        self._logger.warn('{}}: have no subscription for identifier = {}', log_prefix, subscr_ident.text)
        return None
