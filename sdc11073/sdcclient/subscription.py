import copy
import http.client
import threading
import time
import traceback
import urllib
import uuid

from lxml import etree as etree_

from sdc11073.pysoap.soapclient import HTTPReturnCodeError
from sdc11073.pysoap.soapenvelope import SoapResponseException
from .. import etc, isoduration
from .. import loghelper
from .. import observableproperties as properties
from ..namespaces import nsmap as _global_nsmap
from ..namespaces import wseTag, wsaTag

SUBSCRIPTION_CHECK_INTERVAL = 5  # seconds


class _ClSubscription:
    """ This class handles a subscription to an event source.
    It stores all key data of the subscription and can renew and unsubscribe this subscription."""
    notification = properties.ObservableProperty()
    IDENT_TAG = etree_.QName('http.local.com', 'MyClIdentifier')

    def __init__(self, msg_factory, dpws_hosted, actions, notification_url, end_to_url, log_prefix):
        """
        :param serviceClient:
        :param filter_:
        :param notification_url: e.g. http://1.2.3.4:9999, or https://1.2.3.4:9999
        """
        self._msg_factory = msg_factory
        self.dpws_hosted = dpws_hosted
        self._actions = actions
        self._filter = ' '.join(actions)
        self.is_subscribed = False
        self.expire_at = None
        self.expire_minutes = None
        self.dev_reference_param = None

        self.notification_url = notification_url
        self.notify_to_identifier = None
        # self.notify_to_identifier = etree_.Element(self.IDENT_TAG)
        # self.notify_to_identifier.text = uuid.uuid4().urn

        self.end_to_url = end_to_url
        self.end_to_identifier = None
        # self.end_to_identifier = etree_.Element(self.IDENT_TAG)
        # self.end_to_identifier.text = uuid.uuid4().urn

        self._subscription_manager_address = None
        self._logger = loghelper.get_logger_adapter('sdc.client.subscr', log_prefix)
        self.event_counter = 0  # for display purpose, we count notifications
        self.cl_ident = log_prefix
        self._device_epr = urllib.parse.urlparse(self.dpws_hosted.endpoint_references[0].address).path

    def _mk_subscribe_envelope(self, subscribe_epr, expire_minutes):
        return self._msg_factory.mk_subscribe_envelope(
            subscribe_epr, self.notification_url, self.notify_to_identifier,
            self.end_to_url, self.end_to_identifier, expire_minutes, self._filter)

    def _handle_subscribe_response(self, envelope):
        # Check content of response; raise Error if subscription was not successful
        try:
            msg_node = envelope.msg_node
            if msg_node.tag == wseTag('SubscribeResponse'):
                address = msg_node.xpath('wse:SubscriptionManager/wsa:Address/text()', namespaces=_global_nsmap)
                self.dev_reference_param = None

                reference_params = msg_node.xpath('wse:SubscriptionManager/wsa:ReferenceParameters',
                                                  namespaces=_global_nsmap)
                if reference_params:
                    self.dev_reference_param = reference_params[0]
                expires = msg_node.xpath('wse:Expires/text()', namespaces=_global_nsmap)

                self._subscription_manager_address = urllib.parse.urlparse(address[0])
                expire_seconds = isoduration.parse_duration(expires[0])
                self.expire_at = time.time() + expire_seconds
                self.is_subscribed = True
                self._logger.info('Subscribe was successful: expires at {}, address="{}"',
                                  self.expire_at, self._subscription_manager_address)
            else:
                # This is a failure response or even rubbish. log it and raise error
                self._logger.error('Subscribe response has unexpected content: {}', envelope.as_xml(pretty=True))
                self.is_subscribed = False
                raise SoapResponseException(envelope)
        except AttributeError:
            self._logger.error('Subscribe response has unexpected content: {}', envelope.as_xml(pretty=True))
            self.is_subscribed = False
            raise SoapResponseException(envelope)

    def subscribe(self, expire_minutes=60):
        self._logger.info('### startSubscription "{}" ###', self._filter)
        self.event_counter = 0
        self.expire_minutes = expire_minutes  # saved for later renewal, we will use the same interval
        # ToDo: check if there is more than one address. In that case a clever selection is needed
        address = self.dpws_hosted.endpoint_references[0].address
        envelope = self._mk_subscribe_envelope(address, expire_minutes)
        msg = 'subscribe {}'.format(self._filter)
        try:
            result_envelope = self.dpws_hosted.soap_client.post_soap_envelope_to(self._device_epr, envelope,
                                                                                 msg=msg)
            self._handle_subscribe_response(result_envelope)
        except HTTPReturnCodeError:
            self._logger.error('could not subscribe: {}'.format(HTTPReturnCodeError))

    def _add_device_references(self, envelope):
        """ add references for requests to device (renew, getstatus, unsubscribe)"""
        if self.dev_reference_param is not None:
            for element in self.dev_reference_param:
                element_ = copy.copy(element)
                # mandatory attribute acc. to ws_addressing SOAP Binding (https://www.w3.org/TR/2006/REC-ws-addr-soap-20060509/)
                element_.set(wsaTag('IsReferenceParameter'), 'true')
                envelope.add_header_element(element_)

    def _mk_renew_envelope(self, expire_minutes):
        return self._msg_factory.mk_renew_envelope(
            urllib.parse.urlunparse(self._subscription_manager_address),
            dev_reference_param=self.dev_reference_param, expire_minutes=expire_minutes)

    def _handle_renew_response(self, envelope):
        # Check content of response; raise Error if subscription was not successful
        body_node = envelope.body_node
        renew_response = body_node.xpath('wse:RenewResponse', namespaces=_global_nsmap)
        if len(renew_response) == 1:
            # this means renew was accepted
            expires = body_node.xpath('wse:RenewResponse/wse:Expires/text()', namespaces=_global_nsmap)
            expire_seconds = isoduration.parse_duration(expires[0])
            self.expire_at = time.time() + expire_seconds
        else:
            raise SoapResponseException(envelope)

    def renew(self, expire_minutes=60):
        envelope = self._mk_renew_envelope(expire_minutes)
        try:
            result_envelope = self.dpws_hosted.soap_client.post_soap_envelope_to(
                self._subscription_manager_address.path, envelope, msg='renew')
            self._logger.debug('{}', result_envelope.as_xml(pretty=True))
        except HTTPReturnCodeError as ex:
            self.is_subscribed = False
            self._logger.error('could not renew: {}'.format(HTTPReturnCodeError))
        except (http.client.HTTPException, ConnectionError) as ex:
            self._logger.warn('renew failed: {}', ex)
            self.is_subscribed = False
        except Exception as ex:
            self._logger.error('Exception in renew: {}', ex)
            self.is_subscribed = False
        else:
            try:
                self._handle_renew_response(result_envelope)
                return self.remaining_subscription_seconds
            except SoapResponseException as ex:
                self.is_subscribed = False
                self._logger.warn('renew failed: {}',
                                  etree_.tostring(ex.response_envelope.body_node, pretty_print=True))

    def unsubscribe(self):
        if not self.is_subscribed:
            return
        soap_envelope = self._msg_factory.mk_unsubscribe_envelope(
            urllib.parse.urlunparse(self._subscription_manager_address),
            dev_reference_param=self.dev_reference_param)

        result_envelope = self.dpws_hosted.soap_client.post_soap_envelope_to(self._subscription_manager_address.path,
                                                                             soap_envelope, msg='unsubscribe')
        response_action = result_envelope.address.action
        # check response: response does not contain explicit status. If action== UnsubscribeResponse all is fine.
        if response_action == 'http://schemas.xmlsoap.org/ws/2004/08/eventing/UnsubscribeResponse':
            self._logger.info('unsubscribe: end of subscription {} was confirmed.', self._filter)
        else:
            self._logger.error('unsubscribe: unexpected response action: {}', result_envelope.as_xml(pretty=True))
            raise RuntimeError(
                'unsubscribe: unexpected response action: {}'.format(result_envelope.as_xml(pretty=True)))

    def _mk_get_status_envelope(self):
        return self._msg_factory.mk_getstatus_envelope(
            urllib.parse.urlunparse(self._subscription_manager_address),
            dev_reference_param=self.dev_reference_param)

    def get_status(self):
        """ Sends a GetStatus Request to the device.
        @return: the remaining time of the subscription or None, if the request was not successful
        """
        envelope = self._mk_get_status_envelope()
        try:
            result_envelope = self.dpws_hosted.soap_client.post_soap_envelope_to(
                self._subscription_manager_address.path,
                envelope, msg='get_status')
        except HTTPReturnCodeError as ex:
            self.is_subscribed = False
            self._logger.error('could not get status: {}'.format(HTTPReturnCodeError))
        except (http.client.HTTPException, ConnectionError) as ex:
            self.is_subscribed = False
            self._logger.warn('get_status: Connection Error {} for subscription {}', ex, self._filter)
        except Exception as ex:
            self._logger.error('Exception in get_status: {}', ex)
            self.is_subscribed = False
        else:
            try:
                expires_node = result_envelope.msg_node.find('wse:Expires', namespaces=_global_nsmap)
                if expires_node is None:
                    self._logger.warn('get_status for {}: Could not find "Expires" node! get_status={} ', self._filter,
                                      result_envelope.rawdata)
                    raise SoapResponseException(result_envelope)
                expires = expires_node.text
                expires_value = isoduration.parse_duration(expires)
                self._logger.debug('get_status for {}: Expires = {} = {} seconds, counter = {}', self._filter,
                                   expires,
                                   expires_value,
                                   self.event_counter)
                return expires_value
            except AttributeError:
                self._logger.warn('No msg in envelope')

    def check_status(self, renew_limit):
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
        self.notification = request_data.envelope

    @property
    def short_filter_string(self):
        return etc.short_filter_string(self._actions)

    def __str__(self):
        return 'Subscription of "{}", is_subscribed={}, remaining time = {} sec., count={}'.format(
            self.short_filter_string,
            self.is_subscribed,
            int(self.remaining_subscription_seconds),
            self.event_counter)


class ClientSubscriptionManager(threading.Thread):
    """
     Factory for Subscription objects. It automatically renews expiring subscriptions.
    """
    all_subscriptions_okay = properties.ObservableProperty(True)  # a boolean
    keep_alive_with_renew = True  # enable as workaround if checkstatus is not supported

    def __init__(self, msg_factory, notification_url, end_to_url=None, check_interval=None, log_prefix=''):
        """

        :param msg_factory:
        :param notification_url:
        :param end_to_url:
        :param check_interval:
        :param log_prefix:
        """
        super().__init__(name='SubscriptionClient{}'.format(log_prefix))
        self.daemon = True
        self._msg_factory = msg_factory
        self._check_interval = check_interval or SUBSCRIPTION_CHECK_INTERVAL
        self.subscriptions = {}
        self._subscriptions_lock = threading.Lock()

        self._run = False
        self._notification_url = notification_url
        self._end_to_url = end_to_url or notification_url
        self._logger = loghelper.get_logger_adapter('sdc.client.subscrMgr', log_prefix)
        self.log_prefix = log_prefix

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
        notification_url = f'{self._notification_url}{uuid.uuid4().hex}'
        end_to_url = f'{self._end_to_url}{uuid.uuid4().hex}'
        subscription = _ClSubscription(self._msg_factory, dpws_hosted, filters, notification_url,
                                       end_to_url, self.log_prefix)
        filter_ = ' '.join(filters)
        with self._subscriptions_lock:
            self.subscriptions[filter_] = subscription
        return subscription

    def _find_subscription(self, request_data, log_prefix):
        for subscription in self.subscriptions.values():
            if subscription.end_to_url.endswith(request_data.current):
                return subscription
        self._logger.warn('{}: have no subscription for identifier = {}', log_prefix, request_data.current)
        return None

    def on_subscription_end(self, request_data):
        envelope = request_data.envelope
        statuus = envelope.body_node.xpath('wse:SubscriptionEnd/wse:Status/text()', namespaces=_global_nsmap)
        reasons = envelope.body_node.xpath('wse:SubscriptionEnd/wse:Reason/text()', namespaces=_global_nsmap)
        if statuus:
            info = ' status={} '.format(statuus[0])
        else:
            info = ''
        if reasons:
            if len(reasons) == 1:
                info += ' reason = {}'.format(reasons[0])
            else:
                info += ' reasons = {}'.format(reasons)
        subscription = self._find_subscription(request_data, 'on_subscription_end')
        if subscription is not None:
            self._logger.info('on_subscription_end: received Subscription End for {} {}',
                              subscription.short_filter_string,
                              info)
            subscription.is_subscribed = False

    def unsubscribe_all(self):
        with self._subscriptions_lock:
            current_subscriptions = list(self.subscriptions.values())  # make a copy
            self.subscriptions.clear()
            for subscription in current_subscriptions:
                try:
                    subscription.unsubscribe()
                except Exception:
                    self._logger.warn('unsubscribe error: {}\n call stack:{} ', traceback.format_exc(),
                                      traceback.format_stack())


class ClientSubscriptionManagerReferenceParams(ClientSubscriptionManager):
    def mk_subscription(self, dpws_hosted, filters):
        subscription = _ClSubscription(self._msg_factory, dpws_hosted, filters, self._notification_url,
                                       self._end_to_url, self.log_prefix)
        subscription.notify_to_identifier = etree_.Element(_ClSubscription.IDENT_TAG)
        subscription.notify_to_identifier.text = uuid.uuid4().urn
        subscription.end_to_identifier = etree_.Element(_ClSubscription.IDENT_TAG)
        subscription.end_to_identifier.text = uuid.uuid4().urn

        filter_ = ' '.join(filters)
        with self._subscriptions_lock:
            self.subscriptions[filter_] = subscription
        return subscription

    def _find_subscription(self, request_data, log_prefix):
        subscr_ident_list = request_data.envelope.header_node.findall(_ClSubscription.IDENT_TAG,
                                                                      namespaces=_global_nsmap)
        if not subscr_ident_list:
            return None
        subscr_ident = subscr_ident_list[0]
        for subscription in self.subscriptions.values():
            if subscr_ident.text == subscription.end_to_identifier.text:
                return subscription
        self._logger.warn('{}}: have no subscription for identifier = {}', log_prefix, subscr_ident.text)
        return None
