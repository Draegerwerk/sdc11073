import copy
import http.client
import socket
import time
import traceback
import urllib
import uuid
from collections import deque, defaultdict

from lxml import etree as etree_

from .. import isoduration
from .. import loghelper
from .. import multikey
from .. import observableproperties
from ..compression import CompressionHandler
from ..etc import apply_map, short_filter_string
from ..namespaces import Prefixes
from ..namespaces import xmlTag, wseTag, wsaTag, msgTag, nsmap, DocNamespaceHelper
from ..pysoap.soapclient import SoapClient, HTTPReturnCodeError
from ..pysoap.soapenvelope import Soap12Envelope, SoapFault, WsAddress, WsaEndpointReferenceType

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
        return 'min={:.4f} max={:.4f} avg={:.4f} absmax={:.4f}'.format(self.min, self.max, self.avg, self.abs_max)


class _DevSubscription:
    MAX_NOTIFY_ERRORS = 1
    IDENT_TAG = etree_.QName('http.local.com', 'MyDevIdentifier')

    def __init__(self, mode, base_urls, notify_to_address, notify_ref_node, end_to_address, end_to_ref_node, expires,
                 max_subscription_duration, filter_, ssl_context, biceps_schema,
                 accepted_encodings):  # pylint:disable=too-many-arguments
        """
        :param notify_to_address: dom node of Subscribe Request
        :param end_to_address: dom node of Subscribe Request
        :param expires: seconds as float
        :param filter: a space separated list of actions, or only one action
        """
        self.mode = mode
        self.base_urls = base_urls
        self.notify_to_address = notify_to_address
        self._url = urllib.parse.urlparse(notify_to_address)

        self.notify_ref_nodes = []
        if notify_ref_node is not None:
            self.notify_ref_nodes = list(notify_ref_node)  # all children

        self.end_to_address = end_to_address
        self.end_to_ref_nodes = []
        if end_to_ref_node is not None:
            self.end_to_ref_nodes = list(end_to_ref_node)  # all children
        self.my_identifier = etree_.Element(self.IDENT_TAG)
        self.my_identifier.text = uuid.uuid4().urn

        self._max_subscription_duration = max_subscription_duration
        self._started = None
        self._expireseconds = None
        self.renew(expires)  # sets self._started and self._expireseconds
        self._filters = filter_.split()
        self._ssl_context = ssl_context
        self._biceps_schema = biceps_schema

        self._accepted_encodings = accepted_encodings  # these encodings does the other side accept
        self._soap_client = None

        self._notify_errors = 0
        self._is_closed = False
        self._is_connection_error = False
        self.last_roundtrip_times = deque(
            maxlen=MAX_ROUNDTRIP_VALUES)  # a list of last n roundtrip times for notifications
        self.max_roundtrip_time = 0

    def set_soap_client(self, soap_client):
        self._soap_client = soap_client

    def renew(self, expires):
        self._started = time.monotonic()
        if expires:
            self._expireseconds = min(expires, self._max_subscription_duration)
        else:
            self._expireseconds = self._max_subscription_duration

    @property
    def soap_client(self):
        return self._soap_client

    @property
    def remaining_seconds(self):
        duration = int(self._expireseconds - (time.monotonic() - self._started))
        return 0 if duration < 0 else duration

    @property
    def expire_string(self):
        return isoduration.duration_string(self.remaining_seconds)

    @property
    def has_delivery_failure(self):
        return self._notify_errors >= self.MAX_NOTIFY_ERRORS

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
        for filter_string in self._filters:
            if filter_string.endswith(action):
                return True
        return False

    def _mk_end_report(self, envelope, action):
        to_addr = self.end_to_address or self.notify_to_address
        addr = WsAddress(addr_to=to_addr,
                         action=action,
                         addr_from=None,
                         reply_to=None,
                         fault_to=None,
                         reference_parameters_node=None)
        envelope.set_address(addr)
        ref_nodes = self.end_to_ref_nodes or self.notify_ref_nodes
        for ident_node in ref_nodes:
            ident_node_ = copy.copy(ident_node)
            # mandatory attribute acc. to ws_addressing SOAP Binding (https://www.w3.org/TR/2006/REC-ws-addr-soap-20060509/)
            ident_node_.set(wsaTag('IsReferenceParameter'), 'true')
            envelope.add_header_element(ident_node_)
        return envelope

    def send_notification_report(self, msg_factory, body_node, action, doc_nsmap):
        if not self.is_valid:
            return
        addr = WsAddress(addr_to=self.notify_to_address,
                         action=action,
                         addr_from=None,
                         reply_to=None,
                         fault_to=None,
                         reference_parameters_node=None)
        soap_envelope = msg_factory.mk_notification_report(addr, body_node, self.notify_ref_nodes, doc_nsmap)
        try:
            roundtrip_timer = observableproperties.SingleValueCollector(self._soap_client, 'roundtrip_time')

            self._soap_client.post_soap_envelope_to(self._url.path, soap_envelope, response_factory=None,
                                                    msg='send_notification_report {}'.format(action))
            try:
                roundtrip_time = roundtrip_timer.result(0)
                self.last_roundtrip_times.append(roundtrip_time)
                self.max_roundtrip_time = max(self.max_roundtrip_time, roundtrip_time)
            except observableproperties.CollectTimeoutError:
                pass
            self._notify_errors = 0
            self._is_connection_error = False
        except HTTPReturnCodeError:
            self._notify_errors += 1
            raise
        except Exception:  # any other exception is handled as an unreachable location (disconnected)
            self._notify_errors += 1
            self._is_connection_error = True
            raise

    def send_notification_end_message(self, action, code='SourceShuttingDown', reason='Event source going off line.'):
        doc_nsmap = DocNamespaceHelper().doc_ns_map
        my_addr = '{}:{}/{}'.format(self.base_urls[0].scheme, self.base_urls[0].netloc, self.base_urls[0].path)

        if not self.is_valid:
            return
        if self._soap_client is None:
            return
        envelope = Soap12Envelope(doc_nsmap)

        subscription_end_node = etree_.Element(wseTag('SubscriptionEnd'),
                                               nsmap=Prefixes.partial_map(Prefixes.WSE, Prefixes.WSA, Prefixes.XML))
        subscription_manager_node = etree_.SubElement(subscription_end_node, wseTag('SubscriptionManager'))
        # child of Subscriptionmanager is the endpoint reference of the subscription manager (wsa:EndpointReferenceType)
        reference_parameters_node = etree_.Element(wsaTag('ReferenceParameters'))
        reference_parameters_node.append(copy.copy(self.my_identifier))
        epr = WsaEndpointReferenceType(address=my_addr, reference_parameters_node=reference_parameters_node)
        epr.as_etree_subnode(subscription_manager_node)

        # remark: optionally one could add own address and identifier here ...
        status_node = etree_.SubElement(subscription_end_node, wseTag('Status'))
        status_node.text = 'wse:{}'.format(code)
        reason_node = etree_.SubElement(subscription_end_node, wseTag('Reason'),
                                        attrib={xmlTag('lang'): 'en-US'})
        reason_node.text = reason

        envelope.add_body_element(subscription_end_node)
        rep = self._mk_end_report(envelope, action)
        try:
            self._soap_client.post_soap_envelope_to(self._url.path, rep, response_factory=lambda x, schema: x,
                                                    msg='send_notification_end_message {}'.format(action))
            self._notify_errors = 0
            self._is_connection_error = False
            self._is_closed = True
        except Exception:
            # it does not matter that we could not send the message - end is end ;)
            pass

    def close(self):
        self._is_closed = True

    def is_closed(self):
        return self._is_closed

    def __repr__(self):
        try:
            ref_ident = ', '.join([node.text for node in self.notify_ref_nodes])
        except TypeError:
            ref_ident = '<unknown>'
        return 'Subscription(notify_to={} idnt={}, my_identifier={}, expires={}, filter={})'.format(
            self.notify_to_address,
            ref_ident,
            self.my_identifier.text,
            self.remaining_seconds,
            short_filter_string(self._filters))

    @classmethod
    def from_soap_envelope(cls, envelope, ssl_context, biceps_schema, accepted_encodings, max_subscription_duration,
                           base_urls):
        end_to_address = None
        end_to_ref_node = []
        end_to_addresses = envelope.body_node.xpath('wse:Subscribe/wse:EndTo', namespaces=nsmap)
        if len(end_to_addresses) == 1:
            end_to_node = end_to_addresses[0]
            end_to_address = end_to_node.xpath('wsa:Address/text()', namespaces=nsmap)[0]
            end_to_ref_node = end_to_node.find('wsa:ReferenceParameters', namespaces=nsmap)

        # determine (mandatory) notification address
        delivery_node = envelope.body_node.xpath('wse:Subscribe/wse:Delivery', namespaces=nsmap)[0]
        notify_to_node = delivery_node.find('wse:NotifyTo', namespaces=nsmap)
        notify_to_address = notify_to_node.xpath('wsa:Address/text()', namespaces=nsmap)[0]
        notify_ref_node = notify_to_node.find('wsa:ReferenceParameters', namespaces=nsmap)

        mode = delivery_node.get('Mode')  # mandatory attribute

        expires_nodes = envelope.body_node.xpath('wse:Subscribe/wse:Expires/text()', namespaces=nsmap)
        if len(expires_nodes) == 0:
            expires = None
        else:
            expires = isoduration.parse_duration(str(expires_nodes[0]))

        filter_ = envelope.body_node.xpath('wse:Subscribe/wse:Filter/text()', namespaces=nsmap)[0]

        return cls(str(mode), base_urls, notify_to_address, notify_ref_node, end_to_address, end_to_ref_node,
                   expires, max_subscription_duration, str(filter_), ssl_context, biceps_schema, accepted_encodings)

    def get_roundtrip_stats(self):
        if len(self.last_roundtrip_times) > 0:
            return _RoundTripData(self.last_roundtrip_times, self.max_roundtrip_time)
        return _RoundTripData(None, None)

    def short_filter_names(self):
        return tuple([f.split('/')[-1] for f in self._filters])


class SubscriptionsManager:
    BodyNodePrefixes = [Prefixes.PM, Prefixes.MSG, Prefixes.XSI, Prefixes.EXT, Prefixes.XML]
    NotificationPrefixes = [Prefixes.PM, Prefixes.S12, Prefixes.WSA, Prefixes.WSE]
    DEFAULT_MAX_SUBSCR_DURATION = 7200  # max. possible duration of a subscription

    def __init__(self, ssl_context, sdc_definitions, biceps_parser, msg_factory, supported_encodings,
                 max_subscription_duration=None, log_prefix=None, chunked_messages=False):
        self._ssl_context = ssl_context
        self.biceps_parser = biceps_parser
        self.sdc_definitions = sdc_definitions
        self._msg_factory = msg_factory
        self.log_prefix = log_prefix
        self._logger = loghelper.get_logger_adapter('sdc.device.subscrMgr', self.log_prefix)
        self._chunked_messages = chunked_messages
        self.soap_clients = {}  # key: net location, value soap_client instance
        self._supported_encodings = supported_encodings
        self._max_subscription_duration = max_subscription_duration or self.DEFAULT_MAX_SUBSCR_DURATION
        self._subscriptions = multikey.MultiKeyLookup()
        self._subscriptions.add_index('identifier', multikey.UIndexDefinition(lambda obj: obj.my_identifier.text))
        self._subscriptions.add_index('netloc', multikey.IndexDefinition(
            lambda obj: obj._url.netloc))  # pylint:disable=protected-access
        self.base_urls = None

    def set_base_urls(self, base_urls):
        self.base_urls = base_urls

    def on_subscribe_request(self, http_header, envelope, epr_path):
        accepted_encodings = CompressionHandler.parse_header(http_header.get('Accept-Encoding'))
        subscr = _DevSubscription.from_soap_envelope(
            envelope, self._ssl_context, self.biceps_parser, accepted_encodings,
            self._max_subscription_duration, self.base_urls)

        # assign a soap client
        key = subscr._url.netloc  # pylint:disable=protected-access
        soap_client = self.soap_clients.get(key)
        if soap_client is None:
            soap_client = SoapClient(key, loghelper.get_logger_adapter('sdc.device.soap', self.log_prefix),
                                     ssl_context=self._ssl_context, sdc_definitions=self.sdc_definitions,
                                     supported_encodings=self._supported_encodings,
                                     request_encodings=accepted_encodings,
                                     chunked_requests=self._chunked_messages)
            self.soap_clients[key] = soap_client
        subscr.set_soap_client(soap_client)
        with self._subscriptions.lock:
            self._subscriptions.add_object(subscr)
        self._logger.info('new {}', subscr)

        response = Soap12Envelope(Prefixes.partial_map(*self.NotificationPrefixes))
        reply_address = envelope.address.mk_reply_address(
            'http://schemas.xmlsoap.org/ws/2004/08/eventing/SubscribeResponse')
        response.add_header_object(reply_address)
        subscribe_response_node = etree_.Element(wseTag('SubscribeResponse'))
        subscription_manager_node = etree_.SubElement(subscribe_response_node, wseTag('SubscriptionManager'))
        # child of Subscriptionmanager is the endpoint reference of the subscription manager (wsa:EndpointReferenceType)
        reference_parameters_node = etree_.Element(wsaTag('ReferenceParameters'))
        reference_parameters_node.append(copy.copy(subscr.my_identifier))
        if epr_path.startswith('/'):
            epr_path = epr_path[1:]
        my_addr = '{}://{}/{}'.format(self.base_urls[0].scheme, self.base_urls[0].netloc, epr_path)
        epr = WsaEndpointReferenceType(address=my_addr, reference_parameters_node=reference_parameters_node)
        epr.as_etree_subnode(subscription_manager_node)
        expires_node = etree_.SubElement(subscribe_response_node, wseTag('Expires'))
        expires_node.text = subscr.expire_string  # simply confirm request
        response.add_body_element(subscribe_response_node)
        self._logger.debug('on_subscribe_request returns {}', lambda: response.as_xml(pretty=False))
        return response

    def on_unsubscribe_request(self, envelope):
        ident = envelope.header_node.find(_DevSubscription.IDENT_TAG, namespaces=nsmap)
        if ident is not None:
            ident_text = ident.text
            subscr = self._subscriptions.identifier.get_one(ident_text, allow_none=True)
            if subscr is None:
                self._logger.warn('unsubscribe: no object found for id={}', ident_text)
            else:
                subscr.close()
                with self._subscriptions.lock:
                    self._subscriptions.remove_object(subscr)
                self._logger.info('unsubscribe: object found and removed (Xaddr = {}, filter = {})',
                                  subscr.notify_to_address,
                                  subscr._filters)  # pylint: disable=protected-access
                # now check if we can close the soap client
                key = subscr._url.netloc  # pylint: disable=protected-access
                subscriptions_with_same_soap_client = self._subscriptions.netloc.get(key, [])
                if len(subscriptions_with_same_soap_client) == 0:
                    self.soap_clients[key].close()
                    del self.soap_clients[key]
                    self._logger.info('unsubscribe: closed soap client to {})', key)
        else:
            self._logger.error('unsubscribe request did not contain an identifier!!!: {}',
                               envelope.as_xml(pretty=True))

        response = Soap12Envelope(nsmap)
        reply_address = envelope.address.mk_reply_address(
            'http://schemas.xmlsoap.org/ws/2004/08/eventing/UnsubscribeResponse')
        response.add_header_object(reply_address)
        # response has empty body
        return response

    def notify_operation(self, operation, transaction_id, invocation_state,
                         nsmapper, sequence_id, mdib_version,
                         error=None, error_message=None):
        operation_handle_ref = operation.handle
        self._logger.info(
            'notify_operation transaction={} operation_handle_ref={}, operationState={}, error={}, errorMessage={}',
            transaction_id, operation_handle_ref, invocation_state, error, error_message)
        action = self.sdc_definitions.Actions.OperationInvokedReport
        subscribers = self._get_subscriptions_for_action(action)

        ns_map = nsmapper.partial_map(Prefixes.MSG, Prefixes.PM)
        body_node = self._msg_factory.mk_operation_invoked_report_body(ns_map, mdib_version, sequence_id,
                                                                       operation_handle_ref, transaction_id,
                                                                       invocation_state, error, error_message)
        # for s in subscribers:
        #     self._logger.info('notify_operation: sending report to {}', s.notify_to_address)
        #     self._send_notification_report(s, body_bode, action, Prefixes.partial_map(*self.NotificationPrefixes))
        self._send_to_subscribers(subscribers, body_node, action, nsmapper, 'notify_operation')
        self._do_housekeeping()

    def on_get_status_request(self, envelope):
        self._logger.debug('on_get_status_request {}', lambda: envelope.as_xml(pretty=True))
        subscr = self._get_subscription_for_request(envelope)
        if subscr is None:
            response = SoapFault(envelope,
                                 code='Receiver',
                                 reason='unknown Subscription identifier',
                                 subCode=wseTag('InvalidMessage')
                                 )

        else:
            response = Soap12Envelope(Prefixes.partial_map(*self.NotificationPrefixes))
            reply_address = envelope.address.mk_reply_address(
                'http://schemas.xmlsoap.org/ws/2004/08/eventing/GetStatusResponse')
            response.add_header_object(reply_address)
            renew_response_node = etree_.Element(wseTag('GetStatusResponse'))
            expires_node = etree_.SubElement(renew_response_node, wseTag('Expires'))
            expires_node.text = subscr.expire_string  # simply confirm request
            response.add_body_element(renew_response_node)
        return response

    def on_renew_request(self, envelope):
        # identifierNode = envelope.header_node.find(_DevSubscription.IDENT_TAG, namespaces=nsmap)
        expires = envelope.body_node.xpath('wse:Renew/wse:Expires/text()', namespaces=nsmap)
        if len(expires) == 0:
            expires = None
            self._logger.debug('on_renew_request: no requested duration found, allowing max.')
        else:
            expires = isoduration.parse_duration(str(expires[0]))
            self._logger.debug('on_renew_request {} seconds', expires)

        subscr = self._get_subscription_for_request(envelope)
        if subscr is None:
            response = SoapFault(envelope,
                                 code='Receiver',
                                 reason='unknown Subscription identifier',
                                 subCode=wseTag('UnableToRenew')
                                 )

        else:
            subscr.renew(expires)

            response = Soap12Envelope(Prefixes.partial_map(*self.NotificationPrefixes))
            reply_address = envelope.address.mk_reply_address(
                'http://schemas.xmlsoap.org/ws/2004/08/eventing/RenewResponse')
            response.add_header_object(reply_address)
            renew_response_node = etree_.Element(wseTag('RenewResponse'))
            expires_node = etree_.SubElement(renew_response_node, wseTag('Expires'))
            expires_node.text = subscr.expire_string
            response.add_body_element(renew_response_node)
        return response

    def send_episodic_metric_report(self, states, nsmapper, mdib_version, sequence_id):
        action = self.sdc_definitions.Actions.EpisodicMetricReport
        subscribers = self._get_subscriptions_for_action(action)
        if not subscribers:
            return
        self._logger.debug('sending episodic metric report {}', states)
        ns_map = nsmapper.partial_map(*self.BodyNodePrefixes)
        body_node = self._msg_factory.mk_episodic_metric_report_body(
            states, ns_map, mdib_version, sequence_id)
        self._send_to_subscribers(subscribers, body_node, action, nsmapper, 'send_episodic_metric_report')
        self._do_housekeeping()

    def send_periodic_metric_report(self, periodic_states_list, nsmapper, sequence_id):
        action = self.sdc_definitions.Actions.PeriodicMetricReport
        subscribers = self._get_subscriptions_for_action(action)
        if not subscribers:
            return
        self._logger.debug('sending periodic metric report, contains last {} episodic updates',
                           len(periodic_states_list))
        ns_map = nsmapper.partial_map(*self.BodyNodePrefixes)
        body_node = self._msg_factory.mk_periodic_metric_report_body(
            periodic_states_list, ns_map, periodic_states_list[-1].mdib_version, sequence_id)
        self._send_to_subscribers(subscribers, body_node, action, nsmapper, 'send_periodic_metric_report')
        self._do_housekeeping()

    def send_episodic_operational_state_report(self, states, nsmapper, mdib_version, sequence_id):
        action = self.sdc_definitions.Actions.EpisodicOperationalStateReport
        subscribers = self._get_subscriptions_for_action(action)
        if not subscribers:
            return
        self._logger.debug('sending episodic operational state report {}', states)
        ns_map = nsmapper.partial_map(*self.BodyNodePrefixes)
        body_node = self._msg_factory.mk_episodic_operational_state_report_body(
            states, ns_map, mdib_version, sequence_id)
        self._send_to_subscribers(subscribers, body_node, action, nsmapper, 'send_episodic_operational_state_report')
        self._do_housekeeping()

    def send_periodic_operational_state_report(self, periodic_states_list, nsmapper, sequence_id):
        action = self.sdc_definitions.Actions.PeriodicOperationalStateReport
        subscribers = self._get_subscriptions_for_action(action)
        if not subscribers:
            return
        self._logger.debug('sending periodic operational state report, contains last {} episodic updates',
                           len(periodic_states_list))
        ns_map = nsmapper.partial_map(*self.BodyNodePrefixes)
        body_node = self._msg_factory.mk_periodic_operational_state_report_body(
            periodic_states_list, ns_map, periodic_states_list[-1].mdib_version, sequence_id)
        self._send_to_subscribers(subscribers, body_node, action, nsmapper, 'send_periodic_operational_state_report')
        self._do_housekeeping()

    def send_episodic_alert_report(self, states, nsmapper, mdib_version, sequence_id):
        action = self.sdc_definitions.Actions.EpisodicAlertReport
        subscribers = self._get_subscriptions_for_action(action)
        if not subscribers:
            return
        self._logger.debug('sending episodic alert report {}', states)
        ns_map = nsmapper.partial_map(*self.BodyNodePrefixes)
        body_node = self._msg_factory.mk_episodic_alert_report_body(
            states, ns_map, mdib_version, sequence_id)
        self._send_to_subscribers(subscribers, body_node, action, nsmapper, 'send_episodic_alert_report')
        self._do_housekeeping()

    def send_periodic_alert_report(self, periodic_states_list, nsmapper, sequence_id):
        action = self.sdc_definitions.Actions.PeriodicAlertReport
        subscribers = self._get_subscriptions_for_action(action)
        if not subscribers:
            return
        self._logger.debug('sending periodic alert report, contains last {} episodic updates',
                           len(periodic_states_list))
        ns_map = nsmapper.partial_map(*self.BodyNodePrefixes)
        body_node = self._msg_factory.mk_periodic_alert_report_body(
            periodic_states_list, ns_map, periodic_states_list[-1].mdib_version, sequence_id)
        self._send_to_subscribers(subscribers, body_node, action, nsmapper, 'send_periodic_alert_report')
        self._do_housekeeping()

    def send_episodic_component_state_report(self, states, nsmapper, mdib_version, sequence_id):
        action = self.sdc_definitions.Actions.EpisodicComponentReport
        subscribers = self._get_subscriptions_for_action(action)
        if not subscribers:
            return
        self._logger.debug('sending episodic component report {}', states)
        ns_map = nsmapper.partial_map(*self.BodyNodePrefixes)
        body_node = self._msg_factory.mk_episodic_component_state_report_body(
            states, ns_map, mdib_version, sequence_id)
        self._send_to_subscribers(subscribers, body_node, action, nsmapper, 'send_episodic_component_state_report')
        self._do_housekeeping()

    def send_periodic_component_state_report(self, periodic_states_list, nsmapper, sequence_id):
        action = self.sdc_definitions.Actions.PeriodicComponentReport
        subscribers = self._get_subscriptions_for_action(action)
        if not subscribers:
            return
        self._logger.debug('sending periodic component report, contains last {} episodic updates',
                           len(periodic_states_list))
        ns_map = nsmapper.partial_map(*self.BodyNodePrefixes)
        body_node = self._msg_factory.mk_periodic_component_state_report_body(
            periodic_states_list, ns_map, periodic_states_list[-1].mdib_version, sequence_id)
        self._send_to_subscribers(subscribers, body_node, action, nsmapper, 'send_periodic_component_state_report')
        self._do_housekeeping()

    def send_episodic_context_report(self, states, nsmapper, mdib_version, sequence_id):
        action = self.sdc_definitions.Actions.EpisodicContextReport
        subscribers = self._get_subscriptions_for_action(action)
        if not subscribers:
            return
        self._logger.debug('sending episodic context report {}', states)
        ns_map = nsmapper.partial_map(*self.BodyNodePrefixes)
        body_node = self._msg_factory.mk_episodic_context_report_body(
            states, ns_map, mdib_version, sequence_id)
        self._send_to_subscribers(subscribers, body_node, action, nsmapper, 'send_episodic_context_report')
        self._do_housekeeping()

    def send_periodic_context_report(self, periodic_states_list, nsmapper, sequence_id):
        action = self.sdc_definitions.Actions.PeriodicContextReport
        subscribers = self._get_subscriptions_for_action(action)
        if not subscribers:
            return
        self._logger.debug('sending periodic context report, contains last {} episodic updates',
                           len(periodic_states_list))
        ns_map = nsmapper.partial_map(*self.BodyNodePrefixes)
        body_node = self._msg_factory.mk_periodic_context_report_body(
            periodic_states_list, ns_map, periodic_states_list[-1].mdib_version, sequence_id)
        self._send_to_subscribers(subscribers, body_node, action, nsmapper, 'send_periodic_context_report')
        self._do_housekeeping()

    def send_realtime_samples_report(self, realtime_sample_states, nsmapper, mdib_version, sequence_id):
        action = self.sdc_definitions.Actions.Waveform
        subscribers = self._get_subscriptions_for_action(action)
        if not subscribers:
            return
        self._logger.debug('sending real time samples report {}', realtime_sample_states)
        ns_map = nsmapper.partial_map(*self.BodyNodePrefixes)
        body_node = self._msg_factory.mk_realtime_samples_report_body(
            realtime_sample_states, ns_map, mdib_version, sequence_id)
        self._send_to_subscribers(subscribers, body_node, action, nsmapper, None)
        self._do_housekeeping()

    def end_all_subscriptions(self, send_subscription_end):
        action = self.sdc_definitions.Actions.SubscriptionEnd
        with self._subscriptions.lock:
            if send_subscription_end:
                apply_map(lambda subscription: subscription.send_notification_end_message(action),
                          self._subscriptions.objects)
                # for subscr in self._subscriptions.objects:
                #     subscr.send_notification_end_message(action)
            self._subscriptions.clear()

    @staticmethod
    def _mk_descriptor_updates_report_part(parent_node, modification_type, descriptors, updated_states):
        """ Helper that creates ReportPart."""
        # This method creates one ReportPart for every descriptor.
        # An optimization is possible by grouping all descriptors with the same parent handle into one ReportPart.
        # This is not implemented, and I think it is not needed.
        for descriptor in descriptors:
            report_part = etree_.SubElement(parent_node, msgTag('ReportPart'),
                                            attrib={'ModificationType': modification_type})
            if descriptor.parent_handle is not None:  # only Mds can have None
                report_part.set('ParentDescriptor', descriptor.parent_handle)
            report_part.append(descriptor.mk_descriptor_node(tag=msgTag('Descriptor')))
            related_state_containers = [s for s in updated_states if s.descriptorHandle == descriptor.handle]
            state_name = msgTag('State')
            report_part.extend([state.mk_state_node(state_name) for state in related_state_containers])

    def send_descriptor_updates(self, updated, created, deleted, updated_states, nsmapper, mdib_version, sequence_id):
        action = self.sdc_definitions.Actions.DescriptionModificationReport
        subscribers = self._get_subscriptions_for_action(action)
        if not subscribers:
            return
        self._logger.debug('sending DescriptionModificationReport upd={} crt={} del={}', updated, created, deleted)
        body_node = etree_.Element(msgTag('DescriptionModificationReport'),
                                   attrib={'SequenceId': sequence_id,
                                           'MdibVersion': str(mdib_version)},
                                   nsmap=Prefixes.partial_map(Prefixes.MSG, Prefixes.PM))
        self._mk_descriptor_updates_report_part(body_node, 'Upt', updated, updated_states)
        self._mk_descriptor_updates_report_part(body_node, 'Crt', created, updated_states)
        self._mk_descriptor_updates_report_part(body_node, 'Del', deleted, updated_states)

        self._send_to_subscribers(subscribers, body_node, action, nsmapper, 'send_descriptor_updates')
        self._do_housekeeping()

    def _send_to_subscribers(self, subscribers, body_node, action, nsmapper, what):
        for subscriber in subscribers:
            if what:
                self._logger.debug('{}: sending report to {}', what, subscriber.notify_to_address)
            self._send_notification_report(
                subscriber, body_node, action, nsmapper.partial_map(*self.NotificationPrefixes))

    def _send_notification_report(self, subscription, body_node, action, doc_nsmap):
        try:
            subscription.send_notification_report(self._msg_factory, body_node, action, doc_nsmap)
        except HTTPReturnCodeError as ex:
            # this is an error related to the connection => log error and continue
            self._logger.error('could not send notification report: HTTP status= {}, reason={}, {}', ex.status,
                               ex.reason, subscription)
        except http.client.NotConnected as ex:
            # this is an error related to the connection => log error and continue
            self._logger.error('could not send notification report: {!r}:  subscr = {}', ex, subscription)
        except socket.timeout as ex:
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

    def _get_subscriptions_for_action(self, action):
        with self._subscriptions.lock:
            return [s for s in self._subscriptions.objects if s.matches(action)]

    def _get_subscription_for_request(self, envelope):
        request_name = envelope.body_node[0].tag
        identifier_node = envelope.header_node.find(_DevSubscription.IDENT_TAG, namespaces=nsmap)
        if identifier_node is None:
            raise RuntimeError(f'no Identifier found in {request_name}')
        identifier = identifier_node.text
        with self._subscriptions.lock:
            subscr = [s for s in self._subscriptions.objects if s.my_identifier.text == identifier]
        if len(subscr) == 1:
            return subscr[0]
        if len(subscr) == 0:
            self._logger.error('on {}: unknown Subscription identifier "{}"', request_name, identifier)
            return None
        raise RuntimeError(f'Have {len(subscr)} subscriptions with identifier "{identifier}"!')

    def _do_housekeeping(self):
        """ remove expired or invalid subscriptions"""
        with self._subscriptions.lock:
            invalid_subscriptions = [s for s in self._subscriptions.objects if not s.is_valid]
        unreachable_netlocs = []
        for invalid_subscription in invalid_subscriptions:
            if invalid_subscription.has_connection_error:
                # the network location is unreachable, we can remove all subscriptions that use this location
                unreachable_netlocs.append(invalid_subscription.soap_client.netloc)
                try:
                    invalid_subscription.soap_client.close()
                except OSError:
                    self._logger.error('error in soap client.close(): {}', traceback.format_exc())

            self._logger.info('deleting {}, errors={}', invalid_subscription,
                              invalid_subscription._notify_errors)  # pylint: disable=protected-access
            with self._subscriptions.lock:
                self._subscriptions.remove_object(invalid_subscription)

            if invalid_subscription.soap_client.netloc in self.soap_clients:  # remove closed soap client from list
                del self.soap_clients[invalid_subscription.soap_client.netloc]

        # now find all subscriptions that have the same address
        with self._subscriptions.lock:
            also_unreachables = [s for s in self._subscriptions.objects if
                                 s.soap_client is not None and s.soap_client.netloc in unreachable_netlocs]
            for unreachable in also_unreachables:
                self._logger.info('deleting also subscription {}, same endpoint', unreachable)
                self._subscriptions.remove_object(unreachable)

    def get_subscription_round_trip_times(self):
        """Calculates round trip times based on last MAX_ROUNDTRIP_VALUES values.

        @return: a dictionary with key=(<notify_to_address>, (subscriptionnames)), value = _RoundTripData with members min, max, avg, abs_max, values
        """
        ret = {}
        with self._subscriptions.lock:
            for subscription in self._subscriptions.objects:
                if subscription.max_roundtrip_time > 0:
                    ret[(subscription.notify_to_address, subscription.short_filter_names())] = subscription.get_roundtrip_stats()
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
