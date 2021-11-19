from __future__ import annotations

import http.client
import socket
import time
import traceback
import urllib
import uuid
from collections import deque, defaultdict
from typing import List, Optional, TYPE_CHECKING

from lxml import etree as etree_

from .. import isoduration
from .. import loghelper
from .. import multikey
from .. import observableproperties
from ..addressing import ReferenceParameters, Address
from ..etc import apply_map, short_filter_string
from ..namespaces import Prefixes
from ..namespaces import wseTag, DocNamespaceHelper
from ..pmtypes import InvocationError, InvocationState
from ..pysoap.soapclient import HTTPReturnCodeError
from ..pysoap.soapenvelope import SoapFault, SoapFaultCode

if TYPE_CHECKING:
    from ssl import SSLContext
    from ..definitions_base import BaseDefinitions
    from ..pysoap.msgfactory import AbstractMessageFactory, CreatedMessage
    from ..httprequesthandler import RequestData
    from ..pysoap.msgreader import SubscribeRequest, MessageReader
    from ..mdib.statecontainers import AbstractStateContainer
    from ..mdib.descriptorcontainers import AbstractDescriptorContainer
    from .sco import OperationDefinition
    from .periodicreports import PeriodicStates

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
        return f'min={self.min:.4f} max={self.max:.4f} avg={self.avg:.4f} absmax={self.abs_max:.4f}'


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

    def __init__(self, subscribe_request, base_urls, max_subscription_duration, ssl_context,
                 msg_factory):
        """
        :param notify_to_address: dom node of Subscribe Request
        :param end_to_address: dom node of Subscribe Request
        :param expires: seconds as float
        :param filter: a space separated list of actions, or only one action
        """
        self.mode = subscribe_request.mode
        self.base_urls = base_urls
        self._msg_factory = msg_factory
        self.notify_to_address = subscribe_request.notify_to_address
        self._notify_to_url = urllib.parse.urlparse(subscribe_request.notify_to_address)

        self.notify_ref_params = subscribe_request.notify_ref_params

        self.end_to_address = subscribe_request.end_to_address
        if self.end_to_address is not None:
            self._end_to_url = urllib.parse.urlparse(self.end_to_address)
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
        self._filters = subscribe_request.subscription_filters
        self._ssl_context = ssl_context

        self._accepted_encodings = subscribe_request.accepted_encodings  # these encodings does the other side accept
        self._soap_client = None

        self._notify_errors = 0
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

    def send_notification_report(self, msg_factory, body_node, action, doc_nsmap):
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

            self._soap_client.post_message_to(self._notify_to_url.path, message,
                                              msg=f'send_notification_report {action}')
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

    def send_notification_end_message(self, msg_factory, code='SourceShuttingDown',
                                      reason='Event source going off line.'):
        url = self.base_urls[0]
        my_addr = f'{url.scheme}:{url.netloc}/{url.path}'

        if not self.is_valid:
            return
        if self._soap_client is None:
            return
        message = msg_factory.mk_notification_end_message(self, my_addr, code, reason)
        try:
            url = self._end_to_url or self._notify_to_url
            self._soap_client.post_message_to(url.path, message,
                                              msg='send_notification_end_message')
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
            if self.notify_ref_params is None:
                ref_ident = '<none>'
            else:
                ref_ident = str(
                    self.notify_ref_params)  # ', '.join([node.text for node in self.notify_ref_params.parameters])
        except TypeError:
            ref_ident = '<unknown>'
        return f'Subscription(notify_to={self.notify_to_address} ident={ref_ident}, ' \
               f'my identifier={self.identifier_uuid.hex}, expires={self.remaining_seconds}, ' \
               f'filter={short_filter_string(self._filters)})'

    def get_roundtrip_stats(self):
        if len(self.last_roundtrip_times) > 0:
            return _RoundTripData(self.last_roundtrip_times, self.max_roundtrip_time)
        return _RoundTripData(None, None)

    def short_filter_names(self):
        return tuple([f.split('/')[-1] for f in self._filters])


class SubscriptionsManagerBase:
    """This implementation uses ReferenceParameters to identify subscriptions."""
    BodyNodePrefixes = [Prefixes.PM, Prefixes.MSG, Prefixes.XSI, Prefixes.EXT, Prefixes.XML]
    NotificationPrefixes = [Prefixes.PM, Prefixes.S12, Prefixes.WSA, Prefixes.WSE]
    DEFAULT_MAX_SUBSCR_DURATION = 7200  # max. possible duration of a subscription

    def __init__(self, ssl_context: SSLContext,
                 sdc_definitions: BaseDefinitions,
                 msg_factory: AbstractMessageFactory,
                 msg_reader: MessageReader,
                 soap_client_class,
                 supported_encodings: List[str],
                 max_subscription_duration: [float, None] = None,
                 log_prefix: str = None,
                 chunked_messages: bool = False):
        self._ssl_context = ssl_context
        self.sdc_definitions = sdc_definitions
        self._msg_factory = msg_factory
        self._msg_reader = msg_reader
        self._soap_client_class = soap_client_class
        self.log_prefix = log_prefix
        self._logger = loghelper.get_logger_adapter('sdc.device.subscrMgr', self.log_prefix)
        self._chunked_messages = chunked_messages
        self.soap_clients = {}  # key: net location, value soap_client instance
        self._supported_encodings = supported_encodings
        self._max_subscription_duration = max_subscription_duration or self.DEFAULT_MAX_SUBSCR_DURATION
        self._subscriptions = multikey.MultiKeyLookup()
        self._subscriptions.add_index(
            'dispatch_identifier',
            multikey.UIndexDefinition(lambda obj: _mk_dispatch_identifier(obj.reference_parameters, obj.path_suffix)))
        self._subscriptions.add_index('identifier', multikey.UIndexDefinition(lambda obj: obj.identifier_uuid.hex))
        self._subscriptions.add_index('netloc', multikey.IndexDefinition(
            lambda obj: obj._notify_to_url.netloc))  # pylint:disable=protected-access
        self.base_urls = None

    def set_base_urls(self, base_urls):
        self.base_urls = base_urls

    def _mk_subscription_instance(self, subscribe_request):
        return _DevSubscription(subscribe_request, self.base_urls, self._max_subscription_duration,
                                self._ssl_context, msg_factory=self._msg_factory)

    def on_subscribe_request(self, request_data: RequestData,
                             subscribe_request: SubscribeRequest) -> CreatedMessage:

        subscr = self._mk_subscription_instance(subscribe_request)
        # assign a soap client
        key = subscr._notify_to_url.netloc  # pylint:disable=protected-access
        soap_client = self.soap_clients.get(key)
        if soap_client is None:
            soap_client = self._soap_client_class(key, loghelper.get_logger_adapter('sdc.device.soap', self.log_prefix),
                                                  ssl_context=self._ssl_context, sdc_definitions=self.sdc_definitions,
                                                  msg_reader=self._msg_reader,
                                                  supported_encodings=self._supported_encodings,
                                                  request_encodings=subscribe_request.accepted_encodings,
                                                  chunked_requests=self._chunked_messages)
            self.soap_clients[key] = soap_client
        subscr.set_soap_client(soap_client)
        with self._subscriptions.lock:
            self._subscriptions.add_object(subscr)
        self._logger.info('new {}', subscr)
        response = self._msg_factory.mk_subscribe_response_message(request_data, subscr, self.base_urls)
        return response

    def on_unsubscribe_request(self, request_data: RequestData) -> CreatedMessage:
        subscription = self._get_subscription_for_request(request_data)
        if subscription is None:
            fault = SoapFault(code=SoapFaultCode.RECEIVER,
                              reason='unknown Subscription identifier',
                              sub_code=wseTag('InvalidMessage')
                              )
            response = self._msg_factory.mk_fault_message(request_data.message_data, fault)
        else:
            subscription.close()
            with self._subscriptions.lock:
                self._subscriptions.remove_object(subscription)
            self._logger.info('unsubscribe: object found and removed (Xaddr = {}, filter = {})',
                              subscription.notify_to_address,
                              subscription._filters)  # pylint: disable=protected-access
            # now check if we can close the soap client
            key = subscription._notify_to_url.netloc  # pylint: disable=protected-access
            subscriptions_with_same_soap_client = self._subscriptions.netloc.get(key, [])
            if len(subscriptions_with_same_soap_client) == 0:
                self.soap_clients[key].close()
                del self.soap_clients[key]
                self._logger.info('unsubscribe: closed soap client to {})', key)
            response = self._msg_factory.mk_unsubscribe_response_message(request_data)
        return response

    def on_get_status_request(self, request_data: RequestData) -> CreatedMessage:
        self._logger.debug('on_get_status_request {}', lambda: request_data.message_data.p_msg.raw_data)
        subscription = self._get_subscription_for_request(request_data)
        if subscription is None:
            fault = SoapFault(code=SoapFaultCode.RECEIVER,
                              reason='unknown Subscription identifier',
                              sub_code=wseTag('InvalidMessage')
                              )
            response = self._msg_factory.mk_fault_message(request_data.message_data, fault)
        else:
            response = self._msg_factory.mk_getstatus_response_message(request_data, subscription.remaining_seconds)
        return response

    def on_renew_request(self, request_data: RequestData) -> CreatedMessage:
        reader = request_data.message_data.msg_reader
        expires = reader.read_renew_request(request_data.message_data)
        subscription = self._get_subscription_for_request(request_data)
        if subscription is None:
            fault = SoapFault(code=SoapFaultCode.RECEIVER,
                              reason='unknown Subscription identifier',
                              sub_code=wseTag('UnableToRenew')
                              )
            response = self._msg_factory.mk_fault_message(request_data.message_data, fault)

        else:
            subscription.renew(expires)
            response = self._msg_factory.mk_renew_response_message(request_data, subscription.remaining_seconds)
        return response

    def end_all_subscriptions(self, send_subscription_end: bool):
        with self._subscriptions.lock:
            if send_subscription_end:
                apply_map(lambda subscription: subscription.send_notification_end_message(self._msg_factory),
                          self._subscriptions.objects)
            self._subscriptions.clear()

    def notify_operation(self, operation: OperationDefinition,
                         transaction_id: int,
                         invocation_state: InvocationState,
                         mdib_version: int,
                         sequence_id: str,
                         nsmapper: DocNamespaceHelper,
                         error: Optional[InvocationError] = None,
                         error_message: Optional[str] = None):
        operation_handle_ref = operation.handle
        self._logger.info(
            'notify_operation transaction={} operation_handle_ref={}, operationState={}, error={}, errorMessage={}',
            transaction_id, operation_handle_ref, invocation_state, error, error_message)
        action = self.sdc_definitions.Actions.OperationInvokedReport
        subscribers = self._get_subscriptions_for_action(action)

        body_node = self._msg_factory.mk_operation_invoked_report_body(mdib_version, sequence_id,
                                                                       operation_handle_ref, transaction_id,
                                                                       invocation_state, error, error_message, nsmapper)
        self._send_to_subscribers(subscribers, body_node, action, nsmapper, 'notify_operation')
        self._do_housekeeping()

    def send_episodic_metric_report(self, states: List[AbstractStateContainer],
                                    nsmapper: DocNamespaceHelper,
                                    mdib_version: int,
                                    sequence_id: str):
        action = self.sdc_definitions.Actions.EpisodicMetricReport
        subscribers = self._get_subscriptions_for_action(action)
        if not subscribers:
            return
        self._logger.debug('sending episodic metric report {}', states)
        body_node = self._msg_factory.mk_episodic_metric_report_body(
            mdib_version, sequence_id, states, nsmapper)
        self._send_to_subscribers(subscribers, body_node, action, nsmapper, 'send_episodic_metric_report')
        self._do_housekeeping()

    def send_periodic_metric_report(self, periodic_states_list: List[PeriodicStates],
                                    nsmapper: DocNamespaceHelper,
                                    sequence_id: str):
        action = self.sdc_definitions.Actions.PeriodicMetricReport
        subscribers = self._get_subscriptions_for_action(action)
        if not subscribers:
            return
        self._logger.debug('sending periodic metric report, contains last {} episodic updates',
                           len(periodic_states_list))
        body_node = self._msg_factory.mk_periodic_metric_report_body(
            periodic_states_list[-1].mdib_version, sequence_id, periodic_states_list, nsmapper)
        self._send_to_subscribers(subscribers, body_node, action, nsmapper, 'send_periodic_metric_report')
        self._do_housekeeping()

    def send_episodic_alert_report(self, states: List[AbstractStateContainer],
                                   nsmapper: DocNamespaceHelper,
                                   mdib_version: int,
                                   sequence_id: str):
        action = self.sdc_definitions.Actions.EpisodicAlertReport
        subscribers = self._get_subscriptions_for_action(action)
        if not subscribers:
            return
        self._logger.debug('sending episodic alert report {}', states)
        body_node = self._msg_factory.mk_episodic_alert_report_body(
            mdib_version, sequence_id, states, nsmapper)
        self._send_to_subscribers(subscribers, body_node, action, nsmapper, 'send_episodic_alert_report')
        self._do_housekeeping()

    def send_periodic_alert_report(self, periodic_states_list: List[PeriodicStates],
                                   nsmapper: DocNamespaceHelper,
                                   sequence_id: str):
        action = self.sdc_definitions.Actions.PeriodicAlertReport
        subscribers = self._get_subscriptions_for_action(action)
        if not subscribers:
            return
        self._logger.debug('sending periodic alert report, contains last {} episodic updates',
                           len(periodic_states_list))
        body_node = self._msg_factory.mk_periodic_alert_report_body(
            periodic_states_list[-1].mdib_version, sequence_id, periodic_states_list, nsmapper)
        self._send_to_subscribers(subscribers, body_node, action, nsmapper, 'send_periodic_alert_report')
        self._do_housekeeping()

    def send_episodic_operational_state_report(self, states: List[AbstractStateContainer],
                                               nsmapper: DocNamespaceHelper,
                                               mdib_version: int,
                                               sequence_id: str):
        action = self.sdc_definitions.Actions.EpisodicOperationalStateReport
        subscribers = self._get_subscriptions_for_action(action)
        if not subscribers:
            return
        self._logger.debug('sending episodic operational state report {}', states)
        body_node = self._msg_factory.mk_episodic_operational_state_report_body(
            mdib_version, sequence_id, states, nsmapper)
        self._send_to_subscribers(subscribers, body_node, action, nsmapper, 'send_episodic_operational_state_report')
        self._do_housekeeping()

    def send_periodic_operational_state_report(self, periodic_states_list: List[PeriodicStates],
                                               nsmapper: DocNamespaceHelper,
                                               sequence_id: str):
        action = self.sdc_definitions.Actions.PeriodicOperationalStateReport
        subscribers = self._get_subscriptions_for_action(action)
        if not subscribers:
            return
        self._logger.debug('sending periodic operational state report, contains last {} episodic updates',
                           len(periodic_states_list))
        body_node = self._msg_factory.mk_periodic_operational_state_report_body(
            periodic_states_list[-1].mdib_version, sequence_id, periodic_states_list, nsmapper)
        self._send_to_subscribers(subscribers, body_node, action, nsmapper, 'send_periodic_operational_state_report')
        self._do_housekeeping()

    def send_episodic_component_state_report(self, states: List[AbstractStateContainer],
                                             nsmapper: DocNamespaceHelper,
                                             mdib_version: int,
                                             sequence_id: str):
        action = self.sdc_definitions.Actions.EpisodicComponentReport
        subscribers = self._get_subscriptions_for_action(action)
        if not subscribers:
            return
        self._logger.debug('sending episodic component report {}', states)
        body_node = self._msg_factory.mk_episodic_component_state_report_body(
            mdib_version, sequence_id, states, nsmapper)
        self._send_to_subscribers(subscribers, body_node, action, nsmapper, 'send_episodic_component_state_report')
        self._do_housekeeping()

    def send_periodic_component_state_report(self, periodic_states_list: List[PeriodicStates],
                                             nsmapper: DocNamespaceHelper,
                                             sequence_id: str):
        action = self.sdc_definitions.Actions.PeriodicComponentReport
        subscribers = self._get_subscriptions_for_action(action)
        if not subscribers:
            return
        self._logger.debug('sending periodic component report, contains last {} episodic updates',
                           len(periodic_states_list))
        body_node = self._msg_factory.mk_periodic_component_state_report_body(
            periodic_states_list[-1].mdib_version, sequence_id, periodic_states_list, nsmapper)
        self._send_to_subscribers(subscribers, body_node, action, nsmapper, 'send_periodic_component_state_report')
        self._do_housekeeping()

    def send_episodic_context_report(self, states: List[AbstractStateContainer],
                                     nsmapper: DocNamespaceHelper,
                                     mdib_version: int,
                                     sequence_id: str):
        action = self.sdc_definitions.Actions.EpisodicContextReport
        subscribers = self._get_subscriptions_for_action(action)
        if not subscribers:
            return
        self._logger.debug('sending episodic context report {}', states)
        body_node = self._msg_factory.mk_episodic_context_report_body(
            mdib_version, sequence_id, states, nsmapper)
        self._send_to_subscribers(subscribers, body_node, action, nsmapper, 'send_episodic_context_report')
        self._do_housekeeping()

    def send_periodic_context_report(self, periodic_states_list: List[PeriodicStates],
                                     nsmapper: DocNamespaceHelper,
                                     sequence_id: str):
        action = self.sdc_definitions.Actions.PeriodicContextReport
        subscribers = self._get_subscriptions_for_action(action)
        if not subscribers:
            return
        self._logger.debug('sending periodic context report, contains last {} episodic updates',
                           len(periodic_states_list))
        body_node = self._msg_factory.mk_periodic_context_report_body(
            periodic_states_list[-1].mdib_version, sequence_id, periodic_states_list, nsmapper)
        self._send_to_subscribers(subscribers, body_node, action, nsmapper, 'send_periodic_context_report')
        self._do_housekeeping()

    def send_realtime_samples_report(self, realtime_sample_states: List[AbstractStateContainer],
                                     nsmapper: DocNamespaceHelper,
                                     mdib_version: int,
                                     sequence_id: str):
        action = self.sdc_definitions.Actions.Waveform
        subscribers = self._get_subscriptions_for_action(action)
        if not subscribers:
            return
        self._logger.debug('sending real time samples report {}', realtime_sample_states)
        body_node = self._msg_factory.mk_realtime_samples_report_body(
            mdib_version, sequence_id, realtime_sample_states, nsmapper)
        self._send_to_subscribers(subscribers, body_node, action, nsmapper, None)
        self._do_housekeeping()

    def send_descriptor_updates(self, updated: List[AbstractDescriptorContainer],
                                created: List[AbstractDescriptorContainer],
                                deleted: List[AbstractDescriptorContainer],
                                updated_states: List[AbstractStateContainer],
                                nsmapper: DocNamespaceHelper,
                                mdib_version: int,
                                sequence_id: str):
        action = self.sdc_definitions.Actions.DescriptionModificationReport
        subscribers = self._get_subscriptions_for_action(action)
        if not subscribers:
            return
        self._logger.debug('sending DescriptionModificationReport upd={} crt={} del={}', updated, created, deleted)
        body_node = self._msg_factory.mk_description_modification_report_body(
            mdib_version, sequence_id, updated, created, deleted, updated_states, nsmapper)
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

    def _get_subscription_for_request(self, request_data):
        reader = request_data.message_data.msg_reader
        reference_parameters = reader.read_header_reference_parameters(request_data.message_data)
        path_suffix = '/'.join(request_data.path_elements)  # not consumed path elements
        dispatch_identifier = _mk_dispatch_identifier(reference_parameters, path_suffix)
        with self._subscriptions.lock:
            subscription = self._subscriptions.dispatch_identifier.get_one(dispatch_identifier, allow_none=True)
        if subscription is None:
            self._logger.error('{}: unknown Subscription identifier "{}" from {}',
                               request_data.message_data.msg_name, dispatch_identifier, request_data.peer_name)
        return subscription

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


class SubscriptionsManagerPath(SubscriptionsManagerBase):
    """This implementation uses path dispatching to identify subscriptions."""

    def _mk_subscription_instance(self, subscribe_request):
        subscription = super()._mk_subscription_instance(subscribe_request)
        subscription.path_suffix = subscription.identifier_uuid.hex
        return subscription


class SubscriptionsManagerReferenceParam(SubscriptionsManagerBase):
    """This implementation uses reference parameters to identify subscriptions."""

    def _mk_subscription_instance(self, subscribe_request):
        subscription = super()._mk_subscription_instance(subscribe_request)
        # add  a reference parameter
        subscription.set_reference_parameter()
        return subscription
