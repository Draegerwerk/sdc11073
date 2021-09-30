from __future__ import annotations

import http.client
import socket
import time
import traceback
import urllib
import uuid
from abc import ABC, abstractmethod
from collections import deque, defaultdict
from typing import ForwardRef, List, Iterable, TYPE_CHECKING

from lxml import etree as etree_

from .. import isoduration
from .. import loghelper
from .. import multikey
from .. import observableproperties
from ..etc import apply_map, short_filter_string
from ..namespaces import Prefixes
from ..namespaces import wseTag, DocNamespaceHelper
from ..pmtypes import InvocationError, InvocationState
from ..pysoap.soapclient import SoapClient, HTTPReturnCodeError
from ..pysoap.soapenvelope import SoapFault, WsAddress

if TYPE_CHECKING:
    from ssl import SSLContext
    from ..definitions_base import BaseDefinitions, SchemaValidators
    from ..pysoap.msgfactory import AbstractMessageFactory
    from ..httprequesthandler import RequestData
    from ..pysoap.msgreader import SubscribeRequest
    from ..pysoap.soapenvelope import Soap12Envelope

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


def _mk_dispatch_identifier(reference_parameter_node, path_suffix):
    if path_suffix == '':
        path_suffix = None
    if reference_parameter_node is None:
        return None, path_suffix
    return reference_parameter_node.text, path_suffix


class _DevSubscription:
    MAX_NOTIFY_ERRORS = 1
    IDENT_TAG = etree_.QName('http.local.com', 'MyDevIdentifier')

    def __init__(self, subscribe_request, base_urls, max_subscription_duration, ssl_context, schema_validators):
        """
        :param notify_to_address: dom node of Subscribe Request
        :param end_to_address: dom node of Subscribe Request
        :param expires: seconds as float
        :param filter: a space separated list of actions, or only one action
        """
        self.mode = subscribe_request.mode
        self.base_urls = base_urls
        self.notify_to_address = subscribe_request.notify_to_address
        self._notify_to_url = urllib.parse.urlparse(subscribe_request.notify_to_address)

        self.notify_ref_nodes = []
        if subscribe_request.notify_ref_node is not None:
            self.notify_ref_nodes = list(subscribe_request.notify_ref_node)  # all children

        self.end_to_address = subscribe_request.end_to_address
        if self.end_to_address is not None:
            self._end_to_url = urllib.parse.urlparse(self.end_to_address)
        else:
            self._end_to_url = None
        self.end_to_ref_nodes = []
        if subscribe_request.end_to_ref_node is not None:
            self.end_to_ref_nodes = list(subscribe_request.end_to_ref_node)  # all children
        self.identifier_uuid = uuid.uuid4()
        self.reference_parameter = None  # etree node, used for reference parameters based dispatching
        self.path_suffix = None  # used for path based dispatching

        self._max_subscription_duration = max_subscription_duration
        self._started = None
        self._expireseconds = None
        self.renew(subscribe_request.expires)  # sets self._started and self._expireseconds
        self._filters = subscribe_request.subscription_filters
        self._ssl_context = ssl_context
        self._schema_validators = schema_validators

        self._accepted_encodings = subscribe_request.accepted_encodings  # these encodings does the other side accept
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

            self._soap_client.post_soap_envelope_to(self._notify_to_url.path, soap_envelope,
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

    def send_notification_end_message(self, msg_factory, code='SourceShuttingDown',
                                      reason='Event source going off line.'):
        my_addr = '{}:{}/{}'.format(self.base_urls[0].scheme, self.base_urls[0].netloc, self.base_urls[0].path)

        if not self.is_valid:
            return
        if self._soap_client is None:
            return
        envelope = msg_factory.mk_notification_end_report(self, my_addr, code, reason)
        try:
            url = self._end_to_url or self._notify_to_url
            self._soap_client.post_soap_envelope_to(url.path, envelope,
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
            ref_ident = ', '.join([node.text for node in self.notify_ref_nodes])
        except TypeError:
            ref_ident = '<unknown>'
        return 'Subscription(notify_to={} idnt={}, my identifier={}, expires={}, filter={})'.format(
            self.notify_to_address,
            ref_ident,
            self.identifier_uuid.hex,
            self.remaining_seconds,
            short_filter_string(self._filters))

    def get_roundtrip_stats(self):
        if len(self.last_roundtrip_times) > 0:
            return _RoundTripData(self.last_roundtrip_times, self.max_roundtrip_time)
        return _RoundTripData(None, None)

    def short_filter_names(self):
        return tuple([f.split('/')[-1] for f in self._filters])


class AbstractSubscriptionsManager(ABC):
    @abstractmethod
    def __init__(self, ssl_context: SSLContext,
                 sdc_definitions: BaseDefinitions,
                 schema_validators: SchemaValidators,
                 msg_factory: AbstractMessageFactory,
                 supported_encodings: List[str],
                 max_subscription_duration: [float, None] = None,
                 log_prefix: str = None,
                 chunked_messages: bool = False):
        """

        :param ssl_context:
        :param sdc_definitions:
        :param schema_validators:
        :param msg_factory:
        :param supported_encodings:
        :param max_subscription_duration:
        :param log_prefix:
        :param chunked_messages:
        """

    @abstractmethod
    def on_subscribe_request(self, request_data: RequestData, subscribe_request: SubscribeRequest) -> Soap12Envelope:
        """

        :param request_data: the request
        :return: a response
        """

    @abstractmethod
    def on_unsubscribe_request(self, request_data: RequestData) -> Soap12Envelope:
        """

        :param request_data: the request
        :return: a response
        """

    @abstractmethod
    def on_get_status_request(self, request_data: RequestData) -> Soap12Envelope:
        """

        :param request_data: the request
        :return: a response
        """

    @abstractmethod
    def on_renew_request(self, request_data: RequestData) -> Soap12Envelope:
        """

        :param request_data: the request
        :return: a response
        """

    @abstractmethod
    def end_all_subscriptions(self, send_subscription_end: bool) -> None:
        """

        :param send_subscription_end:
        :return:
        """

    @abstractmethod
    def notify_operation(self, operation: ForwardRef('OperationDefinition'),
                         transaction_id: int,
                         invocation_state: InvocationState,
                         mdib_version: int,
                         sequence_id: str,
                         nsmapper: DocNamespaceHelper,
                         error: [InvocationError, None] = None,
                         error_message: [str, None] = None) -> None:
        """

        :param operation:
        :param transaction_id:
        :param invocation_state:
        :param nsmapper:
        :param sequence_id:
        :param mdib_version:
        :param error:
        :param error_message:
        :return:
        """

    @abstractmethod
    def send_episodic_metric_report(self, states: Iterable[ForwardRef('AbstractStateContainer')],
                                    nsmapper: DocNamespaceHelper,
                                    mdib_version: int,
                                    sequence_id: str) -> None:
        """

        :param states:
        :param nsmapper:
        :param mdib_version:
        :param sequence_id:
        :return:
        """

    @abstractmethod
    def send_periodic_metric_report(self, periodic_states_list: Iterable[ForwardRef('PeriodicStates')],
                                    nsmapper: DocNamespaceHelper,
                                    sequence_id: str) -> None:
        """

        :param periodic_states_list:
        :param nsmapper:
        :param sequence_id:
        :return:
        """

    @abstractmethod
    def send_episodic_alert_report(self, states: Iterable[ForwardRef('AbstractStateContainer')],
                                   nsmapper: DocNamespaceHelper,
                                   mdib_version: int,
                                   sequence_id: str) -> None:
        """

        :param states:
        :param nsmapper:
        :param mdib_version:
        :param sequence_id:
        :return:
        """

    @abstractmethod
    def send_periodic_alert_report(self, periodic_states_list: Iterable[ForwardRef('PeriodicStates')],
                                   nsmapper: DocNamespaceHelper,
                                   sequence_id: str) -> None:
        """

        :param periodic_states_list:
        :param nsmapper:
        :param sequence_id:
        :return:
        """

    @abstractmethod
    def send_episodic_operational_state_report(self, states: Iterable[ForwardRef('AbstractStateContainer')],
                                               nsmapper: DocNamespaceHelper,
                                               mdib_version: int,
                                               sequence_id: str) -> None:
        """

        :param states:
        :param nsmapper:
        :param mdib_version:
        :param sequence_id:
        :return:
        """

    @abstractmethod
    def send_periodic_operational_state_report(self, periodic_states_list: Iterable[ForwardRef('PeriodicStates')],
                                               nsmapper: DocNamespaceHelper,
                                               sequence_id: str) -> None:
        """

        :param periodic_states_list:
        :param nsmapper:
        :param sequence_id:
        :return:
        """

    @abstractmethod
    def send_episodic_component_state_report(self, states: Iterable[ForwardRef('AbstractStateContainer')],
                                             nsmapper: DocNamespaceHelper,
                                             mdib_version: int,
                                             sequence_id: str) -> None:
        """

        :param states:
        :param nsmapper:
        :param mdib_version:
        :param sequence_id:
        :return:
        """

    @abstractmethod
    def send_periodic_component_state_report(self, periodic_states_list: Iterable[ForwardRef('PeriodicStates')],
                                             nsmapper: DocNamespaceHelper,
                                             sequence_id: str) -> None:
        """

        :param periodic_states_list:
        :param nsmapper:
        :param sequence_id:
        :return:
        """

    @abstractmethod
    def send_episodic_context_report(self, states: Iterable[ForwardRef('AbstractContextStateContainer')],
                                     nsmapper: DocNamespaceHelper,
                                     mdib_version: int,
                                     sequence_id: str) -> None:
        """

        :param states:
        :param nsmapper:
        :param mdib_version:
        :param sequence_id:
        :return:
        """

    @abstractmethod
    def send_periodic_context_report(self, periodic_states_list: Iterable[ForwardRef('PeriodicStates')],
                                     nsmapper: DocNamespaceHelper,
                                     sequence_id: str) -> None:
        """

        :param periodic_states_list:
        :param nsmapper:
        :param sequence_id:
        :return:
        """

    @abstractmethod
    def send_realtime_samples_report(self,
                                     realtime_sample_statesList: Iterable[
                                         ForwardRef('RealTimeSampleArrayMetricStateContainer')],
                                     nsmapper: DocNamespaceHelper,
                                     mdib_version: int,
                                     sequence_id: str) -> None:
        """

        :param realtime_sample_statesList:
        :param nsmapper:
        :param mdib_version:
        :param sequence_id:
        :return:
        """

    @abstractmethod
    def send_descriptor_updates(self,
                                updated: Iterable[ForwardRef('AbstractDescriptorContainer')],
                                created: Iterable[ForwardRef('AbstractDescriptorContainer')],
                                deleted: Iterable[ForwardRef('AbstractDescriptorContainer')],
                                updated_states: Iterable[ForwardRef('AbstractStateContainer')],
                                nsmapper: DocNamespaceHelper,
                                mdib_version: int,
                                sequence_id: str):
        """

        :param updated:
        :param created:
        :param deleted:
        :param updated_states:
        :param nsmapper:
        :param mdib_version:
        :param sequence_id:
        :return:
        """


class _SubscriptionsManagerBase(AbstractSubscriptionsManager):
    """This implementation uses ReferenceParameters to identify subscriptions."""
    BodyNodePrefixes = [Prefixes.PM, Prefixes.MSG, Prefixes.XSI, Prefixes.EXT, Prefixes.XML]
    NotificationPrefixes = [Prefixes.PM, Prefixes.S12, Prefixes.WSA, Prefixes.WSE]
    DEFAULT_MAX_SUBSCR_DURATION = 7200  # max. possible duration of a subscription

    def __init__(self, ssl_context, sdc_definitions, schema_validators, msg_factory, msg_reader, supported_encodings,
                 max_subscription_duration=None, log_prefix=None, chunked_messages=False):
        self._ssl_context = ssl_context
        self.schema_validators = schema_validators
        self.sdc_definitions = sdc_definitions
        self._msg_factory = msg_factory
        self._msg_reader = msg_reader
        self.log_prefix = log_prefix
        self._logger = loghelper.get_logger_adapter('sdc.device.subscrMgr', self.log_prefix)
        self._chunked_messages = chunked_messages
        self.soap_clients = {}  # key: net location, value soap_client instance
        self._supported_encodings = supported_encodings
        self._max_subscription_duration = max_subscription_duration or self.DEFAULT_MAX_SUBSCR_DURATION
        self._subscriptions = multikey.MultiKeyLookup()
        self._subscriptions.add_index(
            'dispatch_identifier',
            multikey.UIndexDefinition(lambda obj: _mk_dispatch_identifier(obj.reference_parameter, obj.path_suffix)))
        self._subscriptions.add_index('identifier', multikey.UIndexDefinition(lambda obj: obj.identifier_uuid.hex))
        self._subscriptions.add_index('netloc', multikey.IndexDefinition(
            lambda obj: obj._notify_to_url.netloc))  # pylint:disable=protected-access
        self.base_urls = None

    def set_base_urls(self, base_urls):
        self.base_urls = base_urls

    def _mk_subscription_instance(self, subscribe_request):
        return _DevSubscription(subscribe_request, self.base_urls, self._max_subscription_duration,
                                self._ssl_context, self.schema_validators)

    def on_subscribe_request(self, request_data, subscribe_request):
        subscr = self._mk_subscription_instance(subscribe_request)
        # assign a soap client
        key = subscr._notify_to_url.netloc  # pylint:disable=protected-access
        soap_client = self.soap_clients.get(key)
        if soap_client is None:
            soap_client = SoapClient(key, loghelper.get_logger_adapter('sdc.device.soap', self.log_prefix),
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
        response = self._msg_factory.mk_subscribe_response_envelope(request_data, subscr, self.base_urls)
        return response

    def on_unsubscribe_request(self, request_data):
        subscription = self._get_subscription_for_request(request_data)
        if subscription is None:
            response = SoapFault(request_data.message_data.raw_data,
                                 code='Receiver',
                                 reason='unknown Subscription identifier',
                                 subCode=wseTag('InvalidMessage')
                                 )
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
            response = self._msg_factory.mk_unsubscribe_response_envelope(request_data)
        return response

    def on_get_status_request(self, request_data):
        self._logger.debug('on_get_status_request {}', lambda: request_data.message_data.raw_data.as_xml(pretty=True))
        subscription = self._get_subscription_for_request(request_data)
        if subscription is None:
            response = SoapFault(request_data.message_data.raw_data,
                                 code='Receiver',
                                 reason='unknown Subscription identifier',
                                 subCode=wseTag('InvalidMessage')
                                 )
        else:
            response = self._msg_factory.mk_getstatus_response_envelope(request_data, subscription.remaining_seconds)
        return response

    def on_renew_request(self, request_data):
        reader = request_data.message_data.msg_reader
        expires = reader.read_renew_request(request_data.message_data)
        subscription = self._get_subscription_for_request(request_data)
        if subscription is None:
            response = SoapFault(request_data.message_data.raw_data,
                                 code='Receiver',
                                 reason='unknown Subscription identifier',
                                 subCode=wseTag('UnableToRenew')
                                 )

        else:
            subscription.renew(expires)
            response = self._msg_factory.mk_renew_response_envelope(request_data, subscription.remaining_seconds)
        return response

    def end_all_subscriptions(self, send_subscription_end):
        with self._subscriptions.lock:
            if send_subscription_end:
                apply_map(lambda subscription: subscription.send_notification_end_message(self._msg_factory),
                          self._subscriptions.objects)
            self._subscriptions.clear()

    def notify_operation(self, operation, transaction_id, invocation_state,
                         mdib_version, sequence_id, nsmapper,
                         error=None, error_message=None):
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

    def send_episodic_metric_report(self, states, nsmapper, mdib_version, sequence_id):
        action = self.sdc_definitions.Actions.EpisodicMetricReport
        subscribers = self._get_subscriptions_for_action(action)
        if not subscribers:
            return
        self._logger.debug('sending episodic metric report {}', states)
        body_node = self._msg_factory.mk_episodic_metric_report_body(
            mdib_version, sequence_id, states, nsmapper)
        self._send_to_subscribers(subscribers, body_node, action, nsmapper, 'send_episodic_metric_report')
        self._do_housekeeping()

    def send_periodic_metric_report(self, periodic_states_list, nsmapper, sequence_id):
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

    def send_episodic_alert_report(self, states, nsmapper, mdib_version, sequence_id):
        action = self.sdc_definitions.Actions.EpisodicAlertReport
        subscribers = self._get_subscriptions_for_action(action)
        if not subscribers:
            return
        self._logger.debug('sending episodic alert report {}', states)
        body_node = self._msg_factory.mk_episodic_alert_report_body(
            mdib_version, sequence_id, states, nsmapper)
        self._send_to_subscribers(subscribers, body_node, action, nsmapper, 'send_episodic_alert_report')
        self._do_housekeeping()

    def send_periodic_alert_report(self, periodic_states_list, nsmapper, sequence_id):
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

    def send_episodic_operational_state_report(self, states, nsmapper, mdib_version, sequence_id):
        action = self.sdc_definitions.Actions.EpisodicOperationalStateReport
        subscribers = self._get_subscriptions_for_action(action)
        if not subscribers:
            return
        self._logger.debug('sending episodic operational state report {}', states)
        body_node = self._msg_factory.mk_episodic_operational_state_report_body(
            mdib_version, sequence_id, states, nsmapper)
        self._send_to_subscribers(subscribers, body_node, action, nsmapper, 'send_episodic_operational_state_report')
        self._do_housekeeping()

    def send_periodic_operational_state_report(self, periodic_states_list, nsmapper, sequence_id):
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

    def send_episodic_component_state_report(self, states, nsmapper, mdib_version, sequence_id):
        action = self.sdc_definitions.Actions.EpisodicComponentReport
        subscribers = self._get_subscriptions_for_action(action)
        if not subscribers:
            return
        self._logger.debug('sending episodic component report {}', states)
        body_node = self._msg_factory.mk_episodic_component_state_report_body(
            mdib_version, sequence_id, states, nsmapper)
        self._send_to_subscribers(subscribers, body_node, action, nsmapper, 'send_episodic_component_state_report')
        self._do_housekeeping()

    def send_periodic_component_state_report(self, periodic_states_list, nsmapper, sequence_id):
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

    def send_episodic_context_report(self, states, nsmapper, mdib_version, sequence_id):
        action = self.sdc_definitions.Actions.EpisodicContextReport
        subscribers = self._get_subscriptions_for_action(action)
        if not subscribers:
            return
        self._logger.debug('sending episodic context report {}', states)
        body_node = self._msg_factory.mk_episodic_context_report_body(
            mdib_version, sequence_id, states, nsmapper)
        self._send_to_subscribers(subscribers, body_node, action, nsmapper, 'send_episodic_context_report')
        self._do_housekeeping()

    def send_periodic_context_report(self, periodic_states_list, nsmapper, sequence_id):
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

    def send_realtime_samples_report(self, realtime_sample_states, nsmapper, mdib_version, sequence_id):
        action = self.sdc_definitions.Actions.Waveform
        subscribers = self._get_subscriptions_for_action(action)
        if not subscribers:
            return
        self._logger.debug('sending real time samples report {}', realtime_sample_states)
        body_node = self._msg_factory.mk_realtime_samples_report_body(
            mdib_version, sequence_id, realtime_sample_states, nsmapper)
        self._send_to_subscribers(subscribers, body_node, action, nsmapper, None)
        self._do_housekeeping()

    def send_descriptor_updates(self, updated, created, deleted, updated_states, nsmapper, mdib_version, sequence_id):
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
        identifier = reader.read_identifier(request_data.message_data)
        path_suffix = '/'.join(request_data.path_elements)  # not consumed path elements
        dispatch_identifier = _mk_dispatch_identifier(identifier, path_suffix)
        with self._subscriptions.lock:
            subscription = self._subscriptions.dispatch_identifier.get_one(dispatch_identifier, allow_none=True)
        if subscription is None:
            self._logger.error('unknown Subscription identifier "{}"', dispatch_identifier)
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


class SubscriptionsManagerPath(_SubscriptionsManagerBase):
    """This implementation uses path dispatching to identify subscriptions."""

    def _mk_subscription_instance(self, subscribe_request):
        subscription = super()._mk_subscription_instance(subscribe_request)
        subscription.path_suffix = subscription.identifier_uuid.hex
        return subscription


class SubscriptionsManagerReferenceParam(_SubscriptionsManagerBase):
    """This implementation uses reference parameters to identify subscriptions."""

    def _mk_subscription_instance(self, subscribe_request):
        subscription = super()._mk_subscription_instance(subscribe_request)
        # add  a reference parameter
        subscription.reference_parameter = etree_.Element(_DevSubscription.IDENT_TAG)
        subscription.reference_parameter.text = subscription.identifier_uuid.hex
        return subscription
