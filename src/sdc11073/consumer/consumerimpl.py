"""Using lxml based SoapClient."""
from __future__ import annotations

import copy
import functools
import logging
import ssl
import time
import traceback
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

from lxml import etree as etree_

import sdc11073.certloader
from sdc11073 import commlog, loghelper, network, xml_utils
from sdc11073 import observableproperties as properties
from sdc11073.definitions_base import ProtocolsRegistry
from sdc11073.dispatch import DispatchKey, MessageConverterMiddleware
from sdc11073.exceptions import ApiUsageError
from sdc11073.httpserver import compression
from sdc11073.httpserver.httpserverimpl import HttpServerThreadBase
from sdc11073.namespaces import EventingActions
from sdc11073.xml_types import eventing_types, mex_types
from sdc11073.xml_types.addressing_types import HeaderInformationBlock
from sdc11073.xml_types.dpws_types import DeviceEventingFilterDialectURI
from sdc11073.xml_types.wsd_types import ProbeMatchesType, ProbeType

from .components import default_sdc_consumer_components
from .request_handler_deferred import EmptyResponse

if TYPE_CHECKING:
    from collections.abc import Iterable

    from sdc11073.consumer.serviceclients.serviceclientbase import HostedServiceClient
    from sdc11073.consumer.subscription import ConsumerSubscriptionManagerProtocol
    from sdc11073.definitions_base import AbstractDataModel, BaseDefinitions
    from sdc11073.dispatch.request import RequestData
    from sdc11073.mdib.consumermdib import ConsumerMdib
    from sdc11073.pysoap.msgfactory import MessageFactory
    from sdc11073.pysoap.msgreader import MessageReader, ReceivedMessage
    from sdc11073.pysoap.soapclient import SoapClientProtocol
    from sdc11073.wsdiscovery.service import Service
    from sdc11073.xml_types.mex_types import HostedServiceType

    from .components import SdcConsumerComponents
    from .subscription import ConsumerSubscription


class HostedServiceDescription:
    """HostedServiceDescription collects initial structural data from provider."""

    def __init__(self, service_id: str,  # noqa: PLR0913
                 endpoint_address: str,
                 msg_reader: MessageReader,
                 msg_factory: MessageFactory,
                 data_model: AbstractDataModel,
                 log_prefix: str = ''):
        self._endpoint_address = endpoint_address
        self.service_id = service_id
        self._msg_reader = msg_reader
        self.msg_factory = msg_factory
        self._data_model = data_model
        self.log_prefix = log_prefix
        self.meta_data: mex_types.Metadata | None = None
        self.wsdl_string = None
        self.wsdl_node: xml_utils.LxmlElement | None = None
        self._logger = loghelper.get_logger_adapter('sdc.client.hosted', log_prefix)
        self._url = urlparse(endpoint_address)
        self.services = {}

    def read_metadata(self, soap_client: SoapClientProtocol):
        """Read metadata from provider.

        This gives consumer information about offered services and its addresses.
        Members meta_data, wsdl_node and wsdl_string are filled with data from provider.
        """
        payload = mex_types.GetMetadata()
        inf = HeaderInformationBlock(action=payload.action, addr_to=self._endpoint_address)
        created_message = self.msg_factory.mk_soap_message(inf, payload=payload)
        message_data = soap_client.post_message_to(self._url.path,
                                                   created_message,
                                                   msg=f'<{self.service_id}> read_metadata')
        self.meta_data = mex_types.Metadata.from_node(message_data.p_msg.body_node)
        if self.meta_data.wsdl_location is not None:
            self._read_wsdl(soap_client, self.meta_data.wsdl_location)

    def _read_wsdl(self, soap_client: SoapClientProtocol, wsdl_url: str):
        parsed = urlparse(wsdl_url)
        actual_path = parsed.path + f'?{parsed.query}' if parsed.query else parsed.path
        self.wsdl_bytes = soap_client.get_from_url(actual_path, msg=f'{self.log_prefix}:getwsdl')
        try:
            wsdl_element_tree = self._msg_reader.read_wsdl(self.wsdl_bytes)
            self.wsdl_node = wsdl_element_tree.getroot()
            try:
                encoding = wsdl_element_tree.docinfo.encoding or 'UTF-8'
            except AttributeError:
                encoding = 'UTF-8'
            self.wsdl_string = self.wsdl_bytes.decode(encoding)
            logging.getLogger(commlog.WSDL).debug(self.wsdl_string)
        except etree_.XMLSyntaxError as ex:
            self._logger.error(  # noqa: PLE1205
                'could not read wsdl from {}: error={}, data=\n{}', actual_path, ex, self.wsdl_bytes)

    def __repr__(self) -> str:
        return f'{self.__class__.__name__} "{self.service_id}" endpoint = {self._endpoint_address}'


@dataclass(frozen=True)
class SubscriptionEndData:
    """When a subscription end message is received, sdc consumer writes a SubscriptionEndData instance to observable.

    By doing so the application can react to the message.
    """

    subscription: ConsumerSubscription
    request_data: RequestData


class _NotificationsSplitter:
    """Distributes incoming notification to the specific observables."""

    def __init__(self, sdc_consumer: SdcConsumer):
        self._sdc_consumer = sdc_consumer
        self._lookup = self._mk_lookup()

    def on_notification(self, message_data: ReceivedMessage):
        observable_name = self._lookup.get(message_data.action)
        if observable_name is None:
            raise ValueError(f'unknown message {message_data.action}')
        setattr(self._sdc_consumer, observable_name, message_data)

    def _mk_lookup(self) -> dict[str, str]:
        actions = self._sdc_consumer.sdc_definitions.Actions
        return {
            actions.Waveform: 'waveform_report',
            actions.EpisodicMetricReport: 'episodic_metric_report',
            actions.EpisodicAlertReport: 'episodic_alert_report',
            actions.EpisodicComponentReport: 'episodic_component_report',
            actions.EpisodicOperationalStateReport: 'episodic_operational_state_report',
            actions.EpisodicContextReport: 'episodic_context_report',
            actions.PeriodicMetricReport: 'periodic_metric_report',
            actions.PeriodicAlertReport: 'periodic_alert_report',
            actions.PeriodicComponentReport: 'periodic_component_report',
            actions.PeriodicOperationalStateReport: 'periodic_operational_state_report',
            actions.PeriodicContextReport: 'periodic_context_report',
            actions.DescriptionModificationReport: 'description_modification_report',
            actions.OperationInvokedReport: 'operation_invoked_report',
            actions.SystemErrorReport: 'system_error_report',
        }


class SdcConsumer:
    """The SdcConsumer can be used with a known device location.

    The location is typically the result of a wsdiscovery process.
    This class expects that the BICEPS services are available in the device.
    What if not???? => raise exception in _discover_hosted_services.
    """

    subscription_status = properties.ObservableProperty({})
    # indicates whether all subscriptions have been subscribed to and are renewed successfully
    is_connected = properties.ObservableProperty(False)

    # observable properties for all notifications
    # all incoming Notifications can be observed in state_event_report ( as soap envelope)
    state_event_report = properties.ObservableProperty()

    # the following observables can be used to observe the incoming notifications by message type.
    # They contain only the body node of the notification, not the envelope
    waveform_report: ReceivedMessage = properties.ObservableProperty()
    episodic_metric_report: ReceivedMessage = properties.ObservableProperty()
    episodic_alert_report: ReceivedMessage = properties.ObservableProperty()
    episodic_component_report: ReceivedMessage = properties.ObservableProperty()
    episodic_operational_state_report: ReceivedMessage = properties.ObservableProperty()
    episodic_context_report: ReceivedMessage = properties.ObservableProperty()
    periodic_metric_report: ReceivedMessage = properties.ObservableProperty()
    periodic_alert_report: ReceivedMessage = properties.ObservableProperty()
    periodic_component_report: ReceivedMessage = properties.ObservableProperty()
    periodic_operational_state_report: ReceivedMessage = properties.ObservableProperty()
    periodic_context_report: ReceivedMessage = properties.ObservableProperty()
    description_modification_report: ReceivedMessage = properties.ObservableProperty()
    operation_invoked_report: ReceivedMessage = properties.ObservableProperty()
    subscription_end_data: ReceivedMessage = properties.ObservableProperty()
    system_error_report: ReceivedMessage = properties.ObservableProperty()

    SSL_CIPHERS = None  # None : use SSL default

    def __init__(self, device_location: str,  # noqa: PLR0913
                 sdc_definitions: type[BaseDefinitions],
                 ssl_context_container: sdc11073.certloader.SSLContextContainer | None,
                 epr: str | uuid.UUID | None = None,
                 validate: bool = True,
                 log_prefix: str = '',
                 default_components: SdcConsumerComponents | None = None,
                 specific_components: SdcConsumerComponents | None = None,
                 request_chunk_size: int = 0,
                 socket_timeout: int = 5,
                 force_ssl_connect: bool = False,
                 alternative_hostname: str | None = None
                 ):
        """Construct a SdcConsumer.

        :param device_location: the XAddr location for meta data,
                                e.g. http://10.52.219.67:62616/72c08f50-74cc-11e0-8092-027599143341
        :param sdc_definitions: a class derived from BaseDefinitions
        :param epr: the path of this client in http server
        :param ssl_context_container: used for ssl connection to device and for own HTTP Server (notifications receiver)
        :param validate: bool
        :param log_prefix: a string used as prefix for logging
        :param specific_components: a SdcConsumerComponents instance or None
        :param request_chunk_size: if value > 0, message is split into chunks of this size
        :param socket_timeout: timeout for connections to provider
        :param force_ssl_connect: True: only accept ssl connections (requires a ssl_context_container)
                                  False: if ssl_context_container is provided, consumer first tries an
                                         encrypted connection, and if this raises an SSLError,
                                         it tries an unencrypted connection
        :param alternative_hostname: if supplied this hostname is used in xaddr, default is to use numerical
                                     ipv4 address (can be used to use full qualified hostname)
        """
        if not device_location.startswith('http'):
            raise ValueError('Invalid device_location, it must be match http(s)://<netloc> syntax')
        self.is_ssl_connection: bool | None
        if force_ssl_connect:
            if ssl_context_container is None:
                raise ValueError(
                    'Invalid combination of ssl_connect (True) and ssl_context_container (None) parameters')
            self.is_ssl_connection = True
        elif ssl_context_container is None:
            self.is_ssl_connection = False
        else:
            self.is_ssl_connection = None  # options allow both, needs to be decided when connecting

        self._device_location = device_location
        self.sdc_definitions = sdc_definitions
        if default_components is None:
            default_components = default_sdc_consumer_components
        self._components = copy.deepcopy(default_components)
        if specific_components is not None:
            self._components.merge(specific_components)

        self.subscription_status: dict[str, bool] = {}

        # available after start_all
        self._network_adapter: network.NetworkAdapter | None = None

        self.log_prefix = log_prefix
        self.request_chunk_size = request_chunk_size
        self._socket_timeout = socket_timeout
        self._logger = loghelper.get_logger_adapter('sdc.client', self.log_prefix)
        self.host_description: mex_types.Metadata | None = None
        self.hosted_services = {}  # lookup by service id
        self._validate = validate
        self._alternative_hostname = alternative_hostname

        try:
            self._logger.info('Using SSL is enabled. TLS 1.3 Support = {}', ssl.HAS_TLSv1_3)  # noqa: PLE1205
        except AttributeError:
            self._logger.info('Using SSL is enabled. TLS 1.3 is not supported')
        self._ssl_context_container = ssl_context_container
        self._epr = epr or uuid.uuid4()

        self._http_server = None
        self._is_internal_http_server = False

        self._logger.info('created {} for {}', self.__class__.__name__, self._device_location)  # noqa: PLE1205

        self._compression_methods = compression.CompressionHandler.available_encodings[:]
        self._subscription_mgr = None
        self.operations_manager = None
        self._service_clients = {}
        self._mdib = None
        self._soap_clients = {}  # all http connections that this client holds
        self.peer_certificate = None
        self.binary_peer_certificate = None
        self.all_subscribed = False
        # look for schemas added by services and components spec
        additional_schema_specs = set(self._components.additional_schema_specs)
        for handler_cls in self._components.service_handlers:
            additional_schema_specs.update(handler_cls.additional_namespaces)
        msg_reader_cls = self._components.msg_reader_class
        self.msg_reader = msg_reader_cls(self.sdc_definitions,
                                         list(additional_schema_specs),
                                         self._logger,
                                         validate=validate)

        msg_factory_cls = self._components.msg_factory_class
        self.msg_factory = msg_factory_cls(self.sdc_definitions,
                                           list(additional_schema_specs),
                                           self._logger,
                                           validate=validate)

        action_dispatcher_class = self._components.action_dispatcher_class
        self._services_dispatcher = action_dispatcher_class(log_prefix)

        self._notifications_splitter = _NotificationsSplitter(self)

        self._msg_converter = MessageConverterMiddleware(
            self.msg_reader, self.msg_factory, self._logger, self._services_dispatcher)

        # parameters of start_all call, will be set later in start_all
        self._not_subscribed_actions_param: Iterable[str] | None = None
        self._fixed_renew_interval_param: float | None = None
        self._shared_http_server_param: Any | None = None
        self._check_get_service_param: bool | None = None

    def set_mdib(self, mdib: ConsumerMdib | None):
        """SdcConsumer sometimes must know the mdib data (e.g. Set service, activate method)."""
        if mdib is not None and self._mdib is not None:
            raise ApiUsageError('SdcConsumer has already an registered mdib')
        self._mdib = mdib
        if self.client('Set') is not None:
            self.client('Set').register_mdib(mdib)
        if self.client('Context') is not None:
            self.client('Context').register_mdib(mdib)

    @property
    def mdib(self) -> ConsumerMdib | None:
        """Return associated mdib."""
        return self._mdib

    @property
    def network_adapter(self) -> network.NetworkAdapter | None:
        """The network adapter used by this consumer."""
        return self._network_adapter

    @property
    def _epr_urn(self) -> str:
        """Return end point reference, e.g 'urn:uuid:8c26f673-fdbf-4380-b5ad-9e2454a65b6b'."""
        try:
            return self._epr.urn
        except AttributeError:
            return self._epr

    @property
    def path_prefix(self) -> str:
        """Return http path prefix of service e.g '8c26f673fdbf4380b5ad9e2454a65b6b'."""
        try:
            return self._epr.hex
        except AttributeError:
            return self._epr

    @property
    def base_url(self) -> str:
        """Return the base url of consumer.

        Replace servers ip address with own ip address (server might have 0.0.0.0).
        """
        if self._http_server is None:
            return ''
        p = urlparse(self._http_server.base_url)
        tmp = f'{p.scheme}://{self._alternative_hostname or self._network_adapter.ip}:{p.port}{p.path}'
        sep = '' if tmp.endswith('/') else '/'
        tmp = f'{tmp}{sep}{self.path_prefix}/'
        return tmp

    def mk_subscription(self, dpws_hosted: HostedServiceType,
                        filter_type: eventing_types.FilterType,
                        actions: Iterable[DispatchKey]) -> ConsumerSubscription:
        """Create a subscription object and registers it in dispatcher.

        :param dpws_hosted: proxy for the hosted service that provides the events we want to subscribe to
                           This is the target for all subscribe/unsubscribe ... messages
        :param filter_type: the filter that is sent to device
        :param actions: a list of DispatchKey that this subscription shall handle.
        :return: a subscription object.
        """
        subscription = self._subscription_mgr.mk_subscription(dpws_hosted, filter_type)

        def update_subscription_status(subscription_filter: str, status: bool):
            subscription_status = dict(self.subscription_status)
            subscription_status[subscription_filter] = status
            self.subscription_status = subscription_status  # trigger observable if status has changed

        properties.strongbind(subscription, is_subscribed=functools.partial(update_subscription_status,
                                                                            filter_type.text))

        # direct subscribed notifications to this subscription
        for action in actions:
            self._services_dispatcher.register_post_handler(action, subscription.on_notification)
        return subscription

    def do_subscribe(self, dpws_hosted: HostedServiceType,  # noqa: PLR0913
                     filter_type: eventing_types.FilterType,
                     actions: Iterable[DispatchKey],
                     expire_minutes: int = 60,
                     any_elements: list[xml_utils.LxmlElement] | None = None,
                     any_attributes: dict | None = None) -> ConsumerSubscription:
        """Send subscribe request to provider.

        :param dpws_hosted: proxy for the hosted service that provides the events we want to subscribe to
                           This is the target for all subscribe/unsubscribe ... messages
        :param filter_type: the filter that is sent to device
        :param actions: a list of DispatchKeys that this subscription shall handle
        :param expire_minutes: defaults to 1 hour
        :param any_elements: optional list of lxml elements
        :param any_attributes: optional dictionary of name:str - value:str pairs
        :return: a subscription object that has callback already registered.
        """
        subscription = self.mk_subscription(dpws_hosted, filter_type, actions)
        properties.bind(subscription, notification_data=self._on_notification)
        subscription.subscribe(expire_minutes, any_elements, any_attributes)
        return subscription

    def client(self, port_type_name: str) -> HostedServiceClient | None:
        """Return the client for the given port type name.

        WDP and SDC use different port type names, e.g. WPF="Get", SDC="GetService".
        If the port type is not found directly, it tries also with or without "Service" in name.
        :param port_type_name: string, e.g "Get", or "GetService", ...
        """
        client = self._service_clients.get(port_type_name)
        if client is None and port_type_name.endswith('Service'):
            client = self._service_clients.get(port_type_name[:-7])
        if client is None and not port_type_name.endswith('Service'):
            client = self._service_clients.get(port_type_name + "Service")
        return client

    @property
    def get_service_client(self) -> HostedServiceClient:
        """Return a HostedServiceClient."""
        return self.client('GetService')

    @property
    def set_service_client(self) -> HostedServiceClient:
        """Return a HostedServiceClient."""
        return self.client('SetService')

    @property
    def description_event_service_client(self) -> HostedServiceClient:
        """Return a HostedServiceClient."""
        return self.client('DescriptionEventService')

    @property
    def state_event_service_client(self) -> HostedServiceClient:
        """Return a HostedServiceClient."""
        return self.client('StateEventService')

    @property
    def context_service_client(self) -> HostedServiceClient:
        """Return a HostedServiceClient."""
        return self.client('ContextService')

    @property
    def waveform_service_client(self) -> HostedServiceClient:
        """Return a HostedServiceClient."""
        return self.client('Waveform')

    @property
    def containment_tree_service_client(self) -> HostedServiceClient:
        """Return a HostedServiceClient."""
        return self.client('ContainmentTreeService')

    @property
    def archive_service_client(self) -> HostedServiceClient:
        """Return a HostedServiceClient."""
        return self.client('ArchiveService')

    @property
    def localization_service_client(self) -> HostedServiceClient:
        """Return a HostedServiceClient."""
        return self.client('LocalizationService')

    @property
    def subscription_mgr(self) -> ConsumerSubscriptionManagerProtocol:
        """Return the subscription manager."""
        return self._subscription_mgr

    def start_all(self, not_subscribed_actions: Iterable[str] | None = None,
                  fixed_renew_interval: float | None = None,
                  shared_http_server: Any | None = None,
                  check_get_service: bool = True) -> None:
        """Start background threads, read metadata from device, instantiate detected port type clients and subscribe.

        :param not_subscribed_actions: a list of pmtypes.Actions elements or None. if None, everything is subscribed.
        :param fixed_renew_interval: an interval in seconds or None
                    if None, renew is sent when remaining time <= 50% of granted time
                    if set, subscription renew is sent in this interval.
        :param shared_http_server: if provided, use this http server, else client creates its own.
        :param check_get_service: if True (default) it checks that a GetService is detected,
               which is the minimal requirement for a sdc provider.
        :return: None
        """
        self._not_subscribed_actions_param = not_subscribed_actions
        self._fixed_renew_interval_param = fixed_renew_interval
        self._shared_http_server_param = shared_http_server
        self._check_get_service_param = check_get_service
        self._logger.debug('connecting to %s', self._device_location)
        self._connect()
        self._logger.debug('reading meta data from %s', self._device_location)
        self.host_description = self._get_metadata()

        # now query also metadata of hosted services
        self._mk_hosted_services(self.host_description)
        self._logger.debug('Services: {}', self._service_clients.keys())  # noqa: PLE1205

        used_ip = self.get_soap_client(self._device_location).sock_name[0]
        self._network_adapter = network.get_adapter_containing_ip(used_ip)
        self._logger.info('SdcConsumer for {} uses network adapter {}',  # noqa: PLE1205
                          self._device_location,
                          self._network_adapter)

        # only GetService is mandatory!!!
        if check_get_service and self.get_service_client is None:
            raise RuntimeError(f'GetService not detected! found services = {list(self._service_clients.keys())}')

        self._start_event_sink(shared_http_server)

        # start subscription manager
        subscription_manager_class = self._components.subscription_manager_class
        self._subscription_mgr = subscription_manager_class(self.msg_reader,
                                                            self.msg_factory,
                                                            self.sdc_definitions.data_model,
                                                            self.get_soap_client,
                                                            self.base_url,
                                                            log_prefix=self.log_prefix,
                                                            fixed_renew_interval=fixed_renew_interval)
        self._subscription_mgr.start()

        # flag 'self.all_subscribed' tells mdib that mdib state versions shall not have any gaps
        # => log warnings for missing versions
        self.all_subscribed = True
        not_subscribed_actions_set = set() if not_subscribed_actions is None else set(not_subscribed_actions)
        if not_subscribed_actions:
            not_subscribed_episodic_actions = [a for a in not_subscribed_actions
                                               if ("Episodic" in a or "DescriptionModificationReport" in a)]
            if not_subscribed_episodic_actions:
                self.all_subscribed = False

        # start operationInvoked subscription and tell all
        operations_manager_class = self._components.operations_manager_class
        self.operations_manager = operations_manager_class(self.msg_reader, self.log_prefix)
        properties.bind(self, operation_invoked_report=self.operations_manager.on_operation_invoked_report)
        for client in self._service_clients.values():
            client.set_operations_manager(self.operations_manager)

        # start all subscriptions
        # group subscriptions per hosted service
        for dpws_hosted in self.host_description.relationship.Hosted:
            available_actions: list[DispatchKey] = []
            if dpws_hosted.Types is not None:
                for port_type_qname in dpws_hosted.Types:
                    client = self.client(port_type_qname.localname)
                    if client is not None:
                        available_actions.extend(client.get_available_subscriptions())
            if len(available_actions) > 0:
                subscribe_actions = {a for a in available_actions if a.action not in not_subscribed_actions_set}
                if len(subscribe_actions) > 0:
                    filter_type = eventing_types.FilterType()
                    filter_type.text = ' '.join(x.action for x in subscribe_actions)
                    filter_type.Dialect = DeviceEventingFilterDialectURI.ACTION
                    try:
                        self.do_subscribe(dpws_hosted, filter_type, subscribe_actions)
                    except Exception:  # noqa: BLE001
                        self.all_subscribed = False  # => don't log errors when mdib versions are missing
                        self._logger.error('start_all: could not subscribe: error = {}, actions= {}',  # noqa: PLE1205
                                           traceback.format_exc(), subscribe_actions)

        def _update_is_connected(subscription_status: dict[str, bool]):
            self.is_connected = all(subscription_status.values()) and any(subscription_status)

        properties.strongbind(self, subscription_status=_update_is_connected)
        _update_is_connected(self.subscription_status)

        # register callback for end of subscription
        self._services_dispatcher.register_post_handler(
            DispatchKey(EventingActions.SubscriptionEnd,
                        self.sdc_definitions.data_model.ns_helper.WSE.tag('SubscriptionEnd')),
            self._on_subscription_end)

    def stop_all(self, unsubscribe: bool = True):
        """Stop all threads, optionally unsubscribe."""
        if self._subscription_mgr is not None:
            if unsubscribe:
                self._subscription_mgr.unsubscribe_all()
            self._subscription_mgr.stop()
        self.set_mdib(None)

        for client in self._soap_clients.values():
            client.close()
        self._soap_clients = {}
        self._stop_event_sink()

    def restart(self):
        """forget existing data and restart from the beginning."""
        mdib = self._mdib  # keep existing mdib connection
        self.stop_all()  # with unsubscribe
        # start with the same parameters as initially
        self.start_all(self._not_subscribed_actions_param,
                       self._fixed_renew_interval_param,
                       self._shared_http_server_param,
                       self._check_get_service_param)
        self.set_mdib(mdib)

    def set_used_compression(self, *compression_methods: str):
        """Use only one of these compression methods."""
        del self._compression_methods[:]
        self._compression_methods.extend(compression_methods)

    def _connect(self):
        soap_client = self.get_soap_client(self._device_location)
        if self.is_ssl_connection is not None:
            # decision was already made in constructor
            soap_client.connect()
        else:
            try:
                soap_client.connect()
                self.is_ssl_connection = True
            except ssl.SSLError:
                # could not connect with ssl, try without it
                soap_client.close()
                self._forget_soap_client(soap_client)
                self.is_ssl_connection = False
                soap_client = self.get_soap_client(self._device_location)
                # if this also fails, something else is wrong and error needs handling on application level.
                soap_client.connect()
        if self.is_ssl_connection:
            sock = soap_client.sock
            self.peer_certificate = sock.getpeercert(binary_form=False)
            self.binary_peer_certificate = sock.getpeercert(binary_form=True)  # in case the application needs it...
            self._logger.info('Peer Certificate: {}', self.peer_certificate)  # noqa: PLE1205

    def _get_metadata(self) -> mex_types.Metadata:
        _url = urlparse(self._device_location)
        soap_client = self.get_soap_client(self._device_location)
        nsh = self.sdc_definitions.data_model.ns_helper
        inf = HeaderInformationBlock(action=f'{nsh.WXF.namespace}/Get',
                                     addr_to=self._device_location)
        message = self.msg_factory.mk_soap_message_etree_payload(inf, payload_element=None)

        received_message_data = soap_client.post_message_to(_url.path, message, msg='getMetadata')
        return mex_types.Metadata.from_node(received_message_data.p_msg.body_node)

    def send_probe(self) -> ProbeMatchesType:
        """Send Probe directly to provider."""
        _url = urlparse(self._device_location)
        wsc = self.get_soap_client(self._device_location)
        probe = ProbeType()
        inf = HeaderInformationBlock(action=probe.action,
                                     addr_to=self._device_location)

        message = self.msg_factory.mk_soap_message(inf, payload=probe)
        received_message_data = wsc.post_message_to(_url.path, message, msg='Probe')
        return ProbeMatchesType.from_node(received_message_data.p_msg.msg_node)

    def get_soap_client(self, address: str) -> SoapClientProtocol:
        """Return the soap client for address.

        Method creates a new soap client if needed and considers self.is_ssl_connection value.
        """
        _url = urlparse(address)
        use_ssl = self.is_ssl_connection is not False  # if is_ssl_connection is still None, default to use_ssl = True
        key = (use_ssl, _url.netloc)
        soap_client = self._soap_clients.get(key)
        if soap_client is None:
            soap_client = self._mk_soap_client(use_ssl, _url.netloc)
            self._soap_clients[key] = soap_client
        return soap_client

    def _forget_soap_client(self, soap_client: SoapClientProtocol):
        for key, value in self._soap_clients.items():
            if value is soap_client:
                del self._soap_clients[key]
                return

    def _mk_soap_client(self, use_ssl: bool, netloc: str) -> SoapClientProtocol:
        _ssl_context = self._ssl_context_container.client_context if use_ssl else None
        cls = self._components.soap_client_class
        return cls(netloc,
                   self._socket_timeout,
                   loghelper.get_logger_adapter('sdc.client.soap', self.log_prefix),
                   ssl_context=_ssl_context,
                   sdc_definitions=self.sdc_definitions,
                   msg_reader=self.msg_reader,
                   supported_encodings=self._compression_methods,
                   chunk_size=self.request_chunk_size)

    def _mk_hosted_services(self, host_description: mex_types.Metadata):
        for hosted in host_description.relationship.Hosted:
            address = hosted.EndpointReference[0].Address
            soap_client = self.get_soap_client(address)
            h_descr = HostedServiceDescription(
                hosted.ServiceId, address,
                self.msg_reader, self.msg_factory, self.sdc_definitions.data_model, self.log_prefix)
            self.hosted_services[hosted.ServiceId] = h_descr
            h_descr.read_metadata(soap_client)
            for port_type in hosted.Types:
                hosted_service_client = self._mk_hosted_service_client(port_type,
                                                                       soap_client,
                                                                       hosted)
                if hosted_service_client is not None:
                    self._service_clients[port_type.localname] = hosted_service_client
                    h_descr.services[port_type.localname] = hosted_service_client
                else:
                    self._logger.warning('Unknown port type {}', str(port_type))  # noqa: PLE1205

    def _mk_hosted_service_client(self, port_type: str,
                                  soap_client: SoapClientProtocol,
                                  hosted: HostedServiceType) -> HostedServiceClient | None:
        for cls in self._components.service_handlers:
            if cls.port_type_name == port_type:
                return cls(self, soap_client, hosted, port_type)
        return None

    def _start_event_sink(self, shared_http_server: Any):
        if shared_http_server is None:
            self._is_internal_http_server = True
            ssl_context_container = self._ssl_context_container if self.is_ssl_connection else None
            logger = loghelper.get_logger_adapter('sdc.client.notif_dispatch', self.log_prefix)
            self._http_server = HttpServerThreadBase(
                str(self._network_adapter.ip),
                ssl_context_container.server_context if ssl_context_container else None,
                logger=logger,
                supported_encodings=self._compression_methods,
            )
            self._http_server.start()
            self._http_server.started_evt.wait(timeout=5)
            # it sometimes still happens that http server is not completely started without waiting.
            # TODO: find better solution, see issue #320
            time.sleep(1)
            self._logger.info('serving EventSink on {}', self._http_server.base_url)  # noqa: PLE1205
        else:
            self._http_server = shared_http_server
        # register own epr in http server
        self._http_server.dispatcher.register_instance(self.path_prefix, self._msg_converter)

    def _stop_event_sink(self):
        if self._is_internal_http_server and self._http_server is not None:
            self._http_server.stop()

    def _on_notification(self, message_data: ReceivedMessage):
        self.state_event_report = message_data  # update observable
        self._notifications_splitter.on_notification(message_data)

    def _on_subscription_end(self, request_data: RequestData) -> EmptyResponse:
        subscription = self._subscription_mgr.on_subscription_end(request_data)  # subscription can be None
        self.subscription_end_data = SubscriptionEndData(subscription, request_data)
        return EmptyResponse()

    def __str__(self) -> str:
        return f'SdcConsumer to {self.host_description.this_device} {self.host_description.this_model} on {self._device_location}'

    @classmethod
    def from_wsd_service(cls, wsd_service: Service,  # noqa: PLR0913
                         ssl_context_container: sdc11073.certloader.SSLContextContainer | None,
                         validate: bool = True,
                         log_prefix: str = '',
                         default_components: SdcConsumerComponents | None = None,
                         specific_components: SdcConsumerComponents | None = None):
        """Construct a SdcConsumer from a Service.

        :param wsd_service: a wsdiscovery.Service instance
        :param ssl_context_container: a ssl context or None
        :param validate: bool
        :param log_prefix: a string
        :param default_components: a SdcConsumerComponents instance or None
        :param specific_components: a SdcConsumerComponents instance or None
        :return:
        """
        device_locations = wsd_service.x_addrs
        if not device_locations:
            raise RuntimeError(f'discovered Service has no address!{wsd_service}')
        device_location = device_locations[0]
        for sdc_definition in ProtocolsRegistry.protocols:
            if sdc_definition.types_match(wsd_service.types):
                return cls(device_location, sdc_definition, ssl_context_container, validate=validate,
                           log_prefix=log_prefix,
                           default_components=default_components, specific_components=specific_components)
        raise RuntimeError('no matching protocol definition found for this service!')
