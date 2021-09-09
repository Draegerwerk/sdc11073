""" Using lxml based SoapClient"""
import copy
import functools
import ssl
import traceback
import urllib
import weakref

from lxml import etree as etree_

from .. import commlog
from .. import compression
from .. import loghelper
from .. import netconn
from .. import observableproperties as properties
from ..definitions_base import ProtocolsRegistry, SchemaValidators
from ..namespaces import Prefixes, msgTag
from ..namespaces import nsmap
from ..pysoap.soapclient import SoapClient
from ..pysoap.soapenvelope import WsAddress, Soap12Envelope, DPWSEnvelope, MetaDataSection


def _mk_soap_client(scheme, netloc, logger, ssl_context, sdc_definitions, supported_encodings=None,
                    request_encodings=None, chunked_requests=False):
    if scheme == 'https':
        _ssl_context = ssl_context
    else:
        _ssl_context = None
    return SoapClient(netloc, logger, ssl_context=_ssl_context,
                      sdc_definitions=sdc_definitions,
                      supported_encodings=supported_encodings,
                      request_encodings=request_encodings,
                      chunked_requests=chunked_requests)


class HostDescription:
    def __init__(self, dpws_envelope):
        self._dpws_envelope = dpws_envelope
        self.this_model = dpws_envelope.this_model
        self.this_device = dpws_envelope.this_device
        self.host = dpws_envelope.host

    def __str__(self):
        return 'HostDescription: this_model = {}, this_device = {}, host = {}'.format(self.this_model, self.this_device,
                                                                                      self.host)


class HostedServiceDescription:
    VALIDATE_MEX = False  # workaraound as long as validation error due to missing dpws schema is not solved

    def __init__(self, service_id, endpoint_address, validate, biceps_schema, msg_factory, log_prefix=''):
        self._endpoint_address = endpoint_address
        self.service_id = service_id
        self._validate = validate
        self._biceps_schema = biceps_schema
        self._msg_factory = msg_factory
        self.log_prefix = log_prefix
        self.metadata = None
        self.wsdl_string = None
        self.wsdl = None
        self._logger = loghelper.get_logger_adapter('sdc.client.{}'.format(service_id), log_prefix)
        self._url = urllib.parse.urlparse(endpoint_address)
        self.services = {}

    @property
    def _mex_schema(self):
        return None if not self._validate else self._biceps_schema.mex_schema

    def read_metadata(self, soap_client):
        soap_envelope = self._msg_factory.mk_getmetadata_envelope(self._endpoint_address)
        if self.VALIDATE_MEX:
            soap_envelope.validate_body(self._mex_schema)
        endpoint_envelope = soap_client.post_soap_envelope_to(self._url.path,
                                                              soap_envelope,
                                                              msg='<{}> read_metadata'.format(self.service_id))
        if self.VALIDATE_MEX:
            endpoint_envelope.validate_body(self._mex_schema)
        self.metadata = MetaDataSection.from_etree_node(endpoint_envelope.body_node)
        self._read_wsdl(soap_client, self.metadata.wsdl_location)

    def _read_wsdl(self, soap_client, wsdl_url):
        parsed = urllib.parse.urlparse(wsdl_url)
        actual_path = parsed.path + '?{}'.format(parsed.query) if parsed.query else parsed.path
        self.wsdl_string = soap_client.get_url(actual_path, msg='{}:getwsdl'.format(self.log_prefix))
        commlog.get_communication_logger().log_wsdl(self.wsdl_string, self.service_id)
        try:
            self.wsdl = etree_.fromstring(self.wsdl_string, parser=etree_.ETCompatXMLParser(
                resolve_entities=False))  # make am ElementTree instance
        except etree_.XMLSyntaxError as ex:
            self._logger.error(
                'could not read wsdl from {}: error={}, data=\n{}'.format(actual_path, ex, self.wsdl_string))

    def __repr__(self):
        return '{} "{}" endpoint = {}'.format(self.__class__.__name__, self.service_id, self._endpoint_address)


def ip_addr_to_int(ip_string):
    """ Convert string like '192.168.0.1' to an integer
    (helper for sort_ip_addresses)"""
    return functools.reduce(lambda x, y: x * 256 + y, (int(x) for x in ip_string.split('.')), 0)


def _cmp(address_a, address_bb, _ref_int):
    """ helper for sort_ip_addresses"""
    _a_abs = abs(ip_addr_to_int(address_a) - _ref_int)
    _b_abs = abs(ip_addr_to_int(address_bb) - _ref_int)
    diff = _a_abs - _b_abs
    if diff < 0:
        return -1
    if diff > 0:
        return 1
    return 0


def sort_ip_addresses(adresses, ref_ip):
    """ sorts list addresses by distance to refIP, shortest distance first"""
    _ref = ip_addr_to_int(ref_ip)
    adresses.sort(key=lambda a: abs(ip_addr_to_int(a) - _ref))
    return adresses


class NotificationsDispatcherBase:
    def __init__(self, sdc_client, logger):
        self._sdc_client = sdc_client
        self._logger = logger
        self._lookup = self._mk_lookup()

    def _mk_lookup(self):
        raise NotImplementedError

    def on_notification(self, envelope):
        self._sdc_client.state_event_report = envelope  # update observable

    def _on_operation_invoked_report(self, envelope):
        ret = self._sdc_client.operations_manager.on_operation_invoked_report(envelope)
        report = envelope.body_node.xpath('msg:OperationInvokedReport', namespaces=nsmap)
        self._sdc_client.operation_invoked_report = report[0]  # update observable
        return ret

    def _on_waveform_report(self, envelope):
        try:
            waveform_node = envelope.body_node[0]  # the msg:WaveformStreamReport node
        except IndexError:
            waveform_node = None

        if waveform_node is None:
            self._logger.error('WaveformStream does not contain msg:WaveformStream!', envelope)

        self._sdc_client.waveform_report = waveform_node  # update observable

    def _on_episodic_metric_report(self, envelope):
        report = self._get_report(envelope, 'EpisodicMetricReport')
        if report is not None:
            self._sdc_client.episodic_metric_report = report

    def _on_periodic_metric_report(self, envelope):
        report = self._get_report(envelope, 'PeriodicMetricReport')
        if report is not None:
            self._sdc_client.periodic_metric_report = report

    def _on_episodic_alert_report(self, envelope):
        report = self._get_report(envelope, 'EpisodicAlertReport')
        if report is not None:
            self._sdc_client.episodic_alert_report = report

    def _on_periodic_alert_report(self, envelope):
        report = self._get_report(envelope, 'PeriodicAlertReport')
        if report is not None:
            self._sdc_client.periodic_alert_report = report

    def _on_episodic_component_report(self, envelope):
        report = self._get_report(envelope, 'EpisodicComponentReport')
        if report is not None:
            self._sdc_client.episodic_component_report = report

    def _on_periodic_component_report(self, envelope):
        report = self._get_report(envelope, 'PeriodicComponentReport')
        if report is not None:
            self._sdc_client.periodic_component_report = report

    def _on_episodic_operational_state_report(self, envelope):
        report = self._get_report(envelope, 'EpisodicOperationalStateReport')
        if report is not None:
            self._sdc_client.episodic_operational_state_report = report

    def _on_periodic_operational_state_report(self, envelope):
        report = self._get_report(envelope, 'PeriodicOperationalStateReport')
        if report is not None:
            self._sdc_client.periodic_operational_state_report = report

    def _on_episodic_context_report(self, envelope):
        report = self._get_report(envelope, 'EpisodicContextReport')
        if report is not None:
            self._sdc_client.episodic_context_report = report

    def _on_periodic_context_report(self, envelope):
        report = self._get_report(envelope, 'PeriodicContextReport')
        if report is not None:
            self._sdc_client.periodic_context_report = report

    def _on_description_report(self, envelope):
        report = self._get_report(envelope, 'DescriptionModificationReport')
        if report is not None:
            self._sdc_client.description_modification_report = report

    def _get_report(self, envelope, name):
        reports = envelope.body_node.xpath(f'msg:{name}', namespaces=nsmap)
        if len(reports) == 1:
            self._logger.debug('_get_report {}', name)
            return reports[0]
        if len(reports) > 1:
            self._logger.error('report contains {} elements of msg:{}!', len(reports), name)
        else:
            self._logger.error('report does not contain msg:{}!', name)
        return None


class NotificationsDispatcherByBody(NotificationsDispatcherBase):
    def _mk_lookup(self):
        return {
            msgTag('EpisodicMetricReport'): self._on_episodic_metric_report,
            msgTag('EpisodicAlertReport'): self._on_episodic_alert_report,
            msgTag('EpisodicComponentReport'): self._on_episodic_component_report,
            msgTag('EpisodicOperationalStateReport'): self._on_episodic_operational_state_report,
            msgTag('WaveformStream'): self._on_waveform_report,
            msgTag('OperationInvokedReport'): self._on_operation_invoked_report,
            msgTag('EpisodicContextReport'): self._on_episodic_context_report,
            msgTag('DescriptionModificationReport'): self._on_description_report,
            msgTag('PeriodicMetricReport'): self._on_periodic_metric_report,
            msgTag('PeriodicAlertReport'): self._on_periodic_alert_report,
            msgTag('PeriodicComponentReport'): self._on_periodic_component_report,
            msgTag('PeriodicOperationalStateReport'): self._on_periodic_operational_state_report,
            msgTag('PeriodicContextReport'): self._on_periodic_context_report,
        }

    def on_notification(self, envelope):
        """ dispatch by message body"""
        super().on_notification(envelope)
        message = envelope.body_node[0].tag
        q_name = etree_.QName(message)
        method = self._lookup.get(q_name)
        if method is None:
            raise RuntimeError('unknown message {}'.format(q_name))
        method(envelope)


class NotificationsDispatcherByAction(NotificationsDispatcherBase):
    def _mk_lookup(self):
        actions = self._sdc_client.sdc_definitions.Actions

        return {
            actions.EpisodicMetricReport: self._on_episodic_metric_report,
            actions.EpisodicAlertReport: self._on_episodic_alert_report,
            actions.EpisodicComponentReport: self._on_episodic_component_report,
            actions.EpisodicOperationalStateReport: self._on_episodic_operational_state_report,
            actions.Waveform: self._on_waveform_report,
            actions.OperationInvokedReport: self._on_operation_invoked_report,
            actions.EpisodicContextReport: self._on_episodic_context_report,
            actions.DescriptionModificationReport: self._on_description_report,
            actions.PeriodicMetricReport: self._on_periodic_metric_report,
            actions.PeriodicAlertReport: self._on_periodic_alert_report,
            actions.PeriodicComponentReport: self._on_periodic_component_report,
            actions.PeriodicOperationalStateReport: self._on_periodic_operational_state_report,
            actions.PeriodicContextReport: self._on_periodic_context_report,
        }

    def on_notification(self, envelope):
        """ dispatch by message body"""
        super().on_notification(envelope)
        action = envelope.address.action
        method = self._lookup.get(action)
        if method is None:
            raise RuntimeError('unknown message {}'.format(action))
        method(envelope)


class SdcClient:
    """ The SdcClient can be used with a known device location.
    The location is typically the result of a wsdiscovery process.
    This class expects that the BICEPS services are available in the device.
    What if not???? => raise exception in _discover_hosted_services
    """
    is_connected = properties.ObservableProperty(False)  # a boolean

    # observable properties for all notifications
    # all incoming Notifications can be observed in state_event_report ( as soap envelope)
    state_event_report = properties.ObservableProperty()

    # the following observables can be used to observe the incoming notifications by message type.
    # They contain only the body node of the notification, not the envelope
    waveform_report = properties.ObservableProperty()
    episodic_metric_report = properties.ObservableProperty()
    episodic_alert_report = properties.ObservableProperty()
    episodic_component_report = properties.ObservableProperty()
    episodic_operational_state_report = properties.ObservableProperty()
    episodic_context_report = properties.ObservableProperty()
    periodic_metric_report = properties.ObservableProperty()
    periodic_alert_report = properties.ObservableProperty()
    periodic_component_report = properties.ObservableProperty()
    periodic_operational_state_report = properties.ObservableProperty()
    periodic_context_report = properties.ObservableProperty()
    description_modification_report = properties.ObservableProperty()
    operation_invoked_report = properties.ObservableProperty()

    SSL_CIPHERS = None  # None : use SSL default

    def __init__(self, device_location, sdc_definitions, ssl_context, validate=True,
                 log_prefix='',
                 specific_components=None,
                 chunked_requests=False):  # pylint:disable=too-many-arguments
        """
        :param device_location: the XAddr location for meta data, e.g. http://10.52.219.67:62616/72c08f50-74cc-11e0-8092-027599143341
        :param sdc_definitions: a class derived from BaseDefinitions
        :param ssl_context: used for ssl connection to device and for own HTTP Server (notifications receiver)
             If value is None, best own address is determined automatically (recommended).
        :param validate: bool
        :param log_prefix: a string used as prefix for logging
        :param specific_components: a SdcClientComponents instance or None
        :param chunked_requests: bool
        """
        if not device_location.startswith('http'):
            raise ValueError('Invalid device_location, it must be match http(s)://<netloc> syntax')
        self._device_location = device_location
        self.sdc_definitions = sdc_definitions
        self._components = copy.deepcopy(sdc_definitions.DefaultSdcClientComponents)
        if specific_components is not None:
            self._components.merge(specific_components)
        self._biceps_schema = SchemaValidators(self.sdc_definitions)
        splitted = urllib.parse.urlsplit(self._device_location)
        self._device_uses_https = splitted.scheme.lower() == 'https'

        self.log_prefix = log_prefix
        self.chunked_requests = chunked_requests
        self._logger = loghelper.get_logger_adapter('sdc.client', self.log_prefix)
        # self._logger_wf = loghelper.get_logger_adapter('sdc.client.wf', self.log_prefix)  # waveform logger
        self._my_ipaddress = self._find_best_own_ip_address()
        self._logger.info('SdcClient for {} uses own IP Address {}', self._device_location, self._my_ipaddress)
        self.metadata = None
        self.host_description = None
        self.hosted_services = {}  # lookup by service id
        self._validate = validate
        try:
            self._logger.info('Using SSL is enabled. TLS 1.3 Support = {}', ssl.HAS_TLSv1_3)
        except AttributeError:
            self._logger.info('Using SSL is enabled. TLS 1.3 is not supported')
        self._ssl_context = ssl_context
        self._notifications_dispatcher_thread = None

        self._logger.info('created {} for {}', self.__class__.__name__, self._device_location)

        self._compression_methods = compression.CompressionHandler.available_encodings[:]
        self._subscription_mgr = None
        self.operations_manager = None
        self._service_clients = {}
        self._mdib = None
        self._soap_clients = {}  # all http connections that this client holds
        self.peer_certificate = None
        self.binary_peer_certificate = None
        self.all_subscribed = False
        msg_reader_cls = self._components.msg_reader_class
        self.msg_reader = msg_reader_cls(self._logger, 'msg_reader')
        msg_factory_cls = self._components.msg_factory_class
        self._msg_factory = msg_factory_cls(self.sdc_definitions, self._logger)
        notifications_dispatcher_cls = self._components.notifications_dispatcher_class
        self._notifications_dispatcher = notifications_dispatcher_cls(self, self._logger)

    def _register_mdib(self, mdib):
        """ SdcClient sometimes must know the mdib data (e.g. Set service, activate method)."""
        if mdib is not None and self._mdib is not None:
            raise RuntimeError('SdcClient has already an registered mdib')
        self._mdib = None if mdib is None else weakref.ref(mdib)
        if mdib is not None:
            mdib.biceps_schema = self._biceps_schema
        if self.client('Set') is not None:
            self.client('Set').register_mdib(mdib)
        if self.client('Context') is not None:
            self.client('Context').register_mdib(mdib)
        self._msg_factory.register_mdib(mdib)

    @property
    def mdib(self):
        return self._mdib()

    @property
    def my_ipaddress(self):
        return self._my_ipaddress

    def _find_best_own_ip_address(self):
        my_addresses = [conn.ip for conn in netconn.get_network_adapter_configs() if conn.ip not in (None, '0.0.0.0')]
        splitted = urllib.parse.urlsplit(self._device_location)
        device_addr = splitted.hostname
        if device_addr is None:
            device_addr = splitted.netloc.split(':')[0]  # without port
        sort_ip_addresses(my_addresses, device_addr)
        return my_addresses[0]

    def _subscribe(self, dpws_hosted, actions, callback):
        """ creates a subscription object and registers it in
        :param dpws_hosted: proxy for the hosted service that provides the events we want to subscribe to
                           This is the target for all subscribe/unsubscribe ... messages
        :param actions: a list of filters. this (joined) string is sent to the sdc server in the Subscribe message
        :param callback: callable with signature callback(soapEnvlope)
        @return: a subscription object that has callback already registerd
        """
        subscription = self._subscription_mgr.mk_subscription(dpws_hosted, actions)
        for action in actions:
            self._notifications_dispatcher_thread.dispatcher.register_function(action, subscription.on_notification)
        if callback is not None:
            properties.bind(subscription, notification=callback)
        subscription.subscribe()
        return subscription

    def client(self, port_type_name):
        """ returns the client for the given port type name.
        WDP and SDC use different port type names, e.g WPF="Get", SDC="GetService".
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
    def get_service_client(self):
        return self.client('GetService')

    @property
    def set_service_client(self):
        return self.client('SetService')

    @property
    def description_event_service_client(self):
        return self.client('DescriptionEventService')

    @property
    def state_event_service_client(self):
        return self.client('StateEventService')

    @property
    def context_service_client(self):
        return self.client('ContextService')

    @property
    def waveform_service_client(self):
        return self.client('Waveform')

    @property
    def containment_tree_service_client(self):
        return self.client('ContainmentTreeService')

    @property
    def archive_service_client(self):
        return self.client('ArchiveService')

    @property
    def localization_service_client(self):
        return self.client('LocalizationService')

    def start_all(self, not_subscribed_actions=None, subscriptions_check_interval=None, async_dispatch=True,
                  subscribe_periodic_reports=False):
        """
        :param not_subscribed_actions: a list of pmtypes.Actions elements or None. if None, everything is subscribed.
        :param subscriptions_check_interval: an interval in seconds or None
        :param async_dispatch: if True, incoming requests are queued and response is sent immediately (processing is done later).
                                if False, response is sent after the complete processing is done.
        :return: None
        """
        self._discover_hosted_services()
        self._start_event_sink(async_dispatch)
        periodic_actions = {self.sdc_definitions.Actions.PeriodicMetricReport,
                            self.sdc_definitions.Actions.PeriodicAlertReport,
                            self.sdc_definitions.Actions.PeriodicComponentReport,
                            self.sdc_definitions.Actions.PeriodicContextReport,
                            self.sdc_definitions.Actions.PeriodicOperationalStateReport}

        # start subscription manager
        subscription_manager_class = self._components.subscription_manager_class
        self._subscription_mgr = subscription_manager_class(self._msg_factory,
                                                            self._notifications_dispatcher_thread.base_url,
                                                            log_prefix=self.log_prefix,
                                                            check_interval=subscriptions_check_interval)
        self._subscription_mgr.start()

        # flag 'self.all_subscribed' tells mdib that mdib state versions shall not have any gaps
        # => log warnings for missing versions
        self.all_subscribed = True
        not_subscribed_actions_set = set([])
        if not_subscribed_actions:
            not_subscribed_episodic_actions = [a for a in not_subscribed_actions if not 'Periodic' in a]
            if not_subscribed_episodic_actions:
                self.all_subscribed = False
                not_subscribed_actions_set = set(not_subscribed_actions)

        # start operationInvoked subscription and tell all
        operations_manager_class = self._components.operations_manager_class
        self.operations_manager = operations_manager_class(self.log_prefix)

        for client in self._service_clients.values():
            client.set_operations_manager(self.operations_manager)

        # start all subscriptions
        # group subscriptions per hosted service
        for service_id, dpws_hosted in self.metadata.hosted.items():
            available_actions = []
            for port_type_qname in dpws_hosted.types:
                port_type = port_type_qname.split(':')[-1]
                client = self.client(port_type)
                if client is not None:
                    available_actions.extend(client.get_subscribable_actions())
            if len(available_actions) > 0:
                subscribe_actions = set(available_actions) - not_subscribed_actions_set
                if not subscribe_periodic_reports:
                    subscribe_actions -= set(periodic_actions)
                try:
                    self._subscribe(dpws_hosted, subscribe_actions,
                                    self._notifications_dispatcher.on_notification)
                except Exception as ex:
                    self.all_subscribed = False  # => do not log errors when mdib versions are missing in notifications
                    self._logger.error('start_all: could not subscribe: error = {}, actions= {}',
                                       traceback.format_exc(), subscribe_actions)

        # register callback for end of subscription
        self._notifications_dispatcher_thread.dispatcher.register_function(
            self.sdc_definitions.Actions.SubscriptionEnd, self._on_subscription_end)

        # connect self.is_connected observable to all_subscriptions_okay observable in subscriptionsmanager
        def set_is_connected(is_ok):
            self.is_connected = is_ok

        properties.strongbind(self._subscription_mgr, all_subscriptions_okay=set_is_connected)
        self.is_connected = self._subscription_mgr.all_subscriptions_okay

    def stop_all(self, unsubscribe=True, close_all_connections=True):
        if self._subscription_mgr is not None:
            if unsubscribe:
                self._subscription_mgr.unsubscribe_all()
            self._subscription_mgr.stop()
        self._stop_event_sink(close_all_connections)
        self._register_mdib(None)

        for client in self._soap_clients.values():
            client.close()
        self._soap_clients = {}

    def set_used_compression(self, *compression_methods):
        # update list in place
        del self._compression_methods[:]
        self._compression_methods.extend(compression_methods)

    def get_metadata(self):
        _url = urllib.parse.urlparse(self._device_location)
        wsc = self._get_soap_client(self._device_location)

        if self._ssl_context is not None and _url.scheme == 'https':
            if wsc.is_closed():
                wsc.connect()
            sock = wsc.sock
            self.peer_certificate = sock.getpeercert(binary_form=False)
            self.binary_peer_certificate = sock.getpeercert(binary_form=True)  # in case the application needs it...

            self._logger.info('Peer Certificate: {}', self.peer_certificate)

        envelope = Soap12Envelope(nsmap)
        envelope.set_address(WsAddress(action='{}/Get'.format(Prefixes.WXF.namespace),
                                       addr_to=self._device_location))

        self.metadata = wsc.post_soap_envelope_to(_url.path, envelope,
                                                  response_factory=DPWSEnvelope,
                                                  msg='getMetadata')
        self.host_description = HostDescription(self.metadata)
        self._logger.debug('HostDescription: {}', self.host_description)

    def _discover_hosted_services(self):
        """ Discovers all hosted services.
        Raises RuntimeError if device does not provide the expected BICEPS services
        """
        # we need to read the meta data of the device only once => temporary soap client is sufficient
        self._logger.debug('reading meta data from {}', self._device_location)
        # self.metadata =
        if self.metadata is None:
            self.get_metadata()

        # now query also meta data of hosted services
        self._mk_hosted_services()
        self._logger.debug('Services: {}', self._service_clients.keys())

        # only GetService is mandatory!!!
        if self.get_service_client is None:
            raise RuntimeError('GetService not detected! found services = {}'.format(self._service_clients.keys()))

    def _get_soap_client(self, address):
        _url = urllib.parse.urlparse(address)
        key = (_url.scheme, _url.netloc)
        soap_client = self._soap_clients.get(key)
        if soap_client is None:
            soap_client = _mk_soap_client(_url.scheme, _url.netloc,
                                          loghelper.get_logger_adapter('sdc.client.soap', self.log_prefix),
                                          ssl_context=self._ssl_context,
                                          sdc_definitions=self.sdc_definitions,
                                          supported_encodings=self._compression_methods,
                                          chunked_requests=self.chunked_requests)
            self._soap_clients[key] = soap_client
        return soap_client

    def _mk_hosted_services(self):
        for hosted in self.metadata.hosted.values():
            endpoint_reference = hosted.endpoint_references[0].address
            soap_client = self._get_soap_client(endpoint_reference)
            hosted.soap_client = soap_client
            ns_types = [t.split(':') for t in hosted.types]
            h_descr = HostedServiceDescription(
                hosted.service_id, endpoint_reference,
                self._validate, self._biceps_schema, self._msg_factory, self.log_prefix)
            self.hosted_services[hosted.service_id] = h_descr
            h_descr.read_metadata(soap_client)
            for _, porttype in ns_types:
                hosted_service_client = self._mk_hosted_service_client(porttype, soap_client, hosted)
                self._service_clients[porttype] = hosted_service_client
                h_descr.services[porttype] = hosted_service_client

    def _mk_hosted_service_client(self, port_type, soap_client, hosted):
        cls = self._components.service_handlers[port_type]
        return cls(soap_client, self._msg_factory, hosted, port_type, self._validate,
                   self.sdc_definitions, self._biceps_schema, self.log_prefix)

    def _start_event_sink(self, async_dispatch):
        ssl_context = self._ssl_context if self._device_uses_https else None

        # create Event Server
        notifications_receiver_class = self._components.notifications_receiver_class  # thread
        notifications_handler_class = self._components.notifications_handler_class
        self._notifications_dispatcher_thread = notifications_receiver_class(
            self._my_ipaddress,
            ssl_context,
            log_prefix=self.log_prefix,
            sdc_definitions=self.sdc_definitions,
            supported_encodings=self._compression_methods,
            notifications_handler_class=notifications_handler_class,
            async_dispatch=async_dispatch)

        self._notifications_dispatcher_thread.start()
        self._notifications_dispatcher_thread.started_evt.wait(timeout=5)
        self._logger.info('serving EventSink on {}', self._notifications_dispatcher_thread.base_url)

    def _stop_event_sink(self, close_all_connections):
        if self._notifications_dispatcher_thread is not None:
            self._notifications_dispatcher_thread.stop(close_all_connections)

    def _on_subscription_end(self, request_data):
        self.state_event_report = request_data.envelope  # update observable
        self._subscription_mgr.on_subscription_end(request_data)

    def __str__(self):
        return 'SdcClient to {} {} on {}'.format(self.host_description.this_device,
                                                 self.host_description.this_model,
                                                 self._device_location)

    @classmethod
    def from_wsd_service(cls, wsd_service, ssl_context, validate=True, log_prefix='', specific_components=None):
        """

        :param wsd_service: a wsdiscovery.Service instance
        :param ssl_context: a ssl context or None
        :param validate: bool
        :param log_prefix: a string
        :param specific_components: a SdcClientComponents instance or None
        :return:
        """
        device_locations = wsd_service.get_x_addrs()
        if not device_locations:
            raise RuntimeError('discovered Service has no address!{}'.format(wsd_service))
        device_location = device_locations[0]
        for _q_name in wsd_service.types:
            q_name = etree_.QName(_q_name.namespace, _q_name.localname)
            for sdc_definition in ProtocolsRegistry.protocols:
                if sdc_definition.ns_matches(q_name):
                    return cls(device_location, sdc_definition, ssl_context, validate=validate,
                               log_prefix=log_prefix, specific_components=specific_components)
        raise RuntimeError('no matching protocol definition found for this service!')
