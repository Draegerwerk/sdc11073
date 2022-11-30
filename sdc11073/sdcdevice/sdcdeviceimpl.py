import copy
from urllib.parse import SplitResult
import uuid

from . import httpserver
from .components import default_sdc_device_components
from .hostedserviceimpl import SoapMessageHandler
from .periodicreports import PeriodicReportsHandler, PeriodicReportsNullHandler
from .waveforms import WaveformSender
from .. import compression
from .. import loghelper
from .. import observableproperties as properties
from ..addressing import EndpointReferenceType
from ..dpws import HostServiceType
from ..exceptions import ApiUsageError
from ..location import SdcLocation


class SdcDevice:
    DEFAULT_CONTEXTSTATES_IN_GETMDIB = True  # defines if get_mdib and getMdStates contain context states or not.

    def __init__(self, ws_discovery, this_model, this_device, device_mdib_container, my_uuid=None,
                 validate=True, ssl_context=None,
                 max_subscription_duration=7200, log_prefix='',
                 default_components=None, specific_components=None,
                 chunked_messages=False):  # pylint:disable=too-many-arguments
        """

        :param ws_discovery: a WsDiscovers instance
        :param this_model: a pysoap.soapenvelope.DPWSThisModel instance
        :param this_device: a pysoap.soapenvelope.DPWSThisDevice instance
        :param device_mdib_container: a DeviceMdibContainer instance
        :param my_uuid: a uuid instance or None
        :param validate: bool
        :param ssl_context: if not None, this context is used and https url is used. Otherwise http
        :param max_subscription_duration: max. possible duration of a subscription, default is 7200 seconds
        :param log_prefix: a string
        :param specific_components: a SdcDeviceComponents instance
        :param chunked_messages: bool
        """
        # ssl protocol handling itself is delegated to a handler.
        # Specific protocol versions or behaviours are implemented there.
        self._wsdiscovery = ws_discovery
        self.model = this_model
        self.device = this_device
        self._mdib = device_mdib_container
        self._my_uuid = my_uuid or uuid.uuid4()
        self._validate = validate
        self._ssl_context = ssl_context
        self._max_subscription_duration = max_subscription_duration
        self._log_prefix = log_prefix
        if default_components is None:
            default_components = default_sdc_device_components
        self._components = copy.deepcopy(default_components)
        if specific_components is not None:
            # merge specific stuff into _components
            self._components.merge(specific_components)
        self.chunked_messages = chunked_messages

        self._mdib.log_prefix = log_prefix
        self._compression_methods = compression.CompressionHandler.available_encodings[:]
        self._logger = loghelper.get_logger_adapter('sdc.device', log_prefix)
        self._location = None
        self._http_server_thread = None

        if self._ssl_context is not None:
            self._urlschema = 'https'
        else:
            self._urlschema = 'http'

        self.collect_rt_samples_period = 0.1  # in seconds
        self._waveform_sender = None
        self.contextstates_in_getmdib = self.DEFAULT_CONTEXTSTATES_IN_GETMDIB  # can be overridden per instance

        self.msg_reader = self._components.msg_reader_class(self._mdib.sdc_definitions,
                                                            self._logger,
                                                            self._log_prefix,
                                                            validate=validate)
        logger = loghelper.get_logger_adapter('sdc.device.msgfactory', log_prefix)
        self.msg_factory = self._components.msg_factory_class(sdc_definitions=self._mdib.sdc_definitions,
                                                              logger=logger,
                                                              validate=validate)

        # host dispatcher provides data of the sdc device itself.
        self._host_dispatcher = SoapMessageHandler(None, get_key_method=self._components.msg_dispatch_method,
                                                   msg_factory=self.msg_factory)
        nsh = self._mdib.sdc_definitions.data_model.ns_helper
        self._host_dispatcher.register_post_handler(f'{nsh.WXF.namespace}/Get', self._on_get_metadata)
        self._host_dispatcher.register_post_handler(f'{nsh.WSD.namespace}/Probe', self._on_probe_request)
        self._host_dispatcher.register_post_handler('Probe', self._on_probe_request)

        self.dpws_host = HostServiceType(
            endpoint_reference=EndpointReferenceType(self.epr),
            types_list=self._mdib.sdc_definitions.MedicalDeviceTypesFilter)

        self._hosted_service_dispatcher = httpserver.HostedServiceDispatcher(
            self.msg_reader, self.msg_factory, self._logger)

        self._hosted_service_dispatcher.register_hosted_service(self._host_dispatcher)

        # these are initialized in _setup_components:
        self._subscriptions_manager = None
        self._sco_operations_registry = None
        self._service_factory = None
        self.product_roles = None
        self.hosted_services = None
        self._periodic_reports_handler = PeriodicReportsNullHandler()
        self._setup_components()
        self.base_urls = []  # will be set after httpserver is started
        properties.bind(device_mdib_container, transaction=self._send_notifications)
        properties.bind(device_mdib_container, rt_updates=self._send_rt_notifications)

    def _setup_components(self):

        cls = self._components.subscriptions_manager_class
        self._subscriptions_manager = cls(self._ssl_context,
                                          self._mdib.sdc_definitions,
                                          self.msg_factory,
                                          self.msg_reader,
                                          self._components.soap_client_class,
                                          self._compression_methods,
                                          self._max_subscription_duration,
                                          log_prefix=self._log_prefix,
                                          chunked_messages=self.chunked_messages)

        cls = self._components.sco_operations_registry_class
        self._sco_operations_registry = cls(self._subscriptions_manager,
                                            self._components.operation_cls_getter,
                                            self._mdib,
                                            handle='_sco',
                                            log_prefix=self._log_prefix)

        services_factory = self._components.services_factory
        self.hosted_services = services_factory(self,
                                                self._components,
                                                self._mdib.sdc_definitions)
        for dpws_service in self.hosted_services.dpws_hosted_services:
            self._hosted_service_dispatcher.register_hosted_service(dpws_service)
        self.product_roles = self._components.role_provider_class(self._mdib,
                                                                  self._sco_operations_registry,
                                                                  self._log_prefix)
        self.product_roles.init_operations()

    @property
    def localization_storage(self):
        if self.hosted_services.localization_service is not None:
            return self.hosted_services.localization_service.localization_storage
        return None

    def _on_get_metadata(self, request_data):  # pylint: disable=unused-argument
        self._logger.info('_on_get_metadata from {}', request_data.peer_name)
        _nsm = self._mdib.nsmapper

        message = self.msg_factory.mk_get_metadata_response_message(
            request_data.message_data, self.device, self.model, self.dpws_host,
            self.hosted_services.dpws_hosted_services)
        self._logger.debug('returned meta data = {}', message.serialize_message())
        return message

    def _on_probe_request(self, request):
        response = self.msg_factory.mk_probe_matches_response_message(request.message_data, self.get_xaddrs())
        return response

    def set_location(self, location: SdcLocation,
                     validators=None,
                     publish_now: bool = True):
        '''
        :param location: an SdcLocation instance
        :param validators: a list of pmtypes.InstanceIdentifier objects or None; in that case the defaultInstanceIdentifiers member is used
        :param publish_now: if True, the device is published via its wsdiscovery reference.
        '''
        if location == self._location:
            return
        self._location = location
        if validators is None:
            validators = self._mdib.xtra.default_instance_identifiers
        self._mdib.xtra.set_location(location, validators)
        if publish_now:
            self.publish()

    def publish(self):
        """
        publish device on the network (sends HELLO message)
        :return:
        """
        scopes = self._components.scopes_factory(self._mdib)
        x_addrs = self.get_xaddrs()
        self._wsdiscovery.publish_service(self.epr, self._mdib.sdc_definitions.MedicalDeviceTypesFilter, scopes,
                                          x_addrs)

    @property
    def mdib(self):
        return self._mdib

    @property
    def subscriptions_manager(self):
        return self._subscriptions_manager

    @property
    def sco_operations_registry(self):
        return self._sco_operations_registry

    @property
    def epr(self):
        # End Point Reference, e.g 'urn:uuid:8c26f673-fdbf-4380-b5ad-9e2454a65b6b'
        return str(self._my_uuid.urn)

    @property
    def path_prefix(self):
        # http path prefix of service e.g '8c26f673-fdbf-4380-b5ad-9e2454a65b6b'
        return str(self._my_uuid.hex)

    def register_operation(self, operation):
        self._sco_operations_registry.register_operation(operation)

    def unregister_operation_by_handle(self, operation_handle):
        self._sco_operations_registry.register_operation(operation_handle)

    def get_operation_by_handle(self, operation_handle):
        return self._sco_operations_registry.get_operation_by_handle(operation_handle)

    def enqueue_operation(self, operation, request, operation_request):
        return self._sco_operations_registry.enqueue_operation(operation, request, operation_request)

    def start_all(self, start_rtsample_loop=True, periodic_reports_interval=None, shared_http_server=None):
        """

        :param start_rtsample_loop: flag
        :param periodic_reports_interval: if provided, a value in seconds
        :param shared_http_server: id provided, use this http server. Otherwise device creates its own.
        :return:
        """
        if periodic_reports_interval or self._mdib.retrievability_periodic:
            self._logger.info('starting PeriodicReportsHandler')
            self._periodic_reports_handler = PeriodicReportsHandler(self._mdib,
                                                                    self._subscriptions_manager,
                                                                    periodic_reports_interval)
            self._periodic_reports_handler.start()
        else:
            self._logger.info('no PeriodicReportsHandler')
            self._periodic_reports_handler = PeriodicReportsNullHandler()
        self._start_services(shared_http_server)

        if start_rtsample_loop:
            self.start_rt_sample_loop()

    def _start_services(self, shared_http_server=None):
        """ start the services"""
        self._logger.info('starting services, addr = {}', self._wsdiscovery.get_active_addresses())
        self._sco_operations_registry.start_worker()
        if shared_http_server:
            self._http_server_thread = shared_http_server
        else:
            self._http_server_thread = httpserver.DeviceHttpServerThread(
                my_ipaddress='0.0.0.0', ssl_context=self._ssl_context, supported_encodings=self._compression_methods,
                msg_reader=self.msg_reader, msg_factory=self.msg_factory,
                log_prefix=self._log_prefix, chunked_responses=self.chunked_messages)

            # first start http server, the services need to know the ip port number
            self._http_server_thread.start()
            event_is_set = self._http_server_thread.started_evt.wait(timeout=15.0)
            if not event_is_set:
                self._logger.error('Cannot start device, start event of http server not set.')
                raise RuntimeError('Cannot start device, start event of http server not set.')

        host_ips = self._wsdiscovery.get_active_addresses()
        self._http_server_thread.dispatcher.register_dispatcher(self.path_prefix, self._hosted_service_dispatcher)
        if len(host_ips) == 0:
            self._logger.error('Cannot start device, there is no IP address to bind it to.')
            raise RuntimeError('Cannot start device, there is no IP address to bind it to.')

        port = self._http_server_thread.my_port
        if port is None:
            self._logger.error('Cannot start device, could not bind HTTP server to a port.')
            raise RuntimeError('Cannot start device, could not bind HTTP server to a port.')

        self.base_urls = []  # e.g https://192.168.1.5:8888/8c26f673-fdbf-4380-b5ad-9e2454a65b6b; list has one member for each used ip address
        for addr in host_ips:
            self.base_urls.append(
                SplitResult(self._urlschema, f'{addr}:{port}', self.path_prefix, query=None, fragment=None))

        for host_ip in host_ips:
            self._logger.info('serving Services on {}:{}', host_ip, port)
        self._subscriptions_manager.set_base_urls(self.base_urls)

    def stop_all(self, send_subscription_end=True):
        self.stop_realtime_sample_loop()
        if self._periodic_reports_handler:
            self._periodic_reports_handler.stop()
        self._subscriptions_manager.stop_all(send_subscription_end)
        self._sco_operations_registry.stop_worker()
        try:
            self._wsdiscovery.clear_service(self.epr)
        except KeyError:
            self._logger.info('epr "{}" not known in self._wsdiscovery', self.epr)
        if self.product_roles is not None:
            self.product_roles.stop()

    def start_rt_sample_loop(self):
        if self._waveform_sender:
            raise ApiUsageError(' realtime send loop already started')
        self._waveform_sender = WaveformSender(self._mdib, self._logger, self.collect_rt_samples_period)
        self._waveform_sender.start()

    def stop_realtime_sample_loop(self):
        if self._waveform_sender:
            self._waveform_sender.stop()

    def get_xaddrs(self):
        addresses = self._wsdiscovery.get_active_addresses()  # these own IP addresses are currently used by discovery
        port = self._http_server_thread.my_port
        xaddrs = []
        for addr in addresses:
            xaddrs.append(f'{self._urlschema}://{addr}:{port}/{self.path_prefix}')
        return xaddrs

    def _send_notifications(self, transaction_processor):
        mdib_version_group = self._mdib.mdib_version_group
        ns_mapper = self._mdib.nsmapper
        if transaction_processor.has_descriptor_updates:
            updated = transaction_processor.descr_updated
            created = transaction_processor.descr_created
            deleted = transaction_processor.descr_deleted
            states = transaction_processor.all_states()
            self._subscriptions_manager.send_descriptor_updates(
                updated, created, deleted, states, ns_mapper, mdib_version_group)

        states = transaction_processor.metric_updates
        if len(states) > 0:
            self._subscriptions_manager.send_episodic_metric_report(
                states, ns_mapper, mdib_version_group)
            self._periodic_reports_handler.store_metric_states(mdib_version_group.mdib_version, transaction_processor.metric_updates)

        states = transaction_processor.alert_updates
        if len(states) > 0:
            self._subscriptions_manager.send_episodic_alert_report(
                states, ns_mapper, mdib_version_group)
            self._periodic_reports_handler.store_alert_states(mdib_version_group.mdib_version, states)

        states = transaction_processor.comp_updates
        if len(states) > 0:
            self._subscriptions_manager.send_episodic_component_state_report(
                states, ns_mapper, mdib_version_group)
            self._periodic_reports_handler.store_component_states(mdib_version_group.mdib_version, states)

        states = transaction_processor.ctxt_updates
        if len(states) > 0:
            self._subscriptions_manager.send_episodic_context_report(
                states, ns_mapper,mdib_version_group)
            self._periodic_reports_handler.store_context_states(mdib_version_group.mdib_version, states)

        states = transaction_processor.op_updates
        if len(states) > 0:
            self._subscriptions_manager.send_episodic_operational_state_report(states, ns_mapper, mdib_version_group)
            self._periodic_reports_handler.store_operational_states(mdib_version_group.mdib_version, states)

        states = transaction_processor.rt_updates
        if len(states) > 0:
            self._subscriptions_manager.send_realtime_samples_report(
                states, ns_mapper, mdib_version_group)

    def _send_rt_notifications(self, rt_states):
        if len(rt_states) > 0:
            self._subscriptions_manager.send_realtime_samples_report(
                rt_states, self._mdib.nsmapper, self._mdib.mdib_version_group)

    def set_used_compression(self, *compression_methods):
        del self._compression_methods[:]
        self._compression_methods.extend(compression_methods)
