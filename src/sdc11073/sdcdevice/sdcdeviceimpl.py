from __future__ import annotations

import copy
import uuid
from typing import TYPE_CHECKING, Optional, Union, Protocol
from urllib.parse import SplitResult

from .components import default_sdc_device_components
from .periodicreports import PeriodicReportsHandler, PeriodicReportsNullHandler
from .waveforms import WaveformSender
from .. import loghelper
from .. import observableproperties as properties
from ..dispatch import DispatchKey, DispatchKeyRegistry
from ..dispatch import PathElementRegistry
from ..dispatch import RequestData, MessageConverterMiddleware
from ..exceptions import ApiUsageError
from ..httpserver import compression
from ..httpserver.httpserverimpl import HttpServerThreadBase
from ..location import SdcLocation
from ..namespaces import WSA_ANONYMOUS
from ..pysoap.soapclientpool import SoapClientPool
from ..xml_types import mex_types
from ..xml_types.addressing_types import EndpointReferenceType
from ..xml_types.dpws_types import HostServiceType, ThisDeviceType, ThisModelType
from ..xml_types.wsd_types import ProbeMatchesType, ProbeMatchType

if TYPE_CHECKING:
    from ..pysoap.msgfactory import CreatedMessage
    from ..mdib.devicemdib import DeviceMdibContainer
    from .components import SdcDeviceComponents
    from ssl import SSLContext
    from ..xml_types.wsd_types import ScopesType


class _PathElementDispatcher(PathElementRegistry):
    """ Dispatch to one of the registered instances, based on path element.
    Implements RequestHandlerProtocol"""

    def register_instance(self, path_element: Union[str, None], instance: DispatchKeyRegistry):
        super().register_instance(path_element, instance)

    def on_post(self, request_data: RequestData) -> CreatedMessage:
        path_element = request_data.consume_current_path_element()
        dispatcher = self.get_instance(path_element)
        return dispatcher.on_post(request_data)

    def on_get(self, request_data: RequestData) -> str:
        dispatcher = self.get_instance(request_data.consume_current_path_element())
        return dispatcher.on_get(request_data)


class WsDiscoveryProtocol(Protocol):
    """This is the interface that SdcDevice expects"""

    def publish_service(self, epr: str, types: list, scopes: ScopesType, x_addrs: list):
        ...

    def get_active_addresses(self) -> list:
        ...

    def clear_service(self, epr: str):
        ...


class SdcDevice:
    DEFAULT_CONTEXTSTATES_IN_GETMDIB = True  # defines weather get_mdib and getMdStates contain context states or not.

    def __init__(self, ws_discovery: WsDiscoveryProtocol,
                 this_model: ThisModelType,
                 this_device: ThisDeviceType,
                 device_mdib_container: DeviceMdibContainer,
                 epr: Union[str, uuid.UUID, None] = None,
                 validate: bool = True,
                 ssl_context: Optional[SSLContext] = None,
                 max_subscription_duration: int = 7200,
                 log_prefix: str = '',
                 default_components: Optional[SdcDeviceComponents] = None,
                 specific_components: Optional[SdcDeviceComponents] = None,
                 chunked_messages: bool = False):
        """

        :param ws_discovery: a WsDiscovers instance
        :param this_model: a ThisModelType instance
        :param this_device: a ThisDeviceType instance
        :param device_mdib_container: a DeviceMdibContainer instance
        :param epr: something that serves as a unique identifier of this device for discovery.
                    If epr is a string, it must be usable as a path element in an url (no spaces, ...)
        :param validate: bool
        :param ssl_context: if not None, this context is used and https url is used, otherwise http
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
        if epr is None:
            self._epr = uuid.uuid4()
        else:
            self._epr = epr
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
        self._http_server = None
        self._is_internal_http_server = False

        if self._ssl_context is not None:
            self._urlschema = 'https'
        else:
            self._urlschema = 'http'

        self.collect_rt_samples_period = 0.1  # in seconds
        self._waveform_sender = None
        self.contextstates_in_getmdib = self.DEFAULT_CONTEXTSTATES_IN_GETMDIB  # can be overridden per instance
        # look for schemas added by services
        additional_schema_specs = []
        for hosted_service in self._components.hosted_services.values():
            for port_type_impl in hosted_service.values():
                additional_schema_specs.extend(port_type_impl.additional_namespaces)
        logger = loghelper.get_logger_adapter('sdc.device.msgreader', log_prefix)
        self.msg_reader = self._components.msg_reader_class(self._mdib.sdc_definitions,
                                                            additional_schema_specs,
                                                            logger,
                                                            validate=validate)

        logger = loghelper.get_logger_adapter('sdc.device.msgfactory', log_prefix)
        self.msg_factory = self._components.msg_factory_class(self._mdib.sdc_definitions,
                                                              additional_schema_specs,
                                                              logger=logger,
                                                              validate=validate)

        # host dispatcher provides data of the sdc device itself.
        self._host_dispatcher = DispatchKeyRegistry()
        nsh = self._mdib.sdc_definitions.data_model.ns_helper
        self._host_dispatcher.register_post_handler(
            DispatchKey(f'{nsh.WXF.namespace}/Get', None),
            self._on_get_metadata)
        self._host_dispatcher.register_post_handler(
            DispatchKey(f'{nsh.WSD.namespace}/Probe', nsh.WSD.tag('Probe')),
            self._on_probe_request)
        epr_type = EndpointReferenceType()
        epr_type.Address = self.epr_urn
        self.dpws_host = HostServiceType()
        self.dpws_host.EndpointReference = epr_type
        self.dpws_host.Types = self._mdib.sdc_definitions.MedicalDeviceTypesFilter

        self._hosted_service_dispatcher = _PathElementDispatcher()
        self._hosted_service_dispatcher.register_instance(None, self._host_dispatcher)

        self._msg_converter = MessageConverterMiddleware(
            self.msg_reader, self.msg_factory, self._logger, self._hosted_service_dispatcher)

        # these are initialized in _setup_components:
        self._subscriptions_managers = {}
        self._soap_client_pool = SoapClientPool(self._mk_soap_client, log_prefix)
        self._sco_operations_registries = {}  # key is mds handle ?
        self._service_factory = None
        self.product_roles_lookup = {}
        self.hosted_services = None
        self._periodic_reports_handler = PeriodicReportsNullHandler()
        self._setup_components()
        self.base_urls = []  # will be set after httpserver is started
        properties.bind(device_mdib_container, transaction=self._send_episodic_reports)
        properties.bind(device_mdib_container, rt_updates=self._send_rt_notifications)

    def _mk_soap_client(self, netloc, accepted_encodings):
        cls = self._components.soap_client_class
        soap_client = cls(netloc,
                          loghelper.get_logger_adapter('sdc.device.soap', self._log_prefix),
                          ssl_context=self._ssl_context,
                          sdc_definitions=self._mdib.sdc_definitions,
                          msg_reader=self.msg_reader,
                          supported_encodings=self._compression_methods,
                          request_encodings=accepted_encodings,
                          chunked_requests=self.chunked_messages)
        return soap_client

    def _setup_components(self):
        self._subscriptions_managers = {}
        for name, cls in self._components.subscriptions_manager_class.items():
            mgr = cls(self._mdib.sdc_definitions,
                      self.msg_factory,
                      self._soap_client_pool,
                      self._max_subscription_duration,
                      log_prefix=self._log_prefix
                      )
            self._subscriptions_managers[name] = mgr

        services_factory = self._components.services_factory
        self.hosted_services = services_factory(self, self._components, self._subscriptions_managers)
        for dpws_service in self.hosted_services.dpws_hosted_services.values():
            self._hosted_service_dispatcher.register_instance(dpws_service.path_element, dpws_service)

        cls = self._components.sco_operations_registry_class
        pm_names = self._mdib.data_model.pm_names

        sco_descr_list = self._mdib.descriptions.NODETYPE.get(pm_names.ScoDescriptor, [])
        for sco_descr in sco_descr_list:
            sco_operations_registry = cls(self.hosted_services.set_service,
                                          self._components.operation_cls_getter,
                                          self._mdib,
                                          sco_descr,
                                          log_prefix=self._log_prefix)
            self._sco_operations_registries[sco_descr.Handle] = sco_operations_registry

            product_roles = self._components.role_provider_class(self._mdib,
                                                                 sco_operations_registry,
                                                                 self._log_prefix)
            self.product_roles_lookup[sco_descr.Handle] = product_roles
            product_roles.init_operations()
        # product roles might have added descriptors, set source mds for all
        self._mdib.xtra.set_all_source_mds()

    @property
    def localization_storage(self):
        if self.hosted_services.localization_service is not None:
            return self.hosted_services.localization_service.localization_storage
        return None

    def _on_get_metadata(self, request_data):  # pylint: disable=unused-argument
        self._logger.info('_on_get_metadata from {}', request_data.peer_name)
        metadata = mex_types.Metadata()
        section = mex_types.ThisModelMetadataSection()
        section.MetadataReference = self.model
        metadata.MetadataSection.append(section)

        section = mex_types.ThisDeviceMetadataSection()
        section.MetadataReference = self.device
        metadata.MetadataSection.append(section)

        section = mex_types.RelationshipMetadataSection()
        section.MetadataReference.Host = self.dpws_host

        # add all hosted services:
        for service in self.hosted_services.dpws_hosted_services.values():
            hosted = service.mk_dpws_hosted_instance()
            section.MetadataReference.Hosted.append(hosted)
        metadata.MetadataSection.append(section)

        # find namespaces that are used in Types of Host and Hosted
        _nsm = self._mdib.nsmapper
        needed_namespaces = [_nsm.DPWS, _nsm.WSX]
        q_names = []
        q_names.extend(self.dpws_host.Types)
        for h in section.MetadataReference.Hosted:
            q_names.extend(h.Types)
        for q_name in q_names:
            for e in _nsm.prefix_enum:
                if e.namespace == q_name.namespace and e not in needed_namespaces:
                    needed_namespaces.append(e)
        response = self.msg_factory.mk_reply_soap_message(request_data, metadata, needed_namespaces)
        return response

    def _on_probe_request(self, request):
        _nsm = self._mdib.nsmapper
        probe_matches = ProbeMatchesType()
        probe_match = ProbeMatchType()
        probe_match.Types.append(_nsm.DPWS.tag('Device'))
        probe_match.Types.append(_nsm.MDPWS.tag('MedicalDevice'))
        probe_match.XAddrs.extend(self.get_xaddrs())
        probe_matches.ProbeMatch.append(probe_match)
        needed_namespaces = [_nsm.DPWS, _nsm.MDPWS]
        response = self.msg_factory.mk_reply_soap_message(request, probe_matches, needed_namespaces)
        response.p_msg.header_info_block.set_to(WSA_ANONYMOUS)
        return response

    def set_location(self, location: SdcLocation,
                     validators=None,
                     publish_now: bool = True):
        """
        :param location: an SdcLocation instance
        :param validators: a list of pmtypes.InstanceIdentifier objects or None; in that case the defaultInstanceIdentifiers member is used
        :param publish_now: if True, the device is published via its wsdiscovery reference.
        """
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
        self._wsdiscovery.publish_service(self.epr_urn,
                                          self._mdib.sdc_definitions.MedicalDeviceTypesFilter,
                                          scopes,
                                          x_addrs)

    @property
    def mdib(self):
        return self._mdib

    @property
    def epr_urn(self):
        # End Point Reference, e.g 'urn:uuid:8c26f673-fdbf-4380-b5ad-9e2454a65b6b'
        try:
            return self._epr.urn
        except AttributeError:
            return self._epr

    @property
    def path_prefix(self):
        # http path prefix of service e.g '8c26f673fdbf4380b5ad9e2454a65b6b'
        try:
            return self._epr.hex
        except AttributeError:
            return self._epr

    def get_operation_by_handle(self, operation_handle):
        for sco in self._sco_operations_registries.values():
            op = sco.get_operation_by_handle(operation_handle)
            if op is not None:
                return op
        return None

    def enqueue_operation(self, operation, request, operation_request):
        for sco in self._sco_operations_registries.values():
            has_this_operation = sco.get_operation_by_handle(operation.handle) is not None
            if has_this_operation:
                return sco.enqueue_operation(operation, request, operation_request)

    def get_toplevel_sco_list(self) -> list:
        pm_names = self._mdib.data_model.pm_names
        mds_handles = [d.Handle for d in self._mdib.descriptions.NODETYPE.get(pm_names.MdsDescriptor, [])]
        ret = []
        for sco in self._sco_operations_registries.values():
            if sco.sco_descriptor_container.parent_handle in mds_handles:
                ret.append(sco)
        return ret

    def start_all(self, start_rtsample_loop=True, periodic_reports_interval=None, shared_http_server=None):
        """

        :param start_rtsample_loop: flag
        :param periodic_reports_interval: if provided, a value in seconds
        :param shared_http_server: if provided, use this http server, else device creates its own.
        :return:
        """
        if periodic_reports_interval or self._mdib.retrievability_periodic:
            self._logger.info('starting PeriodicReportsHandler')
            self._periodic_reports_handler = PeriodicReportsHandler(self._mdib,
                                                                    self.hosted_services,
                                                                    # self._subscriptions_managers['StateEvent'],
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
        for sco in self._sco_operations_registries.values():
            sco.start_worker()

        if shared_http_server:
            self._http_server = shared_http_server
        else:
            self._is_internal_http_server = True
            logger = loghelper.get_logger_adapter('sdc.device.httpsrv', self._log_prefix)

            self._http_server = HttpServerThreadBase(
                my_ipaddress='0.0.0.0', ssl_context=self._ssl_context, supported_encodings=self._compression_methods,
                logger=logger, chunked_responses=self.chunked_messages)

            # first start http server, the services need to know the ip port number
            self._http_server.start()
            event_is_set = self._http_server.started_evt.wait(timeout=15.0)
            if not event_is_set:
                self._logger.error('Cannot start device, start event of http server not set.')
                raise RuntimeError('Cannot start device, start event of http server not set.')

        host_ips = self._wsdiscovery.get_active_addresses()
        self._http_server.dispatcher.register_instance(self.path_prefix, self._msg_converter)
        if len(host_ips) == 0:
            self._logger.error('Cannot start device, there is no IP address to bind it to.')
            raise RuntimeError('Cannot start device, there is no IP address to bind it to.')

        port = self._http_server.my_port
        if port is None:
            self._logger.error('Cannot start device, could not bind HTTP server to a port.')
            raise RuntimeError('Cannot start device, could not bind HTTP server to a port.')

        self.base_urls = []  # e.g https://192.168.1.5:8888/8c26f673-fdbf-4380-b5ad-9e2454a65b6b; list has one member for each used ip address
        for addr in host_ips:
            self.base_urls.append(
                SplitResult(self._urlschema, f'{addr}:{port}', self.path_prefix, query=None, fragment=None))

        for host_ip in host_ips:
            self._logger.info('serving Services on {}:{}', host_ip, port)
        for subscriptions_manager in self._subscriptions_managers.values():
            subscriptions_manager.set_base_urls(self.base_urls)

    def stop_all(self, send_subscription_end=True):
        self.stop_realtime_sample_loop()
        if self._periodic_reports_handler:
            self._periodic_reports_handler.stop()
        for subscriptions_manager in self._subscriptions_managers.values():
            subscriptions_manager.stop_all(send_subscription_end)
        for sco in self._sco_operations_registries.values():
            sco.stop_worker()
        try:
            self._wsdiscovery.clear_service(self.epr_urn)
        except KeyError:
            self._logger.info('epr "{}" not known in self._wsdiscovery', self.epr_urn)
        for role in self.product_roles_lookup.values():
            role.stop()
        if self._is_internal_http_server and self._http_server is not None:
            self._http_server.stop()

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
        port = self._http_server.my_port
        xaddrs = []
        for addr in addresses:
            xaddrs.append(f'{self._urlschema}://{addr}:{port}/{self.path_prefix}')
        return xaddrs

    def _send_episodic_reports(self, transaction_processor):
        mdib_version_group = self._mdib.mdib_version_group
        if transaction_processor.has_descriptor_updates:
            port_type_impl = self.hosted_services.description_event_service
            updated = transaction_processor.descr_updated
            created = transaction_processor.descr_created
            deleted = transaction_processor.descr_deleted
            states = transaction_processor.all_states()
            port_type_impl.send_descriptor_updates(
                updated, created, deleted, states, mdib_version_group)

        states = transaction_processor.metric_updates
        if len(states) > 0:
            port_type_impl = self.hosted_services.state_event_service
            port_type_impl.send_episodic_metric_report(
                states, mdib_version_group)
            self._periodic_reports_handler.store_metric_states(mdib_version_group.mdib_version,
                                                               transaction_processor.metric_updates)

        states = transaction_processor.alert_updates
        if len(states) > 0:
            port_type_impl = self.hosted_services.state_event_service
            port_type_impl.send_episodic_alert_report(
                states, mdib_version_group)
            self._periodic_reports_handler.store_alert_states(mdib_version_group.mdib_version, states)

        states = transaction_processor.comp_updates
        if len(states) > 0:
            port_type_impl = self.hosted_services.state_event_service
            port_type_impl.send_episodic_component_state_report(
                states, mdib_version_group)
            self._periodic_reports_handler.store_component_states(mdib_version_group.mdib_version, states)

        states = transaction_processor.ctxt_updates
        if len(states) > 0:
            port_type_impl = self.hosted_services.context_service
            port_type_impl.send_episodic_context_report(
                states, mdib_version_group)
            self._periodic_reports_handler.store_context_states(mdib_version_group.mdib_version, states)

        states = transaction_processor.op_updates
        if len(states) > 0:
            port_type_impl = self.hosted_services.state_event_service
            port_type_impl.send_episodic_operational_state_report(states, mdib_version_group)
            self._periodic_reports_handler.store_operational_states(mdib_version_group.mdib_version, states)

        states = transaction_processor.rt_updates
        if len(states) > 0:
            port_type_impl = self.hosted_services.waveform_service
            port_type_impl.send_realtime_samples_report(states, mdib_version_group)

    def _send_rt_notifications(self, rt_states):
        if len(rt_states) > 0:
            port_type_impl = self.hosted_services.waveform_service
            port_type_impl.send_realtime_samples_report(rt_states, self._mdib.mdib_version_group)

    def set_used_compression(self, *compression_methods):
        del self._compression_methods[:]
        self._compression_methods.extend(compression_methods)
