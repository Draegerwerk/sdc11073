from __future__ import annotations

import copy
import threading
import uuid
from typing import TYPE_CHECKING, Any, Protocol
from urllib.parse import SplitResult

import sdc11073.certloader
from sdc11073 import loghelper
from sdc11073 import observableproperties as properties
from sdc11073.dispatch import (
    DispatchKey,
    MessageConverterMiddleware,
    PathElementRegistry,
    RequestData,
    RequestDispatcher,
)
from sdc11073.exceptions import ApiUsageError
from sdc11073.httpserver import compression
from sdc11073.httpserver.httpserverimpl import HttpServerThreadBase
from sdc11073.namespaces import WSA_ANONYMOUS
from sdc11073.pysoap.soapclientpool import SoapClientPool
from sdc11073.xml_types import mex_types
from sdc11073.xml_types.addressing_types import EndpointReferenceType
from sdc11073.xml_types.dpws_types import HostServiceType, ThisDeviceType, ThisModelType
from sdc11073.xml_types.wsd_types import ProbeMatchesType, ProbeMatchType
from sdc11073.roles.protocols import ProductProtocol, WaveformProviderProtocol # import here for code cov. :(

from .periodicreports import PeriodicReportsHandler, PeriodicReportsNullHandler

if TYPE_CHECKING:
    from enum import Enum
    from sdc11073.location import SdcLocation
    from sdc11073.mdib.providermdib import ProviderMdib
    from sdc11073.mdib.transactionsprotocol import TransactionResultProtocol
    from sdc11073.mdib.statecontainers import AbstractStateProtocol
    from sdc11073.provider.porttypes.localizationservice import LocalizationStorage
    from sdc11073.pysoap.msgfactory import CreatedMessage
    from sdc11073.pysoap.soapenvelope import ReceivedSoapMessage
    from sdc11073.xml_types.msg_types import AbstractSet
    from sdc11073.xml_types.pm_types import InstanceIdentifier
    from sdc11073.xml_types.wsd_types import ScopesType

    from .components import SdcProviderComponents
    from .operations import OperationDefinitionBase

class _PathElementDispatcher(PathElementRegistry):
    """Dispatch to one of the registered instances, based on path element.

    Implements RequestHandlerProtocol.
    """

    def register_instance(self, path_element: str | None, instance: RequestDispatcher):
        super().register_instance(path_element, instance)

    def on_post(self, request_data: RequestData) -> CreatedMessage:
        path_element = request_data.consume_current_path_element()
        dispatcher = self.get_instance(path_element)
        return dispatcher.on_post(request_data)

    def on_get(self, request_data: RequestData) -> str:
        dispatcher = self.get_instance(request_data.consume_current_path_element())
        return dispatcher.on_get(request_data)


class WsDiscoveryProtocol(Protocol):
    """WsDiscoveryProtocol is the interface that SdcProvider expects."""

    def publish_service(self, epr: str, types: list, scopes: ScopesType, x_addrs: list):  # noqa: D102
        ...

    def get_active_addresses(self) -> list:  # noqa: D102
        ...

    def clear_service(self, epr: str):  # noqa: D102
        ...


class SdcProvider:
    """SdcProvider is the host for sdc services, subscription manager etc."""

    DEFAULT_CONTEXTSTATES_IN_GETMDIB = True  # defines weather get_mdib and getMdStates contain context states or not.

    def __init__(self, ws_discovery: WsDiscoveryProtocol,
                 this_model: ThisModelType,
                 this_device: ThisDeviceType,
                 device_mdib_container: ProviderMdib,
                 epr: str | uuid.UUID | None = None,
                 validate: bool = True,
                 ssl_context_container: sdc11073.certloader.SSLContextContainer | None = None,
                 max_subscription_duration: int = 15,
                 socket_timeout: int | float | None = None,
                 log_prefix: str = '',
                 default_components: SdcProviderComponents | None = None,
                 specific_components: SdcProviderComponents | None = None,
                 chunk_size: int = 0,
                 alternative_hostname: str | None = None):
        """Construct an SdcProvider.

        :param ws_discovery: a WsDiscovers instance
        :param this_model: a ThisModelType instance
        :param this_device: a ThisDeviceType instance
        :param device_mdib_container: a ProviderMdib instance
        :param epr: something that serves as a unique identifier of this device for discovery.
                    If epr is a string, it must be usable as a path element in an url (no spaces, ...)
        :param validate: bool
        :param ssl_context_container: if not None, the contexts are used and an https url is used, otherwise http
        :param max_subscription_duration: max. possible duration of a subscription
        :param socket_timeout: timeout for tcp sockets that send notifications.
                               If None, it is set to max_subscription_duration * 1.2
        :param log_prefix: a string
        :param specific_components: a SdcProviderComponents instance
        :param chunk_size: if value > 0, messages are split into chunks of this size.
        :param alternative_hostname: if supplied this hostname is used in xaddr, default is to use numerical
                                     ipv4 address (can be used to use full qualified hostname)
        """
        self._wsdiscovery = ws_discovery
        self.model = this_model
        self.device = this_device
        self._mdib = device_mdib_container
        if epr is None:
            self._epr = uuid.uuid4()
        else:
            self._epr = epr
        self._validate = validate
        self._ssl_context_container = ssl_context_container
        self._max_subscription_duration = max_subscription_duration
        self._socket_timeout = socket_timeout or int(max_subscription_duration * 1.2)
        self._log_prefix = log_prefix
        if default_components is None:
            from .components import default_sdc_provider_components  # lazy import avoids cyclic import
            default_components = default_sdc_provider_components
        self._components = copy.deepcopy(default_components)
        if specific_components is not None:
            # merge specific stuff into _components
            self._components.merge(specific_components)
        self.chunk_size = chunk_size
        self._alternative_hostname = alternative_hostname
        self._mdib.log_prefix = log_prefix
        self._compression_methods = compression.CompressionHandler.available_encodings[:]
        self._logger = loghelper.get_logger_adapter('sdc.device', log_prefix)
        self._location = None
        self._http_server = None
        self._is_internal_http_server = False

        if self._ssl_context_container is not None:
            self._urlschema = 'https'
        else:
            self._urlschema = 'http'

        self.collect_rt_samples_period = 0.1  # in seconds
        self.contextstates_in_getmdib = self.DEFAULT_CONTEXTSTATES_IN_GETMDIB  # can be overridden per instance
        # look for schemas added by services and components spec
        additional_schema_specs = set(self._components.additional_schema_specs)
        for hosted_service in self._components.hosted_services.values():
            for port_type_impl in hosted_service:
                additional_schema_specs.update(port_type_impl.additional_namespaces)
        logger = loghelper.get_logger_adapter('sdc.device.msgreader', log_prefix)
        self.msg_reader = self._components.msg_reader_class(self._mdib.sdc_definitions,
                                                            list(additional_schema_specs),
                                                            logger,
                                                            validate=validate)

        logger = loghelper.get_logger_adapter('sdc.device.msgfactory', log_prefix)
        self.msg_factory = self._components.msg_factory_class(self._mdib.sdc_definitions,
                                                              list(additional_schema_specs),
                                                              logger=logger,
                                                              validate=validate)

        # host dispatcher provides data of the sdc device itself.
        self._host_dispatcher = RequestDispatcher()
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

        self._transaction_id = 0  # central transaction number handling for all called operations.
        self._transaction_id_lock = threading.Lock()

        # these are initialized in _setup_components:
        self._subscriptions_managers = {}
        self._soap_client_pool = SoapClientPool(self._mk_soap_client, log_prefix)
        self._sco_operations_registries = {}  # key is mds handle ?
        self._service_factory = None
        self.product_lookup: dict[str, ProductProtocol] = {}  # one product per sco,  key is a sco handle
        self.hosted_services = None
        self._periodic_reports_handler = PeriodicReportsNullHandler()
        self.waveform_provider: WaveformProviderProtocol | None = None
        self._setup_components()
        self.base_urls = []  # will be set after httpserver is started
        properties.bind(device_mdib_container, transaction=self._send_episodic_reports)
        properties.bind(device_mdib_container, rt_updates=self._send_rt_notifications)

    def generate_transaction_id(self) -> int:
        """Return a new transaction id."""
        with self._transaction_id_lock:
            self._transaction_id += 1
            return self._transaction_id

    def _mk_soap_client(self, netloc: str, accepted_encodings: list[str]) -> Any:
        cls = self._components.soap_client_class
        return cls(netloc,
                   self._socket_timeout,
                   loghelper.get_logger_adapter('sdc.device.soap', self._log_prefix),
                   ssl_context=self._ssl_context_container.client_context if self._ssl_context_container else None,
                   sdc_definitions=self._mdib.sdc_definitions,
                   msg_reader=self.msg_reader,
                   supported_encodings=self._compression_methods,
                   request_encodings=accepted_encodings,
                   chunk_size=self.chunk_size)

    def _setup_components(self):
        self._subscriptions_managers = {}
        for name, cls in self._components.subscriptions_manager_class.items():
            mgr = cls(self._mdib.sdc_definitions,
                      self.msg_factory,
                      self._soap_client_pool,
                      self._max_subscription_duration,
                      log_prefix=self._log_prefix,
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
            self.product_lookup[sco_descr.Handle] = product_roles
            product_roles.init_operations()
        if self._components.waveform_provider_class is not None:
            self.waveform_provider = self._components.waveform_provider_class(self._mdib,
                                                                              self._log_prefix)

        # product roles might have added descriptors, set source mds for all
        self._mdib.xtra.set_all_source_mds()

    @property
    def localization_storage(self) -> LocalizationStorage | None:
        """Convenience method for easier access to LocalizationStorage."""
        if self.hosted_services.localization_service is not None:
            return self.hosted_services.localization_service.localization_storage
        return None

    def _on_get_metadata(self, request_data: RequestData) -> CreatedMessage:
        self._logger.info('_on_get_metadata from %s', request_data.peer_name)
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
        return self.msg_factory.mk_reply_soap_message(request_data, metadata, needed_namespaces)

    def _on_probe_request(self, request: RequestData) -> CreatedMessage:
        _nsm = self._mdib.nsmapper
        probe_matches = ProbeMatchesType()
        probe_match = ProbeMatchType()
        probe_match.Types.append(_nsm.DPWS.tag('Device'))
        probe_match.Types.append(_nsm.MDPWS.tag('MedicalDevice'))
        probe_match.XAddrs.extend(self.get_xaddrs())
        probe_matches.ProbeMatch.append(probe_match)
        needed_namespaces = [_nsm.DPWS, _nsm.MDPWS]
        response = self.msg_factory.mk_reply_soap_message(request, probe_matches, needed_namespaces)
        response.p_msg.header_info_block.To = WSA_ANONYMOUS
        return response

    def set_location(self,
                     location: SdcLocation,
                     validators: list[InstanceIdentifier] | None = None,
                     publish_now: bool = True,
                     location_context_descriptor_handle: str | None = None):
        """Set a new associated location.

        :param location: an SdcLocation instance
        :param validators: a list of InstanceIdentifier objects or None;
            If it is None, the defaultInstanceIdentifiers member is used
        :param publish_now: if True, the device is published via its wsdiscovery reference.
        :param location_context_descriptor_handle: Only needed if the mdib contains more than one
               LocationContextDescriptor. Then this defines the descriptor for which a new LocationContextState
               shall be created.

        """
        if location == self._location:
            return
        self._location = location
        self._mdib.xtra.set_location(location,
                                     validators,
                                     location_context_descriptor_handle = location_context_descriptor_handle)
        if publish_now:
            self.publish()

    def publish(self):
        """Publish device on the network (sends HELLO message)."""
        scopes = self._components.scopes_factory(self._mdib)
        x_addrs = self.get_xaddrs()
        self._wsdiscovery.publish_service(self.epr_urn,
                                          list(self._mdib.sdc_definitions.MedicalDeviceTypesFilter),
                                          scopes,
                                          x_addrs)

    @property
    def mdib(self) -> ProviderMdib:
        """Return mdib reference."""
        return self._mdib

    @property
    def epr_urn(self) -> str:
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

    def get_operation_by_handle(self, operation_handle: str) -> OperationDefinitionBase | None:
        """Return OperationDefinitionBase for given handle or None if it does not exist."""
        for sco in self._sco_operations_registries.values():
            op = sco.get_operation_by_handle(operation_handle)
            if op is not None:
                return op
        return None

    def handle_operation_request(self,
                                 operation: OperationDefinitionBase,
                                 request: ReceivedSoapMessage,
                                 operation_request: AbstractSet,
                                 transaction_id: int) -> Enum:
        """Find the responsible sco and forward request to it."""
        for sco in self._sco_operations_registries.values():
            has_this_operation = sco.get_operation_by_handle(operation.handle) is not None
            if has_this_operation:
                return sco.handle_operation_request(operation, request, operation_request, transaction_id)
        self._logger.error('no sco has operation %s', operation.handle)
        return self.mdib.data_model.msg_types.InvocationState.FAILED

    def start_all(self,
                  start_rtsample_loop: bool = True,
                  periodic_reports_interval: float | None = None,
                  shared_http_server=None):
        """:param start_rtsample_loop: flag
        :param periodic_reports_interval: if provided, a value in seconds
        :param shared_http_server: if provided, use this http server, else device creates its own.
        :return:
        """
        if periodic_reports_interval or self._mdib.retrievability_periodic:
            self._logger.info('starting PeriodicReportsHandler')
            self._periodic_reports_handler = PeriodicReportsHandler(self._mdib,
                                                                    self.hosted_services,
                                                                    periodic_reports_interval)
            self._periodic_reports_handler.start()
        else:
            self._logger.info('no PeriodicReportsHandler')
            self._periodic_reports_handler = PeriodicReportsNullHandler()
        self._start_services(shared_http_server)

        if start_rtsample_loop:
            self.start_rt_sample_loop()

    def _start_services(self, shared_http_server=None):
        """Start the services."""
        self._logger.info('starting services, addr = %r', self._wsdiscovery.get_active_addresses())
        for sco in self._sco_operations_registries.values():
            sco.start_worker()

        if shared_http_server:
            self._http_server = shared_http_server
        else:
            self._is_internal_http_server = True
            logger = loghelper.get_logger_adapter('sdc.device.httpsrv', self._log_prefix)

            self._http_server = HttpServerThreadBase(
                my_ipaddress='0.0.0.0',  # noqa: S104
                ssl_context=self._ssl_context_container.server_context if self._ssl_context_container else None,
                supported_encodings=self._compression_methods,
                logger=logger,
                chunk_size=self.chunk_size)

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
            self._logger.info('serving Services on %s:%d', host_ip, port)
        for subscriptions_manager in self._subscriptions_managers.values():
            subscriptions_manager.set_base_urls(self.base_urls)

    def stop_all(self, send_subscription_end: bool = True):
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
            self._logger.info('epr "%s" not known in self._wsdiscovery', self.epr_urn)
        for role in self.product_lookup.values():
            role.stop()
        if self._is_internal_http_server and self._http_server is not None:
            self._http_server.stop()
        self._soap_client_pool.close_all()

    def start_rt_sample_loop(self):
        if self.waveform_provider is None:
            raise ApiUsageError('no waveform provider configured.')
        if self.waveform_provider.is_running:
            raise ApiUsageError('realtime send loop already started')
        self.waveform_provider.start()

    def stop_realtime_sample_loop(self):
        if self.waveform_provider is not None and self.waveform_provider.is_running:
            self.waveform_provider.stop()

    def get_xaddrs(self) -> list[str]:
        if self._alternative_hostname:
            addresses = [self._alternative_hostname]
        else:
            addresses = self._wsdiscovery.get_active_addresses()  # these own IP addresses are currently used by discovery

        port = self._http_server.my_port
        xaddrs = []
        for addr in addresses:
            xaddrs.append(f'{self._urlschema}://{addr}:{port}/{self.path_prefix}')
        return xaddrs

    def _send_episodic_reports(self, transaction_result: TransactionResultProtocol):
        mdib_version_group = self._mdib.mdib_version_group
        if transaction_result.has_descriptor_updates:
            port_type_impl = self.hosted_services.description_event_service
            updated = transaction_result.descr_updated
            created = transaction_result.descr_created
            deleted = transaction_result.descr_deleted
            states = transaction_result.all_states()
            port_type_impl.send_descriptor_updates(
                updated, created, deleted, states, mdib_version_group)

        states = transaction_result.metric_updates
        if len(states) > 0:
            port_type_impl = self.hosted_services.state_event_service
            port_type_impl.send_episodic_metric_report(
                states, mdib_version_group)
            self._periodic_reports_handler.store_metric_states(mdib_version_group.mdib_version,
                                                               transaction_result.metric_updates)

        states = transaction_result.alert_updates
        if len(states) > 0:
            port_type_impl = self.hosted_services.state_event_service
            port_type_impl.send_episodic_alert_report(
                states, mdib_version_group)
            self._periodic_reports_handler.store_alert_states(mdib_version_group.mdib_version, states)

        states = transaction_result.comp_updates
        if len(states) > 0:
            port_type_impl = self.hosted_services.state_event_service
            port_type_impl.send_episodic_component_state_report(
                states, mdib_version_group)
            self._periodic_reports_handler.store_component_states(mdib_version_group.mdib_version, states)

        states = transaction_result.ctxt_updates
        if len(states) > 0:
            port_type_impl = self.hosted_services.context_service
            port_type_impl.send_episodic_context_report(
                states, mdib_version_group)
            self._periodic_reports_handler.store_context_states(mdib_version_group.mdib_version, states)

        states = transaction_result.op_updates
        if len(states) > 0:
            port_type_impl = self.hosted_services.state_event_service
            port_type_impl.send_episodic_operational_state_report(states, mdib_version_group)
            self._periodic_reports_handler.store_operational_states(mdib_version_group.mdib_version, states)

        states = transaction_result.rt_updates
        if len(states) > 0:
            port_type_impl = self.hosted_services.waveform_service
            port_type_impl.send_realtime_samples_report(states, mdib_version_group)

    def _send_rt_notifications(self, rt_states: list[AbstractStateProtocol]):
        if len(rt_states) > 0:
            port_type_impl = self.hosted_services.waveform_service
            port_type_impl.send_realtime_samples_report(rt_states, self._mdib.mdib_version_group)

    def set_used_compression(self, *compression_methods: str):
        """Set supported compression methods, e.g. 'gzip'."""
        del self._compression_methods[:]
        self._compression_methods.extend(compression_methods)
