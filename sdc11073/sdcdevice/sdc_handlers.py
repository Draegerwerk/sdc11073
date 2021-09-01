import threading
import time
import traceback
import urllib
import uuid
from collections import namedtuple
from functools import reduce

from lxml import etree as etree_

from . import httpserver
from . import intervaltimer
from .sdcservicesimpl import SOAPActionDispatcher, DPWSHostedService
from .. import compression
from .. import loghelper
from .. import pmtypes
from .. import pysoap
from .. import roles
from .. import wsdiscovery
from ..location import SdcLocation
from ..namespaces import Prefixes, WSA_ANONYMOUS, DocNamespaceHelper, nsmap
from ..namespaces import domTag, wsdTag, wsxTag, dpwsTag

Soap12Envelope = pysoap.soapenvelope.Soap12Envelope

PROFILING = False
if PROFILING:
    pass

PeriodicStates = namedtuple('PeriodicStates', 'mdib_version states')


class SdcHandlerBase:
    ''' This is the base class for the sdc device handler. It contains all functionality of a device except the definition of the hosted services.
    These must be instantiated in a derived class.'''

    SSL_CIPHERS = 'HIGH:!3DES:!DSS:!aNULL@STRENGTH'

    WARN_LIMIT_REALTIMESAMPLES_BEHIND_SCHEDULE = 0.2  # warn limit when real time samples cannot be sent in time (typically because receiver is too slow)
    WARN_RATE_REALTIMESAMPLES_BEHIND_SCHEDULE = 5  # max. every x seconds a message

    DEFAULT_CONTEXTSTATES_IN_GETMDIB = True  # defines if get_mdib and getMdStates contain context states or not.
    # This is a default, it can be overidden per instande in
    # member "contextstates_in_getmdib".
    defaultInstanceIdentifiers = (pmtypes.InstanceIdentifier(root='rootWithNoMeaning', extension_string='System'),)

    def __init__(self, my_uuid, ws_discovery, model, device, device_mdib_container, validate,
                 roleProvider, ssl_context,
                 max_subscription_duration,
                 components,
                 log_prefix='', chunked_messages=False):  # pylint:disable=too-many-arguments
        """
        :param uuid: a string that becomes part of the devices url (no spaces, no special characters please. This could cause an invalid url!).
                     Parameter can be None, in this case a random uuid string is generated.
        :param ws_discovery: reference to the wsDiscovery instance
        :param model: a pysoap.soapenvelope.DPWSThisModel instance
        :param device: a pysoap.soapenvelope.DPWSThisDevice instance
        :param device_mdib_container: a DeviceMdibContainer instance
        :param roleProvider: handles the operation calls
        :param ssl_context: if not None, this context is used and https url is used. Otherwise http
        :param components: a dictionary
        :param log_prefix: a string
        :param max_subscription_duration: max. possible duration of a subscription, default is 7200 seconds
        :param ident: names a device, used for logging
        """
        self._my_uuid = my_uuid or uuid.uuid4()
        self._wsdiscovery = ws_discovery
        self.model = model
        self.device = device
        self._mdib = device_mdib_container
        self._log_prefix = log_prefix
        self._mdib.log_prefix = log_prefix
        self._validate = validate
        self._ssl_context = ssl_context
        self._components = components
        self._compression_methods = compression.CompressionHandler.available_encodings[:]
        self._http_server_thread = None
        # self._setup_logging(log_level)
        self._logger = loghelper.get_logger_adapter('sdc.device', log_prefix)

        self.chunked_messages = chunked_messages
        self.contextstates_in_getmdib = self.DEFAULT_CONTEXTSTATES_IN_GETMDIB  # can be overridden per instance

        msg_reader_cls = self._components.MsgReaderClass
        self.msg_reader = msg_reader_cls(self._logger)
        msg_factory_cls = self._components.MsgFactoryClass
        self.msg_factory = msg_factory_cls(sdc_definitions=device_mdib_container.sdc_definitions,
                                           logger=self._logger)

        # hostDispatcher provides data of the sdc device itself
        self._host_dispatcher = self._mk_host_dispatcher()

        self._get_dispatcher = None
        self._localization_dispatcher = None
        self._get_service_hosted = None
        self._context_dispatcher = None
        self._description_event_dispatcher = None
        self._state_event_dispatcher = None
        self._waveform_dispatcher = None
        self._sdc_service_hosted = None
        self._set_dispatcher = None
        self._set_service_hosted = None
        self._containment_tree_dispatcher = None
        self._containment_tree_service_hosted = None
        self._hosted_services = []
        self._url_dispatcher = None
        self._rt_sample_send_thread = None
        self._run_rt_sample_thread = False
        self.collect_rt_samples_period = 0.1  # in seconds
        if self._ssl_context is not None:
            self._urlschema = 'https'
        else:
            self._urlschema = 'http'

        self.dpws_host = None

        cls = self._components.SubscriptionsManagerClass
        self._subscriptions_manager = cls(self._ssl_context,
                                          self._mdib.sdc_definitions,
                                          self._mdib.biceps_schema,
                                          self.msg_factory,
                                          self._compression_methods,
                                          max_subscription_duration,
                                          log_prefix=self._log_prefix,
                                          chunked_messages=self.chunked_messages)

        # self._subscriptions_manager = self._mkSubscriptionManager(max_subscription_duration)

        cls = self._components.ScoOperationsRegistryClass
        operations_factory = self._components.OperationsFactory
        handle = '_sco'
        self._sco_operations_registry = cls(self._subscriptions_manager, operations_factory, self._mdib, handle,
                                            log_prefix=self._log_prefix)

        device_mdib_container.set_sdc_device(self)

        self.product_roles = roleProvider
        if self.product_roles is None:
            self.mk_default_role_handlers()

        self._location = None
        self._periodic_reports_lock = threading.Lock()
        self._periodic_reports_thread = None
        self._run_periodic_reports_thread = None
        self._periodic_reports_interval = None
        self._periodic_metric_reports = []
        self._periodic_alert_reports = []
        self._periodic_component_state_reports = []
        self._periodic_context_state_reports = []
        self._periodic_operational_state_reports = []
        self._last_log_time = 0
        self._last_logged_delay = 0

    def mk_scopes(self):
        scopes = []
        locations = self._mdib.context_states.NODETYPE.get(domTag('LocationContextState'), [])
        assoc_loc = [l for l in locations if l.ContextAssociation == pmtypes.ContextAssociation.ASSOCIATED]
        for loc in assoc_loc:
            det = loc.LocationDetail
            dr_loc = SdcLocation(fac=det.Facility, poc=det.PoC, bed=det.Bed, bld=det.Building,
                                 flr=det.Floor, rm=det.Room)
            scopes.append(wsdiscovery.Scope(dr_loc.scope_string))

        for nodetype, scheme in (
                ('OperatorContextDescriptor', 'sdc.ctxt.opr'),
                ('EnsembleContextDescriptor', 'sdc.ctxt.ens'),
                ('WorkflowContextDescriptor', 'sdc.ctxt.wfl'),
                ('MeansContextDescriptor', 'sdc.ctxt.mns'),
        ):
            descriptors = self._mdib.descriptions.NODETYPE.get(domTag(nodetype), [])
            for descriptor in descriptors:
                states = self._mdib.context_states.descriptorHandle.get(descriptor.Handle, [])
                assoc_st = [s for s in states if s.ContextAssociation == pmtypes.ContextAssociation.ASSOCIATED]
                for state in assoc_st:
                    for ident in state.Identification:
                        scopes.append(wsdiscovery.Scope('{}:/{}/{}'.format(scheme, urllib.parse.quote_plus(ident.Root),
                                                                           urllib.parse.quote_plus(ident.Extension))))

        scopes.extend(self._get_device_component_based_scopes())
        scopes.append(wsdiscovery.Scope('sdc.mds.pkp:1.2.840.10004.20701.1.1'))  # key purpose Service provider
        return scopes

    def _get_device_component_based_scopes(self):
        '''
        SDC: For every instance derived from pm:AbstractComplexDeviceComponentDescriptor in the MDIB an
        SDC SERVICE PROVIDER SHOULD include a URIencoded pm:AbstractComplexDeviceComponentDescriptor/pm:Type
        as dpws:Scope of the MDPWS discovery messages. The URI encoding conforms to the given Extended Backus-Naur Form.
        E.G.  sdc.cdc.type:///69650, sdc.cdc.type:/urn:oid:1.3.6.1.4.1.3592.2.1.1.0//DN_VMD
        After discussion with David: use only MDSDescriptor, VmdDescriptor makes no sense.
        :return: a set of scopes
        '''
        scopes = set()
        descriptors = self._mdib.descriptions.NODETYPE.get(domTag('MdsDescriptor'))
        for descriptor in descriptors:
            if descriptor.Type is not None:
                coding_systems = '' if descriptor.Type.CodingSystem == pmtypes.DEFAULT_CODING_SYSTEM \
                    else descriptor.Type.CodingSystem
                csv = descriptor.Type.CodingSystemVersion or ''
                scope = wsdiscovery.Scope('sdc.cdc.type:/{}/{}/{}'.format(coding_systems, csv, descriptor.Type.Code))
                scopes.add(scope)
        return scopes

    def _mk_host_dispatcher(self):
        host_dispatcher = SOAPActionDispatcher()
        host_dispatcher.register_action_callback('{}/Get'.format(Prefixes.WXF.namespace), self._on_get_metadata)
        host_dispatcher.register_action_callback('{}/Probe'.format(Prefixes.WSD.namespace), self._on_probe_request)
        host_dispatcher.epr = '/' + str(self._my_uuid.hex)
        return host_dispatcher

    def mk_default_role_handlers(self):
        self.product_roles = roles.product.MinimalProduct(self._log_prefix)

    @property
    def _bmm_schema(self):
        return None if not self._validate else self._mdib.biceps_schema.message_schema

    @property
    def shall_validate(self):
        return self._validate

    @property
    def mdib(self):
        return self._mdib

    @property
    def subscriptions_manager(self):
        return self._subscriptions_manager

    @property
    def epr(self):
        # End Point Reference, e.g 'urn:uuid:8c26f673-fdbf-4380-b5ad-9e2454a65b6b'
        return self._my_uuid.urn

    @property
    def path_prefix(self):
        # http path prefix of service e.g '8c26f673-fdbf-4380-b5ad-9e2454a65b6b'
        return self._my_uuid.hex

    @property
    def sco_operations_registry(self):
        return self._sco_operations_registry

    def register_operation(self, operation):
        self._sco_operations_registry.register_operation(operation)

    def unregister_operation_by_handle(self, operation_handle):
        self._sco_operations_registry.register_operation(operation_handle)

    def get_operation_by_handle(self, operation_handle):
        return self._sco_operations_registry.get_operation_by_handle(operation_handle)

    def enqueue_operation(self, operation, request, argument):
        return self._sco_operations_registry.enqueue_operation(operation, request, argument)

    def dispatch_get_request(self, parse_result, headers):
        ''' device itself can also handle GET requests. This is the handler'''
        return self._host_dispatcher.dispatch_get_request(parse_result, headers)

    def _start_services(self, shared_http_server=None):
        ''' start the services'''
        self._logger.info('starting services, addr = {}', self._wsdiscovery.get_active_addresses())

        self._sco_operations_registry.start_worker()
        if shared_http_server:
            self._http_server_thread = shared_http_server
        else:
            self._http_server_thread = httpserver.DeviceHttpServerThread(
                my_ipaddress='0.0.0.0', ssl_context=self._ssl_context, supported_encodings=self._compression_methods,
                log_prefix=self._log_prefix, chunked_responses=self.chunked_messages)

            # first start http server, the services need to know the ip port number
            self._http_server_thread.start()
            event_is_set = self._http_server_thread.started_evt.wait(timeout=15.0)
            if not event_is_set:
                self._logger.error('Cannot start device, start event of http server not set.')
                raise RuntimeError('Cannot start device, start event of http server not set.')

        host_ips = self._wsdiscovery.get_active_addresses()
        self._url_dispatcher = httpserver.HostedServiceDispatcher(self._mdib.sdc_definitions, self._logger)
        self._http_server_thread.dispatcher.register_device_dispatcher(self.path_prefix, self._url_dispatcher)
        if len(host_ips) == 0:
            self._logger.error('Cannot start device, there is no IP address to bind it to.')
            raise RuntimeError('Cannot start device, there is no IP address to bind it to.')

        port = self._http_server_thread.my_port
        if port is None:
            self._logger.error('Cannot start device, could not bind HTTP server to a port.')
            raise RuntimeError('Cannot start device, could not bind HTTP server to a port.')

        base_urls = []  # e.g https://192.168.1.5:8888/8c26f673-fdbf-4380-b5ad-9e2454a65b6b; list has one member for each used ip address
        for addr in host_ips:
            base_urls.append(
                urllib.parse.SplitResult(self._urlschema, '{}:{}'.format(addr, port), self.path_prefix, query=None,
                                         fragment=None))
        self.dpws_host = pysoap.soapenvelope.DPWSHost(
            endpoint_references_list=[pysoap.soapenvelope.WsaEndpointReferenceType(self.epr)],
            types_list=self._mdib.sdc_definitions.MedicalDeviceTypesFilter)
        # register two addresses for hostDispatcher: '' and /<uuid>
        self._url_dispatcher.register_hosted_service(self._host_dispatcher)

        self._register_hosted_services(base_urls)

        for host_ip in host_ips:
            self._logger.info('serving Services on {}:{}', host_ip, port)
        self._subscriptions_manager.set_base_urls(base_urls)

    def _register_hosted_services(self, base_urls):
        pass  # to be implemented in derived classes

    def start_all(self, start_rtsample_loop=True, periodic_reports_interval=None, shared_http_server=None):
        if self.product_roles is not None:
            self.product_roles.init_operations(self._mdib, self._sco_operations_registry)

        self._start_services(shared_http_server)
        if start_rtsample_loop:
            self._run_rt_sample_thread = True
            self._rt_sample_send_thread = threading.Thread(target=self._rt_sample_sendloop, name='DevRtSampleSendLoop')
            self._rt_sample_send_thread.daemon = True
            self._rt_sample_send_thread.start()
        if periodic_reports_interval:
            # This setting activates the simple periodic send loop, retrievability settings are ignored
            self._run_periodic_reports_thread = True
            self._periodic_reports_interval = periodic_reports_interval
            self._periodic_reports_thread = threading.Thread(target=self._simple_periodic_reports_send_loop,
                                                             name='DevPeriodicSendLoop')
            self._periodic_reports_thread.daemon = True
            self._periodic_reports_thread.start()
        elif self._mdib.retrievability_periodic:
            # Periodic Retrievalility is set at least once, start handler loop
            self._run_periodic_reports_thread = True
            self._periodic_reports_interval = periodic_reports_interval
            self._periodic_reports_thread = threading.Thread(target=self._periodic_reports_send_loop,
                                                             name='DevPeriodicSendLoop')
            self._periodic_reports_thread.daemon = True
            self._periodic_reports_thread.start()

    def stop_all(self, close_all_connections, send_subscription_end):
        self.stop_realtime_sample_loop()
        if self._run_periodic_reports_thread:
            self._run_periodic_reports_thread = False
            self._periodic_reports_thread.join()

        self._subscriptions_manager.end_all_subscriptions(send_subscription_end)
        self._sco_operations_registry.stop_worker()
        self._http_server_thread.stop(close_all_connections)
        try:
            self._wsdiscovery.clear_service(self.epr)
        except KeyError:
            self._logger.info('epr "{}" not known in self._wsdiscovery'.format(self.epr))

        if self.product_roles is not None:
            self.product_roles.stop()

    def stop_realtime_sample_loop(self):
        if self._rt_sample_send_thread is not None:
            self._run_rt_sample_thread = False
            self._rt_sample_send_thread.join()
            self._rt_sample_send_thread = None

    def get_xaddrs(self):
        addresses = self._wsdiscovery.get_active_addresses()  # these own IP addresses are currently used by discovery
        port = self._http_server_thread.my_port
        xaddrs = []
        for addr in addresses:
            xaddrs.append('{}://{}:{}/{}'.format(self._urlschema, addr, port, self.path_prefix))
        return xaddrs

    def _on_get_metadata(self, http_header, request):  # pylint: disable=unused-argument
        self._logger.info('_on_get_metadata')
        _nsm = self._mdib.nsmapper
        response = pysoap.soapenvelope.Soap12Envelope(_nsm.doc_ns_map)
        reply_address = request.address.mk_reply_address('{}/GetResponse'.format(Prefixes.WXF.namespace))
        reply_address.addr_to = WSA_ANONYMOUS
        reply_address.message_id = uuid.uuid4().urn
        response.add_header_object(reply_address)
        metadata_node = self._mk_metadata_node()
        response.add_body_element(metadata_node)
        response.validate_body(self.mdib.biceps_schema.mex_schema)
        self._logger.debug('returned meta data = {}', response.as_xml(pretty=False))
        return response

    def _on_probe_request(self, http_header, request):  # pylint: disable=unused-argument
        _nsm = DocNamespaceHelper()
        response = pysoap.soapenvelope.Soap12Envelope(_nsm.doc_ns_map)
        reply_address = request.address.mk_reply_address('{}/ProbeMatches'.format(Prefixes.WSD.namespace))
        reply_address.addr_to = WSA_ANONYMOUS
        reply_address.message_id = uuid.uuid4().urn
        response.add_header_object(reply_address)
        probe_match_node = etree_.Element(wsdTag('Probematch'),
                                          nsmap=_nsm.doc_ns_map)
        types = etree_.SubElement(probe_match_node, wsdTag('Types'))
        types.text = '{}:Device {}:MedicalDevice'.format(Prefixes.DPWS.prefix, Prefixes.MDPWS.prefix)
        scopes = etree_.SubElement(probe_match_node, wsdTag('Scopes'))
        scopes.text = ''
        xaddrs = etree_.SubElement(probe_match_node, wsdTag('XAddrs'))
        xaddrs.text = ' '.join(self.get_xaddrs())
        response.add_body_element(probe_match_node)
        return response

    def _validate_dpws(self, node):
        if not self.shall_validate:
            return
        try:
            self.mdib.biceps_schema.dpws_schema.assertValid(node)
        except etree_.DocumentInvalid as ex:
            tmp_str = etree_.tostring(node, pretty_print=True).decode('utf-8')
            self._logger.error('invalid dpws: {}\ndata = {}', ex, tmp_str)
            raise

    def _mk_metadata_node(self):
        metadata_node = etree_.Element(wsxTag('Metadata'),
                                       nsmap=self._mdib.nsmapper.doc_ns_map)

        # ThisModel
        metadata_section_node = etree_.SubElement(metadata_node,
                                                  wsxTag('MetadataSection'),
                                                  attrib={'Dialect': '{}/ThisModel'.format(nsmap['dpws'])})
        self.model.as_etree_subnode(metadata_section_node)
        self._validate_dpws(metadata_section_node[-1])

        # ThisDevice
        metadata_section_node = etree_.SubElement(metadata_node,
                                                  wsxTag('MetadataSection'),
                                                  attrib={'Dialect': '{}/ThisDevice'.format(nsmap['dpws'])})
        self.device.as_etree_subnode(metadata_section_node)

        self._validate_dpws(metadata_section_node[-1])

        # Relationship
        metadata_section_node = etree_.SubElement(metadata_node,
                                                  wsxTag('MetadataSection'),
                                                  attrib={'Dialect': '{}/Relationship'.format(nsmap['dpws'])})
        relationship_node = etree_.SubElement(metadata_section_node,
                                              dpwsTag('Relationship'),
                                              attrib={'Type': '{}/host'.format(nsmap['dpws'])})

        self.dpws_host.as_etree_subnode(relationship_node)
        self._validate_dpws(relationship_node[-1])

        # add all hosted services:
        for service in self._hosted_services:
            service.hosted_inf.as_etree_subnode(relationship_node)
            self._validate_dpws(relationship_node[-1])
        return metadata_node

    def _store_for_periodic_report(self, mdib_version, state_updates, dest_list):
        if self._run_periodic_reports_thread:
            copied_updates = [s.mk_copy() for s in state_updates]
            with self._periodic_reports_lock:
                dest_list.append(PeriodicStates(mdib_version, copied_updates))

    def send_metric_state_updates(self, mdib_version, state_updates):
        self._logger.debug('sending metric state updates {}', state_updates)
        self._subscriptions_manager.send_episodic_metric_report(state_updates, self._mdib.nsmapper, mdib_version,
                                                                self.mdib.sequence_id)
        self._store_for_periodic_report(mdib_version, state_updates, self._periodic_metric_reports)

    def send_alert_state_updates(self, mdib_version, state_updates):
        self._logger.debug('sending alert updates {}', state_updates)
        self._subscriptions_manager.send_episodic_alert_report(state_updates, self._mdib.nsmapper, mdib_version,
                                                               self.mdib.sequence_id)
        self._store_for_periodic_report(mdib_version, state_updates, self._periodic_alert_reports)

    def send_component_state_updates(self, mdib_version, state_updates):
        self._logger.debug('sending component state updates {}', state_updates)
        self._subscriptions_manager.send_episodic_component_state_report(state_updates, self._mdib.nsmapper,
                                                                         mdib_version,
                                                                         self.mdib.sequence_id)
        self._store_for_periodic_report(mdib_version, state_updates, self._periodic_component_state_reports)

    def send_context_state_updates(self, mdib_version, state_updates):
        self._logger.debug('sending context updates {}', state_updates)
        self._subscriptions_manager.send_episodic_context_report(state_updates, self._mdib.nsmapper, mdib_version,
                                                                 self.mdib.sequence_id)
        self._store_for_periodic_report(mdib_version, state_updates, self._periodic_context_state_reports)

    def send_operational_state_updates(self, mdib_version, state_updates):
        self._logger.debug('sending operational state updates {}', state_updates)
        self._subscriptions_manager.send_episodic_operational_state_report(state_updates, self._mdib.nsmapper,
                                                                           mdib_version,
                                                                           self.mdib.sequence_id)
        self._store_for_periodic_report(mdib_version, state_updates, self._periodic_operational_state_reports)

    def send_realtime_samples_state_updates(self, mdib_version, state_updates):
        self._logger.debug('sending real time sample state updates {}', state_updates)
        self._subscriptions_manager.send_realtime_samples_report(state_updates, self._mdib.nsmapper, mdib_version,
                                                                 self.mdib.sequence_id)

    def send_descriptor_updates(self, mdib_version, updated, created, deleted, updated_states):
        self._logger.debug('sending descriptor updates updated={} created={} deleted={}', updated, created, deleted)
        self._subscriptions_manager.send_descriptor_updates(updated, created, deleted, updated_states,
                                                            self._mdib.nsmapper,
                                                            mdib_version,
                                                            self.mdib.sequence_id)

    def _rt_sample_sendloop(self):
        """Periodically send waveform samples."""
        # start delayed in order to have a fully initialized device when waveforms start
        # (otherwise timing issues might happen)
        time.sleep(0.1)
        timer = intervaltimer.IntervalTimer(period_in_seconds=self.collect_rt_samples_period)
        try:
            while self._run_rt_sample_thread:
                behind_schedule_seconds = timer.wait_next_interval_begin()
                try:
                    self._mdib.update_all_rt_samples()  # update from waveform generators
                    self._log_waveform_timing(behind_schedule_seconds)
                except Exception:
                    self._logger.warn(' could not update real time samples: {}', traceback.format_exc())
            self._logger.info('_run_rt_sample_thread = False')
        finally:
            self._logger.info('rt_sample_sendloop end')

    def _simple_periodic_reports_send_loop(self):
        """This is a very basic implementation of periodic reports, it only supports fixed interval.
        It does not care about retrievability settings in the mdib.
        """
        self._logger.debug('_simple_periodic_reports_send_loop start')
        time.sleep(0.1)  # start delayed
        timer = intervaltimer.IntervalTimer(period_in_seconds=self._periodic_reports_interval)
        while self._run_periodic_reports_thread:
            timer.wait_next_interval_begin()
            self._logger.debug('_simple_periodic_reports_send_loop')
            mgr = self._subscriptions_manager
            for reports_list, send_func, msg in \
                    [(self._periodic_metric_reports, mgr.send_periodic_metric_report, 'metric'),
                     (self._periodic_alert_reports, mgr.send_periodic_alert_report, 'alert'),
                     (self._periodic_component_state_reports, mgr.send_periodic_component_state_report, 'component'),
                     (self._periodic_context_state_reports, mgr.send_periodic_context_report, 'context'),
                     (self._periodic_operational_state_reports, mgr.send_periodic_operational_state_report, 'operational'),
                     ]:
                tmp = None
                with self._periodic_reports_lock:
                    if reports_list:
                        tmp = reports_list[:]
                        del reports_list[:]
                if tmp:
                    self._logger.debug('send periodic %s report', msg)
                    send_func(tmp, self._mdib.nsmapper, self.mdib.sequence_id)

    def _periodic_reports_send_loop(self):
        """This implementation of periodic reports send loop considers retrievability settings in the mdib.
        """

        # helper for reduce
        def _next(x, y):  # pylint: disable=invalid-name
            return x if x[1].remaining_time() < y[1].remaining_time() else y

        self._logger.debug('_periodic_reports_send_loop start')
        time.sleep(0.1)  # start delayed
        # create an interval timer for each period
        timers = {}
        for period_ms in self._mdib.retrievability_periodic.keys():
            timers[period_ms] = intervaltimer.IntervalTimer(period_in_seconds=period_ms / 1000)
        while self._run_periodic_reports_thread:
            # find timer with shortest remaining time
            period_ms, timer = reduce(lambda x, y: _next(x, y), timers.items())  # pylint: disable=invalid-name
            timer.wait_next_interval_begin()
            self._logger.debug('_periodic_reports_send_loop {} msec timer', period_ms)
            all_handles = self._mdib.retrievability_periodic.get(period_ms, [])
            # separate them by notification types
            metrics = []
            components = []
            alerts = []
            operationals = []
            contexts = []
            for handle in all_handles:
                descr = self._mdib.descriptions.handle.get_one(handle)
                if descr.isMetricDescriptor and not descr.isRealtimeSampleArrayMetricDescriptor:
                    metrics.append(handle)
                elif descr.isSystemContextDescriptor or descr.isComponentDescriptor:
                    components.append(handle)
                elif descr.isAlertDescriptor:
                    alerts.append(handle)
                elif descr.isOperationalDescriptor:
                    operationals.append(handle)
                elif descr.isContextDescriptor:
                    contexts.append(handle)

            with self._mdib.mdib_lock:
                mdib_version = self._mdib.mdib_version
                sequence_id = self._mdib.sequence_id
                metric_states = [self._mdib.states.descriptorHandle.get_one(h).mk_copy() for h in metrics]
                component_states = [self._mdib.states.descriptorHandle.get_one(h).mk_copy() for h in components]
                alert_states = [self._mdib.states.descriptorHandle.get_one(h).mk_copy() for h in alerts]
                operational_states = [self._mdib.states.descriptorHandle.get_one(h).mk_copy() for h in operationals]
                context_states = []
                for context in contexts:
                    context_states.extend(
                        [st.mk_copy() for st in self._mdib.context_states.descriptorHandle.get(context, [])])
            self._logger.debug('   _periodic_reports_send_loop {} metric_states', len(metric_states))
            self._logger.debug('   _periodic_reports_send_loop {} component_states', len(component_states))
            self._logger.debug('   _periodic_reports_send_loop {} alert_states', len(alert_states))
            self._logger.debug('   _periodic_reports_send_loop {} alert_states', len(alert_states))
            self._logger.debug('   _periodic_reports_send_loop {} context_states', len(context_states))
            if metric_states:
                periodic_states = PeriodicStates(mdib_version, metric_states)
                self._subscriptions_manager.send_periodic_metric_report(
                    [periodic_states], self._mdib.nsmapper, sequence_id)
            if component_states:
                periodic_states = PeriodicStates(mdib_version, component_states)
                self._subscriptions_manager.send_periodic_component_state_report(
                    [periodic_states], self._mdib.nsmapper, sequence_id)
            if alert_states:
                periodic_states = PeriodicStates(mdib_version, alert_states)
                self._subscriptions_manager.send_periodic_alert_report(
                    [periodic_states], self._mdib.nsmapper, sequence_id)
            if operational_states:
                periodic_states = PeriodicStates(mdib_version, operational_states)
                self._subscriptions_manager.send_periodic_operational_state_report(
                    [periodic_states], self._mdib.nsmapper, sequence_id)
            if context_states:
                periodic_states = PeriodicStates(mdib_version, context_states)
                self._subscriptions_manager.send_periodic_context_report(
                    [periodic_states], self._mdib.nsmapper, sequence_id)

    # def _setup_logging(self, log_level):
    #     loghelper.ensure_log_stream()
    #     if log_level is None:
    #         return
    #     logging.getLogger('sdc.device').setLevel(log_level)

    def _log_waveform_timing(self, behind_schedule_seconds):
        try:
            last_log_time = self._last_log_time
        except AttributeError:
            self._last_log_time = 0
            last_log_time = self._last_log_time
        try:
            last_logged_delay = self._last_logged_delay
        except AttributeError:
            self._last_logged_delay = 0
            last_logged_delay = self._last_logged_delay

        # max. one log per second
        now = time.monotonic()
        if now - last_log_time < self.WARN_RATE_REALTIMESAMPLES_BEHIND_SCHEDULE:
            return
        if last_logged_delay >= self.WARN_LIMIT_REALTIMESAMPLES_BEHIND_SCHEDULE and behind_schedule_seconds < self.WARN_LIMIT_REALTIMESAMPLES_BEHIND_SCHEDULE:
            self._logger.info('RealTimeSampleTimer delay is back inside limit of {:.2f} seconds (mdib version={}',
                              self.WARN_LIMIT_REALTIMESAMPLES_BEHIND_SCHEDULE, self._mdib.mdib_version)
            self._last_logged_delay = behind_schedule_seconds
            self._last_log_time = now
        elif behind_schedule_seconds >= self.WARN_LIMIT_REALTIMESAMPLES_BEHIND_SCHEDULE:
            self._logger.warn('RealTimeSampleTimer is {:.4f} seconds behind schedule (mdib version={})',
                              behind_schedule_seconds, self._mdib.mdib_version)
            self._last_logged_delay = behind_schedule_seconds
            self._last_log_time = now

    def set_used_compression(self, *compression_methods):
        # update list in place
        del self._compression_methods[:]
        self._compression_methods.extend(compression_methods)


class SdcHandlerFull(SdcHandlerBase):
    """ This class instantiates all port types."""

    def _register_hosted_services(self, base_urls):
        # register all services with their endpoint references acc. to sdc standard
        actions = self._mdib.sdc_definitions.Actions
        service_handlers_lookup = self._components.ServiceHandlers

        cls = service_handlers_lookup['GetService']
        self._get_dispatcher = cls('GetService', self)
        cls = service_handlers_lookup['LocalizationService']
        self._localization_dispatcher = cls('LocalizationService', self)
        offered_subscriptions = []
        self._get_service_hosted = DPWSHostedService(self, base_urls, 'Get',
                                                     [self._get_dispatcher, self._localization_dispatcher],
                                                     offered_subscriptions)
        self._url_dispatcher.register_hosted_service(self._get_service_hosted)

        # grouped acc to sdc REQ 0035
        cls = service_handlers_lookup['ContextService']
        self._context_dispatcher = cls('ContextService', self)
        cls = service_handlers_lookup['DescriptionEventService']
        self._description_event_dispatcher = cls('DescriptionEventService', self)
        cls = service_handlers_lookup['StateEventService']
        self._state_event_dispatcher = cls('StateEventService', self)
        cls = service_handlers_lookup['WaveformService']
        self._waveform_dispatcher = cls('WaveformService', self)

        offered_subscriptions = [actions.EpisodicContextReport,
                                 actions.DescriptionModificationReport,
                                 actions.EpisodicMetricReport,
                                 actions.EpisodicAlertReport,
                                 actions.EpisodicComponentReport,
                                 actions.EpisodicOperationalStateReport,
                                 actions.Waveform,
                                 actions.SystemErrorReport,
                                 actions.PeriodicMetricReport,
                                 actions.PeriodicAlertReport,
                                 actions.PeriodicContextReport,
                                 actions.PeriodicComponentReport,
                                 actions.PeriodicOperationalStateReport
                                 ]

        self._sdc_service_hosted = DPWSHostedService(self, base_urls, 'StateEvent',
                                                     [self._context_dispatcher,
                                                      self._description_event_dispatcher,
                                                      self._state_event_dispatcher,
                                                      self._waveform_dispatcher],
                                                     offered_subscriptions)
        self._url_dispatcher.register_hosted_service(self._sdc_service_hosted)

        cls = service_handlers_lookup['SetService']
        self._set_dispatcher = cls('SetService', self)
        offered_subscriptions = [actions.OperationInvokedReport]

        self._set_service_hosted = DPWSHostedService(self, base_urls, 'Set', [self._set_dispatcher],
                                                     offered_subscriptions)
        self._url_dispatcher.register_hosted_service(self._set_service_hosted)

        cls = service_handlers_lookup['ContainmentTreeService']
        self._containment_tree_dispatcher = cls('ContainmentTreeService', self)
        offered_subscriptions = []
        self._containment_tree_service_hosted = DPWSHostedService(self, base_urls, 'ContainmentTree',
                                                                  [self._containment_tree_dispatcher],
                                                                  offered_subscriptions)
        self._url_dispatcher.register_hosted_service(self._containment_tree_service_hosted)
        self._hosted_services = [self._get_service_hosted,
                                 self._sdc_service_hosted,
                                 self._set_service_hosted,
                                 self._containment_tree_service_hosted]


class SdcHandlerMinimal(SdcHandlerBase):
    """This class instantiates only GetService and LocalizationService"""

    def _register_hosted_services(self, base_urls):
        service_handlers_lookup = self._components['ServiceHandlers']
        cls = service_handlers_lookup['GetService']
        self._get_dispatcher = cls('GetService', self)
        cls = service_handlers_lookup['LocalizationService']
        self._localization_dispatcher = cls('LocalizationService', self)
        offered_subscriptions = []
        self._get_service_hosted = DPWSHostedService(self, base_urls, 'Get',
                                                     [self._get_dispatcher, self._localization_dispatcher],
                                                     offered_subscriptions)
        self._url_dispatcher.register_hosted_service(self._get_service_hosted)

        self._url_dispatcher.register_hosted_service(self._containment_tree_service_hosted)
        self._hosted_services = [self._get_service_hosted]
