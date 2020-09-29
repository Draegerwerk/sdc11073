import uuid
import json
import os
import time
import ssl
import urllib
import logging
import threading
import traceback

from lxml import etree as etree_

from .. import compression
from .. import loghelper
from .. import pmtypes
from .. import wsdiscovery
from ..location import SdcLocation
from .. import namespaces
from .. import pysoap

from .sdcservicesimpl import SOAPActionDispatcher, DPWSHostedService
from .sdcservicesimpl import GetService, SetService, StateEventService,  ContainmentTreeService, ContextService, WaveformService, DescriptionEventService
from .localizationservice import LocalizationService
from . import subscriptionmgr
from . import sco
from . import httpserver
from . import intervaltimer

Soap12Envelope = pysoap.soapenvelope.Soap12Envelope

Prefix = namespaces.Prefix_Namespace

PROFILING = False
if PROFILING:
    import cProfile
    import pstats
    from io import StringIO


# default ssl context data
here = os.path.dirname(__file__)
caFolder = os.path.join(os.path.dirname(here), 'ca')
_ssl_certfile = os.path.join(caFolder, 'sdccert.pem') # this is the certification chain ( contains root ca and signed public key
_ssl_keyfile = os.path.join(caFolder, 'userkey.pem')     # this is the private key of own certificate
_ssl_cacert = os.path.join(caFolder, 'cacert.pem')    # this is the common root ca that signed all sdc devices
_ssl_passwd = 'dummypass' #'Phase1' #dummypass
_ssl_cypherfile = os.path.join(caFolder, 'cyphers.json') # Json file that determines ciphers to be used


class SdcHandler_Base(object):
    ''' This is the base class for the sdc device handler. It contains all functionality of a device except the definition of the hosted services.
    These must be instantiated in a derived class.'''

    SSL_CIPHERS = 'HIGH:!3DES:!DSS:!aNULL@STRENGTH'

    WARN_LIMIT_REALTIMESAMPLES_BEHIND_SCHEDULE = 0.2  # warn limit when real time samples cannot be sent in time (typically because receiver is too slow)
    WARN_RATE_REALTIMESAMPLES_BEHIND_SCHEDULE = 5  # max. every x seconds a message

    DEFAULT_CONTEXTSTATES_IN_GETMDIB = True  # defines if getMdib and getMdStates contain context states or not.
    # This is a default, it can be overidden per instande in
    # member "contextstates_in_getmdib".
    defaultInstanceIdentifiers = (pmtypes.InstanceIdentifier(root='rootWithNoMeaning', extensionString='System'),)

    def __init__(self, my_uuid, ws_discovery, model, device, deviceMdibContainer, validate=True,
                 roleProvider=None, sslContext=None,
                 logLevel=None, max_subscription_duration=7200, log_prefix='', chunked_messages=False):  # pylint:disable=too-many-arguments
        """
        @param uuid: a string that becomes part of the devices url (no spaces, no special characters please. This could cause an invalid url!).
                     Parameter can be None, in this case a random uuid string is generated.
        @param ws_discovery: reference to the wsDiscovery instance
        @param model: a pysoap.soapenvelope.DPWSThisModel instance
        @param device: a pysoap.soapenvelope.DPWSThisDevice instance
        @param deviceMdibContainer: a DeviceMdibContainer instance
        @param roleProvider: handles the operation calls
        @param sslContext: if not None, this context is used and https url is used. Otherwise http
        @param logLevel: if not None, the "sdc.device" logger will use this level
        @param max_subscription_duration: max. possible duration of a subscription, default is 7200 seconds
        @param ident: names a device, used for logging
        """
        self._my_uuid = my_uuid or uuid.uuid4()
        self._wsdiscovery = ws_discovery
        self.model = model
        self.device = device
        self._mdib = deviceMdibContainer
        self._log_prefix = log_prefix
        self._mdib.log_prefix = log_prefix
        self._validate = validate
        self._sslContext = sslContext
        self._compression_methods = compression.encodings[:]
        self._httpServerThread = None
        self._setupLogging(logLevel)
        self._logger = loghelper.getLoggerAdapter('sdc.device', log_prefix)

        self.chunked_messages = chunked_messages
        self.contextstates_in_getmdib = self.DEFAULT_CONTEXTSTATES_IN_GETMDIB  # can be overridden per instance
        # hostDispatcher provides data of the sdc device itself
        self._hostDispatcher = self._mkHostDispatcher()

        self._GetDispatcher = None
        self._LocalizationDispatcher = None
        self._GetServiceHosted = None
        self._ContextDispatcher = None
        self._DescriptionEventDispatcher = None
        self._StateEventDispatcher = None
        self._WaveformDispatcher = None
        self._SdcServiceHosted = None
        self.__SetDispatcher = None
        self._SetServiceHosted = None
        self._hostedServices = []
        self._url_dispatcher = None
        self._rtSampleSendThread = None
        self._runRtSampleThread = False
        self.collectRtSamplesPeriod = 0.1  # in seconds
        if self._sslContext is not None:
            self._urlschema = 'https'
        else:
            self._urlschema = 'http'

        self.dpwsHost = None
        self._subscriptionsManager = self._mkSubscriptionManager(max_subscription_duration)
        self._scoOperationsRegistry = self._mkScoOperationsRegistry(handle='_sco')

        deviceMdibContainer.setSdcDevice(self)

        self.product_roles = roleProvider
        if self.product_roles is None:
            self.mkDefaultRoleHandlers()

        self._location = None

    def mkScopes(self):
        scopes = []
        locations = self._mdib.contextStates.NODETYPE.get(namespaces.domTag('LocationContextState'))
        assoc_loc = [l for l in locations if l.ContextAssociation == pmtypes.ContextAssociation.ASSOCIATED]
        for loc in assoc_loc:
            dr_loc = SdcLocation(fac=loc.Facility, poc=loc.PoC, bed=loc.Bed, bld=loc.Building,
                                 flr=loc.Floor, rm=loc.Room)
            scopes.append(wsdiscovery.Scope(dr_loc.scopeStringSdc))

        for nodetype, scheme in (
                ('OperatorContextDescriptor', 'sdc.ctxt.opr'),
                ('EnsembleContextDescriptor', 'sdc.ctxt.ens'),
                ('WorkflowContextDescriptor', 'sdc.ctxt.wfl'),
                ('MeansContextDescriptor', 'sdc.ctxt.mns'),
        ):
            descriptors = self._mdib.descriptions.NODETYPE.get(namespaces.domTag(nodetype), [])
            for descriptor in descriptors:
                states = self._mdib.contextStates.descriptorHandle.get(descriptor.Handle, [])
                assoc_st = [s for s in states if s.ContextAssociation == pmtypes.ContextAssociation.ASSOCIATED]
                for st in assoc_st:
                    for ident in st.Identification:
                        scopes.append(wsdiscovery.Scope('{}:/{}/{}'.format(scheme, urllib.parse.quote_plus(ident.Root),
                                                                           urllib.parse.quote_plus(ident.Extension))))

        scopes.extend(self._getDeviceComponentBasedScopes())
        scopes.append(wsdiscovery.Scope('sdc.mds.pkp:1.2.840.10004.20701.1.1'))  # key purpose Service provider
        return scopes

    def _getDeviceComponentBasedScopes(self):
        '''
        SDC: For every instance derived from pm:AbstractComplexDeviceComponentDescriptor in the MDIB an
        SDC SERVICE PROVIDER SHOULD include a URIencoded pm:AbstractComplexDeviceComponentDescriptor/pm:Type
        as dpws:Scope of the MDPWS discovery messages. The URI encoding conforms to the given Extended Backus-Naur Form.
        E.G.  sdc.cdc.type:///69650, sdc.cdc.type:/urn:oid:1.3.6.1.4.1.3592.2.1.1.0//DN_VMD
        After discussion with David: use only MDSDescriptor, VmdDescriptor makes no sense.
        :return: a set of scopes
        '''
        scopes = set()
        for t in (namespaces.domTag('MdsDescriptor'),):
            descriptors = self._mdib.descriptions.NODETYPE.get(t)
            for d in descriptors:
                if d.Type is not None:
                    cs = '' if d.Type.CodingSystem == pmtypes.DefaultCodingSystem else d.Type.CodingSystem
                    csv = d.Type.CodingSystemVersion or ''
                    sc = wsdiscovery.Scope('sdc.cdc.type:/{}/{}/{}'.format(cs, csv, d.Type.Code))
                    scopes.add(sc)
        return scopes

    def _mkHostDispatcher(self):
        hostDispatcher = SOAPActionDispatcher()
        hostDispatcher.register_soapActionCallback('{}/Get'.format(Prefix.WXF.namespace), self._onGetMetaData)
        hostDispatcher.register_soapActionCallback('{}/Probe'.format(Prefix.WSD.namespace), self._onProbeRequest)
        hostDispatcher.epr = '/' + str(self._my_uuid.hex)
        return hostDispatcher

    def _mkSubscriptionManager(self, max_subscription_duration):
        return subscriptionmgr.SubscriptionsManager(self._sslContext,
                                                    self._mdib.sdc_definitions,
                                                    self._mdib.bicepsSchema,
                                                    self._compression_methods,
                                                    max_subscription_duration,
                                                    log_prefix=self._log_prefix,
                                                    chunked_messages=self.chunked_messages)

    def _mkScoOperationsRegistry(self, handle):
        return sco.ScoOperationsRegistry(self._subscriptionsManager, self._mdib, handle, log_prefix=self._log_prefix)

    def mkDefaultRoleHandlers(self):
        from .. import roles
        self.product_roles = roles.product.MinimalProduct(self._log_prefix)

    @property
    def _bmmSchema(self):
        return None if not self._validate else self._mdib.bicepsSchema.bmmSchema

    @property
    def shallValidate(self):
        return self._validate

    @property
    def mdib(self):
        return self._mdib

    @property
    def subscriptionsManager(self):
        return self._subscriptionsManager

    @property
    def epr(self):
        # End Point Reference, e.g 'urn:uuid:8c26f673-fdbf-4380-b5ad-9e2454a65b6b'
        return self._my_uuid.urn

    @property
    def path_prefix(self):
        # http path prefix of service e.g '8c26f673-fdbf-4380-b5ad-9e2454a65b6b'
        return self._my_uuid.hex

    def registerOperation(self, operation):
        self._scoOperationsRegistry.registerOperation(operation)

    def unRegisterOperationByHandle(self, operationHandle):
        self._scoOperationsRegistry.registerOperation(operationHandle)

    def getOperationByHandle(self, operationHandle):
        return self._scoOperationsRegistry.getOperationByHandle(operationHandle)

    def enqueueOperation(self, operation, request):
        return self._scoOperationsRegistry.enqueueOperation(operation, request)

    def dispatchGetRequest(self, parseResult, headers):
        ''' device itself can also handle GET requests. This is the handler'''
        return self._hostDispatcher.dispatchGetRequest(parseResult, headers)

    def _startServices(self, shared_http_server=None):
        ''' start the services'''
        self._logger.info('starting services, addr = {}', self._wsdiscovery.getActiveAddresses())

        self._scoOperationsRegistry.startWorker()
        if shared_http_server:
            self._httpServerThread = shared_http_server
        else:
            self._httpServerThread = httpserver.HttpServerThread(my_ipaddress='0.0.0.0',
                                                                 sslContext=self._sslContext,
                                                                 supportedEncodings=self._compression_methods,
                                                                 log_prefix=self._log_prefix,
                                                                 chunked_responses=self.chunked_messages)

            # first start http server, the services need to know the ip port number
            self._httpServerThread.start()
            event_is_set = self._httpServerThread.started_evt.wait(timeout=15.0)
            if not event_is_set:
                self._logger.error('Cannot start device, start event of http server not set.')
                raise RuntimeError('Cannot start device, start event of http server not set.')

        host_ips = self._wsdiscovery.getActiveAddresses()
        self._url_dispatcher = httpserver.HostedServiceDispatcher(self._mdib.sdc_definitions, self._logger)
        self._httpServerThread.devices_dispatcher.register_device_dispatcher(self.path_prefix, self._url_dispatcher)
        if len(host_ips) == 0:
            self._logger.error('Cannot start device, there is no IP address to bind it to.')
            raise RuntimeError('Cannot start device, there is no IP address to bind it to.')

        port = self._httpServerThread.my_port
        if port is None:
            self._logger.error('Cannot start device, could not bind HTTP server to a port.')
            raise RuntimeError('Cannot start device, could not bind HTTP server to a port.')

        base_urls = []  # e.g https://192.168.1.5:8888/8c26f673-fdbf-4380-b5ad-9e2454a65b6b; list has one member for each used ip address
        for addr in host_ips:
            base_urls.append(
                urllib.parse.SplitResult(self._urlschema, '{}:{}'.format(addr, port), self.path_prefix, query=None,
                                         fragment=None))
        self.dpwsHost = pysoap.soapenvelope.DPWSHost(
            endpointReferencesList=[pysoap.soapenvelope.WsaEndpointReferenceType(self.epr)],
            typesList=self._mdib.sdc_definitions.MedicalDeviceTypesFilter)
        # register two addresses for hostDispatcher: '' and /<uuid>
        self._url_dispatcher.register_hosted_service(self._hostDispatcher)

        self._register_hosted_services(base_urls)

        for host_ip in host_ips:
            self._logger.info('serving Services on {}:{}', host_ip, port)
        self._subscriptionsManager.setBaseUrls(base_urls)

    def startAll(self, startRealtimeSampleLoop=True, shared_http_server=None):
        if self.product_roles is not None:
            self.product_roles.initOperations(self._mdib, self._scoOperationsRegistry)

        self._startServices(shared_http_server)
        if startRealtimeSampleLoop:
            self._runRtSampleThread = True
            self._rtSampleSendThread = threading.Thread(target=self._rtSampleSendLoop, name='DevRtSampleSendLoop')
            self._rtSampleSendThread.daemon = True
            self._rtSampleSendThread.start()

    def stopAll(self, closeAllConnections, sendSubscriptionEnd):
        if self._rtSampleSendThread is not None:
            self._runRtSampleThread = False
            self._rtSampleSendThread.join()
            self._rtSampleSendThread = None

        self._subscriptionsManager.endAllSubscriptions(sendSubscriptionEnd)
        self._scoOperationsRegistry.stopWorker()
        self._httpServerThread.stop(closeAllConnections)
        try:
            self._wsdiscovery.clearService(self.epr)
        except KeyError:
            print('epr "{}" not known in self._wsdiscovery'.format(self.epr))

        if self.product_roles is not None:
            self.product_roles.stop()

    def getXAddrs(self):
        addresses = self._wsdiscovery.getActiveAddresses()  # these own IP addresses are currently used by discovery
        port = self._httpServerThread.my_port
        xaddrs = []
        for xa in addresses:
            xaddrs.append('{}://{}:{}/{}'.format(self._urlschema, xa, port, self.path_prefix))
        return xaddrs

    def _onGetMetaData(self, httpHeader, request):
        self._logger.info('_onGetMetaData')
        _nsm = self._mdib.nsmapper
        response = pysoap.soapenvelope.Soap12Envelope(_nsm.docNssmap)
        replyAddress = request.address.mkReplyAddress('{}/GetResponse'.format(Prefix.WXF.namespace))
        replyAddress.to = namespaces.WSA_ANONYMOUS
        replyAddress.messageId = uuid.uuid4().urn
        response.addHeaderObject(replyAddress)
        metaDataNode = self._mkMetaDataNode()
        response.addBodyElement(metaDataNode)
        response.validateBody(self.mdib.bicepsSchema.mexSchema)
        self._logger.debug('returned meta data = {}', response.as_xml(pretty=False))
        return response

    def _onProbeRequest(self, httpHeader, request):
        _nsm = namespaces.DocNamespaceHelper()
        response = pysoap.soapenvelope.Soap12Envelope(_nsm.docNssmap)
        replyAddress = request.address.mkReplyAddress('{}/ProbeMatches'.format(Prefix.WSD.namespace))
        replyAddress.to = namespaces.WSA_ANONYMOUS
        replyAddress.messageId = uuid.uuid4().urn
        response.addHeaderObject(replyAddress)
        probe_match_node = etree_.Element(namespaces.wsdTag('Probematch'),
                                          nsmap=_nsm.docNssmap)
        types = etree_.SubElement(probe_match_node, namespaces.wsdTag('Types'))
        types.text = '{}:Device {}:MedicalDevice'.format(Prefix.DPWS.prefix, Prefix.MDPWS.prefix)
        scopes = etree_.SubElement(probe_match_node, namespaces.wsdTag('Scopes'))
        scopes.text = ''
        xaddrs = etree_.SubElement(probe_match_node, namespaces.wsdTag('XAddrs'))
        xaddrs.text = ' '.join(self.getXAddrs())
        response.addBodyElement(probe_match_node)
        return response

    def _validateDPWS(self, node):
        if not self.shallValidate:
            return
        try:
            self.mdib.bicepsSchema.dpwsSchema.assertValid(node)
        except etree_.DocumentInvalid as ex:
            tmp_str = etree_.tostring(node, pretty_print=True).decode('utf-8')
            self._logger.error('invalid dpws: {}\ndata = {}', ex, tmp_str)
            raise

    def _mkMetaDataNode(self):
        metaDataNode = etree_.Element(namespaces.wsxTag('Metadata'),
                                      nsmap=self._mdib.nsmapper.docNssmap)

        # ThisModel
        metaDataSectionNode = etree_.SubElement(metaDataNode,
                                                namespaces.wsxTag('MetadataSection'),
                                                attrib={'Dialect': '{}/ThisModel'.format(namespaces.nsmap['dpws'])})
        self.model.asEtreeSubNode(metaDataSectionNode)
        self._validateDPWS(metaDataSectionNode[-1])

        # ThisDevice
        metaDataSectionNode = etree_.SubElement(metaDataNode,
                                                namespaces.wsxTag('MetadataSection'),
                                                attrib={'Dialect': '{}/ThisDevice'.format(namespaces.nsmap['dpws'])})
        self.device.asEtreeSubNode(metaDataSectionNode)

        self._validateDPWS(metaDataSectionNode[-1])

        # Relationship
        metaDataSectionNode = etree_.SubElement(metaDataNode,
                                                namespaces.wsxTag('MetadataSection'),
                                                attrib={'Dialect': '{}/Relationship'.format(namespaces.nsmap['dpws'])})
        relationshipNode = etree_.SubElement(metaDataSectionNode,
                                             namespaces.dpwsTag('Relationship'),
                                             attrib={'Type': '{}/host'.format(namespaces.nsmap['dpws'])})

        self.dpwsHost.asEtreeSubNode(relationshipNode)
        self._validateDPWS(relationshipNode[-1])

        # add all hosted services:
        for service in self._hostedServices:
            service.hostedInf.asEtreeSubNode(relationshipNode)
            self._validateDPWS(relationshipNode[-1])
        return metaDataNode

    def sendMetricStateUpdates(self, mdibVersion, stateUpdates):
        self._logger.debug('sending metric state updates {}', stateUpdates)
        self._subscriptionsManager.sendEpisodicMetricReport(stateUpdates, self._mdib.nsmapper, mdibVersion,
                                                            self.mdib.sequenceId)

    def sendAlertStateUpdates(self, mdibVersion, stateUpdates):
        self._logger.debug('sending alert updates {}', stateUpdates)
        self._subscriptionsManager.sendEpisodicAlertReport(stateUpdates, self._mdib.nsmapper, mdibVersion,
                                                           self.mdib.sequenceId)

    def sendComponentStateUpdates(self, mdibVersion, stateUpdates):
        self._logger.debug('sending component state updates {}', stateUpdates)
        self._subscriptionsManager.sendEpisodicComponentStateReport(stateUpdates, self._mdib.nsmapper, mdibVersion,
                                                                    self.mdib.sequenceId)

    def sendContextStateUpdates(self, mdibVersion, stateUpdates):
        self._logger.debug('sending context updates {}', stateUpdates)
        self._subscriptionsManager.sendEpisodicContextReport(stateUpdates, self._mdib.nsmapper, mdibVersion,
                                                             self.mdib.sequenceId)

    def sendOperationalStateUpdates(self, mdibVersion, stateUpdates):
        self._logger.debug('sending operational state updates {}', stateUpdates)
        self._subscriptionsManager.sendEpisodicOperationalStateReport(stateUpdates, self._mdib.nsmapper, mdibVersion,
                                                                      self.mdib.sequenceId)

    def sendRealtimeSamplesStateUpdates(self, mdibVersion, stateUpdates):
        self._logger.debug('sending real time sample state updates {}', stateUpdates)
        self._subscriptionsManager.sendRealtimeSamplesReport(stateUpdates, self._mdib.nsmapper, mdibVersion,
                                                             self.mdib.sequenceId)

    def sendDescriptorUpdates(self, mdibVersion, updated, created, deleted, updated_states):
        self._logger.debug('sending descriptor updates updated={} created={} deleted={}', updated, created, deleted)
        self._subscriptionsManager.sendDescriptorUpdates(updated, created, deleted, updated_states,
                                                         self._mdib.nsmapper,
                                                         mdibVersion,
                                                         self.mdib.sequenceId)

    def sendWaveformUpdates(self, changedSamples):
        '''
        @param changedSamples: a dictionary with key = handle, value= devicemdib.RtSampleArray instance
        '''
        with self._mdib.mdibUpdateTransaction() as tr:
            for descriptorHandle, changedSample in changedSamples.items():
                determinationTime = changedSample.determinationTime
                samples = [s[0] for s in changedSample.samples]  # only the values without the 'start of cycle' flags
                activationState = changedSample.activationState
                st = tr.getRealTimeSampleArrayMetricState(descriptorHandle)
                if st.metricValue is None:
                    st.mkMetricValue()
                st.metricValue.Samples = samples
                st.metricValue.DeterminationTime = determinationTime  # set Attribute
                st.metricValue.Annotations = changedSample.annotations
                st.metricValue.ApplyAnnotations = changedSample.applyAnnotations
                st.ActivationState = activationState

    def _rtSampleSendLoop(self):
        if PROFILING:
            pr = cProfile.Profile()
        time.sleep(
            0.1)  # start delayed in order to have a fully initialized device when waveforms start (otherwise timing issues might happen)
        timer = intervaltimer.IntervalTimer(periodInSeconds=self.collectRtSamplesPeriod)
        if PROFILING:
            pr_time = time.monotonic()
            initial_time = pr_time  # delayed start of profiler, ignore init calls
        while self._runRtSampleThread:
            if PROFILING:
                if initial_time is not None and time.monotonic() - initial_time > 2:
                    pr.enable()
                    initial_time = None
            behindScheduleSeconds = timer.waitForNextIntervalBegin()
            changedSamples = self._mdib.getUpdatedDeviceRtSamples()
            if len(changedSamples) > 0:
                self._logWaveformTiming(behindScheduleSeconds)  #
                self.sendWaveformUpdates(changedSamples)
            if PROFILING and initial_time is None:
                if time.monotonic() - pr_time > 5:
                    print('profile')
                    pr.disable()
                    s = StringIO()
                    ps = pstats.Stats(pr, stream=s).sort_stats('time')
                    ps.print_stats(30)
                    print(s.getvalue())
                    pr.enable()
                    pr_time = time.monotonic()

    def _setupLogging(self, logLevel):
        loghelper.ensureLogStream()
        if logLevel is None:
            return
        deviceLog = logging.getLogger('sdc.device')
        deviceLog.setLevel(logLevel)

    def _logWaveformTiming(self, behindScheduleSeconds):
        try:
            lastLogTime = self._lastLogTime
        except AttributeError:
            self._lastLogTime = 0
            lastLogTime = self._lastLogTime
        try:
            lastLoggedDelay = self._lastLoggedDelay
        except AttributeError:
            self._lastLoggedDelay = 0
            lastLoggedDelay = self._lastLoggedDelay

        # max. one log per second
        now = time.monotonic()
        if now - lastLogTime < self.WARN_RATE_REALTIMESAMPLES_BEHIND_SCHEDULE:
            return
        if lastLoggedDelay >= self.WARN_LIMIT_REALTIMESAMPLES_BEHIND_SCHEDULE and behindScheduleSeconds < self.WARN_LIMIT_REALTIMESAMPLES_BEHIND_SCHEDULE:
            self._logger.info('RealTimeSampleTimer delay is back inside limit of {:.2f} seconds (mdib version={}',
                              self.WARN_LIMIT_REALTIMESAMPLES_BEHIND_SCHEDULE, self._mdib.mdibVersion)
            self._lastLoggedDelay = behindScheduleSeconds
            self._lastLogTime = now
        elif behindScheduleSeconds >= self.WARN_LIMIT_REALTIMESAMPLES_BEHIND_SCHEDULE:
            self._logger.warn('RealTimeSampleTimer is {:.4f} seconds behind schedule (mdib version={})',
                              behindScheduleSeconds, self._mdib.mdibVersion)
            self._lastLoggedDelay = behindScheduleSeconds
            self._lastLogTime = now

    def setUsedCompression(self, *compression_methods):
        # update list in place
        del self._compression_methods[:]
        self._compression_methods.extend(compression_methods)



class SdcHandler_Full(SdcHandler_Base):
    """ This class instantiates all port types."""
    def _register_hosted_services(self, base_urls):
        # register all services with their endpoint references acc. to sdc standard
        actions = self._mdib.sdc_definitions.Actions

        self._GetDispatcher = GetService('GetService', self)
        self._LocalizationDispatcher = LocalizationService('LocalizationService', self)
        offeredSubscriptions = []
        self._GetServiceHosted = DPWSHostedService(self, base_urls, 'Get',
                                                   [self._GetDispatcher, self._LocalizationDispatcher],
                                                   offeredSubscriptions)
        self._url_dispatcher.register_hosted_service(self._GetServiceHosted)

        # grouped acc to sdc REQ 0035
        self._ContextDispatcher = ContextService('ContextService', self)
        self._DescriptionEventDispatcher = DescriptionEventService('DescriptionEventService', self)
        self._StateEventDispatcher = StateEventService('StateEventService', self)
        self._WaveformDispatcher = WaveformService('WaveformService', self)

        offeredSubscriptions = [actions.EpisodicContextReport,
                                actions.DescriptionModificationReport,
                                actions.EpisodicMetricReport,
                                actions.EpisodicAlertReport,
                                actions.EpisodicComponentReport,
                                actions.EpisodicOperationalStateReport,
                                actions.Waveform,
                                actions.SystemErrorReport
                                ]

        self._SdcServiceHosted = DPWSHostedService(self, base_urls, 'StateEvent',
                                                   [self._ContextDispatcher,
                                                    self._DescriptionEventDispatcher,
                                                    self._StateEventDispatcher,
                                                    self._WaveformDispatcher],
                                                   offeredSubscriptions)
        self._url_dispatcher.register_hosted_service(self._SdcServiceHosted)

        self.__SetDispatcher = SetService('SetService', self)
        offeredSubscriptions = [actions.OperationInvokedReport]

        self._SetServiceHosted = DPWSHostedService(self, base_urls, 'Set', [self.__SetDispatcher], offeredSubscriptions)
        self._url_dispatcher.register_hosted_service(self._SetServiceHosted)

        self._ContainmentTreeDispatcher = ContainmentTreeService('ContainmentTreeService', self)
        offeredSubscriptions = []
        self._ContainmentTreeServiceHosted = DPWSHostedService(self, base_urls, 'ContainmentTree',
                                                               [self._ContainmentTreeDispatcher], offeredSubscriptions)
        self._url_dispatcher.register_hosted_service(self._ContainmentTreeServiceHosted)
        self._hostedServices = [self._GetServiceHosted,
                                self._SdcServiceHosted,
                                self._SetServiceHosted,
                                self._ContainmentTreeServiceHosted]


class SdcHandler_Minimal(SdcHandler_Base):
    """This class instantiates only GetService and LocalizationService"""
    def _register_hosted_services(self, base_urls):
        self._GetDispatcher = GetService('GetService', self)
        self._LocalizationDispatcher = LocalizationService('LocalizationService', self)
        offeredSubscriptions = []
        self._GetServiceHosted = DPWSHostedService(self, base_urls, 'Get',
                                                   [self._GetDispatcher, self._LocalizationDispatcher],
                                                   offeredSubscriptions)
        self._url_dispatcher.register_hosted_service(self._GetServiceHosted)

        self._url_dispatcher.register_hosted_service(self._ContainmentTreeServiceHosted)
        self._hostedServices = [self._GetServiceHosted]
