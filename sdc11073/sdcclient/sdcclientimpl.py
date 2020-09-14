''' Using lxml based SoapClient'''
import weakref
import functools
import logging
import os
import traceback
import ssl
import json
import urllib
from lxml import etree as etree_
from cryptography import x509
from cryptography.hazmat import backends
from cryptography.x509 import extensions

import sdc11073
from .. import observableproperties as properties
from .. import commlog
from .. import loghelper
from .. import xmlparsing
from . import subscription
from .operations import OperationsManager
from .hostedservice import HostedServiceClient, GetServiceClient, SetServiceClient, StateEventClient
from .hostedservice import CTreeServiceClient, DescriptionEventClient, ContextServiceClient, WaveformClient
from .localizationservice import LocalizationServiceClient
from ..namespaces import nsmap
from ..namespaces import Prefix_Namespace as Prefix
from ..definitions_base import ProtocolsRegistry
from ..definitions_sdc import SDC_v1_Definitions
# shortcuts
GenericNode = sdc11073.pysoap.soapenvelope.GenericNode
WsAddress = sdc11073.pysoap.soapenvelope.WsAddress
Soap12Envelope = sdc11073.pysoap.soapenvelope.Soap12Envelope
AddressedSoap12Envelope = sdc11073.pysoap.soapenvelope.AddressedSoap12Envelope
DPWSEnvelope = sdc11073.pysoap.soapenvelope.DPWSEnvelope
MetaDataSection = sdc11073.pysoap.soapenvelope.MetaDataSection
SoapResponseException = sdc11073.pysoap.soapenvelope.SoapResponseException


def _mkSoapClient(scheme, netloc, logger, sslContext, sdc_definitions, supportedEncodings=None, requestEncodings=None):
    if scheme == 'https':
        _sslContext = sslContext
    else:
        _sslContext = None
    return  sdc11073.pysoap.soapclient.SoapClient(netloc, logger, sslContext=_sslContext, sdc_definitions=sdc_definitions,
                                                  supportedEncodings=supportedEncodings, requestEncodings=requestEncodings)


# default ssl context data
here = os.path.dirname(__file__)
caFolder = os.path.join(os.path.dirname(here), 'ca')
_ssl_certfile = os.path.join(caFolder, 'sdccert.pem') # this is the certification chain ( contains root ca and signed public key
_ssl_keyfile = os.path.join(caFolder, 'userkey.pem')     # this is the private key of own certificate
_ssl_cacert = os.path.join(caFolder, 'cacert.pem')    # this is the common root ca that signed all sdc devices 
_ssl_passwd = 'dummypass' #'Phase1' #dummypass
_ssl_cypherfile = os.path.join(caFolder, 'cyphers.json') # Json file that determines ciphers to be used



class HostDescription(object):
    def __init__(self, dpws_envelope):
        self._dpws_envelope = dpws_envelope
        self.thisModel = dpws_envelope.thisModel 
        self.thisDevice = dpws_envelope.thisDevice 
        self.host = dpws_envelope.host

    def __str__(self):
        return 'HostDescription: thisModel = {}, thisDevice = {}, host = {}'.format(self.thisModel, self.thisDevice, self.host)


class HostedServiceDescription(object):
    VALIDATE_MEX = False # workaraound as long as validation error due to missing dpws schema is not solved
    def __init__(self, service_id, endpoint_address, validate, bicepsSchema, log_prefix=''):
        self._endpoint_address = endpoint_address
        self.service_id = service_id
        self._validate = validate
        self._bicepsSchema = bicepsSchema
        self.log_prefix = log_prefix
        self.metaData = None
        self.wsdl_string = None
        self.wsdl = None
        self._logger = loghelper.getLoggerAdapter('sdc.client.{}'.format(service_id), log_prefix)
        self._url = urllib.parse.urlparse(endpoint_address) 
        self.services = {}

    @property
    def _mexSchema(self):
        return None if not self._validate else self._bicepsSchema.mexSchema

    def readMetadata(self, soap_client):
        soapEnvelope = Soap12Envelope(nsmap)
        self._logger.debug('calling GetMetadata on {}', self._endpoint_address)
        soapEnvelope.setAddress(WsAddress(action='http://schemas.xmlsoap.org/ws/2004/09/mex/GetMetadata/Request',
                                          to=self._endpoint_address))
        soapEnvelope.addBodyObject(GenericNode(etree_.Element('{http://schemas.xmlsoap.org/ws/2004/09/mex}GetMetadata')))
        if self.VALIDATE_MEX:
            soapEnvelope.validateBody(self._mexSchema)
        endpointEnvelope = soap_client.postSoapEnvelopeTo(self._url.path,
                                                          soapEnvelope,
                                                          msg='<{}> readMetadata'.format(self.service_id))
        if self.VALIDATE_MEX:
            endpointEnvelope.validateBody(self._mexSchema)
        self.metaData = MetaDataSection.fromEtreeNode(endpointEnvelope.bodyNode)
        self.readwsdl(soap_client, self.metaData.wsdl_location)
        return

    def readwsdl(self, soap_client, wsdl_url):
        p = urllib.parse.urlparse(wsdl_url)
        actual_path = p.path + '?{}'.format(p.query) if p.query else p.path
        self.wsdl_string = soap_client.getUrl(actual_path, msg='{}:getwsdl'.format(self.log_prefix))
        commlog.defaultLogger.logWsdl(self.wsdl_string, self.service_id)
        try:
            self.wsdl = etree_.fromstring(self.wsdl_string, parser=etree_.ETCompatXMLParser()) # make am ElementTree instance
        except etree_.XMLSyntaxError as ex:
            self._logger.error('could not read wsdl from {}: error={}, data=\n{}'.format(actual_path, ex, self.wsdl_string))

    def __repr__(self):
        return '{} "{}" endpoint = {}'.format(self.__class__.__name__, self.service_id, self._endpoint_address)


def ip2Int(ipString):
    ''' Convert string like '192.168.0.1' to an integer
    (helper for sortIPAddresses)'''
    return functools.reduce(lambda x,y: x*256+y, (int(x) for x in ipString.split('.')), 0)


def _cmp(a, b, _refInt):
    ''' helper for sortIPAddresses'''
    _a = abs(ip2Int(a) - _refInt)
    _b = abs(ip2Int(b) - _refInt)
    diff = _a - _b
    if diff < 0:
        return -1
    elif diff > 0:
        return 1
    else:
        return 0


def sortIPAddresses(adresses, refIp):
    ''' sorts list addresses by distance to refIP, shortest distance first'''
    _ref = ip2Int(refIp)
    adresses.sort(key=lambda a: abs(ip2Int(a) - _ref))
    return adresses



class SdcClient(object):
    ''' The SdcClient can be used with a known device location.
    The location is typically the result of a wsdiscovery process.
    This class expects that the BICEPS services are available in the device.
    What if not???? => raise exception in discoverHostedServices
    ''' 
    isConnected = properties.ObservableProperty(False) # a boolean
    
    # observable properties for all notifications
    # all incoming Notifications can be observed in stateEventReportEnvelope ( as soap envelope)
    stateEventReportEnvelope = properties.ObservableProperty()

    # the following observables can be used to observe the incoming notifications by message type.
    # They contain only the body node of the notification, not the envelope
    waveFormReport = properties.ObservableProperty()
    episodicMetricReport = properties.ObservableProperty()
    episodicAlertReport = properties.ObservableProperty()
    episodicComponentReport = properties.ObservableProperty()
    episodicOperationalStateReport = properties.ObservableProperty()
    episodicContextReport = properties.ObservableProperty()
    descriptionModificationReport = properties.ObservableProperty()
    operationInvokedReport = properties.ObservableProperty()

    _servicesLookup = {'ContainmentTree': CTreeServiceClient,     # wpf naming
                       'ContainmentTreeService': CTreeServiceClient, # sdc naming
                       'Get': GetServiceClient,
                       'GetService': GetServiceClient,
                       'StateEvent': StateEventClient,
                       'StateEventService': StateEventClient,
                       'Context': ContextServiceClient,
                       'ContextService': ContextServiceClient,
                       'Waveform': WaveformClient,
                       'WaveformService': WaveformClient,
                       'Set': SetServiceClient,
                       'SetService': SetServiceClient,
                       'DescriptionEvent': DescriptionEventClient,
                       'DescriptionEventService': DescriptionEventClient,
                       'LocalizationService': LocalizationServiceClient,
                       }

    SSL_CIPHERS = None  # None : use SSL default
    def __init__(self, devicelocation, deviceType, validate=True, sslEvents='auto', sslContext=None,
                 my_ipaddress=None, logLevel=None, ident='',
                 soap_notifications_handler_class=None):  # pylint:disable=too-many-arguments
        '''
        @param devicelocation: the XAddr location for meta data, e.g. http://10.52.219.67:62616/72c08f50-74cc-11e0-8092-027599143341
        @param deviceType: a QName that defines the device type, e.g. '{http://standards.ieee.org/downloads/11073/11073-20702-2016}MedicalDevice'
                          can be None, in that case value from pysdc.xmlparsing.Final is used
        @param sslEvents: define if HTTP server of client uses https
             sslEvents='auto': use https if Xaddress of device is https
             sslEvents=True: always use https
             sslEvents=False: use only http 
        @param sslContext: if not None, this context is used. Otherwise a sSSLContext is automatically generated.
        @param my_ipAddress: This address is used for the http server that receives notifications. 
             If value is None, best own address is determined automatically (recommended).  
        '''
        self._devicelocation = devicelocation
        self._soap_notifications_handler_class = soap_notifications_handler_class
        if deviceType is None:
            self.sdc_definitions = SDC_v1_Definitions
        else:
            self.sdc_definitions = None
            for definition_cls in ProtocolsRegistry.protocols:
                if definition_cls.ns_matches(deviceType.namespace):
                    self.sdc_definitions = definition_cls
                    break
            if self.sdc_definitions is None:
                raise ValueError('cannot create instance, no known BICEPS schema version identified')

        self._bicepsSchema = xmlparsing.BicepsSchema(self.sdc_definitions)
        splitted = urllib.parse.urlsplit(self._devicelocation)
        self._device_uses_https = splitted.scheme.lower() == 'https'

        self.log_prefix = ident or ''

        self._sslEvents = sslEvents
        self._setupLogging(logLevel)
        self._logger = loghelper.getLoggerAdapter('sdc.client', self.log_prefix)
        self._logger_wf = loghelper.getLoggerAdapter('sdc.client.wf', self.log_prefix) # waveform logger
        if my_ipaddress is None:
            self._my_ipaddress = self._findBestOwnIpAddress()
        else:
            self._my_ipaddress = my_ipaddress
        self._logger.info('SdcClient for {} uses own IP Address {}', self._devicelocation, self._my_ipaddress)
        self.metaData = None
        self.hostDescription = None
        self._hostedServices = {} # lookup by service id
        self._validate = validate
        try:
            self._logger.info('Using SSL is enabled. TLS 1.3 Support = {}', ssl.HAS_TLSv1_3)
        except AttributeError:
            self._logger.info('Using SSL is enabled. TLS 1.3 is not supported')
        self._sslContext = sslContext
        self._notificationsDispatcherThread = None
        
        self._logger.info('created {} for {}', self.__class__.__name__, self._devicelocation)

        self._compression_methods = sdc11073.compression.encodings[:]
        self._subscriptionMgr = None
        self._operationsManager = None
        self._serviceClients = {}
        self._mdib = None   
        self._soapClients = {} # all http connections that this client holds
        self.peerCertificate = None
        self.all_subscribed = False


    def _register_mdib(self, mdib):
        ''' SdcClient sometimes must know the mdib data (e.g. Set service, activate method).'''
        if mdib is not None and self._mdib is not None:
            raise RuntimeError('SdcClient has already an registered mdib')
        self._mdib = None if mdib is None else weakref.ref(mdib)
        if mdib is not None:
            mdib.bicepsSchema = self._bicepsSchema
        self.client('Set').register_mdib(mdib)
        self.client('Context').register_mdib(mdib)


    @property
    def mdib(self):
        return self._mdib()

    @property
    def my_ipaddress(self):
        return self._my_ipaddress

    def _findBestOwnIpAddress(self):
        myIpAddresses = [conn.ip for conn in sdc11073.netconn.getNetworkAdapterConfigs() if conn.ip not in (None, '0.0.0.0')]
        splitted = urllib.parse.urlsplit(self._devicelocation)
        sortIPAddresses(myIpAddresses, splitted.hostname)
        return myIpAddresses[0]


    def _subscribe(self, dpwsHosted, actions, callback):
        ''' creates a subscription object and registers it in 
        @param dpwsHosted: proxy for the hosted service that provides the events we want to subscribe to
                           This is the target for all subscribe/unsubscribe ... messages
        @param actions: a list of filters. this (joined) string is sent to the sdc server in the Subscribe message
        @param callback: callable with signature callback(soapEnvlope)
        @return: a subscription object that has callback already registerd
        '''
        s = self._subscriptionMgr.mkSubscription(dpwsHosted, actions)
        for f in actions:
            self._notificationsDispatcherThread.dispatcher.register_function(f, s.onNotification)
        if callback is not None:
            properties.bind(s, notification=callback)
        s.subscribe()
        return s


    def client(self, porttypename):
        ''' returns the client for the given port type name.
        WDP and SDC use different port type names, e.g WPF="Get", SDC="GetService".
        If the port type is not found directly, it tries also with or without "Service" in name.
        :param porttypename: string, e.g "Get", or "GetService", ...
        '''
        client = self._serviceClients.get(porttypename)
        if client is None and porttypename.endswith('Service'):
            client = self._serviceClients.get(porttypename[:-7])
        if client is None and not porttypename.endswith('Service'):
            client = self._serviceClients.get(porttypename+"Service")
        return client

    @property
    def GetService_client(self):
        return self.client('GetService')

    @property
    def SetService_client(self):
        return self.client('SetService')

    @property
    def DescriptionEventService_client(self):
        return self.client('DescriptionEventService')

    @property
    def StateEventService_client(self):
        return self.client('StateEventService')

    @property
    def ContextService_client(self):
        return self.client('ContextService')

    @property
    def WaveformService_client(self):
        return self.client('Waveform')

    @property
    def ContainmentTreeService_client(self):
        return self.client('ContainmentTreeService')

    @property
    def ArchiveService_client(self):
        return self.client('ArchiveService')

    @property
    def LocalizationService_client(self):
        return self.client('LocalizationService')


    def startAll(self, notSubscribedActions = None, subscriptionsCheckInterval=None, async_dispatch=True):
        '''
        :param notSubscribedActions: a list of pmtypes.Actions elements or None. if None, everything is subscribed.
        :param subscriptionsCheckInterval: an interval in seconds or None
        :param async_dispatch: if True, incoming requests are queued and response is sent immediately (processing is done later).
                                if False, response is sent after the complete processing is done.
        :return: None
        '''
        self.discoverHostedServices()
        self._startEventSink(async_dispatch)
        
        # start subscription manager
        self._subscriptionMgr = subscription.SubscriptionManager(self._notificationsDispatcherThread.base_url, log_prefix=self.log_prefix, checkInterval=subscriptionsCheckInterval)
        self._subscriptionMgr.start()

        if notSubscribedActions is None:
            self.all_subscribed = True # this tells mdib that mdib state versions shall not have any gaps => log warnings for missing versions
            notSubscribedActionsSet = set([])
        else:
            self.all_subscribed = False # this tells mdib that mdib state versions can have gaps => do not log warnings for missing versions
            notSubscribedActionsSet = set(notSubscribedActions)

        # start operationInvoked subscription and tell all
        self._operationsManager = OperationsManager(self.log_prefix)

        for client in self._serviceClients.values():
            client.setOperationsManager(self._operationsManager)

        # start all subscriptions
        # group subscriptions per hosted service
        for service_id, dpwsHosted in self.metaData.hosted.items():
            available_actions = []
            for port_type_qname in dpwsHosted.types:
                port_type = port_type_qname.split(':')[-1]
                client = self.client(port_type)
                if client is not None:
                    available_actions.extend( client.getSubscribableActions())
            if len(available_actions) > 0:
                subscribe_actions = set(available_actions) - notSubscribedActionsSet
                try:
                    self._subscribe(dpwsHosted, subscribe_actions,
                                    self._onAnyStateEventReport)
                except Exception as ex:
                    self.all_subscribed = False # => do not log errors when mdib versions are missing in notifications
                    self._logger.error('startAll: could not subscribe: error = {}, actions= {}',
                                       traceback.format_exc(), subscribe_actions)

        # register callback for end of subscription
        self._notificationsDispatcherThread.dispatcher.register_function( self.sdc_definitions.Actions.SubscriptionEnd, self._onSubScriptionEnd)

        #connect self.isConnected observable to allSubscriptionsOkay observable in subscriptionsmanager
        def setIsConnected(isOk):
            self.isConnected = isOk
        properties.strongbind(self._subscriptionMgr, allSubscriptionsOkay=setIsConnected)
        self.isConnected = self._subscriptionMgr.allSubscriptionsOkay

    def stopAll(self, unsubscribe=True, closeAllConnections=True):
        if self._subscriptionMgr is not None:
            if unsubscribe:
                self._subscriptionMgr.unsubscribeAll()
            self._subscriptionMgr.stop()
        self._stopEventSink(closeAllConnections)
        self._register_mdib(None)   
            
        for cl in self._soapClients.values():
            cl.close()
        self._soapClients = {}

    def setUsedCompression(self, *compression_methods):
        # update list in place
        del self._compression_methods[:]
        self._compression_methods.extend(compression_methods)


    def get_peer_cert_extended_key_usages(self):
        _url = urllib.parse.urlparse(self._devicelocation)
        if self._sslContext is not None and _url.scheme == 'https':
            wsc = self._getSoapClient(self._devicelocation)
            if wsc.isClosed():
                wsc.connect()
            sock = wsc.sock
            binary_peer_cert = sock.getpeercert(binary_form=True)
            if binary_peer_cert:
                cert = x509.load_der_x509_certificate(binary_peer_cert, backends.default_backend())
                try:
                    ext_key_usage = cert.extensions.get_extension_for_class(extensions.ExtendedKeyUsage)
                    return [e for e in ext_key_usage.value]
                except Exception as ex:
                    self._logger.warn('Unable to read EKU:{}'.format(repr(ex)))
        return list()

    def getMetaData(self):
        _url = urllib.parse.urlparse(self._devicelocation)
        wsc = self._getSoapClient(self._devicelocation)

        if self._sslContext is not None and _url.scheme == 'https':
            if wsc.isClosed():
                wsc.connect()
            sock = wsc.sock
            self.peerCertificate = sock.getpeercert(binary_form=False)
            self._logger.info('Peer Certificate: {}', self.peerCertificate)
            self._logger.info('Peer Certificate Extended Key Usages: {}', self.get_peer_cert_extended_key_usages())

        soapEnvelope = Soap12Envelope(nsmap)
        soapEnvelope.setAddress(WsAddress(action='{}/Get'.format(Prefix.WXF.namespace),
                                          to=self._devicelocation))
        
        self.metaData = wsc.postSoapEnvelopeTo(_url.path, soapEnvelope, responseFactory=DPWSEnvelope.fromXMLString,
                                               msg='getMetadata')
        self.hostDescription = HostDescription(self.metaData)
        self._logger.debug('HostDescription: {}', self.hostDescription)


    def discoverHostedServices(self):
        ''' Discovers all hosted services.
        Raises RuntimeError if device does not provide the expected BICEPS services
        '''
        # we need to read the meta data of the device only once => temporary soap client is sufficient
        self._logger.debug('reading meta data from {}', self._devicelocation)
        #self.metaData = 
        if self.metaData is None:
            self.getMetaData()

        # now query also meta data of hosted services
        self._mkHostedServices() 
        self._logger.debug('Services: {}', self._serviceClients.keys())

        # only GetService is mandatory!!!
        if self.GetService_client is None:
            raise RuntimeError('GetService not detected! found services = {}'.format(self._serviceClients.keys()))


    def _getSoapClient(self, address):
        _url = urllib.parse.urlparse(address)
        key = (_url.scheme, _url.netloc)
        soapClient = self._soapClients.get(key)
        if soapClient is None:
            soapClient = _mkSoapClient(_url.scheme, _url.netloc,
                                       loghelper.getLoggerAdapter('sdc.client.soap', self.log_prefix),
                                       sslContext=self._sslContext,
                                       sdc_definitions=self.sdc_definitions,
                                       supportedEncodings=self._compression_methods)
            self._soapClients[key] = soapClient
        return soapClient


    def _mkHostedServices(self):
        for hosted in self.metaData.hosted.values():
            endpoint_reference = hosted.endpointReferences[0].address
            soapClient = self._getSoapClient(endpoint_reference)
            hosted.soapClient = soapClient
            ns_types = [t.split(':') for t in hosted.types]
            h_descr = HostedServiceDescription(hosted.serviceId, endpoint_reference,
                                               self._validate, self._bicepsSchema, self.log_prefix)
            self._hostedServices[hosted.serviceId] = h_descr
            h_descr.readMetadata(soapClient)
            for _, porttype in ns_types:
                h = self._mkHostedServiceClient(porttype, soapClient, hosted)
                self._serviceClients[porttype] = h
                h_descr.services[porttype] = h


    def _mkHostedServiceClient(self, porttype, soapClient, hosted):
        cls = self._servicesLookup.get(porttype, HostedServiceClient)
        return cls(soapClient, hosted, porttype, self._validate, self.sdc_definitions, self._bicepsSchema, self.log_prefix)

    def _startEventSink(self, async_dispatch):
        if self._sslEvents == 'auto':
            sslContext = self._sslContext if self._device_uses_https else None
        elif self._sslEvents: # True
            sslContext = self._sslContext
        else:   # False
            sslContext = None

        # create Event Server
        self._notificationsDispatcherThread = subscription.NotificationsReceiverDispatcherThread(
            self._my_ipaddress,
            sslContext,
            log_prefix=self.log_prefix,
            sdc_definitions=self.sdc_definitions,
            supportedEncodings=self._compression_methods,
            soap_notifications_handler_class=self._soap_notifications_handler_class,
            async_dispatch = async_dispatch)

        self._notificationsDispatcherThread.start()
        self._notificationsDispatcherThread.started_evt.wait(timeout=5)
        self._logger.info('serving EventSink on {}', self._notificationsDispatcherThread.base_url)


    def _stopEventSink(self, closeAllConnections):
        if self._notificationsDispatcherThread is not None:
            self._notificationsDispatcherThread.stop(closeAllConnections)


    def _onAnyStateEventReport(self, soapenvelope):
        ''' dispatch by message body'''
        self.stateEventReportEnvelope = soapenvelope # update observable
        message = soapenvelope.bodyNode[0].tag
        if message.endswith('EpisodicMetricReport'):
            return self._onEpisodicMetricReport(soapenvelope)
        elif message.endswith('EpisodicAlertReport'):
            return self._onEpisodicAlertReport(soapenvelope)
        elif message.endswith('EpisodicComponentReport'):
            return self._onEpisodicComponentReport(soapenvelope)
        elif message.endswith('EpisodicOperationalStateReport'):
            return self._onEpisodicOperationalStateReport(soapenvelope)
        elif message.endswith('EpisodicContextReport'):
            return self._onEpisodicContextReport(soapenvelope)
        elif message.endswith('WaveformStream') or message.endswith('WaveformStreamReport'): # different names in Draft6 and Final
            return self._onWaveFormReport(soapenvelope)
        elif message.endswith('OperationInvokedReport'):
            return self._onOperationInvokedReport(soapenvelope)
        elif message.endswith('EpisodicContextReport'):
            return self._onEpisodicContextReport(soapenvelope)
        elif message.endswith('DescriptionModificationReport'):
            return self._onEpisodicDescriptionReport(soapenvelope)
        else:
            raise RuntimeError('unknown message {}'.format(message))


    def _onOperationInvokedReport(self, soapenvelope):
        ret = self._operationsManager.onOperationInvokedReport(soapenvelope)
        report = soapenvelope.bodyNode.xpath('msg:OperationInvokedReport', namespaces=nsmap)
        self.operationInvokedReport = report[0] # update observable
        return ret


    def _onWaveFormReport(self, soapenvelope):
        try:
            waveformStream = soapenvelope.bodyNode[0] # the msg:WaveformStreamReport node
        except IndexError:
            waveformStream = None
            
        if waveformStream is not None:
            self._logger_wf.debug('_onWaveFormReport')
        else:
            self._logger_wf.error('WaveformStream does not contain msg:WaveformStream!', soapenvelope)
        
        self.waveFormReport = waveformStream # update observable


    def _onEpisodicMetricReport(self, soapenvelope):
        reports = soapenvelope.bodyNode.xpath('msg:EpisodicMetricReport', namespaces=nsmap)
        if len(reports) == 1:
            self._logger.debug('_onEpisodicMetricReport')
            self.episodicMetricReport = reports[0] # update observable
        elif len(reports) > 1:
            self._logger.error('EpisodicMetricReport contains {} elements of msg:EpisodicMetricReport!', len(reports))
        else:
            self._logger.error('EpisodicMetricReport does not contain msg:EpisodicMetricReport!')


    def _onEpisodicAlertReport(self, soapenvelope):
        reports = soapenvelope.bodyNode.xpath('msg:EpisodicAlertReport', namespaces=nsmap)
        if len(reports) == 1:
            self._logger.debug('_onEpisodicAlertReport')
            self.episodicAlertReport = reports[0] # update observable
        elif len(reports) > 1:
            self._logger.error('EpisodicAlertReport contains {} elements of msg:EpisodicAlertReport!', len(reports))
        else:
            self._logger.error('EpisodicAlertReport does not contain msg:EpisodicAlertReport!', soapenvelope)


    def _onEpisodicComponentReport(self, soapenvelope):
        report = soapenvelope.bodyNode.xpath('msg:EpisodicComponentReport', namespaces=nsmap)
        if len(report) == 1:
            self._logger.debug('EpisodicComponentReport received')
            self.episodicComponentReport = report[0] # update observable
        elif len(report) > 1:
            self._logger.error('EpisodicComponentReport contains {} elements of msg:EpisodicComponentReport!', len(report))
        else:
            self._logger.error('EpisodicComponentReport does not contain msg:EpisodicComponentReport!', soapenvelope)


    def _onEpisodicOperationalStateReport(self, soapenvelope):
        report = soapenvelope.bodyNode.xpath('msg:EpisodicOperationalStateReport', namespaces=nsmap)
        if len(report) == 1:
            self._logger.debug('EpisodicOperationalStateReport: {}', lambda:etree_.tostring(report[0]))
            self.episodicOperationalStateReport = report[0] # update observable
        elif len(report) > 1:
            self._logger.error('OperationalStateReport contains {} elements of msg:OperationalStateReport!', len(report))
        else:
            self._logger.error('OperationalStateReport does not contain msg:OperationalStateReport!', soapenvelope)
    
    
    def _onSubScriptionEnd(self, soapenvelope):
        self.stateEventReportEnvelope = soapenvelope # update observable
        self._subscriptionMgr.onSubScriptionEnd(soapenvelope)

    
    def _onEpisodicContextReport(self, soapenvelope):
        report = soapenvelope.bodyNode.xpath('msg:EpisodicContextReport', namespaces=nsmap)
        if len(report) == 1:
            self._logger.debug('EpisodicContextReport: {}', lambda:etree_.tostring(report[0]))
            self.episodicContextReport = report[0] # update observable
        elif len(report) > 1:
            self._logger.error('EpisodicContextReport contains {} elements of msg:EpisodicContextReport!', len(report))
        else:
            self._logger.error('EpisodicContextReport does not contain msg:EpisodicContextReport!', soapenvelope)


    def _onEpisodicDescriptionReport(self, soapenvelope):
        report = soapenvelope.bodyNode.xpath('msg:DescriptionModificationReport', namespaces=nsmap)
        if len(report) == 1:
            self.descriptionModificationReport = report[0]
        elif len(report) > 1:
            self._logger.error('DescriptionModificationReport contains {} elements of msg:DescriptionModificationReport!', len(report))
        else:
            self._logger.error('DescriptionModificationReport does not contain msg:DescriptionModificationReport!', soapenvelope)


    def _setupLogging(self, logLevel):
        loghelper.ensureLogStream()
        if logLevel is None:
            return
        clientLog = logging.getLogger('sdc.client')
        clientLog.setLevel(logLevel)


    def __str__(self):
        return 'SdcClient to {} {} on {}'.format(self.hostDescription.thisDevice, 
                                                    self.hostDescription.thisModel,
                                                    self._devicelocation)


    @classmethod
    def fromWsdService(cls, wsdService, validate=True, sslEvents='auto',
                     sslContext=None, my_ipaddress=None, logLevel=logging.INFO,
                     ident='', soap_notifications_handler_class=None):
        device_locations = wsdService.getXAddrs()
        if not device_locations:
            raise RuntimeError('discovered Service has no address!{}'.format(wsdService))
        device_location = device_locations[0]
        deviceType = None
        for _qname in wsdService.getTypes():
            qname = etree_.QName(_qname.namespace, _qname.localname)
            for protocol in ProtocolsRegistry.protocols:
                if protocol.ns_matches(qname):
                    deviceType = protocol.MedicalDeviceType
                    break
        return cls(device_location, deviceType=deviceType, validate=validate, sslEvents=sslEvents,
                   sslContext=sslContext, my_ipaddress=my_ipaddress, logLevel=logLevel, ident=ident,
                   soap_notifications_handler_class=soap_notifications_handler_class)
