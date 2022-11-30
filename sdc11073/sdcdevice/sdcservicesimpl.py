import time
from collections import namedtuple, OrderedDict
from io import BytesIO
from lxml import etree as etree_

from .. import pysoap
from ..namespaces import Prefix_Namespace as Prefix
from ..namespaces import msgTag, domTag, s12Tag, wsxTag, wseTag, dpwsTag, mdpwsTag, nsmap
from .. import pmtypes
from .. import loghelper
from .exceptions import InvalidActionError, FunctionNotImplementedError
_msg_prefix = Prefix.MSG.prefix

_wsdl_ns = Prefix.WSDL.namespace
_wsdl_message = etree_.QName(_wsdl_ns, 'message')
_wsdl_part = etree_.QName(_wsdl_ns, 'part')
_wsdl_operation = etree_.QName(_wsdl_ns, 'operation')

_wsp_ns = 'http://www.w3.org/ns/ws-policy'
_wsp_prefix = 'wsp'

# DiscoveryType, only used in SDC
_dt = "http://standards.ieee.org/downloads/11073/11073-10207-2017"

_WSDL_S12 = "http://schemas.xmlsoap.org/wsdl/soap12/" # old soap 12 namespace, used in wsdl 1.1. used only for wsdl


# WSDL Generation:
# types to allow declaration of a wsdl data per service
WSDLMessageDescription  = namedtuple('WSDLMessageDescription', 'name parameters ')
WSDLOperationBinding = namedtuple('WSDLOperationBinding', 'name input output')


def etreeFromFile(path):
    parser = etree_.ETCompatXMLParser()
    with open(path, 'rb') as f:
        xml_text = f.read()
    return etree_.fromstring(xml_text, parser=parser, base_url=path)


class SOAPActionRegistry(object):
    def __init__(self, log_prefix=None):
        self._soapActionCallbacks = {}
        self._getCallbacks = {}
        self._logger = loghelper.getLoggerAdapter('sdc.device.{}'.format(self.__class__.__name__), log_prefix)

    def register_soapActionCallback(self, action, fn):
        self._soapActionCallbacks[action] = fn

    def register_getCallback(self, path, query, fn):
        if path.endswith('/'):
            path = path[:-1]
        self._getCallbacks[(path, query)] = fn

    def _getActionHandler(self, action):
        ''' returns a callable or None'''
        return self._soapActionCallbacks.get(action)

    def getActions(self):
        """ returns a list of action strings that can be handled."""
        return  list(self._soapActionCallbacks.keys())


class SOAPActionDispatcher(SOAPActionRegistry):

    def dispatchSoapRequest(self, path, httpHeader, soapEnvelope):
        begin = time.monotonic()
        action = soapEnvelope.address.action
        fn = self._getActionHandler(action)
        if fn is None:
            raise InvalidActionError(soapEnvelope)
        returnedSoapEnvelope = fn(httpHeader, soapEnvelope)
        duration = time.monotonic() - begin
        self._logger.debug('incoming soap action "{}" to {}: duration={:.3f}sec.', action, path, duration)
        return returnedSoapEnvelope

    def dispatchGetRequest(self, parseResult, httpHeader): #pylint:disable=unused-argument
        begin = time.monotonic()
        path = parseResult.path
        if path.endswith('/'):
            path = path[:-1]
        key = (path, parseResult.query)
        fn = self._getCallbacks.get(key)
        if fn is not None:
            self._logger.debug('dispatchGetRequest:path="{}" ,function="{}"', key, fn.__name__)
            result = fn()
            duration = time.monotonic() - begin
            self._logger.debug('dispatchGetRequest:duration="{:.4f}"', duration)
            return result
        else:
            self._logger.error('dispatchGetRequest:path="{}" ,no handler found!', key)
            raise KeyError('dispatchGetRequest:path="{}" ,no handler found!'.format(key))


class _SOAPActionDispatcherWithSubDispatchers(SOAPActionDispatcher):
    ''' receiver of all messages'''

    def __init__(self, subDispatchers=None):
        super(_SOAPActionDispatcherWithSubDispatchers, self).__init__()
        self.subDispatchers = subDispatchers or [] # chained SOAPActionDispatcher


    def _getActionHandler(self, action):
        ''' returns a callable or None'''
        fn = self._soapActionCallbacks.get(action)
        if fn is not None:
            return fn
        for sd in self.subDispatchers:
            fn = sd._getActionHandler(action)
            if fn is not None:
                return fn
        return None


    def getActions(self):
        ''' returns a list of action strings that can be handled.'''
        actions =  list(self._soapActionCallbacks.keys())
        for sd in self.subDispatchers:
            actions.extend(sd.getActions())
        return actions

    def __repr__(self):
        return '{} actions={}'.format(self.__class__.__name__, self._soapActionCallbacks.keys())



class EventService(_SOAPActionDispatcherWithSubDispatchers):
    ''' A service that offers subscriptions'''
    def __init__(self, sdcDevice, subDispatchers, offeredSubscriptions):
        super(EventService, self).__init__(subDispatchers)

        self._sdcDevice = sdcDevice
        self._subscriptionsManager = sdcDevice.subscriptionsManager
        self._offeredSubscriptions = offeredSubscriptions
        self.register_soapActionCallback('{}/Subscribe'.format(Prefix.WSE.namespace), self._onSubscribe)
        self.register_soapActionCallback('{}/Unsubscribe'.format(Prefix.WSE.namespace), self._onUnsubscribe)
        self.register_soapActionCallback('{}/GetStatus'.format(Prefix.WSE.namespace), self._onGetStatus)
        self.register_soapActionCallback('{}/Renew'.format(Prefix.WSE.namespace), self._onRenewStatus)
        self.epr = None

    def dispatchSoapRequest(self, path, httpHeader, soapEnvelope):
        if self._sdcDevice.shallValidate:
            soapEnvelope.validate_envelope(self._sdcDevice.xml_validator)
        response_envelope = super().dispatchSoapRequest(path, httpHeader, soapEnvelope)
        if self._sdcDevice.shallValidate:
            response_envelope.validate_envelope(self._sdcDevice.xml_validator)
        return response_envelope

    def _onSubscribe(self, httpHeader, soapEnvelope):
        subscriptionFilters = soapEnvelope.bodyNode.xpath(
            "//wse:Filter[@Dialect='{}/Action']".format(Prefix.DPWS.namespace),
            namespaces=nsmap)
        if len(subscriptionFilters) != 1:
            raise Exception
        else:
            sfilters = subscriptionFilters[0].text
            for sfilter in sfilters.split():
                if sfilter not in self._offeredSubscriptions:
                    raise Exception('{}::{}: "{}" is not in offered subscriptions: {}'.format(self.__class__.__name__,
                                                                                              self.epr,
                                                                                              sfilter,
                                                                                              self._offeredSubscriptions))
        returnedSoapEnvelope = self._subscriptionsManager.onSubscribeRequest(httpHeader, soapEnvelope, self.epr)
        return returnedSoapEnvelope

    def _onUnsubscribe(self, httpHeader, soapEnvelope): #pylint:disable=unused-argument
        returnedSoapEnvelope = self._subscriptionsManager.onUnsubscribeRequest(soapEnvelope)
        return returnedSoapEnvelope

    def _onGetStatus(self, httpHeader, soapEnvelope): #pylint:disable=unused-argument
        returnedSoapEnvelope = self._subscriptionsManager.onGetStatusRequest(soapEnvelope)
        return returnedSoapEnvelope

    def _onRenewStatus(self, httpHeader, soapEnvelope): #pylint:disable=unused-argument
        returnedSoapEnvelope = self._subscriptionsManager.onRenewRequest(soapEnvelope)
        return returnedSoapEnvelope


class DPWSHostedService(EventService):
    ''' An Endpoint (url) with one or more DPWS Types'''

    def __init__(self, sdcDevice, base_urls, path_suffix, subDispatchers, offeredSubscriptions):
        '''
        @param base_urls: urlparse.SplitResult instances. They define the base addresses of this service
        '''
        super(DPWSHostedService, self).__init__(sdcDevice, subDispatchers, offeredSubscriptions)

        self._base_urls = base_urls
        self._mdib = sdcDevice.mdib
        self._my_port_types = [p.port_type_string for p in subDispatchers]
        self._wsdlString = self._mkWsdlString()
        my_uuid = base_urls[0].path
        for s in subDispatchers:
            s.hostingService = self
        self.epr = '/{}/{}'.format(my_uuid, path_suffix)  # end point reference
        endpointReferencesList = []
        for addr in base_urls:
            endpointReferencesList.append(pysoap.soapenvelope.WsaEndpointReferenceType('{}/{}'.format(addr.geturl(), path_suffix)))
        porttype_ns = sdcDevice.mdib.sdc_definitions.PortTypeNamespace
        # little bit ugly: normalizeXMLText needs bytes, not string. and it looks for namespace in "".
        porttype_ns = sdcDevice.mdib.sdc_definitions.normalizeXMLText(b'"'+porttype_ns.encode('utf-8')+b'"')[1:-1].decode('utf-8')
        self.hostedInf = pysoap.soapenvelope.DPWSHosted(endpointReferencesList=endpointReferencesList,
                                                        typesList=[
                                                            etree_.QName(porttype_ns, p) for p in self._my_port_types],
        serviceId=self._my_port_types[0])

        self.register_soapActionCallback('{}/GetMetadata/Request'.format(Prefix.WSX.namespace), self._onGetMetaData)
        self.register_getCallback(path=self.epr, query='wsdl', fn=self._onGetWSDL)


    def _onGetWSDL(self):
        ''' return wsdl'''
        return self._wsdlString


    def _mkWsdlString(self):
        #biceps_schema = self._sdcDevice.mdib.bicepsSchema
        sdc_definitions = self._sdcDevice.mdib.sdc_definitions
        my_nsmap = Prefix.partialMap(Prefix.MSG, Prefix.PM, Prefix.WSA, Prefix.WSE, Prefix.DPWS)
        my_nsmap['tns'] = Prefix.SDC.namespace
        my_nsmap['dt'] = _dt
        porttype_prefix = 'tns'
        my_nsmap['wsdl'] = _wsdl_ns
        my_nsmap['s12'] = _WSDL_S12
        my_nsmap[_wsp_prefix] = _wsp_ns
        wsdl_definitions = etree_.Element(etree_.QName(_wsdl_ns, 'definitions'),
                                          nsmap=my_nsmap,
                                          attrib={'targetNamespace': sdc_definitions.PortTypeNamespace})

        types = etree_.SubElement(wsdl_definitions, etree_.QName(_wsdl_ns, 'types'))
        #remove annotations from schemas, this reduces wsdl size from 280kb to 100kb!
        extSchema_ = etreeFromFile(sdc_definitions.ExtensionPointSchemaFile)
        extSchema = self._removeAnnotations(extSchema_)
        pmSchema_ = etreeFromFile(sdc_definitions.ParticipantModelSchemaFile)
        pmSchema = self._removeAnnotations(pmSchema_)
        bmmSchema_ = etreeFromFile(sdc_definitions.MessageModelSchemaFile)
        bmmSchema = self._removeAnnotations(bmmSchema_)
        types.append(extSchema)
        types.append(pmSchema)
        types.append(bmmSchema)
        # append all message nodes
        for s in self.subDispatchers:
            s.addWsdlMessages(wsdl_definitions)
        for s in self.subDispatchers:
            s.addWsdlPortType(wsdl_definitions)
        for s in self.subDispatchers:
            s.addWsdlBinding(wsdl_definitions, porttype_prefix)

        s =  etree_.tostring(wsdl_definitions)
        return sdc_definitions.denormalizeXMLText(s)


    def _removeAnnotations(self, root_node):
        remove_annotations_string = b'''<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                                      xmlns:xs="http://www.w3.org/2001/XMLSchema">
          <xsl:output method="xml" indent="yes"/>

          <xsl:template match="@* | node()">
            <xsl:copy>
              <xsl:apply-templates select="@* | node()"/>
            </xsl:copy>
          </xsl:template>

          <xsl:template match="xs:annotation" />
        </xsl:stylesheet>'''
        remove_annotations_doc = etree_.parse(BytesIO(remove_annotations_string))
        remove_annotations_xslt = etree_.XSLT(remove_annotations_doc)
        return remove_annotations_xslt(root_node).getroot()


    def _onGetMetaData(self, httpHeader, request):
        _nsm = self._mdib.nsmapper
        response = pysoap.soapenvelope.Soap12Envelope(_nsm.docNssmap)
        replyAddress = request.address.mkReplyAddress('http://schemas.xmlsoap.org/ws/2004/09/mex/GetMetadata/Response')
        response.addHeaderObject(replyAddress)

        metaDataNode = etree_.Element(wsxTag('Metadata'), nsmap=_nsm.docNssmap)

        # Relationship
        metaDataSectionNode = etree_.SubElement(metaDataNode,
                                                wsxTag('MetadataSection'),
                                                attrib={'Dialect': '{}/Relationship'.format(Prefix.DPWS.namespace)})

        relationshipNode = etree_.SubElement(metaDataSectionNode,
                                             dpwsTag('Relationship'),
                                             attrib={'Type': '{}/host'.format(Prefix.DPWS.namespace)})
        self._sdcDevice.dpwsHost.asEtreeSubNode(relationshipNode)

        self.hostedInf.asEtreeSubNode(relationshipNode)

        metaDataSectionNode = etree_.SubElement(metaDataNode,
                                                wsxTag('MetadataSection'),
                                                attrib={'Dialect': _wsdl_ns})
        locationNode = etree_.SubElement(metaDataSectionNode,
                                         wsxTag('Location'))
        # determine the correct location of wsdl, depending on call
        host = httpHeader['Host'] # this is the address that was called.
        my_baseUrls = [u for u in self._base_urls if u.netloc == host]
        my_baseUrl = my_baseUrls[0] if len(my_baseUrls) > 0 else self._base_urls[0]
        locationNode.text = '{}://{}{}/?wsdl'.format(my_baseUrl.scheme,
                                               my_baseUrl.netloc,
                                               self.epr                                               )
        response.addBodyElement(metaDataNode)
        return response

    def __repr__(self):
        return '{} epr={} Porttypes={}'.format(self.__class__.__name__, self.epr, [dp.port_type_string for dp in self.subDispatchers])


class DPWSPortTypeImpl(SOAPActionRegistry):
    ''' Base class of all PortType implementations'''
    WSDLOperationBindings = () # overwrite in derived classes
    WSDLMessageDescriptions = () # overwrite in derived classes

    def __init__(self, port_type_string, sdcDevice):
        '''
        :param port_type_string: port type without namespace, e.g 'Get'
        :param sdcDevice:
        '''
        super(DPWSPortTypeImpl, self).__init__()
        self.port_type_string = port_type_string
        self._sdcDevice = sdcDevice
        self._mdib = sdcDevice.mdib
        self.hostingService = None  # the parent

    def _getActionString(self, methodName):
        actions_lookup = self._mdib.sdc_definitions.Actions
        return getattr(actions_lookup, methodName)

    def addWsdlPortType(self, parentNode):
        raise NotImplementedError

    def __repr__(self):
        return '{} Porttype={} actions={}'.format(self.__class__.__name__, self.port_type_string,
                                                  self._soapActionCallbacks.keys())


    def addWsdlMessages(self, parentNode):
        '''
        add wsdl:message node to parentNode.
        xml looks like this:
        <wsdl:message name="GetMdDescription">
            <wsdl:part element="msg:GetMdDescription" name="parameters" />
        </wsdl:message>
        :param parentNode:
        :return:
        '''
        for msg in self.WSDLMessageDescriptions:
            elem = etree_.SubElement(parentNode, _wsdl_message, attrib={'name': msg.name})
            for elementName in msg.parameters:
                etree_.SubElement(elem, _wsdl_part,
                                  attrib={'name': 'parameters',
                                          'element': elementName})


    def addWsdlBinding(self, parentNode, porttype_prefix):
        '''
        add wsdl:binding node to parentNode.
        xml looks like this:
        <wsdl:binding name="GetBinding" type="msg:Get">
            <s12:binding style="document" transport="http://schemas.xmlsoap.org/soap/http" />
            <wsdl:operation name="GetMdib">
                <s12:operation soapAction="http://p11073-10207/draft6/msg/2016/12/08/Get/GetMdib" />
                <wsdl:input>
                    <s12:body use="literal" />
                </wsdl:input>
                <wsdl:output>
                    <s12:body use="literal" />
                </wsdl:output>
            </wsdl:operation>
            ...
        </wsdl:binding>
        :param parentNode:
        :param porttype_prefix:
        :return:
        '''
        v_ref = self._sdcDevice.mdib.sdc_definitions
        wsdl_binding = etree_.SubElement(parentNode, etree_.QName(_wsdl_ns,'binding'),
                                     attrib={'name': self.port_type_string+'Binding', 'type': '{}:{}'.format(porttype_prefix, self.port_type_string)})
        s12_binding = etree_.SubElement(wsdl_binding, etree_.QName(_WSDL_S12, 'binding'),
                                        attrib={'style':'document', 'transport': 'http://schemas.xmlsoap.org/soap/http'})
        #ToDo: wsp:policy?
        for wsdl_op in self.WSDLOperationBindings:
            wsdl_operation = etree_.SubElement(wsdl_binding, etree_.QName(_wsdl_ns, 'operation'), attrib={'name': wsdl_op.name})
            s12_operation = etree_.SubElement(wsdl_operation, etree_.QName(_WSDL_S12, 'operation'),
                                              attrib={'soapAction': '{}/{}/{}'.format(v_ref.ActionsNamespace, self.port_type_string, wsdl_op.name)})
            if wsdl_op.input is not None:
                wsdl_input = etree_.SubElement(wsdl_operation, etree_.QName(_wsdl_ns, 'input'))
                etree_.SubElement(wsdl_input, etree_.QName(_WSDL_S12, 'body'), attrib={'use': wsdl_op.input})
            if wsdl_op.output is not None:
                wsdl_output = etree_.SubElement(wsdl_operation, etree_.QName(_wsdl_ns, 'output'))
                etree_.SubElement(wsdl_output, etree_.QName(_WSDL_S12, 'body'), attrib={'use': wsdl_op.output})
        _addPolicy_dpwsProfile(wsdl_binding)


def __mkWsdlOperation(parentNode, operationName, inputMessageName, outputMessageName,  fault):
    elem = etree_.SubElement(parentNode, _wsdl_operation, attrib={'name': operationName})
    if inputMessageName is not None:
        etree_.SubElement(elem,  etree_.QName(_wsdl_ns, 'input'),
                          attrib={'message': '{}:{}'.format('tns', inputMessageName),
                                 })
    if outputMessageName is not None:
        etree_.SubElement(elem,  etree_.QName(_wsdl_ns, 'output'),
                          attrib={'message': '{}:{}'.format('tns', outputMessageName),
                                 })
    if fault is not None:
        faultName, messageName, action = fault # unpack 3 parameters
        etree_.SubElement(elem,  etree_.QName(_wsdl_ns, 'fault'),
                          attrib={'name': faultName,
                                  'message': '{}:{}'.format('tns', messageName),
                                  })
    return elem


def _mkWsdlTwowayOperation(parentNode, operationName, inputMessageName=None, outputMessageName=None,  fault=None):
    # has input and output
    input_MessageName = inputMessageName or operationName # defaults to operation name
    output_MessageName = outputMessageName or operationName+'Response' # defaults to operation name + "Response"
    return __mkWsdlOperation(parentNode, operationName=operationName, inputMessageName=input_MessageName,
                            outputMessageName=output_MessageName, fault=fault)

def _mkWsdlOnewayOperation(parentNode, operationName, outputMessageName=None, fault=None):
    # has only output
    output_MessageName = outputMessageName or operationName # defaults to operation name
    return __mkWsdlOperation(parentNode, operationName=operationName, inputMessageName=None,
                            outputMessageName=output_MessageName, fault=fault)

def _addPolicy_dpwsProfile(parentNode):
    '''
    :param parentNode:
    :return: <wsp:Policy>
            <dpws:Profile wsp:Optional="true"/>
            <mdpws:Profile wsp:Optional="true"/>
          </wsp:Policy>
    '''
    wsp_policyNode = etree_.SubElement(parentNode, etree_.QName(_wsp_ns, 'Policy'), attrib=None)
    _ = etree_.SubElement(wsp_policyNode, dpwsTag('Profile'), attrib={etree_.QName(_wsp_ns, 'Optional'): 'true'})
    _ = etree_.SubElement(wsp_policyNode, mdpwsTag('Profile'), attrib={etree_.QName(_wsp_ns, 'Optional'): 'true'})


class GetService(DPWSPortTypeImpl):
    WSDLMessageDescriptions = (WSDLMessageDescription('GetMdState', ('{}:GetMdState'.format(_msg_prefix),)),
                               WSDLMessageDescription('GetMdStateResponse', ('{}:GetMdStateResponse'.format(_msg_prefix),)),
                               WSDLMessageDescription('GetMdib', ('{}:GetMdib'.format(_msg_prefix),)),
                               WSDLMessageDescription('GetMdibResponse', ('{}:GetMdibResponse'.format(_msg_prefix),)),
                               WSDLMessageDescription('GetMdDescription', ('{}:GetMdDescription'.format(_msg_prefix),)),
                               WSDLMessageDescription('GetMdDescriptionResponse', ('{}:GetMdDescriptionResponse'.format(_msg_prefix),)),
                               )
    WSDLOperationBindings = (WSDLOperationBinding('GetMdState', 'literal', 'literal'),
                             WSDLOperationBinding('GetMdib', 'literal', 'literal'),
                             WSDLOperationBinding('GetMdDescription', 'literal', 'literal'),)

    def __init__(self, port_type_string, sdcDevice):
        super(GetService, self).__init__(port_type_string, sdcDevice )
        actions = self._mdib.sdc_definitions.Actions
        self.register_soapActionCallback(actions.GetMdState, self._onGetMdState)
        self.register_soapActionCallback(actions.GetMdib, self._onGetMdib)
        self.register_soapActionCallback(actions.GetMdDescription, self._onGetMdDescription)


    def _onGetMdState(self, httpHeader, request):  # pylint:disable=unused-argument
        self._logger.debug('_onGetMdState')
        requestedHandles = request.bodyNode.xpath('*/msg:HandleRef/text()', namespaces=nsmap)
        if len(requestedHandles) > 0:
            self._logger.info('_onGetMdState requested Handles:{}', requestedHandles)

        # get the requested state containers from mdib
        stateContainers = []
        with self._mdib.mdibLock:
            if len(requestedHandles) == 0:
                # MessageModel: If the HANDLE reference list is empty, all states in the MDIB SHALL be included in the result list.
                for stateContainer in self._mdib.states.objects:
                    stateContainers.append(stateContainer)
                if self._sdcDevice.contextstates_in_getmdib:
                    for stateContainer in self._mdib.contextStates.objects:
                        stateContainers.append(stateContainer)
            else:
                if self._sdcDevice.contextstates_in_getmdib:
                    for handle in requestedHandles:
                        try:
                            # If a HANDLE reference does match a multi state HANDLE, the corresponding multi state SHALL be included in the result list
                            stateContainers.append(self._mdib.contextStates.handle.getOne(handle))
                        except RuntimeError:
                            # If a HANDLE reference does match a descriptor HANDLE, all states that belong to the corresponding descriptor SHALL be included in the result list
                            stateContainers.extend(self._mdib.states.descriptorHandle.get(handle, []))
                            stateContainers.extend(self._mdib.contextStates.descriptorHandle.get(handle, []))
                else:
                    for handle in requestedHandles:
                        stateContainers.extend(self._mdib.states.descriptorHandle.get(handle, []))

                self._logger.info('_onGetMdState requested Handles:{} found {} states', requestedHandles,
                                  len(stateContainers))

            # build response
            nsmapper = self._mdib.nsmapper
            responseSoapEnvelope = pysoap.soapenvelope.Soap12Envelope(
                nsmapper.partialMap(Prefix.S12, Prefix.WSA, Prefix.PM, Prefix.MSG))
            replyAddress = request.address.mkReplyAddress(action=self._getActionString('GetMdStateResponse'))
            responseSoapEnvelope.addHeaderObject(replyAddress)
            getMdStateResponseNode = etree_.Element(msgTag('GetMdStateResponse'), nsmap=nsmap)
            self._mdib.mdib_version_group.update_node(getMdStateResponseNode)

            mdStateNode = etree_.Element(msgTag('MdState'), attrib=None, nsmap=self._mdib.nsmapper.docNssmap)
            for stateContainer in stateContainers:
                mdStateNode.append(stateContainer.mkStateNode())

            getMdStateResponseNode.append(mdStateNode)
            responseSoapEnvelope.addBodyElement(getMdStateResponseNode)
        self._logger.debug('_onGetMdState returns {}', lambda: responseSoapEnvelope.as_xml(pretty=False))
        return responseSoapEnvelope

    def _onGetMdib(self, httpHeader, request):  # pylint:disable=unused-argument
        self._logger.debug('_onGetMdib')
        nsmapper = self._mdib.nsmapper
        responseSoapEnvelope = pysoap.soapenvelope.Soap12Envelope(
            nsmapper.partialMap(Prefix.S12, Prefix.WSA, Prefix.PM, Prefix.MSG))
        replyAddress = request.address.mkReplyAddress(action=self._getActionString('GetMdibResponse'))
        responseSoapEnvelope.addHeaderObject(replyAddress)
        if self._sdcDevice.contextstates_in_getmdib:
            mdibNode, mdib_version_group = self._mdib.reconstructMdibWithContextStates()
        else:
            mdibNode, mdib_version_group = self._mdib.reconstructMdib()
        getMdibResponseNode = etree_.Element(msgTag('GetMdibResponse'), nsmap=Prefix.partialMap(Prefix.MSG, Prefix.PM))
        mdib_version_group.update_node(getMdibResponseNode)
        getMdibResponseNode.append(mdibNode)
        responseSoapEnvelope.addBodyElement(getMdibResponseNode)
        self._logger.debug('_onGetMdib returns {}', lambda: responseSoapEnvelope.as_xml(pretty=False))
        return responseSoapEnvelope

    def _onGetMdDescription(self, httpHeader, request):  # pylint:disable=unused-argument
        '''
        MdDescription comprises the requested set of MDS descriptors. Which MDS descriptors are included depends on the msg:GetMdDescription/msg:HandleRef list:
        - If the HANDLE reference list is empty, all MDS descriptors SHALL be included in the result list.
        - If a HANDLE reference does match an MDS descriptor, it SHALL be included in the result list.
        - If a HANDLE reference does not match an MDS descriptor (any other descriptor), the MDS descriptor that is in the parent tree of the HANDLE reference SHOULD be included in the result list.
        '''
        # currently this implementation only supports a single mds.
        # => if at least one handle matches any descriptor, the one mds is returned, otherwise empty payload

        self._logger.debug('_onGetMdDescription')
        requestedHandles = request.bodyNode.xpath('*/msg:HandleRef/text()', namespaces=nsmap)
        if len(requestedHandles) > 0:
            self._logger.info('_onGetMdDescription requested Handles:{}', requestedHandles)
        includeMds = True if len(requestedHandles) == 0 else False  # if we have handles, we need to check them
        for h in requestedHandles:
            if self._sdcDevice.mdib.descriptions.handle.getOne(h, allowNone=True) is not None:
                includeMds = True
                break
        my_namespaces = self._sdcDevice.mdib.nsmapper.partialMap(Prefix.S12, Prefix.WSA, Prefix.MSG, Prefix.PM)
        responseSoapEnvelope = pysoap.soapenvelope.Soap12Envelope(my_namespaces)
        replyAddress = request.address.mkReplyAddress(action=self._getActionString('GetMdDescriptionResponse'))
        responseSoapEnvelope.addHeaderObject(replyAddress)

        getMdDescriptionResponseNode = etree_.Element(msgTag('GetMdDescriptionResponse'),
                                                      nsmap=nsmap)

        if includeMds:
            mdDescriptionNode, mdib_version_group = self._mdib.reconstructMdDescription()
            mdDescriptionNode.tag = msgTag('MdDescription')  # rename according to message
            mdib_version_group.update_node(getMdDescriptionResponseNode)
        else:
            mdDescriptionNode = etree_.Element(msgTag('MdDescription'))
            self._mdib.mdib_version_group.update_node(getMdDescriptionResponseNode)

        getMdDescriptionResponseNode.append(mdDescriptionNode)
        responseSoapEnvelope.addBodyElement(getMdDescriptionResponseNode)
        self._logger.debug('_onGetMdDescription returns {}', lambda: responseSoapEnvelope.as_xml(pretty=False))
        return responseSoapEnvelope


    def addWsdlPortType(self, parentNode):
        '''
        add wsdl:portType node to parentNode.
        xml looks like this:
        <wsdl:portType name="GetService" dpws:DiscoveryType="dt:ServiceProvider">
          <wsdl:operation name="GetMdState">
            <wsdl:input message="msg:GetMdState"/>
            <wsdl:output message="msg:GetMdStateResponse"/>
          </wsdl:operation>
          <wsp:Policy>
            <dpws:Profile wsp:Optional="true"/>
          </wsp:Policy>
          ...
        </wsdl:portType>
        :param parentNode:
        :return:
        '''
        if 'dt' in parentNode.nsmap:
            portType = etree_.SubElement(parentNode, etree_.QName(_wsdl_ns,'portType'),
                                         attrib={'name': self.port_type_string,
                                                 dpwsTag('DiscoveryType'):'dt:ServiceProvider'})
        else:
            portType = etree_.SubElement(parentNode, etree_.QName(_wsdl_ns, 'portType'),
                                         attrib={'name': self.port_type_string})
        _mkWsdlTwowayOperation(portType, operationName='GetMdState')
        _mkWsdlTwowayOperation(portType, operationName='GetMdib')
        _mkWsdlTwowayOperation(portType, operationName='GetMdDescription')


class ContainmentTreeService(DPWSPortTypeImpl):
    WSDLMessageDescriptions = (WSDLMessageDescription('GetDescriptor', ('{}:GetDescriptor'.format(_msg_prefix),)),
                               WSDLMessageDescription('GetDescriptorResponse', ('{}:GetDescriptorResponse'.format(_msg_prefix),)),
                               WSDLMessageDescription('GetContainmentTree', ('{}:GetContainmentTreeResponse'.format(_msg_prefix),)),
                               WSDLMessageDescription('GetContainmentTreeResponse', ('{}:GetContainmentTreeResponse'.format(_msg_prefix),)),
                               )
    WSDLOperationBindings = (WSDLOperationBinding('GetDescriptor', 'literal', 'literal'),
                             WSDLOperationBinding('GetContainmentTree', 'literal', 'literal'))


    def __init__(self, port_type_string, sdcDevice):
        super(ContainmentTreeService, self).__init__(port_type_string, sdcDevice)
        actions = self._mdib.sdc_definitions.Actions
        self.register_soapActionCallback(actions.GetContainmentTree, self._onGetContainmentTree)
        self.register_soapActionCallback(actions.GetDescriptor, self._onGetDescriptor)


    def _onGetContainmentTree(self, httpHeader, request):
        #ToDo: implement, currently method only raises a soap fault
        raise FunctionNotImplementedError(request)


    def _onGetDescriptor(self, httpHeader, request):
        #ToDo: implement, currently method only raises a soap fault
        raise FunctionNotImplementedError(request)


    def addWsdlPortType(self, parentNode):
        portType = etree_.SubElement(parentNode, etree_.QName(_wsdl_ns,'portType'),
                                     attrib={'name':self.port_type_string,
                                             dpwsTag('DiscoveryType'):'dt:ServiceProvider'})
        _mkWsdlTwowayOperation(portType, operationName='GetDescriptor')
        _mkWsdlTwowayOperation(portType, operationName='GetContainmentTree')


class SetService(DPWSPortTypeImpl):
    WSDLMessageDescriptions = (WSDLMessageDescription('Activate', ('{}:Activate'.format(_msg_prefix),)),
                               WSDLMessageDescription('ActivateResponse', ('{}:ActivateResponse'.format(_msg_prefix),)),
                               WSDLMessageDescription('SetString', ('{}:SetString'.format(_msg_prefix),)),
                               WSDLMessageDescription('SetStringResponse', ('{}:SetStringResponse'.format(_msg_prefix),)),
                               WSDLMessageDescription('SetComponentState', ('{}:SetComponentState'.format(_msg_prefix),)),
                               WSDLMessageDescription('SetComponentStateResponse', ('{}:SetComponentStateResponse'.format(_msg_prefix),)),
                               WSDLMessageDescription('SetAlertState', ('{}:SetAlertState'.format(_msg_prefix),)),
                               WSDLMessageDescription('SetAlertStateResponse', ('{}:SetAlertStateResponse'.format(_msg_prefix),)),
                               WSDLMessageDescription('SetMetricState', ('{}:SetMetricState'.format(_msg_prefix),)),
                               WSDLMessageDescription('SetMetricStateResponse', ('{}:SetMetricStateResponse'.format(_msg_prefix),)),
                               WSDLMessageDescription('SetValue', ('{}:SetValue'.format(_msg_prefix),)),
                               WSDLMessageDescription('SetValueResponse', ('{}:SetValueResponse'.format(_msg_prefix),)),
                               WSDLMessageDescription('OperationInvokedReport', ('{}:OperationInvokedReport'.format(_msg_prefix),)),
                               )
    WSDLOperationBindings = (WSDLOperationBinding('Activate', 'literal', 'literal'), # fault?
                             WSDLOperationBinding('SetString', 'literal', 'literal'),  # fault?
                             WSDLOperationBinding('SetComponentState', 'literal', 'literal'), # fault?
                             WSDLOperationBinding('SetAlertState', 'literal', 'literal'), # fault?
                             WSDLOperationBinding('SetMetricState', 'literal', 'literal'), # fault?
                             WSDLOperationBinding('SetValue', 'literal', 'literal'), # fault?
                             WSDLOperationBinding('OperationInvokedReport', None, 'literal'),
                             )

    def __init__(self, port_type_string, sdcDevice):
        super(SetService, self).__init__(port_type_string, sdcDevice)
        actions = self._mdib.sdc_definitions.Actions
        self.register_soapActionCallback(actions.Activate, self._onActivate)
        self.register_soapActionCallback(actions.SetValue, self._onSetValue)
        self.register_soapActionCallback(actions.SetString, self._onSetString)
        self.register_soapActionCallback(actions.SetMetricState, self._onSetMetricState)
        self.register_soapActionCallback(actions.SetAlertState, self._onSetAlertState)
        self.register_soapActionCallback(actions.SetComponentState, self._onSetComponentState)


    def _onActivate(self, httpHeader, request): #pylint:disable=unused-argument
        '''Handler for Active calls.
        It enques an operation and generates the expected operation invoked report. '''
        return self._handleOperationRequest(request, 'ActivateResponse')

    def _onSetValue(self, httpHeader, request): #pylint:disable=unused-argument
        '''Handler for SetValue calls.
        It enques an operation and generates the expected operation invoked report. '''
        self._logger.info('_onSetValue')
        ret = self._handleOperationRequest(request, 'SetValueResponse')
        self._logger.info('_onSetValue done')
        return ret

    def _onSetString(self, httpHeader, request):  # pylint:disable=unused-argument
        '''Handler for SetString calls.
        It enques an operation and generates the expected operation invoked report.'''
        self._logger.debug('_onSetString')
        return self._handleOperationRequest(request, 'SetStringResponse')

    def _onSetMetricState(self, httpHeader, request):  # pylint:disable=unused-argument
        '''Handler for SetMetricState calls.
        It enques an operation and generates the expected operation invoked report.'''
        self._logger.debug('_onSetMetricState')
        return self._handleOperationRequest(request, 'SetMetricStateResponse')


    def _onSetAlertState(self, httpHeader, request):  # pylint:disable=unused-argument
        '''Handler for SetMetricState calls.
        It enques an operation and generates the expected operation invoked report.'''
        self._logger.debug('_onSetAlertState')
        return self._handleOperationRequest(request, 'SetAlertStateResponse')


    def _onSetComponentState(self, httpHeader, request):  # pylint:disable=unused-argument
        '''Handler for SetMetricState calls.
        It enques an operation and generates the expected operation invoked report.'''
        self._logger.debug('_onSetAlertState')
        return self._handleOperationRequest(request, 'SetComponentStateResponse')


    def _handleOperationRequest(self, request, responseName):  # pylint:disable=unused-argument
        '''
        It enques an operation and generate the expected operation invoked report.
        :param request:
        :param responseName:
        :return:
        '''
        response = pysoap.soapenvelope.Soap12Envelope(self._mdib.nsmapper.partialMap(Prefix.S12, Prefix.WSA))
        replyAddress = request.address.mkReplyAddress(action=self._getActionString(responseName))
        response.addHeaderObject(replyAddress)
        replyBodyNode = etree_.Element(msgTag(responseName),
                                       nsmap=Prefix.partialMap(Prefix.MSG))
        self._mdib.mdib_version_group.update_node(replyBodyNode)

        invocationInfoNode = etree_.SubElement(replyBodyNode, msgTag('InvocationInfo'))

        transactionIdNode = etree_.SubElement(invocationInfoNode, msgTag('TransactionId'))
        invocationStateNode = etree_.SubElement(invocationInfoNode, msgTag('InvocationState'))

        errorTexts = []

        operationHandleRefs = request.bodyNode.xpath('*/msg:OperationHandleRef/text()', namespaces=nsmap)
        operation = None
        if len(operationHandleRefs) == 1:
            operationHandleRef = operationHandleRefs[0]
            operation = self._sdcDevice.getOperationByHandle(operationHandleRef)
            if operation is None:
                errorTexts.append('operation not known: "{}"'.format(operationHandleRef))
        else:
            errorTexts.append('no OperationHandleRef found in Request')

        if errorTexts:
            self._logger.warn('_handleOperationRequest: errorTexts = {}'.format(errorTexts))

            invocationStateNode.text = pmtypes.InvocationState.FAILED
            transactionIdNode.text = '0'
            operationErrorNode = etree_.SubElement(invocationInfoNode, msgTag('InvocationError'))
            operationErrorNode.text = pmtypes.InvocationError.INVALID_VALUE
            operationErrorMessageNode = etree_.SubElement(invocationInfoNode,
                                                          msgTag('InvocationErrorMessage'))
            operationErrorMessageNode.text = '; '.join(errorTexts)
        else:
            self._logger.info('_handleOperationRequest: enqueued')
            transactionId = self._sdcDevice.enqueueOperation(operation, request)
            transactionIdNode.text = str(transactionId)
            invocationStateNode.text = pmtypes.InvocationState.WAIT

        response.addBodyElement(replyBodyNode)
        return response

    def addWsdlPortType(self, parentNode):
        portType = etree_.SubElement(parentNode, etree_.QName(_wsdl_ns,'portType'),
                                     attrib={'name':self.port_type_string,
                                             dpwsTag('DiscoveryType'):'dt:ServiceProvider',
                                             wseTag('EventSource'): 'true'})
        _mkWsdlTwowayOperation(portType, operationName='Activate')
        _mkWsdlTwowayOperation(portType, operationName='SetString')
        _mkWsdlTwowayOperation(portType, operationName='SetComponentState')
        _mkWsdlTwowayOperation(portType, operationName='SetAlertState')
        _mkWsdlTwowayOperation(portType, operationName='SetMetricState')
        _mkWsdlTwowayOperation(portType, operationName='SetValue')
        _mkWsdlOnewayOperation(portType, operationName='OperationInvokedReport')


class WaveformService(DPWSPortTypeImpl):
    WSDLMessageDescriptions = (WSDLMessageDescription('Waveform', ('{}:WaveformStreamReport'.format(_msg_prefix),)),)
    WSDLOperationBindings = (WSDLOperationBinding('Waveform', None, 'literal'),)

    def addWsdlPortType(self, parentNode):
        portType = etree_.SubElement(parentNode, etree_.QName(_wsdl_ns,'portType'),
                                     attrib={'name':self.port_type_string,
                                             dpwsTag('DiscoveryType'):'dt:ServiceProvider',
                                             wseTag('EventSource'): 'true'})
        _mkWsdlOnewayOperation(portType, operationName='Waveform')


class StateEventService(DPWSPortTypeImpl):
    WSDLMessageDescriptions = (WSDLMessageDescription('EpisodicAlertReport', ('{}:EpisodicAlertReport'.format(_msg_prefix),)),
                               WSDLMessageDescription('SystemErrorReport', ('{}:SystemErrorReport'.format(_msg_prefix),)),
                               WSDLMessageDescription('PeriodicAlertReport', ('{}:PeriodicAlertReport'.format(_msg_prefix),)),
                               WSDLMessageDescription('EpisodicComponentReport', ('{}:EpisodicComponentReport'.format(_msg_prefix),)),
                               WSDLMessageDescription('PeriodicOperationalStateReport', ('{}:PeriodicOperationalStateReport'.format(_msg_prefix),)),
                               WSDLMessageDescription('PeriodicComponentReport', ('{}:PeriodicComponentReport'.format(_msg_prefix),)),
                               WSDLMessageDescription('EpisodicOperationalStateReport', ('{}:EpisodicOperationalStateReport'.format(_msg_prefix),)),
                               WSDLMessageDescription('PeriodicMetricReport', ('{}:PeriodicMetricReport'.format(_msg_prefix),)),
                               WSDLMessageDescription('EpisodicMetricReport', ('{}:EpisodicMetricReport'.format(_msg_prefix),)),
                               )

    WSDLOperationBindings = (WSDLOperationBinding('EpisodicAlertReport', None, 'literal'),
                             WSDLOperationBinding('SystemErrorReport', None, 'literal'),
                             WSDLOperationBinding('PeriodicAlertReport', None, 'literal'),
                             WSDLOperationBinding('EpisodicComponentReport', None, 'literal'),
                             WSDLOperationBinding('PeriodicOperationalStateReport', None, 'literal'),
                             WSDLOperationBinding('PeriodicComponentReport', None, 'literal'),
                             WSDLOperationBinding('EpisodicOperationalStateReport', None, 'literal'),
                             WSDLOperationBinding('PeriodicMetricReport', None, 'literal'),
                             WSDLOperationBinding('EpisodicMetricReport', None, 'literal'),
                             )

    def addWsdlPortType(self, parentNode):
        portType = etree_.SubElement(parentNode, etree_.QName(_wsdl_ns,'portType'),
                                     attrib={'name':self.port_type_string,
                                             dpwsTag('DiscoveryType'):'dt:ServiceProvider',
                                             wseTag('EventSource'): 'true'})
        _mkWsdlOnewayOperation(portType, operationName='EpisodicAlertReport')
        _mkWsdlOnewayOperation(portType, operationName='SystemErrorReport')
        _mkWsdlOnewayOperation(portType, operationName='PeriodicAlertReport')
        _mkWsdlOnewayOperation(portType, operationName='EpisodicComponentReport')
        _mkWsdlOnewayOperation(portType, operationName='PeriodicOperationalStateReport')
        _mkWsdlOnewayOperation(portType, operationName='PeriodicComponentReport')
        _mkWsdlOnewayOperation(portType, operationName='EpisodicOperationalStateReport')
        _mkWsdlOnewayOperation(portType, operationName='PeriodicMetricReport')
        _mkWsdlOnewayOperation(portType, operationName='EpisodicMetricReport')


class ContextService(DPWSPortTypeImpl):
    WSDLMessageDescriptions = (WSDLMessageDescription('SetContextState', ('{}:SetContextState'.format(_msg_prefix),)),
                               WSDLMessageDescription('SetContextStateResponse', ('{}:SetContextStateResponse'.format(_msg_prefix),)),
                               WSDLMessageDescription('GetContextStates', ('{}:GetContextStates'.format(_msg_prefix),)),
                               WSDLMessageDescription('GetContextStatesResponse', ('{}:GetContextStatesResponse'.format(_msg_prefix),)),
                               WSDLMessageDescription('EpisodicContextReport', ('{}:EpisodicContextReport'.format(_msg_prefix),)),
                               WSDLMessageDescription('PeriodicContextReport', ('{}:PeriodicContextReport'.format(_msg_prefix),)),
                               )
    WSDLOperationBindings = (WSDLOperationBinding('SetContextState', 'literal', 'literal'), #ToDo: generate wsdl:fault
                             WSDLOperationBinding('GetContextStates', 'literal', 'literal'),
                             WSDLOperationBinding('EpisodicContextReport', None, 'literal'),
                             WSDLOperationBinding('PeriodicContextReport', None, 'literal'),
                             )


    def __init__(self, port_type_string, sdcDevice):
        super(ContextService, self).__init__(port_type_string, sdcDevice)
        actions = self._mdib.sdc_definitions.Actions
        self.register_soapActionCallback(actions.SetContextState, self._onSetContextState)
        self.register_soapActionCallback(actions.GetContextStates, self._onGetContextStates)

    def _onSetContextState(self, httpHeader, request):  # pylint:disable=unused-argument
        ''' enqueues an operation and returns a 'wait' reponse.'''
        response = pysoap.soapenvelope.Soap12Envelope(
            self._mdib.nsmapper.partialMap(Prefix.S12, Prefix.PM, Prefix.WSA, Prefix.MSG))
        replyAddress = request.address.mkReplyAddress(action=self._getActionString('SetContextStateResponse'))
        response.addHeaderObject(replyAddress)
        replyBodyNode = etree_.Element(msgTag('SetContextStateResponse'),
                                       nsmap=Prefix.partialMap(Prefix.MSG),
                                       attrib={'SequenceId': self._mdib.sequenceId,
                                               'MdibVersion': str(self._mdib.mdibVersion)})
        invocationInfoNode = etree_.SubElement(replyBodyNode,
                                               msgTag('InvocationInfo'))
        transactionIdNode = etree_.SubElement(invocationInfoNode, msgTag('TransactionId'))
        invocationStateNode = etree_.SubElement(invocationInfoNode, msgTag('InvocationState'))

        errorTexts = []

        operationHandleRefs = request.bodyNode.xpath('msg:SetContextState/msg:OperationHandleRef/text()',
                                                     namespaces=nsmap)
        if len(operationHandleRefs) == 1:
            operationHandleRef = operationHandleRefs[0]
            operation = self._sdcDevice.getOperationByHandle(operationHandleRef)
            if operation is None:
                errorTexts.append('operation "{}" not known'.format(operationHandleRef))
        elif len(operationHandleRefs) > 1:
            errorTexts.append('multiple OperationHandleRefs found: "{}"'.format(operationHandleRefs))
        else:
            errorTexts.append('no OperationHandleRef found')

        if errorTexts:
            invocationStateNode.text = pmtypes.InvocationState.FAILED
            transactionIdNode.text = '0'
            operationErrorNode = etree_.SubElement(invocationInfoNode, msgTag('InvocationError'))
            operationErrorNode.text = pmtypes.InvocationError.INVALID_VALUE
            operationErrorMessageNode = etree_.SubElement(invocationInfoNode,
                                                          msgTag('InvocationErrorMessage'))
            operationErrorMessageNode.text = '; '.join(errorTexts)
        else:
            transactionId = self._sdcDevice.enqueueOperation(operation, request)
            transactionIdNode.text = str(transactionId)
            invocationStateNode.text = pmtypes.InvocationState.WAIT

        response.addBodyElement(replyBodyNode)
        return response

    def _onGetContextStates(self, httpHeader, request):  # pylint:disable=unused-argument
        self._logger.debug('_onGetContextStates')
        requestedHandles = request.bodyNode.xpath('*/msg:HandleRef/text()', namespaces=nsmap)
        if len(requestedHandles) > 0:
            self._logger.info('_onGetContextStates requested Handles:{}', requestedHandles)
        nsmapper = self._mdib.nsmapper
        response = pysoap.soapenvelope.Soap12Envelope(
            nsmapper.partialMap(Prefix.S12, Prefix.WSA, Prefix.PM, Prefix.MSG))
        replyAddress = request.address.mkReplyAddress(action=self._getActionString('GetContextStatesResponse'))
        response.addHeaderObject(replyAddress)
        getContextStatesResponseNode = etree_.Element(msgTag('GetContextStatesResponse'))
        with self._mdib.mdibLock:
            self._mdib.mdib_version_group.update_node(getContextStatesResponseNode)
            if len(requestedHandles) == 0:
                # MessageModel: If the HANDLE reference list is empty, all states in the MDIB SHALL be included in the result list.
                contextStateContainers = list(self._mdib.contextStates.objects)
            else:
                contextStateContainersLookup = OrderedDict() # lookup to avoid double entries
                for handle in requestedHandles:
                    # If a HANDLE reference does match a multi state HANDLE,
                    # the corresponding multi state SHALL be included in the result list
                    tmp = self._mdib.contextStates.handle.getOne(handle, allowNone=True)
                    if tmp:
                        tmp = [tmp]
                    if not tmp:
                        # If a HANDLE reference does match a descriptor HANDLE,
                        # all states that belong to the corresponding descriptor SHALL be included in the result list
                        tmp = self._mdib.contextStates.descriptorHandle.get(handle)
                    if not tmp:
                        # R5042: If a HANDLE reference from the msg:GetContextStates/msg:HandleRef list does match an
                        # MDS descriptor, then all context states that are part of this MDS SHALL be included in the result list.
                        descr = self._mdib.descriptions.handle.getOne(handle, allowNone=True)
                        if descr:
                            if descr.NODETYPE == domTag('MdsDescriptor'):
                                tmp = list(self._mdib.contextStates.objects)
                    if tmp:
                        for st in tmp:
                            contextStateContainersLookup[st.Handle] = st
                contextStateContainers = contextStateContainersLookup.values()
            if contextStateContainers:
                for contextStateContainer in contextStateContainers:
                    node = contextStateContainer.mkStateNode()
                    getContextStatesResponseNode.append(node)
                    node.tag = msgTag('ContextState')
        response.addBodyElement(getContextStatesResponseNode)
        self._logger.debug('_onGetContextStates returns {}', lambda: response.as_xml(pretty=False))
        return response

    def addWsdlPortType(self, parentNode):
        portType = etree_.SubElement(parentNode, etree_.QName(_wsdl_ns,'portType'),
                                     attrib={'name': self.port_type_string,
                                             dpwsTag('DiscoveryType'):'dt:ServiceProvider',
                                             wseTag('EventSource'): 'true'})
        _mkWsdlTwowayOperation(portType, operationName='SetContextState')
        _mkWsdlTwowayOperation(portType, operationName='GetContextStates')
        _mkWsdlOnewayOperation(portType, operationName='EpisodicContextReport')
        _mkWsdlOnewayOperation(portType, operationName='PeriodicContextReport')



class DescriptionEventService(DPWSPortTypeImpl):
    WSDLMessageDescriptions = (WSDLMessageDescription('DescriptionModificationReport', ('{}:DescriptionModificationReport'.format(_msg_prefix),)),
                               )
    WSDLOperationBindings = (WSDLOperationBinding('DescriptionModificationReport', None, 'literal'),
                             )

    def addWsdlPortType(self, parentNode):
        portType = etree_.SubElement(parentNode, etree_.QName(_wsdl_ns,'portType'),
                                     attrib={'name':self.port_type_string,
                                             dpwsTag('DiscoveryType'):'dt:ServiceProvider',
                                             wseTag('EventSource'): 'true'})
        _mkWsdlOnewayOperation(portType, operationName='DescriptionModificationReport')
