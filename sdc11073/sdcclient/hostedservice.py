import weakref
from lxml import etree as etree_
import urllib

from sdc11073 import namespaces
from .. import loghelper
from ..namespaces import msgTag, domTag, QN_TYPE, nsmap, DocNamespaceHelper
from ..namespaces import Prefix_Namespace as Prefix
from ..pysoap.soapenvelope import Soap12Envelope, WsAddress, GenericNode, ExtendedDocumentInvalid
from ..safety import SafetyInfoHeader

class HostedServiceClient(object):
    """ Base class of clients that call hosted services of a dpws device."""
    VALIDATE_MEX = False # workaraound as long as validation error due to missing dpws schema is not solved
    subscribeable_actions = tuple()
    #def __init__(self, soapClient, dpws_hosted, porttype, validate, sdc_definitions, bicepsParser, log_prefix=''):
    def __init__(self, soapClient, dpws_hosted, porttype, sdc_definitions, log_prefix=''):
        '''
        @param simple_xml_hosted_node: a "Hosted" node in a simplexml document
        '''
        self.endpoint_reference = dpws_hosted.endpointReferences[0]
        self._url = urllib.parse.urlparse(self.endpoint_reference.address)
        self.porttype = porttype
        self._logger = loghelper.getLoggerAdapter('sdc.client.{}'.format(porttype), log_prefix)
        self._operationsManager = None
        #self._validate = validate
        self._sdc_definitions = sdc_definitions
        #self._bicepsParser = bicepsParser
        self.soapClient = soapClient
        self.log_prefix = log_prefix
        self._mdib_wref = None
        self.predefined_actions = {} # calculated actions for subscriptions

        for s in self.subscribeable_actions:
            self.predefined_actions[s] = self._getActionString(s)

    def register_mdib(self, mdib):
        ''' Client sometimes must know the mdib data (e.g. Set service, activate method).'''
        if mdib is not None and self._mdib_wref is not None:
            raise RuntimeError('Client "{}" has already an registered mdib'.format(self.porttype))
        self._mdib_wref = None if mdib is None else weakref.ref(mdib)


    def setOperationsManager(self, operationsManager):
        self._operationsManager = operationsManager


    def _callOperation(self, soapEnvelope, request_manipulator=None):
        return self._operationsManager.callOperation(self, soapEnvelope, request_manipulator)


    def getSubscribableActions(self):
        """ action strings only predefined"""
        return self.predefined_actions.values()

    def _getActionString(self, methodName):
        actions_lookup = self._sdc_definitions.Actions
        try:
            return getattr(actions_lookup, methodName)
        except AttributeError: # fallback, if a definition is missing
            return '{}/{}/{}'.format(self._sdc_definitions.ActionsNamespace, self.porttype, methodName)

    def __repr__(self):
        return '{} "{}" endpoint = {}'.format(self.__class__.__name__, self.porttype, self.endpoint_reference)


    def postSoapEnvelope(self, soapEnvelope, msg, request_manipulator=None):
        return self.soapClient.postSoapEnvelopeTo(self._url.path, soapEnvelope, msg=msg, request_manipulator=request_manipulator)

    def _mkSetMethodSoapEnvelope(self, methodName, operationHandle, requestNodes, additionalNamespaces=None):
        ''' helper to create the soap envelope
        @param methodName: last element of name of the called action
        @param operationHandle: handle name as string
        @param requestNodes: a list of etree_ nodes that will become Subelement of Method name element
        '''
        soapBodyNode = etree_.Element( msgTag(methodName))
        ref = etree_.SubElement(soapBodyNode, msgTag('OperationHandleRef'), attrib={QN_TYPE: '{}:HandleRef'.format(Prefix.PM.prefix)}, nsmap=Prefix.partialMap(Prefix.PM))
        ref.text = operationHandle
        for n in requestNodes:
            soapBodyNode.append(n)
        if additionalNamespaces:
            my_ns = Prefix.partialMap(Prefix.S12, Prefix.WSA, Prefix.PM, Prefix.MSG, *additionalNamespaces)
        else:
            my_ns = Prefix.partialMap(Prefix.S12, Prefix.WSA, Prefix.PM, Prefix.MSG)

        sih = self._mkOptionalSafetyHeader(soapBodyNode, operationHandle) # a header or None

        soapEnvelope = Soap12Envelope(my_ns)
        action = self._getActionString(methodName)
        soapEnvelope.setAddress(WsAddress(action=action, to=self.endpoint_reference.address))
        if sih is not None:
            soapEnvelope.addHeaderObject(sih)

        soapEnvelope.addBodyElement(soapBodyNode)
        return soapEnvelope


    def _mkGetMethodEnvelope(self, method, params = None):
        action = self._getActionString(method)
        bodyNode = etree_.Element(msgTag(method))
        soapEnvelope = Soap12Envelope(Prefix.partialMap(Prefix.S12, Prefix.WSA, Prefix.MSG))
        soapEnvelope.setAddress(WsAddress(action=action,
                                          to=self.endpoint_reference.address))
        if params:
            for p in params:
                bodyNode.append(p)
        soapEnvelope.addBodyObject(GenericNode(bodyNode))

        return soapEnvelope

    def _callGetMethod(self, method, params = None, request_manipulator=None):
        self._logger.info('calling {} on {}:{}', method, self._url.netloc, self._url.path)
        soapEnvelope = self._mkGetMethodEnvelope(method, params)
        returnedEnvelope = self.postSoapEnvelope(soapEnvelope, msg='get {}'.format(method),
                                                 request_manipulator=request_manipulator)
        return returnedEnvelope

    def _mkSoapEnvelope(self, methodName, xmlBodyString=None, additionalHeaders=None):
        action = self._getActionString(methodName)
        soapEnvelope = Soap12Envelope(Prefix.partialMap(Prefix.S12, Prefix.MSG, Prefix.WSA))
        soapEnvelope.setAddress(WsAddress(action=action, to=self.endpoint_reference.address))
        if additionalHeaders is not None:
            for h in additionalHeaders:
                soapEnvelope.addHeaderObject(h)
        if xmlBodyString is not None:
            soapEnvelope.addBodyString(xmlBodyString)
        return soapEnvelope


    def _mkSoapEnvelopeWithEtreeBody(self, methodName, etreeBody=None, additionalHeaders=None):
        tmp = etree_.tostring(etreeBody)
        return self._mkSoapEnvelope(methodName, tmp, additionalHeaders)


    def _callMethodWithXMLStringArgument(self, portTypeName, methodName, xmlStringArgument=None, additionalHeaders=None):
        soapEnvelope = self._mkSoapEnvelope(methodName, xmlStringArgument, additionalHeaders)
        retEnvelope = self.postSoapEnvelope(soapEnvelope, msg='port {} method {}'.format(portTypeName, methodName))
        return retEnvelope


    def _callMethodWithEtreeNodeArgument(self, portTypeName, methodName, etreeNodeArgument=None, additionalHeaders=None):
        tmp = etree_.tostring(etreeNodeArgument)
        return self._callMethodWithXMLStringArgument(portTypeName, methodName, tmp, additionalHeaders)


    def _mkOptionalSafetyHeader(self, soapBodyNode, operationHandle):

        if self._mdib_wref is not None:
            op_descriptor = self._mdib_wref().descriptions.handle.getOne(operationHandle, allowNone=True)
            if op_descriptor is not None and op_descriptor.SafetyReq is not None:
                mdib_node, mdib_version_group = self._mdib_wref().reconstructMdibWithContextStates()
                return self._mkSoapSafetyHeader(soapBodyNode, op_descriptor.SafetyReq, mdib_node)
        return None


    def _mkSoapSafetyHeader(self, soapBodyNode, t_SafetyReq, mdibNode):
        dualChannelSelectors = {}
        safetyContextSelectors = {}

        if not t_SafetyReq.DualChannelDef:
            self._logger.info('no DualChannel selectors specified')
        else:
            for sel in  t_SafetyReq.DualChannelDef.Selector:
                selectorId = sel.Id
                selectorPath = sel.text
                values = soapBodyNode.xpath(selectorPath, namespaces=mdibNode.nsmap)
                if len(values) == 1:
                    self._logger.debug('DualChannel selector "{}": value = "{}", path= "{}"', selectorId, values[0], selectorPath)
                    dualChannelSelectors[selectorId] = str(values[0]).strip()
                elif len(values) == 0:
                    self._logger.error('DualChannel selector "{}": no value found! path= "{}"', selectorId, selectorPath)
                else:
                    self._logger.error('DualChannel selector "{}": path= "{}", multiple values found: {}', selectorId, selectorPath, values)

        if not t_SafetyReq.SafetyContextDef:
            self._logger.info('no Safety selectors specified')
        else:
            for sel in  t_SafetyReq.SafetyContextDef.Selector:
                selectorId = sel.Id
                selectorPath = sel.text
                # check the selector, there is a potential problem with the starting point of the xpath search path:
                if selectorPath.startswith('//'):
                    # double slashes means that the matching pattern can be located anywhere in the dom tree.
                    # No problem.
                    pass #
                elif selectorPath.startswith('/'):
                    # Problem! if the selector starts with a single slash, this is a xpath search that starts at the document root.
                    # But the convention is that the xpath search shall start from the top level element (=> without the toplevel element in the path)
                    # In order to follow this convention, remove the leading slash and start the search relative to the lop level node.
                    selectorPath = selectorPath[1:]
                values =  mdibNode.xpath(selectorPath, namespaces=mdibNode.nsmap)
                if len(values) == 1:
                    self._logger.debug('Safety selector "{}": value = "{}"  path= "{}"', selectorId, values[0], selectorPath)
                    safetyContextSelectors[selectorId] = str(values[0]).strip()
                elif len(values) == 0:
                    self._logger.error('Safety selector "{}":  no value found! path= "{}"', selectorId, selectorPath)
                else:
                    self._logger.error('Safety selector "{}": path= "{}", multiple values found: {}', selectorId, selectorPath, values)

        if dualChannelSelectors or safetyContextSelectors:
            return SafetyInfoHeader(dualChannelSelectors, safetyContextSelectors)
        else:
            return None


class GetServiceClient(HostedServiceClient):

    def getMdDescriptionNode(self, requestedHandles=None, request_manipulator=None):
        """
        @param requestedHandles: None if all descriptors shall be requested, otherwise a list of handles
        """
        requestparams = []
        if requestedHandles is not None:
            for h in requestedHandles:
                node = etree_.Element(msgTag('HandleRef'))
                node.text = h
                requestparams.append(node)
        resultSoapEnvelope = self._callGetMethod('GetMdDescription', params=requestparams,
                                                 request_manipulator=request_manipulator)
        return resultSoapEnvelope.msgNode

    def getMdib(self, request_manipulator=None):
        resultSoapEnvelope = self._callGetMethod('GetMdib', request_manipulator=request_manipulator)
        return resultSoapEnvelope

    def getMdibNode(self, request_manipulator=None):
        resultSoapEnvelope = self._callGetMethod('GetMdib', request_manipulator=request_manipulator)
        return resultSoapEnvelope.msgNode

    def getMdState(self, requestedHandles=None, request_manipulator=None):
        """
        @param requestedHandles: None if all states shall be requested, otherwise a list of handles
        """
        requestparams = []
        if requestedHandles is not None:
            for h in requestedHandles:
                node = etree_.Element(msgTag('HandleRef'))
                node.text = h
            requestparams.append(node)

        resultSoapEnvelope = self._callGetMethod('GetMdState', params=requestparams,
                                                 request_manipulator=request_manipulator)
        return resultSoapEnvelope

    def getMdStateNode(self, requestedHandles=None, request_manipulator=None):
        """
        @param requestedHandles: None if all states shall be requested, otherwise a list of handles
        """
        return self.getMdState(requestedHandles, request_manipulator=request_manipulator).msgNode


class SetServiceClient(HostedServiceClient):
    subscribeable_actions = ('OperationInvokedReport',)

    def setNumericValue(self, operationHandle, requestedNumericValue, request_manipulator=None):
        """ call SetNumericValue Method of device
        @param operationHandle: a string
        @param requestedNumericValue: int or float or a string representing a decimal number
        @return a Future object
        """
        self._logger.info('setNumericValue operationHandle={} requestedNumericValue={}',
                          operationHandle, requestedNumericValue)
        soapEnvelope = self._mkRequestedNumericValueEnvelope(operationHandle, requestedNumericValue)
        return self._callOperation(soapEnvelope, request_manipulator=request_manipulator)

    def setString(self, operationHandle, requestedString, request_manipulator=None):
        """ call SetString Method of device
        @param operationHandle: a string
        @param requestedString: a string
        @return a Future object
        """
        self._logger.info('setString operationHandle={} requestedString={}',
                          operationHandle, requestedString)
        soapEnvelope = self._mkRequestedStringEnvelope(operationHandle, requestedString)
        return self._callOperation(soapEnvelope, request_manipulator=request_manipulator)

    def setAlertState(self, operationHandle, proposedAlertState, request_manipulator=None):
        """The SetAlertState method corresponds to the SetAlertStateOperation objects in the MDIB and allows the modification of an alert.
        It can handle a single proposed AlertState as argument (only for backwards compatibility) and a list of them.
        @param operationHandle: handle name as string
        @param proposedAlertState: domainmodel.AbstractAlertState instance or a list of them
        """
        self._logger.info('setAlertState operationHandle={} requestedAlertState={}',
                          operationHandle, proposedAlertState)
        if hasattr(proposedAlertState, 'NODETYPE'):
            # this is a state container. make it a list
            proposedAlertState = [proposedAlertState]
        soapEnvelope = self._mkSetAlertEnvelope(operationHandle, proposedAlertState)
        return self._callOperation(soapEnvelope, request_manipulator=request_manipulator)

    def setMetricState(self, operationHandle, proposedMetricStates, request_manipulator=None):
        """The SetMetricState method corresponds to the SetMetricStateOperation objects in the MDIB and allows the modification of metric states.
        @param operationHandle: handle name as string
        @param proposedMetricStates: a list of domainmodel.AbstractMetricState instance or derived class
        """
        self._logger.info('setMetricState operationHandle={} requestedMetricState={}',
                          operationHandle, proposedMetricStates)
        soapEnvelope = self._mkSetMetricStateEnvelope(operationHandle, proposedMetricStates)
        return self._callOperation(soapEnvelope, request_manipulator=request_manipulator)

    def activate(self, operationHandle, value, request_manipulator=None):
        """ an activate call does not return the result of the operation directly. Instead you get an transaction id,
        and will receive the status of this transaction as notification ("OperationInvokedReport").
        This method returns a "future" object. The future object has a result as soon as a final transaction state is received.
        @param operationHandle: a string
        @param value: a string
        @return: a concurrent.futures.Future object
        """
        # make message body
        self._logger.info('activate handle={} value={}', operationHandle, value)
        soapBodyNode = etree_.Element(msgTag('Activate'), attrib=None, nsmap=nsmap)
        ref = etree_.SubElement(soapBodyNode, msgTag('OperationHandleRef'))
        ref.text = operationHandle
        argNode = None
        if value is not None:
            argNode = etree_.SubElement(soapBodyNode, msgTag('Argument'))
            argVal = etree_.SubElement(argNode, msgTag('ArgValue'))
            argVal.text = value

        # look for safety context in mdib
        sih = self._mkOptionalSafetyHeader(soapBodyNode, operationHandle)
        if sih is not None:
            sih = [sih]

        soapEnvelope = self._mkSoapEnvelopeWithEtreeBody('Activate', soapBodyNode, additionalHeaders=sih)
        futureObject = self._callOperation(soapEnvelope, request_manipulator=request_manipulator)
        return futureObject

    def setComponentState(self, operationHandle, proposedComponentStates, request_manipulator=None):
        """
        The setComponentState method corresponds to the SetComponentStateOperation objects in the MDIB and allows to insert or modify context states.
        @param operationHandle: handle name as string
        @param proposedComponentStates: a list of domainmodel.AbstractDeviceComponentState instances or derived class
        :return: a concurrent.futures.Future
        """
        tmp = ', '.join(['{}(descriptorHandle={})'.format(st.__class__.__name__, st.descriptorHandle)
                         for st in proposedComponentStates])
        self._logger.info('setComponentState {}', tmp)
        soapEnvelope = self._mkSetComponentStateEnvelope(operationHandle, proposedComponentStates)
        self._logger.debug('setComponentState sends {}', lambda: soapEnvelope.as_xml(pretty=True))
        futureObject = self._callOperation(soapEnvelope, request_manipulator=request_manipulator)
        return futureObject

    def _mkRequestedNumericValueEnvelope(self, operationHandle, requestedNumericValue):
        """create soap envelope, but do not send it. Used for unit testing"""
        requestedValueNode = etree_.Element(msgTag('RequestedNumericValue'),
                                            attrib={QN_TYPE: '{}:decimal'.format(Prefix.XSD.prefix)})
        requestedValueNode.text = str(requestedNumericValue)
        return self._mkSetMethodSoapEnvelope('SetValue', operationHandle, [requestedValueNode],
                                             additionalNamespaces=[Prefix.XSD])

    def _mkRequestedStringEnvelope(self, operationHandle, requestedString):
        """create soap envelope, but do not send it. Used for unit testing"""
        requestedStringNode = etree_.Element(msgTag('RequestedStringValue'),
                                             attrib={QN_TYPE: '{}:string'.format(Prefix.XSD.prefix)})
        requestedStringNode.text = requestedString
        return self._mkSetMethodSoapEnvelope('SetString', operationHandle, [requestedStringNode],
                                             additionalNamespaces=[Prefix.XSD])

    def _mkSetAlertEnvelope(self, operationHandle, proposedAlertStates):
        """create soap envelope, but do not send it. Used for unit testing
        :param proposedAlertStates: a list AbstractAlertStateContainer or derived class """
        _proposedAlertStates = [p.mkCopy() for p in proposedAlertStates]
        for p in _proposedAlertStates:
            p.nsmapper = DocNamespaceHelper()  # use my namespaces
        _proposedAlertStateNodes = [p.mkStateNode(msgTag('ProposedAlertState')) for p in _proposedAlertStates]

        return self._mkSetMethodSoapEnvelope('SetAlertState', operationHandle, _proposedAlertStateNodes)

    def _mkSetMetricStateEnvelope(self, operationHandle, proposedMetricStates):
        """create soap envelope, but do not send it. Used for unit testing
        :param proposedMetricState: a list of AbstractMetricStateContainer or derived classes """
        _proposedMetricStates = [p.mkCopy() for p in proposedMetricStates]
        nsmapper = DocNamespaceHelper()
        for p in _proposedMetricStates:
            p.nsmapper = nsmapper  # use my namespaces
        _proposedMetricStateNodes = [p.mkStateNode(msgTag('ProposedMetricState')) for p in _proposedMetricStates]

        return self._mkSetMethodSoapEnvelope('SetMetricState', operationHandle, _proposedMetricStateNodes)

    def _mkSetComponentStateEnvelope(self, operationHandle, proposedComponentStates):
        """Create soap envelope, but do not send it. Used for unit testing
        :param proposedComponentStates: a list of AbstractComponentStateContainers or derived classes """
        _proposedComponentStates = [p.mkCopy() for p in proposedComponentStates]
        nsmapper = DocNamespaceHelper()
        for p in _proposedComponentStates:
            p.nsmapper = nsmapper  # use my namespaces
        _proposedComponentStateNodes = [p.mkStateNode(msgTag('ProposedComponentState')) for p in
                                        _proposedComponentStates]

        return self._mkSetMethodSoapEnvelope('SetComponentState', operationHandle, _proposedComponentStateNodes)


class CTreeServiceClient(HostedServiceClient):

    def getDescriptorNode(self, handles, request_manipulator=None):
        """

        :param handles: a list of strings
        :return: a list of etree nodes
        """
        handle_nodes = []
        for h in handles:
            node = etree_.Element(msgTag('HandleRef'))
            node.text = h
            handle_nodes.append(node)
        resultSoapEnvelope = self._callGetMethod('GetDescriptor', params=handle_nodes,
                                                 request_manipulator=request_manipulator)
        return resultSoapEnvelope.msgNode

    def getContainmentTreeNodes(self, handles, request_manipulator=None):
        """

        :param handles: a list of strings
        :return: a list of etree nodes
        """
        handle_nodes = []
        for h in handles:
            node = etree_.Element(msgTag('HandleRef'))
            node.text = h
            handle_nodes.append(node)
        resultSoapEnvelope = self._callGetMethod('GetContainmentTree', params=handle_nodes,
                                                 request_manipulator=request_manipulator)
        return resultSoapEnvelope.msgNode


class StateEventClient(HostedServiceClient):
    subscribeable_actions = ('EpisodicMetricReport',
                             'EpisodicAlertReport',
                             'EpisodicComponentReport',
                             'EpisodicOperationalStateReport',
                             'PeriodicMetricReport',
                             'PeriodicAlertReport',
                             'PeriodicComponentReport',
                             'PeriodicOperationalStateReport'
                             )


class DescriptionEventClient(HostedServiceClient):
    subscribeable_actions = ('DescriptionModificationReport',)


class ContextServiceClient(HostedServiceClient):
    subscribeable_actions = ('EpisodicContextReport', 'PeriodicContextReport')

    def mkProposedContextObject(self, descriptorHandle, handle=None):
        """
        Helper method that create a state that can be used in setContextState operation
        :param descriptorHandle: the descriptor for which a state shall be created or updated
        :param handle: if None, a new object with default values is created (INSERT operation).
                       Otherwise a copy of an existing state with this handle is returned.
        :return: a context state instance
        """
        mdib = self._mdib_wref()
        if mdib is None:
            raise RuntimeError('no mdib information')
        contextDescriptorContainer = mdib.descriptions.handle.getOne(descriptorHandle)
        if handle is None:
            cls = self._sdc_definitions.sc.getContainerClass(contextDescriptorContainer.STATE_QNAME)
            obj = cls(nsmapper=DocNamespaceHelper(), descriptorContainer=contextDescriptorContainer)
            obj.Handle = descriptorHandle # this indicates that this is a new context state
        else:
            _obj = mdib.contextStates.handle.getOne(handle)
            obj = _obj.mkCopy()
        return obj

    def setContextState(self, operationHandle, proposedContextStates, request_manipulator=None):
        """
        """
        tmp = ', '.join(['{}(descriptorHandle={}, handle={})'.format(st.__class__.__name__,
                                                                     st.descriptorHandle,
                                                                     st.Handle)
                         for st in proposedContextStates])
        self._logger.info('setContextState {}', tmp)
        soapEnvelope = self._mkSetContextStateEnvelope(operationHandle, proposedContextStates)
        futureObject = self._callOperation(soapEnvelope, request_manipulator=request_manipulator)
        return futureObject

    def _mkSetContextStateEnvelope(self, operationHandle, proposedContextStates):
        """create soap envelope, but do not send it. Used for unit testing
        :param proposedContextStates: a list AbstractContextState or derived class """
        _proposedContextStates = [p.mkCopy() for p in proposedContextStates]
        for p in _proposedContextStates:
            # BICEPS: if handle == descriptorHandle, it means insert.
            if p.Handle is None:
                p.Handle = p.DescriptorHandle
            p.nsmapper = DocNamespaceHelper()  # use my namespaces
        _proposedContextStateNodes = [p.mkStateNode(msgTag('ProposedContextState')) for p in _proposedContextStates]

        return self._mkSetMethodSoapEnvelope('SetContextState', operationHandle, _proposedContextStateNodes)


    def getContextStatesNode(self, handles=None, request_manipulator=None):
        """
        @param handles: a list of handles
        """
        params = []
        if handles:
            for h in handles:
                params.append(etree_.Element(msgTag('HandleRef'), attrib={QN_TYPE: '{}:HandleRef'.format(Prefix.MSG.prefix)},
                                             nsmap=Prefix.partialMap(Prefix.MSG, Prefix.PM)))
                params[-1].text = h
        resultSoapEnvelope = self._callGetMethod('GetContextStates', params, request_manipulator=request_manipulator)
        return resultSoapEnvelope.msgNode

    def getContextStateByIdentification(self, identifications, contextType=None, request_manipulator=None):
        """
        :param identifications: list of identifiers (type: InstanceIdentifier from pmtypes)
        :param contextType: Type to query
        :return:
        """
        params = []
        for oneId in identifications:
            params.append(oneId.asEtreeNode(qname=namespaces.msgTag('Identification'), nsmap=namespaces.nsmap))
        # todo: set attribute type based on contextType if set
        resultSoapEnvelope = self._callGetMethod('GetContextStatesByIdentification', params, request_manipulator=request_manipulator)
        return resultSoapEnvelope.msgNode

class WaveformClient(HostedServiceClient):
    subscribeable_actions = ('Waveform',)

