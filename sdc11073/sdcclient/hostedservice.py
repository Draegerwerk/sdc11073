import weakref
import urllib

from .. import loghelper
from ..namespaces import DocNamespaceHelper
from ..pysoap.soapenvelope import ExtendedDocumentInvalid

class HostedServiceClient:
    """ Base class of clients that call hosted services of a dpws device."""
    VALIDATE_MEX = False # workaraound as long as validation error due to missing dpws schema is not solved
    subscribeable_actions = tuple()
    def __init__(self, soapClient, msg_factory, dpws_hosted, porttype, validate, sdc_definitions, bicepsParser, log_prefix=''):
        '''
        @param simple_xml_hosted_node: a "Hosted" node in a simplexml document
        '''
        self.endpoint_reference = dpws_hosted.endpointReferences[0]
        self._url = urllib.parse.urlparse(self.endpoint_reference.address)
        self.porttype = porttype
        self._logger = loghelper.get_logger_adapter('sdc.client.{}'.format(porttype), log_prefix)
        self._operationsManager = None
        self._validate = validate
        self._sdc_definitions = sdc_definitions
        self._bicepsParser = bicepsParser
        self.soapClient = soapClient
        self.log_prefix = log_prefix
        self._mdib_wref = None
        self._msg_factory = msg_factory
        self.predefined_actions = {} # calculated actions for subscriptions
        for s in self.subscribeable_actions:
            self.predefined_actions[s] = self._msg_factory.get_action_string(porttype, s)

    @property
    def _bmmSchema(self):
        return None if not self._validate else self._bicepsParser.message_schema

    @property
    def _mexSchema(self):
        return None if not self._validate else self._bicepsParser.mex_schema

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

    def __repr__(self):
        return '{} "{}" endpoint = {}'.format(self.__class__.__name__, self.porttype, self.endpoint_reference)


    def postSoapEnvelope(self, soapEnvelope, msg, request_manipulator=None):
        return self.soapClient.postSoapEnvelopeTo(self._url.path, soapEnvelope, msg=msg,
                                                  request_manipulator=request_manipulator)

    def _callGetMethod(self, envelope, method, request_manipulator=None):
        self._logger.info('calling {} on {}:{}', method, self._url.netloc, self._url.path)
        envelope.validateBody(self._bmmSchema)
        result_envelope = self.postSoapEnvelope(envelope, msg='get {}'.format(method),
                                                request_manipulator=request_manipulator)
        try:
            result_envelope.validateBody(self._bmmSchema)
        except ExtendedDocumentInvalid as ex:
            self._logger.error('Validation error: {}', ex)
        except TypeError as ex:
            self._logger.error('Could not validate Body, Type Error :{}', ex)
        except Exception as ex:
            self._logger.error('Validation error: "{}" msgNode={}', ex, result_envelope.msgNode)
        return result_envelope


class GetServiceClient(HostedServiceClient):

    def getMdDescriptionNode(self, requestedHandles=None, request_manipulator=None):
        """
        @param requestedHandles: None if all descriptors shall be requested, otherwise a list of handles
        """
        envelope = self._msg_factory.mk_getmddescription_envelope(
            self.endpoint_reference.address, self.porttype, requestedHandles)

        resultSoapEnvelope = self._callGetMethod(envelope, 'GetMdDescription', request_manipulator=request_manipulator)
        return resultSoapEnvelope.msgNode

    def getMdib(self, request_manipulator=None):
        envelope = self._msg_factory.mk_getmdib_envelope(self.endpoint_reference.address, self.porttype)

        result_envelope = self._callGetMethod(envelope, 'GetMdib', request_manipulator=request_manipulator)
        return result_envelope

    def getMdibNode(self, request_manipulator=None):
        return self.getMdib(request_manipulator).msgNode

    def getMdState(self, requestedHandles=None, request_manipulator=None):
        """
        @param requestedHandles: None if all states shall be requested, otherwise a list of handles
        """
        envelope = self._msg_factory.mk_getmdstate_envelope(self.endpoint_reference.address,
                                                           self.porttype, requestedHandles)
        result_envelope = self._callGetMethod(envelope, 'GetMdState',
                                                 request_manipulator=request_manipulator)
        return result_envelope

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
        envelope = self._msg_factory.mk_activate_envelope(self.endpoint_reference.address,
                                                         self.porttype,
                                                         operationHandle,
                                                         value)
        envelope.validateBody(self._bmmSchema)
        return self._callOperation(envelope, request_manipulator=request_manipulator)

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
        soapEnvelope = self._msg_factory.mk_setcomponentstate_envelope(self.endpoint_reference.address, self.porttype,
                                                                      operationHandle, proposedComponentStates)
        self._logger.debug('setComponentState sends {}', lambda: soapEnvelope.as_xml(pretty=True))
        return self._callOperation(soapEnvelope, request_manipulator=request_manipulator)

    def _mkRequestedNumericValueEnvelope(self, operationHandle, requestedNumericValue):
        """create soap envelope, but do not send it. Used for unit testing"""
        return self._msg_factory.mk_requestednumericvalue_envelope(self.endpoint_reference.address, self.porttype, operationHandle, requestedNumericValue)

    def _mkRequestedStringEnvelope(self, operationHandle, requestedString):
        """create soap envelope, but do not send it. Used for unit testing"""
        return self._msg_factory.mk_requestedstring_envelope(self.endpoint_reference.address, self.porttype, operationHandle, requestedString)

    def _mkSetAlertEnvelope(self, operationHandle, proposedAlertStates):
        return self._msg_factory.mk_setalert_envelope(self.endpoint_reference.address, self.porttype, operationHandle, proposedAlertStates)

    def _mkSetMetricStateEnvelope(self, operationHandle, proposedMetricStates):
        """create soap envelope, but do not send it. Used for unit testing
        :param proposedMetricState: a list of AbstractMetricStateContainer or derived classes """
        return self._msg_factory.mk_setmetricstate_envelope(self.endpoint_reference.address, self.porttype, operationHandle, proposedMetricStates)


class CTreeServiceClient(HostedServiceClient):

    def getDescriptorNode(self, handles, request_manipulator=None):
        """

        :param handles: a list of strings
        :return: a list of etree nodes
        """
        envelope = self._msg_factory.mk_getdescriptor_envelope(
            self.endpoint_reference.address, self.porttype, handles)
        resultSoapEnvelope = self._callGetMethod(
            envelope, 'GetMdState', request_manipulator=request_manipulator)
        return resultSoapEnvelope.msgNode

    def getContainmentTreeNodes(self, handles, request_manipulator=None):
        """

        :param handles: a list of strings
        :return: a list of etree nodes
        """
        envelope = self._msg_factory.mk_getcontainmenttree_envelope(
            self.endpoint_reference.address, self.porttype, handles)
        resultSoapEnvelope = self._callGetMethod(
            envelope, 'GetContainmentTree', request_manipulator=request_manipulator)
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
            cls = self._sdc_definitions.sc.get_container_class(contextDescriptorContainer.STATE_QNAME)
            obj = cls(nsmapper=DocNamespaceHelper(), descriptor_container=contextDescriptorContainer)
            obj.Handle = descriptorHandle # this indicates that this is a new context state
        else:
            _obj = mdib.context_states.handle.getOne(handle)
            obj = _obj.mk_copy()
        return obj

    def setContextState(self, operationHandle, proposedContextStates, request_manipulator=None):
        """
        @return: a concurrent.futures.Future object
        """
        tmp = ', '.join(['{}(descriptorHandle={}, handle={})'.format(st.__class__.__name__,
                                                                     st.descriptorHandle,
                                                                     st.Handle)
                         for st in proposedContextStates])
        self._logger.info('setContextState {}', tmp)
        soapEnvelope = self._msg_factory.mk_setcontextstate_envelope(
            self.endpoint_reference.address, self.porttype, operationHandle, proposedContextStates)
        return self._callOperation(soapEnvelope, request_manipulator=request_manipulator)

    def getContextStatesNode(self, handles=None, request_manipulator=None):
        """
        @param handles: a list of handles
        """
        envelope = self._msg_factory.mk_getcontextstates_envelope(
            self.endpoint_reference.address, self.porttype, handles)
        resultSoapEnvelope = self._callGetMethod(
            envelope, 'GetContextStates', request_manipulator=request_manipulator)
        resultSoapEnvelope.validateBody(self._bmmSchema)
        return resultSoapEnvelope.msgNode

    def getContextStateByIdentification(self, identifications, contextType=None, request_manipulator=None):
        """
        :param identifications: list of identifiers (type: InstanceIdentifier from pmtypes)
        :param contextType: Type to query
        :return:
        """
        envelope = self._msg_factory.mk_getcontextstates_by_identification_envelope(
            self.endpoint_reference.address, self.porttype, identifications)
        resultSoapEnvelope = self._callGetMethod(
            envelope, 'GetContextStatesByIdentification', request_manipulator=request_manipulator)
        resultSoapEnvelope.validateBody(self._bmmSchema)
        return resultSoapEnvelope.msgNode

class WaveformClient(HostedServiceClient):
    subscribeable_actions = ('Waveform',)

