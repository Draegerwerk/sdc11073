"""
This Module contains code handles Service Controller operations (sco).
All remote control commands of a client are executed by sco's

These operations share a common behavior:
A remote control command is executed async. The respone to such soap request contains a state (typically 'wait') and a transaction id.
The progress of the transaction is reported with an OperationInvokedReport.
A client must subscribe to the OperationInvokeReport Event of the 'Set' service, otherwise it would not get informed about progress.
"""
import threading
from collections import namedtuple
import traceback
import time
import queue
from .. import observableproperties as properties
from ..namespaces import domTag
from ..pmtypes import InvocationState, SafetyClassification
from .. import loghelper
from .. import mdib as mdib_


class _OperationsWorker(threading.Thread):
    """ Thread that enqueues and processes all operations.
    It manages transaction ids for all operations.
    Progress notifications are sent via subscriptionmanager."""

    def __init__(self, subscriptionsmgr, mdib, log_prefix):
        """
        @param subscriptionsmgr: subscriptionsmgr.notifyOperation is called in order to notify all subscribers of OperationInvokeReport Events
        """
        super().__init__(name='DeviceOperationsWorker')
        self.daemon = True
        self._subscriptionsmgr = subscriptionsmgr
        self._mdib = mdib
        self._operationsQ = queue.Queue(10)  # spooled operations
        self._transactionId = 1
        self._transactionIdLock = threading.Lock()
        self._logger = loghelper.getLoggerAdapter('sdc.device.op_worker', log_prefix)

    def enqueueOperation(self, operation, request, argument):
        """ enqueues operation "operation".
        @param operation: a callable with signature operation(request, mdib)
        @param request: the soapEnvelope of the request
        @param argument: parsed argument for the operation handler
        @return: a transaction Id
        """
        with self._transactionIdLock:
            transactionId = self._transactionId
            self._transactionId += 1
        self._operationsQ.put((transactionId, operation, request, argument), timeout=1)
        return transactionId

    def run(self):
        while True:
            try:
                op = self._operationsQ.get()
                if op == 'stop_sco':
                    self._logger.info('stop request found. Terminating now.')
                    return
                tr_id, operation, request, argument = op
                time.sleep(0.001)
                self._logger.info('{}: starting operation "{}"', operation.__class__.__name__, operation.handle)
                # duplicate the WAIT response to the operation request as notification. Standard requires this.
                self._subscriptionsmgr.notifyOperation(self._mdib.sequenceId, self._mdib.mdibVersion, tr_id, operation,
                                                       InvocationState.WAIT)
                time.sleep(0.001)  # not really necessary, but in real world there might also be some delay.
                self._subscriptionsmgr.notifyOperation(self._mdib.sequenceId, self._mdib.mdibVersion, tr_id, operation,
                                                       InvocationState.START)
                try:
                    operation.executeOperation(request, argument)
                    self._logger.info('{}: successfully finished operation "{}"', operation.__class__.__name__,
                                      operation.handle)
                    self._subscriptionsmgr.notifyOperation(self._mdib.sequenceId, self._mdib.mdibVersion, tr_id,
                                                           operation, InvocationState.FINISHED)
                except Exception as ex:
                    self._logger.info('{}: error executing operation "{}": {}', operation.__class__.__name__,
                                      operation.handle, traceback.format_exc())
                    self._subscriptionsmgr.notifyOperation(self._mdib.sequenceId, self._mdib.mdibVersion, tr_id,
                                                           operation, InvocationState.FAILED, error='Oth',
                                                           errorMessage=repr(ex))
            except Exception as ex:
                self._logger.error('{}: unexpected error while handling operation "{}": {}',
                                   operation.__class__.__name__, operation.handle, traceback.format_exc())

    def stop(self):
        self._operationsQ.put('stop_sco')  # a dummy request to stop the thread
        self.join(timeout=1)


class ScoOperationsRegistry(object):
    """ Registry for Sco operations.
    from BICEPS:
    A service control object to define remote control operations. Any pm:AbstractOperationDescriptor/@OperationTarget
    within this SCO SHALL only reference this or child descriptors within the CONTAINMENT TREE.
    NOTE - In modular systems, dynamically plugged-in modules would typically be modeled as VMDs.
    Such VMDs potentially have their own SCO. In every other case, SCO operations are modeled in pm:MdsDescriptor/pm:Sco.
    """

    def __init__(self, subscriptionsmgr, mdib, handle='_sco', log_prefix=None):
        self._worker = None
        self._subscriptionsmgr = subscriptionsmgr
        self._mdib = mdib
        self._log_prefix = log_prefix
        self._logger = loghelper.getLoggerAdapter('sdc.device.op_reg', log_prefix)
        self._registeredOperations = {}  # lookup by handle
        self._handle = handle

        # find the Sco of the Mds, this will be the default sco for new operations
        mdsDescriptorContainer = mdib.descriptions.NODETYPE.getOne(domTag('MdsDescriptor'))
        scos = mdib.descriptions.find(parentHandle=mdsDescriptorContainer.handle).find(
            NODETYPE=domTag('ScoDescriptor')).objects
        if len(scos) == 1:
            self._logger.info('found Sco node in mds, using it')
            self._mds_sco_descriptorContainer = scos[0]
        else:
            self._logger.info('not found Sco node in mds, creating it')
            # create sco and add to mdib
            self._mds_sco_descriptorContainer = mdib_.descriptorcontainers.ScoDescriptorContainer(
                mdib.nsmapper, self._handle, mdsDescriptorContainer.handle)
            mdib.descriptions.addObject(self._mds_sco_descriptorContainer)

    def registerOperation(self, operation, scoDescriptorContainer=None):
        self._logger.info('register operation "{}"', operation)
        if operation.handle in self._registeredOperations:
            self._logger.info('handle {} is already registered, will re-use it', operation.handle)
        parentContainer = scoDescriptorContainer or self._mds_sco_descriptorContainer
        operation.setMdib(self._mdib, parentContainer)
        self._registeredOperations[operation.handle] = operation

    def unRegisterOperationByHandle(self, operationHandle):
        del self._registeredOperations[operationHandle]

    def getOperationByHandle(self, operationHandle):
        return self._registeredOperations.get(operationHandle)

    def enqueueOperation(self, operation, request, argument):
        """ enqueues operation "operation".
        @param operation: a callable with signature operation(request, mdib)
        @param request: the soapEnvelope of the request
        @return: a transaction Id
        """
        return self._worker.enqueueOperation(operation, request, argument)

    def startWorker(self):
        if self._worker is not None:
            raise RuntimeError('SCO worker is already running')
        self._worker = _OperationsWorker(self._subscriptionsmgr, self._mdib, self._log_prefix)
        self._worker.start()

    def stopWorker(self):
        if self._worker is not None:
            self._worker.stop()
            self._worker = None


class OperationDefinition(object):
    """ This is the base class of all provided operations.
    An operation is a point for remote control over the network."""
    currentValue = properties.ObservableProperty(fireOnlyOnChangedValue=False)
    currentRequest = properties.ObservableProperty(fireOnlyOnChangedValue=False)
    currentArgument = properties.ObservableProperty(fireOnlyOnChangedValue=False)
    OP_DESCR_QNAME = None
    OP_STATE_QNAME = None

    def __init__(self, handle, operationTarget,
                 safetyClassification=SafetyClassification.INF,
                 codedValue=None,
                 log_prefix=None):  # pylint:disable=too-many-arguments
        """
        @param handle: the handle of the operation itself.
        @param operationTarget: the handle of the modified data (MdDescription)
        @param safetyClassification: one of pmtypes.SafetyClassification values
        @param codedValue: a pmtypes.CodedValue instance
        """
        self._logger = loghelper.getLoggerAdapter('sdc.device.op.{}'.format(self.__class__.__name__), log_prefix)
        self._mdib = None
        self._descriptorContainer = None
        self._operationStateContainer = None
        self._operationTargetContainer = None
        self._handle = handle
        self._operationTargetHandle = operationTarget
        # documentation of operationTarget:
        # A HANDLE reference this operation is targeted to. In case of a single state this is the HANDLE of the descriptor. 
        # In case that multiple states may belong to one descriptor (pm:AbstractMultiState), OperationTarget is the HANDLE 
        # of one of the state instances (if the state is modified by the operation).
        # self._operationDescriptorQName = operationDescriptorQName
        # self._operationStateQName = operationStateQName

        self._safetyClassification = safetyClassification
        self._codedValue = codedValue
        self.calls = []  # record when operation was called

    @property
    def handle(self):
        return self._handle

    @property
    def operationTarget(self):
        return self._operationTargetHandle

    @property
    def operationTargetStorage(self):
        return self._mdib.states

    def executeOperation(self, request, argument):  # pylint: disable=unused-argument
        """ This is the code that executes the operation itself.
        A handler that executes the operation must be bound to observable "currentRequest"."""
        self.calls.append((time.time(), request))
        self.currentRequest = request
        self.currentArgument = argument

    def setMdib(self, mdib, parentDescriptorContainer):
        """ The operation needs to know the mdib that it operates on.
        This is called by SubscriptionManager on registration.
        Needs to be implemented by derived classes if specific things have to be initialized."""
        if self._mdib is not None:
            raise RuntimeError('Mdib is already set')
        self._mdib = mdib
        self._logger.log_prefix = mdib.log_prefix  # use same prefix as mdib for logging
        self._descriptorContainer = self._mdib.descriptions.handle.getOne(self._handle, allowNone=True)
        if self._descriptorContainer is not None:
            # there is already a descriptor
            self._logger.info('descriptor for operation "{}" is already present, re-using it'.format(self._handle))
        else:
            operationDescriptorClass = mdib.getDescriptorContainerClass(self.OP_DESCR_QNAME)
            self._descriptorContainer = operationDescriptorClass(
                mdib.nsmapper, self._handle, parentDescriptorContainer.handle)
            self._initOperationDescriptorContainer()
            mdib.descriptions.addObject(self._descriptorContainer)

        self._operationStateContainer = self._mdib.states.descriptorHandle.getOne(self._handle, allowNone=True)
        if self._operationStateContainer is not None:
            self._logger.info('operation state for operation "{}" is already present, re-using it'.format(self._handle))
            self._operationStateContainer.updateNode()
        else:
            operationStateClass = mdib.getStateContainerClass(self.OP_STATE_QNAME)
            self._operationStateContainer = operationStateClass(mdib.nsmapper, self._descriptorContainer)
            self._operationStateContainer.updateNode()
            mdib.states.addObject(self._operationStateContainer)

        # now add the object that is target of operation
        self._initOperationTargetContainer()

    def _initOperationDescriptorContainer(self):
        self._descriptorContainer.OperationTarget = self._operationTargetHandle
        if self._codedValue is not None:
            self._descriptorContainer.Type = self._codedValue

    def _initOperationTargetContainer(self):
        """ Create the object that is manipulated by the operation"""
        operationTargetDescriptor = self._mdib.descriptions.handle.getOne(self._operationTargetHandle)
        self._operationTargetContainer = self._mdib.states.descriptorHandle.getOne(self._operationTargetHandle,
                                                                                   allowNone=True)  # pylint:disable=protected-access
        if self._operationTargetContainer is not None:
            self._logger.info('operation target state for operation "{}" is already present, re-using it'.format(
                self._operationTargetHandle))
        else:
            self._operationTargetContainer = self._mdib.mkStateContainerFromDescriptor(
                operationTargetDescriptor)  # pylint:disable=protected-access
            self._operationTargetContainer.updateNode()
            self._logger.info('creating {} DescriptorHandle = {}', self._operationTargetContainer.__class__.__name__,
                              self._operationTargetHandle)
            if self._operationTargetContainer is not None:
                self.operationTargetStorage.addObject(self._operationTargetContainer)

    def setOperatingMode(self, mode):
        """ Mode is one of En, Dis, NA"""
        with self._mdib.mdibUpdateTransaction() as tr:
            st = tr.getOperationalState(self._handle)
            st.OperatingMode = mode

    def collectValues(self, numberOfValues=None):
        """ Async way to retrieve next value(s):
        Returns a Future-like object that has a result() method.
        For details see properties.SingleValueCollector and propertiesValuesCollector documentation.
        """
        if numberOfValues is None:
            return properties.SingleValueCollector(self, 'currentValue')
        else:
            return properties.ValuesCollector(self, 'currentValue', numberOfValues)

    def __str__(self):
        return '{} handle={} operationTarget={}'.format(self.__class__.__name__, self._handle,
                                                        self._operationTargetHandle)


class SetStringOperation(OperationDefinition):
    OP_DESCR_QNAME = domTag('SetStringOperationDescriptor')
    OP_STATE_QNAME = domTag('SetStringOperationState')

    def __init__(self, handle, operationTarget, initialValue=None, codedValue=None):
        super().__init__(handle=handle,
                         operationTarget=operationTarget,
                         codedValue=codedValue)
        self.currentValue = initialValue

    @classmethod
    def fromOperationContainer(cls, operationContainer):
        return cls(handle=operationContainer.handle,
                   operationTarget=operationContainer.OperationTarget,
                   initialValue=None, codedValue=None)


class SetValueOperation(OperationDefinition):
    OP_DESCR_QNAME = domTag('SetValueOperationDescriptor')
    OP_STATE_QNAME = domTag('SetValueOperationState')

    def __init__(self, handle, operationTarget, initialValue=None, codedValue=None):
        super().__init__(handle=handle,
                         operationTarget=operationTarget,
                         codedValue=codedValue)
        self.currentValue = initialValue


class SetContextStateOperation(OperationDefinition):
    """Default implementation of SetContextOperation."""
    OP_DESCR_QNAME = domTag('SetContextStateOperationDescriptor')
    OP_STATE_QNAME = domTag('SetContextStateOperationState')

    def __init__(self, handle, operationTarget, codedValue=None):
        super().__init__(handle,
                         operationTarget,
                         codedValue=codedValue)

    @property
    def operationTargetStorage(self):
        return self._mdib.contextStates

    def _initOperationTargetContainer(self):
        """ initially no patient context is created."""
        pass

    @classmethod
    def fromOperationContainer(cls, operationContainer):
        return cls(handle=operationContainer.handle,
                   operationTarget=operationContainer.OperationTarget)


RecordedCall = namedtuple('RecordedCall', 'timestamp args')


class ActivateOperation(OperationDefinition):
    """ This default implementation only registers calls, no manipulation of operation target
    """
    OP_DESCR_QNAME = domTag('ActivateOperationDescriptor')
    OP_STATE_QNAME = domTag('ActivateOperationState')

    def __init__(self, handle, operationTarget, codedValue=None):
        super().__init__(handle=handle,
                         operationTarget=operationTarget,
                         codedValue=codedValue)


class SetAlertStateOperation(OperationDefinition):
    """ This default implementation only registers calls, no manipulation of operation target
    """
    OP_DESCR_QNAME = domTag('SetAlertStateOperationDescriptor')
    OP_STATE_QNAME = domTag('SetAlertStateOperationState')

    def __init__(self, handle, operationTarget, codedValue=None, log_prefix=None):
        super().__init__(handle=handle,
                         operationTarget=operationTarget,
                         codedValue=codedValue,
                         log_prefix=log_prefix)


class SetComponentStateOperation(OperationDefinition):
    """ This default implementation only registers calls, no manipulation of operation target
    """
    OP_DESCR_QNAME = domTag('SetComponentStateOperationDescriptor')
    OP_STATE_QNAME = domTag('SetComponentStateOperationState')

    def __init__(self, handle, operationTarget, codedValue=None, log_prefix=None):
        super().__init__(handle=handle,
                         operationTarget=operationTarget,
                         codedValue=codedValue,
                         log_prefix=log_prefix)


class SetMetricStateOperation(OperationDefinition):
    """ This default implementation only registers calls, no manipulation of operation target
    """
    OP_DESCR_QNAME = domTag('SetMetricStateOperationDescriptor')
    OP_STATE_QNAME = domTag('SetMetricStateOperationState')

    def __init__(self, handle, operationTarget, codedValue=None, log_prefix=None):
        super().__init__(handle=handle,
                         operationTarget=operationTarget,
                         codedValue=codedValue,
                         log_prefix=log_prefix)


def getOperationClass(qname):
    if qname == domTag('SetStringOperationDescriptor'):
        return SetStringOperation
    elif qname == domTag('SetValueOperationDescriptor'):
        return SetValueOperation
    elif qname == domTag('SetContextStateOperationDescriptor'):
        return SetContextStateOperation
    elif qname == domTag('ActivateOperationDescriptor'):
        return ActivateOperation
    elif qname == domTag('SetAlertStateOperationDescriptor'):
        return SetAlertStateOperation
    elif qname == domTag('SetMetricStateOperationDescriptor'):
        return SetMetricStateOperation
    elif qname == domTag('SetComponentStateOperationDescriptor'):
        return SetComponentStateOperation
    elif qname == domTag('SetComponentStateOperationDescriptor'):
        return SetComponentStateOperation
