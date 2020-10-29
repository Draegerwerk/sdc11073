from sdc11073.roles import operationprovider
from . import alarmprovider
from . import audiopauseprovider
from . import clockprovider
from . import metricprovider
from . import contextprovider
from . import patientcontextprovider
from . import providerbase
from .. import loghelper
from .. import namespaces
from .. import sdcdevice


class GenericSetComponentStateOperationProvider(providerbase.ProviderRole):
    '''
    Responsible for SetComponentState Operations
    '''
    def makeOperationInstance(self, operationDescriptorContainer):
        ''' Can handle following cases:
        SetComponentStateOperationDescriptor, target = any AbstractComponentDescriptor: => handler = _setComponentState
        '''
        operationTargetHandle = operationDescriptorContainer.OperationTarget
        operationTargetDescriptorContainer = self._mdib.descriptions.handle.getOne(operationTargetHandle)

        if operationDescriptorContainer.NODETYPE == namespaces.domTag('SetComponentStateOperationDescriptor'):
            if operationTargetDescriptorContainer.NODETYPE in (namespaces.domTag('MdsDescriptor'),
                                                               namespaces.domTag('ChannelDescriptor'),
                                                               namespaces.domTag('VmdDescriptor'),
                                                               namespaces.domTag('ClockDescriptor'),
                                                               namespaces.domTag('ScoDescriptor'),
                                                               ):
                operation = self._mkOperation(sdcdevice.sco.SetComponentStateOperation,
                                              handle=operationDescriptorContainer.handle,
                                              operationTargetHandle=operationTargetHandle,
                                              codedValue=operationDescriptorContainer.Type,
                                              currentArgumentHandler=self._setComponentState)
                return operation
            return None # the operation target is no AbstractDeviceComponentDescriptor
        elif operationDescriptorContainer.NODETYPE == namespaces.domTag('ActivateOperationDescriptor'):
            #  on what can activate be called?
            if operationTargetDescriptorContainer.NODETYPE in (namespaces.domTag('MdsDescriptor'),
                                                               namespaces.domTag('ChannelDescriptor'),
                                                               namespaces.domTag('VmdDescriptor'),
                                                               namespaces.domTag('ScoDescriptor'),
                                                               ):
                # no generic handler to be called!
                return self._mkOperation(sdcdevice.sco.ActivateOperation,
                                              handle=operationDescriptorContainer.handle,
                                              operationTargetHandle=operationTargetHandle,
                                              codedValue=operationDescriptorContainer.Type)
            return None



    def _setComponentState(self, operationInstance, value):
        '''

        :param operationInstance: the operation
        :param value: a list of proposed metric states
        :return:
        '''
        #ToDo: consider ModifiableDate attribute
        operationInstance.currentValue = value
        with self._mdib.mdibUpdateTransaction() as mgr:
            for proposedComponentState in value:
                state = mgr.getComponentState(proposedComponentState.descriptorHandle)
                if state.isComponentState:
                    self._logger.info('updating {} with proposed component state', state)
                    state.updateFromOtherContainer(proposedComponentState, skippedProperties=['StateVersion', 'DescriptorVersion'])
                else:
                    self._logger.warn('_setComponentState operation: ignore invalid referenced type {} in operation', state.NODETYPE)


class BaseProduct(object):
    def __init__(self, log_prefix):
        # self.contextstate_provider = contextprovider.GenericContextProvider(log_prefix=log_prefix)  # default handler
        # self.componentstate_provider = GenericSetComponentStateOperationProvider(log_prefix=log_prefix) # default handler
        # self.metric_provider = None
        # self.patientcontext_provider = None
        # self.alarm_provider = None
        # self.alarm_provider = None
        # self.operation_provider = None
        # self.audiopause_provider = None
        # self.daynight_provider = None
        # self.clock_provider = None
        # self.ensembleContextProvider = None
        self._mdib = None
        self._ordered_providers = []    # order matters, each provider can hide operations of later ones
                                        # start with most specific providers, end with most general ones
        self._logger = loghelper.getLoggerAdapter('sdc.device.{}'.format(self.__class__.__name__), log_prefix)


    def _all_providers_sorted(self):
        return self._ordered_providers
        # # specialized roles first, generic roles last
        # return self._withoutNoneValues([self.audiopause_provider, self.daynight_provider, self.clock_provider,
        #         self.patientcontext_provider, self.alarm_provider,
        #         self.metric_provider, self.ensembleContextProvider, self.operation_provider,
        #         self.contextstate_provider,
        #         self.componentstate_provider])

    @staticmethod
    def _withoutNoneValues(some_list):
        return [ e for e in some_list if e is not None]


    def initOperations(self, mdib, sco):
        self._mdib = mdib
        ''' register all actively provided operations '''
        for role_handler in self._all_providers_sorted():
            role_handler.initOperations(mdib)

        self._registerExistingMdibOperations(sco)

        for role_handler in self._all_providers_sorted():
            ops = role_handler.makeMissingOperations()
            for op in ops:
                sco.registerOperation(op)

        # log all operations that do not have a handler now
        all_mdib_ops = []
        for nodetype in [namespaces.domTag('SetValueOperationDescriptorContainer'),
                         namespaces.domTag('SetStringOperationDescriptor'),
                         namespaces.domTag('SetContextStateOperationDescriptorContainer'),
                         namespaces.domTag('SetMetricStateOperationDescriptorContainer'),
                         namespaces.domTag('SetComponentStateOperationDescriptorContainer'),
                         namespaces.domTag('SetAlertStateOperationDescriptorContainer'),
                         namespaces.domTag('ActivateOperationDescriptorContainer')]:
            all_mdib_ops.extend(self._mdib.descriptions.NODETYPE.get(nodetype, []))
        all_mdib_op_handles = [op.Handle for op in all_mdib_ops]
        all_not_registered_op_handles = [op_h for op_h in all_mdib_op_handles if sco.getOperationByHandle(op_h) is None]
        if not all_mdib_op_handles:
            self._logger.info('this device has no operations in mdib.')
        elif all_not_registered_op_handles:
            self._logger.info('there are operations without handler! handles = {}', all_not_registered_op_handles)
        else:
            self._logger.info('there are no operations without handler.')
        mdib.mkStateContainersforAllDescriptors()
        mdib.preCommitHandler = self._onPreCommit
        mdib.postCommitHandler = self._onPostCommit

    def stop(self):
        for role_handler in self._all_providers_sorted():
            role_handler.stop()

    def makeOperationInstance(self, operationDescriptorContainer):
        ''' try to get an operation for this operationDescriptorContainer ( given in mdib) '''
        operationTargetHandle = operationDescriptorContainer.OperationTarget
        operationTargetDescr = self._mdib.descriptions.handle.getOne(operationTargetHandle, allowNone=True) # descriptor container
        if operationTargetDescr is None:
            # this operation is incomplete, the operation target does not exist. Registration not possible.
            self._logger.warn(
                'Operation {}: target {} does not exist, will not register operation'.format(operationDescriptorContainer.handle, operationTargetHandle))
            return
        for role_handler in self._all_providers_sorted():
            op = role_handler.makeOperationInstance(operationDescriptorContainer)
            if op is not None:
                self._logger.info('{} provided operation for {}'.format(role_handler.__class__.__name__, operationDescriptorContainer))
                return op
            else:
                self._logger.debug('{}: no handler for {}'.format(op.__class__.__name__, operationDescriptorContainer))
        return None

    def _registerExistingMdibOperations(self, sco):
        operationDescriptorContainers = self._mdib.getOperationDescriptors()
        for c in operationDescriptorContainers:
            registered_op = sco.getOperationByHandle(c.handle)
            if registered_op is None:
                self._logger.info('found unregistered {} in mdib, handle={}, code={} target={}'.format(
                    c.NODETYPE.localname, c.Handle, c.Type, c.OperationTarget))
                op = self.makeOperationInstance(c)
                if op is not None:
                    sco.registerOperation(op)

    def _onPreCommit(self, mdib, transaction):
        for p in self._all_providers_sorted():
            p.onPreCommit(mdib, transaction)
        self._addMissingStatesToTransaction(mdib, transaction)


    def _onPostCommit(self, mdib, transaction):
        for p in self._all_providers_sorted():
            p.onPostCommit(mdib, transaction)
        self._removeStatesForDeletedDescriptors(mdib, transaction)

    def _removeStatesForDeletedDescriptors(self, mdib, transaction):
        '''
        remove states from mdib for deleted descriptors
        :return:
        '''
        for tr_item in transaction.descriptorUpdates.values():
            if tr_item.new is None:
                # deleted descriptor
                objects = mdib.states.descriptorHandle.get(tr_item.old.handle)
                if objects:
                    mdib.states.removeObjects(objects)
                objects = mdib.contextStates.descriptorHandle.get(tr_item.old.handle)
                if objects:
                    mdib.contextStates.removeObjects(objects)


    def _addMissingStatesToTransaction(self, mdib, transaction):
        '''
        add states to new descriptors if they are not part of this transaction
        '''
        for tr_item in transaction.descriptorUpdates.values():
            if tr_item.old is None:
                # new descriptor
                state_cls = mdib.getStateClsForDescriptor(tr_item.new)
                if not state_cls.isMultiState:
                    if not transaction.hasState(tr_item.new.handle):
                        st = state_cls(mdib.nsmapper, tr_item.new)
                        st.updateNode()
                        transaction.addState(st)


class GenericProduct(BaseProduct):
    def __init__(self, audiopause_provider, daynight_provider, clock_provider, log_prefix):
        super().__init__(log_prefix)

        self._ordered_providers.extend([audiopause_provider, daynight_provider, clock_provider])
        self._ordered_providers.extend([patientcontextprovider.GenericPatientContextProvider(log_prefix=log_prefix),
                                       alarmprovider.GenericAlarmProvider(log_prefix=log_prefix),
                                       metricprovider.GenericMetricProvider(log_prefix=log_prefix),
                                       operationprovider.OperationProvider(log_prefix=log_prefix),
                                       GenericSetComponentStateOperationProvider(log_prefix=log_prefix)
        ])


class MinimalProduct(BaseProduct):
    def __init__(self, log_prefix=None):
        super().__init__(log_prefix)
        self.metric_provider = metricprovider.GenericMetricProvider(log_prefix=log_prefix) # needed in a test
        self._ordered_providers.extend([audiopauseprovider.GenericSDCAudioPauseProvider(log_prefix=log_prefix),
                                       clockprovider.GenericSDCClockProvider(log_prefix=log_prefix),
                                       patientcontextprovider.GenericPatientContextProvider(log_prefix=log_prefix),
                                       alarmprovider.GenericAlarmProvider(log_prefix=log_prefix),
                                       self.metric_provider,
                                       operationprovider.OperationProvider(log_prefix=log_prefix),
                                       GenericSetComponentStateOperationProvider(log_prefix=log_prefix)
        ])


class ExtendedProduct(BaseProduct):
    def __init__(self, log_prefix=None):
        super().__init__(log_prefix)
        self._ordered_providers.extend([audiopauseprovider.GenericSDCAudioPauseProvider(log_prefix=log_prefix),
                                       clockprovider.GenericSDCClockProvider(log_prefix=log_prefix),
                                       contextprovider.EnsembleContextProvider(log_prefix=log_prefix),
                                       contextprovider.LocationContextProvider(log_prefix=log_prefix),
                                       patientcontextprovider.GenericPatientContextProvider(log_prefix=log_prefix),
                                       alarmprovider.GenericAlarmProvider(log_prefix=log_prefix),
                                       self.metric_provider,
                                       operationprovider.OperationProvider(log_prefix=log_prefix),
                                       GenericSetComponentStateOperationProvider(log_prefix=log_prefix)
        ])
