import time
import random
import traceback
from threading import Thread, Event
from .. import namespaces
from ..pmtypes import  AlertActivation, AlertConditionKind, AlertSignalPresence, SystemSignalActivation, AlertSignalManifestation
from . import providerbase


class GenericAlarmProvider(providerbase.ProviderRole):
    WORKERTHREAD_INTERVAL = 1.0 # seconds
    def __init__(self, log_prefix):
        super(GenericAlarmProvider, self).__init__(log_prefix)

        # some time stamps for handling of delegable alert signals
        self._lastActivateAllDelegableAlerts = 0 # time when _activateAllDelegableAlertSignals has been called last time
        self._lastSetAlertSignalState = {} # a lookup by alert signal handle , value = time of last call

        self._stopWorker = Event()
        self._workerThread = None

    def initOperations(self, mdib):
        super(GenericAlarmProvider, self).initOperations(mdib)
        self._setAlertSystemStatesInitialValues()
        self._setAlertStatesInitialValues()
        self._workerThread = Thread(target=self._workerThreadLoop)
        self._workerThread.daemon = True
        self._workerThread.start()

    def makeMissingOperations(self):
        return []

    def stop(self):
        self._stopWorker.set()
        self._workerThread.join()


    def makeOperationInstance(self, operationDescriptorContainer):
        operationTargetHandle = operationDescriptorContainer.OperationTarget
        operationTargetDescr = self._mdib.descriptions.handle.getOne(operationTargetHandle) # descriptor container
        if operationDescriptorContainer.NODETYPE == namespaces.domTag('SetValueOperationDescriptor'):
            pass
        elif operationDescriptorContainer.NODETYPE == namespaces.domTag('ActivateOperationDescriptor'):
            pass
        elif operationDescriptorContainer.NODETYPE == namespaces.domTag('SetAlertStateOperationDescriptor'):
            if operationTargetDescr.NODETYPE == namespaces.domTag('AlertSignalDescriptor'):
                # no check for code, because the setAlertState operation always means setting
                # ActivationState, Presence and ActualSignalGenerationDelay
                # if stricter checking needed, one might add it
                operation = self._mkOperationFromOperationDescriptor(operationDescriptorContainer,
                                                                     currentArgumentHandler=self._setAlertSignalState)

                self._logger.info(
                    'GenericAlarmProvider: added handler "self._setAlertState" for {} target= {} '.format(operationDescriptorContainer,
                                                                                                          operationTargetDescr))
                return operation

        return None  # None == no handler for this operation instantiated

    def _setAlertSystemStatesInitialValues(self):
        alertSystemStates = self._mdib.states.NODETYPE.get(namespaces.domTag('AlertSystemState'), [])
        for alertSystemState in alertSystemStates:
            if hasattr(alertSystemState, 'SystemSignalActivation'): # attribute not exists in Draft6
                alertSystemState.SystemSignalActivation.append(SystemSignalActivation(manifestation=AlertSignalManifestation.AUD,
                                                                                              state=AlertActivation.ON))

    def _setAlertStatesInitialValues(self):
        '''
        - if an AlertCondition.ActivationState is 'On', then the local AlertSignals shall also be 'On'
        - all remote alert Signals shall be 'Off' initially (must be explicitely enabled by delegating device)'''
        for alertCondition in self._mdib.states.NODETYPE.get(namespaces.domTag('AlertConditionState'),[]):
            alertCondition.ActivationState = AlertActivation.ON
        for alertCondition in self._mdib.states.NODETYPE.get(namespaces.domTag('LimitAlertConditionState'),[]):
            alertCondition.ActivationState = AlertActivation.ON
        for alertSignalState in self._mdib.states.NODETYPE.get(namespaces.domTag('AlertSignalState'),[]):
            if alertSignalState.Location == 'Rem':
                alertSignalState.ActivationState = AlertActivation.OFF
                alertSignalState.updateNode()
            else:
                alertSignalDescr = self._mdib.descriptions.handle.getOne(alertSignalState.descriptorHandle)
                # ConditionSignaled can be None, in that case do nothing
                if alertSignalDescr.ConditionSignaled:
                    alertConditionState = self._mdib.states.descriptorHandle.getOne(alertSignalDescr.ConditionSignaled, allowNone=True)
                    if alertConditionState and alertSignalState.ActivationState != alertConditionState.ActivationState:
                        alertSignalState.ActivationState = alertConditionState.ActivationState
                        alertSignalState.updateNode()

    def _getDescriptor(self, handle, mdib, transaction):
        ''' Helper that looks for descriptor first in current transaction, then in mdib. returns first found one or raises KeyError'''
        descriptor = None
        tr_item = transaction.descriptorUpdates.get(handle)
        if tr_item is not None:
            descriptor = tr_item.new
        if descriptor is None:
            # it is not part of this transaction
            descriptor = mdib.descriptions.handle.getOne(handle, allowNone=True)
        if descriptor is None:
            raise KeyError('there is no descriptor for {}'.format(handle))
        return descriptor

    def _getChangedAlertConditionStates(self, transaction):
        result = []
        for item in list(transaction.alertStateUpdates.values()):
            tmp = item.old if item.new is None else item.new
            if tmp.NODETYPE in(namespaces.domTag('AlertConditionState'),
                               namespaces.domTag('LimitAlertConditionState')):
                result.append(tmp)
        return result

    def _findAlertSystemsWithModifications(self, mdib, transaction):
        # find all alert systems with changed states
        alertSystemStates = set()
        changedAlertConditions = self._getChangedAlertConditionStates(transaction)
        for tmp in changedAlertConditions:
            alertDescriptor = self._getDescriptor(tmp.descriptorHandle, mdib, transaction)
            alertSystemDescriptor = self._getDescriptor(alertDescriptor.parentHandle, mdib, transaction)
            if alertSystemDescriptor.handle in transaction.alertStateUpdates:
                tmp_st = transaction.alertStateUpdates[alertSystemDescriptor.handle]
                if tmp_st.new is not None:
                    alertSystemStateContainer = tmp_st.new
                    alertSystemStates.add(alertSystemStateContainer)
            else:
                alertSystemStateContainer = transaction.getAlertState(alertSystemDescriptor.handle)
                alertSystemStates.add(alertSystemStateContainer)
        return alertSystemStates

    def onPreCommit(self, mdib, transaction):
        if not transaction.alertStateUpdates:
            return
        # find all alert systems with changed states
        alertSystemStates = self._findAlertSystemsWithModifications( mdib, transaction)
        if alertSystemStates:
            self._updateAlertSystemStates(mdib, transaction, alertSystemStates) # add found alert system states to transaction

        # change AlertSignal Settings in order to be compliant with changed Alert Conditions
        changedAlertConditions = self._getChangedAlertConditionStates(transaction)
        for changedAlertCondition in changedAlertConditions:
            self._updateAlertSignals(changedAlertCondition, mdib, transaction)

    def _updateAlertSystemStates(self, mdib, transaction, alertSystemStates):
        '''update alert system states and add them to transaction
        '''
        def _getAlertState(descriptorHandle):
            alertState = None
            tr_item = transaction.getStateTransactionItem(descriptorHandle)
            if tr_item is not None:
                alertState = tr_item.new
            if alertState is None:
                # it is not part of this transaction
                alertState = mdib.states.descriptorHandle.getOne(descriptorHandle, allowNone=True)
            if alertState is None:
                raise RuntimeError('there is no alert state for {}'.format(descriptorHandle))
            return alertState
        for st in alertSystemStates:
            all_child_descriptors = mdib.descriptions.parentHandle.get(st.descriptorHandle, list())
            all_child_descriptors.extend([i.new for i in transaction.descriptorUpdates.values() if i.new.parentHandle == st.descriptorHandle])
            all_alert_condition_descr = [d for d in all_child_descriptors if hasattr(d, 'Kind')]
            # select all state containers with technical alarms present
            all_tech_descr = [ d for d in all_alert_condition_descr if d.Kind == AlertConditionKind.TECHNICAL]
            all_tech_states = [_getAlertState(d.handle) for d in all_tech_descr]
            all_tech_states = [s for s in all_tech_states if s is not None]
            all_present_tech_states = [s for s in all_tech_states if s.Presence]
            # select all state containers with physiolocical alarms present
            all_phys_descr = [ d for d in all_alert_condition_descr if d.Kind == AlertConditionKind.PHYSIOLOGICAL]
            all_phys_states = [_getAlertState(d.handle) for d in all_phys_descr]
            all_phys_states = [ s for s in all_phys_states if s is not None]
            all_present_phys_states = [s for s in all_phys_states if s.Presence]

            st.PresentTechnicalAlarmConditions = [s.descriptorHandle for s in all_present_tech_states]
            st.PresentPhysiologicalAlarmConditions = [s.descriptorHandle for s in all_present_phys_states]

            st.LastSelfCheck = time.time()
            st.SelfCheckCount = 1 if st.SelfCheckCount is None else st.SelfCheckCount + 1

    def _updateAlertSignals(self, changedAlertCondition, mdib, transaction):
        ''' Handle alert signals for a changed alert condition.
        This method only changes states of local signals.
        Handling of delegated signals is in the responsibility of the delegated device!'''
        alertSignalDescriptors = mdib.descriptions.ConditionSignaled.get(changedAlertCondition.descriptorHandle, [])
        # separate remote from local
        remoteAlertSignalDescriptors = [a for a in alertSignalDescriptors if a.SignalDelegationSupported]
        localAlertSignalDescriptors = [a for a in alertSignalDescriptors if not a.SignalDelegationSupported]

        # look for active delegations (we only need the Manifestation value here)
        active_delegate_manifestations = []
        for a in remoteAlertSignalDescriptors:
            alertSignalState = mdib.states.descriptorHandle.getOne(a.handle)
            if alertSignalState.Presence != AlertSignalPresence.OFF and alertSignalState.Location == 'Rem':
                active_delegate_manifestations.append(a.Manifestation)

        # this lookup gives the values that a local signal shall have:
        # key = (Cond.Presence, isDelegated): value = (SignalState.ActivationState, SignalState.Presence)
        # see BICEPS standard table 9: valid combinations of alert activation states, alert condition presence, ...
        # this is the relevant subset for our case
        lookup = {(True, True): (AlertActivation.PAUSED, AlertSignalPresence.OFF),
                  (True, False): (AlertActivation.ON, AlertSignalPresence.ON),
                  (False, True): (AlertActivation.PAUSED, AlertSignalPresence.OFF),
                  (False, False): (AlertActivation.ON, AlertSignalPresence.OFF)
                  }
        for a in localAlertSignalDescriptors:
            tr_item = transaction.getStateTransactionItem(a.handle)
            if tr_item is None:
                isDelegated = a.Manifestation in active_delegate_manifestations  # is this local signal delegated?
                activation, presence = lookup[(changedAlertCondition.Presence, isDelegated)]
                alertSignalState = transaction.getAlertState(a.handle)

                if alertSignalState.ActivationState != activation or alertSignalState.Presence != presence:
                    alertSignalState.ActivationState = activation
                    alertSignalState.Presence = presence
                else:
                    # don't change
                    transaction.ungetState(alertSignalState)

    def _setUpperLimit(self, operationDescriptorContainer, value):
        ''' set upper limit of an LimitAlertConditionStateContainer'''
        operationTargetHandle = operationDescriptorContainer.operationTarget
        with self._mdib.mdibUpdateTransaction() as mgr:
            operationStateContainer = mgr._deviceMdibContainer.states.descriptorHandle.getOne(operationDescriptorContainer.handle)
            self._inAllowedRange(operationStateContainer, value)
            state = mgr.getAlertState(operationTargetHandle)
            self._logger.info('set upper limit handle="{}" from {} to {}', operationTargetHandle, state.Limits.Upper, value)
            state.Limits.Upper = value

    def _setLowerLimit(self, operationDescriptorContainer, value):
        ''' set lower limit of an LimitAlertConditionStateContainer'''
        operationTargetHandle = operationDescriptorContainer.operationTarget
        with self._mdib.mdibUpdateTransaction() as mgr:
            operationStateContainer = mgr._deviceMdibContainer.states.descriptorHandle.getOne(operationDescriptorContainer.handle)
            self._inAllowedRange(operationStateContainer, value)
            state = mgr.getAlertState(operationTargetHandle)
            self._logger.info('set lower limit handle={} from {} to {}', operationTargetHandle, state.Limits.Lower, value)
            state.Limits.Lower = value

    @staticmethod
    def _inAllowedRange(operationStateContainer, value):
        if operationStateContainer.AllowedRange:
            max_upper = max([i.Upper for i in operationStateContainer.AllowedRange])
            min_lower = min([i.Lower for i in operationStateContainer.AllowedRange])
            if not min_lower <= value <= max_upper:
                raise ValueError('value "{}" to be set is not in AllowedRanges {}'.format(value, operationStateContainer.AllowedRange))

    def _getAcSourceRange(self, source):
        """
        Given the source of the AlertCondition return PhysiologicalRange
        Only returns range with Lower, Upper and StepWidth
        @param source: Source metric handle
        @return: (Range) PhysiologicalRange
        """

        for handle in source:
            state = self._mdib.states.descriptorHandle.getOne(handle)
            ranges = state.PhysiologicalRange
            # find range that has Upper Lower and Step
            for range in ranges:
                if range.Upper is not None and range.Lower is not None and range.StepWidth is not None:
                    return range
        return None

    def _generateValueWithinRange(self, sourceRange, previousValue, isLower=True):
        """ Generates a value within range different from the previous value"""
        if sourceRange is None:
            if isLower:
                return -random.random()
            else:
                return random.random()

        exp = str(sourceRange.StepWidth)[::-1].find(".")
        if exp == -1:
            exp = 0

        value = previousValue
        for cnt in range(int((sourceRange.Upper - sourceRange.Lower) // sourceRange.StepWidth)):
            if value != previousValue: break

            if isLower:
                value = round(sourceRange.Lower + (sourceRange.StepWidth*cnt), exp)
            else:
                value = round(sourceRange.Upper - (sourceRange.StepWidth*cnt), exp)

        return value

    def _activateAllDelegableAlertSignals(self, operationDescriptorContainer, value): #pylint: disable=unused-argument
        # find all delegable Alert Signals in main alert system
        # set ActivationState to "On"
        # set all corresponding local alert signals to paused
        allAlertConditionDescriptors = self._mdib.descriptions.NODETYPE.get(namespaces.domTag('AlertConditionDescriptor'), [])
        allAlertConditionDescriptors += self._mdib.descriptions.NODETYPE.get(namespaces.domTag('LimitAlertConditionDescriptor'), [])
        self._lastActivateAllDelegableAlerts = time.time()
        with self._mdib.mdibUpdateTransaction() as mgr:
            for acd in allAlertConditionDescriptors:
                signalDescriptors = self._mdib.descriptions.ConditionSignaled.get(acd.handle, [])
                for sd in signalDescriptors:
                    if sd.SignalDelegationSupported:
                        ss = mgr.getAlertState(sd.handle)
                        if ss.ActivationState == AlertActivation.OFF:
                            ss.ActivationState = AlertActivation.ON
                            self._pauseFallbackAlertSignals(sd, signalDescriptors, mgr)
                        else:
                            mgr.ungetState(ss)

    def _pauseFallbackAlertSignals(self, delegableSignalDescriptor, allSignalDescriptors, transaction):
        if allSignalDescriptors is None:
            allSignalDescriptors = self._mdib.descriptions.ConditionSignaled.get(delegableSignalDescriptor.ConditionSignaled, [])

        # look for local fallback signal (same Manifestation), and set it to paused
        fallbacks = [tmp for tmp in allSignalDescriptors if
                     not tmp.SignalDelegationSupported and tmp.Manifestation == delegableSignalDescriptor.Manifestation]
        for f in fallbacks:
            ss_fallback = transaction.getAlertState(f.handle)
            if ss_fallback.ActivationState != AlertActivation.PAUSED:
                ss_fallback.ActivationState = AlertActivation.PAUSED
            else:
                transaction.ungetState(ss_fallback)

    def _activateFallbackAlertSignals(self, delegableSignalDescriptor, allSignalDescriptors, transaction):
        if allSignalDescriptors is None:
            allSignalDescriptors = self._mdib.descriptions.ConditionSignaled.get(delegableSignalDescriptor.ConditionSignaled, [])

        # look for local fallback signal (same Manifestation), and set it to paused
        fallbacks = [tmp for tmp in allSignalDescriptors if
                     not tmp.SignalDelegationSupported and tmp.Manifestation == delegableSignalDescriptor.Manifestation]
        for f in fallbacks:
            ss_fallback = transaction.getAlertState(f.handle)
            if ss_fallback.ActivationState == AlertActivation.PAUSED:
                ss_fallback.ActivationState = AlertActivation.ON
            else:
                transaction.ungetState(ss_fallback)

    def _activateAlertSystem(self, operationDescriptorContainer, _):
        self._setAlertSystemActivationState(operationDescriptorContainer.operationTarget, AlertActivation.ON)

    def _deactivateAlertSystem(self, operationDescriptorContainer, _):
        self._setAlertSystemActivationState(operationDescriptorContainer.operationTarget, AlertActivation.OFF)

    def _setAlertSystemActivationState(self, handle, alertActivationState):
        """
        R0116: The activation state of pm:AlertSystemState SHALL result in an activation state of
            pm:AlertConditionState and pm:AlertSignalState
        """
        alertConditions, alertSignals = [], []
        for child in self._mdib.descriptions.parentHandle.get(handle, []):
            if child.isAlertSignalDescriptor:
                alertSignals.append(child.handle)
            elif child.isAlertConditionDescriptor:
                alertConditions.append(child.handle)

        with self._mdib.mdibUpdateTransaction() as mgr:
            state = mgr.getAlertState(handle)
            state.ActivationState = alertActivationState
            for alertConditionHandle in alertConditions:
                self._setAlertConditionActivationState(mgr, alertConditionHandle, alertActivationState)
            for alertSignalHandle in alertSignals:
                self._setAlertSignalActivationState(mgr, alertSignalHandle, alertActivationState)

    def _activateAlertCondition(self, operationDescriptorContainer, _):
        with self._mdib.mdibUpdateTransaction() as mgr:
            self._setAlertConditionActivationState(mgr, operationDescriptorContainer.operationTarget, AlertActivation.ON)

    def _deactivateAlertCondition(self, operationDescriptorContainer, _):
        with self._mdib.mdibUpdateTransaction() as mgr:
            self._setAlertConditionActivationState(mgr, operationDescriptorContainer.operationTarget, AlertActivation.OFF)

    def _setAlertConditionActivationState(self, mgr, handle, alertActivationState):
        state = mgr.getAlertState(handle)
        state.ActivationState = alertActivationState

    def _activateAlertSignal(self, operationDescriptorContainer, _):
        operationTargetHandle = operationDescriptorContainer.operationTarget
        with self._mdib.mdibUpdateTransaction() as mgr:
            self._setAlertSignalActivationState(mgr, operationTargetHandle, AlertActivation.ON)

    def _deactivateAlertSignal(self, operationDescriptorContainer, _):
        operationTargetHandle = operationDescriptorContainer.operationTarget
        with self._mdib.mdibUpdateTransaction() as mgr:
            self._setAlertSignalActivationState(mgr, operationTargetHandle, AlertActivation.OFF)

    def _setAlertSignalActivationState(self, mgr, handle, alertActivationState):
        self._lastSetAlertSignalState[handle] = time.time()
        state = mgr.getAlertState(handle)
        state.ActivationState = alertActivationState
        descr = self._mdib.descriptions.handle.getOne(handle)
        if descr.SignalDelegationSupported:
            if alertActivationState == AlertActivation.ON:
                self._pauseFallbackAlertSignals(descr, None, mgr)
            else:
                self._activateFallbackAlertSignals(descr, None, mgr)

    def _setAlertSignalState(self, operationDescriptorContainer, value):
        operationTargetHandle = operationDescriptorContainer.operationTarget
        self._lastSetAlertSignalState[operationTargetHandle] = time.time()
        with self._mdib.mdibUpdateTransaction() as mgr:
            state = mgr.getAlertState(operationTargetHandle)
            self._logger.info('set alert state {} of {} from {} to {}', operationTargetHandle, state, state.ActivationState, value.ActivationState)
            state.ActivationState = value.ActivationState
            state.Presence = value.Presence
            state.ActualSignalGenerationDelay = value.ActualSignalGenerationDelay
            descr = self._mdib.descriptions.handle.getOne(operationTargetHandle)
            if descr.SignalDelegationSupported:
                if value.ActivationState == AlertActivation.ON:
                    self._pauseFallbackAlertSignals(descr, None, mgr)
                else:
                    self._activateFallbackAlertSignals(descr, None, mgr)

    def _workerThreadLoop(self):
        # delay start of operation
        shall_stop = self._stopWorker.wait(timeout=self.WORKERTHREAD_INTERVAL)
        if shall_stop:
            return

        while True:
            shall_stop = self._stopWorker.wait(timeout=self.WORKERTHREAD_INTERVAL)
            if shall_stop:
                return
            self._updateAlertSystemState_CurrentAlerts()
            self._handleDelegateTimeouts()

    def _getAlertSystemStates_needingUpdate(self):
        '''

        :return: all AlertSystemStateContainers of those last
        '''
        alertStatesNeedingUpdate = []
        try:
            all_alert_systems_descr = self._mdib.descriptions.NODETYPE.get(namespaces.domTag('AlertSystemDescriptor'), list())
            for alert_system_descr in all_alert_systems_descr:
                alert_system_state = self._mdib.states.descriptorHandle.getOne(alert_system_descr.handle, allowNone=True)
                selfcheck_period = alert_system_descr.SelfCheckPeriod
                if selfcheck_period is not None:
                    last_selfcheck = alert_system_state.LastSelfCheck or 0.0
                    if time.time() - last_selfcheck >= selfcheck_period:
                        alertStatesNeedingUpdate.append(alert_system_state)
        except:
            exc = traceback.format_exc()
            self._logger.error('_getAlertSystemStates_needingUpdate: {}', exc)
        return alertStatesNeedingUpdate

    def _updateAlertSystemState_CurrentAlerts(self):
        ''' updates AlertSystemState present alarms list'''
        alertStatesNeedingUpdate = self._getAlertSystemStates_needingUpdate()
        if len(alertStatesNeedingUpdate) > 0:
            try:
                with self._mdib.mdibUpdateTransaction() as tr:
                    tr_states = [tr.getAlertState(s.descriptorHandle) for s in alertStatesNeedingUpdate]
                    self._updateAlertSystemStates(self._mdib, tr, tr_states)
            except:
                exc = traceback.format_exc()
                self._logger.error('_checkAlertStates: {}', exc)

    def _handleDelegateTimeouts(self):
        if self._lastActivateAllDelegableAlerts:
            # find the minimal invocation_effective_timeout
            all_op_descrs = self._mdib.descriptions.NODETYPE.get(namespaces.domTag('SetAlertStateOperationDescriptor'), [])
            timeouts = [op.InvocationEffectiveTimeout for op in all_op_descrs]
            timeouts = [t for t in timeouts if t is not None]
            if not timeouts:
                return # nothing to do
            minimal_invocation_effective_timeout = min(timeouts)
            if time.time() - self._lastActivateAllDelegableAlerts > minimal_invocation_effective_timeout:
                # expired, set all AlertSignalState.ActivationState to 'Off'
                with self._mdib.mdibUpdateTransaction() as mgr:
                    for op in all_op_descrs:
                        signalDescr = self._mdib.descriptions.handle.getOne(op.OperationTarget)
                        allSignalDescriptors = self._mdib.descriptions.ConditionSignaled.get(signalDescr.ConditionSignaled, [])
                        ss = mgr.getAlertState(signalDescr.handle)
                        if ss.ActivationState == AlertActivation.ON:
                            ss.ActivationState = AlertActivation.OFF
                            self._activateFallbackAlertSignals(signalDescr, allSignalDescriptors, mgr)
                        else:
                            mgr.ungetState(ss)
                self._lastActivateAllDelegableAlerts = 0
