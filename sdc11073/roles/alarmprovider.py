import time
import traceback
from threading import Thread, Event

from . import providerbase
from .. import namespaces
from ..pmtypes import AlertActivation, AlertConditionKind, AlertSignalPresence, SystemSignalActivation, \
    AlertSignalManifestation


class GenericAlarmProvider(providerbase.ProviderRole):
    WORKERTHREAD_INTERVAL = 1.0  # seconds

    def __init__(self, log_prefix):
        super().__init__(log_prefix)

        # some time stamps for handling of delegable alert signals
        self._last_activate_all_delegable_alerts = 0  # time when _activate_all_delegable_alert_signals has been called last time
        self._last_set_alert_signal_state = {}  # a lookup by alert signal handle , value = time of last call

        self._stop_worker = Event()
        self._worker_thread = None

    def init_operations(self, mdib):
        super().init_operations(mdib)
        self._set_alert_system_states_initial_values()
        self._set_alert_states_initial_values()
        self._worker_thread = Thread(target=self._worker_thread_loop)
        self._worker_thread.daemon = True
        self._worker_thread.start()

    def make_missing_operations(self, operations_factory):
        return []

    def stop(self):
        self._stop_worker.set()
        self._worker_thread.join()

    def make_operation_instance(self, operation_descriptor_container, operations_factory):
        operation_target_handle = operation_descriptor_container.OperationTarget
        operation_target_descr = self._mdib.descriptions.handle.get_one(operation_target_handle)  # descriptor container
        if operation_descriptor_container.NODETYPE == namespaces.domTag('SetValueOperationDescriptor'):
            pass
        elif operation_descriptor_container.NODETYPE == namespaces.domTag('ActivateOperationDescriptor'):
            pass
        elif operation_descriptor_container.NODETYPE == namespaces.domTag('SetAlertStateOperationDescriptor'):
            if operation_target_descr.NODETYPE == namespaces.domTag('AlertSignalDescriptor'):
                # no check for code, because the set_alert_state operation always means setting
                # ActivationState, Presence and ActualSignalGenerationDelay
                # if stricter checking needed, one might add it
                operation = self._mk_operation_from_operation_descriptor(operation_descriptor_container,
                                                                         operations_factory,
                                                                         current_argument_handler=self._set_alert_signal_state)

                self._logger.info(
                    'GenericAlarmProvider: added handler "self._setAlertState" for {} target= {} '.format(
                        operation_descriptor_container,
                        operation_target_descr))
                return operation

        return None  # None == no handler for this operation instantiated

    def _set_alert_system_states_initial_values(self):
        states = self._mdib.states.NODETYPE.get(namespaces.domTag('AlertSystemState'), [])
        for state in states:
            if hasattr(state, 'SystemSignalActivation'):  # attribute not exists in Draft6
                state.SystemSignalActivation.append(
                    SystemSignalActivation(manifestation=AlertSignalManifestation.AUD,
                                           state=AlertActivation.ON))

    def _set_alert_states_initial_values(self):
        """
        - if an AlertCondition.ActivationState is 'On', then the local AlertSignals shall also be 'On'
        - all remote alert Signals shall be 'Off' initially (must be explicitely enabled by delegating device)"""
        for alert_condition in self._mdib.states.NODETYPE.get(namespaces.domTag('AlertConditionState'), []):
            alert_condition.ActivationState = AlertActivation.ON
        for alert_condition in self._mdib.states.NODETYPE.get(namespaces.domTag('LimitAlertConditionState'), []):
            alert_condition.ActivationState = AlertActivation.ON
        for alert_condition in self._mdib.states.NODETYPE.get(namespaces.domTag('AlertSignalState'), []):
            if alert_condition.Location == 'Rem':
                alert_condition.ActivationState = AlertActivation.OFF
                alert_condition.set_node_member()
            else:
                alert_signal_descr = self._mdib.descriptions.handle.get_one(alert_condition.descriptorHandle)
                # ConditionSignaled can be None, in that case do nothing
                if alert_signal_descr.ConditionSignaled:
                    alert_condition_state = self._mdib.states.descriptorHandle.get_one(
                        alert_signal_descr.ConditionSignaled,
                        allow_none=True)
                    if alert_condition_state and alert_condition.ActivationState != alert_condition_state.ActivationState:
                        alert_condition.ActivationState = alert_condition_state.ActivationState
                        alert_condition.set_node_member()

    @staticmethod
    def _get_descriptor(handle, mdib, transaction):
        """ Helper that looks for descriptor first in current transaction, then in mdib. returns first found one or raises KeyError"""
        descriptor = None
        tr_item = transaction.descriptor_updates.get(handle)
        if tr_item is not None:
            descriptor = tr_item.new
        if descriptor is None:
            # it is not part of this transaction
            descriptor = mdib.descriptions.handle.get_one(handle, allow_none=True)
        if descriptor is None:
            raise KeyError('there is no descriptor for {}'.format(handle))
        return descriptor

    @staticmethod
    def _get_changed_alert_condition_states(transaction):
        result = []
        for item in list(transaction.alert_state_updates.values()):
            tmp = item.old if item.new is None else item.new
            if tmp.NODETYPE in (namespaces.domTag('AlertConditionState'),
                                namespaces.domTag('LimitAlertConditionState')):
                result.append(tmp)
        return result

    def _find_alert_systems_with_modifications(self, mdib, transaction):
        # find all alert systems with changed states
        alert_system_states = set()
        changed_alert_conditions = self._get_changed_alert_condition_states(transaction)
        for tmp in changed_alert_conditions:
            alert_descriptor = self._get_descriptor(tmp.descriptorHandle, mdib, transaction)
            alert_system_descriptor = self._get_descriptor(alert_descriptor.parent_handle, mdib, transaction)
            if alert_system_descriptor.handle in transaction.alert_state_updates:
                tmp_st = transaction.alert_state_updates[alert_system_descriptor.handle]
                if tmp_st.new is not None:
                    alert_system_states.add(tmp_st.new)
            else:
                alert_system_states.add(transaction.get_state(alert_system_descriptor.handle))
        return alert_system_states

    def on_pre_commit(self, mdib, transaction):
        if not transaction.alert_state_updates:
            return
        # find all alert systems with changed states
        alert_system_states = self._find_alert_systems_with_modifications(mdib, transaction)
        if alert_system_states:
            self._update_alert_system_states(mdib, transaction,
                                             alert_system_states)  # add found alert system states to transaction

        # change AlertSignal Settings in order to be compliant with changed Alert Conditions
        changed_alert_conditions = self._get_changed_alert_condition_states(transaction)
        for changed_alert_condition in changed_alert_conditions:
            self._update_alert_signals(changed_alert_condition, mdib, transaction)

    @staticmethod
    def _update_alert_system_states(mdib, transaction, alert_system_states):
        """update alert system states and add them to transaction
        """

        def _get_alert_state(descriptor_handle):
            alert_state = None
            tr_item = transaction.get_state_transaction_item(descriptor_handle)
            if tr_item is not None:
                alert_state = tr_item.new
            if alert_state is None:
                # it is not part of this transaction
                alert_state = mdib.states.descriptorHandle.get_one(descriptor_handle, allow_none=True)
            if alert_state is None:
                raise RuntimeError('there is no alert state for {}'.format(descriptor_handle))
            return alert_state

        for state in alert_system_states:
            all_child_descriptors = mdib.descriptions.parent_handle.get(state.descriptorHandle, list())
            all_child_descriptors.extend(
                [i.new for i in transaction.descriptor_updates.values() if
                 i.new.parent_handle == state.descriptorHandle])
            all_alert_condition_descr = [d for d in all_child_descriptors if hasattr(d, 'Kind')]
            # select all state containers with technical alarms present
            all_tech_descr = [d for d in all_alert_condition_descr if d.Kind == AlertConditionKind.TECHNICAL]
            all_tech_states = [_get_alert_state(d.handle) for d in all_tech_descr]
            all_tech_states = [s for s in all_tech_states if s is not None]
            all_present_tech_states = [s for s in all_tech_states if s.Presence]
            # select all state containers with physiolocical alarms present
            all_phys_descr = [d for d in all_alert_condition_descr if d.Kind == AlertConditionKind.PHYSIOLOGICAL]
            all_phys_states = [_get_alert_state(d.handle) for d in all_phys_descr]
            all_phys_states = [s for s in all_phys_states if s is not None]
            all_present_phys_states = [s for s in all_phys_states if s.Presence]

            state.PresentTechnicalAlarmConditions = [s.descriptorHandle for s in all_present_tech_states]
            state.PresentPhysiologicalAlarmConditions = [s.descriptorHandle for s in all_present_phys_states]

            state.LastSelfCheck = time.time()
            state.SelfCheckCount = 1 if state.SelfCheckCount is None else state.SelfCheckCount + 1

    @staticmethod
    def _update_alert_signals(changed_alert_condition, mdib, transaction):
        """ Handle alert signals for a changed alert condition.
        This method only changes states of local signals.
        Handling of delegated signals is in the responsibility of the delegated device!"""
        alert_signal_descriptors = mdib.descriptions.ConditionSignaled.get(changed_alert_condition.descriptorHandle, [])
        # separate remote from local
        remote_alert_signal_descriptors = [a for a in alert_signal_descriptors if a.SignalDelegationSupported]
        local_alert_signal_descriptors = [a for a in alert_signal_descriptors if not a.SignalDelegationSupported]

        # look for active delegations (we only need the Manifestation value here)
        active_delegate_manifestations = []
        for descriptor in remote_alert_signal_descriptors:
            alert_signal_state = mdib.states.descriptorHandle.get_one(descriptor.handle)
            if alert_signal_state.Presence != AlertSignalPresence.OFF and alert_signal_state.Location == 'Rem':
                active_delegate_manifestations.append(descriptor.Manifestation)

        # this lookup gives the values that a local signal shall have:
        # key = (Cond.Presence, isDelegated): value = (SignalState.ActivationState, SignalState.Presence)
        # see BICEPS standard table 9: valid combinations of alert activation states, alert condition presence, ...
        # this is the relevant subset for our case
        lookup = {(True, True): (AlertActivation.PAUSED, AlertSignalPresence.OFF),
                  (True, False): (AlertActivation.ON, AlertSignalPresence.ON),
                  (False, True): (AlertActivation.PAUSED, AlertSignalPresence.OFF),
                  (False, False): (AlertActivation.ON, AlertSignalPresence.OFF)
                  }
        for descriptor in local_alert_signal_descriptors:
            tr_item = transaction.get_state_transaction_item(descriptor.handle)
            if tr_item is None:
                is_delegated = descriptor.Manifestation in active_delegate_manifestations  # is this local signal delegated?
                activation, presence = lookup[(changed_alert_condition.Presence, is_delegated)]
                alert_signal_state = transaction.get_state(descriptor.handle)

                if alert_signal_state.ActivationState != activation or alert_signal_state.Presence != presence:
                    alert_signal_state.ActivationState = activation
                    alert_signal_state.Presence = presence
                else:
                    # don't change
                    transaction.unget_state(alert_signal_state)

    def _pause_fallback_alert_signals(self, delegable_signal_descriptor, all_signal_descriptors, transaction):
        if all_signal_descriptors is None:
            all_signal_descriptors = self._mdib.descriptions.ConditionSignaled.get(
                delegable_signal_descriptor.ConditionSignaled, [])

        # look for local fallback signal (same Manifestation), and set it to paused
        fallbacks = [tmp for tmp in all_signal_descriptors if
                     not tmp.SignalDelegationSupported and tmp.Manifestation == delegable_signal_descriptor.Manifestation]
        for fallback in fallbacks:
            ss_fallback = transaction.get_state(fallback.handle)
            if ss_fallback.ActivationState != AlertActivation.PAUSED:
                ss_fallback.ActivationState = AlertActivation.PAUSED
            else:
                transaction.unget_state(ss_fallback)

    def _activate_fallback_alert_signals(self, delegable_signal_descriptor, all_signal_descriptors, transaction):
        if all_signal_descriptors is None:
            all_signal_descriptors = self._mdib.descriptions.ConditionSignaled.get(
                delegable_signal_descriptor.ConditionSignaled, [])

        # look for local fallback signal (same Manifestation), and set it to paused
        fallbacks = [tmp for tmp in all_signal_descriptors if
                     not tmp.SignalDelegationSupported and tmp.Manifestation == delegable_signal_descriptor.Manifestation]
        for fallback in fallbacks:
            ss_fallback = transaction.get_state(fallback.handle)
            if ss_fallback.ActivationState == AlertActivation.PAUSED:
                ss_fallback.ActivationState = AlertActivation.ON
            else:
                transaction.unget_state(ss_fallback)

    #
    # def _activateAlertSystem(self, operation_descriptor_container, _):
    #     self._setAlertSystemActivationState(operation_descriptor_container.OperationTarget, AlertActivation.ON)
    #
    # def _deactivateAlertSystem(self, operation_descriptor_container, _):
    #     self._setAlertSystemActivationState(operation_descriptor_container.OperationTarget, AlertActivation.OFF)
    #
    # def _setAlertSystemActivationState(self, handle, alertActivationState):
    #     """
    #     R0116: The activation state of pm:AlertSystemState SHALL result in an activation state of
    #         pm:AlertConditionState and pm:AlertSignalState
    #     """
    #     alertConditions, alertSignals = [], []
    #     for child in self._mdib.descriptions.parent_handle.get(handle, []):
    #         if child.isAlertSignalDescriptor:
    #             alertSignals.append(child.handle)
    #         elif child.isAlertConditionDescriptor:
    #             alertConditions.append(child.handle)
    #
    #     with self._mdib.transaction_manager() as mgr:
    #         state = mgr.get_state(handle)
    #         state.ActivationState = alertActivationState
    #         for alertConditionHandle in alertConditions:
    #             self._setAlertConditionActivationState(mgr, alertConditionHandle, alertActivationState)
    #         for alertSignalHandle in alertSignals:
    #             self._setAlertSignalActivationState(mgr, alertSignalHandle, alertActivationState)
    #
    # def _activateAlertCondition(self, operation_descriptor_container, _):
    #     with self._mdib.transaction_manager() as mgr:
    #         self._setAlertConditionActivationState(mgr, operation_descriptor_container.OperationTarget,
    #                                                AlertActivation.ON)
    #
    # def _deactivateAlertCondition(self, operation_descriptor_container, _):
    #     with self._mdib.transaction_manager() as mgr:
    #         self._setAlertConditionActivationState(mgr, operation_descriptor_container.OperationTarget,
    #                                                AlertActivation.OFF)
    #
    # def _setAlertConditionActivationState(self, mgr, handle, alertActivationState):
    #     state = mgr.get_state(handle)
    #     state.ActivationState = alertActivationState
    #
    # def _activateAlertSignal(self, operation_descriptor_container, _):
    #     operation_target_handle = operation_descriptor_container.OperationTarget
    #     with self._mdib.transaction_manager() as mgr:
    #         self._setAlertSignalActivationState(mgr, operation_target_handle, AlertActivation.ON)
    #
    # def _deactivateAlertSignal(self, operation_descriptor_container, _):
    #     operation_target_handle = operation_descriptor_container.OperationTarget
    #     with self._mdib.transaction_manager() as mgr:
    #         self._setAlertSignalActivationState(mgr, operation_target_handle, AlertActivation.OFF)
    #
    # def _setAlertSignalActivationState(self, mgr, handle, alertActivationState):
    #     self._last_set_alert_signal_state[handle] = time.time()
    #     state = mgr.get_state(handle)
    #     state.ActivationState = alertActivationState
    #     descr = self._mdib.descriptions.handle.get_one(handle)
    #     if descr.SignalDelegationSupported:
    #         if alertActivationState == AlertActivation.ON:
    #             self._pause_fallback_alert_signals(descr, None, mgr)
    #         else:
    #             self._activate_fallback_alert_signals(descr, None, mgr)
    #
    def _set_alert_signal_state(self, operation_descriptor_container, value):
        operation_target_handle = operation_descriptor_container.OperationTarget
        self._last_set_alert_signal_state[operation_target_handle] = time.time()
        with self._mdib.transaction_manager() as mgr:
            state = mgr.get_state(operation_target_handle)
            self._logger.info('set alert state {} of {} from {} to {}', operation_target_handle, state,
                              state.ActivationState, value.ActivationState)
            state.ActivationState = value.ActivationState
            state.Presence = value.Presence
            state.ActualSignalGenerationDelay = value.ActualSignalGenerationDelay
            descr = self._mdib.descriptions.handle.get_one(operation_target_handle)
            if descr.SignalDelegationSupported:
                if value.ActivationState == AlertActivation.ON:
                    self._pause_fallback_alert_signals(descr, None, mgr)
                else:
                    self._activate_fallback_alert_signals(descr, None, mgr)

    def _worker_thread_loop(self):
        # delay start of operation
        shall_stop = self._stop_worker.wait(timeout=self.WORKERTHREAD_INTERVAL)
        if shall_stop:
            return

        while True:
            shall_stop = self._stop_worker.wait(timeout=self.WORKERTHREAD_INTERVAL)
            if shall_stop:
                return
            self._update_alert_system_state_current_alerts()
            self._handle_delegate_timeouts()

    def _get_alert_system_states_needing_update(self):
        """

        :return: all AlertSystemStateContainers of those last
        """
        states_needing_update = []
        try:
            all_alert_systems_descr = self._mdib.descriptions.NODETYPE.get(namespaces.domTag('AlertSystemDescriptor'),
                                                                           list())
            for alert_system_descr in all_alert_systems_descr:
                alert_system_state = self._mdib.states.descriptorHandle.get_one(alert_system_descr.handle,
                                                                                allow_none=True)
                if alert_system_state is not None:
                    selfcheck_period = alert_system_descr.SelfCheckPeriod
                    if selfcheck_period is not None:
                        last_selfcheck = alert_system_state.LastSelfCheck or 0.0
                        if time.time() - last_selfcheck >= selfcheck_period:
                            states_needing_update.append(alert_system_state)
        except:
            exc = traceback.format_exc()
            self._logger.error('_get_alert_system_states_needing_update: {}', exc)
        return states_needing_update

    def _update_alert_system_state_current_alerts(self):
        """ updates AlertSystemState present alarms list"""
        states_needing_update = self._get_alert_system_states_needing_update()
        if len(states_needing_update) > 0:
            try:
                with self._mdib.transaction_manager() as mgr:
                    tr_states = [mgr.get_state(s.descriptorHandle) for s in states_needing_update]
                    self._update_alert_system_states(self._mdib, mgr, tr_states)
            except:
                exc = traceback.format_exc()
                self._logger.error('_checkAlertStates: {}', exc)

    def _handle_delegate_timeouts(self):
        if self._last_activate_all_delegable_alerts:
            # find the minimal invocation_effective_timeout
            all_op_descrs = self._mdib.descriptions.NODETYPE.get(namespaces.domTag('SetAlertStateOperationDescriptor'),
                                                                 [])
            timeouts = [op.InvocationEffectiveTimeout for op in all_op_descrs]
            timeouts = [t for t in timeouts if t is not None]
            if not timeouts:
                return  # nothing to do
            minimal_invocation_effective_timeout = min(timeouts)
            if time.time() - self._last_activate_all_delegable_alerts > minimal_invocation_effective_timeout:
                # expired, set all AlertSignalState.ActivationState to 'Off'
                with self._mdib.transaction_manager() as mgr:
                    for op_descrs in all_op_descrs:
                        signal_descr = self._mdib.descriptions.handle.get_one(op_descrs.OperationTarget)
                        all_signal_descriptors = self._mdib.descriptions.ConditionSignaled.get(
                            signal_descr.ConditionSignaled, [])
                        signal_state = mgr.get_state(signal_descr.handle)
                        if signal_state.ActivationState == AlertActivation.ON:
                            signal_state.ActivationState = AlertActivation.OFF
                            self._activate_fallback_alert_signals(signal_descr, all_signal_descriptors, mgr)
                        else:
                            mgr.unget_state(signal_state)
                self._last_activate_all_delegable_alerts = 0
