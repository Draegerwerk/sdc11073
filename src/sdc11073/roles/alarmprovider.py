from __future__ import annotations

import time
import traceback
from threading import Event, Thread
from typing import TYPE_CHECKING, cast

from sdc11073.mdib.descriptorcontainers import AbstractSetStateOperationDescriptorContainer
from sdc11073.mdib.statecontainers import AbstractStateProtocol, AlertConditionStateContainer
from sdc11073.provider.operations import ExecuteResult

from . import providerbase

if TYPE_CHECKING:
    from collections.abc import Iterable

    from sdc11073.mdib import ProviderMdib
    from sdc11073.mdib.descriptorcontainers import AbstractDescriptorProtocol, AbstractOperationDescriptorProtocol
    from sdc11073.mdib.transactions import TransactionManagerProtocol
    from sdc11073.provider.operations import OperationDefinitionBase, OperationDefinitionProtocol, ExecuteParameters
    from sdc11073.provider.sco import AbstractScoOperationsRegistry

    from .providerbase import OperationClassGetter


class GenericAlarmProvider(providerbase.ProviderRole):
    """Provide some generic alarm handling functionality.

    - in pre commit handler it updates present alarms list of alarm system states
    - runs periodic job to send currently present alarms in AlertSystemState
    - supports alert delegation acc. to BICEPS chapter 6.2
    """

    WORKER_THREAD_INTERVAL = 1.0  # seconds
    self_check_safety_margin = 1.0 # how many seconds before SelfCheckInterval elapses a new self check is performed.

    def __init__(self, mdib: ProviderMdib, log_prefix: str):
        super().__init__(mdib, log_prefix)

        self._stop_worker = Event()
        self._worker_thread = None

    def init_operations(self, sco: AbstractScoOperationsRegistry):
        """Initialize and start what the provider needs.

        - set initial values of all AlertSystemStateContainers.
        - set initial values of all AlertStateContainers.
        - start a worker thread that periodically updates AlertSystemStateContainers.
        """
        super().init_operations(sco)
        self._set_alert_system_states_initial_values()
        self._set_alert_states_initial_values()
        self._worker_thread = Thread(target=self._worker_thread_loop)
        self._worker_thread.daemon = True
        self._worker_thread.start()

    def stop(self):
        """Stop worker thread."""
        self._stop_worker.set()
        self._worker_thread.join()

    def make_operation_instance(self,
                                operation_descriptor_container: AbstractOperationDescriptorProtocol,
                                operation_cls_getter: OperationClassGetter) -> OperationDefinitionBase | None:
        """Return a callable for this operation or None.

        Creates operation handler for:
        - set alert signal state
            => SetAlertStateOperation
                operation target Is an AlertSignalDescriptor
            handler = self._delegate_alert_signal
        """
        pm_names = self._mdib.data_model.pm_names
        op_target_handle = operation_descriptor_container.OperationTarget
        op_target_descr = self._mdib.descriptions.handle.get_one(op_target_handle)
        if pm_names.SetAlertStateOperationDescriptor == operation_descriptor_container.NODETYPE:
            if pm_names.AlertSignalDescriptor == op_target_descr.NODETYPE and op_target_descr.SignalDelegationSupported:
                # operation_descriptor_container is a SetAlertStateOperationDescriptor
                set_state_descriptor_container = cast(AbstractSetStateOperationDescriptorContainer,
                                                      operation_descriptor_container)
                modifiable_data = set_state_descriptor_container.ModifiableData
                if 'Presence' in modifiable_data \
                        and 'ActivationState' in modifiable_data \
                        and 'ActualSignalGenerationDelay' in modifiable_data:
                    # ToDo:  check for appropriate code
                    operation = self._mk_operation_from_operation_descriptor(
                        operation_descriptor_container,
                        operation_cls_getter,
                        operation_handler=self._delegate_alert_signal,
                        timeout_handler=self._on_timeout_delegate_alert_signal)

                    self._logger.debug('GenericAlarmProvider: added handler "self._setAlertState" for %s target=%s',
                                       operation_descriptor_container, op_target_descr)
                    return operation

        return None  # None == no handler for this operation instantiated

    def _set_alert_system_states_initial_values(self):
        """Set ActivationState to ON in all alert systems.

        Adds audible SystemSignalActivation, state=ON to all AlertSystemState instances.      Why????
        """
        pm_names = self._mdib.data_model.pm_names
        pm_types = self._mdib.data_model.pm_types

        states = self._mdib.states.NODETYPE.get(pm_names.AlertSystemState, [])
        for state in states:
            state.ActivationState = pm_types.AlertActivation.ON
            state.SystemSignalActivation.append(
                pm_types.SystemSignalActivation(manifestation=pm_types.AlertSignalManifestation.AUD,
                                                state=pm_types.AlertActivation.ON))

    def _set_alert_states_initial_values(self):
        """Set AlertConditions and AlertSignals.

        - if an AlertCondition.ActivationState is 'On', then the local AlertSignals shall also be 'On'
        - all remote alert Signals shall be 'Off' initially (must be explicitly enabled by delegating device).
        """
        pm_types = self._mdib.data_model.pm_types
        pm_names = self._mdib.data_model.pm_names
        for alert_condition in self._mdib.states.NODETYPE.get(pm_names.AlertConditionState, []):
            alert_condition.ActivationState = pm_types.AlertActivation.ON
            alert_condition.Presence = False
        for alert_condition in self._mdib.states.NODETYPE.get(pm_names.LimitAlertConditionState, []):
            alert_condition.ActivationState = pm_types.AlertActivation.ON
            alert_condition.Presence = False

        for alert_signal_state in self._mdib.states.NODETYPE.get(pm_names.AlertSignalState, []):
            alert_signal_descr = self._mdib.descriptions.handle.get_one(alert_signal_state.DescriptorHandle)
            if alert_signal_descr.SignalDelegationSupported:
                alert_signal_state.Location = pm_types.AlertSignalPrimaryLocation.REMOTE
                alert_signal_state.ActivationState = pm_types.AlertActivation.OFF
                alert_signal_state.Presence = pm_types.AlertSignalPresence.OFF
            else:
                alert_signal_state.ActivationState = pm_types.AlertActivation.ON
                alert_signal_state.Presence = pm_types.AlertSignalPresence.OFF

    def _get_changed_alert_condition_states(self,
                                            transaction: TransactionManagerProtocol) -> list[AbstractStateProtocol]:
        pm_names = self._mdib.data_model.pm_names
        result = []
        for item in list(transaction.alert_state_updates.values()):
            tmp = item.old if item.new is None else item.new
            if tmp.NODETYPE in (pm_names.AlertConditionState,
                                pm_names.LimitAlertConditionState):
                result.append(tmp)
        return result

    def on_pre_commit(self, mdib: ProviderMdib, transaction: TransactionManagerProtocol):
        """Manipulate the transaction.

        - Updates alert system states and adds them to transaction, if at least one of its alert
          conditions changed ( is in transaction).
        - Updates all AlertSignals for changed Alert Conditions and adds them to transaction.

        :param mdib:
        :param transaction:
        :return:
        """
        if not transaction.alert_state_updates:
            return

        changed_alert_conditions = self._get_changed_alert_condition_states(transaction)
        # change AlertSignal Settings in order to be compliant with changed Alert Conditions
        for changed_alert_condition in changed_alert_conditions:
            self._update_alert_signals(changed_alert_condition, mdib, transaction)

        # find all alert systems with changed states
        alert_system_states = self._find_alert_systems_with_modifications(transaction, changed_alert_conditions)
        if alert_system_states:
            # add found alert system states to transaction
            self._update_alert_system_states(mdib, transaction, alert_system_states, is_self_check=False)

    @staticmethod
    def _find_alert_systems_with_modifications(transaction: TransactionManagerProtocol,
                                               changed_alert_conditions: list[AbstractStateProtocol]) \
            -> set[AbstractStateProtocol]:
        # find all alert systems for the changed alert conditions
        alert_system_states = set()
        for tmp in changed_alert_conditions:
            alert_descriptor = transaction.get_descriptor_in_transaction(tmp.DescriptorHandle)
            alert_system_descriptor = transaction.get_descriptor_in_transaction(alert_descriptor.parent_handle)
            if alert_system_descriptor.Handle in transaction.alert_state_updates:
                tmp_st = transaction.alert_state_updates[alert_system_descriptor.Handle]
                if tmp_st.new is not None:
                    alert_system_states.add(tmp_st.new)
            else:
                alert_system_states.add(transaction.get_state(alert_system_descriptor.Handle))
        return alert_system_states

    @staticmethod
    def _update_alert_system_states(mdib: ProviderMdib,
                                    transaction: TransactionManagerProtocol,
                                    alert_system_states: Iterable[AbstractStateProtocol],
                                    is_self_check: bool = True):
        """Update alert system states."""
        pm_types = mdib.data_model.pm_types

        def _get_alert_state(descriptor_handle: str) -> AbstractStateProtocol:
            alert_state = None
            tr_item = transaction.get_state_transaction_item(descriptor_handle)
            if tr_item is not None:
                alert_state = tr_item.new
            if alert_state is None:
                # it is not part of this transaction
                alert_state = mdib.states.descriptor_handle.get_one(descriptor_handle, allow_none=True)
            if alert_state is None:
                raise ValueError(f'there is no alert state for {descriptor_handle}')
            return alert_state

        for state in alert_system_states:
            all_child_descriptors = mdib.descriptions.parent_handle.get(state.DescriptorHandle, [])
            all_child_descriptors.extend(
                [i.new for i in transaction.descriptor_updates.values() if
                 i.new.parent_handle == state.DescriptorHandle])
            all_alert_condition_descr = [d for d in all_child_descriptors if hasattr(d, 'Kind')]
            # select all state containers with technical alarms present
            all_tech_descr = [d for d in all_alert_condition_descr if d.Kind == pm_types.AlertConditionKind.TECHNICAL]
            _all_tech_states = [_get_alert_state(d.Handle) for d in all_tech_descr]
            all_tech_states = cast(list[AlertConditionStateContainer], _all_tech_states)
            all_tech_states = [s for s in all_tech_states if s is not None]
            all_present_tech_states = [s for s in all_tech_states if s.Presence]
            # select all state containers with physiological alarms present
            all_phys_descr = [d for d in all_alert_condition_descr if
                              d.Kind == pm_types.AlertConditionKind.PHYSIOLOGICAL]
            _all_phys_states = [_get_alert_state(d.Handle) for d in all_phys_descr]
            all_phys_states = cast(list[AlertConditionStateContainer], _all_phys_states)
            all_phys_states = [s for s in all_phys_states if s is not None]
            all_present_phys_states = [s for s in all_phys_states if s.Presence]

            state.PresentTechnicalAlarmConditions = [s.DescriptorHandle for s in all_present_tech_states]
            state.PresentPhysiologicalAlarmConditions = [s.DescriptorHandle for s in all_present_phys_states]
            if is_self_check:
                state.LastSelfCheck = time.time()
                state.SelfCheckCount = 1 if state.SelfCheckCount is None else state.SelfCheckCount + 1

    @staticmethod
    def _update_alert_signals(changed_alert_condition: AbstractStateProtocol,
                              mdib: ProviderMdib,
                              transaction: TransactionManagerProtocol):
        """Handle alert signals for a changed alert condition.

        This method only changes states of local signals.
        Handling of delegated signals is in the responsibility of the delegated device!
        """
        pm_types = mdib.data_model.pm_types
        alert_signal_descriptors = mdib.descriptions.condition_signaled.get(changed_alert_condition.DescriptorHandle,
                                                                            [])
        # separate remote from local
        remote_alert_signal_descriptors = [a for a in alert_signal_descriptors if a.SignalDelegationSupported]
        local_alert_signal_descriptors = [a for a in alert_signal_descriptors if not a.SignalDelegationSupported]

        # look for active delegations (we only need the Manifestation value here)
        active_delegate_manifestations = []
        for descriptor in remote_alert_signal_descriptors:
            alert_signal_state = mdib.states.descriptor_handle.get_one(descriptor.Handle)
            if alert_signal_state.Presence != pm_types.AlertSignalPresence.OFF and alert_signal_state.Location == 'Rem':
                active_delegate_manifestations.append(descriptor.Manifestation)

        # this lookup gives the values that a local signal shall have:
        # key = (Cond.Presence, isDelegated): value = (SignalState.ActivationState, SignalState.Presence)
        # see BICEPS standard table 9: valid combinations of alert activation states, alert condition presence, ...
        # this is the relevant subset for our case
        lookup = {(True, True): (pm_types.AlertActivation.PAUSED, pm_types.AlertSignalPresence.OFF),
                  (True, False): (pm_types.AlertActivation.ON, pm_types.AlertSignalPresence.ON),
                  (False, True): (pm_types.AlertActivation.PAUSED, pm_types.AlertSignalPresence.OFF),
                  (False, False): (pm_types.AlertActivation.ON, pm_types.AlertSignalPresence.OFF),
                  }
        for descriptor in local_alert_signal_descriptors:
            tr_item = transaction.get_state_transaction_item(descriptor.Handle)
            if tr_item is None:
                is_delegated = descriptor.Manifestation in active_delegate_manifestations  # is this local signal delegated?
                activation, presence = lookup[(changed_alert_condition.Presence, is_delegated)]
                alert_signal_state = transaction.get_state(descriptor.Handle)

                if alert_signal_state.ActivationState != activation or alert_signal_state.Presence != presence:
                    alert_signal_state.ActivationState = activation
                    alert_signal_state.Presence = presence
                else:
                    # don't change
                    transaction.unget_state(alert_signal_state)

    def _pause_fallback_alert_signals(self,
                                      delegable_signal_descriptor: AbstractDescriptorProtocol,
                                      all_signal_descriptors: list[AbstractDescriptorProtocol] | None,
                                      transaction: TransactionManagerProtocol):
        """Pause fallback signals.

        The idea of the fallback signal is to set it paused when the delegable signal is currently ON,
        and to set it back to ON when the delegable signal is not ON.
        This method sets the fallback to PAUSED value.
        :param delegable_signal_descriptor: a descriptor container
        :param all_signal_descriptors: list of descriptor containers
        :param transaction: the current transaction.
        :return:
        """
        pm_types = self._mdib.data_model.pm_types
        # look for local fallback signal (same Manifestation), and set it to paused
        for fallback in self._get_fallback_signals(delegable_signal_descriptor, all_signal_descriptors):
            ss_fallback = transaction.get_state(fallback.Handle)
            if ss_fallback.ActivationState != pm_types.AlertActivation.PAUSED:
                ss_fallback.ActivationState = pm_types.AlertActivation.PAUSED
            else:
                transaction.unget_state(ss_fallback)

    def _activate_fallback_alert_signals(self, delegable_signal_descriptor: AbstractDescriptorProtocol,
                                         all_signal_descriptors: list[AbstractDescriptorProtocol] | None,
                                         transaction: TransactionManagerProtocol):
        pm_types = self._mdib.data_model.pm_types
        # look for local fallback signal (same Manifestation), and set it to paused
        for fallback in self._get_fallback_signals(delegable_signal_descriptor, all_signal_descriptors):
            ss_fallback = transaction.get_state(fallback.Handle)
            if ss_fallback.ActivationState == pm_types.AlertActivation.PAUSED:
                ss_fallback.ActivationState = pm_types.AlertActivation.ON
            else:
                transaction.unget_state(ss_fallback)

    def _get_fallback_signals(self,
                              delegable_signal_descriptor: AbstractDescriptorProtocol,
                              all_signal_descriptors: list[AbstractDescriptorProtocol] | None) -> list[
        AbstractDescriptorProtocol]:
        """Return a list of all fallback signals for descriptor.

        looks in all_signal_descriptors for a signal with same ConditionSignaled and same
        Manifestation as delegable_signal_descriptor and SignalDelegationSupported == True.
        """
        if all_signal_descriptors is None:
            all_signal_descriptors = self._mdib.descriptions.condition_signaled.get(
                delegable_signal_descriptor.ConditionSignaled, [])
        return [tmp for tmp in all_signal_descriptors if not tmp.SignalDelegationSupported
                and tmp.Manifestation == delegable_signal_descriptor.Manifestation
                and tmp.ConditionSignaled == delegable_signal_descriptor.ConditionSignaled]

    def _delegate_alert_signal(self, params: ExecuteParameters) -> ExecuteResult:
        """Handle operation call from remote (ExecuteHandler).

        Sets ActivationState, Presence and ActualSignalGenerationDelay of the corresponding state in mdib.
        If this is a delegable signal, it also sets the ActivationState of the fallback signal.

        :param operation_instance: OperationDefinition instance
        :param value: AlertSignalStateContainer instance
        :return:
        """
        value = params.operation_request.argument
        pm_types = self._mdib.data_model.pm_types
        operation_target_handle = params.operation_instance.operation_target_handle
        with self._mdib.transaction_manager() as mgr:
            state = mgr.get_state(operation_target_handle)
            self._logger.info('delegate alert signal %s of %s from %s to %s', operation_target_handle, state,
                              state.ActivationState, value.ActivationState)
            for name in params.operation_instance.descriptor_container.ModifiableData:
                tmp = getattr(value, name)
                setattr(state, name, tmp)
            descr = self._mdib.descriptions.handle.get_one(operation_target_handle)
            if descr.SignalDelegationSupported:
                if value.ActivationState == pm_types.AlertActivation.ON:
                    self._pause_fallback_alert_signals(descr, None, mgr)
                else:
                    self._activate_fallback_alert_signals(descr, None, mgr)
        return ExecuteResult(operation_target_handle,
                             self._mdib.data_model.msg_types.InvocationState.FINISHED)

    def _on_timeout_delegate_alert_signal(self, operation_instance: OperationDefinitionProtocol):
        """TimeoutHandler for delegated signal."""
        pm_types = self._mdib.data_model.pm_types
        operation_target_handle = operation_instance.operation_target_handle
        with self._mdib.transaction_manager() as mgr:
            state = mgr.get_state(operation_target_handle)
            self._logger.info('timeout alert signal delegate operation=%s target=%s',
                              operation_instance.handle, operation_target_handle)
            state.ActivationState = pm_types.AlertActivation.OFF
            descr = self._mdib.descriptions.handle.get_one(operation_target_handle)
            self._activate_fallback_alert_signals(descr, None, mgr)

    def _worker_thread_loop(self):
        # delay start of operation
        shall_stop = self._stop_worker.wait(timeout=self.WORKER_THREAD_INTERVAL)
        if shall_stop:
            return

        while True:
            shall_stop = self._stop_worker.wait(timeout=self.WORKER_THREAD_INTERVAL)
            if shall_stop:
                return
            self._run_worker_job()

    def _run_worker_job(self):
        self._update_alert_system_state_current_alerts()

    def _update_alert_system_state_current_alerts(self):
        """Update AlertSystemState present alarms list."""
        states_needing_update = self._get_alert_system_states_needing_update()
        if len(states_needing_update) > 0:
            try:
                with self._mdib.transaction_manager() as mgr:
                    tr_states = [mgr.get_state(s.DescriptorHandle) for s in states_needing_update]
                    self._update_alert_system_states(self._mdib, mgr, tr_states)
            except Exception:
                exc = traceback.format_exc()
                self._logger.error('_checkAlertStates: %s', exc)

    def _get_alert_system_states_needing_update(self) -> list[AbstractStateProtocol]:
        """:return: all AlertSystemStateContainers of those last"""
        pm_names = self._mdib.data_model.pm_names
        states_needing_update = []
        try:
            all_alert_systems_descr = self._mdib.descriptions.NODETYPE.get(pm_names.AlertSystemDescriptor,
                                                                           [])
            for alert_system_descr in all_alert_systems_descr:
                alert_system_state = self._mdib.states.descriptor_handle.get_one(alert_system_descr.Handle,
                                                                                 allow_none=True)
                if alert_system_state is not None:
                    self_check_period = alert_system_descr.SelfCheckPeriod
                    if self_check_period is not None:
                        last_self_check = alert_system_state.LastSelfCheck or 0.0
                        if time.time() - last_self_check >= self_check_period - self.self_check_safety_margin:
                            states_needing_update.append(alert_system_state)
        except Exception:
            exc = traceback.format_exc()
            self._logger.error('_get_alert_system_states_needing_update: %r', exc)
        return states_needing_update
