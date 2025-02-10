"""Implementation of alarm provider functionality."""
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

    from sdc11073.mdib.descriptorcontainers import AbstractOperationDescriptorProtocol
    from sdc11073.mdib.entityprotocol import EntityProtocol
    from sdc11073.mdib.mdibprotocol import ProviderMdibProtocol
    from sdc11073.mdib.transactionsprotocol import StateTransactionManagerProtocol
    from sdc11073.provider.operations import ExecuteParameters, OperationDefinitionBase, OperationDefinitionProtocol
    from sdc11073.provider.sco import AbstractScoOperationsRegistry

    from .providerbase import OperationClassGetter


class AlertDelegateProvider(providerbase.ProviderRole):
    """Support alert delegation acc. to BICEPS chapter 6.2."""

    def __init__(self, mdib: ProviderMdibProtocol, log_prefix: str):
        super().__init__(mdib, log_prefix)

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
        op_target_entity = self._mdib.entities.by_handle(op_target_handle)
        if pm_names.SetAlertStateOperationDescriptor == operation_descriptor_container.NODETYPE:
            if (pm_names.AlertSignalDescriptor == op_target_entity.node_type
                    and op_target_entity.descriptor.SignalDelegationSupported):
                # operation_descriptor_container is a SetAlertStateOperationDescriptor
                set_state_descriptor_container = cast(AbstractSetStateOperationDescriptorContainer,
                                                      operation_descriptor_container)
                modifiable_data = set_state_descriptor_container.ModifiableData
                if 'Presence' in modifiable_data \
                        and 'ActivationState' in modifiable_data \
                        and 'ActualSignalGenerationDelay' in modifiable_data:
                    operation = self._mk_operation_from_operation_descriptor(
                        operation_descriptor_container,
                        operation_cls_getter,
                        operation_handler=self._delegate_alert_signal,
                        timeout_handler=self._on_timeout_delegate_alert_signal)

                    self._logger.debug('%s: added handler "self._delegate_alert_signal" for %s target=%s',
                                       self.__class__.__name__, operation_descriptor_container, op_target_handle)
                    return operation

        return None  # None == no handler for this operation instantiated

    def _delegate_alert_signal(self, params: ExecuteParameters) -> ExecuteResult:
        """Handle operation call from remote (ExecuteHandler).

        Sets ActivationState, Presence and ActualSignalGenerationDelay of the corresponding state in mdib.
        If this is a delegable signal, it also sets the ActivationState of the fallback signal.
        """
        value = params.operation_request.argument
        pm_types = self._mdib.data_model.pm_types
        pm_names = self._mdib.data_model.pm_names
        all_alert_signal_entities = self._mdib.entities.by_node_type(pm_names.AlertSignalDescriptor)

        operation_target_handle = params.operation_instance.operation_target_handle
        op_target_entity = self._mdib.entities.by_handle(operation_target_handle)

        self._logger.info('delegate alert signal %s of %s from %s to %s', operation_target_handle,
                          op_target_entity.state, op_target_entity.state.ActivationState, value.ActivationState)
        for name in params.operation_instance.descriptor_container.ModifiableData:
            tmp = getattr(value, name)
            setattr(op_target_entity.state, name, tmp)
        modified = []
        if op_target_entity.descriptor.SignalDelegationSupported:
            if value.ActivationState == pm_types.AlertActivation.ON:
                modified = self._pause_fallback_alert_signals(op_target_entity,
                                                              all_alert_signal_entities)
            else:
                modified = self._activate_fallback_alert_signals(op_target_entity,
                                                                 all_alert_signal_entities)
        with self._mdib.alert_state_transaction() as mgr:
            mgr.write_entity(op_target_entity)
            mgr.write_entities(modified)

        return ExecuteResult(operation_target_handle,
                             self._mdib.data_model.msg_types.InvocationState.FINISHED)

    def _on_timeout_delegate_alert_signal(self, operation_instance: OperationDefinitionProtocol):
        """TimeoutHandler for delegated signal."""
        pm_types = self._mdib.data_model.pm_types
        pm_names = self._mdib.data_model.pm_names

        operation_target_handle = operation_instance.operation_target_handle
        op_target_entity = self._mdib.entities.by_handle(operation_target_handle)

        all_alert_signal_entities = self._mdib.entities.by_node_type(pm_names.AlertSignalDescriptor)
        self._logger.info('timeout alert signal delegate operation=%s target=%s',
                          operation_instance.handle, operation_target_handle)
        op_target_entity.state.ActivationState = pm_types.AlertActivation.OFF
        modified = self._activate_fallback_alert_signals(op_target_entity,
                                                         all_alert_signal_entities)

        with self._mdib.alert_state_transaction() as mgr:
            mgr.write_entity(op_target_entity)
            mgr.write_entities(modified)

    def _pause_fallback_alert_signals(self,
                                      delegable_signal_entity: EntityProtocol,
                                      all_signal_entities: list[EntityProtocol],
                                      ) -> list[EntityProtocol]:
        """Pause fallback signals.

        The idea of the fallback signal is to set it paused when the delegable signal is currently ON,
        and to set it back to ON when the delegable signal is not ON.
        This method sets the fallback to PAUSED value.
        :param delegable_signal_entity: the signal that the fallback signals are looked for.
        :param all_signal_entities: list of all signals
        :return: list of modified entities
        """
        pm_types = self._mdib.data_model.pm_types
        modified: list[EntityProtocol] = []
        # look for local fallback signal (same Manifestation), and set it to paused
        for fallback_entity in self._get_fallback_signals(delegable_signal_entity,
                                                          all_signal_entities):
            if fallback_entity.state.ActivationState != pm_types.AlertActivation.PAUSED:
                fallback_entity.state.ActivationState = pm_types.AlertActivation.PAUSED
                modified.append(fallback_entity)
        return modified

    def _activate_fallback_alert_signals(self, delegable_signal_entity: EntityProtocol,
                                         all_signal_entities: list[EntityProtocol],
                                         ) -> list[EntityProtocol]:
        pm_types = self._mdib.data_model.pm_types
        modified: list[EntityProtocol] = []

        # look for local fallback signal (same Manifestation), and set it to paused
        for fallback_entity in self._get_fallback_signals(delegable_signal_entity,
                                                          all_signal_entities):
            if fallback_entity.state.ActivationState == pm_types.AlertActivation.PAUSED:
                fallback_entity.state.ActivationState = pm_types.AlertActivation.ON
                modified.append(fallback_entity)
        return modified

    @staticmethod
    def _get_fallback_signals(delegable_signal_entity: EntityProtocol,
                              all_signal_entities: list[EntityProtocol]) -> list[EntityProtocol]:
        """Return a list of all fallback signals for descriptor.

        looks in all_signal_descriptors for a signal with same ConditionSignaled and same
        Manifestation as delegable_signal_descriptor and SignalDelegationSupported == True.
        """
        return [tmp for tmp in all_signal_entities if not tmp.descriptor.SignalDelegationSupported
                and tmp.descriptor.Manifestation == delegable_signal_entity.descriptor.Manifestation
                and tmp.descriptor.ConditionSignaled == delegable_signal_entity.descriptor.ConditionSignaled]


class AlertSystemStateMaintainer(providerbase.ProviderRole):
    """Provide some generic alarm handling functionality.

    - runs periodic job to update currently present alarms in AlertSystemState
    """

    WORKER_THREAD_INTERVAL = 1.0  # seconds
    self_check_safety_margin = 1.0  # how many seconds before SelfCheckInterval elapses a new self check is performed.

    def __init__(self, mdib: ProviderMdibProtocol, log_prefix: str):
        super().__init__(mdib, log_prefix)

        self._stop_worker = Event()
        self._worker_thread = None

    def init_operations(self, sco: AbstractScoOperationsRegistry):
        """Start a worker thread that periodically updates AlertSystemStateContainers."""
        super().init_operations(sco)
        self._worker_thread = Thread(target=self._worker_thread_loop)
        self._worker_thread.daemon = True
        self._worker_thread.start()

    def stop(self):
        """Stop worker thread."""
        self._stop_worker.set()
        self._worker_thread.join()

    def _worker_thread_loop(self):
        # delay start of operation
        shall_stop = self._stop_worker.wait(timeout=self.WORKER_THREAD_INTERVAL)
        if shall_stop:
            return

        while True:
            shall_stop = self._stop_worker.wait(timeout=self.WORKER_THREAD_INTERVAL)
            if shall_stop:
                return
            self._update_alert_system_state_current_alerts()

    def _update_alert_system_state_current_alerts(self):
        """Update AlertSystemState present alarms list."""
        try:
            entities_needing_update = self._get_alert_system_entities_needing_update()
            if len(entities_needing_update) > 0:
                with self._mdib.alert_state_transaction() as mgr:
                    self._update_alert_system_states(entities_needing_update)
                    mgr.write_entities(entities_needing_update)
        except Exception: # noqa: BLE001
            self._logger.error('_update_alert_system_state_current_alerts: %s', traceback.format_exc())  # noqa: TRY400

    def _get_alert_system_entities_needing_update(self) -> list[EntityProtocol]:
        pm_names = self._mdib.data_model.pm_names
        entities_needing_update = []
        try:
            all_alert_system_entities = self._mdib.entities.by_node_type(pm_names.AlertSystemDescriptor)
            for alert_system_entity in all_alert_system_entities:
                if alert_system_entity.state is not None:
                    self_check_period = alert_system_entity.descriptor.SelfCheckPeriod
                    if self_check_period is not None:
                        last_self_check = alert_system_entity.state.LastSelfCheck or 0.0
                        if time.time() - last_self_check >= self_check_period - self.self_check_safety_margin:
                            entities_needing_update.append(alert_system_entity)
        except Exception: # noqa: BLE001
            self._logger.error('_get_alert_system_entities_needing_update: %s', traceback.format_exc())  # noqa: TRY400
        return entities_needing_update

    def _update_alert_system_states(self, alert_system_entities: Iterable[EntityProtocol]):
        """Update alert system states."""
        pm_types = self._mdib.data_model.pm_types

        for alert_system_entity in alert_system_entities:
            all_child_entities = self._mdib.entities.by_parent_handle(alert_system_entity.handle)
            all_alert_condition_entities = [d for d in all_child_entities if d.descriptor.is_alert_condition_descriptor]
            # select all state containers with technical alarms present
            all_tech_entities = [d for d in all_alert_condition_entities if
                                 d.descriptor.Kind == pm_types.AlertConditionKind.TECHNICAL]
            all_present_tech_entities = [s for s in all_tech_entities if s.state.Presence]
            # select all state containers with physiological alarms present
            all_phys_entities = [d for d in all_alert_condition_entities if
                                 d.descriptor.Kind == pm_types.AlertConditionKind.PHYSIOLOGICAL]
            all_present_phys_entities = [s for s in all_phys_entities if s.state.Presence]

            alert_system_entity.state.PresentTechnicalAlarmConditions = [e.handle for e in all_present_tech_entities]
            alert_system_entity.state.PresentPhysiologicalAlarmConditions = [e.handle for e in
                                                                             all_present_phys_entities]
            alert_system_entity.state.LastSelfCheck = time.time()
            alert_system_entity.state.SelfCheckCount = 1 if alert_system_entity.state.SelfCheckCount is None \
                else alert_system_entity.state.SelfCheckCount + 1


class AlertPreCommitHandler(providerbase.ProviderRole):
    """Provide some generic alarm handling functionality.

    - in pre commit handler it updates present alarms list of alarm system states
    """

    def on_pre_commit(self, mdib: ProviderMdibProtocol, transaction: StateTransactionManagerProtocol):
        """Manipulate the transaction.

        - Updates alert system states and adds them to transaction, if at least one of its alert
          conditions changed ( is in transaction).
        - Updates all AlertSignals for changed Alert Conditions and adds them to transaction.

        :param mdib:
        :param transaction:
        :return:
        """
        if not transaction.alert_state_updates:
            # nothing to do
            return

        all_alert_signal_entities = self._mdib.entities.by_node_type(
            self._mdib.data_model.pm_names.AlertSignalDescriptor)
        changed_alert_condition_states = self._get_changed_alert_condition_states(transaction)
        # change AlertSignal Settings in order to be compliant with changed Alert Conditions
        for changed_alert_condition_state in changed_alert_condition_states:
            self._update_alert_signals(changed_alert_condition_state,
                                       all_alert_signal_entities,
                                       mdib,
                                       transaction)

        # find all alert systems for changed_alert_condition_states
        alert_system_states = self._find_alert_systems_with_modifications(transaction,
                                                                          changed_alert_condition_states)
        if alert_system_states:
            # add found alert system states to transaction
            self._update_alert_system_states(mdib, alert_system_states, transaction)

    @staticmethod
    def _update_alert_system_states(mdib: ProviderMdibProtocol,
                                    alert_system_states: Iterable[AbstractStateProtocol],
                                    transaction: StateTransactionManagerProtocol):
        """Update alert system states PresentTechnicalAlarmConditions and PresentPhysiologicalAlarmConditions."""
        pm_types = mdib.data_model.pm_types

        def _get_alert_state(state: AbstractStateProtocol) -> AbstractStateProtocol:
            """Return the equivalent state from current transaction, if it already in transaction."""
            _item = transaction.get_state_transaction_item(state.DescriptorHandle)
            if _item is not None:
                return _item.new
            return state

        for _alert_system_state in alert_system_states:
            write_entity = True
            tr_item = transaction.get_state_transaction_item(_alert_system_state.DescriptorHandle)
            # If the alert system state is already part of the transaction, make changes in that instance instead.
            if tr_item is not None:
                alert_system_state = tr_item.new
                write_entity = False
            else:
                alert_system_state = _alert_system_state
            all_child_entities = mdib.entities.by_parent_handle(alert_system_state.DescriptorHandle)
            all_alert_condition_entities = [d for d in all_child_entities if d.descriptor.is_alert_condition_descriptor]
            # select all state containers with technical alarms present
            all_tech_entities = [d for d in all_alert_condition_entities if
                                 d.descriptor.Kind == pm_types.AlertConditionKind.TECHNICAL]
            _all_tech_states = [_get_alert_state(d.state) for d in all_tech_entities]
            all_tech_states = cast(list[AlertConditionStateContainer], _all_tech_states)
            all_present_tech_states = [s for s in all_tech_states if s.Presence]

            all_phys_entities = [d for d in all_alert_condition_entities if
                                 d.descriptor.Kind == pm_types.AlertConditionKind.PHYSIOLOGICAL]
            _all_phys_states = [_get_alert_state(d.state) for d in all_phys_entities]
            all_phys_states = cast(list[AlertConditionStateContainer], _all_phys_states)
            all_phys_states = [s for s in all_phys_states if s is not None]
            all_present_phys_states = [s for s in all_phys_states if s.Presence]

            alert_system_state.PresentTechnicalAlarmConditions = [s.DescriptorHandle for s in all_present_tech_states]
            alert_system_state.PresentPhysiologicalAlarmConditions = [s.DescriptorHandle for s in
                                                                      all_present_phys_states]
            if write_entity:
                transaction.write_entity(alert_system_state)

    @staticmethod
    def _get_changed_alert_condition_states(transaction: StateTransactionManagerProtocol) -> list[
        AbstractStateProtocol]:
        """Return all alert conditions in current transaction."""
        result = []
        for item in list(transaction.alert_state_updates.values()):
            tmp = item.old if item.new is None else item.new
            if tmp.is_alert_condition:
                result.append(tmp)
        return result

    @staticmethod
    def _update_alert_signals(changed_alert_condition: AbstractStateProtocol,
                              all_alert_signal_entities: list[EntityProtocol],
                              mdib: ProviderMdibProtocol,
                              transaction: StateTransactionManagerProtocol):
        """Handle alert signals for a changed alert condition.

        This method only changes states of local signals.
        Handling of delegated signals is in the responsibility of the delegated device!
        """
        pm_types = mdib.data_model.pm_types

        my_alert_signal_entities = [e for e in all_alert_signal_entities
                                    if e.descriptor.ConditionSignaled == changed_alert_condition.DescriptorHandle]
        # separate remote from local
        remote_alert_signal_entities = [a for a in my_alert_signal_entities if a.descriptor.SignalDelegationSupported]
        local_alert_signal_entities = [a for a in my_alert_signal_entities if
                                       not a.descriptor.SignalDelegationSupported]

        # look for active delegations (we only need the Manifestation value here)
        active_delegate_manifestations = []
        for entity in remote_alert_signal_entities:
            if (entity.state.Presence != pm_types.AlertSignalPresence.OFF
                    and entity.state.Location == pm_types.AlertSignalPrimaryLocation.REMOTE):
                active_delegate_manifestations.append(entity.descriptor.Manifestation) # noqa: PERF401

        # this lookup gives the values that a local signal shall have:
        # key = (Cond.Presence, isDelegated): value = (SignalState.ActivationState, SignalState.Presence)
        # see BICEPS standard table 9: valid combinations of alert activation states, alert condition presence, ...
        # this is the relevant subset for our case
        lookup = {(True, True): (pm_types.AlertActivation.PAUSED, pm_types.AlertSignalPresence.OFF),
                  (True, False): (pm_types.AlertActivation.ON, pm_types.AlertSignalPresence.ON),
                  (False, True): (pm_types.AlertActivation.PAUSED, pm_types.AlertSignalPresence.OFF),
                  (False, False): (pm_types.AlertActivation.ON, pm_types.AlertSignalPresence.OFF),
                  }
        for entity in local_alert_signal_entities:
            tr_item = transaction.get_state_transaction_item(entity.handle)
            if tr_item is None:
                # is this local signal delegated?
                is_delegated = entity.descriptor.Manifestation in active_delegate_manifestations
                activation, presence = lookup[(changed_alert_condition.Presence, is_delegated)]

                if entity.state.ActivationState != activation or entity.state.Presence != presence:
                    entity.state.ActivationState = activation
                    entity.state.Presence = presence
                    transaction.write_entity(entity)

    def _find_alert_systems_with_modifications(self,
                                               transaction: StateTransactionManagerProtocol,
                                               changed_alert_conditions: list[AbstractStateProtocol]) \
            -> set[AbstractStateProtocol]:
        # find all alert systems for the changed alert conditions
        alert_system_states = set()
        for tmp in changed_alert_conditions:
            alert_condition_entity = self._mdib.entities.by_handle(tmp.DescriptorHandle)
            alert_system_entity = self._mdib.entities.by_handle(alert_condition_entity.parent_handle)

            if alert_system_entity.handle not in transaction.alert_state_updates:
                transaction.write_entity(alert_system_entity)

            transaction_item = transaction.alert_state_updates[alert_system_entity.handle]
            if transaction_item.new is not None:
                alert_system_states.add(transaction_item.new)
        return alert_system_states
