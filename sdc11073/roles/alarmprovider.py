import time
import traceback
from threading import Thread, Event

from . import providerbase

class GenericAlarmProvider(providerbase.ProviderRole):
    WORKER_THREAD_INTERVAL = 1.0  # seconds

    def __init__(self, mdib, log_prefix):
        super().__init__(mdib, log_prefix)

        # some time stamps for handling of delegable alert signals
        self._last_activate_all_delegable_alerts = 0  # time when _activate_all_delegable_alert_signals has been called last time
        self._last_set_alert_signal_state = {}  # a lookup by alert signal handle , value = time of last call

        self._stop_worker = Event()
        self._worker_thread = None

    def init_operations(self):
        super().init_operations()
        self._set_alert_system_states_initial_values()
        self._set_alert_states_initial_values()
        self._worker_thread = Thread(target=self._worker_thread_loop)
        self._worker_thread.daemon = True
        self._worker_thread.start()

    def stop(self):
        self._stop_worker.set()
        self._worker_thread.join()

    def make_operation_instance(self, operation_descriptor_container, operation_cls_getter):
        """
        creates operation handler for:
        - set alert signal state
            => SetAlertStateOperation
                operation target Is an AlertSignalDescriptor
            handler = self._set_alert_signal_state
        :param operation_descriptor_container:
        :param operation_cls_getter:
        :return: None or an OperationDefinition instance
        """
        pm_names = self._mdib.data_model.pm_names
        operation_target_handle = operation_descriptor_container.OperationTarget
        operation_target_descr = self._mdib.descriptions.handle.get_one(operation_target_handle)  # descriptor container
        if operation_descriptor_container.NODETYPE == pm_names.SetValueOperationDescriptor:
            pass
        elif operation_descriptor_container.NODETYPE == pm_names.ActivateOperationDescriptor:
            pass
        elif operation_descriptor_container.NODETYPE == pm_names.SetAlertStateOperationDescriptor:
            if operation_target_descr.NODETYPE == pm_names.AlertSignalDescriptor:
                # no check for code, because the set_alert_state operation always means setting
                # ActivationState, Presence and ActualSignalGenerationDelay
                # if stricter checking needed, one might add it
                operation = self._mk_operation_from_operation_descriptor(operation_descriptor_container,
                                                                         operation_cls_getter,
                                                                         current_argument_handler=self._set_alert_signal_state)

                self._logger.info(f'GenericAlarmProvider: added handler "self._setAlertState" '
                                  f'for {operation_descriptor_container} target= {operation_target_descr} ')
                return operation

        return None  # None == no handler for this operation instantiated

    def _set_alert_system_states_initial_values(self):
        """  Sets ActivationState to ON in all alert systems.
        adds audible SystemSignalActivation, state=ON to all AlertSystemState instances.      Why????
        :return:
        """
        pm_names = self._mdib.data_model.pm_names
        pm_types = self._mdib.data_model.pmtypes

        states = self._mdib.states.NODETYPE.get(pm_names.AlertSystemState, [])
        for state in states:
            state.ActivationState = pm_types.AlertActivation.ON
            state.SystemSignalActivation.append(
                pm_types.SystemSignalActivation(manifestation=pm_types.AlertSignalManifestation.AUD,
                                       state=pm_types.AlertActivation.ON))


    def _set_alert_states_initial_values(self):
        """
        - if an AlertCondition.ActivationState is 'On', then the local AlertSignals shall also be 'On'
        - all remote alert Signals shall be 'Off' initially (must be explicitly enabled by delegating device)"""
        pm_types = self._mdib.data_model.pmtypes
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
                alert_signal_state.ActivationState =  pm_types.AlertActivation.OFF
                alert_signal_state.Presence = pm_types.AlertSignalPresence.OFF
            else:
                alert_signal_state.ActivationState =  pm_types.AlertActivation.ON
                alert_signal_state.Presence = pm_types.AlertSignalPresence.OFF

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
            raise KeyError(f'there is no descriptor for {handle}')
        return descriptor

    def _get_changed_alert_condition_states(self, transaction):
        pm_names = self._mdib.data_model.pm_names
        result = []
        for item in list(transaction.alert_state_updates.values()):
            tmp = item.old if item.new is None else item.new
            if tmp.NODETYPE in (pm_names.AlertConditionState,
                                pm_names.LimitAlertConditionState):
                result.append(tmp)
        return result

    def _find_alert_systems_with_modifications(self, mdib, transaction, changed_alert_conditions):
        # find all alert systems with changed alert conditions
        alert_system_states = set()
        for tmp in changed_alert_conditions:
            alert_descriptor = self._get_descriptor(tmp.DescriptorHandle, mdib, transaction)
            alert_system_descriptor = self._get_descriptor(alert_descriptor.parent_handle, mdib, transaction)
            if alert_system_descriptor.Handle in transaction.alert_state_updates:
                tmp_st = transaction.alert_state_updates[alert_system_descriptor.Handle]
                if tmp_st.new is not None:
                    alert_system_states.add(tmp_st.new)
            else:
                alert_system_states.add(transaction.get_state(alert_system_descriptor.Handle))
        return alert_system_states

    def on_pre_commit(self, mdib, transaction):
        """
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
        alert_system_states = self._find_alert_systems_with_modifications(mdib, transaction, changed_alert_conditions)
        if alert_system_states:
            self._update_alert_system_states(mdib, transaction,
                                             alert_system_states)  # add found alert system states to transaction


    @staticmethod
    def _update_alert_system_states(mdib, transaction, alert_system_states):
        """update alert system states and add them to transaction
        """
        pm_types = mdib.data_model.pmtypes

        def _get_alert_state(descriptor_handle):
            alert_state = None
            tr_item = transaction.get_state_transaction_item(descriptor_handle)
            if tr_item is not None:
                alert_state = tr_item.new
            if alert_state is None:
                # it is not part of this transaction
                alert_state = mdib.states.descriptorHandle.get_one(descriptor_handle, allow_none=True)
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
            all_tech_states = [_get_alert_state(d.Handle) for d in all_tech_descr]
            all_tech_states = [s for s in all_tech_states if s is not None]
            all_present_tech_states = [s for s in all_tech_states if s.Presence]
            # select all state containers with physiolocical alarms present
            all_phys_descr = [d for d in all_alert_condition_descr if d.Kind == pm_types.AlertConditionKind.PHYSIOLOGICAL]
            all_phys_states = [_get_alert_state(d.Handle) for d in all_phys_descr]
            all_phys_states = [s for s in all_phys_states if s is not None]
            all_present_phys_states = [s for s in all_phys_states if s.Presence]

            state.PresentTechnicalAlarmConditions = [s.DescriptorHandle for s in all_present_tech_states]
            state.PresentPhysiologicalAlarmConditions = [s.DescriptorHandle for s in all_present_phys_states]

            state.LastSelfCheck = time.time()
            state.SelfCheckCount = 1 if state.SelfCheckCount is None else state.SelfCheckCount + 1

    @staticmethod
    def _update_alert_signals(changed_alert_condition, mdib, transaction):
        """ Handle alert signals for a changed alert condition.
        This method only changes states of local signals.
        Handling of delegated signals is in the responsibility of the delegated device!"""
        pm_types = mdib.data_model.pmtypes
        alert_signal_descriptors = mdib.descriptions.condition_signaled.get(changed_alert_condition.DescriptorHandle, [])
        # separate remote from local
        remote_alert_signal_descriptors = [a for a in alert_signal_descriptors if a.SignalDelegationSupported]
        local_alert_signal_descriptors = [a for a in alert_signal_descriptors if not a.SignalDelegationSupported]

        # look for active delegations (we only need the Manifestation value here)
        active_delegate_manifestations = []
        for descriptor in remote_alert_signal_descriptors:
            alert_signal_state = mdib.states.descriptorHandle.get_one(descriptor.Handle)
            if alert_signal_state.Presence != pm_types.AlertSignalPresence.OFF and alert_signal_state.Location == 'Rem':
                active_delegate_manifestations.append(descriptor.Manifestation)

        # this lookup gives the values that a local signal shall have:
        # key = (Cond.Presence, isDelegated): value = (SignalState.ActivationState, SignalState.Presence)
        # see BICEPS standard table 9: valid combinations of alert activation states, alert condition presence, ...
        # this is the relevant subset for our case
        lookup = {(True, True): (pm_types.AlertActivation.PAUSED, pm_types.AlertSignalPresence.OFF),
                  (True, False): (pm_types.AlertActivation.ON, pm_types.AlertSignalPresence.ON),
                  (False, True): (pm_types.AlertActivation.PAUSED, pm_types.AlertSignalPresence.OFF),
                  (False, False): (pm_types.AlertActivation.ON, pm_types.AlertSignalPresence.OFF)
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

    def _pause_fallback_alert_signals(self, delegable_signal_descriptor, all_signal_descriptors, transaction):
        """ The idea of the fallback signal is to set it paused when the delegable signal is currently ON,
        and to set it back to ON when the delegable signal is not ON.
        This method sets the fallback to PAUSED value.
        :param delegable_signal_descriptor: a descriptor container
        :param all_signal_descriptors: list of descriptor containers
        :param transaction: the current transaction.
        :return:
        """
        pm_types = self._mdib.data_model.pmtypes
        if all_signal_descriptors is None:
            all_signal_descriptors = self._mdib.descriptions.condition_signaled.get(
                delegable_signal_descriptor.ConditionSignaled, [])

        # look for local fallback signal (same Manifestation), and set it to paused
        fallbacks = [tmp for tmp in all_signal_descriptors if
                     not tmp.SignalDelegationSupported and tmp.Manifestation == delegable_signal_descriptor.Manifestation]
        for fallback in fallbacks:
            ss_fallback = transaction.get_state(fallback.Handle)
            if ss_fallback.ActivationState != pm_types.AlertActivation.PAUSED:
                ss_fallback.ActivationState = pm_types.AlertActivation.PAUSED
            else:
                transaction.unget_state(ss_fallback)

    def _activate_fallback_alert_signals(self, delegable_signal_descriptor, all_signal_descriptors, transaction):
        pm_types = self._mdib.data_model.pmtypes
        if all_signal_descriptors is None:
            all_signal_descriptors = self._mdib.descriptions.condition_signaled.get(
                delegable_signal_descriptor.ConditionSignaled, [])

        # look for local fallback signal (same Manifestation), and set it to paused
        fallbacks = [tmp for tmp in all_signal_descriptors if
                     not tmp.SignalDelegationSupported and tmp.Manifestation == delegable_signal_descriptor.Manifestation]
        for fallback in fallbacks:
            ss_fallback = transaction.get_state(fallback.Handle)
            if ss_fallback.ActivationState == pm_types.AlertActivation.PAUSED:
                ss_fallback.ActivationState = pm_types.AlertActivation.ON
            else:
                transaction.unget_state(ss_fallback)

    def _set_alert_signal_state(self, operation_descriptor_container, value):
        """Handler for an operation call from remote.
        Sets ActivationState, Presence and ActualSignalGenerationDelay of the corresponding state in mdib.
        If this is a delegable signal, it also sets the ActivationState of the fallback signal.

        :param operation_descriptor_container: OperationDescriptorContainer instance
        :param value: AlertSignalStateContainer instance
        :return:
        """
        pm_types = self._mdib.data_model.pmtypes
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
                if value.ActivationState == pm_types.AlertActivation.ON:
                    self._pause_fallback_alert_signals(descr, None, mgr)
                else:
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
        #self._handle_delegate_all_timeouts()
        self._handle_delegate_timeouts()

    def _get_alert_system_states_needing_update(self):
        """

        :return: all AlertSystemStateContainers of those last
        """
        pm_names = self._mdib.data_model.pm_names
        states_needing_update = []
        try:
            all_alert_systems_descr = self._mdib.descriptions.NODETYPE.get(pm_names.AlertSystemDescriptor,
                                                                           [])
            for alert_system_descr in all_alert_systems_descr:
                alert_system_state = self._mdib.states.descriptorHandle.get_one(alert_system_descr.Handle,
                                                                                allow_none=True)
                if alert_system_state is not None:
                    selfcheck_period = alert_system_descr.SelfCheckPeriod
                    if selfcheck_period is not None:
                        last_selfcheck = alert_system_state.LastSelfCheck or 0.0
                        if time.time() - last_selfcheck >= selfcheck_period:
                            states_needing_update.append(alert_system_state)
        except Exception:
            exc = traceback.format_exc()
            self._logger.error('_get_alert_system_states_needing_update: {}', exc)
        return states_needing_update

    def _update_alert_system_state_current_alerts(self):
        """ updates AlertSystemState present alarms list"""
        states_needing_update = self._get_alert_system_states_needing_update()
        self._logger.debug(f'{len(states_needing_update)} states_needing_update')
        if len(states_needing_update) > 0:
            try:
                with self._mdib.transaction_manager() as mgr:
                    tr_states = [mgr.get_state(s.DescriptorHandle) for s in states_needing_update]
                    self._update_alert_system_states(self._mdib, mgr, tr_states)
            except Exception:
                exc = traceback.format_exc()
                self._logger.error('_checkAlertStates: {}', exc)


    def _handle_delegate_timeouts(self):
        #ToDo: rework, this is must become on timeout handlers in operations
        """"""
        pm_types = self._mdib.data_model.pmtypes
        pm_names = self._mdib.data_model.pm_names
        # BICEPS 6.2.1
        all_op_descrs = [ self._mdib.descriptions.handle.get_one(h) for h in self._last_set_alert_signal_state.keys()]
        all_op_descrs = [descr for descr in all_op_descrs if 'ActivationState' in descr.ModifiableData]
        all_op_descrs = [descr for descr in all_op_descrs if 'Presence' in descr.ModifiableData]
        all_op_descrs = [descr for descr in all_op_descrs if 'ActualSignalGenerationDelay' in descr.ModifiableData]
        all_op_descrs = [descr for descr in all_op_descrs if descr.InvocationEffectiveTimeout is not None]

        # filter for operation targets that are delegable and remote
        all_expired_remote_signals = []  #  list of handles
        for op_descr in all_op_descrs:
            try:
                op_target_entity = self._mdib.get_entity(op_descr.OperationTarget)
            except KeyError:
                continue
            if op_target_entity.descriptor.is_alert_signal \
                    and op_target_entity.descriptor.SignalDelegationSupported \
                    and op_target_entity.state.Location == pm_types.AlertSignalPrimaryLocation.REMOTE \
                    and op_target_entity.state.ActivationState == pm_types.AlertActivation.ON:
                age = time.time() - self._last_set_alert_signal_state[op_descr.Handle]
                if age >= op_descr.InvocationEffectiveTimeout:
                    all_expired_remote_signals.append(op_target_entity.state.DescriptorHandle)
        if all_expired_remote_signals:
            with self._mdib.transaction_manager() as mgr:
                for handle in all_expired_remote_signals:
                    st = mgr.get_state(handle)
                    st.ActivationState = pm_types.AlertActivation.OFF
                    self._last_set_alert_signal_state.pop(op_descr.Handle)

