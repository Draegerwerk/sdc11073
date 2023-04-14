from . import providerbase
from .nomenclature import NomenclatureCodes as nc
from sdc11073.xml_types.pm_types import Coding

# coded values for SDC audio pause
MDC_OP_SET_ALL_ALARMS_AUDIO_PAUSE = Coding(nc.MDC_OP_SET_ALL_ALARMS_AUDIO_PAUSE)
MDC_OP_SET_CANCEL_ALARMS_AUDIO_PAUSE = Coding(nc.MDC_OP_SET_CANCEL_ALARMS_AUDIO_PAUSE)


class GenericAudioPauseProvider(providerbase.ProviderRole):
    """Handling of global audio pause.
    It guarantees that there are operations with codes "MDC_OP_SET_ALL_ALARMS_AUDIO_PAUSE"
    and "MDC_OP_SET_CANCEL_ALARMS_AUDIO_PAUSE".
    """

    def __init__(self, mdib, log_prefix):
        super().__init__(mdib, log_prefix)
        self._set_global_audio_pause_operations = []
        self._cancel_global_audio_pause_operations = []

    def make_operation_instance(self, operation_descriptor_container, operation_cls_getter):
        if operation_descriptor_container.coding == MDC_OP_SET_ALL_ALARMS_AUDIO_PAUSE:
            self._logger.debug('instantiating "set audio pause" operation from existing descriptor '
                              f'handle={operation_descriptor_container.Handle}')
            set_ap_operation = self._mk_operation_from_operation_descriptor(operation_descriptor_container,
                                                                            operation_cls_getter,
                                                                            current_request_handler=self._set_global_audio_pause)
            self._set_global_audio_pause_operations.append(set_ap_operation)
            return set_ap_operation
        if operation_descriptor_container.coding == MDC_OP_SET_CANCEL_ALARMS_AUDIO_PAUSE:
            self._logger.debug('instantiating "cancel audio pause" operation from existing descriptor '
                              f'handle={operation_descriptor_container.Handle}')
            cancel_ap_operation = self._mk_operation_from_operation_descriptor(operation_descriptor_container,
                                                                               operation_cls_getter,
                                                                               current_request_handler=self._cancel_global_audio_pause)

            self._cancel_global_audio_pause_operations.append(cancel_ap_operation)
            return cancel_ap_operation
        return None

    def _set_global_audio_pause(self, operation_instance, request):  # pylint: disable=unused-argument
        """ This is the code that executes the operation itself:
        SF1132: If global audio pause is initiated, all SystemSignalActivation/State for all alarm systems of the
        product with SystemSignalActivation/Manifestation evaluating to 'Aud' shall be set to 'Psd'.

        SF958: If signal pause is initiated for an alert signal that is not an ACKNOWLEDGE CAPABLE ALERT SIGNAL,
        then the Alert Provider shall set the AlertSignalState/ActivationState to 'Psd' and the AlertSignalState/Presence to 'Off'.

        SF959: If signal pause is initiated for an ACKNOWLEDGEABLE ALERT SIGNAL, the the Alert Provider shall set the
        AlertSignalState/ActivationState to 'Psd' and AlertSignalState/Presence to 'Ack' for that ALERT SIGNAL.
         """
        pm_types = self._mdib.data_model.pm_types
        pm_names = self._mdib.data_model.pm_names
        alert_system_descriptors = self._mdib.descriptions.NODETYPE.get(pm_names.AlertSystemDescriptor)
        if alert_system_descriptors is None:
            self._logger.error('SDC_SetAudioPauseOperation called, but no AlertSystemDescriptor in mdib found')
            return
        with self._mdib.transaction_manager() as mgr:
            for alert_system_descriptor in alert_system_descriptors:
                alert_system_state = mgr.get_state(alert_system_descriptor.Handle)
                if alert_system_state.ActivationState != pm_types.AlertActivation.ON:
                    self._logger.info('SDC_SetAudioPauseOperation: nothing to do')
                    mgr.unget_state(alert_system_state)
                else:
                    audible_signals = [ssa for ssa in alert_system_state.SystemSignalActivation if
                                       ssa.Manifestation == pm_types.AlertSignalManifestation.AUD]
                    active_audible_signals = [ssa for ssa in audible_signals if
                                              ssa.State != pm_types.AlertActivation.PAUSED]
                    if not active_audible_signals:
                        # Alert System has no audible SystemSignalActivations, no action required
                        mgr.unget_state(alert_system_state)
                    else:
                        for ssa in active_audible_signals:
                            ssa.State = pm_types.AlertActivation.PAUSED  # SF1132
                        self._logger.info(
                            f'SetAudioPauseOperation: set alert system "{alert_system_descriptor.Handle}" to paused')
                        # handle all audible alert signals of this alert system
                        all_alert_signal_descriptors = self._mdib.descriptions.NODETYPE.get(
                            pm_names.AlertSignalDescriptor, [])
                        child_alert_signal_descriptors = [d for d in all_alert_signal_descriptors if
                                                          d.parent_handle == alert_system_descriptor.Handle]
                        audible_child_alert_signal_descriptors = [d for d in child_alert_signal_descriptors if
                                                                  d.Manifestation == pm_types.AlertSignalManifestation.AUD]
                        for descriptor in audible_child_alert_signal_descriptors:
                            alert_signal_state = mgr.get_state(descriptor.Handle)
                            if descriptor.AcknowledgementSupported:  # SF959
                                if alert_signal_state.ActivationState != pm_types.AlertActivation.PAUSED \
                                        or alert_signal_state.Presence != pm_types.AlertSignalPresence.ACK:
                                    alert_signal_state.ActivationState = pm_types.AlertActivation.PAUSED
                                    alert_signal_state.Presence = pm_types.AlertSignalPresence.ACK
                                else:
                                    mgr.unget_state(alert_signal_state)
                            else:  # SF958
                                if alert_signal_state.ActivationState != pm_types.AlertActivation.PAUSED \
                                        or alert_signal_state.Presence != pm_types.AlertSignalPresence.OFF:
                                    alert_signal_state.ActivationState = pm_types.AlertActivation.PAUSED
                                    alert_signal_state.Presence = pm_types.AlertSignalPresence.OFF
                                else:
                                    mgr.unget_state(alert_signal_state)

    def _cancel_global_audio_pause(self, operation_instance, request):  # pylint: disable=unused-argument
        """ This is the code that executes the operation itself:
        If global audio pause is initiated, all SystemSignalActivation/State for all alarm systems of the product with
        SystemSignalActivation/Manifestation evaluating to 'Aud' shall be set to 'Psd'.
         """
        pm_types = self._mdib.data_model.pm_types
        pm_names = self._mdib.data_model.pm_names
        alert_system_descriptors = self._mdib.descriptions.NODETYPE.get(pm_names.AlertSystemDescriptor)
        with self._mdib.transaction_manager() as mgr:
            for alert_system_descriptor in alert_system_descriptors:
                alert_system_state = mgr.get_state(alert_system_descriptor.Handle)
                if alert_system_state.ActivationState != pm_types.AlertActivation.ON:
                    self._logger.info('SDC_CancelAudioPauseOperation: nothing to do')
                    mgr.unget_state(alert_system_state)
                else:
                    audible_signals = [ssa for ssa in alert_system_state.SystemSignalActivation if
                                       ssa.Manifestation == pm_types.AlertSignalManifestation.AUD]
                    paused_audible_signals = [ssa for ssa in audible_signals if
                                              ssa.State == pm_types.AlertActivation.PAUSED]
                    if not paused_audible_signals:
                        mgr.unget_state(alert_system_state)
                    else:
                        for ssa in paused_audible_signals:
                            ssa.State = pm_types.AlertActivation.ON
                        self._logger.info(
                            f'SetAudioPauseOperation: set alert system "{alert_system_descriptor.Handle}" to ON')
                        # handle all audible alert signals of this alert system
                        all_alert_signal_descriptors = self._mdib.descriptions.NODETYPE.get(
                            pm_names.AlertSignalDescriptor, [])
                        child_alert_signal_descriptors = [d for d in all_alert_signal_descriptors if
                                                          d.parent_handle == alert_system_descriptor.Handle]
                        audible_child_alert_signal_descriptors = [d for d in child_alert_signal_descriptors if
                                                                  d.Manifestation == pm_types.AlertSignalManifestation.AUD]
                        for descriptor in audible_child_alert_signal_descriptors:
                            alert_signal_state = mgr.get_state(descriptor.Handle)
                            alert_condition_state = self._mdib.states.descriptorHandle.get_one(
                                descriptor.ConditionSignaled)
                            if alert_condition_state.Presence:
                                # set signal back to 'ON'
                                if alert_signal_state.ActivationState == pm_types.AlertActivation.PAUSED:
                                    alert_signal_state.ActivationState = pm_types.AlertActivation.ON
                                    alert_signal_state.Presence = pm_types.AlertSignalPresence.ON
                                else:
                                    mgr.unget_state(alert_signal_state)


class AudioPauseProvider(GenericAudioPauseProvider):
    """This Implementation adds operations to mdib if they do not exist."""

    def make_missing_operations(self, sco):
        pm_types = self._mdib.data_model.pm_types
        pm_names = self._mdib.data_model.pm_names
        ops = []
        # in this case only the top level sco shall have the additional operations.
        # Check if this is the top level sco (parent is mds)
        parent_descriptor = self._mdib.descriptions.handle.get_one(sco.sco_descriptor_container.parent_handle)
        if parent_descriptor.NODETYPE != pm_names.MdsDescriptor:
            return ops
        operation_cls_getter = sco.operation_cls_getter
        # find mds for this sco
        mds_descr = None
        current_descr = sco.sco_descriptor_container
        while mds_descr is None:
            parent_descr = self._mdib.descriptions.handle.get_one(current_descr.parent_handle)
            if parent_descr is None:
                raise ValueError(f'could not find mds descriptor for sco {sco.sco_descriptor_container.Handle}')
            if parent_descr.NODETYPE == pm_names.MdsDescriptor:
                mds_descr = parent_descr
            else:
                current_descr = parent_descr
        operation_target_container = mds_descr  # the operation target is the mds itself
        activate_op_cls = operation_cls_getter(pm_names.ActivateOperationDescriptor)
        if not self._set_global_audio_pause_operations:
            self._logger.debug(
                f'adding "set audio pause" operation, no descriptor in mdib (looked for code = {nc.MDC_OP_SET_ALL_ALARMS_AUDIO_PAUSE})')
            set_ap_operation = self._mk_operation(activate_op_cls,
                                                  handle='AP__ON',
                                                  operation_target_handle=operation_target_container.Handle,
                                                  coded_value=pm_types.CodedValue(nc.MDC_OP_SET_ALL_ALARMS_AUDIO_PAUSE),
                                                  current_request_handler=self._set_global_audio_pause)
            self._set_global_audio_pause_operations.append(set_ap_operation)
            ops.append(set_ap_operation)
        if not self._cancel_global_audio_pause_operations:
            self._logger.debug(
                f'adding "cancel audio pause" operation, no descriptor in mdib (looked for code = {nc.MDC_OP_SET_CANCEL_ALARMS_AUDIO_PAUSE})')
            cancel_ap_operation = self._mk_operation(activate_op_cls,
                                                     handle='AP__CANCEL',
                                                     operation_target_handle=operation_target_container.Handle,
                                                     coded_value=pm_types.CodedValue(nc.MDC_OP_SET_CANCEL_ALARMS_AUDIO_PAUSE),
                                                     current_request_handler=self._cancel_global_audio_pause)
            ops.append(cancel_ap_operation)
            self._set_global_audio_pause_operations.append(cancel_ap_operation)
        return ops
