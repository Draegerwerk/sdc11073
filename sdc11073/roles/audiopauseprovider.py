from . import providerbase
from .nomenclature import NomenclatureCodes as nc
from .. import pmtypes
from ..namespaces import domTag

# coded values for SDC audio pause
MDC_OP_SET_ALL_ALARMS_AUDIO_PAUSE = pmtypes.CodedValue(nc.MDC_OP_SET_ALL_ALARMS_AUDIO_PAUSE)
MDC_OP_SET_CANCEL_ALARMS_AUDIO_PAUSE = pmtypes.CodedValue(nc.MDC_OP_SET_CANCEL_ALARMS_AUDIO_PAUSE)


class GenericAudioPauseProvider(providerbase.ProviderRole):
    """Handling of global audio pause.
    It guarantees that there are operations with codes "MDC_OP_SET_ALL_ALARMS_AUDIO_PAUSE"
    and "MDC_OP_SET_CANCEL_ALARMS_AUDIO_PAUSE".
    """

    def __init__(self, log_prefix):
        super().__init__(log_prefix)
        self._set_global_audio_pause_operations = []
        self._cancel_global_audio_pause_operations = []

    def make_operation_instance(self, operation_descriptor_container, operation_cls_getter):
        if operation_descriptor_container.coding == MDC_OP_SET_ALL_ALARMS_AUDIO_PAUSE.coding:
            self._logger.info('instantiating "set audio pause" operation from existing descriptor handle={}'.format(
                operation_descriptor_container.handle))
            set_ap_operation = self._mk_operation_from_operation_descriptor(operation_descriptor_container,
                                                                            operation_cls_getter,
                                                                            current_request_handler=self._set_global_audio_pause)
            self._set_global_audio_pause_operations.append(set_ap_operation)
            return set_ap_operation

        if operation_descriptor_container.coding == MDC_OP_SET_CANCEL_ALARMS_AUDIO_PAUSE.coding:
            self._logger.info('instantiating "cancel audio pause" operation from existing descriptor handle={}'.format(
                operation_descriptor_container.handle))
            cancel_ap_operation = self._mk_operation_from_operation_descriptor(operation_descriptor_container,
                                                                               operation_cls_getter,
                                                                               current_request_handler=self._cancel_global_audio_pause)

            self._cancel_global_audio_pause_operations.append(cancel_ap_operation)
            return cancel_ap_operation
        return None

    # def make_missing_operations(self, operations_factory):
    #     ops = []
    #     operation_target_container = self._mdib.descriptions.NODETYPE.get_one(
    #         domTag('MdsDescriptor'))  # the operation target is the mds itself
    #     activate_op_cls = operations_factory(domTag('ActivateOperationDescriptor'))
    #     if not self._set_global_audio_pause_operations:
    #         self._logger.info('adding "set audio pause" operation, no descriptor in mdib (looked for code = {})'.format(
    #             nc.MDC_OP_SET_ALL_ALARMS_AUDIO_PAUSE))
    #         set_ap_operation = self._mk_operation(activate_op_cls,
    #                                               handle='AP__ON',
    #                                               operation_target_handle=operation_target_container.handle,
    #                                               coded_value=MDC_OP_SET_ALL_ALARMS_AUDIO_PAUSE,
    #                                               current_request_handler=self._set_global_audio_pause)
    #         self._set_global_audio_pause_operations.append(set_ap_operation)
    #         ops.append(set_ap_operation)
    #     if not self._cancel_global_audio_pause_operations:
    #         self._logger.info(
    #             'adding "cancel audio pause" operation, no descriptor in mdib (looked for code = {})'.format(
    #                 nc.MDC_OP_SET_CANCEL_ALARMS_AUDIO_PAUSE))
    #         cancel_ap_operation = self._mk_operation(activate_op_cls,
    #                                                  handle='AP__CANCEL',
    #                                                  operation_target_handle=operation_target_container.handle,
    #                                                  coded_value=MDC_OP_SET_CANCEL_ALARMS_AUDIO_PAUSE,
    #                                                  current_request_handler=self._cancel_global_audio_pause)
    #         ops.append(cancel_ap_operation)
    #         self._set_global_audio_pause_operations.append(cancel_ap_operation)
    #     return ops

    def _set_global_audio_pause(self, operation_instance, request):  # pylint: disable=unused-argument
        """ This is the code that executes the operation itself:
        SF1132: If global audio pause is initiated, all SystemSignalActivation/State for all alarm systems of the
        product with SystemSignalActivation/Manifestation evaluating to 'Aud' shall be set to 'Psd'.

        SF958: If signal pause is initiated for an alert signal that is not an ACKNOWLEDGE CAPABLE ALERT SIGNAL,
        then the Alert Provider shall set the AlertSignalState/ActivationState to 'Psd' and the AlertSignalState/Presence to 'Off'.

        SF959: If signal pause is initiated for an ACKNOWLEDGEABLE ALERT SIGNAL, the the Alert Provider shall set the
        AlertSignalState/ActivationState to 'Psd' and AlertSignalState/Presence to 'Ack' for that ALERT SIGNAL.
         """
        alert_system_descriptors = self._mdib.descriptions.NODETYPE.get(domTag('AlertSystemDescriptor'))
        if alert_system_descriptors is None:
            self._logger.error('SDC_SetAudioPauseOperation called, but no AlertSystemDescriptor in mdib found')
            return
        with self._mdib.transaction_manager() as mgr:
            for alert_system_descriptor in alert_system_descriptors:
                alert_system_state = mgr.get_state(alert_system_descriptor.handle)
                if alert_system_state.ActivationState != pmtypes.AlertActivation.ON:
                    self._logger.info('SDC_SetAudioPauseOperation: nothing to do')
                    mgr.unget_state(alert_system_state)
                else:
                    audible_signals = [ssa for ssa in alert_system_state.SystemSignalActivation if
                                       ssa.Manifestation == pmtypes.AlertSignalManifestation.AUD]
                    active_audible_signals = [ssa for ssa in audible_signals if
                                              ssa.State != pmtypes.AlertActivation.PAUSED]
                    if not active_audible_signals:
                        # Alert System has no audible SystemSignalActivations, no action required
                        mgr.unget_state(alert_system_state)
                    else:
                        for ssa in active_audible_signals:
                            ssa.State = pmtypes.AlertActivation.PAUSED  # SF1132
                        self._logger.info('SDC_SetAudioPauseOperation: set alertsystem "{}" to paused'.format(
                            alert_system_descriptor.handle))
                        # handle all audible alert signals of this alert system
                        all_alert_signal_descriptors = self._mdib.descriptions.NODETYPE.get(
                            domTag('AlertSignalDescriptor'), [])
                        child_alert_signal_descriptors = [d for d in all_alert_signal_descriptors if
                                                          d.parent_handle == alert_system_descriptor.handle]
                        audible_child_alert_signal_descriptors = [d for d in child_alert_signal_descriptors if
                                                                  d.Manifestation == pmtypes.AlertSignalManifestation.AUD]
                        for descriptor in audible_child_alert_signal_descriptors:
                            alert_signal_state = mgr.get_state(descriptor.handle)
                            if descriptor.AcknowledgementSupported:  # SF959
                                if alert_signal_state.ActivationState != pmtypes.AlertActivation.PAUSED \
                                        or alert_signal_state.Presence != pmtypes.AlertSignalPresence.ACK:
                                    alert_signal_state.ActivationState = pmtypes.AlertActivation.PAUSED
                                    alert_signal_state.Presence = pmtypes.AlertSignalPresence.ACK
                                else:
                                    mgr.unget_state(alert_signal_state)
                            else:  # SF958
                                if alert_signal_state.ActivationState != pmtypes.AlertActivation.PAUSED \
                                        or alert_signal_state.Presence != pmtypes.AlertSignalPresence.OFF:
                                    alert_signal_state.ActivationState = pmtypes.AlertActivation.PAUSED
                                    alert_signal_state.Presence = pmtypes.AlertSignalPresence.OFF
                                else:
                                    mgr.unget_state(alert_signal_state)

    def _cancel_global_audio_pause(self, operation_instance, request):  # pylint: disable=unused-argument
        """ This is the code that executes the operation itself:
        If global audio pause is initiated, all SystemSignalActivation/State for all alarm systems of the product with
        SystemSignalActivation/Manifestation evaluating to 'Aud' shall be set to 'Psd'.
         """
        alert_system_descriptors = self._mdib.descriptions.NODETYPE.get(domTag('AlertSystemDescriptor'))
        with self._mdib.transaction_manager() as mgr:
            for alert_system_descriptor in alert_system_descriptors:
                alert_system_state = mgr.get_state(alert_system_descriptor.handle)
                if alert_system_state.ActivationState != pmtypes.AlertActivation.ON:
                    self._logger.info('SDC_CancelAudioPauseOperation: nothing to do')
                    mgr.unget_state(alert_system_state)
                else:
                    audible_signals = [ssa for ssa in alert_system_state.SystemSignalActivation if
                                       ssa.Manifestation == pmtypes.AlertSignalManifestation.AUD]
                    paused_audible_signals = [ssa for ssa in audible_signals if
                                              ssa.State == pmtypes.AlertActivation.PAUSED]
                    if not paused_audible_signals:
                        mgr.unget_state(alert_system_state)
                    else:
                        for ssa in paused_audible_signals:
                            ssa.State = pmtypes.AlertActivation.ON
                        self._logger.info('SDC_SetAudioPauseOperation: set alertsystem "{}" to ON'.format(
                            alert_system_descriptor.handle))
                        # handle all audible alert signals of this alert system
                        all_alert_signal_descriptors = self._mdib.descriptions.NODETYPE.get(
                            domTag('AlertSignalDescriptor'), [])
                        child_alert_signal_descriptors = [d for d in all_alert_signal_descriptors if
                                                          d.parent_handle == alert_system_descriptor.handle]
                        audible_child_alert_signal_descriptors = [d for d in child_alert_signal_descriptors if
                                                                  d.Manifestation == pmtypes.AlertSignalManifestation.AUD]
                        for descriptor in audible_child_alert_signal_descriptors:
                            alert_signal_state = mgr.get_state(descriptor.handle)
                            alert_condition_state = self._mdib.states.descriptorHandle.get_one(
                                descriptor.ConditionSignaled)
                            if alert_condition_state.Presence:
                                # set signal back to 'ON'
                                if alert_signal_state.ActivationState == pmtypes.AlertActivation.PAUSED:
                                    alert_signal_state.ActivationState = pmtypes.AlertActivation.ON
                                    alert_signal_state.Presence = pmtypes.AlertSignalPresence.ON
                                else:
                                    mgr.unget_state(alert_signal_state)


class AudioPauseProvider(GenericAudioPauseProvider):
    """This Implementation adds operations to mdib if they do not exist."""
    def make_missing_operations(self, operation_cls_getter):
        ops = []
        operation_target_container = self._mdib.descriptions.NODETYPE.get_one(
            domTag('MdsDescriptor'))  # the operation target is the mds itself
        activate_op_cls = operation_cls_getter(domTag('ActivateOperationDescriptor'))
        if not self._set_global_audio_pause_operations:
            self._logger.info('adding "set audio pause" operation, no descriptor in mdib (looked for code = {})'.format(
                nc.MDC_OP_SET_ALL_ALARMS_AUDIO_PAUSE))
            set_ap_operation = self._mk_operation(activate_op_cls,
                                                  handle='AP__ON',
                                                  operation_target_handle=operation_target_container.handle,
                                                  coded_value=MDC_OP_SET_ALL_ALARMS_AUDIO_PAUSE,
                                                  current_request_handler=self._set_global_audio_pause)
            self._set_global_audio_pause_operations.append(set_ap_operation)
            ops.append(set_ap_operation)
        if not self._cancel_global_audio_pause_operations:
            self._logger.info(
                'adding "cancel audio pause" operation, no descriptor in mdib (looked for code = {})'.format(
                    nc.MDC_OP_SET_CANCEL_ALARMS_AUDIO_PAUSE))
            cancel_ap_operation = self._mk_operation(activate_op_cls,
                                                     handle='AP__CANCEL',
                                                     operation_target_handle=operation_target_container.handle,
                                                     coded_value=MDC_OP_SET_CANCEL_ALARMS_AUDIO_PAUSE,
                                                     current_request_handler=self._cancel_global_audio_pause)
            ops.append(cancel_ap_operation)
            self._set_global_audio_pause_operations.append(cancel_ap_operation)
        return ops
