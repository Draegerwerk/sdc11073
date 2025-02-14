"""Implementation of audio pause functionality."""
from __future__ import annotations

from typing import TYPE_CHECKING

from sdc11073.provider.operations import ExecuteResult
from sdc11073.xml_types.msg_types import InvocationState
from sdc11073.xml_types.pm_types import Coding

from .nomenclature import NomenclatureCodes
from .providerbase import OperationClassGetter, ProviderRole

if TYPE_CHECKING:
    from sdc11073.mdib.descriptorcontainers import AbstractOperationDescriptorProtocol
    from sdc11073.mdib.mdibprotocol import ProviderMdibProtocol
    from sdc11073.provider.operations import ExecuteParameters, OperationDefinitionBase
    from sdc11073.provider.sco import AbstractScoOperationsRegistry

# coded values for SDC audio pause
MDC_OP_SET_ALL_ALARMS_AUDIO_PAUSE = Coding(NomenclatureCodes.MDC_OP_SET_ALL_ALARMS_AUDIO_PAUSE)
MDC_OP_SET_CANCEL_ALARMS_AUDIO_PAUSE = Coding(NomenclatureCodes.MDC_OP_SET_CANCEL_ALARMS_AUDIO_PAUSE)


class GenericAudioPauseProvider(ProviderRole):
    """Example for handling of global audio pause.

    This provider handles Activate operations with codes "MDC_OP_SET_ALL_ALARMS_AUDIO_PAUSE"
    and "MDC_OP_SET_CANCEL_ALARMS_AUDIO_PAUSE".
    Nothing is added to the mdib. If the mdib does not contain these operations, the functionality is not available.
    """

    def __init__(self, mdib: ProviderMdibProtocol, log_prefix: str):
        super().__init__(mdib, log_prefix)
        self._set_global_audio_pause_operations = []
        self._cancel_global_audio_pause_operations = []

    def make_operation_instance(self,
                                operation_descriptor_container: AbstractOperationDescriptorProtocol,
                                operation_cls_getter: OperationClassGetter) -> OperationDefinitionBase | None:
        """Create operation handlers for existing mdib entries.

        Handle codes MDC_OP_SET_ALL_ALARMS_AUDIO_PAUSE and MDC_OP_SET_CANCEL_ALARMS_AUDIO_PAUSE.
        """
        if operation_descriptor_container.coding == MDC_OP_SET_ALL_ALARMS_AUDIO_PAUSE:
            self._logger.debug('instantiating "set audio pause" operation from existing descriptor handle=%s',
                               operation_descriptor_container.Handle)
            set_ap_operation = self._mk_operation_from_operation_descriptor(
                operation_descriptor_container,
                operation_cls_getter,
                operation_handler=self._set_global_audio_pause)
            self._set_global_audio_pause_operations.append(set_ap_operation)
            return set_ap_operation
        if operation_descriptor_container.coding == MDC_OP_SET_CANCEL_ALARMS_AUDIO_PAUSE:
            self._logger.debug('instantiating "cancel audio pause" operation from existing descriptor handle=%s',
                               operation_descriptor_container.Handle)
            cancel_ap_operation = self._mk_operation_from_operation_descriptor(
                operation_descriptor_container,
                operation_cls_getter,
                operation_handler=self._cancel_global_audio_pause)

            self._cancel_global_audio_pause_operations.append(cancel_ap_operation)
            return cancel_ap_operation
        return None

    def _set_global_audio_pause(self, params: ExecuteParameters) -> ExecuteResult:
        """Set global audio pause (ExecuteHandler).

        If global audio pause is initiated, all SystemSignalActivation/State for all alarm systems of the
        product with SystemSignalActivation/Manifestation evaluating to 'Aud' are set to 'Psd'.

        If signal pause is initiated for an alert signal that is not an ACKNOWLEDGE CAPABLE ALERT SIGNAL,
        then the AlertSignalState/ActivationState is set to 'Psd' and the AlertSignalState/Presence to 'Off'.

        If signal pause is initiated for an ACKNOWLEDGEABLE ALERT SIGNAL, the
        AlertSignalState/ActivationState is set to 'Psd' and AlertSignalState/Presence to 'Ack' for that ALERT SIGNAL.
        """
        pm_types = self._mdib.data_model.pm_types
        pm_names = self._mdib.data_model.pm_names

        alert_system_entities = self._mdib.entities.by_node_type(pm_names.AlertSystemDescriptor)
        if len(alert_system_entities) == 0:
            self._logger.warning('_set_global_audio_pause called, but no AlertSystemDescriptor in mdib found')
            return ExecuteResult(params.operation_instance.operation_target_handle, InvocationState.FAILED)

        with self._mdib.alert_state_transaction() as mgr:
            for as_entity in alert_system_entities:
                if as_entity.state.ActivationState != pm_types.AlertActivation.ON:
                    self._logger.info('_set_global_audio_pause: nothing to do for alert system %s', as_entity.handle)
                    continue

                audible_signals = [ssa for ssa in as_entity.state.SystemSignalActivation if
                                   ssa.Manifestation == pm_types.AlertSignalManifestation.AUD]
                active_audible_signals = [ssa for ssa in audible_signals if
                                          ssa.State != pm_types.AlertActivation.PAUSED]
                if len(active_audible_signals) > 0:
                    for ssa in active_audible_signals:
                        ssa.State = pm_types.AlertActivation.PAUSED
                    self._logger.info('SetAudioPauseOperation: set alert system "%s" to paused',
                                      as_entity.handle)

                    # handle all audible alert signals of this alert system
                    all_alert_signal_entities = self._mdib.entities.by_node_type(
                        pm_names.AlertSignalDescriptor)
                    child_alert_signals = [d for d in all_alert_signal_entities if d.parent_handle == as_entity.handle]
                    audible_child_alert_signals = [d for d in child_alert_signals if
                                                   d.descriptor.Manifestation == pm_types.AlertSignalManifestation.AUD]

                    for aud_signal in audible_child_alert_signals:
                        if aud_signal.descriptor.AcknowledgementSupported:
                            if aud_signal.state.ActivationState != pm_types.AlertActivation.PAUSED \
                                    or aud_signal.state.Presence != pm_types.AlertSignalPresence.ACK:
                                aud_signal.state.ActivationState = pm_types.AlertActivation.PAUSED
                                aud_signal.state.Presence = pm_types.AlertSignalPresence.ACK
                                mgr.write_entity(aud_signal)
                        elif aud_signal.state.ActivationState != pm_types.AlertActivation.PAUSED \
                                or aud_signal.state.Presence != pm_types.AlertSignalPresence.OFF:
                            aud_signal.state.ActivationState = pm_types.AlertActivation.PAUSED
                            aud_signal.state.Presence = pm_types.AlertSignalPresence.OFF
                            mgr.write_entity(aud_signal)
                    mgr.write_entity(as_entity)

        return ExecuteResult(params.operation_instance.operation_target_handle,
                             self._mdib.data_model.msg_types.InvocationState.FINISHED)

    def _cancel_global_audio_pause(self, params: ExecuteParameters) -> ExecuteResult:
        """Cancel global audio pause (ExecuteHandler).

        If global audio pause is initiated, all SystemSignalActivation/State for all alarm systems of the product with
        SystemSignalActivation/Manifestation evaluating to 'Aud' shall be set to 'Psd'.
        """
        pm_types = self._mdib.data_model.pm_types
        pm_names = self._mdib.data_model.pm_names
        with self._mdib.alert_state_transaction() as mgr:
            alert_system_entities = self._mdib.entities.by_node_type(pm_names.AlertSystemDescriptor)
            if len(alert_system_entities) == 0:
                self._logger.warning('_cancel_global_audio_pause called, but no AlertSystemDescriptor in mdib found')
                return ExecuteResult(params.operation_instance.operation_target_handle, InvocationState.FAILED)

            for as_entity in alert_system_entities:
                if as_entity.state.ActivationState != pm_types.AlertActivation.ON:
                    self._logger.info('_cancel_global_audio_pause: nothing to do for alert system %s', as_entity.handle)
                    continue

                audible_signals = [ssa for ssa in as_entity.state.SystemSignalActivation if
                                   ssa.Manifestation == pm_types.AlertSignalManifestation.AUD]
                paused_audible_signals = [ssa for ssa in audible_signals if
                                          ssa.State == pm_types.AlertActivation.PAUSED]
                if len(paused_audible_signals) > 0:
                    for ssa in paused_audible_signals:
                        ssa.State = pm_types.AlertActivation.ON
                    self._logger.info('_cancel_global_audio_pause: set alert system "%s" to ON', as_entity.handle)
                    # handle all audible alert signals of this alert system
                    all_alert_signal_entities = self._mdib.entities.by_node_type(pm_names.AlertSignalDescriptor)
                    child_alert_signals = [e for e in all_alert_signal_entities if
                                                   e.parent_handle == as_entity.handle]
                    audible_child_alert_signals = [d for d in child_alert_signals if
                                                   d.descriptor.Manifestation == pm_types.AlertSignalManifestation.AUD]
                    for aud_signal in audible_child_alert_signals:
                        alert_condition_entity = self._mdib.entities.by_handle(aud_signal.descriptor.ConditionSignaled)
                        if alert_condition_entity.state.Presence:
                            # set signal back to 'ON'
                            if aud_signal.state.ActivationState == pm_types.AlertActivation.PAUSED:
                                aud_signal.state.ActivationState = pm_types.AlertActivation.ON
                                aud_signal.state.Presence = pm_types.AlertSignalPresence.ON
                                mgr.write_entity(aud_signal)
                    mgr.write_entity(as_entity)
        return ExecuteResult(params.operation_instance.operation_target_handle,
                             self._mdib.data_model.msg_types.InvocationState.FINISHED)


class AudioPauseProvider(GenericAudioPauseProvider):
    """Handling of global audio pause example.

    This provider guarantees that there are Activate operations with codes "MDC_OP_SET_ALL_ALARMS_AUDIO_PAUSE"
    and "MDC_OP_SET_CANCEL_ALARMS_AUDIO_PAUSE". It adds them to mdib if they do not exist.
    """

    def make_missing_operations(self, sco: AbstractScoOperationsRegistry) -> list[OperationDefinitionBase]:
        """Add operations to mdib if they do not exist.

        - code MDC_OP_SET_ALL_ALARMS_AUDIO_PAUSE starts alarm pause
        - code MDC_OP_SET_CANCEL_ALARMS_AUDIO_PAUSE cancels alarm pause
        It creates two activate operations with the MDS element as operation target.
        """
        pm_types = self._mdib.data_model.pm_types
        pm_names = self._mdib.data_model.pm_names
        ops = []
        # in this case only the top level sco shall have the additional operations.
        # Check if this is the top level sco (parent is mds)
        parent_entity = self._mdib.entities.by_handle(sco.sco_descriptor_container.parent_handle)
        if pm_names.MdsDescriptor != parent_entity.descriptor.NODETYPE:
            return ops
        operation_cls_getter = sco.operation_cls_getter
        # find mds for this sco
        mds_entity = self._mdib.entities.by_handle(parent_entity.descriptor.source_mds)
        if mds_entity is None:
            msg = f"no source mds found for entity {parent_entity.handle}"
            raise ValueError(msg)

        activate_op_cls = operation_cls_getter(pm_names.ActivateOperationDescriptor)
        if not self._set_global_audio_pause_operations:
            self._logger.debug('adding "set audio pause" operation, no descriptor in mdib (looked for code = %s)',
                               NomenclatureCodes.MDC_OP_SET_ALL_ALARMS_AUDIO_PAUSE)
            set_ap_operation = activate_op_cls('AP__ON',
                                               mds_entity.handle,
                                               self._set_global_audio_pause,
                                               coded_value=pm_types.CodedValue(
                                                   NomenclatureCodes.MDC_OP_SET_ALL_ALARMS_AUDIO_PAUSE))
            self._set_global_audio_pause_operations.append(set_ap_operation)
            ops.append(set_ap_operation)
        if not self._cancel_global_audio_pause_operations:
            self._logger.debug('adding "cancel audio pause" operation, no descriptor in mdib (looked for code = %s)',
                               NomenclatureCodes.MDC_OP_SET_CANCEL_ALARMS_AUDIO_PAUSE)
            cancel_ap_operation = activate_op_cls('AP__CANCEL',
                                                  mds_entity.handle,
                                                  self._cancel_global_audio_pause,
                                                  coded_value=pm_types.CodedValue(
                                                      NomenclatureCodes.MDC_OP_SET_CANCEL_ALARMS_AUDIO_PAUSE))
            ops.append(cancel_ap_operation)
            self._set_global_audio_pause_operations.append(cancel_ap_operation)
        return ops
