"""Implementation of clock provider functionality."""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

from sdc11073.provider.operations import ExecuteResult

from .nomenclature import NomenclatureCodes
from .providerbase import OperationClassGetter, ProviderRole

if TYPE_CHECKING:
    from sdc11073.mdib.descriptorcontainers import AbstractOperationDescriptorProtocol
    from sdc11073.mdib.mdibprotocol import ProviderMdibProtocol
    from sdc11073.provider.operations import ExecuteParameters, OperationDefinitionBase
    from sdc11073.provider.sco import AbstractScoOperationsRegistry


class GenericSDCClockProvider(ProviderRole):
    """Handles operations for setting ntp server and time zone.

    This provider handles SetString operations with codes
    "MDC_OP_SET_TIME_SYNC_REF_SRC" and "MDC_ACT_SET_TIME_ZONE".
    Nothing is added to the mdib. If the mdib does not contain these operations, the functionality is not available.
    """

    def __init__(self, mdib: ProviderMdibProtocol, log_prefix: str):
        super().__init__(mdib, log_prefix)
        self._set_ntp_operations = []
        self._set_tz_operations = []
        pm_types = self._mdib.data_model.pm_types

        self.MDC_OP_SET_TIME_SYNC_REF_SRC = pm_types.CodedValue(NomenclatureCodes.MDC_OP_SET_TIME_SYNC_REF_SRC)
        self.MDC_ACT_SET_TIME_ZONE = pm_types.CodedValue(NomenclatureCodes.MDC_ACT_SET_TIME_ZONE)

    def init_operations(self, sco: AbstractScoOperationsRegistry):
        """Create a ClockDescriptor and ClockState in mdib if they do not exist in mdib."""
        super().init_operations(sco)
        pm_types = self._mdib.data_model.pm_types
        pm_names = self._mdib.data_model.pm_names
        clock_entities = self._mdib.entities.by_node_type(pm_names.ClockDescriptor)
        if len(clock_entities) == 0:
            mds_entities = self._mdib.entities.by_node_type(pm_names.MdsDescriptor)
            if len(mds_entities) == 0:
                self._logger.info('empty mdib, cannot create a clock descriptor')
                return
            # create a clock descriptor for the first mds
            my_mds_entity = mds_entities[0]
            clock_descr_handle = 'clock_' + uuid.uuid4().hex
            self._logger.debug('creating a clock descriptor, handle=%s', clock_descr_handle)
            model = self._mdib.data_model
            clock_entity = self._mdib.entities.new_entity(
                model.pm_names.ClockDescriptor,
                handle=clock_descr_handle,
                parent_handle=my_mds_entity.handle,
            )
            clock_entity.descriptor.SafetyClassification = pm_types.SafetyClassification.INF
            clock_entity.descriptor.Type = pm_types.CodedValue('123')
            with self._mdib.descriptor_transaction() as mgr:
                mgr.write_entity(clock_entity)

    def make_operation_instance(
        self,
        operation_descriptor_container: AbstractOperationDescriptorProtocol,
        operation_cls_getter: OperationClassGetter,
    ) -> OperationDefinitionBase | None:
        """Create operation handlers.

        Handle codes MDC_OP_SET_TIME_SYNC_REF_SRC, MDC_ACT_SET_TIME_ZONE.
        """
        if operation_descriptor_container.coding == self.MDC_OP_SET_TIME_SYNC_REF_SRC.coding:
            self._logger.debug(
                'instantiating "set ntp server" operation from existing descriptor handle=%s',
                operation_descriptor_container.Handle,
            )
            set_ntp_operation = self._mk_operation_from_operation_descriptor(
                operation_descriptor_container,
                operation_cls_getter,
                operation_handler=self._set_ntp_string,
            )
            self._set_ntp_operations.append(set_ntp_operation)
            return set_ntp_operation
        if operation_descriptor_container.coding == self.MDC_ACT_SET_TIME_ZONE.coding:
            self._logger.debug(
                'instantiating "set time zone" operation from existing descriptor handle=%s',
                operation_descriptor_container.Handle,
            )
            set_tz_operation = self._mk_operation_from_operation_descriptor(
                operation_descriptor_container,
                operation_cls_getter,
                operation_handler=self._set_tz_string,
            )
            self._set_tz_operations.append(set_tz_operation)
            return set_tz_operation
        return None

    def _set_ntp_string(self, params: ExecuteParameters) -> ExecuteResult:
        """Set the ReferenceSource value of clock state (ExecuteHandler)."""
        value = params.operation_request.argument
        pm_names = self._mdib.data_model.pm_names
        self._logger.info(
            'set value %s from %s to %s',
            params.operation_instance.operation_target_handle,
            params.operation_instance.current_value,
            value,
        )

        op_target_entity = self._mdib.entities.by_handle(params.operation_instance.operation_target_handle)

        # look for clock entities that are a direct child of this mds
        mds_handle = op_target_entity.descriptor.source_mds or op_target_entity.handle
        clock_entities = self._mdib.entities.by_node_type(pm_names.ClockDescriptor)
        clock_entities = [c for c in clock_entities if c.parent_handle == mds_handle]

        if len(clock_entities) == 0:
            self._logger.warning('_set_ntp_string: no clock entity found')
            return ExecuteResult(
                params.operation_instance.operation_target_handle,
                self._mdib.data_model.msg_types.InvocationState.FAILED,
            )

        clock_entities[0].state.ReferenceSource = [value]
        with self._mdib.component_state_transaction() as mgr:
            mgr.write_entity(clock_entities[0])
        return ExecuteResult(
            params.operation_instance.operation_target_handle,
            self._mdib.data_model.msg_types.InvocationState.FINISHED,
        )

    def _set_tz_string(self, params: ExecuteParameters) -> ExecuteResult:
        """Set the TimeZone value of clock state (ExecuteHandler)."""
        value = params.operation_request.argument
        pm_names = self._mdib.data_model.pm_names
        self._logger.info(
            'set value %s from %s to %s',
            params.operation_instance.operation_target_handle,
            params.operation_instance.current_value,
            value,
        )

        op_target_entity = self._mdib.entities.by_handle(params.operation_instance.operation_target_handle)

        # look for clock entities that are a direct child of this mds
        mds_handle = op_target_entity.descriptor.source_mds or op_target_entity.handle
        clock_entities = self._mdib.entities.by_node_type(pm_names.ClockDescriptor)
        clock_entities = [c for c in clock_entities if c.parent_handle == mds_handle]

        if len(clock_entities) == 0:
            self._logger.warning('_set_ntp_string: no clock entity found')
            return ExecuteResult(
                params.operation_instance.operation_target_handle,
                self._mdib.data_model.msg_types.InvocationState.FAILED,
            )

        clock_entities[0].state.TimeZone = value
        with self._mdib.component_state_transaction() as mgr:
            mgr.write_entity(clock_entities[0])
        return ExecuteResult(
            params.operation_instance.operation_target_handle,
            self._mdib.data_model.msg_types.InvocationState.FINISHED,
        )
