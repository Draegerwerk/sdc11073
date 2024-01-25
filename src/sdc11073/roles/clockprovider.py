from __future__ import annotations

from typing import TYPE_CHECKING

from sdc11073.provider.operations import ExecuteResult

from .nomenclature import NomenclatureCodes
from .providerbase import OperationClassGetter, ProviderRole

if TYPE_CHECKING:
    from sdc11073.mdib.descriptorcontainers import AbstractDescriptorProtocol, AbstractOperationDescriptorProtocol
    from sdc11073.mdib.providermdib import ProviderMdib
    from sdc11073.provider.operations import ExecuteParameters, OperationDefinitionBase
    from sdc11073.provider.sco import AbstractScoOperationsRegistry
    from sdc11073.xml_types.pm_types import CodedValue, SafetyClassification


class GenericSDCClockProvider(ProviderRole):
    """Handles operations for setting ntp server and time zone.

    This provider handles SetString operations with codes
    "MDC_OP_SET_TIME_SYNC_REF_SRC" and "MDC_ACT_SET_TIME_ZONE".
    Nothing is added to the mdib. If the mdib does not contain these operations, the functionality is not available.
    """

    def __init__(self, mdib: ProviderMdib, log_prefix: str):
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
        clock_descriptor = self._mdib.descriptions.NODETYPE.get_one(pm_names.ClockDescriptor,
                                                                    allow_none=True)
        if clock_descriptor is None:
            mds_container = self._mdib.descriptions.NODETYPE.get_one(pm_names.MdsDescriptor)
            clock_descr_handle = 'clock_' + mds_container.Handle
            self._logger.debug('creating a clock descriptor, handle=%s', clock_descr_handle)
            clock_descriptor = self._create_clock_descriptor_container(
                handle=clock_descr_handle,
                parent_handle=mds_container.Handle,
                coded_value=pm_types.CodedValue('123'),
                safety_classification=pm_types.SafetyClassification.INF)
            self._mdib.descriptions.add_object(clock_descriptor)
        clock_state = self._mdib.states.descriptor_handle.get_one(clock_descriptor.Handle, allow_none=True)
        if clock_state is None:
            clock_state = self._mdib.data_model.mk_state_container(clock_descriptor)
            self._mdib.states.add_object(clock_state)

    def make_operation_instance(self,
                                operation_descriptor_container: AbstractOperationDescriptorProtocol,
                                operation_cls_getter: OperationClassGetter) -> OperationDefinitionBase | None:
        """Create operation handlers.

        Handle codes MDC_OP_SET_TIME_SYNC_REF_SRC, MDC_ACT_SET_TIME_ZONE.
        """
        if operation_descriptor_container.coding == self.MDC_OP_SET_TIME_SYNC_REF_SRC.coding:
            self._logger.debug('instantiating "set ntp server" operation from existing descriptor handle=%s',
                               operation_descriptor_container.Handle)
            set_ntp_operation = self._mk_operation_from_operation_descriptor(operation_descriptor_container,
                                                                             operation_cls_getter,
                                                                             operation_handler=self._set_ntp_string)
            self._set_ntp_operations.append(set_ntp_operation)
            return set_ntp_operation
        if operation_descriptor_container.coding == self.MDC_ACT_SET_TIME_ZONE.coding:
            self._logger.debug('instantiating "set time zone" operation from existing descriptor handle=%s',
                               operation_descriptor_container.Handle)
            set_tz_operation = self._mk_operation_from_operation_descriptor(operation_descriptor_container,
                                                                            operation_cls_getter,
                                                                            operation_handler=self._set_tz_string)
            self._set_tz_operations.append(set_tz_operation)
            return set_tz_operation
        return None

    def _set_ntp_string(self, params: ExecuteParameters) -> ExecuteResult:
        """Set the ReferenceSource value of clock state (ExecuteHandler)."""
        value = params.operation_request.argument
        pm_names = self._mdib.data_model.pm_names
        self._logger.info('set value %s from %s to %s',
                          params.operation_instance.operation_target_handle,
                          params.operation_instance.current_value, value)
        with self._mdib.component_state_transaction() as mgr:
            state = mgr.get_state(params.operation_instance.operation_target_handle)
            if pm_names.MdsState == state.NODETYPE:
                mds_handle = state.DescriptorHandle
                mgr.unget_state(state)
                # look for the ClockState child
                clock_descriptors = self._mdib.descriptions.NODETYPE.get(pm_names.ClockDescriptor, [])
                clock_descriptors = [c for c in clock_descriptors if c.parent_handle == mds_handle]
                if len(clock_descriptors) == 1:
                    state = mgr.get_state(clock_descriptors[0].handle)
            if pm_names.ClockState != state.NODETYPE:
                raise ValueError(f'_set_ntp_string: expected ClockState, got {state.NODETYPE.localname}')
            state.ReferenceSource = [value]
        return ExecuteResult(params.operation_instance.operation_target_handle,
                             self._mdib.data_model.msg_types.InvocationState.FINISHED)

    def _set_tz_string(self, params: ExecuteParameters) -> ExecuteResult:
        """Set the TimeZone value of clock state (ExecuteHandler)."""
        value = params.operation_request.argument
        pm_names = self._mdib.data_model.pm_names
        self._logger.info('set value %s from %s to %s',
                          params.operation_instance.operation_target_handle,
                          params.operation_instance.current_value, value)
        with self._mdib.component_state_transaction() as mgr:
            state = mgr.get_state(params.operation_instance.operation_target_handle)
            if pm_names.MdsState == state.NODETYPE:
                mds_handle = state.DescriptorHandle
                mgr.unget_state(state)
                # look for the ClockState child
                clock_descriptors = self._mdib.descriptions.NODETYPE.get(pm_names.ClockDescriptor, [])
                clock_descriptors = [c for c in clock_descriptors if c.parent_handle == mds_handle]
                if len(clock_descriptors) == 1:
                    state = mgr.get_state(clock_descriptors[0].handle)

            if pm_names.ClockState != state.NODETYPE:
                raise ValueError(f'_set_ntp_string: expected ClockState, got {state.NODETYPE.localname}')
            state.TimeZone = value
        return ExecuteResult(params.operation_instance.operation_target_handle,
                             self._mdib.data_model.msg_types.InvocationState.FINISHED)

    def _create_clock_descriptor_container(self, handle: str,
                                           parent_handle: str,
                                           coded_value: CodedValue,
                                           safety_classification: SafetyClassification) -> AbstractDescriptorProtocol:
        """Create a ClockDescriptorContainer with the given properties.

        :param handle: Handle of the new container
        :param parent_handle: Handle of the parent
        :param coded_value: a pmtypes.CodedValue instance that defines what this onject represents in medical terms.
        :param safety_classification: a pmtypes.SafetyClassification value
        :return: the created object
        """
        model = self._mdib.data_model
        cls = model.get_descriptor_container_class(model.pm_names.ClockDescriptor)
        return self._create_descriptor_container(cls, handle, parent_handle, coded_value, safety_classification)


class SDCClockProvider(GenericSDCClockProvider):
    """SDCClockProvider adds SetString operations to set ntp server and time zone if they do not exist.

    This provider guarantees that there are SetString operations with codes "MDC_OP_SET_TIME_SYNC_REF_SRC"
    and "MDC_ACT_SET_TIME_ZONE" if mdib contains a ClockDescriptor. It adds them to mdib if they do not exist.

    """

    def make_missing_operations(self, sco: AbstractScoOperationsRegistry) -> list[OperationDefinitionBase]:
        """Add operations to mdib if mdib contains a ClockDescriptor, but not the operations."""
        pm_names = self._mdib.data_model.pm_names
        ops = []
        operation_cls_getter = sco.operation_cls_getter

        mds_container = self._mdib.descriptions.NODETYPE.get_one(pm_names.MdsDescriptor)
        clock_descriptor = self._mdib.descriptions.NODETYPE.get_one(pm_names.ClockDescriptor,
                                                                    allow_none=True)
        if clock_descriptor is None:
            # there is no clock element in mdib,
            return ops
        set_string_op_cls = operation_cls_getter(pm_names.SetStringOperationDescriptor)

        if not self._set_ntp_operations:
            self._logger.debug('adding "set ntp server" operation, code = %r',
                               NomenclatureCodes.MDC_OP_SET_TIME_SYNC_REF_SRC)
            set_ntp_operation = set_string_op_cls('SET_NTP_SRV_' + mds_container.handle,
                                                  clock_descriptor.handle,
                                                  self._set_ntp_string,
                                                  coded_value=self.MDC_OP_SET_TIME_SYNC_REF_SRC)
            self._set_ntp_operations.append(set_ntp_operation)
            ops.append(set_ntp_operation)
        if not self._set_tz_operations:
            self._logger.debug('adding "set time zone" operation, code = %r',
                               NomenclatureCodes.MDC_ACT_SET_TIME_ZONE)
            set_tz_operation = set_string_op_cls('SET_TZONE_' + mds_container.handle,
                                                 clock_descriptor.handle,
                                                 self._set_tz_string,
                                                 coded_value=self.MDC_ACT_SET_TIME_ZONE)
            self._set_tz_operations.append(set_tz_operation)
            ops.append(set_tz_operation)
        return ops
