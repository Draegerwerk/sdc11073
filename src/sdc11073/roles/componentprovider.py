"""Implementation of component provider functionality."""
from __future__ import annotations

from typing import TYPE_CHECKING

from sdc11073.provider.operations import ExecuteResult

from . import providerbase

if TYPE_CHECKING:
    from sdc11073.mdib.descriptorcontainers import AbstractOperationDescriptorProtocol
    from sdc11073.provider.operations import ExecuteParameters, OperationDefinitionBase

    from .providerbase import OperationClassGetter


class GenericSetComponentStateOperationProvider(providerbase.ProviderRole):
    """Class is responsible for SetComponentState operations."""

    def make_operation_instance(self,
                                operation_descriptor_container: AbstractOperationDescriptorProtocol,
                                operation_cls_getter: OperationClassGetter) -> OperationDefinitionBase | None:
        """Return an operation definition instance for this operation or None.

        Can handle following case:
        operation_descriptor_container is a SetComponentStateOperationDescriptor and
        target is any AbstractComponentDescriptor.
        """
        pm_names = self._mdib.data_model.pm_names
        operation_target_handle = operation_descriptor_container.OperationTarget
        op_target_entity = self._mdib.entities.by_handle(operation_target_handle)

        if operation_descriptor_container.NODETYPE == pm_names.SetComponentStateOperationDescriptor:  # noqa: SIM300
            if op_target_entity.node_type in (pm_names.MdsDescriptor,
                                              pm_names.ChannelDescriptor,
                                              pm_names.VmdDescriptor,
                                              pm_names.ClockDescriptor,
                                              pm_names.ScoDescriptor,
                                              ):
                op_cls = operation_cls_getter(pm_names.SetComponentStateOperationDescriptor)
                return op_cls(operation_descriptor_container.Handle,
                              operation_target_handle,
                              self._set_component_state,
                              coded_value=operation_descriptor_container.Type)
        elif operation_descriptor_container.NODETYPE == pm_names.ActivateOperationDescriptor:  # noqa: SIM300
            #  on what can activate be called?
            if op_target_entity.node_type in (pm_names.MdsDescriptor,
                                              pm_names.ChannelDescriptor,
                                              pm_names.VmdDescriptor,
                                              pm_names.ScoDescriptor,
                                              ):
                # no generic handler to be called!
                op_cls = operation_cls_getter(pm_names.ActivateOperationDescriptor)
                return op_cls(operation_descriptor_container.Handle,
                              operation_target_handle,
                              self._do_nothing,
                              coded_value=operation_descriptor_container.Type)
        return None

    def _set_component_state(self, params: ExecuteParameters) -> ExecuteResult:
        """Handle SetComponentState operation (ExecuteHandler)."""
        value = params.operation_request.argument
        params.operation_instance.current_value = value
        with self._mdib.component_state_transaction() as mgr:
            for proposed_state in value:
                entity = self._mdib.entities.by_handle(proposed_state.DescriptorHandle)
                if entity.state.is_component_state:
                    self._logger.info('updating %s with proposed component state', entity.state)
                    entity.state.update_from_other_container(
                        proposed_state, skipped_properties=['StateVersion', 'DescriptorVersion'])
                    mgr.write_entity(entity)
                else:
                    self._logger.warning(
                        '_set_component_state operation: ignore invalid referenced type %s in operation',
                        entity.node_type.localname)
        return ExecuteResult(params.operation_instance.operation_target_handle,
                             self._mdib.data_model.msg_types.InvocationState.FINISHED)

    def _do_nothing(self, params: ExecuteParameters) -> ExecuteResult:
        return ExecuteResult(params.operation_instance.operation_target_handle,
                             self._mdib.data_model.msg_types.InvocationState.FINISHED)
