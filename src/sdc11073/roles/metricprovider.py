from __future__ import annotations

from typing import TYPE_CHECKING, cast

from sdc11073.mdib.statecontainers import AbstractMetricStateContainer, MetricStateProtocol
from sdc11073.provider.operations import ExecuteResult
from sdc11073.xml_types.pm_types import ComponentActivation

from .providerbase import ProviderRole

if TYPE_CHECKING:
    from collections.abc import Iterable

    from sdc11073.mdib import ProviderMdib
    from sdc11073.mdib.descriptorcontainers import AbstractOperationDescriptorProtocol
    from sdc11073.mdib.transactionsprotocol import TransactionManagerProtocol, TransactionItem
    from sdc11073.provider.operations import ExecuteParameters, OperationDefinitionBase

    from .providerbase import OperationClassGetter


class GenericMetricProvider(ProviderRole):
    """Generic Handler.

    This is a generic Handler for
    - SetValueOperation on numeric metrics
    - SetStringOperation on (enum) string metrics
    """

    def __init__(self, mdib: ProviderMdib,
                 activation_state_can_remove_metric_value: bool = True,
                 log_prefix: str | None = None):
        """Create a GenericMetricProvider."""
        super().__init__(mdib, log_prefix)
        self.activation_state_can_remove_metric_value = activation_state_can_remove_metric_value

    def make_operation_instance(self,
                                operation_descriptor_container: AbstractOperationDescriptorProtocol,
                                operation_cls_getter: OperationClassGetter) -> OperationDefinitionBase | None:
        """Create an OperationDefinition for SetContextStateOperationDescriptor.

        Handle following cases:
        SetValueOperation, target = NumericMetricDescriptor:
          => handler = _set_numeric_value
        SetStringOperation, target = (Enum)StringMetricDescriptor:
          => handler = _set_string
        SetMetricStateOperationDescriptor, target = any subclass of AbstractMetricDescriptor:
          => handler = _set_metric_state
        """
        pm_names = self._mdib.data_model.pm_names
        operation_target_handle = operation_descriptor_container.OperationTarget
        op_target_descriptor_container = self._mdib.descriptions.handle.get_one(operation_target_handle)

        if operation_descriptor_container.NODETYPE == pm_names.SetValueOperationDescriptor:  # noqa: SIM300
            if op_target_descriptor_container.NODETYPE == pm_names.NumericMetricDescriptor:  # noqa: SIM300
                op_cls = operation_cls_getter(pm_names.SetValueOperationDescriptor)
                return op_cls(operation_descriptor_container.Handle,
                              operation_target_handle,
                              self._set_numeric_value,
                              coded_value=operation_descriptor_container.Type)
            return None
        if operation_descriptor_container.NODETYPE == pm_names.SetStringOperationDescriptor:  # noqa: SIM300
            if op_target_descriptor_container.NODETYPE in (pm_names.StringMetricDescriptor,
                                                           pm_names.EnumStringMetricDescriptor):
                op_cls = operation_cls_getter(pm_names.SetStringOperationDescriptor)
                return op_cls(operation_descriptor_container.Handle,
                              operation_target_handle,
                              self._set_string,
                              coded_value=operation_descriptor_container.Type)
            return None
        if operation_descriptor_container.NODETYPE == pm_names.SetMetricStateOperationDescriptor:  # noqa: SIM300
            op_cls = operation_cls_getter(pm_names.SetMetricStateOperationDescriptor)
            return op_cls(operation_descriptor_container.Handle,
                          operation_target_handle,
                          self._set_metric_state,
                          coded_value=operation_descriptor_container.Type)
        return None

    def _set_metric_state(self, params: ExecuteParameters) -> ExecuteResult:
        """Handle SetMetricState calls (ExecuteHandler)."""
        # ToDo: consider ModifiableDate attribute
        proposed_states = params.operation_request.argument
        params.operation_instance.current_value = proposed_states
        with self._mdib.metric_state_transaction() as mgr:
            for proposed_state in proposed_states:
                state = mgr.get_state(proposed_state.DescriptorHandle)
                if state.is_metric_state:
                    self._logger.info('updating %s with proposed metric state', state)
                    state.update_from_other_container(proposed_state,
                                                      skipped_properties=['StateVersion', 'DescriptorVersion'])
                else:
                    self._logger.warning('_set_metric_state operation: ignore invalid referenced type %s in operation',
                                         state.NODETYPE)
        return ExecuteResult(params.operation_instance.operation_target_handle,
                             self._mdib.data_model.msg_types.InvocationState.FINISHED)

    def on_pre_commit(self, mdib: ProviderMdib,  # noqa: ARG002
                      transaction: TransactionManagerProtocol):
        """Set state.MetricValue to None if state.ActivationState requires this."""
        if not self.activation_state_can_remove_metric_value:
            return
        if transaction.metric_state_updates:
            self._handle_metrics_component_activation(transaction.metric_state_updates.values())
        if transaction.rt_sample_state_updates:
            self._handle_metrics_component_activation(transaction.rt_sample_state_updates.values())

    def _handle_metrics_component_activation(self, metric_state_updates: Iterable[TransactionItem]):
        """Check if MetricValue shall be removed."""
        for tr_item in metric_state_updates:
            new_state = cast(AbstractMetricStateContainer, tr_item.new)
            if new_state is None or not new_state.is_metric_state:
                continue
            # SF717: check if MetricValue shall be automatically removed
            if new_state.ActivationState in (ComponentActivation.OFF,
                                             ComponentActivation.SHUTDOWN,
                                             ComponentActivation.FAILURE):
                if new_state.MetricValue is not None:
                    # remove metric value
                    self._logger.info('%s: remove metric value because ActivationState="%s", handle="%s"',
                                      self.__class__.__name__, new_state.ActivationState, new_state.DescriptorHandle)
                    new_state.MetricValue = None

    def _set_numeric_value(self, params: ExecuteParameters) -> ExecuteResult:
        """Set a numerical metric value (ExecuteHandler)."""
        value = params.operation_request.argument
        pm_types = self._mdib.data_model.pm_types
        self._logger.info('set value of %s via %s from %r to %r',
                          params.operation_instance.operation_target_handle,
                          params.operation_instance.handle,
                          params.operation_instance.current_value, value)
        params.operation_instance.current_value = value
        with self._mdib.metric_state_transaction() as mgr:
            _state = mgr.get_state(params.operation_instance.operation_target_handle)
            state = cast(MetricStateProtocol, _state)
            if state.MetricValue is None:
                state.mk_metric_value()
            state.MetricValue.Value = value
            # SF1823: For Metrics with the MetricCategory = Set|Preset that are being modified as a result of a
            # SetValue or SetString operation a Metric Provider shall set the MetricQuality / Validity = Vld.
            metric_descriptor_container = self._mdib.descriptions.handle.get_one(
                params.operation_instance.operation_target_handle)
            if metric_descriptor_container.MetricCategory in (pm_types.MetricCategory.SETTING,
                                                              pm_types.MetricCategory.PRESETTING):
                state.MetricValue.Validity = pm_types.MeasurementValidity.VALID
        return ExecuteResult(params.operation_instance.operation_target_handle,
                             self._mdib.data_model.msg_types.InvocationState.FINISHED)

    def _set_string(self, params: ExecuteParameters) -> ExecuteResult:
        """Set a string value (ExecuteHandler)."""
        value = params.operation_request.argument
        self._logger.info('set value %s from %s to %s',
                          params.operation_instance.operation_target_handle,
                          params.operation_instance.current_value, value)
        params.operation_instance.current_value = value
        with self._mdib.metric_state_transaction() as mgr:
            _state = mgr.get_state(params.operation_instance.operation_target_handle)
            state = cast(MetricStateProtocol, _state)
            if state.MetricValue is None:
                state.mk_metric_value()
            state.MetricValue.Value = value
        return ExecuteResult(params.operation_instance.operation_target_handle,
                             self._mdib.data_model.msg_types.InvocationState.FINISHED)
