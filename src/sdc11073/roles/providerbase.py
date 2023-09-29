from __future__ import annotations

from typing import TYPE_CHECKING, Callable, cast

from lxml.etree import QName

from sdc11073 import loghelper
from sdc11073.mdib.statecontainers import MetricStateProtocol
from sdc11073.provider.operations import ExecuteResult, OperationDefinitionBase

if TYPE_CHECKING:
    from sdc11073.mdib import ProviderMdib
    from sdc11073.mdib.descriptorcontainers import AbstractDescriptorProtocol, AbstractOperationDescriptorProtocol
    from sdc11073.mdib.transactions import TransactionManagerProtocol
    from sdc11073.provider.operations import ExecuteHandler, TimeoutHandler
    from sdc11073.provider.sco import AbstractScoOperationsRegistry
    from sdc11073.xml_types.pm_types import CodedValue, SafetyClassification
    from sdc11073.provider.operations import ExecuteParameters

OperationClassGetter = Callable[[QName], type[OperationDefinitionBase]]


class ProviderRole:
    """Base class for all role implementations."""

    def __init__(self, mdib: ProviderMdib, log_prefix: str):
        self._mdib = mdib
        self._logger = loghelper.get_logger_adapter(f'sdc.device.{self.__class__.__name__}', log_prefix)

    def stop(self):
        """Stop whatever needs to be stopped.

        Implement method in derived class if needed.
        """

    def init_operations(self, sco: AbstractScoOperationsRegistry):
        """Initialize and start whatever the provider needs.

        Implement method in derived class if needed.
        """

    def make_operation_instance(
            self,
            operation_descriptor_container: AbstractOperationDescriptorProtocol,  # noqa: ARG002
            operation_cls_getter: OperationClassGetter) -> OperationDefinitionBase | None:  # noqa: ARG002
        """Return an operation definition instance for this operation or None.

        If a mdib already has operations defined, this method can connect a handler to a given operation descriptor.
        Use case: initialization from an existing mdib.
        """
        return None

    def make_missing_operations(self, sco: AbstractScoOperationsRegistry) -> list[  # noqa: ARG002
        OperationDefinitionBase]:
        """Create operations that this role provider needs.

        This method is called after all existing operations from mdib have been registered.
        If a role provider needs to add operations beyond that, it can do it here.
        """
        return []

    def on_pre_commit(self, mdib: ProviderMdib, transaction: TransactionManagerProtocol):
        """Manipulate the transaction if needed.

        Derived classes can overwrite this method.
        """

    def on_post_commit(self, mdib: ProviderMdib, transaction: TransactionManagerProtocol):
        """Run stuff after transaction.

        Derived classes can overwrite this method.
        """

    def _set_numeric_value(self, params: ExecuteParameters) -> ExecuteResult:
        """Set a numerical metric value (ExecuteHandler)."""
        value = params.operation_request.argument
        pm_types = self._mdib.data_model.pm_types
        self._logger.info('set value of %s via %s from %r to %r',
                          params.operation_instance.operation_target_handle,
                          params.operation_instance.handle,
                          params.operation_instance.current_value, value)
        params.operation_instance.current_value = value
        with self._mdib.transaction_manager() as mgr:
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
        pm_types = self._mdib.data_model.pm_types
        self._logger.info('set value %s from %s to %s',
                          params.operation_instance.operation_target_handle,
                          params.operation_instance.current_value, value)
        params.operation_instance.current_value = value
        with self._mdib.transaction_manager() as mgr:
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

    def _mk_operation_from_operation_descriptor(self,
                                                operation_descriptor_container: AbstractOperationDescriptorProtocol,
                                                operation_cls_getter: OperationClassGetter,
                                                operation_handler: ExecuteHandler,
                                                timeout_handler: TimeoutHandler | None = None) \
            -> OperationDefinitionBase:
        """Create an OperationDefinition for the operation_descriptor_container."""
        op_cls = operation_cls_getter(operation_descriptor_container.NODETYPE)
        return op_cls(operation_descriptor_container.Handle,
                      operation_descriptor_container.OperationTarget,
                      operation_handler=operation_handler,
                      timeout_handler=timeout_handler,
                      coded_value=operation_descriptor_container.Type)

    @staticmethod
    def _create_descriptor_container(container_cls: type[AbstractDescriptorProtocol],
                                     handle: str,
                                     parent_handle: str,
                                     coded_value: CodedValue,
                                     safety_classification: SafetyClassification) -> AbstractDescriptorProtocol:
        obj = container_cls(handle=handle, parent_handle=parent_handle)
        obj.SafetyClassification = safety_classification
        obj.Type = coded_value
        return obj
