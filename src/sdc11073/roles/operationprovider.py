from __future__ import annotations

import decimal

from sdc11073 import xml_utils
from sdc11073.mdib.descriptorcontainers import AbstractOperationDescriptorProtocol, ActivateOperationDescriptorContainer
from sdc11073.provider.operations import OperationDefinitionBase, ActivateOperation, ExecuteResult, ExecuteParameters
from sdc11073.provider.sco import AbstractScoOperationsRegistry
from sdc11073.roles import providerbase
from sdc11073.roles.providerbase import OperationClassGetter
from sdc11073.xml_types import msg_types
from sdc11073.xml_types.pm_types import ActivateOperationDescriptorArgument


class OperationProvider(providerbase.ProviderRole):
    """Handle operations that work on operation states.

     Empty implementation, not needed/used for sdc11073 tests.
     """

    def make_missing_operations(self, sco: AbstractScoOperationsRegistry) -> list[OperationDefinitionBase]:
        return super().make_missing_operations(sco)

    def _handle_plugathon_activate(self, params: ExecuteParameters) -> ExecuteResult:
        descriptor: ActivateOperationDescriptorContainer = params.operation_instance.descriptor_container
        if len(descriptor.Argument) != len(params.operation_request.argument):
            raise ValueError(f'Expected {len(descriptor.Argument)} arguments, got {len(params.operation_request.argument)}')

        for description, value in zip(descriptor.Argument, params.operation_request.argument, strict=True):
            description: ActivateOperationDescriptorArgument
            value: msg_types.Argument
            # these are types from the plug-a-thon. add more if needed
            if description.Arg == xml_utils.QName('{http://www.w3.org/2001/XMLSchema}string'):
                pass  # value is already a string. no need to check for correct type
            elif description.Arg == xml_utils.QName('{http://www.w3.org/2001/XMLSchema}decimal'):
                float(value.ArgValue)  # just check if its a valid decimal
            elif description.Arg == xml_utils.QName('{http://www.w3.org/2001/XMLSchema}anyURI'):
                pass  # TODO: check for type
            else:
                raise NotImplementedError(f'{description.Arg} is not implemented')

        # metric update has same mdib version as operation result. forbidden by sdpi
        with self._mdib.metric_state_transaction() as mgr:
            state = mgr.get_state(descriptor.OperationTarget)
            if not state.MetricValue:
                state.mk_metric_value()
            state.MetricValue.Value = ''.join(arg.ArgValue for arg in params.operation_request.argument)  # required for pat test 6f
        return ExecuteResult(
            params.operation_instance.operation_target_handle, self._mdib.data_model.msg_types.InvocationState.FINISHED,
        )
    def make_operation_instance(
            self,
            operation_descriptor_container: AbstractOperationDescriptorProtocol,
            operation_cls_getter: OperationClassGetter) -> OperationDefinitionBase | None:
        if operation_descriptor_container.Handle == 'activate_1.sco.mds_0':
            cls: type[ActivateOperation] = operation_cls_getter(operation_descriptor_container.NODETYPE)
            operation = cls(operation_descriptor_container.Handle,
                operation_descriptor_container.OperationTarget,
                operation_handler=self._handle_plugathon_activate)
            return operation
        super().make_operation_instance(operation_descriptor_container, operation_cls_getter)
