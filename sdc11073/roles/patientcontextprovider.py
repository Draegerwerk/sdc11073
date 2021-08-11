from .contextprovider import GenericContextProvider
from .. import namespaces


class GenericPatientContextProvider(GenericContextProvider):

    def __init__(self, log_prefix):
        super().__init__(log_prefix)
        self._patient_context_descriptor_container = None
        self._set_patient_context_operations = []

    def init_operations(self, mdib):
        super().init_operations(mdib)
        # expecting exactly one PatientContextDescriptor
        descriptor_containers = self._mdib.descriptions.NODETYPE.get(
            namespaces.domTag('PatientContextDescriptor'))
        if descriptor_containers is not None and len(descriptor_containers) == 1:
            self._patient_context_descriptor_container = descriptor_containers[0]

    def make_operation_instance(self, operation_descriptor_container, operations_factory):
        if self._patient_context_descriptor_container and operation_descriptor_container.OperationTarget == self._patient_context_descriptor_container.handle:
            pc_operation = self._mk_operation_from_operation_descriptor(operation_descriptor_container,
                                                                        operations_factory,
                                                                        current_argument_handler=self._set_context_state)
            self._set_patient_context_operations.append(pc_operation)
            return pc_operation
        return None

    def make_missing_operations(self, operations_factory):
        ops = []
        if self._patient_context_descriptor_container and not self._set_patient_context_operations:
            set_context_state_op_cls = operations_factory(namespaces.domTag('SetContextStateOperationDescriptor'))

            pc_operation = self._mk_operation(set_context_state_op_cls,
                                              handle='opSetPatCtx',
                                              operation_target_handle=self._patient_context_descriptor_container.handle,
                                              coded_value=None,
                                              current_argument_handler=self._set_context_state)
            ops.append(pc_operation)
        return ops
