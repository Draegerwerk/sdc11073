from .contextprovider import GenericContextProvider


class GenericPatientContextProvider(GenericContextProvider):

    def __init__(self, mdib, log_prefix):
        super().__init__(mdib, log_prefix)
        self._patient_context_descriptor_container = None
        self._set_patient_context_operations = []

    def init_operations(self, sco):
        super().init_operations(sco)
        # expecting exactly one PatientContextDescriptor
        pm_names = self._mdib.data_model.pm_names
        descriptor_containers = self._mdib.descriptions.NODETYPE.get(pm_names.PatientContextDescriptor)
        if descriptor_containers is not None and len(descriptor_containers) == 1:
            self._patient_context_descriptor_container = descriptor_containers[0]

    def make_operation_instance(self, operation_descriptor_container, operation_cls_getter):
        if self._patient_context_descriptor_container and operation_descriptor_container.OperationTarget == self._patient_context_descriptor_container.Handle:
            pc_operation = self._mk_operation_from_operation_descriptor(operation_descriptor_container,
                                                                        operation_cls_getter,
                                                                        current_argument_handler=self._set_context_state)
            self._set_patient_context_operations.append(pc_operation)
            return pc_operation
        return None


class PatientContextProvider(GenericPatientContextProvider):
    """This Implementation adds operations to mdib if they do not exist."""

    def make_missing_operations(self, sco):
        pm_names = self._mdib.data_model.pm_names
        ops = []
        operation_cls_getter = sco.operation_cls_getter
        if self._patient_context_descriptor_container and not self._set_patient_context_operations:
            set_context_state_op_cls = operation_cls_getter(pm_names.SetContextStateOperationDescriptor)

            pc_operation = self._mk_operation(set_context_state_op_cls,
                                              handle='opSetPatCtx',
                                              operation_target_handle=self._patient_context_descriptor_container.handle,
                                              coded_value=None,
                                              current_argument_handler=self._set_context_state,
                                              timeout_handler=None)
            ops.append(pc_operation)
        return ops
