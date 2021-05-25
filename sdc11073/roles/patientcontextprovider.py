from .. import namespaces
from .contextprovider import GenericContextProvider

class GenericPatientContextProvider(GenericContextProvider):

    def __init__(self, log_prefix):
        super().__init__(log_prefix)
        self._patientContextDescriptorContainer = None
        self._setPatientContextOperations = []

    def initOperations(self, mdib):
        super().initOperations(mdib)
        # expecting exactly one PatientContextDescriptor
        patientContextDescriptorContainers = self._mdib.descriptions.NODETYPE.get(namespaces.domTag('PatientContextDescriptor'))
        if patientContextDescriptorContainers is not None and len(patientContextDescriptorContainers) == 1:
            self._patientContextDescriptorContainer = patientContextDescriptorContainers[0]

    def makeOperationInstance(self, operationDescriptorContainer, operations_factory):
        if self._patientContextDescriptorContainer and operationDescriptorContainer.OperationTarget == self._patientContextDescriptorContainer.handle:
            pc_operation = self._mkOperationFromOperationDescriptor(operationDescriptorContainer,
                                                                    operations_factory,
                                                                    currentArgumentHandler=self._setContextState)
            self._setPatientContextOperations.append(pc_operation)
            return pc_operation

    def makeMissingOperations(self, operations_factory):
        ops = []
        if self._patientContextDescriptorContainer and not self._setPatientContextOperations:
            set_context_state_op_cls = operations_factory(namespaces.domTag('SetContextStateOperationDescriptor'))

            pc_operation = self._mkOperation(set_context_state_op_cls,
                                             handle='opSetPatCtx',
                                             operationTargetHandle=self._patientContextDescriptorContainer.handle,
                                             codedValue=None,
                                             currentArgumentHandler=self._setContextState)
            ops.append(pc_operation)
        return ops
