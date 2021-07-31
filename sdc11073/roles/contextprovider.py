import time
from .. import namespaces
from ..pmtypes import ContextAssociation
from . import providerbase


class GenericContextProvider(providerbase.ProviderRole):
    """ Handles SetContextState operations"""
    def __init__(self, opTargetDescriptorTypes=None, forcedNewStateType=None, log_prefix=None):
        super().__init__(log_prefix)
        self._opTargetDescrTypes = opTargetDescriptorTypes
        self._forcedNewStateType = forcedNewStateType

    def makeOperationInstance(self, operationDescriptorContainer, operations_factory):
        """Create a handler for SetContextStateOperationDescriptor if type of operation target
        matches opTargetDescriptorTypes"""
        if operationDescriptorContainer.NODETYPE == namespaces.domTag('SetContextStateOperationDescriptor'):
            opTargetDescrContainer = self._mdib.descriptions.handle.getOne(operationDescriptorContainer.OperationTarget)
            if (not self._opTargetDescrTypes) or (opTargetDescrContainer.NODETYPE not in self._opTargetDescrTypes):
                return # we do not handle this target type
            else:
                pc_operation = self._mkOperationFromOperationDescriptor(operationDescriptorContainer,
                                                                        operations_factory,
                                                                        currentArgumentHandler=self._setContextState)
                return pc_operation

    def _setContextState(self, operationInstance, proposedContextStates):
        ''' This is the code that executes the operation itself.
        '''
        with self._mdib.mdibUpdateTransaction() as tr:
            for proposed_st in proposedContextStates:
                oldContextStateContainer = None
                if proposed_st.descriptorHandle != proposed_st.Handle:
                    # this is an update for an existing state
                    oldContextStateContainer = operationInstance.operationTargetStorage.handle.getOne(
                        proposed_st.Handle, allowNone=True)
                    if oldContextStateContainer is None:
                        raise ValueError('handle {} not found'.format(proposed_st.Handle))
                if oldContextStateContainer is None:
                    # this is a new context state
                    # create a new unique handle
                    handleString = '{}_{}'.format(proposed_st.descriptorHandle, self._mdib.mdib_version)
                    proposed_st.Handle = handleString
                    proposed_st.BindingMdibVersion = self._mdib.mdib_version
                    proposed_st.BindingStartTime = time.time()
                    proposed_st.ContextAssociation = ContextAssociation.ASSOCIATED
                    proposed_st.set_node_member()
                    self._logger.info('new {}, handle={}', proposed_st.NODETYPE.localname, proposed_st.Handle)
                    tr.add_state(proposed_st)

                    # find all associated context states, disassociate them, set unbinding info, and add them to updates
                    oldContextStateContainers = operationInstance.operationTargetStorage.descriptorHandle.get(proposed_st.descriptorHandle, [])
                    for old_st in oldContextStateContainers:
                        if old_st.ContextAssociation != ContextAssociation.DISASSOCIATED or old_st.UnbindingMdibVersion is None:
                            new_st = tr.get_state(old_st.descriptorHandle, old_st.Handle)
                            new_st.ContextAssociation = ContextAssociation.DISASSOCIATED
                            if new_st.UnbindingMdibVersion is None:
                                new_st.UnbindingMdibVersion = self._mdib.mdib_version
                                new_st.BindingEndTime = time.time()
                else:
                    # this is an update to an existing patient
                    # use "regular" way to update via transaction manager
                    self._logger.info('update {}, handle={}', proposed_st.NODETYPE.localname, proposed_st.Handle)
                    tmp = tr.get_state(proposed_st.descriptorHandle, proposed_st.Handle)
                    tmp.update_from_other_container(proposed_st, skipped_properties=['ContextAssociation',
                                                                                     'BindingMdibVersion',
                                                                                     'UnbindingMdibVersion',
                                                                                     'BindingStartTime',
                                                                                     'BindingEndTime',
                                                                                     'StateVersion'])


class EnsembleContextProvider(GenericContextProvider):
    def __init__(self, log_prefix):
        super().__init__(opTargetDescriptorTypes=[namespaces.domTag('EnsembleContextDescriptor')], log_prefix=log_prefix)


class LocationContextProvider(GenericContextProvider):
    def __init__(self, log_prefix):
        super().__init__(opTargetDescriptorTypes=[namespaces.domTag('LocationContextDescriptor')], log_prefix=log_prefix)
