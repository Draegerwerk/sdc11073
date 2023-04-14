import time

from . import providerbase


class GenericContextProvider(providerbase.ProviderRole):
    """ Handles SetContextState operations"""

    def __init__(self, mdib, op_target_descr_types=None, forced_new_state_typ=None, log_prefix=None):
        super().__init__(mdib, log_prefix)
        self._op_target_descr_types = op_target_descr_types
        self._forced_new_state_type = forced_new_state_typ

    def make_operation_instance(self, operation_descriptor_container, operation_cls_getter):
        """Create a handler for SetContextStateOperationDescriptor if type of operation target
        matches opTargetDescriptorTypes"""
        pm_names = self._mdib.data_model.pm_names
        if operation_descriptor_container.NODETYPE == pm_names.SetContextStateOperationDescriptor:
            op_target_descr_container = self._mdib.descriptions.handle.get_one(
                operation_descriptor_container.OperationTarget)
            if (not self._op_target_descr_types) or (
                    op_target_descr_container.NODETYPE not in self._op_target_descr_types):
                return None  # we do not handle this target type
            return self._mk_operation_from_operation_descriptor(operation_descriptor_container,
                                                                operation_cls_getter,
                                                                current_argument_handler=self._set_context_state)
        return None

    def _set_context_state(self, operation_instance, proposed_context_states):
        """ This is the code that executes the operation itself.
        """
        pm_types = self._mdib.data_model.pm_types
        with self._mdib.transaction_manager() as mgr:
            for proposed_st in proposed_context_states:
                old_state_container = None
                if proposed_st.DescriptorHandle != proposed_st.Handle:
                    # this is an update for an existing state
                    old_state_container = operation_instance.operation_target_storage.handle.get_one(
                        proposed_st.Handle, allow_none=True)
                    if old_state_container is None:
                        raise ValueError(f'handle {proposed_st.Handle} not found')
                if old_state_container is None:
                    # this is a new context state
                    # create a new unique handle
                    handle_string = f'{proposed_st.DescriptorHandle}_{self._mdib.mdib_version}'
                    proposed_st.Handle = handle_string
                    proposed_st.BindingMdibVersion = self._mdib.mdib_version
                    proposed_st.BindingStartTime = time.time()
                    proposed_st.ContextAssociation = pm_types.ContextAssociation.ASSOCIATED
                    self._logger.info('new {}, handle={}', proposed_st.NODETYPE.localname, proposed_st.Handle)
                    mgr.add_state(proposed_st)

                    # find all associated context states, disassociate them, set unbinding info, and add them to updates
                    old_state_containers = operation_instance.operation_target_storage.descriptorHandle.get(
                        proposed_st.DescriptorHandle, [])
                    for old_state in old_state_containers:
                        if old_state.ContextAssociation != pm_types.ContextAssociation.DISASSOCIATED or old_state.UnbindingMdibVersion is None:
                            new_state = mgr.get_context_state(old_state.Handle)
                            new_state.ContextAssociation = pm_types.ContextAssociation.DISASSOCIATED
                            if new_state.UnbindingMdibVersion is None:
                                new_state.UnbindingMdibVersion = self._mdib.mdib_version
                                new_state.BindingEndTime = time.time()
                else:
                    # this is an update to an existing patient
                    # use "regular" way to update via transaction manager
                    self._logger.info('update {}, handle={}', proposed_st.NODETYPE.localname, proposed_st.Handle)
                    tmp = mgr.get_context_state(proposed_st.Handle)
                    tmp.update_from_other_container(proposed_st, skipped_properties=['ContextAssociation',
                                                                                     'BindingMdibVersion',
                                                                                     'UnbindingMdibVersion',
                                                                                     'BindingStartTime',
                                                                                     'BindingEndTime',
                                                                                     'StateVersion'])


class EnsembleContextProvider(GenericContextProvider):
    def __init__(self, mdib, log_prefix):
        super().__init__(mdib,
                         op_target_descr_types=[mdib.data_model.pm_names.EnsembleContextDescriptor],
                         log_prefix=log_prefix)


class LocationContextProvider(GenericContextProvider):
    def __init__(self, mdib, log_prefix):
        super().__init__(mdib,
                         op_target_descr_types=[mdib.data_model.pm_names.LocationContextDescriptor],
                         log_prefix=log_prefix)
