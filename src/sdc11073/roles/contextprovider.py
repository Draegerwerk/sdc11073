"""Implementation of context provider functionality."""
from __future__ import annotations

import time
import uuid
from collections import defaultdict
from typing import TYPE_CHECKING

from sdc11073.provider.operations import ExecuteResult

from . import providerbase

if TYPE_CHECKING:
    from lxml import etree

    from sdc11073.mdib.descriptorcontainers import AbstractOperationDescriptorProtocol
    from sdc11073.mdib.mdibprotocol import ProviderMdibProtocol
    from sdc11073.provider.operations import ExecuteParameters, OperationDefinitionBase

    from .providerbase import OperationClassGetter


class GenericContextProvider(providerbase.ProviderRole):
    """Handles SetContextState operations."""

    def __init__(self, mdib: ProviderMdibProtocol,
                 op_target_descr_types: list[etree.QName] | None = None,
                 log_prefix: str | None = None):
        super().__init__(mdib, log_prefix)
        self._op_target_descr_types = op_target_descr_types

    def make_operation_instance(self,
                                operation_descriptor_container: AbstractOperationDescriptorProtocol,
                                operation_cls_getter: OperationClassGetter) -> OperationDefinitionBase | None:
        """Create an OperationDefinition for SetContextStateOperationDescriptor.

        Only if type of operation target matches opTargetDescriptorTypes.
        """
        pm_names = self._mdib.data_model.pm_names
        if pm_names.SetContextStateOperationDescriptor == operation_descriptor_container.NODETYPE:
            op_target_entity = self._mdib.entities.by_handle( operation_descriptor_container.OperationTarget)

            if (not self._op_target_descr_types) or (
                    op_target_entity.descriptor.NODETYPE not in self._op_target_descr_types):
                return None  # we do not handle this target type
            return self._mk_operation_from_operation_descriptor(operation_descriptor_container,
                                                                operation_cls_getter,
                                                                operation_handler=self._set_context_state)
        return None

    def _set_context_state(self, params: ExecuteParameters) -> ExecuteResult: # noqa: C901, PLR0912
        """Execute the operation itself (ExecuteHandler).

        If the proposed context is a new context and ContextAssociation == pm_types.ContextAssociation.ASSOCIATED,
        the before associates state(s) will be set to DISASSOCIATED and UnbindingMdibVersion/BindingEndTime
        are set.
        """
        pm_types = self._mdib.data_model.pm_types
        proposed_context_states = params.operation_request.argument

        # check if there is more than one associated state for a context descriptor in proposed_context_states
        proposed_by_handle = defaultdict(list)
        for st in proposed_context_states:
            if st.ContextAssociation == pm_types.ContextAssociation.ASSOCIATED:
                proposed_by_handle[st.DescriptorHandle].append(st)
        for handle, states in proposed_by_handle.items():
            if len(states) > 1:
                msg = f'more than one associated context for descriptor handle {handle}'
                raise ValueError(msg)

        operation_target_handles = []
        modified_state_handles: dict[str, list[str]] = defaultdict(list)
        modified_entities = []
        with self._mdib.context_state_transaction() as mgr:
            for proposed_st in proposed_context_states:
                entity = self._mdib.entities.by_handle(proposed_st.DescriptorHandle)
                modified_entities.append(entity)
                old_state_container = None
                if proposed_st.DescriptorHandle != proposed_st.Handle:
                    # this is an update for an existing state or a new one
                    old_state_container = entity.states.get(proposed_st.Handle)
                    if old_state_container is None:
                        msg = f'handle {proposed_st.Handle} not found'
                        raise ValueError(msg)
                if old_state_container is None:
                    # this is a new context state
                    # create a new unique handle
                    proposed_st.Handle = uuid.uuid4().hex
                    if proposed_st.ContextAssociation == pm_types.ContextAssociation.ASSOCIATED:
                        # disassociate existing states
                        handles = self._mdib.xtra.disassociate_all(entity,
                                                                   unbinding_mdib_version = mgr.new_mdib_version)
                        operation_target_handles.extend(handles)
                        modified_state_handles[entity.handle].extend(handles)
                        # set version and time in new state
                        proposed_st.BindingMdibVersion = mgr.new_mdib_version
                        proposed_st.BindingStartTime = time.time()
                    self._logger.info('new %s, DescriptorHandle=%s Handle=%s',
                                      proposed_st.NODETYPE.localname, proposed_st.DescriptorHandle, proposed_st.Handle)
                    # add to entity, and keep handle for later
                    entity.states[proposed_st.Handle] = proposed_st
                else:
                    # this is an update to an existing patient
                    # use "regular" way to update via transaction manager
                    self._logger.info('update %s, handle=%s', proposed_st.NODETYPE.localname, proposed_st.Handle)
                    # handle changed ContextAssociation
                    if (old_state_container.ContextAssociation == pm_types.ContextAssociation.ASSOCIATED
                            and proposed_st.ContextAssociation != pm_types.ContextAssociation.ASSOCIATED):
                        proposed_st.UnbindingMdibVersion = mgr.new_mdib_version
                        proposed_st.BindingEndTime = time.time()
                    elif (old_state_container.ContextAssociation != pm_types.ContextAssociation.ASSOCIATED
                          and proposed_st.ContextAssociation == pm_types.ContextAssociation.ASSOCIATED):
                        proposed_st.BindingMdibVersion = mgr.new_mdib_version
                        proposed_st.BindingStartTime = time.time()
                        handles = self._mdib.xtra.disassociate_all(entity,
                                                                   unbinding_mdib_version = mgr.new_mdib_version,
                                                                   ignored_handle=old_state_container.Handle)
                        operation_target_handles.extend(handles)
                        modified_state_handles[entity.handle].extend(handles)
                    old_state_container.update_from_other_container(proposed_st, skipped_properties=[
                                                                                           'BindingMdibVersion',
                                                                                           'UnbindingMdibVersion',
                                                                                           'BindingStartTime',
                                                                                           'BindingEndTime',
                                                                                           'StateVersion'])
                modified_state_handles[entity.handle].append(proposed_st.Handle)
                operation_target_handles.append(proposed_st.Handle)

            # write changes back to mdib
            for entity in modified_entities:
                handles = modified_state_handles[entity.handle]
                mgr.write_entity(entity, handles)

            if len(operation_target_handles) == 1:
                return ExecuteResult(operation_target_handles[0],
                                     self._mdib.data_model.msg_types.InvocationState.FINISHED)
            # the operation manipulated more than one context state, but the operation can only return a single handle.
            # (that is a BICEPS shortcoming, the string return type only reflects that situation).
            return ExecuteResult(params.operation_instance.operation_target_handle,
                                 self._mdib.data_model.msg_types.InvocationState.FINISHED)


class EnsembleContextProvider(GenericContextProvider):
    """EnsembleContextProvider."""

    def __init__(self, mdib: ProviderMdibProtocol, log_prefix: str | None = None):
        super().__init__(mdib,
                         op_target_descr_types=[mdib.data_model.pm_names.EnsembleContextDescriptor],
                         log_prefix=log_prefix)


class LocationContextProvider(GenericContextProvider):
    """LocationContextProvider."""

    def __init__(self, mdib: ProviderMdibProtocol, log_prefix: str | None = None):
        super().__init__(mdib,
                         op_target_descr_types=[mdib.data_model.pm_names.LocationContextDescriptor],
                         log_prefix=log_prefix)
