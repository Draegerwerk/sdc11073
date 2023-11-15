from __future__ import annotations

import time
import uuid
from typing import TYPE_CHECKING

from sdc11073.provider.operations import ExecuteResult

from . import providerbase

if TYPE_CHECKING:
    from lxml.etree import QName

    from sdc11073.mdib.descriptorcontainers import AbstractOperationDescriptorProtocol
    from sdc11073.mdib.providermdib import ProviderMdib
    from sdc11073.provider.operations import OperationDefinitionBase
    from sdc11073.provider.operations import ExecuteParameters

    from .providerbase import OperationClassGetter


class GenericContextProvider(providerbase.ProviderRole):
    """Handles SetContextState operations."""

    def __init__(self, mdib: ProviderMdib,
                 op_target_descr_types: list[QName] | None = None,
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
            op_target_descr_container = self._mdib.descriptions.handle.get_one(
                operation_descriptor_container.OperationTarget)
            if (not self._op_target_descr_types) or (
                    op_target_descr_container.NODETYPE not in self._op_target_descr_types):
                return None  # we do not handle this target type
            return self._mk_operation_from_operation_descriptor(operation_descriptor_container,
                                                                operation_cls_getter,
                                                                operation_handler=self._set_context_state)
        return None

    def _set_context_state(self, params: ExecuteParameters) -> ExecuteResult:
        """Execute the operation itself (ExecuteHandler)."""
        proposed_context_states = params.operation_request.argument
        pm_types = self._mdib.data_model.pm_types
        operation_target_handles = []
        with self._mdib.context_state_transaction() as mgr:
            for proposed_st in proposed_context_states:
                old_state_container = None
                if proposed_st.DescriptorHandle != proposed_st.Handle:
                    # this is an update for an existing state
                    old_state_container = self._mdib.context_states.handle.get_one(
                        proposed_st.Handle, allow_none=True)
                    if old_state_container is None:
                        raise ValueError(f'handle {proposed_st.Handle} not found')
                if old_state_container is None:
                    # this is a new context state
                    # create a new unique handle
                    proposed_st.Handle = uuid.uuid4().hex
                    operation_target_handles.append(proposed_st.Handle)
                    proposed_st.BindingMdibVersion = self._mdib.mdib_version
                    proposed_st.BindingStartTime = time.time()
                    proposed_st.ContextAssociation = pm_types.ContextAssociation.ASSOCIATED
                    self._logger.info('new %s, DescriptorHandle=%s Handle=%s',
                                      proposed_st.NODETYPE.localname, proposed_st.DescriptorHandle, proposed_st.Handle)
                    mgr.add_state(proposed_st)

                    # find all associated context states, disassociate them, set unbinding info, and add them to updates
                    old_state_containers = self._mdib.context_states.descriptor_handle.get(
                        proposed_st.DescriptorHandle, [])
                    for old_state in old_state_containers:
                        if old_state.ContextAssociation != pm_types.ContextAssociation.DISASSOCIATED \
                                or old_state.UnbindingMdibVersion is None:
                            self._logger.info('disassociate %s, handle=%s', old_state.NODETYPE.localname,
                                              old_state.Handle)
                            new_state = mgr.get_context_state(old_state.Handle)
                            new_state.ContextAssociation = pm_types.ContextAssociation.DISASSOCIATED
                            if new_state.UnbindingMdibVersion is None:
                                new_state.UnbindingMdibVersion = self._mdib.mdib_version
                                new_state.BindingEndTime = time.time()
                            operation_target_handles.append(new_state.Handle)
                else:
                    # this is an update to an existing patient
                    # use "regular" way to update via transaction manager
                    self._logger.info('update %s, handle=%s', proposed_st.NODETYPE.localname, proposed_st.Handle)
                    tmp = mgr.get_context_state(proposed_st.Handle)
                    tmp.update_from_other_container(proposed_st, skipped_properties=['ContextAssociation',
                                                                                     'BindingMdibVersion',
                                                                                     'UnbindingMdibVersion',
                                                                                     'BindingStartTime',
                                                                                     'BindingEndTime',
                                                                                     'StateVersion'])
                    operation_target_handles.append(proposed_st.Handle)
            if len(operation_target_handles) == 1:
                return ExecuteResult(operation_target_handles[0],
                                     self._mdib.data_model.msg_types.InvocationState.FINISHED)
            # the operation manipulated more than one context state, but the operation can only return a single handle.
            # (that is a BICEPS shortcoming, the string return type only reflects that situation).
            return ExecuteResult(params.operation_instance.operation_target_handle,
                                 self._mdib.data_model.msg_types.InvocationState.FINISHED)


class EnsembleContextProvider(GenericContextProvider):
    """EnsembleContextProvider."""

    def __init__(self, mdib: ProviderMdib, log_prefix: str | None = None):
        super().__init__(mdib,
                         op_target_descr_types=[mdib.data_model.pm_names.EnsembleContextDescriptor],
                         log_prefix=log_prefix)


class LocationContextProvider(GenericContextProvider):
    """LocationContextProvider."""

    def __init__(self, mdib: ProviderMdib, log_prefix: str | None = None):
        super().__init__(mdib,
                         op_target_descr_types=[mdib.data_model.pm_names.LocationContextDescriptor],
                         log_prefix=log_prefix)
