from __future__ import annotations

import time
import uuid
from dataclasses import dataclass
from enum import Enum
from functools import wraps
from typing import TYPE_CHECKING, Any, Callable, Protocol

from sdc11073.exceptions import ApiUsageError
from .transactions import TransactionType, _TrItem, TransactionManagerProtocol

if TYPE_CHECKING:
    from sdc11073.loghelper import LoggerAdapter
    from sdc11073.multikey import MultiKeyLookup

    from .descriptorcontainers import AbstractDescriptorProtocol
    from .providermdib import ProviderMdib
    from .statecontainers import (
        AbstractMultiStateProtocol,
        AbstractStateContainer,
        AbstractStateProtocol
    )


class DescriptorTransaction:
    def __init__(self,
                 device_mdib_container: ProviderMdib,
                 logger: LoggerAdapter):
        self._mdib = device_mdib_container
        self._logger = logger
        self.descriptor_updates = {}
        self.state_updates = {}

    def get_descriptor_in_transaction(self, descriptor_handle: str) -> AbstractDescriptorProtocol:
        """Look for new or updated descriptor in current transaction and in mdib."""

    def add_descriptor(self,
                       descriptor_container: AbstractDescriptorProtocol,
                       adjust_descriptor_version: bool = True,
                       state_container: AbstractStateProtocol | None = None):
        """Add a new descriptor to mdib."""
        descriptor_handle = descriptor_container.Handle
        if descriptor_handle in self.descriptor_updates:
            raise ValueError(f'Descriptor {descriptor_handle} already in updated set!')
        if descriptor_handle in self._mdib.descriptions.handle:
            raise ValueError(f'cannot create Descriptor {descriptor_handle}, it already exists!')
        if adjust_descriptor_version:
            self._mdib.descriptions.set_version(descriptor_container)
        if descriptor_container.source_mds is None:
            self._mdib.xtra.set_source_mds(descriptor_container)
        self.descriptor_updates[descriptor_handle] = _TrItem(None, descriptor_container)
        if state_container is not None:
            if state_container.DescriptorHandle != descriptor_handle:
                raise ValueError(f'State {state_container.DescriptorHandle} != {descriptor_handle}!')
            self.add_state(state_container)

    def remove_descriptor(self, descriptor_handle: str):
        """Remove existing descriptor from mdib."""
        if descriptor_handle in self.descriptor_updates:
            raise ValueError(f'Descriptor {descriptor_handle} already in updated set!')
        orig_descriptor_container = self._mdib.descriptions.handle.get_one(descriptor_handle)
        self.descriptor_updates[descriptor_handle] = _TrItem(orig_descriptor_container, None)

    def get_descriptor(self, descriptor_handle: str) -> AbstractDescriptorProtocol:
        """Get a descriptor from mdib."""
        if descriptor_handle in self.descriptor_updates:
            raise ValueError(f'Descriptor {descriptor_handle} already in updated set!')
        orig_descriptor_container = self._mdib.descriptions.handle.get_one(descriptor_handle)
        descriptor_container = orig_descriptor_container.mk_copy()
        descriptor_container.increment_descriptor_version()
        self.descriptor_updates[descriptor_handle] = _TrItem(orig_descriptor_container, descriptor_container)
        return descriptor_container

    def add_state(self, state_container: AbstractStateProtocol, adjust_state_version: bool = True):
        """Add a new state to mdib."""
        if state_container.DescriptorHandle not in self.descriptor_updates:
            raise ApiUsageError('transaction has no descriptor for this state!')

        if state_container.DescriptorHandle in self.state_updates:
            raise ValueError(f'State {state_container.DescriptorHandle} already in updated set!')

        if adjust_state_version:
            self._mdib.states.set_version(state_container)

        self.state_updates[state_container.DescriptorHandle] = _TrItem(None, state_container)

    def process_transaction(self, set_determination_time: bool) -> TransactionProcessor:
        proc = TransactionProcessor(self._mdib, self, set_determination_time)
        if self.descriptor_updates:
            proc.has_descriptor_updates = True
            # need to know all to be deleted and to be created descriptors
            to_be_deleted_handles = [tr_item.old.Handle for tr_item in self.descriptor_updates.values()
                                     if tr_item.new is None and tr_item.old is not None]
            to_be_created_handles = [tr_item.new.Handle for tr_item in self.descriptor_updates.values()
                                     if tr_item.old is None and tr_item.new is not None]
            # handling only updated states here: If a descriptor is created, it can be assumed that the
            # application also creates the state in a transaction.
            # The state will then be transported via that notification report.
            # Maybe this needs to be reworked, but at the time of this writing it seems fine.
            for tr_item in self.descriptor_updates.values():
                orig_descriptor, new_descriptor = tr_item.old, tr_item.new
                if new_descriptor is not None:
                    # DescriptionModificationReport also contains the states that are related to the descriptors.
                    # => if there is one, update its DescriptorVersion and add it to list of states that shall be sent
                    # (Assuming that context descriptors (patient, location) are never changed,
                    #  additional check for states in self.context_states is not needed.
                    #  If this assumption is wrong, that functionality must be added!)
                    self._update_corresponding_state(new_descriptor)
                else:  # descriptor delete
                    self._remove_corresponding_state(orig_descriptor)
                if orig_descriptor is None:
                    # this is a create operation
                    self._logger.debug(  # noqa: PLE1205
                        'transaction_manager: new descriptor Handle={}, DescriptorVersion={}',
                        new_descriptor.Handle, new_descriptor.DescriptorVersion)
                    proc.descr_created.append(new_descriptor.mk_copy())
                    self._mdib.descriptions.add_object_no_lock(new_descriptor)
                    # R0033: A SERVICE PROVIDER SHALL increment pm:AbstractDescriptor/@DescriptorVersion by one
                    #   if a direct child descriptor is added or deleted.
                    if new_descriptor.parent_handle is not None \
                            and new_descriptor.parent_handle not in to_be_created_handles:
                        # only update parent if it is not also created in this transaction
                        self._increment_parent_descriptor_version(new_descriptor)
                elif new_descriptor is None:
                    # this is a delete operation
                    self._logger.debug(  # noqa: PLE1205
                        'transaction_manager: rm descriptor Handle={}, DescriptorVersion={}',
                        orig_descriptor.Handle, orig_descriptor.DescriptorVersion)
                    all_descriptors = self._mdib.get_all_descriptors_in_subtree(orig_descriptor)
                    self._mdib.rm_descriptors_and_states(all_descriptors)
                    proc.descr_deleted.extend([d.mk_copy() for d in all_descriptors])
                    # R0033: A SERVICE PROVIDER SHALL increment pm:AbstractDescriptor/@DescriptorVersion by one
                    #   if a direct child descriptor is added or deleted.
                    if orig_descriptor.parent_handle is not None \
                            and orig_descriptor.parent_handle not in to_be_deleted_handles:
                        # only update parent if it is not also deleted in this transaction
                        self._increment_parent_descriptor_version(orig_descriptor)
                else:
                    # this is an update operation
                    proc.descr_updated.append(new_descriptor)
                    self._logger.debug(  # noqa: PLE1205
                        'transaction_manager: update descriptor Handle={}, DescriptorVersion={}',
                        new_descriptor.Handle, new_descriptor.DescriptorVersion)
                    self._mdib.descriptions.replace_object_no_lock(new_descriptor)
        return proc

    def _update_corresponding_state(self, proc, descriptor_container: AbstractDescriptorProtocol):
        update_dict = proc.get_state_update_dict(descriptor_container)
        if descriptor_container.is_context_descriptor:
            all_context_states = self._mdib.context_states.descriptor_handle.get(
                descriptor_container.Handle, [])
            for context_state in all_context_states:
                key = (descriptor_container.Handle, context_state.Handle)
                # check if state is already present in this transaction
                state_update = update_dict.get(key)
                if state_update is not None:
                    # the state has also been updated directly in transaction.
                    # update descriptor version
                    old_state, new_state = state_update
                else:
                    old_state = context_state
                    new_state = old_state.mk_copy()
                    update_dict[key] = _TrItem(old_state, new_state)
                new_state.descriptor_container = descriptor_container
                new_state.increment_state_version()
                new_state.update_descriptor_version()
        else:
            # check if state is already present in this transaction
            tr_item = update_dict.get(descriptor_container.Handle)
            if tr_item is not None:
                # the state has also been updated directly in transaction.
                # update descriptor version
                if tr_item.new is None:
                    raise ValueError(
                        f'state deleted? that should not be possible! handle = {descriptor_container.Handle}')
                tr_item.new.update_descriptor_version()
            else:
                old_state = self._mdib.states.descriptor_handle.get_one(
                    descriptor_container.Handle, allow_none=True)
                if old_state is not None:
                    new_state = old_state.mk_copy()
                    new_state.descriptor_container = descriptor_container  #
                    new_state.DescriptorVersion = descriptor_container.DescriptorVersion
                    new_state.increment_state_version()
                    update_dict[descriptor_container.Handle] = _TrItem(old_state, new_state)


class StateTransaction:
    def __init__(self,
                 device_mdib_container: ProviderMdib,
                 transaction_type: TransactionType,
                 logger: LoggerAdapter):
        self._mdib = device_mdib_container
        self._transaction_type = transaction_type
        self._logger = logger
        self.state_updates = {}

    def has_state(self, descriptor_handle: str) -> bool:
        """Check if transaction has a state with given handle."""
        return descriptor_handle in self.state_updates

    def unget_state(self, state_container: AbstractStateProtocol):
        """Forget a state that was provided before by a get_state or add_state call."""
        if state_container.DescriptorHandle in self.state_updates:
            del self.state_updates[state_container.DescriptorHandle]

    def get_state(self, descriptor_handle: str) -> AbstractStateProtocol:
        """Read a state from mdib and add it to the transaction."""
        if descriptor_handle is None:
            raise ValueError('no handle for state specified')
        if descriptor_handle in self.state_updates:
            raise ValueError(f'State {descriptor_handle} already in updated set!')

        mdib_state = self._mdib.states.descriptor_handle.get_one(descriptor_handle,
                                                                                  allow_none=False)
        self._verify_correct_state_type(mdib_state)
        copied_state = mdib_state.mk_copy()
        copied_state.increment_state_version()
        self.state_updates[descriptor_handle] = _TrItem(mdib_state, copied_state)
        return copied_state

    def _verify_correct_state_type(self, state: AbstractStateProtocol):
        # description modification report can contain any type of state, everything else must match exactly
        if  (state.is_component_state and self._transaction_type == TransactionType.component) or\
            (state.is_operational_state and self._transaction_type == TransactionType.operational) or\
            (state.is_alert_state and self._transaction_type == TransactionType.alert) or\
            (state.is_metric_state and self._transaction_type == TransactionType.metric):
            return
        raise ApiUsageError('Mix of data types in transaction is not allowed!')


class ContextStateTransaction:
    def __init__(self,
                 device_mdib_container: ProviderMdib,
                 logger: LoggerAdapter):
        self._mdib = device_mdib_container
        self._logger = logger
        self.state_updates = {}

    def get_context_state(self, context_state_handle: str) -> AbstractMultiStateProtocol:
        """Read a ContextState from mdib with given state handle."""
        if context_state_handle is None:
            raise ValueError('no handle for context state specified')
        if context_state_handle in self.state_updates:
            raise ValueError(f'Context State {context_state_handle} already in updated set!')

        mdib_state = self._mdib.context_states.handle.get_one(context_state_handle, allow_none=False)
        copied_state = mdib_state.mk_copy()
        copied_state.increment_state_version()
        self.state_updates[context_state_handle] = _TrItem(mdib_state, copied_state)
        return copied_state

    def mk_context_state(self, descriptor_handle: str,
                         context_state_handle: str | None = None,
                         adjust_state_version: bool = True,
                         set_associated: bool = False) -> AbstractMultiStateProtocol:
        """Create a new ContextStateContainer."""
        if descriptor_handle is None:
            raise ValueError('no descriptor handle for context state specified')
        if context_state_handle in self.state_updates:
            raise ValueError(f'Context State {context_state_handle} already in updated set!')
        descriptor_container = self._mdib.descriptions.get_one(descriptor_handle, allow_none=False)
        if not descriptor_container.is_context_descriptor:
            raise ValueError('descriptor is not a context descriptor!')

        if context_state_handle is not None:
            old_state_container = self._mdib.context_states.handle.get_one(context_state_handle,
                                                                                            allow_none=True)
            if old_state_container is not None:
                raise ValueError(f'ContextState with handle={context_state_handle} already exists')

        new_state_container = self._mdib.data_model.mk_state_container(descriptor_container)
        new_state_container.Handle = context_state_handle or uuid.uuid4().hex
        if set_associated:
            # bind to mdib version AFTER this transaction
            new_state_container.BindingMdibVersion = self._mdib.mdib_version + 1
            new_state_container.BindingStartTime = time.time()
            new_state_container.ContextAssociation = \
                self._mdib.data_model.pm_types.ContextAssociation.ASSOCIATED
        if context_state_handle is not None and adjust_state_version:
            self._mdib.context_states.set_version(new_state_container)

        self.state_updates[new_state_container.Handle] = _TrItem(None, new_state_container)
        return new_state_container



class Transaction:
    def __init__(self,
                 device_mdib_container: ProviderMdib,
                 transaction_type: TransactionType,
                 logger: LoggerAdapter):
        self._handler: ContextStateTransaction | DescriptorTransaction | StateTransaction
        if transaction_type == TransactionType.descriptor:
            self._handler = DescriptorTransaction(device_mdib_container,
                                                  logger)
        elif transaction_type == TransactionType.context:
            self._handler = ContextStateTransaction(device_mdib_container,
                                                    logger)
        else:
            self._handler = StateTransaction(device_mdib_container,
                                             transaction_type,
                                             logger)

    def get_descriptor_in_transaction(self, descriptor_handle: str) -> AbstractDescriptorProtocol:
        """Look for new or updated descriptor in current transaction and in mdib."""
        return self._handler.get_descriptor_in_transaction(descriptor_handle)

    def add_descriptor(self,
                       descriptor_container: AbstractDescriptorProtocol,
                       adjust_descriptor_version: bool = True,
                       state_container: AbstractStateProtocol | None = None):
        """Add a new descriptor to mdib."""
        self._handler.add_descriptor(descriptor_container, adjust_descriptor_version, state_container)

    def remove_descriptor(self, descriptor_handle: str):
        """Remove existing descriptor from mdib."""
        self._handler.remove_descriptor(descriptor_handle)

    def get_descriptor(self, descriptor_handle: str) -> AbstractDescriptorProtocol:
        """Get a descriptor from mdib."""
        return self._handler.get_descriptor(descriptor_handle)

    def has_state(self, descriptor_handle: str) -> bool:
        """Check if transaction has a state with given handle."""
        return self._handler.has_state(descriptor_handle)

    def get_state_transaction_item(self, handle: str) -> _TrItem | None:
        """If transaction has a state with given handle, return the transaction-item, otherwise None."""
        return self._handler.get_state_transaction_item(handle)

    def add_state(self, state_container: AbstractStateProtocol, adjust_state_version: bool = True):
        """Add a new state to mdib."""
        self._handler.add_state(state_container, adjust_state_version)

    def unget_state(self, state_container: AbstractStateProtocol):
        """Forget a state that was provided before by a get_state or add_state call."""
        self._handler.unget_state(state_container)

    def get_state(self, descriptor_handle: str) -> AbstractStateProtocol:
        """Read a state from mdib and add it to the transaction."""
        return self._handler.get_state(descriptor_handle)

    def get_context_state(self, context_state_handle: str) -> AbstractMultiStateProtocol:
        """Read a ContextState from mdib with given state handle."""
        return self._handler.get_context_state(context_state_handle)

    def mk_context_state(self, descriptor_handle: str,
                         context_state_handle: str | None = None,
                         adjust_state_version: bool = True,
                         set_associated: bool = False) -> AbstractMultiStateProtocol:
        """Create a new ContextStateContainer."""
        return self._handler.mk_context_state(descriptor_handle,
                                              context_state_handle,
                                              adjust_state_version,
                                              set_associated)


    def process_transaction(self, set_determination_time: bool) -> TransactionProcessor:
        """Process the transaction."""
        return self._handler.process_transaction(set_determination_time)


class TransactionProcessor:
    """The transaction processor, used internally by device mdib."""

    def __init__(self, mdib: ProviderMdib,
                 transaction: TransactionManagerProtocol,
                 set_determination_time: bool,
                 logger: LoggerAdapter):
        self._mdib = mdib
        self._mgr = transaction
        self._logger = logger
        self._set_determination_time = set_determination_time
        self._now = None

        # states and descriptors that were modified are stored here:
        self.descr_updated = []
        self.descr_created = []
        self.descr_deleted = []
        self.metric_updates = []
        self.alert_updates = []
        self.comp_updates = []
        self.ctxt_updates = []
        self.op_updates = []
        self.rt_updates = []
        self.has_descriptor_updates = False  # for easier handling

    def get_state_update_dict(self, descriptor_container: AbstractDescriptorProtocol) -> dict:
        """Return the correct dictionary depending on type of descriptor."""
        if descriptor_container.is_alert_descriptor:
            update_dict = self._mgr.alert_state_updates
        elif descriptor_container.is_component_descriptor:
            update_dict = self._mgr.component_state_updates
        elif descriptor_container.is_context_descriptor:
            update_dict = self._mgr.context_state_updates
        elif descriptor_container.is_realtime_sample_array_metric_descriptor:
            update_dict = self._mgr.rt_sample_state_updates
        elif descriptor_container.is_metric_descriptor:
            update_dict = self._mgr.metric_state_updates
        elif descriptor_container.is_operational_descriptor:
            update_dict = self._mgr.operational_state_updates
        else:
            raise NotImplementedError(f'do not know how to handle {descriptor_container.__class__.__name__}')
        return update_dict