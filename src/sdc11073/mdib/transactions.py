from __future__ import annotations

import time
import uuid
from typing import TYPE_CHECKING, cast

from sdc11073.exceptions import ApiUsageError

from .statecontainers import AbstractMultiStateProtocol
from .transactionsprotocol import (
    AnyTransactionManagerProtocol,
    TransactionItem,
    TransactionResultProtocol,
    TransactionType,
)

if TYPE_CHECKING:
    from sdc11073.loghelper import LoggerAdapter

    from .descriptorcontainers import AbstractDescriptorProtocol
    from .providermdib import ProviderMdib
    from .statecontainers import AbstractStateProtocol


class _TransactionBase:
    def __init__(self,
                 device_mdib_container: ProviderMdib,
                 logger: LoggerAdapter):
        self._mdib = device_mdib_container
        # provide the new mdib version that the commit of this transaction will create
        self.new_mdib_version = device_mdib_container.mdib_version + 1
        self._logger = logger
        self.descriptor_updates: dict[str, TransactionItem] = {}
        self.metric_state_updates: dict[str, TransactionItem] = {}
        self.alert_state_updates: dict[str, TransactionItem] = {}
        self.component_state_updates: dict[str, TransactionItem] = {}
        self.context_state_updates: dict[str, TransactionItem] = {}
        self.operational_state_updates: dict[str, TransactionItem] = {}
        self.rt_sample_state_updates: dict[str, TransactionItem] = {}
        self._error = False

    def _handle_state_updates(self, state_updates_dict: dict) -> list[TransactionItem]:
        """Update mdib table and return a list of states to be sent in notifications."""
        updates_list = []
        for transaction_item in state_updates_dict.values():
            if transaction_item.old is not None:
                table = self._mdib.context_states if transaction_item.old.is_context_state else self._mdib.states
                table.remove_object_no_lock(transaction_item.old)
            else:
                table = self._mdib.context_states if transaction_item.new.is_context_state else self._mdib.states
            table.add_object_no_lock(transaction_item.new)
            updates_list.append(transaction_item.new.mk_copy(copy_node=False))
        return updates_list

    def get_state_transaction_item(self, handle: str) -> TransactionItem | None:
        """If transaction has a state with given handle, return the transaction-item, otherwise None.

        :param handle: the Handle of a context state or the DescriptorHandle in all other cases
        """
        if not handle:
            raise ValueError('No handle for state specified')
        for lookup in (self.metric_state_updates,
                       self.alert_state_updates,
                       self.component_state_updates,
                       self.context_state_updates,
                       self.operational_state_updates,
                       self.rt_sample_state_updates):
            if handle in lookup:
                return lookup[handle]
        return None

    @property
    def error(self) -> bool:
        return self._error


class DescriptorTransaction(_TransactionBase):
    """A Transaction that allows to insert / update / delete Descriptors and to modify states related to them."""

    def __init__(self,
                 device_mdib_container: ProviderMdib,
                 logger: LoggerAdapter):
        super().__init__(device_mdib_container, logger)

    def actual_descriptor(self, descriptor_handle: str) -> AbstractDescriptorProtocol:
        """Return the actual descriptor in open transaction or from mdib.

        This method does not add the descriptor to the transaction!
        The descriptor can already be part of the transaction, and e.g. in pre_commit handlers of role providers
        it can be necessary to have access to it.
        """
        if not descriptor_handle:
            raise ValueError('No handle for descriptor specified')
        tr_container = self.descriptor_updates.get(descriptor_handle)
        if tr_container is not None:
            if tr_container.new is None:  # descriptor is deleted in this transaction!
                raise ValueError(f'The descriptor {descriptor_handle} is going to be deleted')
            return tr_container.new
        return self._mdib.descriptions.handle.get_one(descriptor_handle)

    def add_descriptor(self,
                       descriptor_container: AbstractDescriptorProtocol,
                       adjust_descriptor_version: bool = True,
                       state_container: AbstractStateProtocol | None = None):
        """Add a new descriptor to mdib."""
        descriptor_handle = descriptor_container.Handle
        if descriptor_handle in self.descriptor_updates:
            raise ValueError(f'Descriptor {descriptor_handle} already in updated set!')
        if descriptor_handle in self._mdib.descriptions.handle:
            raise ValueError(f'Cannot create descriptor {descriptor_handle}, it already exists in mdib!')
        if adjust_descriptor_version:
            self._mdib.descriptions.set_version(descriptor_container)
        if descriptor_container.source_mds is None:
            self._mdib.xtra.set_source_mds(descriptor_container)
        self.descriptor_updates[descriptor_handle] = TransactionItem(None, descriptor_container)
        if state_container is not None:
            if state_container.DescriptorHandle != descriptor_handle:
                raise ValueError(
                    f'State {state_container.DescriptorHandle} does not match descriptor {descriptor_handle}!')
            self.add_state(state_container)

    def remove_descriptor(self, descriptor_handle: str):
        """Remove existing descriptor from mdib."""
        if not descriptor_handle:
            raise ValueError('No handle for descriptor specified')
        if descriptor_handle in self.descriptor_updates:
            raise ValueError(f'Descriptor {descriptor_handle} already in updated set!')
        orig_descriptor_container = self._mdib.descriptions.handle.get_one(descriptor_handle)
        self.descriptor_updates[descriptor_handle] = TransactionItem(orig_descriptor_container, None)

    def get_descriptor(self, descriptor_handle: str) -> AbstractDescriptorProtocol:
        """Get a descriptor from mdib."""
        if not descriptor_handle:
            raise ValueError('No handle for descriptor specified')
        if descriptor_handle in self.descriptor_updates:
            raise ValueError(f'Descriptor {descriptor_handle} already in updated set!')
        orig_descriptor_container = self._mdib.descriptions.handle.get_one(descriptor_handle)
        descriptor_container = orig_descriptor_container.mk_copy()
        descriptor_container.increment_descriptor_version()
        self.descriptor_updates[descriptor_handle] = TransactionItem(orig_descriptor_container, descriptor_container)
        return descriptor_container

    def get_state(self, descriptor_handle: str) -> AbstractStateProtocol:
        """Read a state from mdib and add it to the transaction.

        This method only allows to get a state if the corresponding descriptor is already part of the transaction.
        if not, it raises an ApiUsageError.
        """
        if not descriptor_handle:
            raise ValueError('No handle for state specified')
        if descriptor_handle not in self.descriptor_updates:
            raise ApiUsageError('Transaction does not contain the corresponding descriptor!')
        descriptor = self.descriptor_updates[descriptor_handle].new
        if descriptor.is_context_descriptor:
            # prevent this for simplicity reasons
            raise ApiUsageError('Transaction does not support extra handling of context states!')

        updates_dict = self._get_states_update(descriptor)
        if descriptor_handle in updates_dict:
            raise ValueError(f'State {descriptor_handle} already in updated set!')

        mdib_state = self._mdib.states.descriptor_handle.get_one(descriptor_handle, allow_none=False)
        copied_state = mdib_state.mk_copy()
        copied_state.increment_state_version()
        updates_dict[descriptor_handle] = TransactionItem(mdib_state, copied_state)
        return copied_state

    def add_state(self, state_container: AbstractStateProtocol, adjust_state_version: bool = True):
        """Add a new state to mdib.

        This method only allows to add a state if the corresponding descriptor is already part of the transaction.
        if not, it raises an ApiUsageError.
        """
        if state_container.DescriptorHandle not in self.descriptor_updates:
            raise ApiUsageError('Transaction has no descriptor for this state!')
        updates_dict = self._get_states_update(state_container)

        if state_container.is_context_state:
            if state_container.Handle is None:
                state_container.Handle = uuid.uuid4().hex
            key = state_container.Handle
        else:
            key = state_container.DescriptorHandle

        if key in updates_dict:
            raise ValueError(f'State {key} already in updated set!')

        # set reference to descriptor
        state_container.descriptor_container = self.descriptor_updates[state_container.DescriptorHandle].new
        state_container.DescriptorVersion = state_container.descriptor_container.DescriptorVersion
        if adjust_state_version:
            if state_container.is_context_state:
                self._mdib.context_states.set_version(state_container)
            else:
                self._mdib.states.set_version(state_container)
        updates_dict[key] = TransactionItem(None, state_container)

    def process_transaction(self, set_determination_time: bool) -> TransactionResultProtocol:  # noqa: ARG002
        """Process transaction and create a TransactionResult.

        The parameter set_determination_time is only present in order to implement the interface correctly.
        Determination time is not set, because descriptors have no modification time.
        """
        proc = TransactionResult()
        if self.descriptor_updates:
            self._mdib.mdib_version = self.new_mdib_version
            # need to know all to be deleted and to be created descriptors
            to_be_deleted_handles = [tr_item.old.Handle for tr_item in self.descriptor_updates.values()
                                     if tr_item.new is None and tr_item.old is not None]
            to_be_created_handles = [tr_item.new.Handle for tr_item in self.descriptor_updates.values()
                                     if tr_item.old is None and tr_item.new is not None]
            # Remark 1:
            # handling only updated states here: If a descriptor is created, it can be assumed that the
            # application also creates the state in a transaction.
            # The state will then be transported via that notification report.
            # Maybe this needs to be reworked, but at the time of this writing it seems fine.
            #
            # Remark 2:
            # DescriptionModificationReport also contains the states that are related to the descriptors.
            # => if there is one, update its DescriptorVersion and add it to list of states that shall be sent
            # (Assuming that context descriptors (patient, location) are never changed,
            #  additional check for states in self.context_states is not needed.
            #  If this assumption is wrong, that functionality must be added!)

            for tr_item in self.descriptor_updates.values():
                orig_descriptor, new_descriptor = tr_item.old, tr_item.new
                if orig_descriptor is None:
                    # this is a create operation
                    self._logger.debug(  # noqa: PLE1205
                        'transaction_manager: new descriptor Handle={}, DescriptorVersion={}',
                        new_descriptor.Handle, new_descriptor.DescriptorVersion)
                    proc.descr_created.append(new_descriptor.mk_copy())
                    self._mdib.descriptions.add_object_no_lock(new_descriptor)
                    # increment DescriptorVersion if a child descriptor is added or deleted.
                    if new_descriptor.parent_handle is not None \
                            and new_descriptor.parent_handle not in to_be_created_handles:
                        # only update parent if it is not also created in this transaction
                        self._increment_parent_descriptor_version(proc, new_descriptor)
                    self._update_corresponding_state(new_descriptor)
                elif new_descriptor is None:
                    # this is a delete operation
                    self._logger.debug(  # noqa: PLE1205
                        'transaction_manager: rm descriptor Handle={}, DescriptorVersion={}',
                        orig_descriptor.Handle, orig_descriptor.DescriptorVersion)
                    all_descriptors = self._mdib.get_all_descriptors_in_subtree(orig_descriptor)
                    self._mdib.rm_descriptors_and_states(all_descriptors)
                    proc.descr_deleted.extend([d.mk_copy() for d in all_descriptors])
                    # increment DescriptorVersion if a child descriptor is added or deleted.
                    if orig_descriptor.parent_handle is not None \
                            and orig_descriptor.parent_handle not in to_be_deleted_handles:
                        # only update parent if it is not also deleted in this transaction
                        self._increment_parent_descriptor_version(proc, orig_descriptor)
                else:
                    # this is an update operation
                    proc.descr_updated.append(new_descriptor)
                    self._logger.debug(  # noqa: PLE1205
                        'transaction_manager: update descriptor Handle={}, DescriptorVersion={}',
                        new_descriptor.Handle, new_descriptor.DescriptorVersion)
                    orig_descriptor.update_from_other_container(new_descriptor)
                    self._update_corresponding_state(orig_descriptor)
                    self._mdib.descriptions.update_object_no_lock(orig_descriptor)
            for updates_dict, dest_list in ((self.alert_state_updates, proc.alert_updates),
                                            (self.metric_state_updates, proc.metric_updates),
                                            (self.context_state_updates, proc.ctxt_updates),
                                            (self.component_state_updates, proc.comp_updates),
                                            (self.operational_state_updates, proc.op_updates),
                                            (self.rt_sample_state_updates, proc.rt_updates),
                                            ):
                updates = self._handle_state_updates(updates_dict)
                dest_list.extend(updates)
        return proc

    def _update_corresponding_state(self, descriptor_container: AbstractDescriptorProtocol):
        updates_dict = self._get_states_update(descriptor_container)
        if descriptor_container.is_context_descriptor:
            all_context_states = self._mdib.context_states.descriptor_handle.get(
                descriptor_container.Handle, [])
            for context_state in all_context_states:
                state_update = updates_dict.get(context_state.Handle)
                if state_update is not None:
                    # the state has also been updated directly in transaction.
                    # update descriptor version
                    old_state, new_state = state_update
                else:
                    old_state = context_state
                    new_state = old_state.mk_copy()
                    updates_dict[context_state.Handle] = TransactionItem(old_state, new_state)
                new_state.descriptor_container = descriptor_container
                new_state.increment_state_version()
                new_state.update_descriptor_version()
        else:
            # check if state is already present in this transaction
            tr_item = updates_dict.get(descriptor_container.Handle)
            if tr_item is not None:
                # the state has also been updated directly in transaction.
                # update descriptor version
                if tr_item.new is None:
                    raise ValueError(
                        f'State deleted? That should not be possible! handle = {descriptor_container.Handle}')
                tr_item.new.update_descriptor_version()
            else:
                old_state = self._mdib.states.descriptor_handle.get_one(
                    descriptor_container.Handle, allow_none=True)
                if old_state is not None:
                    new_state = old_state.mk_copy()
                    new_state.descriptor_container = descriptor_container  #
                    new_state.DescriptorVersion = descriptor_container.DescriptorVersion
                    new_state.increment_state_version()
                    updates_dict[descriptor_container.Handle] = TransactionItem(old_state, new_state)

    def _increment_parent_descriptor_version(self, proc: TransactionResult,
                                             descriptor_container: AbstractDescriptorProtocol):
        parent_descriptor_container = self._mdib.descriptions.handle.get_one(
            descriptor_container.parent_handle, allow_none=True)
        if parent_descriptor_container is not None:
            parent_descriptor_container.increment_descriptor_version()
            proc.descr_updated.append(parent_descriptor_container.mk_copy())
            self._update_corresponding_state(parent_descriptor_container)

    def _get_states_update(self, container: AbstractStateProtocol | AbstractDescriptorProtocol) -> dict:
        if getattr(container, 'is_realtime_sample_array_metric_state', False) \
                or getattr(container, 'is_realtime_sample_array_metric_descriptor', False):
            return self.rt_sample_state_updates
        if getattr(container, 'is_metric_state', False) or getattr(container, 'is_metric_descriptor', False):
            return self.metric_state_updates
        if getattr(container, 'is_alert_state', False) or getattr(container, 'is_alert_descriptor', False):
            return self.alert_state_updates
        if getattr(container, 'is_component_state', False) or getattr(container, 'is_component_descriptor', False):
            return self.component_state_updates
        if getattr(container, 'is_operational_state', False) or getattr(container, 'is_operational_descriptor', False):
            return self.operational_state_updates
        if getattr(container, 'is_context_state', False) or getattr(container, 'is_context_descriptor', False):
            return self.context_state_updates
        raise NotImplementedError(f'Unhandled case {container}')


class StateTransactionBase(_TransactionBase):
    """Base Class for all transactions that modify states."""

    def __init__(self,
                 device_mdib_container: ProviderMdib,
                 logger: LoggerAdapter):
        super().__init__(device_mdib_container, logger)
        self._state_updates = {}  # will be set to proper value in derived classes

    def has_state(self, descriptor_handle: str) -> bool:
        """Check if transaction has a state with given handle."""
        return descriptor_handle in self._state_updates

    def unget_state(self, state_container: AbstractStateProtocol):
        """Forget a state that was provided before by a get_state or add_state call."""
        if state_container.DescriptorHandle in self._state_updates:
            del self._state_updates[state_container.DescriptorHandle]

    def get_state(self, descriptor_handle: str) -> AbstractStateProtocol:
        """Read a state from mdib and add it to the transaction.

        If the type of the state does not match the transaction type, an ApiUsageError is thrown.
        """
        if not descriptor_handle:
            raise ValueError('No handle for state specified')
        if descriptor_handle in self._state_updates:
            raise ValueError(f'State {descriptor_handle} already in updated set!')

        mdib_state = self._mdib.states.descriptor_handle.get_one(descriptor_handle, allow_none=False)
        if not self._is_correct_state_type(mdib_state):
            raise ApiUsageError(f'Wrong data type in transaction! {self.__class__.__name__}, {mdib_state}')

        copied_state = mdib_state.mk_copy()
        copied_state.increment_state_version()
        self._state_updates[descriptor_handle] = TransactionItem(mdib_state, copied_state)
        return copied_state

    def actual_descriptor(self, descriptor_handle: str) -> AbstractDescriptorProtocol:
        """Look descriptor in mdib, state transaction cannot have descriptor changes."""
        return self._mdib.descriptions.handle.get_one(descriptor_handle)

    @staticmethod
    def _is_correct_state_type(state: AbstractStateProtocol) -> bool:  # noqa: ARG004
        return False


class AlertStateTransaction(StateTransactionBase):
    """A Transaction for alert states."""

    def __init__(self,
                 device_mdib_container: ProviderMdib,
                 logger: LoggerAdapter):
        super().__init__(device_mdib_container, logger)
        self._state_updates = self.alert_state_updates

    def process_transaction(self, set_determination_time: bool) -> TransactionResultProtocol:
        """Process transaction and create a TransactionResult."""
        if set_determination_time:
            for tr_item in self._state_updates.values():
                new_state = tr_item.new
                old_state = tr_item.old
                if new_state is None or not hasattr(new_state, 'Presence'):
                    continue
                if old_state is None:
                    if new_state.Presence:
                        new_state.DeterminationTime = time.time()
                elif new_state.is_alert_condition and new_state.Presence != old_state.Presence:
                    new_state.DeterminationTime = time.time()
        proc = TransactionResult()
        if self._state_updates:
            self._mdib.mdib_version = self.new_mdib_version
            updates = self._handle_state_updates(self._state_updates)
            proc.alert_updates.extend(updates)
        return proc

    @staticmethod
    def _is_correct_state_type(state: AbstractStateProtocol) -> bool:
        return state.is_alert_state


class MetricStateTransaction(StateTransactionBase):
    """A Transaction for metric states (except real time samples)."""

    def __init__(self,
                 device_mdib_container: ProviderMdib,
                 logger: LoggerAdapter):
        super().__init__(device_mdib_container, logger)
        self._state_updates = self.metric_state_updates

    def process_transaction(self, set_determination_time: bool) -> TransactionResultProtocol:
        """Process transaction and create a TransactionResult."""
        if set_determination_time:
            for tr_item in self._state_updates.values():
                if tr_item.new is not None and tr_item.new.MetricValue is not None:
                    tr_item.new.MetricValue.DeterminationTime = time.time()
        proc = TransactionResult()
        if self._state_updates:
            self._mdib.mdib_version = self.new_mdib_version
            updates = self._handle_state_updates(self._state_updates)
            proc.metric_updates.extend(updates)
        return proc

    @staticmethod
    def _is_correct_state_type(state: AbstractStateProtocol) -> bool:
        return state.is_metric_state and not state.is_realtime_sample_array_metric_state


class ComponentStateTransaction(StateTransactionBase):
    """A Transaction for component states."""

    def __init__(self,
                 device_mdib_container: ProviderMdib,
                 logger: LoggerAdapter):
        super().__init__(device_mdib_container, logger)
        self._state_updates = self.component_state_updates

    def process_transaction(self, set_determination_time: bool) -> TransactionResultProtocol:  # noqa: ARG002
        """Process transaction and create a TransactionResult."""
        proc = TransactionResult()
        if self._state_updates:
            self._mdib.mdib_version = self.new_mdib_version
            updates = self._handle_state_updates(self._state_updates)
            proc.comp_updates.extend(updates)
        return proc

    @staticmethod
    def _is_correct_state_type(state: AbstractStateProtocol) -> bool:
        return state.is_component_state


class RtStateTransaction(StateTransactionBase):
    """A Transaction for real time sample states."""

    def __init__(self,
                 device_mdib_container: ProviderMdib,
                 logger: LoggerAdapter):
        super().__init__(device_mdib_container, logger)
        self._state_updates = self.rt_sample_state_updates

    def process_transaction(self, set_determination_time: bool) -> TransactionResultProtocol:  # noqa: ARG002
        """Process transaction and create a TransactionResult."""
        proc = TransactionResult()
        if self._state_updates:
            self._mdib.mdib_version = self.new_mdib_version
            updates = self._handle_state_updates(self._state_updates)
            proc.rt_updates.extend(updates)
        return proc

    @staticmethod
    def _is_correct_state_type(state: AbstractStateProtocol) -> bool:
        return state.is_realtime_sample_array_metric_state


class OperationalStateTransaction(StateTransactionBase):
    """A Transaction for operational states."""

    def __init__(self,
                 device_mdib_container: ProviderMdib,
                 logger: LoggerAdapter):
        super().__init__(device_mdib_container, logger)
        self._state_updates = self.operational_state_updates

    def process_transaction(self, set_determination_time: bool) -> TransactionResultProtocol:  # noqa: ARG002
        """Process transaction and create a TransactionResult."""
        proc = TransactionResult()
        if self._state_updates:
            self._mdib.mdib_version = self.new_mdib_version
            updates = self._handle_state_updates(self._state_updates)
            proc.op_updates.extend(updates)
        return proc

    @staticmethod
    def _is_correct_state_type(state: AbstractStateProtocol) -> bool:
        return state.is_operational_state


class ContextStateTransaction(_TransactionBase):
    """A Transaction for context states."""

    def __init__(self,
                 device_mdib_container: ProviderMdib,
                 logger: LoggerAdapter):
        super().__init__(device_mdib_container, logger)
        self._state_updates = self.context_state_updates

    def get_context_state(self, context_state_handle: str) -> AbstractMultiStateProtocol:
        """Read a ContextState from mdib with given state handle."""
        if not context_state_handle:
            raise ValueError('No handle for context state specified')
        if context_state_handle in self._state_updates:
            raise ValueError(f'Context State {context_state_handle} already in updated set!')

        mdib_state = self._mdib.context_states.handle.get_one(context_state_handle, allow_none=False)
        copied_state = mdib_state.mk_copy()
        copied_state.increment_state_version()
        self._state_updates[context_state_handle] = TransactionItem(mdib_state, copied_state)
        return copied_state

    def mk_context_state(self, descriptor_handle: str,
                         context_state_handle: str | None = None,
                         adjust_state_version: bool = True,
                         set_associated: bool = False) -> AbstractMultiStateProtocol:
        """Create a new ContextStateContainer and add it to transaction.

        If context_state_handle is None, a unique handle is generated.
        """
        if not descriptor_handle:
            raise ValueError('No descriptor handle for context state specified')
        if context_state_handle in self._state_updates:
            raise ValueError(f'Context State {context_state_handle} already in updated set!')
        descriptor_container = self._mdib.descriptions.handle.get_one(descriptor_handle, allow_none=False)
        if not descriptor_container.is_context_descriptor:
            raise ValueError(f'Descriptor {descriptor_handle} is not a context descriptor!')

        if context_state_handle is not None:
            old_state_container = self._mdib.context_states.handle.get_one(context_state_handle, allow_none=True)
            if old_state_container is not None:
                raise ValueError(f'ContextState with handle={context_state_handle} already exists')

        new_state_container = self._mdib.data_model.mk_state_container(descriptor_container)
        new_state_container.Handle = context_state_handle or uuid.uuid4().hex
        if set_associated:
            # bind to new mdib version of this transaction
            new_state_container.BindingMdibVersion = self.new_mdib_version
            new_state_container.BindingStartTime = time.time()
            new_state_container.ContextAssociation = \
                self._mdib.data_model.pm_types.ContextAssociation.ASSOCIATED
        if context_state_handle is not None and adjust_state_version:
            self._mdib.context_states.set_version(new_state_container)

        self._state_updates[new_state_container.Handle] = TransactionItem(None, new_state_container)
        return cast(AbstractMultiStateProtocol, new_state_container)

    def add_state(self, state_container: AbstractMultiStateProtocol, adjust_state_version: bool = True):
        """Add a new state to mdib."""
        if not state_container.is_context_state:
            # prevent this for simplicity reasons
            raise ApiUsageError('Transaction only handles context states!')

        if state_container.descriptor_container is None:
            descr = self._mdib.descriptions.handle.get_one(state_container.DescriptorHandle)
            state_container.descriptor_container = descr
            state_container.DescriptorVersion = state_container.descriptor_container.DescriptorVersion

        if adjust_state_version:
            self._mdib.context_states.set_version(state_container)
        self._state_updates[state_container.Handle] = TransactionItem(None, state_container)

    def disassociate_all(self,
                         context_descriptor_handle: str,
                         ignored_handle: str | None = None) -> list[str]:
        """Disassociate all associated states in mdib for context_descriptor_handle.

        The updated states are added to the transaction.
        The method returns a list of states that were disassociated.
        :param context_descriptor_handle: the handle of the context descriptor
        :param ignored_handle: the context state with this Handle shall not be touched.
        """
        pm_types = self._mdib.data_model.pm_types
        disassociated_state_handles = []
        old_state_containers = self._mdib.context_states.descriptor_handle.get(context_descriptor_handle, [])
        for old_state in old_state_containers:
            if old_state.Handle == ignored_handle or old_state.Handle in self._state_updates:
                # If state is already part of this transaction leave it also untouched, accept what the user wanted.
                continue
            if old_state.ContextAssociation != pm_types.ContextAssociation.DISASSOCIATED \
                    or old_state.UnbindingMdibVersion is None:
                self._logger.info('disassociate %s, handle=%s', old_state.NODETYPE.localname,
                                  old_state.Handle)
                transaction_state = self.get_context_state(old_state.Handle)
                transaction_state.ContextAssociation = pm_types.ContextAssociation.DISASSOCIATED
                if transaction_state.UnbindingMdibVersion is None:
                    transaction_state.UnbindingMdibVersion = self.new_mdib_version
                    transaction_state.BindingEndTime = time.time()
                disassociated_state_handles.append(transaction_state.Handle)
        return disassociated_state_handles

    def process_transaction(self, set_determination_time: bool) -> TransactionResultProtocol:  # noqa: ARG002
        """Process transaction and create a TransactionResult."""
        proc = TransactionResult()
        if self._state_updates:
            self._mdib.mdib_version = self.new_mdib_version
            updates = self._handle_state_updates(self._state_updates)
            proc.ctxt_updates.extend(updates)
        return proc

    @staticmethod
    def _is_correct_state_type(state: AbstractStateProtocol) -> bool:
        return state.is_context_state


class TransactionResult:
    """The transaction result.

    Data is used to create notifications.
    """

    def __init__(self):
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

    @property
    def has_descriptor_updates(self) -> bool:
        """Return True if at least one descriptor is in result."""
        return len(self.descr_updated) > 0 or len(self.descr_created) > 0 or len(self.descr_deleted) > 0

    def all_states(self) -> list[AbstractStateProtocol]:
        """Return all states in this transaction."""
        return self.metric_updates + self.alert_updates + self.comp_updates + self.ctxt_updates \
               + self.op_updates + self.rt_updates


_transaction_type_lookup = {TransactionType.descriptor: DescriptorTransaction,
                            TransactionType.alert: AlertStateTransaction,
                            TransactionType.metric: MetricStateTransaction,
                            TransactionType.operational: OperationalStateTransaction,
                            TransactionType.context: ContextStateTransaction,
                            TransactionType.component: ComponentStateTransaction,
                            TransactionType.rt_sample: RtStateTransaction}


def mk_transaction(provider_mdib: ProviderMdib,
                   transaction_type: TransactionType,
                   logger: LoggerAdapter) -> AnyTransactionManagerProtocol:
    """Create a transaction according to transaction_type."""
    return _transaction_type_lookup[transaction_type](provider_mdib, logger)
