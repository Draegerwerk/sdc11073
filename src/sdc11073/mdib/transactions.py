"""The module contains the implementations of transactions for ProviderMdib."""
from __future__ import annotations

import copy
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
    from .mdibbase import Entity, MultiStateEntity
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
                msg = f'The descriptor {descriptor_handle} is going to be deleted'
                raise ValueError(msg)
            return tr_container.new
        return self._mdib.descriptions.handle.get_one(descriptor_handle)

    def add_descriptor(self,
                       descriptor_container: AbstractDescriptorProtocol,
                       adjust_descriptor_version: bool = True,
                       state_container: AbstractStateProtocol | None = None):
        """Add a new descriptor to mdib."""
        descriptor_handle = descriptor_container.Handle
        if descriptor_handle in self.descriptor_updates:
            msg = f'Descriptor {descriptor_handle} already in updated set!'
            raise ValueError(msg)
        if descriptor_handle in self._mdib.descriptions.handle:
            msg = f'Cannot create descriptor {descriptor_handle}, it already exists in mdib!'
            raise ValueError(msg)
        if adjust_descriptor_version:
            self._mdib.descriptions.set_version(descriptor_container)
        if descriptor_container.source_mds is None:
            self._mdib.xtra.set_source_mds(descriptor_container)
        self.descriptor_updates[descriptor_handle] = TransactionItem(None, descriptor_container)
        if state_container is not None:
            if state_container.DescriptorHandle != descriptor_handle:
                msg = f'State {state_container.DescriptorHandle} does not match descriptor {descriptor_handle}!'
                raise ValueError(msg)
            self.add_state(state_container)

    def remove_descriptor(self, descriptor_handle: str):
        """Remove existing descriptor from mdib."""
        if not descriptor_handle:
            raise ValueError('No handle for descriptor specified')
        if descriptor_handle in self.descriptor_updates:
            msg = f'Descriptor {descriptor_handle} already in updated set!'
            raise ValueError(msg)
        orig_descriptor_container = self._mdib.descriptions.handle.get_one(descriptor_handle)
        self.descriptor_updates[descriptor_handle] = TransactionItem(orig_descriptor_container, None)

    def get_descriptor(self, descriptor_handle: str) -> AbstractDescriptorProtocol:
        """Get a descriptor from mdib."""
        if not descriptor_handle:
            raise ValueError('No handle for descriptor specified')
        if descriptor_handle in self.descriptor_updates:
            msg = f'Descriptor {descriptor_handle} already in updated set!'
            raise ValueError(msg)
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
            msg = f'State {descriptor_handle} already in updated set!'
            raise ValueError(msg)

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
            msg = f'State {key} already in updated set!'
            raise ValueError(msg)

        # set reference to descriptor
        state_container.descriptor_container = self.descriptor_updates[state_container.DescriptorHandle].new
        state_container.DescriptorVersion = state_container.descriptor_container.DescriptorVersion
        if adjust_state_version:
            if state_container.is_context_state:
                self._mdib.context_states.set_version(state_container)
            else:
                self._mdib.states.set_version(state_container)
        updates_dict[key] = TransactionItem(None, state_container)

    def write_entity(self, # noqa: PLR0912, C901
                     entity: Entity | MultiStateEntity,
                     adjust_version_counter: bool = True):
        """Insert or update an entity."""
        descriptor_handle = entity.descriptor.Handle
        if descriptor_handle in self.descriptor_updates:
            msg = f'Entity {descriptor_handle} already in updated set!'
            raise ValueError(msg)

        tmp_descriptor = copy.deepcopy(entity.descriptor)
        orig_descriptor_container = self._mdib.descriptions.handle.get_one(descriptor_handle, allow_none=True)

        if adjust_version_counter:
            if orig_descriptor_container is None:
                # new descriptor, update version from saved versions in mdib if exists
                self._mdib.descriptions.set_version(tmp_descriptor)
            else:
                # update from old
                tmp_descriptor.DescriptorVersion = orig_descriptor_container.DescriptorVersion + 1

        self.descriptor_updates[descriptor_handle] = TransactionItem(orig_descriptor_container,
                                                                     tmp_descriptor)

        if entity.is_multi_state:
            old_states = self._mdib.context_states.descriptor_handle.get(descriptor_handle, [])
            old_states_dict = {s.Handle: s for s in old_states}
            for state_container in entity.states.values():
                tmp_state = copy.deepcopy(state_container)
                old_state = old_states_dict.get(tmp_state.Handle) # can be None => new state
                if adjust_version_counter:
                    tmp_state.DescriptorVersion = tmp_descriptor.DescriptorVersion
                    if old_state is not None:
                        tmp_state.StateVersion = old_state.StateVersion + 1
                    else:
                        self._mdib.context_states.set_version(tmp_state)

                self.context_state_updates[state_container.Handle] = TransactionItem(old_state, tmp_state)
            deleted_states_handles = set(old_states_dict.keys()).difference(set(entity.states.keys()))
            for handle in deleted_states_handles:
                del_state = old_states_dict[handle]
                self.context_state_updates[handle] = TransactionItem(del_state, None)
        else:
            tmp_state = copy.deepcopy(entity.state)
            tmp_state.descriptor_container = tmp_descriptor
            old_state = self._mdib.states.descriptor_handle.get_one(descriptor_handle, allow_none=True)
            if adjust_version_counter:
                if old_state is not None:
                    tmp_state.StateVersion = old_state.StateVersion + 1
                else:
                    self._mdib.states.set_version(tmp_state)

            state_updates_dict = self._get_states_update(tmp_state)
            state_updates_dict[entity.state.DescriptorHandle] = TransactionItem(old_state, tmp_state)

    def write_entities(self, entities: list[Entity | MultiStateEntity], adjust_version_counter: bool = True):
        """Write entities in order parents first."""
        written_handles = []
        ent_dict = {ent.handle: ent for ent in entities}
        while len(written_handles) < len(ent_dict):
            for handle, ent in ent_dict.items():
                write_now = not(ent.parent_handle is not None
                                and ent.parent_handle in ent_dict
                                and ent.parent_handle not in written_handles)
                if write_now and handle not in written_handles:
                    # it has a parent, and parent has not been written yet
                    self.write_entity(ent, adjust_version_counter)
                    written_handles.append(handle)

    def remove_entity(self, entity: Entity | MultiStateEntity):
        """Remove existing entity from mdib."""
        self.remove_descriptor(entity.handle)

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
                    old_state, new_state = state_update.old, state_update.new
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
                    msg = f'State deleted? That should not be possible! handle = {descriptor_container.Handle}'
                    raise ValueError(msg)
                tr_item.new.update_descriptor_version()
            else:
                old_state = self._mdib.states.descriptor_handle.get_one(
                    descriptor_container.Handle, allow_none=True)
                if old_state is not None:
                    new_state = old_state.mk_copy()
                    new_state.descriptor_container = descriptor_container
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
        msg = f'Unhandled case {container}'
        raise NotImplementedError(msg)


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
            msg = f'State {descriptor_handle} already in updated set!'
            raise ValueError(msg)

        mdib_state = self._mdib.states.descriptor_handle.get_one(descriptor_handle, allow_none=False)
        if not self._is_correct_state_type(mdib_state):
            msg = f'Wrong data type in transaction! {self.__class__.__name__}, {mdib_state}'
            raise ApiUsageError(msg)

        copied_state = mdib_state.mk_copy()
        copied_state.increment_state_version()
        self._state_updates[descriptor_handle] = TransactionItem(mdib_state, copied_state)
        return copied_state

    def actual_descriptor(self, descriptor_handle: str) -> AbstractDescriptorProtocol:
        """Look descriptor in mdib, state transaction cannot have descriptor changes."""
        return self._mdib.descriptions.handle.get_one(descriptor_handle)

    def write_entity(self, entity: Entity, adjust_version_counter: bool = True):
        """Insert or update an entity."""
        if entity.is_multi_state:
            msg = f'Transaction {self.__class__.__name__} does not handle multi state entities!'
            raise ApiUsageError(msg)

        if not self._is_correct_state_type(entity.state):
            msg = f'Wrong data type in transaction! {self.__class__.__name__}, {entity.state}'
            raise ApiUsageError(msg)

        descriptor_handle = entity.state.DescriptorHandle
        old_state = self._mdib.states.descriptor_handle.get_one(entity.handle, allow_none=True)
        tmp_state = copy.deepcopy(entity.state)
        if adjust_version_counter:
            descriptor_container = self._mdib.descriptions.handle.get_one(descriptor_handle)
            tmp_state.DescriptorVersion = descriptor_container.DescriptorVersion
            if old_state is not None:
                # update from old state
                tmp_state.StateVersion = old_state.StateVersion + 1
            else:
                # new state, update version from saved versions in mdib if exists
                self._mdib.states.set_version(tmp_state)

        self._state_updates[descriptor_handle] = TransactionItem(old=old_state, new=tmp_state)

    def write_entities(self, entities: list[Entity | MultiStateEntity], adjust_version_counter: bool = True):
        """Write entities in order parents first."""
        for entity in entities:
            # check all states before writing any of them
            if entity.is_multi_state:
                msg = f'Transaction {self.__class__.__name__} does not handle multi state entities!'
                raise ApiUsageError(msg)

            if not self._is_correct_state_type(entity.state):
                msg = f'Wrong data type in transaction! {self.__class__.__name__}, {entity.state}'
                raise ApiUsageError(msg)
        for ent in entities:
            self.write_entity(ent, adjust_version_counter)

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
            msg = f'Context State {context_state_handle} already in updated set!'
            raise ValueError(msg)

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
            msg = f'Context State {context_state_handle} already in updated set!'
            raise ValueError(msg)
        descriptor_container = self._mdib.descriptions.handle.get_one(descriptor_handle, allow_none=False)
        if not descriptor_container.is_context_descriptor:
            msg = f'Descriptor {descriptor_handle} is not a context descriptor!'
            raise ValueError(msg)

        if context_state_handle is not None:
            old_state_container = self._mdib.context_states.handle.get_one(context_state_handle, allow_none=True)
            if old_state_container is not None:
                msg = f'ContextState with handle={context_state_handle} already exists'
                raise ValueError(msg)

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
        """Add a new context state to mdib."""
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

    def write_entity(self, entity: MultiStateEntity,
                  modified_handles: list[str],
                  adjust_version_counter: bool = True):
        """Insert or update a context state in mdib."""
        for handle in modified_handles:
            state_container = entity.states.get(handle)
            old_state = self._mdib.context_states.handle.get_one(handle, allow_none=True)
            if state_container is None:
                # a deleted state : this cannot be communicated via notification.
                # delete in internal_entity, and that is all
                if old_state is not None:
                    self._state_updates[handle] = TransactionItem(old=old_state, new=None)
                else:
                    msg = f'invalid handle {handle}!'
                    raise KeyError(msg)
                continue
            if not state_container.is_context_state:
                raise ApiUsageError('Transaction only handles context states!')

            tmp = copy.deepcopy(state_container)

            if old_state is None:
                # this is a new state
                tmp.descriptor_container = entity.descriptor
                tmp.DescriptorVersion = entity.descriptor.DescriptorVersion
                if adjust_version_counter:
                    self._mdib.context_states.set_version(tmp)
            elif adjust_version_counter:
                tmp.StateVersion = old_state.StateVersion + 1

            self._state_updates[state_container.Handle] = TransactionItem(old=old_state, new=tmp)

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
