"""The module implements transactions for EntityProviderMdib."""
from __future__ import annotations

import copy
import time
from dataclasses import dataclass
from enum import Enum, auto
from typing import TYPE_CHECKING, Union

from sdc11073.exceptions import ApiUsageError
from sdc11073.mdib.transactionsprotocol import (
    AnyTransactionManagerProtocol,
    TransactionItem,
    TransactionResultProtocol,
    TransactionType,
)

if TYPE_CHECKING:
    from sdc11073.loghelper import LoggerAdapter
    from sdc11073.mdib.descriptorcontainers import AbstractDescriptorProtocol
    from sdc11073.mdib.statecontainers import AbstractMultiStateProtocol, AbstractStateProtocol

    from .entities import (
        ProviderEntity,
        ProviderInternalEntity,
        ProviderInternalMultiStateEntity,
        ProviderMultiStateEntity,
    )
    from .entity_providermdib import EntityProviderMdib, ProviderInternalEntityType

    AnyProviderEntity = Union[
        ProviderEntity, ProviderMultiStateEntity, ProviderInternalEntity, ProviderInternalMultiStateEntity]


class _Modification(Enum):
    insert = auto()
    update = auto()
    delete = auto()


@dataclass(frozen=True)
class DescriptorTransactionItem:
    """Transaction Item with old and new container."""

    entity: ProviderEntity | ProviderMultiStateEntity | ProviderInternalEntity | ProviderInternalMultiStateEntity
    modification: _Modification


def _update_multi_states(mdib: EntityProviderMdib, # noqa: C901
                         new: ProviderMultiStateEntity,
                         old: ProviderMultiStateEntity,
                         modified_handles: list[str] | None = None,
                         adjust_state_version: bool = True):
    if not (new.is_multi_state and old.is_multi_state):
        raise ApiUsageError('_update_multi_states only handles context states!')
    if new.handle != old.handle:
        raise ApiUsageError(f'_update_multi_states found different handles! new={new.handle}, old = {old.handle}')
    if not modified_handles:
        modified_handles = new.states.keys()
    for handle in modified_handles:
        state_container = new.states.get(handle)
        if state_container is None:
            # a deleted state : this cannot be communicated via notification.
            # delete it internal_entity anf that is all
            if handle in old.states:
                old.states.pop(handle)
            else:
                raise KeyError(f'invalid handle {handle}!')
            continue

        old_state = old.states.get(state_container.Handle)
        tmp = copy.deepcopy(state_container)

        if old_state is None:
            # this is a new state
            tmp.descriptor_container = old.descriptor
            tmp.DescriptorVersion = old.descriptor.DescriptorVersion
            if adjust_state_version:
                old_state_version = mdib.state_handle_version_lookup.get(tmp.Handle)
                if old_state_version:
                    tmp.StateVersion = old_state_version + 1
        elif adjust_state_version:
            tmp.StateVersion = old_state.StateVersion + 1


def _adjust_version_counters(new_entity: ProviderInternalEntityType,
                             old_entity: ProviderInternalEntityType,
                             increment_descriptor_version: bool = False):
    if increment_descriptor_version:
        new_entity.descriptor.DescriptorVersion = old_entity.descriptor.DescriptorVersion + 1
    if new_entity.is_multi_state:
        for new_state in new_entity.states.values():
            new_state.DescriptorVersion = new_entity.descriptor.DescriptorVersion
            old_state = old_entity.states.get(new_state.Handle)
            if old_state is not None:
                new_state.StateVersion = old_state.StateVersion + 1
    else:
        new_entity.state.DescriptorVersion = new_entity.descriptor.DescriptorVersion
        new_entity.state.StateVersion = old_entity.state.StateVersion + 1


class _TransactionBase:
    def __init__(self,
                 provider_mdib: EntityProviderMdib,
                 logger: LoggerAdapter):
        self._mdib = provider_mdib
        # provide the new mdib version that the commit of this transaction will create
        self.new_mdib_version = provider_mdib.mdib_version + 1
        self._logger = logger
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
            if transaction_item.old is not None and transaction_item.new is not None:
                # update
                entity = self._mdib.internal_entities[transaction_item.old.DescriptorHandle]
                if entity.descriptor.is_context_descriptor:
                    entity.states[transaction_item.old.Handle] = transaction_item.new
                else:
                    entity.state = transaction_item.new
            elif transaction_item.new is not None:
                # insert
                entity = self._mdib.internal_entities[transaction_item.new.DescriptorHandle]
                if entity.descriptor.is_context_descriptor:
                    entity.states[transaction_item.new.Handle] = transaction_item.new
                else:
                    entity.state = transaction_item.new
            else:
                # delete
                entity = self._mdib.internal_entities[transaction_item.old.DescriptorHandle]
                if entity.descriptor.is_context_descriptor:
                    entity.states.pop(transaction_item.old.Handle)

            if transaction_item.new is not None:
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
                 provider_mdib: EntityProviderMdib,
                 logger: LoggerAdapter):
        super().__init__(provider_mdib, logger)
        self.descriptor_updates: dict[str, DescriptorTransactionItem] = {}
        self._new_entities: dict[str, ProviderInternalEntity | ProviderInternalMultiStateEntity] = {}

    def transaction__entity(self, descriptor_handle: str) -> ProviderEntity | ProviderMultiStateEntity | None:
        """Return the entity in open transaction if it exists.

        The descriptor can already be part of the transaction, and e.g. in pre_commit handlers of role providers
        it can be necessary to have access to it.
        """
        if not descriptor_handle:
            raise ValueError('No handle for descriptor specified')
        tr_container = self.descriptor_updates.get(descriptor_handle)
        if tr_container is not None:
            if tr_container.modification == _Modification.delete:
                raise ValueError(f'The descriptor {descriptor_handle} is going to be deleted')
            return tr_container.entity
        return None

    def write_entity(self, # noqa: PLR0912, C901
                     entity: ProviderEntity | ProviderMultiStateEntity,
                     adjust_version_counter: bool = True):
        """Insert or update an entity."""
        descriptor_handle = entity.descriptor.Handle
        if descriptor_handle in self.descriptor_updates:
            raise ValueError(f'Entity {descriptor_handle} already in updated set!')
        tmp = copy.copy(entity)  # cannot deepcopy entity, that would deepcopy also whole mdib
        tmp.descriptor = copy.deepcopy(entity.descriptor) # do not touch original entity of user
        if entity.is_multi_state:
            tmp.states = copy.deepcopy(entity.states) # do not touch original entity of user
        else:
            tmp.state = copy.deepcopy(entity.state) # do not touch original entity of user
        if descriptor_handle in self._mdib.internal_entities:
            # update
            if adjust_version_counter:
                old_entity = self._mdib.internal_entities[descriptor_handle]
                _adjust_version_counters(tmp, old_entity, increment_descriptor_version=True)
            self.descriptor_updates[descriptor_handle] = DescriptorTransactionItem(tmp,
                                                                                   _Modification.update)

        elif descriptor_handle in self._mdib.new_entities:
            # create
            if adjust_version_counter:
                version = self._mdib.descr_handle_version_lookup.get(descriptor_handle)
                if version is not None:
                    tmp.descriptor.DescriptorVersion = version
                if tmp.is_multi_state:
                    for state in tmp.states.values():
                        version = self._mdib.state_handle_version_lookup.get(state.Handle)
                        if version is not None:
                            state.StateVersion = version
                else:
                    version = self._mdib.state_handle_version_lookup.get(descriptor_handle)
                    if version is not None:
                        tmp.state.StateVersion = version
            self.descriptor_updates[descriptor_handle] = DescriptorTransactionItem(tmp,
                                                                                   _Modification.insert)
        else:
            # create without having internal entity
            tmp_entity = self._mdib.entities.new_entity(entity.node_type, entity.handle, entity.parent_handle)
            # replace descriptor and state in tmp_entity with values from tmp, but keep existing version counters
            descriptor_version = tmp_entity.descriptor.DescriptorVersion
            tmp_entity.descriptor = tmp.descriptor
            tmp_entity.descriptor.DescriptorVersion = descriptor_version
            if entity.is_multi_state:
                tmp_entity.states = tmp.states
                # change state versions if they were deleted before
                for handle, state in tmp_entity.states.items():
                    if handle in self._mdib.state_handle_version_lookup:
                        state.StateVersion = self._mdib.state_handle_version_lookup[handle] + 1
            else:
                state_version = tmp_entity.state.StateVersion
                tmp_entity.state = tmp.state
                tmp_entity.state.StateVersion = state_version
                tmp_entity.state.DescriptorVersion = descriptor_version
            self.descriptor_updates[descriptor_handle] = DescriptorTransactionItem(tmp_entity,
                                                                                   _Modification.insert)

    def write_entities(self,
                       entities: list[ProviderEntity | ProviderMultiStateEntity],
                       adjust_version_counter: bool = True):
        """Write entities in order parents first."""
        written_handles = []
        ent_dict = {ent.handle: ent for ent in entities}
        while len(written_handles) < len(ent_dict):
            for handle, ent in ent_dict.items():
                write_now = True
                if (ent.parent_handle is not None
                        and ent.parent_handle in ent_dict
                        and ent.parent_handle not in written_handles):
                        # it has a parent, and parent has not been written yet
                        write_now = False
                if write_now and handle not in written_handles:
                    self.write_entity(ent, adjust_version_counter)
                    written_handles.append(handle)

    def remove_entity(self, entity: ProviderEntity | ProviderMultiStateEntity):
        """Remove existing descriptor from mdib."""
        if entity.handle in self.descriptor_updates:
            raise ValueError(f'Descriptor {entity.handle} already in updated set!')

        internal_entity = self._mdib.internal_entities.get(entity.handle)
        if internal_entity:
            self.descriptor_updates[entity.handle] = DescriptorTransactionItem(internal_entity,
                                                                               _Modification.delete)

    def process_transaction(self, set_determination_time: bool) -> TransactionResultProtocol:  # noqa: ARG002, PLR0915, PLR0912, C901
        """Process transaction and create a TransactionResult.

        The parameter set_determination_time is only present in order to implement the interface correctly.
        Determination time is not set, because descriptors have no modification time.
        """
        proc = TransactionResult()
        if self.descriptor_updates:
            proc.new_mdib_version = self.new_mdib_version
            # need to know all to be deleted and to be created descriptors
            to_be_deleted_handles = [tr_item.entity.descriptor.Handle for tr_item in self.descriptor_updates.values()
                                     if tr_item.modification == _Modification.delete]
            to_be_created_handles = [tr_item.entity.descriptor.Handle for tr_item in self.descriptor_updates.values()
                                     if tr_item.modification == _Modification.insert]
            to_be_updated_handles = [tr_item.entity.descriptor.Handle for tr_item in self.descriptor_updates.values()
                                     if tr_item.modification == _Modification.update]

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

            # Restrict transaction to only insert, update or delete stuff. No mixes!
            # This simplifies handling a lot!
            types = [l for l in (to_be_deleted_handles, to_be_created_handles, to_be_updated_handles) if l]
            if not types:
                return proc  # nothing changed
            if len(types) > 1:
                raise ValueError('this transaction can only handle one of insert, update, delete!')

            for tr_item in self.descriptor_updates.values():
                if tr_item.modification == _Modification.insert:
                    # this is a create operation
                    new_entity = tr_item.entity

                    self._logger.debug(  # noqa: PLE1205
                        'transaction_manager: new entity Handle={}, node type={}',
                        new_entity.handle, new_entity.descriptor.NODETYPE)

                    # move temporary new internal entity to mdib
                    internal_entity = self._mdib.new_entities[new_entity.handle]
                    self._mdib.internal_entities[new_entity.handle] = internal_entity
                    del self._mdib.new_entities[new_entity.handle]

                    self._update_internal_entity(new_entity, internal_entity)

                    proc.descr_created.append(
                        internal_entity.descriptor)  # this will cause a Description Modification Report
                    state_update_list = proc.get_state_updates_list(new_entity.descriptor)

                    if internal_entity.is_multi_state:
                        state_update_list.extend(internal_entity.states)
                        # Todo: update context state handles in mdib

                    else:
                        state_update_list.append(internal_entity.state)

                    if (internal_entity.parent_handle is not None
                            and internal_entity.parent_handle not in to_be_created_handles):
                        self._increment_parent_descriptor_version(proc, internal_entity)

                elif tr_item.modification == _Modification.delete:
                    # this is a delete operation

                    # Todo: is tr_item.entity always an internal entity?
                    handle = tr_item.entity.descriptor.Handle
                    internal_entity = self._mdib.internal_entities.get(handle)
                    if internal_entity is None:
                        self._logger.debug(  # noqa: PLE1205
                            'transaction_manager: cannot remove unknown descriptor Handle={}', handle)
                        return None

                    self._logger.debug(  # noqa: PLE1205
                        'transaction_manager: rm descriptor Handle={}', handle)
                    all_entities = self._mdib.xtra.get_all_entities_in_subtree(internal_entity)
                    for entity in all_entities:

                        # save last versions
                        self._mdib.descr_handle_version_lookup[
                            entity.descriptor.Handle] = entity.descriptor.DescriptorVersion
                        if entity.is_multi_state:
                            for state in entity.states.values():
                                self._mdib.state_handle_version_lookup[state.Handle] = state.StateVersion
                        else:
                            self._mdib.state_handle_version_lookup[entity.descriptor.Handle] = entity.state.StateVersion

                        self._mdib.internal_entities.pop(entity.handle)
                    proc.descr_deleted.extend([e.descriptor for e in all_entities])
                    # increment DescriptorVersion if a child descriptor is added or deleted.
                    if internal_entity.parent_handle is not None \
                            and internal_entity.parent_handle not in to_be_deleted_handles:
                        # Todo: whole parent chain should be checked
                        # only update parent if it is not also deleted in this transaction
                        self._increment_parent_descriptor_version(proc, internal_entity)
                else:
                    # this is an update operation
                    # it does not change tr_item.entity!
                    # Todo: check if state changes exist and raise an error in that case.
                    #       It simplifies code a lot if it is safe to assume that states
                    #       have not changed in description transaction
                    updated_entity = tr_item.entity
                    internal_entity = self._mdib.internal_entities[tr_item.entity.handle]
                    self._logger.debug(  # noqa: PLE1205
                        'transaction_manager: update descriptor Handle={}, DescriptorVersion={}',
                        internal_entity.handle, updated_entity.descriptor.DescriptorVersion)
                    self._update_internal_entity(updated_entity, internal_entity)
                    proc.descr_updated.append(
                        internal_entity.descriptor)  # this will cause a Description Modification Report
                    state_update_list = proc.get_state_updates_list(internal_entity.descriptor)
                    if updated_entity.is_multi_state:
                        state_update_list.extend(internal_entity.states.values())
                        # Todo: update context state handles in mdib

                    else:
                        state_update_list.append(internal_entity.state)
        return proc

    def _update_internal_entity(self, modified_entity: ProviderEntity | ProviderMultiStateEntity,
                                internal_entity: ProviderInternalEntity | ProviderInternalMultiStateEntity):
        """Write back information into internal entity."""
        new_descriptor_version = internal_entity.descriptor.DescriptorVersion + 1
        internal_entity.descriptor.update_from_other_container(modified_entity.descriptor)

        if modified_entity.is_multi_state:
            _update_multi_states(self._mdib,
                                 modified_entity,
                                 internal_entity,
                                 None)
            # Todo: update context state handles in mdib

        else:
            internal_entity.state.update_from_other_container(modified_entity.state)

    def _increment_parent_descriptor_version(self, proc: TransactionResult,
                                             entity: ProviderInternalEntityType):
        """Increment version counter of descriptor and state.

        Add both to transaction result.
        """
        parent_entity = self._mdib.internal_entities.get(entity.parent_handle)
        updates_list = proc.get_state_updates_list(parent_entity.descriptor)

        if parent_entity is not None:
            parent_entity.descriptor.increment_descriptor_version()
            # parent entity can never be a multi state
            parent_entity.state.increment_state_version()

            # Todo: why make a copy?
            proc.descr_updated.append(parent_entity.descriptor.mk_copy())
            updates_list.append(parent_entity.state.mk_copy())

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
                 provider_mdib: EntityProviderMdib,
                 logger: LoggerAdapter):
        super().__init__(provider_mdib, logger)
        self._state_updates = {}  # will be set to proper value in derived classes

    def has_state(self, descriptor_handle: str) -> bool:
        """Check if transaction has a state with given handle."""
        return descriptor_handle in self._state_updates

    def write_entity(self, entity: ProviderEntity, adjust_version_counter: bool = True):
        """Update the state of the entity."""
        if not self._is_correct_state_type(entity.state):
            raise ApiUsageError(f'Wrong data type in transaction! {self.__class__.__name__}, {entity.state}')
        descriptor_handle = entity.state.DescriptorHandle
        old_state = self._mdib.internal_entities[descriptor_handle].state
        tmp = copy.deepcopy(entity.state)  # do not touch original entity of user
        if adjust_version_counter:
            tmp.DescriptorVersion = old_state.DescriptorVersion
            tmp.StateVersion = old_state.StateVersion + 1
        self._state_updates[descriptor_handle] = TransactionItem(old=old_state,
                                                                 new=tmp)

    def write_entities(self, entities: list[ProviderEntity], adjust_version_counter: bool = True):
        """Update the states of entities."""
        for entity in entities:
            # check all states before writing any of them
            if not self._is_correct_state_type(entity.state):
                raise ApiUsageError(f'Wrong data type in transaction! {self.__class__.__name__}, {entity.state}')
        for ent in entities:
            self.write_entity(ent, adjust_version_counter)

    @staticmethod
    def _is_correct_state_type(state: AbstractStateProtocol) -> bool:  # noqa: ARG004
        return False


class AlertStateTransaction(StateTransactionBase):
    """A Transaction for alert states."""

    def __init__(self,
                 provider_mdib: EntityProviderMdib,
                 logger: LoggerAdapter):
        super().__init__(provider_mdib, logger)
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
            proc.new_mdib_version = self.new_mdib_version
            updates = self._handle_state_updates(self._state_updates)
            proc.alert_updates.extend(updates)
        return proc

    @staticmethod
    def _is_correct_state_type(state: AbstractStateProtocol) -> bool:
        return state.is_alert_state


class MetricStateTransaction(StateTransactionBase):
    """A Transaction for metric states (except real time samples)."""

    def __init__(self,
                 provider_mdib: EntityProviderMdib,
                 logger: LoggerAdapter):
        super().__init__(provider_mdib, logger)
        self._state_updates = self.metric_state_updates

    def process_transaction(self, set_determination_time: bool) -> TransactionResultProtocol:
        """Process transaction and create a TransactionResult."""
        if set_determination_time:
            for tr_item in self._state_updates.values():
                if tr_item.new is not None and tr_item.new.MetricValue is not None:
                    tr_item.new.MetricValue.DeterminationTime = time.time()
        proc = TransactionResult()
        if self._state_updates:
            proc.new_mdib_version = self.new_mdib_version
            updates = self._handle_state_updates(self._state_updates)
            proc.metric_updates.extend(updates)
        return proc

    @staticmethod
    def _is_correct_state_type(state: AbstractStateProtocol) -> bool:
        return state.is_metric_state and not state.is_realtime_sample_array_metric_state


class ComponentStateTransaction(StateTransactionBase):
    """A Transaction for component states."""

    def __init__(self,
                 provider_mdib: EntityProviderMdib,
                 logger: LoggerAdapter):
        super().__init__(provider_mdib, logger)
        self._state_updates = self.component_state_updates

    def process_transaction(self, set_determination_time: bool) -> TransactionResultProtocol:  # noqa: ARG002
        """Process transaction and create a TransactionResult."""
        proc = TransactionResult()
        if self._state_updates:
            proc.new_mdib_version = self.new_mdib_version
            updates = self._handle_state_updates(self._state_updates)
            proc.comp_updates.extend(updates)
        return proc

    @staticmethod
    def _is_correct_state_type(state: AbstractStateProtocol) -> bool:
        return state.is_component_state


class RtStateTransaction(StateTransactionBase):
    """A Transaction for real time sample states."""

    def __init__(self,
                 provider_mdib: EntityProviderMdib,
                 logger: LoggerAdapter):
        super().__init__(provider_mdib, logger)
        self._state_updates = self.rt_sample_state_updates

    def process_transaction(self, set_determination_time: bool) -> TransactionResultProtocol:  # noqa: ARG002
        """Process transaction and create a TransactionResult."""
        proc = TransactionResult()
        if self._state_updates:
            proc.new_mdib_version = self.new_mdib_version
            updates = self._handle_state_updates(self._state_updates)
            proc.rt_updates.extend(updates)
        return proc

    @staticmethod
    def _is_correct_state_type(state: AbstractStateProtocol) -> bool:
        return state.is_realtime_sample_array_metric_state


class OperationalStateTransaction(StateTransactionBase):
    """A Transaction for operational states."""

    def __init__(self,
                 provider_mdib: EntityProviderMdib,
                 logger: LoggerAdapter):
        super().__init__(provider_mdib, logger)
        self._state_updates = self.operational_state_updates

    def process_transaction(self, set_determination_time: bool) -> TransactionResultProtocol:  # noqa: ARG002
        """Process transaction and create a TransactionResult."""
        proc = TransactionResult()
        if self._state_updates:
            proc.new_mdib_version = self.new_mdib_version
            updates = self._handle_state_updates(self._state_updates)
            proc.op_updates.extend(updates)
        return proc

    @staticmethod
    def _is_correct_state_type(state: AbstractStateProtocol) -> bool:
        return state.is_operational_state


class ContextStateTransaction(_TransactionBase):
    """A Transaction for context states."""

    def __init__(self,
                 provider_mdib: EntityProviderMdib,
                 logger: LoggerAdapter):
        super().__init__(provider_mdib, logger)
        self._state_updates = self.context_state_updates

    def write_entity(self, entity: ProviderMultiStateEntity,
                     modified_handles: list[str],
                     adjust_version_counter: bool = True):
        """Insert or update a context state in mdib."""
        internal_entity = self._mdib.internal_entities[entity.descriptor.Handle]

        for handle in modified_handles:
            state_container = entity.states.get(handle)
            if state_container is None:
                # a deleted state : this cannot be communicated via notification.
                # delete it internal_entity anf that is all
                if handle in internal_entity.states:
                    internal_entity.states.pop(handle)
                else:
                    raise KeyError(f'invalid handle {handle}!')
                continue
            if not state_container.is_context_state:
                raise ApiUsageError('Transaction only handles context states!')

            old_state = internal_entity.states.get(state_container.Handle)
            tmp = copy.deepcopy(state_container)  # do not touch original entity of user

            if old_state is None:
                # this is a new state
                tmp.descriptor_container = internal_entity.descriptor
                if adjust_version_counter:
                    tmp.DescriptorVersion = internal_entity.descriptor.DescriptorVersion
                    # look for previously existing state with same handle
                    old_state_version = self._mdib.state_handle_version_lookup.get(tmp.Handle)
                    if old_state_version:
                        tmp.StateVersion = old_state_version + 1
            # update
            elif adjust_version_counter:
                tmp.DescriptorVersion = internal_entity.descriptor.DescriptorVersion
                tmp.StateVersion = old_state.StateVersion + 1

            self._state_updates[state_container.Handle] = TransactionItem(old=old_state, new=tmp)

    def process_transaction(self, set_determination_time: bool) -> TransactionResultProtocol:  # noqa: ARG002
        """Process transaction and create a TransactionResult."""
        proc = TransactionResult()
        if self._state_updates:
            proc.new_mdib_version = self.new_mdib_version
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
        self.new_mdib_version: int | None = None
        self.descr_updated: list[AbstractDescriptorProtocol] = []
        self.descr_created: list[AbstractDescriptorProtocol] = []
        self.descr_deleted: list[AbstractDescriptorProtocol] = []
        self.metric_updates: list[AbstractStateProtocol] = []
        self.alert_updates: list[AbstractStateProtocol] = []
        self.comp_updates: list[AbstractStateProtocol] = []
        self.ctxt_updates: list[AbstractMultiStateProtocol] = []
        self.op_updates: list[AbstractStateProtocol] = []
        self.rt_updates: list[AbstractStateProtocol] = []

    @property
    def has_descriptor_updates(self) -> bool:
        """Return True if at least one descriptor is in result."""
        return len(self.descr_updated) > 0 or len(self.descr_created) > 0 or len(self.descr_deleted) > 0

    def all_states(self) -> list[AbstractStateProtocol]:
        """Return all states in this transaction."""
        return self.metric_updates + self.alert_updates + self.comp_updates + self.ctxt_updates \
            + self.op_updates + self.rt_updates

    def get_state_updates_list(self, descriptor: AbstractDescriptorProtocol) -> list:
        """Return the list that stores updated states of this descriptor."""
        if descriptor.is_context_descriptor:
            return self.ctxt_updates
        if descriptor.is_alert_descriptor:
            return self.alert_updates
        if descriptor.is_realtime_sample_array_metric_descriptor:
            return self.rt_updates
        if descriptor.is_metric_descriptor:
            return self.metric_updates
        if descriptor.is_operational_descriptor:
            return self.op_updates
        if descriptor.is_component_descriptor:
            return self.comp_updates
        raise ValueError(f'do not know how to handle {descriptor}')


_transaction_type_lookup = {TransactionType.descriptor: DescriptorTransaction,
                            TransactionType.alert: AlertStateTransaction,
                            TransactionType.metric: MetricStateTransaction,
                            TransactionType.operational: OperationalStateTransaction,
                            TransactionType.context: ContextStateTransaction,
                            TransactionType.component: ComponentStateTransaction,
                            TransactionType.rt_sample: RtStateTransaction}


def mk_transaction(provider_mdib: EntityProviderMdib,
                   transaction_type: TransactionType,
                   logger: LoggerAdapter) -> AnyTransactionManagerProtocol:
    """Create a transaction according to transaction_type."""
    return _transaction_type_lookup[transaction_type](provider_mdib, logger)
