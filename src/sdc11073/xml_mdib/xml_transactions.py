from __future__ import annotations

import copy
import time
import uuid
from enum import Enum, auto
from typing import TYPE_CHECKING, cast, Union
from dataclasses import dataclass
from sdc11073.exceptions import ApiUsageError

from sdc11073.mdib.statecontainers import AbstractMultiStateProtocol
from sdc11073.mdib.transactionsprotocol import (
    AnyTransactionManagerProtocol,
    TransactionItem,
    TransactionResultProtocol,
    TransactionType,
)

if TYPE_CHECKING:
    from lxml.etree import QName
    from sdc11073.loghelper import LoggerAdapter

    from sdc11073.mdib.descriptorcontainers import AbstractDescriptorProtocol
    from sdc11073.mdib.statecontainers import AbstractStateProtocol
    from .xml_entities import ProviderEntity, ProviderMultiStateEntity, ProviderInternalEntity, ProviderInternalMultiStateEntity
    from .xml_providermdib import XmlProviderMdib, ProviderInternalEntityType

    AnyProviderEntity = Union[ProviderEntity, ProviderMultiStateEntity, ProviderInternalEntity, ProviderInternalMultiStateEntity]

class _Modification(Enum):
    insert = auto()
    update = auto()
    delete = auto()

@dataclass(frozen=True)
class DescriptorTransactionItem:
    """Transaction Item with old and new container."""
    entity: ProviderEntity | ProviderMultiStateEntity | ProviderInternalEntity | ProviderInternalMultiStateEntity
    modification: _Modification


class _TransactionBase:
    def __init__(self,
                 device_mdib_container: XmlProviderMdib,
                 logger: LoggerAdapter,
                 manage_version_counters: bool):
        self._mdib = device_mdib_container
        # provide the new mdib version that the commit of this transaction will create
        self.new_mdib_version = device_mdib_container.mdib_version + 1
        self._logger = logger
        self.manage_version_counters = manage_version_counters
        # self.descriptor_updates: dict[str, DescriptorTransactionItem] = {}
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
                entity = self._mdib._entities[transaction_item.old.DescriptorHandle]
                if entity.descriptor.is_context_descriptor:
                    entity.states[transaction_item.old.Handle] =  transaction_item.new
                else:
                    entity.state = transaction_item.new
            elif transaction_item.new is not None:
                # insert
                entity = self._mdib._entities[transaction_item.new.DescriptorHandle]
                if entity.descriptor.is_context_descriptor:
                    entity.states[transaction_item.new.Handle] = transaction_item.new
                else:
                    entity.state = transaction_item.new
            else:
                # delete
                entity = self._mdib._entities[transaction_item.old.DescriptorHandle]
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
                 device_mdib_container: XmlProviderMdib,
                 logger: LoggerAdapter,
                 manage_version_counters: bool = True):
        super().__init__(device_mdib_container, logger, manage_version_counters)
        self.descriptor_updates: dict[str, DescriptorTransactionItem] = {}
        self._new_entities: dict[str, ProviderInternalEntity | ProviderInternalMultiStateEntity] = {}

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

    def handle_entity(self,
                       entity: ProviderEntity | ProviderMultiStateEntity,
                       adjust_descriptor_version: bool = True):
        """insert or update an entity."""
        descriptor_handle = entity.descriptor.Handle
        if descriptor_handle in self.descriptor_updates:
            raise ValueError(f'Entity {descriptor_handle} already in updated set!')

        if descriptor_handle in self._mdib._entities:
            self.descriptor_updates[descriptor_handle] = DescriptorTransactionItem(entity,
                                                                                   _Modification.update)

        elif descriptor_handle in self._mdib._new_entities:
            self.descriptor_updates[descriptor_handle] = DescriptorTransactionItem(entity,
                                                                                   _Modification.insert)
        else:
            raise ValueError(f'Entity {descriptor_handle} is not known!')

        # Todo: adjust_descriptor_version


    def remove_descriptor(self, descriptor_handle: str):
        """Remove existing descriptor from mdib."""
        if not descriptor_handle:
            raise ValueError('No handle for descriptor specified')
        if descriptor_handle in self.descriptor_updates:
            raise ValueError(f'Descriptor {descriptor_handle} already in updated set!')

        internal_entity  = self._mdib._entities.get(descriptor_handle)
        if internal_entity:
            self.descriptor_updates[descriptor_handle] = DescriptorTransactionItem(internal_entity,
                                                                                   _Modification.delete)


    def process_transaction(self, set_determination_time: bool,
                            manage_version_counters: bool = True) -> TransactionResultProtocol:  # noqa: ARG002
        """Process transaction and create a TransactionResult.

        The parameter set_determination_time is only present in order to implement the interface correctly.
        Determination time is not set, because descriptors have no modification time.
        """
        proc = TransactionResult()
        if self.descriptor_updates:
            self._mdib.mdib_version = self.new_mdib_version
            # need to know all to be deleted and to be created descriptors
            # to_be_deleted_handles = [tr_item.old.handle for tr_item in self.descriptor_updates.values()
            #                          if tr_item.new is None and tr_item.old is not None]
            # to_be_created_handles = [tr_item.new.handle for tr_item in self.descriptor_updates.values()
            #                          if tr_item.old is None and tr_item.new is not None]
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
            types = [ l for l in (to_be_deleted_handles, to_be_created_handles, to_be_updated_handles) if l]
            if not types:
                return  # nothing changed
            if len (types) > 1:
                raise ValueError('this transaction can only handle one of insert, update, delete!')

            for tr_item in self.descriptor_updates.values():
                if tr_item.modification == _Modification.insert:
                    # this is a create operation
                    new_entity = tr_item.entity

                    self._logger.debug(  # noqa: PLE1205
                        'transaction_manager: new entity Handle={}, node type={}',
                        new_entity.handle, new_entity.descriptor.NODETYPE)

                    # move temporary new internal entity to mdib
                    internal_entity = self._mdib._new_entities[new_entity.handle]
                    self._mdib._entities[new_entity.handle] = internal_entity
                    del self._mdib._new_entities[new_entity.handle]

                    self._update_internal_entity(new_entity, internal_entity, manage_version_counters)

                    proc.descr_created.append(internal_entity.descriptor)  # this will cause a Description Modification Report
                    state_update_list = proc.get_state_updates_list(new_entity.descriptor)

                    if internal_entity.is_multi_state:
                        state_update_list.extend(internal_entity.states)
                        #Todo: update context state handles in mdib

                    else:
                        state_update_list.append(internal_entity.state)

                    if (internal_entity.parent_handle is not None
                            and internal_entity.parent_handle not in to_be_created_handles
                            and manage_version_counters):
                        self._increment_parent_descriptor_version(proc, internal_entity)

                elif tr_item.modification == _Modification.delete:
                    # this is a delete operation

                    # Todo: is tr_item.entity always an internal entity?
                    handle = tr_item.entity.descriptor.Handle
                    internal_entity = self._mdib._entities.get(handle)
                    if internal_entity is None:
                        self._logger.debug(  # noqa: PLE1205
                            'transaction_manager: cannot remove unknown descriptor Handle={}',handle)
                        return

                    self._logger.debug(  # noqa: PLE1205
                        'transaction_manager: rm descriptor Handle={}', handle)
                    all_entities = self._mdib.xtra.get_all_entities_in_subtree(internal_entity)
                    for entity in all_entities:
                        self._mdib._entities.pop(entity.handle)
                    proc.descr_deleted.extend([e.descriptor for e in all_entities])
                    # increment DescriptorVersion if a child descriptor is added or deleted.
                    if internal_entity.parent_handle is not None \
                            and internal_entity.parent_handle not in to_be_deleted_handles\
                            and manage_version_counters:
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
                    internal_entity = self._mdib._entities[tr_item.entity.handle]
                    self._logger.debug(  # noqa: PLE1205
                        'transaction_manager: update descriptor Handle={}, DescriptorVersion={}',
                        internal_entity.handle, updated_entity.descriptor.DescriptorVersion)
                    self._update_internal_entity(updated_entity, internal_entity, manage_version_counters)
                    proc.descr_updated.append(internal_entity.descriptor)  # this will cause a Description Modification Report
                    state_update_list = proc.get_state_updates_list(internal_entity.descriptor)
                    if updated_entity.is_multi_state:
                        state_update_list.extend(internal_entity.states)
                        #Todo: update context state handles in mdib

                    else:
                        state_update_list.append(internal_entity.state)
        return proc

    def _update_internal_entity(self, modified_entity: ProviderEntity | ProviderMultiStateEntity,
                                internal_entity: ProviderInternalEntity | ProviderInternalMultiStateEntity,
                                manage_version_counters: bool):
        new_descriptor_version = internal_entity.descriptor.DescriptorVersion + 1
        internal_entity.descriptor.update_from_other_container(modified_entity.descriptor)
        if manage_version_counters:
            internal_entity.descriptor.DescriptorVersion = new_descriptor_version

        if modified_entity.is_multi_state:
            raise NotImplementedError
            for state in new_entity.states:
                new_state_version = orig_entity.state.DescriptorVersion + 1

                state.DescriptorVersion = new_entity.descriptor.DescriptorVersion
                state.StateVersion += 1
            orig_entity.states = new_entity.states
            state_update_list.extend(new_entity.states)
            # Todo: update context state handles in mdib

        else:
            new_state_version = internal_entity.state.StateVersion + 1
            internal_entity.state.update_from_other_container(modified_entity.state)
            if manage_version_counters:
                internal_entity.state.DescriptorVersion = internal_entity.descriptor.DescriptorVersion
                internal_entity.state.StateVersion = new_state_version

    def _increment_parent_descriptor_version(self, proc: TransactionResult,
                                             entity: ProviderInternalEntityType):
        """Increment version counter of descriptor and state.

        Add both to transaction result."""
        parent_entity = self._mdib._entities.get(entity.parent_handle)
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
                 device_mdib_container: XmlProviderMdib,
                 logger: LoggerAdapter,
                 manage_version_counters: bool = True):
        super().__init__(device_mdib_container, logger, manage_version_counters)
        self._state_updates = {}  # will be set to proper value in derived classes

    def has_state(self, descriptor_handle: str) -> bool:
        """Check if transaction has a state with given handle."""
        return descriptor_handle in self._state_updates

    def add_state(self, state_container: AbstractStateProtocol):

        if not self._is_correct_state_type(state_container):
                raise ApiUsageError(f'Wrong data type in transaction! {self.__class__.__name__}, {state_container}')
        descriptor_handle = state_container.DescriptorHandle
        old_state = self._mdib._entities[descriptor_handle].state
        tmp = copy.deepcopy(state_container)
        if self.manage_version_counters and old_state is not None:
            tmp.StateVersion = old_state.StateVersion + 1
        self._state_updates[descriptor_handle] = TransactionItem(old=old_state,
                                                                 new=tmp)

    @staticmethod
    def _is_correct_state_type(state: AbstractStateProtocol) -> bool:  # noqa: ARG004
        return False


class AlertStateTransaction(StateTransactionBase):
    """A Transaction for alert states."""

    def __init__(self,
                 device_mdib_container: XmlProviderMdib,
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
                 device_mdib_container: XmlProviderMdib,
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
                 device_mdib_container: XmlProviderMdib,
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
                 device_mdib_container: XmlProviderMdib,
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
                 device_mdib_container: XmlProviderMdib,
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
                 device_mdib_container: XmlProviderMdib,
                 logger: LoggerAdapter,
                 manage_version_counters: bool = True):
        super().__init__(device_mdib_container, logger, manage_version_counters)
        self._state_updates = self.context_state_updates

    def add_state(self, state_container: AbstractMultiStateProtocol, adjust_state_version: bool = True):
        """Insert or update a context state in mdib."""
        if not state_container.is_context_state:
            # prevent this for simplicity reasons
            raise ApiUsageError('Transaction only handles context states!')

        internal_entity = self._mdib._entities[state_container.DescriptorHandle]

        if state_container.descriptor_container is None:
            state_container.descriptor_container = internal_entity.descriptor
            state_container.DescriptorVersion = internal_entity.descriptor.DescriptorVersion

        old_state = internal_entity.states.get(state_container.Handle)

        tmp = copy.deepcopy(state_container)
        if self.manage_version_counters and old_state is not None:
            tmp.StateVersion = old_state.StateVersion + 1

        if adjust_state_version:
            # ToDo: implement:
            pass
            # self._mdib.context_states.set_version(state_container)
        self._state_updates[state_container.Handle] = TransactionItem(old=old_state, new=tmp)

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

    def get_state_updates_list(self, descriptor: AbstractDescriptorProtocol):
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


def mk_transaction(provider_mdib: XmlProviderMdib,
                   transaction_type: TransactionType,
                   logger: LoggerAdapter) -> AnyTransactionManagerProtocol:
    """Create a transaction according to transaction_type."""
    return _transaction_type_lookup[transaction_type](provider_mdib, logger)
