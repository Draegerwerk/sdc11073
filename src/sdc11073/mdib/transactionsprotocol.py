"""The module declares several protocols that are implemented by transactions.

Only these protocols shall be used, the old way of transactions in mdib.transactions should no longer be used.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Protocol, Union

from .statecontainers import AbstractMultiStateProtocol, AbstractStateProtocol

if TYPE_CHECKING:

    from .descriptorcontainers import AbstractDescriptorProtocol
    from .entityprotocol import EntityProtocol, EntityTypeProtocol, MultiStateEntityProtocol

class TransactionType(Enum):
    """The different kinds of transactions.

    Each type results in a different report.
    """

    descriptor = 1
    metric = 2
    alert = 3
    component = 4
    context = 5
    operational = 6
    rt_sample = 7


class TransactionResultProtocol(Protocol): # pragma: no cover
    """TransactionResult contains all state and descriptors that were modified in the transaction.

    The states and descriptors are used to create the notification(s) that keep the consumers up to date.
    """

    descr_updated: list[AbstractDescriptorProtocol]
    descr_created: list[AbstractDescriptorProtocol]
    descr_deleted: list[AbstractDescriptorProtocol]
    metric_updates: list[AbstractStateProtocol]
    alert_updates: list[AbstractStateProtocol]
    comp_updates = list[AbstractStateProtocol]
    ctxt_updates = list[AbstractMultiStateProtocol]
    op_updates = list[AbstractStateProtocol]
    rt_updates = list[AbstractStateProtocol]

    has_descriptor_updates: bool
    new_mdib_version: int

    def all_states(self) -> list[AbstractStateProtocol]:
        """Return all states in this transaction."""

class TransactionItemProtocol(Protocol): # pragma: no cover
    """A container for the old and the new version of a state or descriptor.

    If old is None, this is an object that is added to mdib.
    If new is None, this is an object that is deleted from mdib.
    If neither old nor new is None, this is an update to an existing object.
    """

    old: AbstractStateProtocol | AbstractDescriptorProtocol | None
    new: AbstractStateProtocol | AbstractDescriptorProtocol | None


@dataclass(frozen=True)
class TransactionItem:
    """Transaction Item with old and new container."""

    old: AbstractStateProtocol | AbstractDescriptorProtocol | None
    new: AbstractStateProtocol | AbstractDescriptorProtocol | None




class AbstractTransactionManagerProtocol(Protocol): # pragma: no cover
    """Interface of a TransactionManager."""

    new_mdib_version: int

    def process_transaction(self, set_determination_time: bool) -> TransactionResultProtocol:
        """Process the transaction."""

    # member variables that are available during a running transaction
    descriptor_updates: dict[str, TransactionItem]
    metric_state_updates: dict[str, TransactionItem]
    alert_state_updates: dict[str, TransactionItem]
    component_state_updates: dict[str, TransactionItem]
    context_state_updates: dict[str, TransactionItem]
    operational_state_updates: dict[str, TransactionItem]
    rt_sample_state_updates: dict[str, TransactionItem]
    error: bool


class EntityDescriptorTransactionManagerProtocol(AbstractTransactionManagerProtocol): # pragma: no cover
    """Entity based transaction manager for modification of descriptors (and associated states).

    The entity based transaction manager protocol can only be used with EntityGetter methods!
    The only working approach is:
        1. Read an entity from mdib with one of the EntityGetter Methods. These methods return a
           copy of the mdib data.
        2. Manipulate the copied data as required
        3. Create a transaction and write entity data back to mdib with write_entity method
    """

    def get_state_transaction_item(self, handle: str) -> TransactionItem | None:
        """If transaction has a state with given handle, return the transaction-item, otherwise None."""

    def transaction_entity(self, descriptor_handle: str) -> EntityTypeProtocol | None:
        """Return the entity in open transaction if it exists.

        The descriptor can already be part of the transaction, and e.g. in pre_commit handlers of role providers
        it can be necessary to have access to it.
        """

    def write_entity(self,
                     entity: EntityTypeProtocol,
                     adjust_version_counter: bool = True):
        """Insert or update an entity (state and descriptor)."""

    def write_entities(self,
                       entities: list[EntityTypeProtocol],
                       adjust_version_counter: bool = True):
        """Insert or update list of entities."""

    def remove_entity(self, entity: EntityTypeProtocol):
        """Remove existing descriptor from mdib."""


class EntityStateTransactionManagerProtocol(AbstractTransactionManagerProtocol): # pragma: no cover
    """Entity based transaction manager for modification of states.

    The entity based transaction manager protocol can only be used with EntityGetter methods!
    The only working approach is:
        1. Read an entity from mdib with one of the EntityGetter Methods. These methods return a
           copy of the mdib data.
        2. Manipulate the copied state as required
        3. Create a transaction and write entity data back to mdib with write_entity method
    """

    def has_state(self, descriptor_handle: str) -> bool:
        """Check if transaction has a state with given handle."""

    def write_entity(self,
                     entity: EntityProtocol,
                     adjust_version_counter: bool = True):
        """Update the state of the entity."""

    def write_entities(self,
                       entities: list[EntityProtocol],
                       adjust_version_counter: bool = True):
        """Update the states of entities."""


class EntityContextStateTransactionManagerProtocol(AbstractTransactionManagerProtocol): # pragma: no cover
    """Entity based transaction manager for modification of context states.

    The entity based transaction manager protocol can only be used with EntityGetter methods!
    The only working approach is:
        1. Read an entity from mdib with one of the EntityGetter Methods. These methods return a
           copy of the mdib data.
        2. Manipulate the copied states as required
        3. Create a descriptor transaction context and write entity data back to mdib with write_entity method
    """

    def write_entity(self, entity: MultiStateEntityProtocol,
                  modified_handles: list[str],
                  adjust_version_counter: bool = True):
        """Insert or update a context state in mdib."""


class DescriptorTransactionManagerProtocol(EntityDescriptorTransactionManagerProtocol): # pragma: no cover
    """The classic Interface of a TransactionManager that modifies descriptors.

    The classic transaction manager protocol can not be used with EntityGetter methods!
    The only working approach is:
        case A: update an existing descriptor:
        1. Start a descriptor transaction context
        2. call get_descriptor. This returns a copy of the descriptor in mdib
           Manipulate the copied descriptor as required
        3. optional: call get_state / get_context_state. This returns a copy of the state in mdib
           Manipulate the copied state as required

        case B: create a descriptor ( not context descriptor):
        1. Start a descriptor transaction context
        2. Create a new descriptor (and state instance if this is not a context state)
        3. Call add_descriptor and add_state

        case C: create a context descriptor:
        1. Start a descriptor transaction context
        2. Create a new descriptor
        3. Call mk_context_state  0... n times to add context states

        In all cases: when the transaction context is left, all before retrieved data is written back to mdib.
    """

    def actual_descriptor(self, descriptor_handle: str) -> AbstractDescriptorProtocol:
        """Look for new or updated descriptor in current transaction and in mdib."""

    def add_descriptor(self,
                       descriptor_container: AbstractDescriptorProtocol,
                       adjust_descriptor_version: bool = True,
                       state_container: AbstractStateProtocol | None = None):
        """Add a new descriptor to mdib."""

    def remove_descriptor(self, descriptor_handle: str):
        """Remove existing descriptor from mdib."""

    def get_descriptor(self, descriptor_handle: str) -> AbstractDescriptorProtocol:
        """Get a descriptor from mdib."""

    def has_state(self, descriptor_handle: str) -> bool:
        """Check if transaction has a state with given handle."""

    def add_state(self, state_container: AbstractStateProtocol, adjust_state_version: bool = True):
        """Add a new state to mdib."""

    def unget_state(self, state_container: AbstractStateProtocol):
        """Forget a state that was provided before by a get_state or add_state call."""

    def get_state(self, descriptor_handle: str) -> AbstractStateProtocol:
        """Read a state from mdib and add it to the transaction."""

    def get_context_state(self, context_state_handle: str) -> AbstractMultiStateProtocol:
        """Read a ContextState from mdib with given state handle."""

    def mk_context_state(self, descriptor_handle: str,
                         context_state_handle: str | None = None,
                         adjust_state_version: bool = True,
                         set_associated: bool = False) -> AbstractMultiStateProtocol:
        """Create a new ContextStateContainer."""


class StateTransactionManagerProtocol(EntityStateTransactionManagerProtocol): # pragma: no cover
    """The classic Interface of a TransactionManager that modifies states (except context states).

    The classic transaction manager protocol can not be used with EntityGetter methods!
    The only working approach is:
        1. Start a descriptor transaction context
        2. call get_state. This returns a copy of the state in mdib
           Manipulate the copied state as required

        When the transaction context is left, all before retrieved data is written back to mdib.
    """

    def has_state(self, descriptor_handle: str) -> bool:
        """Check if transaction has a state with given handle."""

    def get_state_transaction_item(self, handle: str) -> TransactionItemProtocol | None:
        """If transaction has a state with given handle, return the transaction-item, otherwise None."""

    def unget_state(self, state_container: AbstractStateProtocol):
        """Forget a state that was provided before by a get_state or add_state call."""

    def get_state(self, descriptor_handle: str) -> AbstractStateProtocol:
        """Read a state from mdib and add it to the transaction."""


class ContextStateTransactionManagerProtocol(EntityContextStateTransactionManagerProtocol): # pragma: no cover
    """The classic Interface of a TransactionManager that modifies context states.

    The classic transaction manager protocol can not be used with EntityGetter methods!
    The only working approach is:
        1. Start a descriptor transaction context
        2a.Call get_context_state if you want to manipulate an existing context state.
           This returns a copy of the state in mdib. Manipulate the copied state as required.
        2b.Call mk_context_state if you want to create a new context state.
           Manipulate the state as required.

        When the transaction context is left, all before retrieved data is written back to mdib.
    """

    def get_context_state(self, context_state_handle: str) -> AbstractMultiStateProtocol:
        """Read a ContextState from mdib with given state handle."""

    def mk_context_state(self, descriptor_handle: str,
                         context_state_handle: str | None = None,
                         adjust_state_version: bool = True,
                         set_associated: bool = False) -> AbstractMultiStateProtocol:
        """Create a new ContextStateContainer."""

    def disassociate_all(self,
                         context_descriptor_handle: str,
                         ignored_handle: str | None = None) -> list[str]:
        """Disassociate all associated states in mdib for context_descriptor_handle."""

AnyEntityTransactionManagerProtocol = Union[EntityContextStateTransactionManagerProtocol,
                                            EntityStateTransactionManagerProtocol,
                                            EntityDescriptorTransactionManagerProtocol]


AnyTransactionManagerProtocol = Union[ContextStateTransactionManagerProtocol,
                                      StateTransactionManagerProtocol,
                                      DescriptorTransactionManagerProtocol]

