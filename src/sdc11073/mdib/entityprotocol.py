from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, Union, Iterable

if TYPE_CHECKING:
    from lxml.etree import QName
    from sdc11073.mdib.descriptorcontainers import AbstractDescriptorContainer
    from sdc11073.mdib.statecontainers import AbstractMultiStateContainer, AbstractStateContainer
    from sdc11073.xml_types.pm_types import Coding, CodedValue

class EntityProtocol(Protocol):
    descriptor: AbstractDescriptorContainer
    state: AbstractStateContainer
    is_multi_state: bool

    def update(self):
        ...

class MultiStateEntityProtocol(Protocol):
    descriptor: AbstractDescriptorContainer
    states: dict[str, AbstractMultiStateContainer]   # key is the Handle member of state
    is_multi_state: bool

    def update(self):
        ...


EntityTypeProtocol = Union[EntityProtocol, MultiStateEntityProtocol]


# Todo: should node_type be QName (this assumes that we talk XML) or just Any to be generic?

class EntityGetterProtocol(Protocol):
    """This protocol defines a way to access mdib data as entities.

    This representation is independent of the internal mdib organization.
    The entities returned by the provided getter methods contain copies of the internal mdib data.
    Changing the data does not change data in the mdib.
    Use the EntityTransactionProtocol to write data back to the mdib."""


    def handle(self, handle: str) -> EntityTypeProtocol:
        """Return entity with given descriptor handle."""
        ...

    def context_handle(self, handle: str) -> MultiStateEntityProtocol:
        """Return multi state entity that contains a state with given handle."""
        ...

    def node_type(self, node_type: QName) -> list[EntityTypeProtocol]:
        """Return all entities with given node type."""
        ...

    def parent_handle(self, parent_handle: str | None) -> list[EntityTypeProtocol]:
        """Return all entities with given parent handle."""
        ...

    def coding(self, coding: Coding) -> list[EntityTypeProtocol]:
        """Return all entities with equivalent Coding."""
        ...

    def coded_value(self, coded_value: CodedValue) -> list[EntityTypeProtocol]:
        """Return all entities with equivalent CodedValue."""
        ...

    def items(self) -> Iterable[tuple[str,EntityTypeProtocol]]:
        """Like items() of a dictionary."""
        ...

    def __len__(self) -> int:
        """Return number of entities"""
        ...


class ProviderEntityGetterProtocol(EntityGetterProtocol):

    def new_entity(self,
            node_type: QName,
            handle: str,
            parent_handle: str) -> EntityTypeProtocol:
        """Create an entity."""
        ...

    def new_state(self,
            entity: MultiStateEntityProtocol,
            handle: str | None = None,
            ) -> AbstractMultiStateContainer:
        """Create a new context state."""
        ...


class EntityDescriptorTransactionProtocol(Protocol):
    """This protocol is used together with EntityGetter protocol.

    In the "classic" mdib interface user has access to the original data in the mdib with the get methods,
    and this data should never be manipulated directly. It is the job of the transaction
    to give the user a copy than can be manipulated.

    The Entity interface handles this just the other way around: the data given to the user is always a separate
    instance that can be safely manipulated without changing the mdib data. The EntityTransaction writes that
    manipulated data back to the mdib and triggers sending of notifications.

    Mixing of classic interface and entity interface does not work!
    """
    def handle_entity(self,
                       entity: EntityTypeProtocol,
                       adjust_descriptor_version: bool = True):
        """insert or update an entity."""

    def remove_descriptor(self, descriptor_handle: str):
        """Remove existing descriptor from mdib."""

    def process_transaction(self, set_determination_time: bool,
                            manage_version_counters: bool = True) -> TransactionResultProtocol:  # noqa: ARG002
        """Process transaction and create a TransactionResult.

        The parameter set_determination_time is only present in order to implement the interface correctly.
        Determination time is not set, because descriptors have no modification time.
        """


class StateTransactionProtocol(Protocol):
    """Interface for all states that are not multi states."""

    def has_state(self, descriptor_handle: str) -> bool:
        """Check if transaction has a state with given handle."""

    def add_state(self, state_container: AbstractStateProtocol):
        "Update a state in mdib"

    def process_transaction(self, set_determination_time: bool) -> TransactionResultProtocol:
        """Process transaction and create a TransactionResult."""


class ContextStateTransaction(_TransactionBase):
    """A Transaction for context states."""

    def add_state(self, state_container: AbstractMultiStateProtocol, adjust_state_version: bool = True):
        ...