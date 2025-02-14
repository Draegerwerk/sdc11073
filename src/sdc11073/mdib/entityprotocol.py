"""The module contains protocol definitions for the entity interface of mdib."""
from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Protocol, Union

if TYPE_CHECKING:
    from collections.abc import Iterable

    from lxml.etree import QName

    from sdc11073.mdib.descriptorcontainers import AbstractDescriptorProtocol
    from sdc11073.mdib.statecontainers import AbstractMultiStateProtocol, AbstractStateProtocol
    from sdc11073.xml_types.pm_types import CodedValue, Coding


class EntityProtocol(Protocol): # pragma: no cover
    """The protocol defines the interface of single-state entities."""

    descriptor: AbstractDescriptorProtocol
    state: AbstractStateProtocol
    is_multi_state: ClassVar[bool]

    @property
    def handle(self) -> str:
        """Return the handle of the descriptor."""

    @property
    def parent_handle(self) -> str | None:
        """Return the parent handle of the descriptor."""

    @property
    def node_type(self) -> QName:
        """Return the node type of the descriptor."""

    def update(self):
        """Update entity with current mdib data."""


class MultiStateEntityProtocol(Protocol): # pragma: no cover
    """The protocol defines the interface of multi-state entities."""

    descriptor: AbstractDescriptorProtocol
    states: dict[str, AbstractMultiStateProtocol]  # key is the Handle member of state
    is_multi_state: bool
    node_type: QName
    handle: str
    parent_handle: str

    def update(self):
        """Update entity with current data in mdib."""

    def new_state(self, state_handle: str | None = None) -> AbstractMultiStateProtocol:
        """Create a new state."""

EntityTypeProtocol = Union[EntityProtocol, MultiStateEntityProtocol]


class EntityGetterProtocol(Protocol): # pragma: no cover
    """The protocol defines a way to access mdib data as entities.

    This representation is independent of the internal mdib organization.
    The entities returned by the provided getter methods contain copies of the internal mdib data.
    Changing the data does not change data in the mdib.
    Use the EntityTransactionProtocol to write data back to the mdib.
    """

    def by_handle(self, handle: str) -> EntityTypeProtocol | None:
        """Return entity with given descriptor handle."""
        ...

    def by_context_handle(self, handle: str) -> MultiStateEntityProtocol | None:
        """Return multi state entity that contains a state with given handle."""
        ...

    def by_node_type(self, node_type: QName) -> list[EntityTypeProtocol]:
        """Return all entities with given node type."""
        ...

    def by_parent_handle(self, parent_handle: str | None) -> list[EntityTypeProtocol]:
        """Return all entities with given parent handle."""
        ...

    def by_coding(self, coding: Coding) -> list[EntityTypeProtocol]:
        """Return all entities with equivalent Coding."""
        ...

    def by_coded_value(self, coded_value: CodedValue) -> list[EntityTypeProtocol]:
        """Return all entities with equivalent CodedValue."""
        ...

    def items(self) -> Iterable[tuple[str, EntityTypeProtocol]]:
        """Return items of a dictionary."""
        ...

    def __len__(self) -> int:
        """Return number of entities."""
        ...


class ProviderEntityGetterProtocol(EntityGetterProtocol): # pragma: no cover
    """The protocol adds the new_entity method to EntityGetterProtocol."""

    def new_entity(self,
                   node_type: QName,
                   handle: str,
                   parent_handle: str | None) -> EntityTypeProtocol:
        """Create an entity."""
