from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, Union, Iterable, ClassVar

if TYPE_CHECKING:
    from lxml.etree import QName
    from sdc11073.mdib.descriptorcontainers import AbstractDescriptorProtocol
    from sdc11073.mdib.statecontainers import AbstractStateProtocol, AbstractMultiStateProtocol
    from sdc11073.xml_types.pm_types import Coding, CodedValue


class EntityProtocol(Protocol):
    descriptor: AbstractDescriptorProtocol
    state: AbstractStateProtocol
    is_multi_state: ClassVar[bool]

    @property
    def handle(self) -> str: ...  # a read-only member

    @property
    def parent_handle(self) -> str: ...  # a read-only member

    @property
    def node_type(self) -> QName: ...  # a read-only member

    def update(self):
        ...


class MultiStateEntityProtocol(Protocol):
    descriptor: AbstractDescriptorProtocol
    states: dict[str, AbstractMultiStateProtocol]  # key is the Handle member of state
    is_multi_state: bool
    node_type: QName
    handle: str
    parent_handle: str

    def update(self):
        """Update entity with current data in mdib."""

    def new_state(self, handle: str | None = None) -> AbstractMultiStateProtocol:
        """create a new state."""

EntityTypeProtocol = Union[EntityProtocol, MultiStateEntityProtocol]


# Todo: should node_type be QName (this assumes that we talk XML) or just Any to be generic?

class EntityGetterProtocol(Protocol):
    """This protocol defines a way to access mdib data as entities.

    This representation is independent of the internal mdib organization.
    The entities returned by the provided getter methods contain copies of the internal mdib data.
    Changing the data does not change data in the mdib.
    Use the EntityTransactionProtocol to write data back to the mdib."""

    def handle(self, handle: str) -> EntityTypeProtocol | None:
        """Return entity with given descriptor handle."""
        ...

    def context_handle(self, handle: str) -> MultiStateEntityProtocol | None:
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

    def items(self) -> Iterable[tuple[str, EntityTypeProtocol]]:
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
