from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, Union, Iterable

if TYPE_CHECKING:
    from lxml.etree import QName
    from sdc11073.definitions_base import BaseDefinitions
    from sdc11073.loghelper import LoggerAdapter
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

    This representation is independent of the internal mdib organization."""


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
