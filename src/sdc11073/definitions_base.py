from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, ClassVar

if TYPE_CHECKING:
    from types import ModuleType

    from lxml.etree import QName

    from .mdib.descriptorcontainers import AbstractDescriptorProtocol
    from .mdib.statecontainers import AbstractStateProtocol
    from .namespaces import NamespaceHelper


class ProtocolsRegistry(type):
    """base class that has the only purpose to register classes that use this as metaclass."""

    protocols: ClassVar[list[type[BaseDefinitions]]] = []

    def __new__(cls, name: str, *arg, **kwarg):
        new_cls: ProtocolsRegistry = super().__new__(cls, name, *arg, **kwarg)
        if name != 'BaseDefinitions':  # ignore the base class itself
            new_cls: type[BaseDefinitions]
            cls.protocols.append(new_cls)
        return new_cls


class AbstractDataModel(ABC):
    """Abstract base class for DataModelProtocol implementation."""

    @abstractmethod
    def get_descriptor_container_class(self, type_qname: QName) -> type[AbstractDescriptorProtocol]:
        """Get the class that represents a BICEPS descriptor entity with given QName."""

    def mk_descriptor_container(self, type_qname: QName, handle: str, parent_descriptor: Any) -> Any:
        """Create an instance that represents a BICEPS entity with given QName."""
        cls = self.get_descriptor_container_class(type_qname)
        if parent_descriptor is not None:
            ret = cls(handle, parent_descriptor.Handle)
            ret.set_source_mds(parent_descriptor.source_mds)
        else:
            ret = cls(handle, None)
        return ret

    @abstractmethod
    def get_state_container_class(self, type_qname: QName) -> type[AbstractStateProtocol]:
        """Get the class that represents a BICEPS state entity with given QName."""

    def get_state_class_for_descriptor(
            self, descriptor_container: AbstractDescriptorProtocol) -> type[AbstractStateProtocol]:
        """Get the corresponding state class for a descriptor."""
        state_class_qtype = descriptor_container.STATE_QNAME
        if state_class_qtype is None:
            raise TypeError(f'No state association for {descriptor_container.__class__.__name__}')
        return self.get_state_container_class(state_class_qtype)

    def mk_state_container(self, descriptor_container: AbstractDescriptorProtocol) -> AbstractStateProtocol:
        """Create an instance that represents a BICEPS entity with given QName."""
        cls = self.get_state_class_for_descriptor(descriptor_container)
        if cls is None:
            raise TypeError(
                f'No state container class for descr={descriptor_container.__class__.__name__}, '
                f'name={descriptor_container.NODETYPE}')
        return cls(descriptor_container)

    @property
    @abstractmethod
    def pm_types(self) -> ModuleType:
        """Get a module with participant model types."""

    @property
    @abstractmethod
    def pm_names(self) -> ModuleType:
        """Get a module with all qualified names of the BICEPS participant model."""

    @property
    @abstractmethod
    def msg_types(self) -> ModuleType:
        """Get a module with message model types."""

    @property
    @abstractmethod
    def msg_names(self) -> ModuleType:
        """Get a module with all qualified names of the BICEPS message model."""

    @property
    @abstractmethod
    def ns_helper(self) -> NamespaceHelper:
        """Gives access to a NamespaceHelper."""


class BaseDefinitions(metaclass=ProtocolsRegistry):
    """Base class for central definitions used by SDC.

    It defines namespaces and handlers for the protocol.
    Derive from this class in order to define different protocol handling.
    """

    # set the following values in derived classes:
    MedicalDeviceType: QName = None  # a QName, needed for types_match method
    ActionsNamespace: str = None  # needed for wsdl generation
    PortTypeNamespace: str = None  # needed for wsdl generation
    MedicalDeviceTypesFilter: tuple[QName] | None = None  # QNames that are used / expected in "types" of wsdiscovery
    Actions = None
    data_model: AbstractDataModel = None

    @classmethod
    def types_match(cls, types: list[QName]) -> bool:
        """Check if this definition can be used for the provided types."""
        return cls.MedicalDeviceType in types
