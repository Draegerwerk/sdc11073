from __future__ import annotations

from typing import TYPE_CHECKING

from .definitions_base import AbstractDataModel, BaseDefinitions
from .mdib.descriptorcontainers import get_container_class as get_descriptor_container_class
from .mdib.statecontainers import get_container_class as get_state_container_class
from .namespaces import default_ns_helper as ns_hlp
from .xml_types import actions, msg_qnames, msg_types, pm_qnames, pm_types

if TYPE_CHECKING:
    from types import ModuleType

    from lxml.etree import QName

    from .namespaces import NamespaceHelper


class V1Model(AbstractDataModel):
    """Data Model for SDC first version (assuming there will be successors in the future...)."""

    def __init__(self):
        super().__init__()
        self._ns_hlp = ns_hlp

    def get_descriptor_container_class(self, type_qname: QName) -> type:
        """Get the class that represents a BICEPS descriptor entity with given QName."""
        return get_descriptor_container_class(type_qname)

    def get_state_container_class(self, type_qname: QName) -> type:
        """Get the class that represents a BICEPS state entity with given QName."""
        return get_state_container_class(type_qname)

    @property
    def pm_types(self) -> ModuleType:
        """Get a module with participant model types."""
        return pm_types

    @property
    def pm_names(self) -> ModuleType:
        """Get a module with all qualified names of the BICEPS participant model."""
        return pm_qnames

    @property
    def msg_types(self) -> ModuleType:
        """Get a module with message model types."""
        return msg_types

    @property
    def msg_names(self) -> ModuleType:
        """Get a module with all qualified names of the BICEPS message model."""
        return msg_qnames

    @property
    def ns_helper(self) -> NamespaceHelper:
        """Gives access to a NamespaceHelper."""
        return self._ns_hlp


class SdcV1Definitions(BaseDefinitions):  # pylint: disable=invalid-name
    """Constants for SDC first version (assuming there will be successors in the future...)."""

    DpwsDeviceType = ns_hlp.DPWS.tag('Device')
    MedicalDeviceType = ns_hlp.MDPWS.tag('MedicalDevice')
    ActionsNamespace = ns_hlp.SDC.namespace
    PortTypeNamespace = ns_hlp.SDC.namespace
    MedicalDeviceTypesFilter = (DpwsDeviceType, MedicalDeviceType)
    Actions = actions.Actions
    data_model = V1Model()
