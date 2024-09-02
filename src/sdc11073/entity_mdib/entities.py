from __future__ import annotations

import copy
from typing import TYPE_CHECKING, Union
from weakref import ref, ReferenceType

from lxml.etree import QName

from sdc11073.namespaces import QN_TYPE
from sdc11073.namespaces import text_to_qname
from sdc11073.xml_types import pm_qnames
from sdc11073.xml_types.pm_types import CodedValue

if TYPE_CHECKING:
    from sdc11073.mdib.descriptorcontainers import AbstractDescriptorContainer
    from sdc11073.mdib.statecontainers import AbstractMultiStateContainer, AbstractStateContainer
    from .entity_mdibbase import EntityMdibBase
    from sdc11073.xml_utils import LxmlElement

# Many types are fixed in schema. This table maps from tag in Element to its type
_static_type_lookup = {
    pm_qnames.Mds: pm_qnames.MdsDescriptor,
    pm_qnames.Vmd: pm_qnames.VmdDescriptor,
    pm_qnames.Channel: pm_qnames.ChannelDescriptor,
    pm_qnames.AlertSystem: pm_qnames.AlertSystemDescriptor,
    pm_qnames.AlertCondition: pm_qnames.AlertConditionDescriptor,
    pm_qnames.AlertSignal: pm_qnames.AlertSignalDescriptor,
    pm_qnames.Sco: pm_qnames.ScoDescriptor,
    pm_qnames.SystemContext: pm_qnames.SystemContextDescriptor,
    pm_qnames.PatientContext: pm_qnames.PatientContextDescriptor,
    pm_qnames.LocationContext: pm_qnames.LocationContextDescriptor,
    pm_qnames.Clock: pm_qnames.ClockDescriptor,
    pm_qnames.Battery: pm_qnames.BatteryDescriptor,
}


def get_xsi_type(element: LxmlElement) -> QName:
    """Return the BICEPS type of an element.

    If there is a xsi:type entry, this specifies the type.
    If not, the tag is used to determine the type.
    """
    xsi_type_str = element.attrib.get(QN_TYPE)

    if xsi_type_str:
        return text_to_qname(xsi_type_str, element.nsmap)
    else:
        _xsi_type = QName(element.tag)
        try:
            return _static_type_lookup[_xsi_type]
        except KeyError:
            raise KeyError(str(_xsi_type))


class _XmlEntityBase:
    """A descriptor element and some info about it for easier access."""

    def __init__(self,
                 parent_handle: str | None,
                 source_mds: str | None,
                 node_type: QName,
                 descriptor: LxmlElement):
        self.parent_handle = parent_handle
        self.source_mds = source_mds
        self.node_type = node_type  # name of descriptor type
        self._descriptor = None
        self.coded_value: CodedValue | None = None
        self.descriptor = descriptor  # setter updates self._descriptor and self.coded_value

    @property
    def descriptor(self) -> LxmlElement:
        return self._descriptor

    @descriptor.setter
    def descriptor(self, new_descriptor):
        self._descriptor = new_descriptor
        type_node = self.descriptor.find(pm_qnames.Type)
        if type_node is not None:
            self.coded_value = CodedValue.from_node(type_node)
        else:
            self.coded_value = None


class XmlEntity(_XmlEntityBase):
    """Groups descriptor and state."""

    def __init__(self,
                 parent_handle: str | None,
                 source_mds: str | None,
                 node_type: QName,
                 descriptor: LxmlElement,
                 state: LxmlElement | None):
        super().__init__(parent_handle, source_mds, node_type, descriptor)
        self.state = state

    @property
    def is_multi_state(self) -> bool:
        return False

    def mk_entity(self, mdib: EntityMdibBase) -> ConsumerEntity:
        """Return a corresponding entity with containers."""
        return ConsumerEntity(self, mdib)


class XmlMultiStateEntity(_XmlEntityBase):
    """Groups descriptor and list of multi-states."""

    def __init__(self,
                 parent_handle: str | None,
                 source_mds: str | None,
                 node_type: QName,
                 descriptor: LxmlElement,
                 states: list[LxmlElement]):
        super().__init__(parent_handle, source_mds, node_type, descriptor)
        self.states: dict[str, LxmlElement] = {node.get('Handle'): node for node in states}

    @property
    def is_multi_state(self) -> bool:
        return True

    def mk_entity(self, mdib: EntityMdibBase) -> ConsumerMultiStateEntity:
        """Return a corresponding entity with containers."""
        return ConsumerMultiStateEntity(self, mdib)


class ConsumerEntityBase:
    """A descriptor container and a weak reference to the corresponding xml entity."""

    def __init__(self,
                 source: XmlEntity | XmlMultiStateEntity,
                 mdib: EntityMdibBase,  # needed if a new state needs to be added
                 ):
        self._source: ReferenceType[XmlEntity | XmlMultiStateEntity] = ref(source)
        self._mdib: EntityMdibBase = mdib
        # self.descriptor: AbstractDescriptorContainer = descriptor

        cls = mdib.sdc_definitions.data_model.get_descriptor_container_class(source.node_type)
        if cls is None:
            raise ValueError(f'do not know how to make container from {str(source.node_type)}')
        handle = source.descriptor.get('Handle')
        self.descriptor: AbstractDescriptorContainer = cls(handle, parent_handle=source.parent_handle)
        self.descriptor.update_from_node(source.descriptor)
        self.descriptor.set_source_mds(source.source_mds)
        self.source_mds = source.source_mds


class ConsumerEntity(ConsumerEntityBase):
    """Groups descriptor container and state container."""

    def __init__(self,
                 source: XmlEntity,
                 mdib: EntityMdibBase,  # needed if a new state needs to be added
                 ):
        super().__init__(source, mdib)
        self.state: AbstractStateContainer | None = None
        if source.state is not None:
            cls = mdib.sdc_definitions.data_model.get_state_container_class(self.descriptor.STATE_QNAME)
            self.state = cls(self.descriptor)
            self.state.update_from_node(source.state)

    def update(self):
        xml_entity: XmlEntity = self._source()
        if xml_entity is None:
            raise ValueError('entity no longer exists in mdib')
        if int(xml_entity.descriptor.get('DescriptorVersion', '0')) != self.descriptor.DescriptorVersion:
            self.descriptor.update_from_node(xml_entity.descriptor)
        if int(xml_entity.state.get('StateVersion', '0')) != self.state.StateVersion:
            self.state.update_from_node(xml_entity.state)


class ConsumerMultiStateEntity(ConsumerEntityBase):
    """Groups descriptor container and list of multi-state containers."""

    def __init__(self,
                 source: XmlMultiStateEntity,
                 mdib: EntityMdibBase):
        super().__init__(source, mdib)
        self.states: dict[str, AbstractMultiStateContainer] = {}
        for handle, state in source.states.items():
            state_type = get_xsi_type(state)
            cls = mdib.sdc_definitions.data_model.get_state_container_class(state_type)
            state_container = cls(self.descriptor)
            state_container.update_from_node(state)
            self.states[handle] = state_container

    def update(self):
        """Update all containers."""
        xml_entity: XmlMultiStateEntity = self._source()
        if xml_entity is None:
            raise ValueError('entity no longer exists in mdib')
        if int(xml_entity.descriptor.get('DescriptorVersion', '0')) != self.descriptor.DescriptorVersion:
            self.descriptor.update_from_node(xml_entity.descriptor)

        for handle, xml_state in xml_entity.states.items():
            create_new_state = False
            try:
                existing_state = self.states[handle]
            except IndexError:
                create_new_state = True
            else:
                if existing_state.StateVersion != int(xml_state.get('StateVersion', '0')):
                    existing_state.update_from_node(xml_state)

            if create_new_state:
                xsi_type = get_xsi_type(xml_state)
                cls = self._mdib.sdc_definitions.data_model.get_state_container_class(xsi_type)
                state_container = cls(self.descriptor)
                state_container.update_from_node(xml_state)
                self.states[handle] = state_container

        # delete states that are no longer in xml_entity
        for handle in list(self.states.keys()):
            if handle not in xml_entity.states:
                self.states.pop(handle)


ConsumerEntityType = Union[ConsumerEntity, ConsumerMultiStateEntity]


##############  provider ##########################

class ProviderInternalEntityBase:
    """A descriptor element and some info about it for easier access."""

    def __init__(self, descriptor: AbstractDescriptorContainer):
        self.descriptor = descriptor

    @property
    def parent_handle(self) -> str:
        return self.descriptor.parent_handle

    @property
    def handle(self) -> str:
        return self.descriptor.Handle

    @property
    def source_mds(self) -> str:
        return self.descriptor.source_mds

    @property
    def node_type(self) -> QName:
        return self.descriptor.NODETYPE


class ProviderInternalEntity(ProviderInternalEntityBase):
    """Groups descriptor and state."""

    def __init__(self,
                 descriptor: AbstractDescriptorContainer,
                 state: AbstractStateContainer | None):
        super().__init__(descriptor)
        self.state = state

    @property
    def is_multi_state(self) -> bool:
        return False

    def mk_entity(self) -> ProviderEntity:
        """Return a corresponding entity with containers."""
        return ProviderEntity(self)


class ProviderInternalMultiStateEntity(ProviderInternalEntityBase):
    """Groups descriptor and list of multi-states."""

    def __init__(self,
                 descriptor: AbstractDescriptorContainer,
                 states: list[AbstractMultiStateContainer]):
        super().__init__(descriptor)
        self.states = {state.Handle: state for state in states}

    @property
    def is_multi_state(self) -> bool:
        return True

    def mk_entity(self) -> ProviderMultiStateEntity:
        """Return a corresponding entity with containers."""
        return ProviderMultiStateEntity(self)


class ProviderEntityBase:
    """A descriptor container and a weak reference to the corresponding xml entity."""

    def __init__(self,
                 source: ProviderInternalEntity | ProviderInternalMultiStateEntity):
        self._source: ReferenceType[ProviderInternalEntity | ProviderInternalMultiStateEntity] = ref(source)
        self.descriptor = copy.deepcopy(source.descriptor)
        self.source_mds = source.source_mds

    @property
    def handle(self) -> str:
        return self.descriptor.Handle


class ProviderEntity(ProviderEntityBase):
    """Groups descriptor container and state container."""

    def __init__(self,
                 source: ProviderInternalEntity,
                 ):
        super().__init__(source)
        self.state: AbstractStateContainer | None = None
        if source.state is not None:
            self.state = copy.deepcopy(source.state)

    @property
    def is_multi_state(self) -> bool:
        return False

    def update(self):
        """Update from internal entity."""
        # Todo: update same instances instead of replacing them
        source_entity: ProviderInternalEntity = self._source()
        if source_entity is None:
            raise ValueError('entity no longer exists in mdib')
        self.descriptor = copy.deepcopy(source_entity.descriptor)
        self.state = copy.deepcopy(source_entity.state)


class ProviderMultiStateEntity(ProviderEntityBase):
    """Groups descriptor container and list of multi-state containers."""

    def __init__(self,
                 source: ProviderInternalMultiStateEntity):
        super().__init__(source)
        self.states: dict[str, AbstractMultiStateContainer] = copy.deepcopy(source.states)

    @property
    def is_multi_state(self) -> bool:
        return True

    def update(self):
        """Update from internal entity."""
        # Todo: update same instances instead of replacing them
        source_entity: ProviderInternalMultiStateEntity = self._source()
        if source_entity is None:
            raise ValueError('entity no longer exists in mdib')
        # update always, this will overwrite modifications that the user might have made
        self.descriptor = copy.deepcopy(source_entity.descriptor)
        self.states = copy.deepcopy(source_entity.states)


ProviderInternalEntityType = Union[ProviderInternalEntity, ProviderInternalMultiStateEntity]
ProviderEntityType = Union[ProviderEntity, ProviderMultiStateEntity]
