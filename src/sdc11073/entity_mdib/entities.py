"""Implementation of entities for EntityProviderMdib."""
from __future__ import annotations

import copy
import uuid
from typing import TYPE_CHECKING, TypeVar, Union

from lxml.etree import QName

from sdc11073.mdib.containerbase import ContainerBase
from sdc11073.mdib.entityprotocol import EntityProtocol, MultiStateEntityProtocol
from sdc11073.namespaces import QN_TYPE, text_to_qname
from sdc11073.xml_types import pm_qnames
from sdc11073.xml_types.pm_types import CodedValue

if TYPE_CHECKING:
    from sdc11073.mdib.descriptorcontainers import AbstractDescriptorContainer
    from sdc11073.mdib.statecontainers import AbstractMultiStateContainer, AbstractStateContainer
    from sdc11073.xml_utils import LxmlElement

    from .entity_consumermdib import EntityConsumerMdib
    from .entity_providermdib import EntityProviderMdib

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
    pm_qnames.EnsembleContext: pm_qnames.EnsembleContextDescriptor,
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
    _xsi_type = QName(element.tag)
    try:
        return _static_type_lookup[_xsi_type]
    except KeyError as err:  # pragma: no cover
        raise KeyError(str(_xsi_type)) from err


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
        self._descriptor = descriptor
        self.coded_value: CodedValue | None = None
        self.descriptor = descriptor  # setter updates self._descriptor and self.coded_value

    @property
    def descriptor(self) -> LxmlElement:
        return self._descriptor

    @descriptor.setter
    def descriptor(self, new_descriptor: LxmlElement):
        self._descriptor = new_descriptor
        type_node = self.descriptor.find(pm_qnames.Type)
        if type_node is not None:
            self.coded_value = CodedValue.from_node(type_node)
        else:
            self.coded_value = None

    def __str__(self):
        return f'{self.__class__.__name__} {self.node_type.localname} handle={self._descriptor.get("Handle")}'


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
        """Return False because this is not a multi state entity."""
        return False

    def mk_entity(self, mdib: EntityConsumerMdib) -> ConsumerEntity:
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
        """Return True because this is a multi state entity."""
        return True

    def mk_entity(self, mdib: EntityConsumerMdib) -> ConsumerMultiStateEntity:
        """Return a corresponding entity with containers."""
        return ConsumerMultiStateEntity(self, mdib)


class ConsumerEntityBase:
    """A descriptor container and a weak reference to the corresponding xml entity."""

    def __init__(self,
                 source: XmlEntity | XmlMultiStateEntity,
                 mdib: EntityConsumerMdib,  # needed if a new state needs to be added
                 ):
        self._mdib: EntityConsumerMdib = mdib

        cls = mdib.sdc_definitions.data_model.get_descriptor_container_class(source.node_type)
        if cls is None:  # pragma: no cover
            msg = f'do not know how to make container from {source.node_type!s}'
            raise ValueError(msg)
        handle = source.descriptor.get('Handle')
        self.descriptor: AbstractDescriptorContainer = cls(handle, parent_handle=source.parent_handle)
        self.descriptor.update_from_node(source.descriptor)
        if source.source_mds is not None:
            self.descriptor.set_source_mds(source.source_mds)
        self.source_mds = source.source_mds

    @property
    def handle(self) -> str:
        """Return the handle of the descriptor."""
        return self.descriptor.Handle

    @property
    def parent_handle(self) -> str | None:
        """Return the parent handle of the descriptor."""
        return self.descriptor.parent_handle

    @property
    def node_type(self) -> QName:
        """Return the node type of the descriptor."""
        return self.descriptor.NODETYPE

    def __str__(self):
        return f'{self.__class__.__name__} {self.node_type} handle={self.handle}'


class ConsumerEntity(ConsumerEntityBase, EntityProtocol):
    """Groups descriptor container and state container."""

    def __init__(self,
                 source: XmlEntity,
                 mdib: EntityConsumerMdib):
        super().__init__(source, mdib)
        self.state: AbstractStateContainer | None = None
        if source.state is not None:
            self._mk_state(source.state)

    def update(self):
        """Update the entity from current data in mdib."""
        xml_entity = self._mdib.internal_entities.get(self.handle)
        if xml_entity is None:
            raise ValueError('entity no longer exists in mdib')
        if int(xml_entity.descriptor.get('DescriptorVersion', '0')) != self.descriptor.DescriptorVersion:
            self.descriptor.update_from_node(xml_entity.descriptor)
        if xml_entity.state is not None:
            if self.state is None:
                self._mk_state(xml_entity.state)
            elif int(xml_entity.state.get('StateVersion', '0')) != self.state.StateVersion:
                self.state.update_from_node(xml_entity.state)

    def _mk_state(self, lxml_state: LxmlElement):
        cls = self._mdib.sdc_definitions.data_model.get_state_container_class(self.descriptor.STATE_QNAME)
        self.state = cls(self.descriptor)
        self.state.update_from_node(lxml_state)


class ConsumerMultiStateEntity(ConsumerEntityBase, MultiStateEntityProtocol):
    """Groups descriptor container and list of multi-state containers."""

    def __init__(self,
                 source: XmlMultiStateEntity,
                 mdib: EntityConsumerMdib):
        super().__init__(source, mdib)
        self.states: dict[str, AbstractMultiStateContainer] = {}
        for handle, state in source.states.items():
            state_type = get_xsi_type(state)
            cls = mdib.sdc_definitions.data_model.get_state_container_class(state_type)
            state_container = cls(self.descriptor)
            state_container.update_from_node(state)
            self.states[handle] = state_container

    def update(self):
        """Update the entity from current data in mdib."""
        xml_entity = self._mdib.internal_entities.get(self.handle)
        if xml_entity is None:
            raise ValueError('entity no longer exists in mdib')
        if int(xml_entity.descriptor.get('DescriptorVersion', '0')) != self.descriptor.DescriptorVersion:
            self.descriptor.update_from_node(xml_entity.descriptor)

        for handle, xml_state in xml_entity.states.items():
            existing_state = self.states.get(handle)
            if existing_state is None:
                # create new state
                xsi_type = get_xsi_type(xml_state)
                cls = self._mdib.sdc_definitions.data_model.get_state_container_class(xsi_type)
                state_container = cls(self.descriptor)
                state_container.update_from_node(xml_state)
                self.states[handle] = state_container
            elif existing_state.StateVersion != int(xml_state.get('StateVersion', '0')):
                existing_state.update_from_node(xml_state)

        # delete states that are no longer in xml_entity
        for handle in list(self.states.keys()):
            if handle not in xml_entity.states:
                self.states.pop(handle)

    def new_state(self, state_handle: str | None = None) -> AbstractMultiStateContainer:
        """Create a new state.

        The new state has handle of descriptor container as handle.
        If this new state is used as a proposed context state in SetContextState operation, this means a new
        state shall be created on providers side.
        """
        if state_handle in self.states:  # pragma: no cover
            msg = f'State handle {state_handle} already exists in {self.__class__.__name__}, handle = {self.handle}'
            raise ValueError(msg)
        cls = self._mdib.data_model.get_state_container_class(self.descriptor.STATE_QNAME)
        state = cls(descriptor_container=self.descriptor)
        state.Handle = state_handle or self.handle
        return state


ConsumerEntityType = Union[ConsumerEntity, ConsumerMultiStateEntity]
ConsumerInternalEntityType = Union[XmlEntity, XmlMultiStateEntity]


##############  provider ##########################

class ProviderInternalEntityBase:
    """A descriptor element and some info about it for easier access."""

    def __init__(self, descriptor: AbstractDescriptorContainer):
        self.descriptor = descriptor

    @property
    def handle(self) -> str:
        """Return the handle of the descriptor."""
        return self.descriptor.Handle

    @property
    def parent_handle(self) -> str | None:
        """Return the parent handle of the descriptor."""
        return self.descriptor.parent_handle

    @property
    def source_mds(self) -> str:
        """Return the source mds of the descriptor."""
        return self.descriptor.source_mds

    @property
    def node_type(self) -> QName:
        """Return the node type of the descriptor."""
        return self.descriptor.NODETYPE

    def __str__(self):
        return f'{self.__class__.__name__} {self.node_type.localname} handle={self.handle}'


class ProviderInternalEntity(ProviderInternalEntityBase):
    """Groups descriptor and state."""

    def __init__(self,
                 descriptor: AbstractDescriptorContainer,
                 state: AbstractStateContainer | None):
        super().__init__(descriptor)
        self._state = state
        if state is not None:
            self._state.descriptor_container = self.descriptor

    @property
    def state(self) -> AbstractStateContainer | None:
        """Return the state member of the entity."""
        return self._state

    @state.setter
    def state(self, new_state: AbstractStateContainer):
        self._state = new_state
        self._state.descriptor_container = self.descriptor

    @property
    def is_multi_state(self) -> bool:
        """Return False because this is not a multi state entity."""
        return False

    def mk_entity(self, mdib: EntityProviderMdib) -> ProviderEntity:
        """Return a corresponding entity with containers."""
        return ProviderEntity(self, mdib)


class ProviderInternalMultiStateEntity(ProviderInternalEntityBase):
    """Groups descriptor and list of multi-states."""

    def __init__(self,
                 descriptor: AbstractDescriptorContainer,
                 states: list[AbstractMultiStateContainer]):
        super().__init__(descriptor)
        self.states = {state.Handle: state for state in states}

    @property
    def is_multi_state(self) -> bool:
        """Return True because this is a multi state entity."""
        return True

    def mk_entity(self, mdib: EntityProviderMdib) -> ProviderMultiStateEntity:
        """Return a corresponding entity with containers."""
        return ProviderMultiStateEntity(self, mdib)


T = TypeVar('T', bound=ContainerBase)
def _mk_copy(original: T) -> T:
    """Return a deep copy of original without node member."""
    node, original.node = original.node, None
    copied = copy.deepcopy(original)
    original.node = node
    return copied


class ProviderEntityBase:
    """A descriptor container and a weak reference to the corresponding xml entity."""

    def __init__(self,
                 source: ProviderInternalEntity | ProviderInternalMultiStateEntity,
                 mdib: EntityProviderMdib):
        self._mdib = mdib
        self.descriptor = _mk_copy(source.descriptor)
        self.source_mds = source.source_mds

    @property
    def handle(self) -> str:
        """Return the handle of the descriptor."""
        return self.descriptor.Handle

    @property
    def parent_handle(self) -> str | None:
        """Return the parent handle of the descriptor."""
        return self.descriptor.parent_handle

    @property
    def node_type(self) -> QName:
        """Return the node type of the descriptor."""
        return self.descriptor.NODETYPE

    def __str__(self):
        return f'{self.__class__.__name__} {self.node_type.localname} handle={self.handle}'


class ProviderEntity(ProviderEntityBase):
    """Groups descriptor container and state container."""

    def __init__(self,
                 source: ProviderInternalEntity,
                 mdib: EntityProviderMdib):
        super().__init__(source, mdib)
        self.state: AbstractStateContainer | None = None
        if source.state is not None:
            self.state = _mk_copy(source.state)

    @property
    def is_multi_state(self) -> bool:
        """Return False because this is not a multi state entity."""
        return False

    def update(self):
        """Update from internal entity."""
        source_entity = self._mdib.internal_entities.get(self.handle)
        if source_entity is None: # pragma: no cover
            msg = f'entity {self.handle} no longer exists in mdib'
            raise ValueError(msg)
        self.descriptor.update_from_other_container(source_entity.descriptor)
        self.state = _mk_copy(source_entity.state)


class ProviderMultiStateEntity(ProviderEntityBase):
    """Groups descriptor container and list of multi-state containers."""

    def __init__(self,
                 source: ProviderInternalMultiStateEntity,
                 mdib: EntityProviderMdib):
        super().__init__(source, mdib)
        self.states: dict[str, AbstractMultiStateContainer] = {st.Handle: _mk_copy(st) for st in source.states.values()}

    @property
    def is_multi_state(self) -> bool:
        """Return True because this is a multi state entity."""
        return True

    def update(self):
        """Update from internal entity."""
        source_entity = self._mdib.internal_entities.get(self.handle)
        if source_entity is None:  # pragma: no cover
            msg = f'entity {self.handle} no longer exists in mdib'
            raise ValueError(msg)
        self.descriptor.update_from_other_container(source_entity.descriptor)
        for handle, src_state in source_entity.states.items():
            dest_state = self.states.get(handle)
            if dest_state is None:
                self.states[handle] = _mk_copy(src_state)
            else:
                dest_state.update_from_other_container(src_state)
        # remove states that are no longer present is source_entity
        for handle in list(self.states.keys()):
            if handle not in source_entity.states:
                self.states.pop(handle)

    def new_state(self, state_handle: str | None = None) -> AbstractMultiStateContainer:
        """Create a new state."""
        if state_handle in self.states: # pragma: no cover
            msg = f'State handle {state_handle} already exists in {self.__class__.__name__}, handle = {self.handle}'
            raise ValueError(msg)
        cls = self._mdib.data_model.get_state_container_class(self.descriptor.STATE_QNAME)
        state = cls(descriptor_container=self.descriptor)
        state.Handle = state_handle or uuid.uuid4().hex
        self.states[state.Handle] = state
        return state


ProviderInternalEntityType = Union[ProviderInternalEntity, ProviderInternalMultiStateEntity]
ProviderEntityType = Union[ProviderEntity, ProviderMultiStateEntity]
AnyProviderEntityType = Union[ProviderInternalEntity, ProviderInternalMultiStateEntity ]
