from __future__ import annotations

from dataclasses import dataclass
from threading import Lock
from typing import TYPE_CHECKING, Callable, Union
from weakref import ref, ReferenceType

from lxml import etree as etree_

from sdc11073 import observableproperties as properties
from sdc11073.mdib.mdibbase import MdibVersionGroup
from sdc11073.namespaces import QN_TYPE
from sdc11073.namespaces import text_to_qname
from sdc11073.xml_types import msg_qnames, pm_qnames

if TYPE_CHECKING:
    from lxml.etree import QName
    from sdc11073.definitions_base import BaseDefinitions
    from sdc11073.loghelper import LoggerAdapter
    from sdc11073.mdib.descriptorcontainers import AbstractDescriptorContainer
    from sdc11073.mdib.statecontainers import AbstractMultiStateContainer, AbstractStateContainer

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

multi_state_q_names = (pm_qnames.PatientContextDescriptor,
                       pm_qnames.LocationContextDescriptor,
                       pm_qnames.WorkflowContextDescriptor,
                       pm_qnames.OperatorContextDescriptor,
                       pm_qnames.MeansContextDescriptor,
                       pm_qnames.EnsembleContextDescriptor)


def get_xsi_type(element: etree_.Element) -> QName:
    """Return the BICEPS type of an element.

    If there is a xsi:type entry, this specifies the type.
    If not, the tag is used to determine the type.
    """
    xsi_type_str = element.attrib.get(QN_TYPE)

    if xsi_type_str:
        return text_to_qname(xsi_type_str, element.nsmap)
    else:
        _xsi_type = etree_.QName(element.tag)
        try:
            return _static_type_lookup[_xsi_type]
        except KeyError:
            raise KeyError(str(_xsi_type))


@dataclass
class _XmlEntityBase:
    """A descriptor element and some info about it for easier access."""
    parent_handle: str | None
    source_mds: str | None
    node_type: etree_.QName  # name of descriptor type
    descriptor: etree_.Element


@dataclass
class XmlEntity(_XmlEntityBase):
    """Groups descriptor and state."""
    state: Union[etree_.Element, None]

    @property
    def is_multi_state(self) -> bool:
        return False

    def mk_entity(self, mdib: XmlMdibBase) -> Entity:
        """Return a corresponding entity with containers."""
        return Entity(self, mdib)


@dataclass
class XmlMultiStateEntity(_XmlEntityBase):
    """Groups descriptor and list of multi-states."""
    states: list[etree_.Element]

    @property
    def is_multi_state(self) -> bool:
        return True

    def mk_entity(self, mdib: XmlMdibBase) -> MultiStateEntity:
        """Return a corresponding entity with containers."""
        return MultiStateEntity(self, mdib)


class EntityBase:
    """A descriptor container and a weak reference to the corresponding xml entity."""

    def __init__(self,
                 source: XmlEntity | XmlMultiStateEntity,
                 mdib: XmlMdibBase,  # needed if a new state needs to be added
                 ):
        self._source: ReferenceType[XmlEntity | XmlMultiStateEntity] = ref(source)
        self._mdib: XmlMdibBase = mdib
        # self.descriptor: AbstractDescriptorContainer = descriptor

        cls = mdib.sdc_definitions.data_model.get_descriptor_container_class(source.node_type)
        if cls is None:
            raise ValueError(f'do not know how to make container from {str(source.node_type)}')
        handle = source.descriptor.get('Handle')
        self.descriptor: AbstractDescriptorContainer = cls(handle, parent_handle=source.parent_handle)
        self.descriptor.update_from_node(source.descriptor)
        self.descriptor.set_source_mds(source.source_mds)
        self.source_mds = source.source_mds


def mk_xml_entity(node, parent_handle, source_mds) -> XmlEntity | XmlMultiStateEntity :
    xsi_type = get_xsi_type(node)
    if xsi_type in multi_state_q_names:
        return XmlMultiStateEntity(parent_handle, source_mds, xsi_type, node, [])
    return XmlEntity(parent_handle, source_mds, xsi_type, node, None)


class Entity(EntityBase):
    """Groups descriptor container and state container."""

    def __init__(self,
                 source: XmlEntity,
                 mdib: XmlMdibBase,  # needed if a new state needs to be added
                 ):
        super().__init__(source, mdib)
        cls = mdib.sdc_definitions.data_model.get_state_container_class(self.descriptor.STATE_QNAME)
        self.state: AbstractStateContainer = cls(self.descriptor)
        self.state.update_from_node(source.state)

    def update(self):
        xml_entity: XmlEntity = self._source()
        if xml_entity is None:
            raise ValueError('entity no longer exists in mdib')
        if int(xml_entity.descriptor.get('DescriptorVersion', '0')) != self.descriptor.DescriptorVersion:
            self.descriptor.update_from_node(xml_entity.descriptor)
        if int(xml_entity.state.get('StateVersion', '0')) != self.state.StateVersion:
            self.state.update_from_node(xml_entity.state)


class MultiStateEntity(EntityBase):
    """Groups descriptor container and list of multi-state containers."""

    def __init__(self,
                 source: XmlMultiStateEntity,
                 mdib: XmlMdibBase):
        super().__init__(source, mdib)
        self.states: list[AbstractMultiStateContainer] = []
        for state in source.states:
            state_type = get_xsi_type(state)
            cls = mdib.sdc_definitions.data_model.get_state_container_class(state_type)
            state_container = cls(self.descriptor)
            state_container.update_from_node(state)
            self.states.append(state_container)

    def update(self):
        """Update all containers.

        States list has always the same order as in source."""
        xml_entity: XmlMultiStateEntity = self._source()
        if xml_entity is None:
            raise ValueError('entity no longer exists in mdib')
        if int(xml_entity.descriptor.get('DescriptorVersion', '0')) != self.descriptor.DescriptorVersion:
            self.descriptor.update_from_node(xml_entity.descriptor)

        for i, xml_state in enumerate(xml_entity.states):
            create_new_state = False
            xml_handle = xml_state.get('Handle')
            try:
                existing_state = self.states[i]
            except IndexError:
                create_new_state = True
            else:
                if existing_state.Handle == xml_handle:
                    if existing_state.StateVersion != int(xml_state.get('StateVersion', '0')):
                        existing_state.update_from_node(xml_state)
                else:
                    del self.states[i]
                    create_new_state = True
            if create_new_state:
                xsi_type = get_xsi_type(xml_state)
                cls = self._mdib.sdc_definitions.data_model.get_state_container_class(xsi_type)
                state_container = cls(self.descriptor)
                state_container.update_from_node(xml_state)
                self.states.insert(i, state_container)


class XmlMdibBase:
    metric_handles = properties.ObservableProperty(fire_only_on_changed_value=False)
    waveform_handles = properties.ObservableProperty(fire_only_on_changed_value=False)
    alert_handles = properties.ObservableProperty(fire_only_on_changed_value=False)
    context_handles = properties.ObservableProperty(fire_only_on_changed_value=False)
    component_handles = properties.ObservableProperty(fire_only_on_changed_value=False)
    new_descriptors_handles = properties.ObservableProperty(fire_only_on_changed_value=False)
    updated_descriptors_handles = properties.ObservableProperty(fire_only_on_changed_value=False)
    deleted_descriptors_handles = properties.ObservableProperty(fire_only_on_changed_value=False)
    operation_handles = properties.ObservableProperty(fire_only_on_changed_value=False)
    sequence_id = properties.ObservableProperty()
    instance_id = properties.ObservableProperty()

    def __init__(self, sdc_definitions: type[BaseDefinitions],
                 logger: LoggerAdapter,
                 entity_factory: Callable | None = None):
        """Construct MdibBase.

        :param sdc_definitions: a class derived from BaseDefinitions
        """
        self.sdc_definitions = sdc_definitions
        self.data_model = sdc_definitions.data_model
        self._logger = logger
        self.mdib_version = 0
        self.sequence_id = ''  # needs to be set to a reasonable value by derived class
        self.instance_id = None  # None or an unsigned int
        self.log_prefix = ''
        self.mdib_lock = Lock()

        self._get_mdib_response_node: Union[etree_.Element, None] = None
        self._md_state_node: Union[etree_.Element, None] = None
        self._entities: dict[str, XmlEntity | XmlMultiStateEntity] = {}  # key is the handle
        self._entity_factory = entity_factory or mk_xml_entity

    @property
    def mdib_version_group(self) -> MdibVersionGroup:
        """"Get current version data."""
        return MdibVersionGroup(self.mdib_version, self.sequence_id, self.instance_id)

    def _update_mdib_version_group(self, mdib_version_group: MdibVersionGroup):
        """Set members and update entries in DOM tree."""
        mdib_node = self._get_mdib_response_node[0]
        if mdib_version_group.mdib_version != self.mdib_version:
            self.mdib_version = mdib_version_group.mdib_version
            self._get_mdib_response_node.set('MdibVersion', str(mdib_version_group.mdib_version))
            mdib_node.set('MdibVersion', str(mdib_version_group.mdib_version))
        if mdib_version_group.sequence_id != self.sequence_id:
            self.sequence_id = mdib_version_group.sequence_id
            self._get_mdib_response_node.set('SequenceId', str(mdib_version_group.sequence_id))
            mdib_node.set('SequenceId', str(mdib_version_group.sequence_id))
        if mdib_version_group.instance_id != self.instance_id:
            self.instance_id = mdib_version_group.instance_id
            self._get_mdib_response_node.set('InstanceId', str(mdib_version_group.instance_id))
            mdib_node.set('InstanceId', str(mdib_version_group.instance_id))

    @property
    def logger(self) -> LoggerAdapter:
        """Return the logger."""
        return self._logger

    def _set_root_node(self, root_node: etree_.Element):
        """Set member and create xml entities"""
        if root_node.tag != msg_qnames.GetMdibResponse:
            raise ValueError(f'root node must be {str(msg_qnames.GetMdibResponse)}, got {str(root_node.tag)}')
        self._get_mdib_response_node = root_node
        self._entities.clear()
        self._md_state_node = root_node[0][1]  # GetMdibResponse -> Mdib -> MdState

        def register_children_with_handle(parent_node, source_mds=None):
            parent_handle = parent_node.attrib.get('Handle')
            for child_node in parent_node[:]:
                child_handle = child_node.attrib.get('Handle')
                if child_node.tag == pm_qnames.Mds:
                    source_mds = child_handle
                if child_handle:
                    print(child_node.attrib)
                    self._entities[child_handle] = self._entity_factory(child_node,
                                                                        parent_handle,
                                                                        source_mds)
                    register_children_with_handle(child_node, source_mds)

        for mdib_node in root_node:
            md_description_node = mdib_node[0]
            md_state_node = mdib_node[1]
            register_children_with_handle(md_description_node)
            for state_node in md_state_node:
                handle = state_node.attrib['DescriptorHandle']
                entity = self._entities[handle]
                if entity.is_multi_state:
                    entity.states.append(state_node)
                else:
                    entity.state = state_node

    def mk_entity(self, handle) -> Entity | MultiStateEntity:
        xml_entity = self._entities[handle]
        return xml_entity.mk_entity(self)

    @property
    def handle(self) -> _HandleGetter:
        return _HandleGetter(self._entities, self.mk_entity)

    @property
    def node_type(self) -> _NodetypeGetter:
        return _NodetypeGetter(self._entities, self.mk_entity)

    @property
    def parent_handle(self) -> _ParentHandleGetter:
        return _ParentHandleGetter(self._entities, self.mk_entity)


class _HandleGetter:
    def __init__(self,
                 entities: dict[str, XmlEntity | XmlMultiStateEntity],
                 mk_entity: Callable[[str], Entity | MultiStateEntity]):
        self._entities = entities
        self._mk_entity = mk_entity

    def get(self, handle: str) -> Entity | MultiStateEntity | None:
        try:
            return self._mk_entity(handle)
        except KeyError:
            return None


class _NodetypeGetter:
    def __init__(self,
                 entities: dict[str, XmlEntity | XmlMultiStateEntity],
                 mk_entity: Callable[[str], Entity | MultiStateEntity]):
        self._entities = entities
        self._mk_entity = mk_entity

    def get(self, node_type: etree_.QName) -> list[Entity | MultiStateEntity]:
        ret = []
        for handle, entity in self._entities.items():
            if entity.node_type == node_type:
                ret.append(self._mk_entity(handle))
        return ret


class _ParentHandleGetter:
    def __init__(self,
                 entities: dict[str, XmlEntity | XmlMultiStateEntity],
                 mk_entity: Callable[[str], Entity | MultiStateEntity]):
        self._entities = entities
        self._mk_entity = mk_entity

    def get(self, parent_handle: str) -> list[Entity | MultiStateEntity]:
        ret = []
        for handle, entity in self._entities.items():
            if entity.parent_handle == parent_handle:
                ret.append(self._mk_entity(handle))
        return ret
