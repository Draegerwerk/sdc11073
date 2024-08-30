from __future__ import annotations

from dataclasses import dataclass
from threading import Lock
from typing import TYPE_CHECKING, Callable, Union, Iterable
from weakref import ref, ReferenceType

from lxml import etree as etree_

from sdc11073 import observableproperties as properties
from sdc11073.mdib.mdibbase import MdibVersionGroup
from sdc11073.namespaces import QN_TYPE
from sdc11073.namespaces import text_to_qname
from sdc11073.xml_types import msg_qnames, pm_qnames
from .xml_entities import mk_xml_entity

if TYPE_CHECKING:
    from lxml.etree import QName
    from sdc11073.definitions_base import BaseDefinitions
    from sdc11073.loghelper import LoggerAdapter
    from sdc11073.xml_types.pm_types import Coding, CodedValue
    from sdc11073.mdib.entityprotocol import EntityGetterProtocol
    from .xml_entities import XmlEntity, XmlMultiStateEntity, Entity, MultiStateEntity


class ConsumerEntityGetter:
    """Implements entityprotocol.EntityGetterProtocol"""
    def __init__(self, entities: dict[str, XmlEntity | XmlMultiStateEntity], mdib: XmlMdibBase):
        self._entities = entities
        self._mdib = mdib

    def handle(self, handle: str) ->  Entity | MultiStateEntity | None:
        """Return entity with given handle."""
        try:
            return self._mdib.mk_entity(handle)
        except KeyError:
            return None

    def node_type(self, node_type: QName) -> list[Entity | MultiStateEntity]:
        """Return all entities with given node type."""
        ret = []
        for handle, entity in self._entities.items():
            if entity.node_type == node_type:
                ret.append(self._mdib.mk_entity(handle))
        return ret

    def parent_handle(self, parent_handle: str | None) -> list[Entity | MultiStateEntity]:
        """Return all entities with given parent handle."""
        ret = []
        for handle, entity in self._entities.items():
            if entity.parent_handle == parent_handle:
                ret.append(self._mdib.mk_entity(handle))
        return ret

    def coding(self, coding: Coding) -> list[Entity | MultiStateEntity]:
        """Return all entities with given Coding."""
        ret = []
        for handle, xml_entity in self._entities.items():
            if xml_entity.coded_value.is_equivalent(coding):
                ret.append(self._mdib.mk_entity(handle))
        return ret

    def coded_value(self, coded_value: CodedValue) -> list[Entity | MultiStateEntity]:
        """Return all entities with given Coding."""
        ret = []
        for handle, xml_entity in self._entities.items():
            if xml_entity.coded_value.is_equivalent(coded_value):
                ret.append(self._mdib.mk_entity(handle))
        return ret

    def items(self) -> Iterable[tuple[str, [Entity | MultiStateEntity]]]:
        """Like items() of a dictionary."""
        for handle, entity in self._entities.items():
            yield handle, self._mdib.mk_entity(handle)

    def __len__(self) -> int:
        """Return number of entities"""
        return len(self._entities)



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
        self._mdib_node: Union[etree_.Element, None] = None
        self._md_description_node: Union[etree_.Element, None] = None
        self._md_state_node: Union[etree_.Element, None] = None
        self._entities: dict[str, XmlEntity | XmlMultiStateEntity] = {}  # key is the handle
        self._entity_factory = entity_factory or mk_xml_entity
        self.entities: EntityGetterProtocol = ConsumerEntityGetter(self._entities, self)

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
        self._mdib_node = root_node[0]
        self._entities.clear()
        for child_element in self._mdib_node:  # MdDescription, MdState; both are optional
            if child_element.tag == pm_qnames.MdState:
                self._md_state_node = child_element
            if child_element.tag == pm_qnames.MdDescription:
                self._md_description_node = child_element

        def register_children_with_handle(parent_node, source_mds=None):
            parent_handle = parent_node.attrib.get('Handle')
            for child_node in parent_node[:]:
                child_handle = child_node.attrib.get('Handle')
                if child_node.tag == pm_qnames.Mds:
                    source_mds = child_handle
                if child_handle:
                    self._entities[child_handle] = self._entity_factory(child_node,
                                                                        parent_handle,
                                                                        source_mds)
                    register_children_with_handle(child_node, source_mds)

        if self._md_description_node is not None:
            register_children_with_handle(self._md_description_node)

        if self._md_state_node is not None:
            for state_node in self._md_state_node:
                descriptor_handle = state_node.attrib['DescriptorHandle']
                entity = self._entities[descriptor_handle]
                if entity.is_multi_state:
                    handle = state_node.attrib['Handle']
                    entity.states[handle] = state_node
                else:
                    entity.state = state_node

    def mk_entity(self, handle) -> Entity | MultiStateEntity:
        xml_entity = self._entities[handle]
        return xml_entity.mk_entity(self)


