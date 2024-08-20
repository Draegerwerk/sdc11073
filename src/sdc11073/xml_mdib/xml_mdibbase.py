from __future__ import annotations

import traceback
from dataclasses import dataclass
from threading import Lock
from typing import TYPE_CHECKING, Callable

from lxml import etree as etree_

from sdc11073 import multikey
from sdc11073 import observableproperties as properties
from sdc11073.etc import apply_map
from sdc11073.xml_types.pm_types import Coding, have_matching_codes
from sdc11073.xml_types import msg_qnames, pm_qnames
from sdc11073.mdib.mdibbase import MdibVersionGroup
from sdc11073.mdib.mdibbase import Entity, MultiStateEntity
from sdc11073.namespaces import default_ns_helper as ns_helper
from sdc11073.namespaces import text_to_qname
if TYPE_CHECKING:
    from lxml.etree import QName
    from sdc11073.definitions_base import BaseDefinitions
    from sdc11073.loghelper import LoggerAdapter
    from sdc11073.xml_types.pm_types import CodedValue
    from sdc11073 import xml_utils

    # from .descriptorcontainers import AbstractDescriptorContainer, AbstractOperationDescriptorContainer
    # from .statecontainers import AbstractMultiStateContainer, AbstractStateContainer


xsi_type_qname = ns_helper.XSI.tag('type')

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


def get_xsi_type(element: etree_.Element) -> QName:
    """Return the BICEPS type of an element.

    If there is a xsi:type entry, this specifies the type.
    If not, the tag is used to determine the type.
    """
    xsi_type_str = element.attrib.get(xsi_type_qname)

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
    """Groups descriptor and state."""
    parent_handle: str | None
    source_mds: str | None
    node_type: etree_.QName
    descriptor: etree_.Element


@dataclass
class XmlEntity(_XmlEntityBase):
    """Groups descriptor and state."""
    state: etree_.Element | None

    @property
    def is_multi_state(self) -> bool:
        return False

@dataclass
class XmlMultiStateEntity(_XmlEntityBase):
    """Groups descriptor and list of multi-states."""
    states: list[etree_.Element]

    @property
    def is_multi_state(self) -> bool:
        return True


class XmlMdibBase:

    metric_handles = properties.ObservableProperty(fire_only_on_changed_value=False)
    waveform_handles = properties.ObservableProperty(fire_only_on_changed_value=False)
    alert_handles = properties.ObservableProperty(fire_only_on_changed_value=False)
    context_handles = properties.ObservableProperty(fire_only_on_changed_value=False)
    component_handles = properties.ObservableProperty(fire_only_on_changed_value=False)
    new_descriptors_handles = properties.ObservableProperty(fire_only_on_changed_value=False)
    updated_descriptors_handles = properties.ObservableProperty(fire_only_on_changed_value=False)
    deleted_descriptors_handles = properties.ObservableProperty(fire_only_on_changed_value=False)
    # deleted_states_handles = properties.ObservableProperty(
    #     fire_only_on_changed_value=False)  # is a result of deleted descriptors
    # description_modifications = properties.ObservableProperty(fire_only_on_changed_value=False)
    operation_handles = properties.ObservableProperty(fire_only_on_changed_value=False)

    multi_state_q_names = (pm_qnames.PatientContextDescriptor,
                           pm_qnames.LocationContextDescriptor,
                           pm_qnames.WorkflowContextDescriptor,
                           pm_qnames.OperatorContextDescriptor,
                           pm_qnames.MeansContextDescriptor,
                           pm_qnames.EnsembleContextDescriptor)

    def __init__(self, sdc_definitions: type[BaseDefinitions], logger: LoggerAdapter):
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
        self.mdstate_version = 0
        self.mddescription_version = 0

        self._get_mdib_response_node: etree_.Element | None = None
        self._md_state_node: etree_.Element | None = None
        self._entities: dict[str, XmlEntity | XmlMultiStateEntity] = {}  # key is the handle

    @property
    def mdib_version_group(self) -> MdibVersionGroup:
        """"Get current version data."""
        return MdibVersionGroup(self.mdib_version, self.sequence_id, self.instance_id)

    @property
    def logger(self) -> LoggerAdapter:
        """Return the logger."""
        return self._logger

    def _set_root_node(self, root_node: etree_.Element):
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
                    print (child_node.attrib)
                    xsi_type = get_xsi_type(child_node)
                    if xsi_type in self.multi_state_q_names:
                        self._entities[child_handle] = XmlMultiStateEntity(parent_handle, source_mds, xsi_type, child_node, [])
                    else:
                        self._entities[child_handle] = XmlEntity(parent_handle, source_mds, xsi_type, child_node, None)
                    register_children_with_handle(child_node, source_mds)

        for mdib_node in root_node:
            md_description_node = mdib_node[0]
            md_state_node = mdib_node[1]
            register_children_with_handle(md_description_node, )
            for state_node in md_state_node:
                handle = state_node.attrib['DescriptorHandle']
                entity = self._entities[handle]
                if isinstance(entity, MultiStateEntity):
                    entity.states.append(state_node)
                else:
                    entity.state = state_node

    def mk_entity(self, handle) -> Entity | MultiStateEntity:
        xml_entity = self._entities[handle]
        cls = self.sdc_definitions.data_model.get_descriptor_container_class(xml_entity.node_type)
        if cls is None:
            raise ValueError(f'do not know how to make container from {str(xml_entity.node_type)}')
        descriptor_container = cls(handle, parent_handle=xml_entity.parent_handle)
        descriptor_container.update_from_node(xml_entity.descriptor)
        descriptor_container.set_source_mds(xml_entity.source_mds)
        if isinstance(xml_entity, XmlEntity):
            cls = self.sdc_definitions.data_model.get_state_container_class(descriptor_container.STATE_QNAME)
            state_container = cls(descriptor_container)
            state_container.update_from_node(xml_entity.state)
            ret = Entity(descriptor_container, state_container)
        else:
            ret = MultiStateEntity(descriptor_container, [])
            for state in xml_entity.states:
                cls = self.sdc_definitions.data_model.get_state_container_class(descriptor_container.STATE_QNAME)
                state_container = cls(descriptor_container)
                state_container.update_from_node(xml_entity.state)
                ret.states.append(state_container)
        return ret

    @property
    def handle(self) -> _HandleGetter:
        return _HandleGetter(self._entities, self.mk_entity)

    @property
    def node_type(self) -> _NodetypeGetter:
        return _NodetypeGetter(self._entities, self.mk_entity)

    @property
    def parent_handle(self) -> _NodetypeGetter:
        return _ParentHandleGetter(self._entities, self.mk_entity)


class _HandleGetter:
    def __init__(self,
                 entities: dict[str, XmlEntity | XmlMultiStateEntity],
                 mk_entity: Callable[[str], Entity | MultiStateEntity]):
        self._entities = entities
        self._mk_entity = mk_entity

    def get(self, handle: str) -> Entity | MultiStateEntity:
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