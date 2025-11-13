"""The module contains the implementation of MdibBase plus entity interface."""
from __future__ import annotations

import copy
import traceback
import uuid
from dataclasses import dataclass
from threading import Lock
from typing import TYPE_CHECKING, Any

from lxml import etree

from sdc11073 import multikey
from sdc11073 import observableproperties as properties
from sdc11073.etc import apply_map
from sdc11073.xml_types.pm_types import Coding, have_matching_codes

if TYPE_CHECKING:
    from collections.abc import Iterable

    from lxml.etree import QName

    from sdc11073 import xml_utils
    from sdc11073.definitions_base import BaseDefinitions
    from sdc11073.loghelper import LoggerAdapter
    from sdc11073.xml_types.pm_types import CodedValue

    from .descriptorcontainers import AbstractDescriptorContainer, AbstractOperationDescriptorContainer
    from .statecontainers import AbstractMultiStateContainer, AbstractStateContainer

@dataclass
class MdibVersionGroup:
    """These 3 values define an mdib version."""

    mdib_version: int
    sequence_id: str
    instance_id: int | None


class _MultikeyWithVersionLookup(multikey.MultiKeyLookup):
    """_MultikeyWithVersionLookup keeps track of versions of removed objects.

    If a descriptor od state gets removed from mdib and later is added again, the version of that instance
    must be greater than the last version before it was removed.
    """

    def __init__(self):
        super().__init__()
        self.handle_version_lookup = {}

    def remove_object(self, obj: Any):
        if obj is not None:
            self._save_version(obj)
        super().remove_object(obj)

    def remove_object_no_lock(self, obj: Any):
        """Remove object from table without locking."""
        if obj is not None:
            self._save_version(obj)
        super().remove_object_no_lock(obj)

    def remove_objects_no_lock(self, objects: list[Any]):
        apply_map(self._save_version, [obj for obj in objects if obj is not None])
        super().remove_objects_no_lock(objects)


class DescriptorsLookup(_MultikeyWithVersionLookup):
    """DescriptorsLookup is the table-like storage for descriptors.

    It has the following search indices:
     - handle is the index for descriptor.Handle.
     - parent_handle is the index for descriptor.parent_handle, it finds all children of a queried descriptor.
     - NODETYPE is the index for descriptor.NODETYPE. It finds all children of a queried type.
       This index works only for exact matches, class hierarchy is unknown here. E.g. AlertDescriptor only returns
       AlertDescriptor objects, not LimitAlertDescriptor!
     - coding is the index for descriptor.coding.
     - condition_signaled is the index for descriptor.ConditionSignaled, it finds only AlertSignalDescriptors.
     - source is the index for descriptor.Source, it finds only AlertConditionDescriptors.
    """

    handle: multikey.UIndexDefinition[str, list[AbstractDescriptorContainer]]
    parent_handle: multikey.IndexDefinition[str, list[AbstractDescriptorContainer]]
    NODETYPE: multikey.IndexDefinition[etree.QName, list[AbstractDescriptorContainer]]
    coding: multikey.IndexDefinition[Coding, list[AbstractDescriptorContainer]]
    condition_signaled: multikey.IndexDefinition[str, list[AbstractDescriptorContainer]]
    source: multikey.IndexDefinition[str, list[AbstractDescriptorContainer]]

    def __init__(self):
        super().__init__()
        self.add_index('handle', multikey.UIndexDefinition(lambda obj: obj.Handle))
        self.add_index('parent_handle', multikey.IndexDefinition(lambda obj: obj.parent_handle))
        self.add_index('NODETYPE', multikey.IndexDefinition(lambda obj: obj.NODETYPE))
        self.add_index('coding', multikey.IndexDefinition(lambda obj: obj.coding))
        self.add_index('condition_signaled',
                       multikey.IndexDefinition(lambda obj: obj.ConditionSignaled, index_none_values=False))
        # an index to find all alert conditions for a metric (AlertCondition is the only class that has a
        # "Source" attribute, therefore this simple approach without type testing is sufficient):
        self.add_index('source', multikey.IndexDefinition1n(lambda obj: obj.Source, index_none_values=False))

    def _save_version(self, obj: AbstractDescriptorContainer):
        self.handle_version_lookup[obj.Handle] = obj.DescriptorVersion

    def set_version(self, obj: AbstractDescriptorContainer):
        """Set DescriptorVersion of obj if descriptor with same handle existed before."""
        version = self.handle_version_lookup.get(obj.Handle)
        if version is not None:
            obj.DescriptorVersion = version + 1

    def add_object(self, obj: AbstractDescriptorContainer):
        """Append object with locking."""
        with self._lock:
            self.add_object_no_lock(obj)

    def add_object_no_lock(self, obj: AbstractDescriptorContainer):
        """Append object without locking."""
        super().add_object_no_lock(obj)

    def add_objects(self, objects: list[AbstractDescriptorContainer]):
        """Append objects with locking."""
        with self._lock:
            self.add_objects_no_lock(objects)

    def add_objects_no_lock(self, objects: list[AbstractDescriptorContainer]):
        """Append objects without locking."""
        apply_map(self.add_object_no_lock, objects)

    def remove_object(self, obj: AbstractDescriptorContainer):
        """Remove object from table."""
        keys = self._object_ids.get(id(obj))
        if keys is None:
            return
        with self._lock:
            self.remove_object_no_lock(obj)

    def remove_object_no_lock(self, obj: AbstractDescriptorContainer):
        """Remove object from table without locking."""
        super().remove_object_no_lock(obj)

    def remove_objects(self, objects: list[AbstractDescriptorContainer]):
        """Remove objects from table with locking."""
        with self._lock:
            self.remove_objects_no_lock(objects)

    def remove_objects_no_lock(self, objects: list[AbstractDescriptorContainer]):
        """Remove objects from table without locking."""
        apply_map(self.remove_object_no_lock, objects)


class StatesLookup(_MultikeyWithVersionLookup):
    """StatesLookup is the table-like storage for states.

    It has search indices:
     - descriptor_handle is the index for descriptor.DescriptorHandle.
     - NODETYPE is the index for descriptor.NODETYPE. It finds all children of a queried type.
       This index works only for exact matches, class hierarchy is unknown here. E.g. AlertState only returns
       AlertState objects, not LimitAlertState!
    """

    descriptor_handle: multikey.UIndexDefinition[str, list[AbstractDescriptorContainer]]
    NODETYPE: multikey.IndexDefinition[etree.QName, list[AbstractDescriptorContainer]]

    def __init__(self):
        super().__init__()
        self.add_index('descriptor_handle', multikey.UIndexDefinition(lambda obj: obj.DescriptorHandle))
        self.add_index('NODETYPE', multikey.IndexDefinition(lambda obj: obj.NODETYPE, index_none_values=False))

    def _save_version(self, obj: AbstractStateContainer):
        self.handle_version_lookup[obj.DescriptorHandle] = obj.StateVersion

    def set_version(self, obj: AbstractStateContainer):
        """Set StateVersion of obj if state with same handle existed before."""
        version = self.handle_version_lookup.get(obj.DescriptorHandle)
        if version is not None:
            obj.StateVersion = version + 1

    def add_object_no_lock(self, obj: AbstractStateContainer):
        """Append object without locking."""
        if obj.is_multi_state:
            raise ValueError('no MultiState!')
        super().add_object_no_lock(obj)


class MultiStatesLookup(_MultikeyWithVersionLookup):
    """MultiStatesLookup is the table-like storage for multi-states.

    It has search indices:
     - descriptor_handle is the index for descriptor.DescriptorHandle.
     - handle is the index for descriptor.Handle.
     - NODETYPE is the index for descriptor.NODETYPE. It finds all children of a queried type.
       This index works only for exact matches, class hierarchy is unknown here.
       AlertDescriptor objects, not LimitAlertDescriptor!
    """

    descriptor_handle: multikey.IndexDefinition[str, list[AbstractMultiStateContainer]]
    handle: multikey.UIndexDefinition[str, list[AbstractMultiStateContainer]]
    NODETYPE: multikey.IndexDefinition[etree.QName, list[AbstractMultiStateContainer]]

    def __init__(self):
        super().__init__()
        self.add_index('descriptor_handle', multikey.IndexDefinition(lambda obj: obj.DescriptorHandle))
        self.add_index('handle',
                       multikey.UIndexDefinition(lambda obj: obj.Handle, index_none_values=False))
        self.add_index('NODETYPE',
                       multikey.IndexDefinition(lambda obj: obj.NODETYPE, index_none_values=False))

    def _save_version(self, obj: AbstractMultiStateContainer):
        self.handle_version_lookup[obj.Handle] = obj.StateVersion

    def set_version(self, obj: AbstractMultiStateContainer):
        """Set StateVersion of obj if state with same handle existed before."""
        version = self.handle_version_lookup.get(obj.Handle)
        if version is not None:
            obj.StateVersion = version + 1


class _EntityBase:

    def __init__(self, mdib: MdibBase, descriptor: AbstractDescriptorContainer):
        self._mdib = mdib
        self.descriptor = descriptor

    @property
    def handle(self) -> str:
        return self.descriptor.Handle

    @property
    def parent_handle(self) -> str:
        return self.descriptor.parent_handle

    @property
    def node_type(self) -> QName:
        return self.descriptor.NODETYPE

    def update(self):
        """Update the entity from current data in mdib."""
        orig = self._mdib.descriptions.get_one(self.handle)
        self.descriptor.update_from_other_container(orig)

class Entity(_EntityBase):
    """Groups descriptor and state."""

    def __init__(self, mdib: MdibBase, descriptor: AbstractDescriptorContainer, state: AbstractStateContainer):
        super().__init__(mdib, descriptor)
        self.state = state


    @property
    def is_multi_state(self) -> bool:
        """Return False because this is not a multi state entity."""
        return False

    def update(self):
        """Update the entity from current data in mdib."""
        super().update()
        orig = self._mdib.states.get_one(self.handle)
        self.state.update_from_other_container(orig)


class MultiStateEntity(_EntityBase):
    """Groups descriptor and list of multi-states."""

    def __init__(self, mdib: MdibBase, descriptor: AbstractDescriptorContainer,
                 states: list[AbstractMultiStateContainer]):
        super().__init__(mdib, descriptor)
        self.states: dict[str, AbstractMultiStateContainer] = {s.Handle: s for s in states}

    @property
    def is_multi_state(self) -> bool:
        """Return True because this is a multi state entity."""
        return True

    def update(self):
        """Update the entity from current data in mdib."""
        super().update()

        all_orig_states = self._mdib.context_states.descriptor_handle.get(self.handle, [])
        states_dict = { st.Handle: st for st in all_orig_states}
        # update existing states, remove deleted ones
        for state in list(self.states.values()):
            orig = states_dict.get(state.Handle)
            if orig is not None:
                state.update_from_other_container(orig)
            else:
                self.states.pop(state.Handle)
        # add new states
        for handle, _ in states_dict.items(): # noqa: PERF102
            if handle not in self.states:
                self.states[handle] = states_dict[handle].mk_copy()

    def new_state(self, state_handle: str | None = None) -> AbstractMultiStateContainer:
        """Create a new state."""
        if state_handle in self.states:
            msg = f'State handle {state_handle} already exists in {self.__class__.__name__}, handle = {self.handle}'
            raise ValueError(msg)
        cls = self._mdib.data_model.get_state_container_class(self.descriptor.STATE_QNAME)
        state = cls(descriptor_container=self.descriptor)
        state.Handle = state_handle or uuid.uuid4().hex
        self.states[state.Handle] = state
        return state


class EntityGetter:
    """Implementation of EntityGetterProtocol for MdibBase."""

    def __init__(self, mdib: MdibBase):
        self._mdib = mdib

    def by_handle(self, handle: str) ->  Entity | MultiStateEntity | None:
        """Return entity with given handle."""
        descriptor = self._mdib.descriptions.handle.get_one(handle, allow_none=True)
        if descriptor is None:
            return None
        return self._mk_entity(descriptor)

    def by_node_type(self, node_type: QName) -> list[Entity | MultiStateEntity]:
        """Return all entities with given node type."""
        descriptors = self._mdib.descriptions.NODETYPE.get(node_type, [])
        return [self._mk_entity(d) for d in descriptors]

    def by_parent_handle(self, parent_handle: str | None) -> list[Entity | MultiStateEntity]:
        """Return all entities with descriptors parent_handle == provided parent_handle."""
        descriptors = self._mdib.descriptions.parent_handle.get(parent_handle, [])
        return [self._mk_entity(d) for d in descriptors]

    def by_coding(self, coding: Coding) -> list[Entity | MultiStateEntity]:
        """Return all entities with descriptors type are equivalent to codeding."""
        descriptors = [d for d in self._mdib.descriptions.objects if d.Type.is_equivalent(coding)]
        return [self._mk_entity(d) for d in descriptors]

    def by_coded_value(self, coded_value: CodedValue) -> list[Entity | MultiStateEntity]:
        """Return all entities with descriptors type are equivalent to coded_value."""
        descriptors = [d for d in self._mdib.descriptions.objects if d.Type.is_equivalent(coded_value)]
        return [self._mk_entity(d) for d in descriptors]

    def _mk_entity(self, descriptor: AbstractDescriptorContainer) -> Entity | MultiStateEntity:
        if descriptor.is_context_descriptor:
            states = self._mdib.context_states.descriptor_handle.get(descriptor.Handle, [])
            return MultiStateEntity(self._mdib,copy.deepcopy(descriptor), copy.deepcopy(states))
        state = self._mdib.states.descriptor_handle.get_one(descriptor.Handle)
        return Entity(self._mdib, copy.deepcopy(descriptor), copy.deepcopy(state))

    def items(self) -> Iterable[tuple[str, Entity | MultiStateEntity]]:
        """Return the items of a dictionary."""
        for descriptor in self._mdib.descriptions.objects:
            yield descriptor.Handle, self._mk_entity(descriptor)

    def __len__(self) -> int:
        """Return number of entities."""
        return len(self._mdib.descriptions.objects)



class MdibBase:
    """Base class with common functionality of provider mdib and consumer mdib."""

    # these observables can be used to watch any change of data in the mdib.
    # They contain lists of containers that were changed.
    # every transaction (device mdib) or notification (client mdib) will report their changes here.
    metrics_by_handle = properties.ObservableProperty(fire_only_on_changed_value=False)
    waveform_by_handle = properties.ObservableProperty(fire_only_on_changed_value=False)
    alert_by_handle = properties.ObservableProperty(fire_only_on_changed_value=False)
    context_by_handle = properties.ObservableProperty(fire_only_on_changed_value=False)
    component_by_handle = properties.ObservableProperty(fire_only_on_changed_value=False)
    new_descriptors_by_handle = properties.ObservableProperty(fire_only_on_changed_value=False)
    updated_descriptors_by_handle = properties.ObservableProperty(fire_only_on_changed_value=False)
    deleted_descriptors_by_handle = properties.ObservableProperty(fire_only_on_changed_value=False)
    deleted_states_by_handle = properties.ObservableProperty(
        fire_only_on_changed_value=False)  # is a result of deleted descriptors
    description_modifications = properties.ObservableProperty(fire_only_on_changed_value=False)
    operation_by_handle = properties.ObservableProperty(fire_only_on_changed_value=False)
    sequence_id = properties.ObservableProperty()
    instance_id = properties.ObservableProperty()

    def __init__(self, sdc_definitions: type[BaseDefinitions], logger: LoggerAdapter):
        """Construct MdibBase.

        :param sdc_definitions: a class derived from BaseDefinitions
        """
        self.sdc_definitions = sdc_definitions
        self.data_model = sdc_definitions.data_model
        self._logger = logger
        self.nsmapper = sdc_definitions.data_model.ns_helper
        self.mdib_version = 0
        self.sequence_id = ''  # needs to be set to a reasonable value by derived class
        self.instance_id = None  # None or an unsigned int
        self.log_prefix = ''
        self.descriptions = DescriptorsLookup()
        self.states = StatesLookup()
        self.context_states = MultiStatesLookup()
        self.mdib_lock = Lock()
        self.mdstate_version = 0
        self.mddescription_version = 0


    @property
    def logger(self) -> LoggerAdapter:
        """Return the logger."""
        return self._logger

    @property
    def mdib_version_group(self) -> MdibVersionGroup:
        """"Get current version data."""
        return MdibVersionGroup(self.mdib_version, self.sequence_id, self.instance_id)

    def add_description_containers(self, description_containers: list[AbstractDescriptorContainer]):
        """Initialize descriptions member with provided descriptors.

        :param description_containers: a list of DescriptorContainer objects
        """
        new_descriptor_by_handle = {}
        with self.descriptions.lock:
            for description_container in description_containers:
                self.descriptions.add_object_no_lock(description_container)
                new_descriptor_by_handle[description_container.Handle] = description_container

        # finally update observable property
        if new_descriptor_by_handle:
            self.new_descriptors_by_handle = new_descriptor_by_handle

    def clear_states(self):
        """Remove all states and context states."""
        with self.states.lock:
            self.states.clear()
            self.context_states.clear()

        # clear also the observable properties
        self.metrics_by_handle = None
        self.waveform_by_handle = None
        self.alert_by_handle = None
        self.context_by_handle = None
        self.component_by_handle = None
        self.operation_by_handle = None

    def _set_descriptor_container_reference(self, state_container: AbstractStateContainer):
        """Set state_container.descriptor_container if all is fine, otherwise logs error."""
        descriptor_container = self.descriptions.handle.get_one(state_container.DescriptorHandle,
                                                                allow_none=True)
        if descriptor_container is None:
            self._logger.warning(  # noqa: PLE1205
                'state "{}" (type={}) has no descriptor in mdib!',
                state_container.DescriptorHandle, state_container.NODETYPE)
        elif descriptor_container.DescriptorVersion == state_container.DescriptorVersion:
            state_container.descriptor_container = descriptor_container
        else:
            self._logger.warning(  # noqa: PLE1205
                'state "{}" (type={}) : descriptor version expect "{}", found "{}"',
                state_container.DescriptorHandle, state_container.NODETYPE,
                descriptor_container.DescriptorVersion, state_container.DescriptorVersion)

    def add_state_containers(self, state_containers: list[AbstractStateContainer | AbstractMultiStateContainer]):
        """Add states to self.states and self.context_states.

        This method does not update the observables, so use with care!
        :param state_containers: a list of StateContainer objects.
        """
        for state_container in state_containers:
            if state_container.descriptor_container is None:
                self._set_descriptor_container_reference(state_container)
            my_multikey = self.context_states if state_container.is_context_state else self.states
            try:
                my_multikey.add_object(state_container)
            except KeyError as ex:
                if state_container.is_context_state:
                    self._logger.error('add_state_containers: {}, Handle={}; {}',  # noqa: PLE1205 TRY400
                                       ex, state_container.Handle, traceback.format_exc())
                else:
                    self._logger.error('add_state_containers: {}, DescriptorHandle={}; {}',  # noqa: PLE1205 TRY400
                                       ex, state_container.DescriptorHandle, traceback.format_exc())

    def _reconstruct_md_description(self) -> xml_utils.LxmlElement:
        """Build dom tree of descriptors from current data."""
        pm = self.data_model.pm_names
        doc_nsmap = self.nsmapper.ns_map
        root_containers = self.descriptions.parent_handle.get(None) or []
        md_description_node = etree.Element(pm.MdDescription,
                                            attrib={'DescriptionVersion': str(self.mddescription_version)},
                                            nsmap=doc_nsmap)
        for root_container in root_containers:
            self.make_descriptor_node(root_container, md_description_node, tag=pm.Mds, set_xsi_type=False)
        return md_description_node

    def make_descriptor_node(self,
                             descriptor_container: AbstractDescriptorContainer,
                             parent_node: xml_utils.LxmlElement,
                             tag: etree.QName,
                             set_xsi_type: bool = True) -> xml_utils.LxmlElement:
        """Create a lxml etree node with subtree from instance data.

        :param descriptor_container: a descriptor container instance
        :param parent_node: parent node
        :param tag: tag of node
        :param set_xsi_type: if true, the NODETYPE will be used to set the xsi:type attribute of the node
        :return: an etree node.
        """
        ns_map = self.nsmapper.partial_map(self.nsmapper.PM, self.nsmapper.XSI) \
            if set_xsi_type else self.nsmapper.partial_map(self.nsmapper.PM)
        node = etree.SubElement(parent_node,
                                tag,
                                attrib={'Handle': descriptor_container.Handle},
                                nsmap=ns_map)
        descriptor_container.update_node(node, self.nsmapper, set_xsi_type)  # create all
        child_list = self.descriptions.parent_handle.get(descriptor_container.Handle, [])
        # append all child containers, then bring all child elements in correct order
        for child in child_list:
            child_tag, set_xsi = descriptor_container.tag_name_for_child_descriptor(child.NODETYPE)
            self.make_descriptor_node(child, node, child_tag, set_xsi)
        descriptor_container.sort_child_nodes(node)
        return node

    def _reconstruct_mdib(self, add_context_states: bool) -> xml_utils.LxmlElement:
        """Build dom tree of mdib from current data.

        If add_context_states is False, context states are not included.
        """
        pm = self.data_model.pm_names
        msg = self.data_model.msg_names
        doc_nsmap = self.nsmapper.ns_map
        mdib_node = etree.Element(msg.Mdib, nsmap=doc_nsmap)
        mdib_node.set('MdibVersion', str(self.mdib_version))
        mdib_node.set('SequenceId', self.sequence_id)
        if self.instance_id is not None:
            mdib_node.set('InstanceId', str(self.instance_id))
        md_description_node = self._reconstruct_md_description()
        mdib_node.append(md_description_node)

        # add a list of states
        md_state_node = etree.SubElement(mdib_node, pm.MdState,
                                         attrib={'StateVersion': str(self.mdstate_version)},
                                         nsmap=doc_nsmap)
        tag = pm.State
        for state_container in self.states.objects:
            md_state_node.append(state_container.mk_state_node(tag, self.nsmapper))
        if add_context_states:
            for state_container in self.context_states.objects:
                md_state_node.append(state_container.mk_state_node(tag, self.nsmapper))
        return mdib_node

    def reconstruct_md_description(self) -> (xml_utils.LxmlElement, MdibVersionGroup):
        """Build dom tree of descriptors from current data."""
        with self.mdib_lock:
            node = self._reconstruct_md_description()
            return node, self.mdib_version_group

    def reconstruct_mdib(self) -> (xml_utils.LxmlElement, MdibVersionGroup):
        """Build dom tree from current data.

        This method does not include context states!
        """
        with self.mdib_lock:
            return self._reconstruct_mdib(add_context_states=False), self.mdib_version_group

    def reconstruct_mdib_with_context_states(self) -> (xml_utils.LxmlElement, MdibVersionGroup):
        """Build dom tree from current data.

        This method includes the context states.
        """
        with self.mdib_lock:
            return self._reconstruct_mdib(add_context_states=True), self.mdib_version_group

    def _get_child_descriptors_by_code(self, parent_handle: str, code: Coding) -> list[AbstractDescriptorContainer]:
        descriptors = self.descriptions.parent_handle.get(parent_handle, [])
        if len(descriptors) == 0:
            return []
        with_types = [d for d in descriptors if d.Type is not None]
        return [d for d in with_types if have_matching_codes(d.Type, code)]

    def get_metric_descriptor_by_code(self,
                                      vmd_code: [Coding, CodedValue],
                                      channel_code: [Coding, CodedValue],
                                      metric_code: [Coding, CodedValue]) -> AbstractDescriptorContainer | None:
        """get_metric_descriptor_by_code is the "correct" way to find a descriptor.

        Using handles is shaky, because they have no meaning and can change over time!
        """
        pm = self.data_model.pm_names
        all_vmds = self.descriptions.NODETYPE.get(pm.VmdDescriptor, [])
        matching_vmd_list = [d for d in all_vmds if have_matching_codes(d.Type, vmd_code)]
        for vmd in matching_vmd_list:
            matching_channels = self._get_child_descriptors_by_code(vmd.Handle, channel_code)
            for channel in matching_channels:
                matching_metrics = self._get_child_descriptors_by_code(channel.Handle, metric_code)
                if len(matching_metrics) == 1:
                    return matching_metrics[0]
                if len(matching_metrics) > 1:
                    msg = f'found multiple metrics for vmd={vmd_code} channel={channel_code} metric={metric_code}'
                    raise ValueError(msg)
        return None

    def get_operations_for_metric(self,
                                  vmd_code: [Coding, CodedValue],
                                  channel_code: [Coding, CodedValue],
                                  metric_code: [Coding, CodedValue]) -> list[AbstractDescriptorContainer]:
        """get_operations_for_metric is the "correct" way to find an operation.

        Using handles is shaky, because they have no meaning and can change over time!
        """
        descriptor_container = self.get_metric_descriptor_by_code(vmd_code, channel_code, metric_code)
        return self.get_operation_descriptors_for_descriptor_handle(descriptor_container.Handle)

    def get_operation_descriptors_for_descriptor_handle(self, descriptor_handle: str,
                                                        **additional_filters: Any) -> list[AbstractDescriptorContainer]:
        """Get operation descriptors that have descriptor_handle as OperationTarget.

        :param descriptor_handle: the handle for that operations shall be found
        :return: a list with operation descriptors that have descriptor_handle as OperationTarget. List can be empty
        :additionalFilters: optional filters for the key = name of member attribute, value = expected value
            example: NODETYPE=pm.SetContextStateOperationDescriptor filters for SetContextStateOperation descriptors
        """
        all_operation_containers = self.get_operation_descriptors()
        my_operations = [op_c for op_c in all_operation_containers if op_c.OperationTarget == descriptor_handle]
        for key, value in additional_filters.items():
            my_operations = [op for op in my_operations if getattr(op, key) == value]
        return my_operations

    def get_operation_descriptors(self) -> list[AbstractOperationDescriptorContainer]:
        """Get a list of all operation descriptors."""
        pm = self.data_model.pm_names
        result = []
        for node_type in (pm.SetValueOperationDescriptor,
                          pm.SetStringOperationDescriptor,
                          pm.ActivateOperationDescriptor,
                          pm.SetContextStateOperationDescriptor,
                          pm.SetMetricStateOperationDescriptor,
                          pm.SetComponentStateOperationDescriptor,
                          pm.SetAlertStateOperationDescriptor):
            result.extend(self.descriptions.NODETYPE.get(node_type, []))
        return result

    def select_descriptors(self, *codings: list[Coding | CodedValue | str]) -> list[AbstractDescriptorContainer]:
        """Return all descriptor containers that match a path defined by list of codings.

        Example:
        -------
        [Coding('70041')] returns all containers that have Coding('70041') in its Type
        [Coding('70041'), Coding('69650')] : returns all descriptors with Coding('69650')
                                     and parent descriptor Coding('70041')
        [Coding('70041'), Coding('69650'), Coding('69651')] : returns all descriptors with Coding('69651') and
                                     parent descriptor Coding('69650') and parent's parent descriptor Coding('70041')
        It is not necessary that path starts at the top of a mds, it can start anywhere.
        :param codings: each element can be a string (which is handled as a Coding with DEFAULT_CODING_SYSTEM),
                         a Coding or a CodedValue.

        """
        selected_objects = self.descriptions.objects  # start with all objects
        for counter, coding in enumerate(codings):
            # normalize coding
            if isinstance(coding, str):
                coding = Coding(coding)  # noqa: PLW2901
            if counter > 0:
                # replace selected_objects with all children of selected objects
                all_handles = [o.Handle for o in selected_objects]  # pylint: disable=not-an-iterable
                selected_objects = []
                for handle in all_handles:
                    selected_objects.extend(self.descriptions.parent_handle.get(handle, []))
            # filter current list
            selected_objects = [o for o in selected_objects if
                                o.Type is not None and have_matching_codes(o.Type, coding)]
        return selected_objects

    def get_all_descriptors_in_subtree(self, root_descriptor_container: AbstractDescriptorContainer,
                                       depth_first: bool = True,
                                       include_root: bool = True) -> list[AbstractDescriptorContainer]:
        """Return the tree below descriptor_container as a flat list.

        :param root_descriptor_container: root descriptor
        :param depth_first: determines order of returned list.
               If depth_first=True result has all leaves on top, otherwise at the end.
        :param include_root: if True descriptor_container itself is also part of returned list
        :return: a list of DescriptorContainer objects.
        """
        result = []

        def _getchildren(parent: AbstractDescriptorContainer):
            child_containers = self.descriptions.parent_handle.get(parent.Handle, [])
            if not depth_first:
                result.extend(child_containers)
            apply_map(_getchildren, child_containers)
            if depth_first:
                result.extend(child_containers)

        if include_root and not depth_first:
            result.append(root_descriptor_container)
        _getchildren(root_descriptor_container)
        if include_root and depth_first:
            result.append(root_descriptor_container)
        return result

    def rm_descriptors_and_states(self, descriptor_containers: list[AbstractDescriptorContainer]):
        """Delete descriptors and all related states."""
        deleted_descriptors = {}
        deleted_states = {}
        for descriptor_container in descriptor_containers:
            self._logger.debug('rm Descriptor node {} handle {}',  # noqa: PLE1205
                               descriptor_container.NODETYPE, descriptor_container.Handle)
            self.descriptions.remove_object(descriptor_container)
            deleted_descriptors[descriptor_container.Handle] = descriptor_container
            for m_key in (self.states, self.context_states):
                state_containers = m_key.descriptor_handle.get(descriptor_container.Handle)
                if state_containers is not None:
                    # make a copy, otherwise remove_objects will manipulate same list in place
                    state_containers = state_containers[:]
                    self._logger.debug('rm {} states(s) associated to descriptor {} ',  # noqa: PLE1205
                                       len(state_containers), descriptor_container.Handle)
                    m_key.remove_objects(state_containers)
                    deleted_states[descriptor_container.Handle] = state_containers

        if deleted_descriptors:
            self.deleted_descriptors_by_handle = deleted_descriptors
        if deleted_states:
            self.deleted_states_by_handle = deleted_states

    def rm_descriptor_by_handle(self, handle: str):
        """Delete descriptor and the subtree and related states."""
        descriptor_container = self.descriptions.handle.get_one(handle, allow_none=True)
        if descriptor_container is not None:
            all_descriptors = self.get_all_descriptors_in_subtree(descriptor_container)
            self.rm_descriptors_and_states(all_descriptors)

    def get_entity(self, handle: str) -> Entity:
        """Return descriptor and state as Entity."""
        descr = self.descriptions.handle.get_one(handle)
        state = self.states.descriptor_handle.get_one(handle)
        return Entity(self, descr, state)

    def get_context_entity(self, handle: str) -> MultiStateEntity:
        """Return descriptor and states as MultiStateEntity."""
        descr = self.descriptions.handle.get_one(handle)
        states = self.context_states.descriptor_handle.get(handle, [])
        return MultiStateEntity(self, descr, states)

    def has_multiple_mds(self) -> bool:
        """Check if there is more than one mds in mdib (convenience method)."""
        all_mds_descriptors = self.descriptions.NODETYPE.get(self.data_model.pm_names.MdsDescriptor)
        return len(all_mds_descriptors) > 1
