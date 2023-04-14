from __future__ import annotations

import traceback
from dataclasses import dataclass
from threading import Lock
from typing import Type, List, TYPE_CHECKING, Union

from lxml import etree as etree_

from .. import multikey
from .. import observableproperties as properties
from ..etc import apply_map
from sdc11073.xml_types.pm_types import have_matching_codes

if TYPE_CHECKING:
    from ..definitions_base import BaseDefinitions
    from .descriptorcontainers import AbstractMetricDescriptorContainer, AbstractDescriptorContainer
    from .statecontainers import AbstractStateContainer
    from sdc11073.xml_types.pm_types import CodedValue, Coding


@dataclass
class MdibVersionGroup:
    mdib_version: int
    sequence_id: str
    instance_id: Union[int, None]


class _MultikeyWithVersionLookup(multikey.MultiKeyLookup):
    """
    This class keeps track of versions of removed objects
    """

    def __init__(self):
        multikey.MultiKeyLookup.__init__(self)
        self.handle_version_lookup = {}

    def remove_object(self, obj):
        if obj is not None:
            self._save_version(obj)
        multikey.MultiKeyLookup.remove_object(self, obj)

    def remove_object_no_lock(self, obj):
        if obj is not None:
            self._save_version(obj)
        multikey.MultiKeyLookup.remove_object_no_lock(self, obj)

    def remove_objects_no_lock(self, objects):
        apply_map(self._save_version, [obj for obj in objects if obj is not None])
        multikey.MultiKeyLookup.remove_objects_no_lock(self, objects)


class DescriptorsLookup(_MultikeyWithVersionLookup):
    """ This class knows about the hierarchy of descriptors and keeps the order of objects """

    def __init__(self):
        _MultikeyWithVersionLookup.__init__(self)
        self.add_index('handle', multikey.UIndexDefinition(lambda obj: obj.Handle))
        self.add_index('parent_handle', multikey.IndexDefinition(lambda obj: obj.parent_handle))
        self.add_index('NODETYPE', multikey.IndexDefinition(lambda obj: obj.NODETYPE))
        self.add_index('coding', multikey.IndexDefinition(lambda obj: obj.coding))
        self.add_index('condition_signaled',
                       multikey.IndexDefinition(lambda obj: obj.ConditionSignaled, index_none_values=False))
        # an index to find all alert conditions for a metric (AlertCondition is the only class that has a
        # "Source" attribute, therefore this simple approach without type testing is sufficient):
        self.add_index('Source',
                       multikey.IndexDefinition1n(lambda obj: [s.text for s in obj.Source], index_none_values=False))

    def _save_version(self, obj):
        self.handle_version_lookup[obj.Handle] = obj.DescriptorVersion

    def set_version(self, obj, increment=True):
        version = self.handle_version_lookup.get(obj.Handle)
        if version is not None:
            if increment:
                version += 1
            obj.DescriptorVersion = version

    def add_object(self, obj):
        with self._lock:
            self.add_object_no_lock(obj)

    def add_object_no_lock(self, obj):
        """ appends obj to parent"""
        _MultikeyWithVersionLookup.add_object_no_lock(self, obj)
        parent = None if obj.parent_handle is None else self.handle.get_one(obj.parent_handle, allow_none=True)
        if parent is not None:
            parent.add_child(obj)

    def add_objects(self, objects):
        with self._lock:
            self.add_objects_no_lock(objects)

    def add_objects_no_lock(self, objects):
        apply_map(self.add_object_no_lock, objects)

    def remove_object(self, obj):
        keys = self._object_ids.get(id(obj))
        if keys is None:
            return
        with self._lock:
            self.remove_object_no_lock(obj)

    def remove_object_no_lock(self, obj):
        _MultikeyWithVersionLookup.remove_object_no_lock(self, obj)
        parent = self.handle.get_one(obj.parent_handle, allow_none=True)
        if parent is not None:
            parent.rm_child(obj)

    def remove_objects(self, objects):
        with self._lock:
            self.remove_objects_no_lock(objects)

    def remove_objects_no_lock(self, objects):
        apply_map(self.remove_object_no_lock, objects)

    def replace_object_no_lock(self, new_obj):
        """ remove existing descriptor_container and add new one, but do not touch child
        list of parent (that keeps order)"""
        orig_obj = self.handle.get_one(new_obj.Handle)
        self.remove_object_no_lock(orig_obj)
        self.add_object_no_lock(new_obj)


class StatesLookup(_MultikeyWithVersionLookup):
    def _save_version(self, obj):
        self.handle_version_lookup[obj.DescriptorHandle] = obj.StateVersion

    def set_version(self, obj, increment=True):
        version = self.handle_version_lookup.get(obj.DescriptorHandle)
        if version is not None:
            if increment:
                version += 1
            obj.StateVersion = version

    def add_object_no_lock(self, obj):
        if obj.is_multi_state:
            raise ValueError('no MultiState!')
        super().add_object_no_lock(obj)


class MultiStatesLookup(_MultikeyWithVersionLookup):
    def _save_version(self, obj):
        self.handle_version_lookup[obj.Handle] = obj.StateVersion

    def set_version(self, obj, increment=True):
        version = self.handle_version_lookup.get(obj.Handle)
        if version is not None:
            if increment:
                version += 1
            obj.StateVersion = version


@dataclass
class Entity:
    descriptor: Type[AbstractDescriptorContainer]
    state: Type[AbstractStateContainer]


@dataclass
class MultiStateEntity:
    descriptor: Type[AbstractDescriptorContainer]
    states: List[Type[AbstractStateContainer]]


class MdibContainer:
    # these observables can be used to watch any change of data in the mdib. They contain lists of containers that were changed.
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

    def __init__(self, sdc_definitions: Type[BaseDefinitions]):
        """
        :param sdc_definitions: a class derived from Definitions_Base
        """
        self.sdc_definitions = sdc_definitions
        # self.data_model = _Model(sdc_definitions)
        self.data_model = sdc_definitions.data_model
        self._logger = None  # must be instantiated by derived class
        self.nsmapper = sdc_definitions.data_model.ns_helper
        self.mdib_version = 0
        self.sequence_id = ''  # needs to be set to a reasonable value by derived class
        self.instance_id = None  # None or an unsigned int
        self.log_prefix = ''

        self.descriptions = DescriptorsLookup()

        self.states = StatesLookup()
        self.states.add_index('descriptorHandle', multikey.UIndexDefinition(lambda obj: obj.DescriptorHandle))
        self.states.add_index('NODETYPE', multikey.IndexDefinition(lambda obj: obj.NODETYPE, index_none_values=False))

        self.context_states = MultiStatesLookup()

        # descriptorHandle index is NOT unique!
        # => multiple ContextStates refer to the same descriptor( history of locations)
        # 'handle' index can be unique, because we ignore None values
        self.context_states.add_index('descriptorHandle', multikey.IndexDefinition(lambda obj: obj.DescriptorHandle))
        self.context_states.add_index('handle',
                                      multikey.UIndexDefinition(lambda obj: obj.Handle, index_none_values=False))
        self.context_states.add_index('NODETYPE',
                                      multikey.IndexDefinition(lambda obj: obj.NODETYPE, index_none_values=False))
        self.mdib_lock = Lock()

        self.mdstate_version = 0
        self.mddescription_version = 0

    @property
    def logger(self):
        return self._logger

    @property
    def mdib_version_group(self):
        return MdibVersionGroup(self.mdib_version, self.sequence_id, self.instance_id)

    def add_description_containers(self, description_containers):
        """ init self.descriptions with provided descriptors
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
        """removes all states and context states. """
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

    def _set_descriptor_container_reference(self, state_container):
        """
        sets state_container.descriptor_container if all is fine, otherwise logs error.
        """
        descriptor_container = self.descriptions.handle.get_one(state_container.DescriptorHandle,
                                                                allow_none=True)
        if descriptor_container is None:
            self._logger.warn(
                'state "{}" (type={}) has no descriptor in mdib!',
                state_container.DescriptorHandle, state_container.NODETYPE)
        elif descriptor_container.DescriptorVersion == state_container.DescriptorVersion:
            state_container.descriptor_container = descriptor_container
        else:
            self._logger.warn(
                'state "{}" (type={}) : descriptor version expect "{}", found "{}"',
                state_container.DescriptorHandle, state_container.NODETYPE,
                descriptor_container.DescriptorVersion, state_container.DescriptorVersion)

    def add_state_containers(self, state_containers):
        """Adds states to self.states and self.context_states.
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
                    self._logger.error('add_state_containers: {}, Handle={}; {}',
                                       ex, state_container.Handle, traceback.format_exc())
                else:
                    self._logger.error('add_state_containers: {}, DescriptorHandle={}; {}',
                                       ex, state_container.DescriptorHandle, traceback.format_exc())

    def _reconstruct_md_description(self):
        """build dom tree from current data
        @return: an etree_ node
        """
        pm = self.data_model.pm_names
        doc_nsmap = self.nsmapper.ns_map
        root_containers = self.descriptions.parent_handle.get(None) or []
        md_description_node = etree_.Element(pm.MdDescription,
                                             attrib={'DescriptionVersion': str(self.mddescription_version)},
                                             nsmap=doc_nsmap)
        for root_container in root_containers:
            node = root_container.mk_descriptor_node(tag=pm.Mds,
                                                     nsmapper=self.nsmapper,
                                                     set_xsi_type=False,
                                                     connect_child_descriptors=True)
            md_description_node.append(node)
        return md_description_node

    def _reconstruct_mdib(self, add_context_states) -> etree_.Element:
        """build dom tree from current data
        :param add_context_states: bool
        @return: an etree_ node
        """
        pm = self.data_model.pm_names
        msg = self.data_model.msg_names
        doc_nsmap = self.nsmapper.ns_map
        mdib_node = etree_.Element(msg.Mdib, nsmap=doc_nsmap)
        mdib_node.set('MdibVersion', str(self.mdib_version))
        mdib_node.set('SequenceId', self.sequence_id)
        md_description_node = self._reconstruct_md_description()
        mdib_node.append(md_description_node)

        # add a list of states
        md_state_node = etree_.SubElement(mdib_node, pm.MdState,
                                          attrib={'StateVersion': str(self.mdstate_version)},
                                          nsmap=doc_nsmap)
        tag = pm.State
        for state_container in self.states.objects:
            md_state_node.append(state_container.mk_state_node(tag, self.nsmapper))
        if add_context_states:
            for state_container in self.context_states.objects:
                md_state_node.append(state_container.mk_state_node(tag, self.nsmapper))
        return mdib_node

    def reconstruct_md_description(self) -> (etree_.Element, MdibVersionGroup):
        """build dom tree from current data
        @return: a tuple etree_ node, mdib_version
        """
        with self.mdib_lock:
            node = self._reconstruct_md_description()
            return node, self.mdib_version_group

    def reconstruct_mdib(self) -> (etree_.Element, MdibVersionGroup):
        """build dom tree from current data
        This method does not include context states!
        @return: an etree_ node
        """
        with self.mdib_lock:
            return self._reconstruct_mdib(add_context_states=False), self.mdib_version_group

    def reconstruct_mdib_with_context_states(self) -> (etree_.Element, MdibVersionGroup):
        """ this method includes the context states in mdib tree.
        """
        with self.mdib_lock:
            return self._reconstruct_mdib(add_context_states=True), self.mdib_version_group

    def _get_child_descriptors_by_code(self, parent_handle, code):
        descriptors = self.descriptions.parent_handle.get(parent_handle, [])
        # descriptors = self.descriptions.coding.get(code, [])
        if len(descriptors) == 0:
            return []
        with_types = [d for d in descriptors if d.Type is not None]
        # return [d for d in with_types if d.Type.equals(code)]
        return [d for d in with_types if have_matching_codes(d.Type, code)]

    def get_metric_descriptor_by_code(self,
                                      vmd_code: [Coding, CodedValue],
                                      channel_code: [Coding, CodedValue],
                                      metric_code: [Coding, CodedValue]) -> [Type[AbstractMetricDescriptorContainer],
                                                                             None]:
        """ This is the "correct" way to find a descriptor.
        Using well known handles is shaky, because they have no meaning and can change over time!
        :param vmd_code: a CodedValue or a Coding instance
        :param channel_code: a CodedValue or a Coding instance
        :param metric_code: a CodedValue or a Coding instance
        """
        pm = self.data_model.pm_names
        aLL_vmds = self.descriptions.NODETYPE.get(pm.VmdDescriptor, [])
        matching_vmd_list = [d for d in aLL_vmds if have_matching_codes(d.Type, vmd_code)]
        for vmd in matching_vmd_list:
            matching_channels = self._get_child_descriptors_by_code(vmd.Handle, channel_code)
            for channel in matching_channels:
                matching_metrics = self._get_child_descriptors_by_code(channel.Handle, metric_code)
                if len(matching_metrics) == 1:
                    return matching_metrics[0]
                if len(matching_metrics) > 1:
                    raise ValueError(
                        f'found multiple metrics for vmd={vmd_code} channel={channel_code} metric={metric_code}')
        return None

    def get_operations_for_metric(self,
                                  vmd_code: [Coding, CodedValue],
                                  channel_code: [Coding, CodedValue],
                                  metric_code: [Coding, CodedValue]):
        """ This is the "correct" way to find an operation.
        Using well known handles is shaky, because they have no meaning and can change over time!
        :param vmd_code: a CodedValue or a Coding instance
        :param channel_code: a CodedValue or a Coding instance
        :param metric_code: a CodedValue or a Coding instance
        @return: a list of matching Operation Containers
        """
        descriptor_container = self.get_metric_descriptor_by_code(vmd_code, channel_code, metric_code)
        return self.get_operation_descriptors_for_descriptor_handle(descriptor_container.Handle)

    def get_operation_descriptors_for_descriptor_handle(self, descriptor_handle, **additional_filters):
        """
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

    def get_operation_descriptors(self):
        """
        :return: a list of all operation descriptors
        """
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

    def select_descriptors(self, *codings):
        """ Returns all descriptor containers that match a path defined by list of codings.
        example:
        [Coding('70041')] returns all containers that have Coding('70041') in its Type
        [Coding('70041'), Coding('69650')] : returns all descriptors with Coding('69650')
                                     and parent descriptor Coding('70041')
        [Coding('70041'), Coding('69650'), Coding('69651')] : returns all descriptors with Coding('69651') and
                                     parent descriptor Coding('69650') and parent's parent descriptor Coding('70041')
        It is not necessary that path starts at the top of a mds, it can start anywhere.
        :param codings: each element can be a string (which is handled as a Coding with DEFAULT_CODING_SYSTEM),
                         a Coding or a CodedValue
        """
        selected_objects = None
        for coding in codings:
            if selected_objects is None:
                selected_objects = self.descriptions.objects  # initially all objects
            else:
                # get all children of selected objects
                all_handles = [o.Handle for o in selected_objects]  # pylint: disable=not-an-iterable
                selected_objects = []
                for handle in all_handles:
                    selected_objects.extend(self.descriptions.parent_handle.get(handle, []))

            # normalize coding
            if isinstance(coding, str):
                coding = self.data_model.pm_types.Coding(coding)

            if coding is not None:
                # apply filter
                tmp_objects = [o for o in selected_objects if
                               o.Type is not None and have_matching_codes(o.Type, coding)]
                selected_objects = tmp_objects
        return selected_objects

    def get_all_descriptors_in_subtree(self, descriptor_container, depth_first=True, include_root=True):
        """ walks the tree below descriptor_container.
        :param descriptor_container:
        :param depth_first: determines order of returned list. DepthFirst=True has all leaves on top, otherwise at the end.
        :param include_root: if True descriptor_container itself is also part of returned list
        :return: a list of DescriptorContainer objects
        """
        result = []

        def _getchildren(parent):
            child_containers = self.descriptions.parent_handle.get(parent.Handle, [])
            if not depth_first:
                result.extend(child_containers)
            apply_map(_getchildren, child_containers)
            if depth_first:
                result.extend(child_containers)

        if include_root and not depth_first:
            result.append(descriptor_container)
        _getchildren(descriptor_container)
        if include_root and depth_first:
            result.append(descriptor_container)
        return result

    def rm_descriptors_and_states(self, descriptor_containers):
        """ recursive delete of a descriptor and all children and all related states"""
        deleted_descriptors = {}
        deleted_states = {}
        for descriptor_container in descriptor_containers:
            self._logger.debug('rm Descriptor node {} handle {}',
                               descriptor_container.NODETYPE, descriptor_container.Handle)
            self.descriptions.remove_object(descriptor_container)
            deleted_descriptors[descriptor_container.Handle] = descriptor_container
            for m_key in (self.states, self.context_states):
                state_containers = m_key.descriptorHandle.get(descriptor_container.Handle)
                if state_containers is not None:
                    # make a copy, otherwise remove_objects will manipulate same list in place
                    state_containers = state_containers[:]
                    self._logger.debug('rm {} states(s) associated to descriptor {} ',
                                       len(state_containers), descriptor_container.Handle)
                    m_key.remove_objects(state_containers)
                    deleted_states[descriptor_container.Handle] = state_containers

        if deleted_descriptors:
            self.deleted_descriptors_by_handle = deleted_descriptors
        if deleted_states:
            self.deleted_states_by_handle = deleted_states

    def rm_descriptor_by_handle(self, handle):
        """deletes descriptor and the subtree"""
        descriptor_container = self.descriptions.handle.get_one(handle, allow_none=True)
        if descriptor_container is not None:
            all_descriptors = self.get_all_descriptors_in_subtree(descriptor_container)
            self.rm_descriptors_and_states(all_descriptors)

    def get_entity(self, handle: str) -> Entity:
        descr = self.descriptions.handle.get_one(handle)
        state = self.states.descriptorHandle.get_one(handle)
        return Entity(descr, state)

    def get_context_entity(self, handle: str) -> MultiStateEntity:
        descr = self.descriptions.handle.get_one(handle)
        states = self.context_states.descriptorHandle.get(handle)
        return MultiStateEntity(descr, states)

    def has_multiple_mds(self):
        all_mds_descriptors = self.descriptions.NODETYPE.get(self.data_model.pm_names.MdsDescriptor)
        return len(all_mds_descriptors) > 1
