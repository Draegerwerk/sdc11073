from __future__ import annotations
import uuid
from threading import Lock
from collections import defaultdict
from contextlib import AbstractContextManager, contextmanager
from pathlib import Path
import time
from lxml.etree import Element, SubElement
from .xml_mdibbase import XmlMdibBase
from typing import TYPE_CHECKING, Callable, Any, Iterable
from sdc11073.mdib.transactionsprotocol import AnyTransactionManagerProtocol, TransactionType
from sdc11073.loghelper import LoggerAdapter
from sdc11073.observableproperties import ObservableProperty
from sdc11073 import loghelper
from sdc11073.definitions_base import ProtocolsRegistry
from sdc11073.pysoap.msgreader import MessageReader
from sdc11073.xml_types.pm_types import RetrievabilityMethod
from sdc11073.mdib.mdibbase import MdibVersionGroup
from sdc11073.etc import apply_map

from .xml_transactions import mk_transaction
from .xml_entities import ProviderInternalEntity, ProviderInternalMultiStateEntity
from .xml_entities import ProviderEntity, ProviderMultiStateEntity

if TYPE_CHECKING:
    from lxml.etree import QName
    from sdc11073.definitions_base import BaseDefinitions
    from sdc11073.location import SdcLocation
    from sdc11073.mdib.descriptorcontainers import AbstractDescriptorContainer
    from sdc11073.mdib.statecontainers import AbstractStateContainer, AbstractContextStateContainer
    from sdc11073.xml_types.pm_types import InstanceIdentifier
    from sdc11073.xml_types.pm_types import Coding, CodedValue
    from sdc11073 import xml_utils

    from sdc11073.mdib.transactionsprotocol import (
        ContextStateTransactionManagerProtocol,
        DescriptorTransactionManagerProtocol,
        StateTransactionManagerProtocol,
        TransactionResultProtocol
    )
    from sdc11073.mdib.entityprotocol import ProviderEntityGetterProtocol

TransactionFactory = Callable[[XmlMdibBase, TransactionType, LoggerAdapter],
AnyTransactionManagerProtocol]


class XmlProviderMdibMethods:

    def __init__(self, provider_mdib: XmlProviderMdib):
        self._mdib = provider_mdib

    def set_all_source_mds(self):
        dict_by_parent_handle = defaultdict(list)
        descriptor_containers = [entity.descriptor for entity in self._mdib._entities.values()]
        for d in descriptor_containers:
            dict_by_parent_handle[d.parent_handle].append(d)

        def tag_tree(source_mds_handle, descriptor_container):
            descriptor_container.set_source_mds(source_mds_handle)
            children = dict_by_parent_handle[descriptor_container.Handle]
            for ch in children:
                tag_tree(source_mds_handle, ch)

        for mds in dict_by_parent_handle[None]:
            tag_tree(mds.Handle, mds)


    def set_location(self, sdc_location: SdcLocation,
                     validators: list[InstanceIdentifier] | None = None,
                     location_context_descriptor_handle: str | None = None):
        pass

    def mk_state_containers_for_all_descriptors(self):
        """Create a state container for every descriptor that is missing a state in mdib.

        The model requires that there is a state for every descriptor (exception: multi-states)
        """
        mdib = self._mdib
        pm = mdib.data_model.pm_names
        for entity in mdib._entities.values():
            if entity.descriptor.is_context_descriptor:
                continue
            if entity.state is None:
                state_cls = mdib.data_model.get_state_class_for_descriptor(entity.descriptor)
                state = state_cls(entity.descriptor)
                entity.state = state
                # add some initial values where needed
                if state.is_alert_condition:
                    state.DeterminationTime = time.time()
                elif state.NODETYPE == pm.AlertSystemState:  # noqa: SIM300
                    state.LastSelfCheck = time.time()
                    state.SelfCheckCount = 1
                elif state.NODETYPE == pm.ClockState:  # noqa: SIM300
                    state.LastSet = time.time()
                if mdib.current_transaction is not None:
                    mdib.current_transaction.add_state(state)


    def update_retrievability_lists(self):
        """Update internal lists, based on current mdib descriptors."""
        mdib = self._mdib
        with mdib.mdib_lock:
            del mdib._retrievability_episodic[:]  # noqa: SLF001
            mdib.retrievability_periodic.clear()
            for entity in mdib._entities.values():
                for r in entity.descriptor.get_retrievability():
                    for r_by in r.By:
                        if r_by.Method == RetrievabilityMethod.EPISODIC:
                            mdib._retrievability_episodic.append(entity.descriptor.Handle)  # noqa: SLF001
                        elif r_by.Method == RetrievabilityMethod.PERIODIC:
                            period_float = r_by.UpdatePeriod
                            period_ms = int(period_float * 1000.0)
                            mdib.retrievability_periodic[period_ms].append(entity.descriptor.Handle)

    def get_all_entities_in_subtree(self, root_entity: ProviderEntity | ProviderMultiStateEntity,
                                    depth_first: bool = True,
                                    include_root: bool = True
                                    ) -> list[ProviderEntity | ProviderMultiStateEntity] :
        """Return the tree below descriptor_container as a flat list."""
        result = []

        def _getchildren(parent: AbstractDescriptorContainer):
            child_containers = [e for e in self._mdib._entities.values() if e.parent_handle == parent.handle]
            if not depth_first:
                result.extend(child_containers)
            apply_map(_getchildren, child_containers)
            if depth_first:
                result.extend(child_containers)

        if include_root and not depth_first:
            result.append(root_entity)
        _getchildren(root_entity)
        if include_root and depth_first:
            result.append(root_entity)
        return result


class ProviderEntityGetter:
    """Implements entityprotocol.ProviderEntityGetterProtocol"""
    def __init__(self,
                 mdib: XmlProviderMdib):
        self._entities: dict[str, ProviderInternalEntity | ProviderInternalMultiStateEntity] = mdib._entities
        self._new_entities: dict[str, ProviderInternalEntity | ProviderInternalMultiStateEntity] = mdib._new_entities
        self._mdib = mdib

    def handle(self, handle: str) ->  ProviderEntity | ProviderMultiStateEntity | None:
        """Return entity with given handle."""
        try:
            internal_entity = self._entities[handle]
            return internal_entity.mk_entity()
        except KeyError:
            return None

    def context_handle(self, handle: str) -> ProviderMultiStateEntity:
        """Return multi state entity that contains a state with given handle."""
        ...

    def node_type(self, node_type: QName) -> list[ProviderEntity | ProviderMultiStateEntity]:
        """Return all entities with given node type."""
        ret = []
        for handle, internal_entity in self._entities.items():
            if internal_entity.descriptor.NODETYPE == node_type:
                ret.append(internal_entity.mk_entity())
        return ret

    def parent_handle(self, parent_handle: str | None) -> list[ProviderEntity | ProviderMultiStateEntity]:
        """Return all entities with given parent handle."""
        ret = []
        for handle, internal_entity in self._entities.items():
            if internal_entity.descriptor.parent_handle == parent_handle:
                ret.append(internal_entity.mk_entity())
        return ret

    def coding(self, coding: Coding) -> list[ProviderEntity | ProviderMultiStateEntity]:
        """Return all entities with given Coding."""
        ret = []
        for handle, internal_entity in self._entities.items():
            if internal_entity.descriptor.Type.is_equivalent(coding):
                ret.append(internal_entity.mk_entity())
        return ret

    def coded_value(self, coded_value: CodedValue) -> list[ProviderEntity | ProviderMultiStateEntity]:
        """Return all entities with given Coding."""
        ret = []
        for handle, internal_entity in self._entities.items():
            if internal_entity.descriptor.Type.is_equivalent(coded_value):
                ret.append(internal_entity.mk_entity())
        return ret

    def items(self) -> Iterable[tuple[str, [ProviderEntity | ProviderMultiStateEntity]]]:
        """Like items() of a dictionary."""
        for handle, internal_entity in self._entities.items():
            yield handle, internal_entity.mk_entity()

    def new_entity(self,
                   node_type: QName,
                   handle: str,
                   parent_handle: str) -> ProviderEntity | ProviderMultiStateEntity:
        """Create an entity.

        User can modify the entity and then add it to transaction via handle_entity!
        It will not become part of mdib without handle_entity call!"""
        if (handle in self._entities
                or handle in self._mdib._context_state_handles
                or handle in self._new_entities
        ):
            raise ValueError('Handle already exists')

        # Todo: check if this node type is a valid child of parent

        descr_cls = self._mdib.data_model.get_descriptor_container_class(node_type)
        descriptor_container = descr_cls(handle=handle, parent_handle=parent_handle)
        parent_entity = self._entities[parent_handle]
        descriptor_container.set_source_mds(parent_entity.descriptor.source_mds)

        new_internal_entity = self._mdib._mk_internal_entity(descriptor_container, [])

        if not new_internal_entity.is_multi_state:
            # create a state
            state_cls = self._mdib.data_model.get_state_container_class(descriptor_container.STATE_QNAME)
            new_internal_entity.state = state_cls(descriptor_container)

        self._new_entities[descriptor_container.Handle] = new_internal_entity # write to mdib in process_transaction
        return new_internal_entity.mk_entity()


    def new_state(self,
            entity: ProviderMultiStateEntity,
            handle: str | None = None,
            ) -> AbstractContextStateContainer:

        if handle is None:
            handle = uuid.uuid4().hex
        elif handle in entity.states:
            raise ValueError(f'State with handle {handle} already exists')

        state_cls = self._mdib.data_model.get_state_container_class(entity.descriptor.STATE_QNAME)
        state = state_cls(entity.descriptor)
        state.Handle = handle
        # entity.states[handle] = state
        return state

    def __len__(self) -> int:
        """Return number of entities"""
        return len(self._entities)


class XmlProviderMdib:
    """Device side implementation of a mdib.

    Do not modify containers directly, use transactions for that purpose.
    Transactions keep track of changes and initiate sending of update notifications to clients.
    """

    transaction: TransactionResultProtocol | None = ObservableProperty(fire_only_on_changed_value=False)
    rt_updates = ObservableProperty(fire_only_on_changed_value=False)  # different observable for performance

    # ToDo: keep track of DescriptorVersions and StateVersion in order to allow correct StateVersion after delete/create
    # new version must be bigger then old version
    def __init__(self,
                 sdc_definitions: type[BaseDefinitions] | None = None,
                 log_prefix: str | None = None,
                 extra_functionality: type | None = None,
                 transaction_factory: TransactionFactory | None = None,
                 ):
        """Construct a ProviderMdib.

        :param sdc_definitions: defaults to sdc11073.definitions_sdc.SdcV1Definitions
        :param log_prefix: a string
        :param extra_functionality: class for extra functionality, default is ProviderMdibMethods
        :param transaction_factory: optional alternative transactions factory.
        """
        if sdc_definitions is None:
            from sdc11073.definitions_sdc import SdcV1Definitions  # lazy import, needed to brake cyclic imports
            sdc_definitions = SdcV1Definitions

        self.sdc_definitions = sdc_definitions
        self.data_model = sdc_definitions.data_model
        self.nsmapper = sdc_definitions.data_model.ns_helper

        self._logger = loghelper.get_logger_adapter('sdc.device.mdib', log_prefix)
        self.mdib_version = 0
        self.sequence_id = ''  # needs to be set to a reasonable value by derived class
        self.instance_id = None  # None or an unsigned int
        self.log_prefix = ''
        self.mdib_lock = Lock()

        if extra_functionality is None:
            extra_functionality = XmlProviderMdibMethods

        self._entities: dict[str, ProviderInternalEntity | ProviderInternalMultiStateEntity] = {}  # key is the handle

        # Keep track of entities that were created but are not yet part of mdib.
        # They become part of mdib when they were added via transaction.
        self._new_entities: dict[str, ProviderInternalEntity | ProviderInternalMultiStateEntity] = {}

        # The official API
        self.entities: ProviderEntityGetterProtocol = ProviderEntityGetter(self)

        # context state handles must be known in order to
        # - efficiently return an entity that has a context state with a specific handle
        # - check if a handle already exists im mdib
        self._context_state_handles: dict[str, ProviderInternalMultiStateEntity] = {}
        self._xtra = extra_functionality(self)
        self._tr_lock = Lock()  # transaction lock

        self.sequence_id = uuid.uuid4().urn  # this uuid identifies this mdib instance

        self._annotators = {}
        self.current_transaction = None

        self.pre_commit_handler = None  # pre_commit_handler can modify transaction if needed before it is committed
        self.post_commit_handler = None  # post_commit_handler can modify mdib if needed after it is committed
        self._transaction_factory = transaction_factory or mk_transaction
        self._retrievability_episodic = []  # a list of handles
        self.retrievability_periodic = defaultdict(list)
        self.mddescription_version = 0
        self.mdstate_version = 0
        self._is_initialized = False
        # In order to be able to re-create a descriptor or state with a bigger version than before,
        # these lookups keep track of version counters for deleted descriptors and states.
        self.descr_handle_version_lookup: dict[str, int] = {}
        self.state_handle_version_lookup: dict[str, int] = {}

    @property
    def xtra(self) -> Any:
        """Give access to extended functionality."""
        return self._xtra

    @property
    def mdib_version_group(self) -> MdibVersionGroup:
        """"Get current version data."""
        return MdibVersionGroup(self.mdib_version, self.sequence_id, self.instance_id)

    @property
    def logger(self) -> LoggerAdapter:
        """Return the logger."""
        return self._logger

    def set_initialized(self):
        self._is_initialized = True

    @contextmanager
    def _transaction_manager(self,
                             transaction_type: TransactionType,
                             set_determination_time: bool = True) -> AbstractContextManager[
        AnyTransactionManagerProtocol]:
        """Start a transaction, return a new transaction manager."""
        with self._tr_lock, self.mdib_lock:
            try:
                self.current_transaction = self._transaction_factory(self, transaction_type, self.logger)
                yield self.current_transaction

                if callable(self.pre_commit_handler):
                    self.pre_commit_handler(self, self.current_transaction)
                if self.current_transaction.error:
                    self._logger.info('transaction_manager: transaction without updates!')
                else:
                    # update observables
                    transaction_result = self.current_transaction.process_transaction(set_determination_time)
                    self.transaction = transaction_result

                    if transaction_result.alert_updates:
                        self.alert_by_handle = {st.DescriptorHandle: st for st in transaction_result.alert_updates}
                    if transaction_result.comp_updates:
                        self.component_by_handle = {st.DescriptorHandle: st for st in transaction_result.comp_updates}
                    if transaction_result.ctxt_updates:
                        self.context_by_handle = {st.Handle: st for st in transaction_result.ctxt_updates}
                    if transaction_result.descr_created:
                        self.new_descriptors_by_handle = {descr.Handle: descr for descr
                                                          in transaction_result.descr_created}
                    if transaction_result.descr_deleted:
                        self.deleted_descriptors_by_handle = {descr.Handle: descr for descr
                                                              in transaction_result.descr_deleted}
                    if transaction_result.descr_updated:
                        self.updated_descriptors_by_handle = {descr.Handle: descr for descr
                                                              in transaction_result.descr_updated}
                    if transaction_result.metric_updates:
                        self.metrics_by_handle = {st.DescriptorHandle: st for st in transaction_result.metric_updates}
                    if transaction_result.op_updates:
                        self.operation_by_handle = {st.DescriptorHandle: st for st in transaction_result.op_updates}
                    if transaction_result.rt_updates:
                        self.waveform_by_handle = {st.DescriptorHandle: st for st in transaction_result.rt_updates}


                    if callable(self.post_commit_handler):
                        self.post_commit_handler(self, self.current_transaction)
            finally:
                self.current_transaction = None

    @contextmanager
    def context_state_transaction(self) -> AbstractContextManager[ContextStateTransactionManagerProtocol]:
        """Return a transaction for context state updates."""
        with self._transaction_manager(TransactionType.context) as mgr:
            yield mgr

    @contextmanager
    def alert_state_transaction(self, set_determination_time: bool = True) \
            -> AbstractContextManager[StateTransactionManagerProtocol]:
        """Return a transaction for alert state updates."""
        with self._transaction_manager(TransactionType.alert, set_determination_time) as mgr:
            yield mgr

    @contextmanager
    def metric_state_transaction(self, set_determination_time: bool = True) \
            -> AbstractContextManager[StateTransactionManagerProtocol]:
        """Return a transaction for metric state updates (not real time samples!)."""
        with self._transaction_manager(TransactionType.metric, set_determination_time) as mgr:
            yield mgr

    @contextmanager
    def rt_sample_state_transaction(self, set_determination_time: bool = False) \
            -> AbstractContextManager[StateTransactionManagerProtocol]:
        """Return a transaction for real time sample state updates."""
        with self._transaction_manager(TransactionType.rt_sample, set_determination_time) as mgr:
            yield mgr

    @contextmanager
    def component_state_transaction(self) -> AbstractContextManager[StateTransactionManagerProtocol]:
        """Return a transaction for component state updates."""
        with self._transaction_manager(TransactionType.component) as mgr:
            yield mgr

    @contextmanager
    def operational_state_transaction(self) -> AbstractContextManager[StateTransactionManagerProtocol]:
        """Return a transaction for operational state updates."""
        with self._transaction_manager(TransactionType.operational) as mgr:
            yield mgr

    @contextmanager
    def descriptor_transaction(self) -> AbstractContextManager[DescriptorTransactionManagerProtocol]:
        """Return a transaction for descriptor updates.

        This transaction also allows to handle the states that relate to the modified descriptors.
        """
        with self._transaction_manager(TransactionType.descriptor) as mgr:
            yield mgr


    def make_descriptor_node(self,
                             descriptor_container: AbstractDescriptorContainer,
                             parent_node: xml_utils.LxmlElement,
                             tag: QName,
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
        node = SubElement(parent_node,
                          tag,
                          attrib={'Handle': descriptor_container.Handle},
                          nsmap=ns_map)
        descriptor_container.update_node(node, self.nsmapper, set_xsi_type)  # create all
        child_entities = self.entities.parent_handle(descriptor_container.Handle)
        # append all child containers, then bring all child elements in correct order
        for child_entity in child_entities:
            child_tag, set_xsi = descriptor_container.tag_name_for_child_descriptor(child_entity.descriptor.NODETYPE)
            self.make_descriptor_node(child_entity.descriptor, node, child_tag, set_xsi)
        descriptor_container.sort_child_nodes(node)
        return node


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

    def reconstruct_md_description(self) -> (xml_utils.LxmlElement, MdibVersionGroup):
        """Build dom tree of descriptors from current data."""
        with self.mdib_lock:
            node = self._reconstruct_md_description()
            return node, self.mdib_version_group

    @staticmethod
    def _mk_internal_entity(descriptor_container: AbstractDescriptorContainer,
                   all_states: list[AbstractStateContainer]) -> ProviderInternalEntity | ProviderInternalMultiStateEntity:
        states = [s for s in all_states if s.DescriptorHandle == descriptor_container.Handle]
        if descriptor_container.is_context_descriptor:
            return ProviderInternalMultiStateEntity(descriptor_container, states)
        if len(states) == 1:
            return ProviderInternalEntity(descriptor_container, states[0])
        if len(states) == 0:
            return ProviderInternalEntity(descriptor_container, None)
        raise ValueError(
            f'found {len(states)} states for {descriptor_container.NODETYPE} handle = {descriptor_container.Handle}')

    def add_internal_entity(self, descriptor_container: AbstractDescriptorContainer,
                   all_states: list[AbstractStateContainer])-> ProviderInternalEntity | ProviderInternalMultiStateEntity:
        """Create new entity and add it to self._entities.

        This method can't be used after mdib is initialized. Adding entities can the only be done via transaction.
        """
        if self._is_initialized:
            raise ValueError('add_entity call not allowed, use a treansaction!' )
        entity =  self._mk_internal_entity(descriptor_container, all_states)
        self._entities[descriptor_container.Handle] = entity
        return entity

    def _reconstruct_mdib(self, add_context_states: bool) -> xml_utils.LxmlElement:
        """Build dom tree of mdib from current data.

        If add_context_states is False, context states are not included.
        """
        pm = self.data_model.pm_names
        msg = self.data_model.msg_names
        doc_nsmap = self.nsmapper.ns_map
        mdib_node = Element(msg.Mdib, nsmap=doc_nsmap)
        mdib_node.set('MdibVersion', str(self.mdib_version))
        mdib_node.set('SequenceId', self.sequence_id)
        if self.instance_id is not None:
            mdib_node.set('InstanceId', str(self.instance_id))
        md_description_node = self._reconstruct_md_description()
        mdib_node.append(md_description_node)

        # add a list of states
        md_state_node = SubElement(mdib_node, pm.MdState,
                                          attrib={'StateVersion': str(self.mdstate_version)},
                                          nsmap=doc_nsmap)
        tag = pm.State
        for entity in self._entities.values():
            if entity.descriptor.is_context_descriptor:
                if add_context_states:
                    for state_container in entity.states.values():
                        md_state_node.append(state_container.mk_state_node(tag, self.nsmapper))
            elif entity.state is not None:
                md_state_node.append(entity.state.mk_state_node(tag, self.nsmapper))
        return mdib_node

    def _reconstruct_md_description(self) -> xml_utils.LxmlElement:
        """Build dom tree of descriptors from current data."""
        pm = self.data_model.pm_names
        doc_nsmap = self.nsmapper.ns_map
        # root_containers = self.descriptions.parent_handle.get(None) or []
        root_entities = self.entities.parent_handle(None)
        if root_entities:
            md_description_node = Element(pm.MdDescription,
                                                 attrib={'DescriptionVersion': str(self.mddescription_version)},
                                                 nsmap=doc_nsmap)
            for root_entity in root_entities:
                self.make_descriptor_node(root_entity.descriptor, md_description_node, tag=pm.Mds, set_xsi_type=False)
        return md_description_node

    @classmethod
    def from_mdib_file(cls,
                       path: str,
                       protocol_definition: type[BaseDefinitions] | None = None,
                       xml_reader_class: type[MessageReader] | None = MessageReader,
                       log_prefix: str | None = None) -> XmlProviderMdib:
        """Construct mdib from a file.

        :param path: the input file path for creating the mdib
        :param protocol_definition: an optional object derived from BaseDefinitions, forces usage of this definition
        :param xml_reader_class: class that is used to read mdib xml file
        :param log_prefix: a string or None
        :return: instance.
        """
        with Path(path).open('rb') as the_file:
            xml_text = the_file.read()
        return cls.from_string(xml_text,
                               protocol_definition,
                               xml_reader_class,
                               log_prefix)

    @classmethod
    def from_string(cls,
                    xml_text: bytes,
                    protocol_definition: type[BaseDefinitions] | None = None,
                    xml_reader_class: type[MessageReader] | None = MessageReader,
                    log_prefix: str | None = None) -> XmlProviderMdib:
        """Construct mdib from a string.

        :param xml_text: the input string for creating the mdib
        :param protocol_definition: an optional object derived from BaseDefinitions, forces usage of this definition
        :param xml_reader_class: class that is used to read mdib xml file
        :param log_prefix: a string or None
        :return: instance.
        """
        # get protocol definition that matches xml_text
        if protocol_definition is None:
            for definition_cls in ProtocolsRegistry.protocols:
                pm_namespace = definition_cls.data_model.ns_helper.PM.namespace.encode('utf-8')
                if pm_namespace in xml_text:
                    protocol_definition = definition_cls
                    break
        if protocol_definition is None:
            raise ValueError('cannot create instance, no known BICEPS schema version identified')
        mdib = cls(protocol_definition, log_prefix=log_prefix)

        xml_msg_reader = xml_reader_class(protocol_definition, None, mdib.logger)
        descriptor_containers, state_containers = xml_msg_reader.read_mdib_xml(xml_text)
        #Todo: msg_reader sets source_mds while reading xml mdib

        for d in descriptor_containers:
            mdib.add_internal_entity(d, state_containers)

        mdib.xtra.set_all_source_mds()

        mdib.xtra.mk_state_containers_for_all_descriptors()
        mdib.xtra.update_retrievability_lists()
        mdib.set_initialized()

        return mdib

