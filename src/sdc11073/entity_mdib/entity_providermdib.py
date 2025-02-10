"""The module contains a provider mdib implementation that uses entities in internal representation."""
from __future__ import annotations

import uuid
from collections import defaultdict
from contextlib import AbstractContextManager, contextmanager
from pathlib import Path
from threading import Lock
from typing import TYPE_CHECKING, Any, Callable, Protocol

from lxml.etree import Element, SubElement

from sdc11073 import loghelper
from sdc11073.definitions_base import ProtocolsRegistry
from sdc11073.mdib.transactionsprotocol import TransactionType
from sdc11073.observableproperties import ObservableProperty
from sdc11073.pysoap.msgreader import MessageReader

from .entities import ProviderInternalEntity, ProviderInternalEntityType, ProviderInternalMultiStateEntity
from .entity_mdibbase import EntityMdibBase
from .entity_providermdibxtra import EntityProviderMdibMethods
from .entity_transactions import mk_transaction

if TYPE_CHECKING:
    from collections.abc import Iterable

    from lxml.etree import QName

    from sdc11073 import xml_utils
    from sdc11073.definitions_base import BaseDefinitions
    from sdc11073.loghelper import LoggerAdapter
    from sdc11073.mdib.descriptorcontainers import AbstractDescriptorContainer
    from sdc11073.mdib.entityprotocol import ProviderEntityGetterProtocol
    from sdc11073.mdib.mdibbase import MdibVersionGroup
    from sdc11073.mdib.statecontainers import AbstractStateContainer
    from sdc11073.mdib.transactionsprotocol import (
        AnyEntityTransactionManagerProtocol,
        EntityContextStateTransactionManagerProtocol,
        EntityDescriptorTransactionManagerProtocol,
        EntityStateTransactionManagerProtocol,
        TransactionResultProtocol,
    )
    from sdc11073.xml_types.pm_types import CodedValue, Coding

    from .entities import ProviderEntityType, ProviderMultiStateEntity

    ProviderEntityFactory = Callable[[AbstractDescriptorContainer, list[AbstractStateContainer]],
    ProviderInternalEntityType]


def _mk_internal_entity(descriptor_container: AbstractDescriptorContainer,
                        states: list[AbstractStateContainer]) -> ProviderInternalEntityType:
    """Create an entity.

    This is the default Implementation of ProviderEntityFactory.
    """
    for s in states:
        s.descriptor_container = descriptor_container
    if descriptor_container.is_context_descriptor:
        return ProviderInternalMultiStateEntity(descriptor_container, states)
    if len(states) == 1:
        return ProviderInternalEntity(descriptor_container, states[0])
    if len(states) == 0:
        return ProviderInternalEntity(descriptor_container, None)
    msg = f'found {len(states)} states for {descriptor_container.NODETYPE} handle = {descriptor_container.Handle}'
    raise ValueError(msg)


class ProviderEntityGetter:
    """Implements entityprotocol.ProviderEntityGetterProtocol."""

    def __init__(self,
                 mdib: EntityProviderMdib):
        self._mdib = mdib

    def by_handle(self, handle: str) -> ProviderEntityType | None:
        """Return entity with given handle."""
        with self._mdib.mdib_lock:
            try:
                internal_entity = self._mdib.internal_entities[handle]
                return internal_entity.mk_entity(self._mdib)
            except KeyError:
                return None

    def by_context_handle(self, handle: str) -> ProviderMultiStateEntity | None:
        """Return multi state entity that contains a state with given handle."""
        with self._mdib.mdib_lock:
            for internal_entity in self._mdib.internal_entities.values():
                if internal_entity.is_multi_state and handle in internal_entity.states:
                    return internal_entity.mk_entity(self._mdib)
        return None

    def by_node_type(self, node_type: QName) -> list[ProviderEntityType]:
        """Return all entities with given node type."""
        ret = []
        with self._mdib.mdib_lock:
            for internal_entity in self._mdib.internal_entities.values():
                if node_type == internal_entity.descriptor.NODETYPE:
                    ret.append(internal_entity.mk_entity(self._mdib)) # noqa: PERF401
        return ret

    def by_parent_handle(self, parent_handle: str | None) -> list[ProviderEntityType]:
        """Return all entities with given parent handle."""
        ret = []
        with self._mdib.mdib_lock:
            for internal_entity in self._mdib.internal_entities.values():
                if internal_entity.descriptor.parent_handle == parent_handle:
                    ret.append(internal_entity.mk_entity(self._mdib))   # noqa: PERF401
        return ret

    def by_coding(self, coding: Coding) -> list[ProviderEntityType]:
        """Return all entities with given Coding."""
        ret = []
        with (self._mdib.mdib_lock):
            for internal_entity in self._mdib.internal_entities.values():
                if internal_entity.descriptor.Type is not None \
                    and internal_entity.descriptor.Type.is_equivalent(coding):
                    ret.append(internal_entity.mk_entity(self._mdib))  # noqa: PERF401
        return ret

    def by_coded_value(self, coded_value: CodedValue) -> list[ProviderEntityType]:
        """Return all entities with given Coding."""
        ret = []
        with self._mdib.mdib_lock:
            for internal_entity in self._mdib.internal_entities.values():
                if internal_entity.descriptor.Type is not None and internal_entity.descriptor.Type.is_equivalent(
                        coded_value):
                    ret.append(internal_entity.mk_entity(self._mdib))  # noqa: PERF401
        return ret

    def items(self) -> Iterable[tuple[str, ProviderEntityType]]:
        """Return the items."""
        with self._mdib.mdib_lock:
            for handle, internal_entity in self._mdib.internal_entities.items():
                yield handle, internal_entity.mk_entity(self._mdib)

    def new_entity(self,
                   node_type: QName,
                   handle: str,
                   parent_handle: str | None) -> ProviderEntityType:
        """Create an entity.

        User can modify the entity and then add it to transaction via write_entity!
        It will not become part of mdib without write_entity call!
        """
        if handle in self._mdib.internal_entities or handle in self._mdib.new_entities:
            raise ValueError('Handle already exists')

        descr_cls = self._mdib.data_model.get_descriptor_container_class(node_type)
        descriptor_container = descr_cls(handle=handle, parent_handle=parent_handle)
        if parent_handle is not None:
            parent_entity = (self._mdib.new_entities.get(parent_handle)
                             or self._mdib.internal_entities.get(parent_handle))
            if parent_entity is None: # pragma: no cover
                msg = f'Entity {handle} has no parent (parent_handle = {parent_handle})!'
                raise ValueError(msg)
            descriptor_container.set_source_mds(parent_entity.descriptor.source_mds)
        else:
            descriptor_container.set_source_mds(descriptor_container.Handle)  # this is a mds, source_mds is itself

        new_internal_entity = self._mdib.entity_factory(descriptor_container, [])
        if handle in self._mdib.descr_handle_version_lookup:
            # This handle existed before. Use last descriptor version + 1
            new_internal_entity.descriptor.DescriptorVersion = self._mdib.descr_handle_version_lookup[handle] + 1
        if not new_internal_entity.is_multi_state:
            # create a state
            state_cls = self._mdib.data_model.get_state_container_class(descriptor_container.STATE_QNAME)
            new_internal_entity.state = state_cls(descriptor_container)
            if handle in self._mdib.state_handle_version_lookup:
                new_internal_entity.state.StateVersion = self._mdib.state_handle_version_lookup[handle] + 1
        # write to mdib in process_transaction
        self._mdib.new_entities[descriptor_container.Handle] = new_internal_entity
        return new_internal_entity.mk_entity(self._mdib)

    def __len__(self) -> int:
        """Return number of entities."""
        return len(self._mdib.internal_entities)


class ProviderXtraProtocol(Protocol):
    """Functionality expected by EntityProviderMdib."""

    def set_initial_content(self,
                            descriptor_containers: list[AbstractDescriptorContainer],
                            state_containers: list[AbstractStateContainer]):
        """Populate mdib."""
        ...


class EntityProviderMdib(EntityMdibBase):
    """Device side implementation of a mdib.

    Do not modify containers directly, use transactions for that purpose.
    Transactions keep track of changes and initiate sending of update notifications to clients.
    """

    transaction: TransactionResultProtocol | None = ObservableProperty(fire_only_on_changed_value=False)
    rt_updates = ObservableProperty(fire_only_on_changed_value=False)  # different observable for performance

    def __init__(self,
                 sdc_definitions: type[BaseDefinitions] | None = None,
                 log_prefix: str | None = None,
                 extra_functionality: Callable[[EntityProviderMdib], ProviderXtraProtocol] | None = None,
                 transaction_factory: Callable[[EntityProviderMdib, TransactionType, LoggerAdapter],
                                                AnyEntityTransactionManagerProtocol] | None = None,
                 ):
        """Construct a ProviderMdib.

        :param sdc_definitions: defaults to sdc11073.definitions_sdc.SdcV1Definitions
        :param log_prefix: a string
        :param extra_functionality: class for extra functionality, default is ProviderMdibMethods
        :param transaction_factory: optional alternative transactions factory.
        """
        if sdc_definitions is None:  # pragma: no cover
            from sdc11073.definitions_sdc import SdcV1Definitions  # lazy import, needed to brake cyclic imports
            sdc_definitions = SdcV1Definitions
        super().__init__(sdc_definitions,
                         loghelper.get_logger_adapter('sdc.device.mdib', log_prefix),
                         )

        self.nsmapper = sdc_definitions.data_model.ns_helper

        if extra_functionality is None:
            extra_functionality = EntityProviderMdibMethods

        self._entities: dict[str, ProviderInternalEntityType] = {}  # key is the handle

        # Keep track of entities that were created but are not yet part of mdib.
        # They become part of mdib when they are added via transaction.
        self._new_entities: dict[str, ProviderInternalEntityType] = {}

        # The official API
        self.entities: ProviderEntityGetterProtocol = ProviderEntityGetter(self)

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
        self.entity_factory: ProviderEntityFactory = _mk_internal_entity

    @property
    def xtra(self) -> Any:
        """Give access to extended functionality."""
        return self._xtra

    @property
    def internal_entities(self) -> dict[str, ProviderInternalEntityType]:
        """The property is needed by transactions. Do not use it otherwise."""
        return self._entities

    @property
    def new_entities(self) -> dict[str, ProviderInternalEntityType]:
        """The property is needed by transactions. Do not use it otherwise."""
        return self._new_entities

    def set_initialized(self):
        """Set initialized state = True."""
        self._is_initialized = True

    @property
    def is_initialized(self) -> bool:
        """Return True if mdib is already initialized."""
        return self._is_initialized

    @contextmanager
    def _transaction_manager(self, # noqa: PLR0912, C901
                             transaction_type: TransactionType,
                             set_determination_time: bool = True) -> AbstractContextManager[
        AnyEntityTransactionManagerProtocol]:
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
                    if transaction_result.new_mdib_version is not None:
                        self.mdib_version = transaction_result.new_mdib_version
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
    def context_state_transaction(self) -> AbstractContextManager[EntityContextStateTransactionManagerProtocol]:
        """Return a transaction for context state updates."""
        with self._transaction_manager(TransactionType.context, False) as mgr:
            yield mgr

    @contextmanager
    def alert_state_transaction(self, set_determination_time: bool = True) \
            -> AbstractContextManager[EntityStateTransactionManagerProtocol]:
        """Return a transaction for alert state updates."""
        with self._transaction_manager(TransactionType.alert, set_determination_time) as mgr:
            yield mgr

    @contextmanager
    def metric_state_transaction(self, set_determination_time: bool = True) \
            -> AbstractContextManager[EntityStateTransactionManagerProtocol]:
        """Return a transaction for metric state updates (not real time samples!)."""
        with self._transaction_manager(TransactionType.metric, set_determination_time) as mgr:
            yield mgr

    @contextmanager
    def rt_sample_state_transaction(self, set_determination_time: bool = False) \
            -> AbstractContextManager[EntityStateTransactionManagerProtocol]:
        """Return a transaction for real time sample state updates."""
        with self._transaction_manager(TransactionType.rt_sample, set_determination_time) as mgr:
            yield mgr

    @contextmanager
    def component_state_transaction(self) -> AbstractContextManager[EntityStateTransactionManagerProtocol]:
        """Return a transaction for component state updates."""
        with self._transaction_manager(TransactionType.component) as mgr:
            yield mgr

    @contextmanager
    def operational_state_transaction(self) -> AbstractContextManager[EntityStateTransactionManagerProtocol]:
        """Return a transaction for operational state updates."""
        with self._transaction_manager(TransactionType.operational) as mgr:
            yield mgr

    @contextmanager
    def descriptor_transaction(self) -> AbstractContextManager[EntityDescriptorTransactionManagerProtocol]:
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
        child_entities = self.entities.by_parent_handle(descriptor_container.Handle)
        # append all child containers, then bring all child elements in correct order
        for child_entity in child_entities:
            child_tag, set_xsi = descriptor_container.tag_name_for_child_descriptor(child_entity.descriptor.NODETYPE)
            self.make_descriptor_node(child_entity.descriptor, node, child_tag, set_xsi)
        descriptor_container.sort_child_nodes(node)
        return node

    def reconstruct_mdib(self) -> tuple[xml_utils.LxmlElement, MdibVersionGroup]:
        """Build dom tree from current data.

        This method does not include context states!
        """
        with self.mdib_lock:
            return self._reconstruct_mdib(add_context_states=False), self.mdib_version_group

    def reconstruct_mdib_with_context_states(self) -> tuple[xml_utils.LxmlElement, MdibVersionGroup]:
        """Build dom tree from current data.

        This method includes the context states.
        """
        with self.mdib_lock:
            return self._reconstruct_mdib(add_context_states=True), self.mdib_version_group

    def reconstruct_md_description(self) -> tuple[xml_utils.LxmlElement, MdibVersionGroup]:
        """Build dom tree of descriptors from current data."""
        with self.mdib_lock:
            node = self._reconstruct_md_description()
            return node, self.mdib_version_group

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
        root_entities = self.entities.by_parent_handle(None)
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
                       log_prefix: str | None = None) -> EntityProviderMdib:
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
                    xml_reader_class: type[MessageReader] = MessageReader,
                    log_prefix: str | None = None) -> EntityProviderMdib:
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
        mdib.xtra.set_initial_content(descriptor_containers, state_containers)
        mdib.set_initialized()
        return mdib
