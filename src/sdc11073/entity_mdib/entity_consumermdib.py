"""The module contains the implementation of the EntityConsumerMdib."""
from __future__ import annotations

import copy
import time
from dataclasses import dataclass
from threading import Lock, Thread
from typing import TYPE_CHECKING, Any, Callable

from sdc11073 import loghelper
from sdc11073 import observableproperties as properties
from sdc11073.exceptions import ApiUsageError
from sdc11073.mdib.consumermdib import ConsumerMdibState
from sdc11073.namespaces import QN_TYPE, default_ns_helper
from sdc11073.xml_types import msg_qnames, pm_qnames

from .entities import XmlEntity, XmlMultiStateEntity, get_xsi_type
from .entity_consumermdibxtra import EntityConsumerMdibMethods
from .entity_mdibbase import EntityMdibBase

if TYPE_CHECKING:
    from collections.abc import Iterable

    from lxml.etree import QName

    from sdc11073.consumer.consumerimpl import SdcConsumer
    from sdc11073.mdib.entityprotocol import EntityGetterProtocol, EntityTypeProtocol
    from sdc11073.mdib.mdibbase import MdibVersionGroup
    from sdc11073.pysoap.msgreader import ReceivedMessage
    from sdc11073.xml_types.pm_types import CodedValue, Coding
    from sdc11073.xml_utils import LxmlElement

    from .entities import ConsumerEntityType, ConsumerInternalEntityType

    XmlEntityFactory = Callable[[LxmlElement, str, str], ConsumerInternalEntityType]


@dataclass
class _BufferedData:
    received_message_data: ReceivedMessage
    handler: callable


multi_state_q_names = (pm_qnames.PatientContextDescriptor,
                       pm_qnames.LocationContextDescriptor,
                       pm_qnames.WorkflowContextDescriptor,
                       pm_qnames.OperatorContextDescriptor,
                       pm_qnames.MeansContextDescriptor,
                       pm_qnames.EnsembleContextDescriptor)


def _mk_xml_entity(node: LxmlElement, parent_handle: str, source_mds: str) -> ConsumerInternalEntityType:
    """Return a new XmlEntity or XmlMultiStateEntity.

    This is the default consumer entity factory.
    It creates one of ConsumerInternalEntityType, which are factories for ConsumerEntityType.
    By using a different factory, the user can change this to use other classes.
    """
    xsi_type = get_xsi_type(node)
    if xsi_type in multi_state_q_names:
        return XmlMultiStateEntity(parent_handle, source_mds, xsi_type, node, [])
    return XmlEntity(parent_handle, source_mds, xsi_type, node, None)


class EntityGetter:
    """Implementation of EntityGetterProtocol."""

    def __init__(self, entities: dict[str, XmlEntity | XmlMultiStateEntity], mdib: EntityConsumerMdib):
        self._entities = entities
        self._mdib = mdib

    def handle(self, handle: str) -> ConsumerEntityType | None:
        """Return entity with given handle."""
        try:
            return self._mk_entity(handle)
        except KeyError:
            return None

    def node_type(self, node_type: QName) -> list[EntityTypeProtocol]:
        """Return all entities with given node type."""
        ret = []
        for handle, entity in self._entities.items():
            if entity.node_type == node_type:
                ret.append(self._mk_entity(handle))
        return ret

    def parent_handle(self, parent_handle: str | None) -> list[EntityTypeProtocol]:
        """Return all entities with given parent handle."""
        ret = []
        for handle, entity in self._entities.items():
            if entity.parent_handle == parent_handle:
                ret.append(self._mk_entity(handle))
        return ret

    def coding(self, coding: Coding) -> list[EntityTypeProtocol]:
        """Return all entities with given Coding."""
        ret = []
        for handle, xml_entity in self._entities.items():
            if xml_entity.coded_value is not None and xml_entity.coded_value.is_equivalent(coding):
                ret.append(self._mk_entity(handle))
        return ret

    def coded_value(self, coded_value: CodedValue) -> list[EntityTypeProtocol]:
        """Return all entities with given Coding."""
        ret = []
        for handle, xml_entity in self._entities.items():
            if xml_entity.coded_value.is_equivalent(coded_value):
                ret.append(self._mk_entity(handle))
        return ret

    def items(self) -> Iterable[tuple[str, [EntityTypeProtocol]]]:
        """Return items of a dictionary."""
        for handle in self._entities:
            yield handle, self._mk_entity(handle)

    def _mk_entity(self, handle: str) -> ConsumerEntityType:
        xml_entity = self._mdib.internal_entities[handle]
        return xml_entity.mk_entity(self._mdib)

    def __len__(self) -> int:
        """Return number of entities."""
        return len(self._entities)


class EntityConsumerMdib(EntityMdibBase):
    """Implementation of the consumer side mdib with EntityGetter Interface.

    The internal entities store descriptors and states as XML nodes. This needs only very little CPU time for
    handling of notifications.
    The instantiation of descriptor and state container instances is only done on demand when the user calls the
    EntityGetter interface.
    """

    sequence_or_instance_id_changed_event: bool = properties.ObservableProperty(
        default_value=False, fire_only_on_changed_value=False)
    # sequence_or_instance_id_changed_event is set to True every time the sequence id changes.
    # It is not reset to False any time later.
    # It is in the responsibility of the application to react on a changed sequence id.
    # Observe this property and call "reload_all" in the observer code.

    MDIB_VERSION_CHECK_DISABLED = False

    # for testing purpose you can disable checking of mdib version, so that every notification is accepted.

    def __init__(self,
                 sdc_client: SdcConsumer,
                 extras_cls: type | None = None,
                 max_realtime_samples: int = 100):
        """Construct a ConsumerMdib instance.

        :param sdc_client: a SdcConsumer instance
        :param  extras_cls: extended functionality
        :param max_realtime_samples: determines how many real time samples are stored per RealtimeSampleArray
        """
        super().__init__(sdc_client.sdc_definitions,
                         loghelper.get_logger_adapter('sdc.client.mdib', sdc_client.log_prefix))
        self._entity_factory: XmlEntityFactory = _mk_xml_entity
        self._entities: dict[str, XmlEntity | XmlMultiStateEntity] = {}  # key is the handle

        self._sdc_client = sdc_client
        if extras_cls is None:
            extras_cls = EntityConsumerMdibMethods
        self._xtra = extras_cls(self, self._logger)
        self._state = ConsumerMdibState.invalid
        self.rt_buffers = {}  # key  is a handle, value is a ConsumerRtBuffer
        self._max_realtime_samples = max_realtime_samples
        self._last_wf_age_log = time.time()
        # a buffer for notifications that are received before initial get_mdib is done
        self._buffered_notifications = []
        self._buffered_notifications_lock = Lock()
        self.entities: EntityGetterProtocol = EntityGetter(self._entities, self)

    @property
    def xtra(self) -> Any:
        """Give access to extended functionality."""
        return self._xtra

    @property
    def internal_entities(self) -> dict[str, ConsumerInternalEntityType]:
        """The property is needed by transactions. Do not use it otherwise."""
        return self._entities

    @property
    def sdc_client(self) -> SdcConsumer:
        """Give access to sdc client."""
        return self._sdc_client

    @property
    def is_initialized(self) -> bool:
        """Returns True if everything has been set up completely."""
        return self._state == ConsumerMdibState.initialized

    def init_mdib(self):
        """Binds own notification handlers to observables of sdc client and calls GetMdib.

        Client mdib is initialized from GetMdibResponse, and from then on updated from incoming notifications.
        :return: None
        """
        if self.is_initialized:
            raise ApiUsageError('ConsumerMdib is already initialized')
        # first start receiving notifications, then call get_mdib.
        # Otherwise, we might miss notifications.
        self._xtra.bind_to_client_observables()
        self.reload_all()
        self._sdc_client.set_mdib(self)  # pylint: disable=protected-access
        self._logger.info('initializing mdib done')

    def reload_all(self):
        """Delete all data and reloads everything."""
        self._logger.info('reload_all called')
        with self.mdib_lock:
            self._state = ConsumerMdibState.initializing  # notifications are now buffered
            self._get_mdib_response_node = None
            self._md_state_node = None
            self.sequence_id = None
            self.instance_id = None
            self.mdib_version = None

            get_service = self._sdc_client.client('Get')
            self._logger.info('initializing mdib...')
            response = get_service.get_mdib()
            self._set_root_node(response.p_msg.msg_node)
            self._update_mdib_version_group(response.mdib_version_group)

            # process buffered notifications
            with self._buffered_notifications_lock:
                self._logger.debug('got _buffered_notifications_lock')
                for buffered_report in self._buffered_notifications:
                    # buffered data might contain notifications that do not fit.
                    mvg = buffered_report.received_message_data.mdib_version_group
                    if mvg.sequence_id != self.sequence_id:
                        self.logger.debug('wrong sequence id "%s"; ignore buffered report',
                                          mvg.sequence_id)
                        continue
                    if mvg.mdib_version <= self.mdib_version:
                        self.logger.debug('older mdib version "%d"; ignore buffered report',
                                          mvg.mdib_version)
                        continue
                    buffered_report.handler(buffered_report.received_message_data)
                del self._buffered_notifications[:]
                self._state = ConsumerMdibState.initialized
            self._logger.info('reload_all done')

    def process_incoming_metric_states_report(self, received_message_data: ReceivedMessage):
        """Check mdib_version_group and process report it if okay."""
        if not self._pre_check_report_ok(received_message_data,
                                         self._process_incoming_metric_states_report):
            return
        with self.mdib_lock:
            self._process_incoming_metric_states_report(received_message_data)

    def _process_incoming_metric_states_report(self, received_message: ReceivedMessage):
        """Check mdib version.

        If okay:
         - update mdib.
         - update observable.
        Call this method only if mdib_lock is already acquired.
        """
        self.metric_handles = self._process_incoming_state_report(received_message,
                                                                  msg_qnames.MetricState)

    def process_incoming_alert_states_report(self, received_message_data: ReceivedMessage):
        """Check mdib_version_group and process report it if okay."""
        if not self._pre_check_report_ok(received_message_data,
                                         self._process_incoming_metric_states_report):
            return
        with self.mdib_lock:
            self._process_incoming_alert_states_report(received_message_data)

    def _process_incoming_alert_states_report(self, received_message: ReceivedMessage):
        """Check mdib version.

        If okay:
         - update mdib.
         - update observable.
        Call this method only if mdib_lock is already acquired.
        """
        self.alert_handles = self._process_incoming_state_report(received_message,
                                                                 msg_qnames.AlertState)

    def process_incoming_component_report(self, received_message_data: ReceivedMessage):
        """Check mdib_version_group and process report it if okay."""
        if not self._pre_check_report_ok(received_message_data,
                                         self._process_incoming_metric_states_report):
            return
        with self.mdib_lock:
            self._process_incoming_component_report(received_message_data)

    def _process_incoming_component_report(self, received_message: ReceivedMessage):
        """Check mdib version.

        If okay:
         - update mdib.
         - update observable.
        Call this method only if mdib_lock is already acquired.
        """
        self.component_handles = self._process_incoming_state_report(received_message,
                                                                     msg_qnames.ComponentState)

    def process_incoming_operational_state_report(self, received_message_data: ReceivedMessage):
        """Check mdib_version_group and process report it if okay."""
        if not self._pre_check_report_ok(received_message_data,
                                         self._process_incoming_metric_states_report):
            return
        with self.mdib_lock:
            self._process_incoming_operational_state_report(received_message_data)

    def _process_incoming_operational_state_report(self, received_message: ReceivedMessage):
        """Check mdib version.

        If okay:
         - update mdib.
         - update observable.
        Call this method only if mdib_lock is already acquired.
        """
        self.operation_handles = self._process_incoming_state_report(received_message,
                                                                     msg_qnames.OperationState)

    def process_incoming_context_report(self, received_message_data: ReceivedMessage):
        """Check mdib_version_group and process report it if okay."""
        if not self._pre_check_report_ok(received_message_data,
                                         self._process_incoming_metric_states_report):
            return
        with self.mdib_lock:
            self._process_incoming_context_report(received_message_data)

    def _process_incoming_context_report(self, received_message: ReceivedMessage):
        """Check mdib version.

        If okay:
         - update mdib.
         - update observable.
        Call this method only if mdib_lock is already acquired.
        """
        if self._can_accept_mdib_version(received_message.mdib_version_group.mdib_version, 'component states'):
            self._update_mdib_version_group(received_message.mdib_version_group)

        handles = []
        for report_part in received_message.p_msg.msg_node:
            for state_node in report_part:
                if state_node.tag == msg_qnames.ContextState:
                    handle = state_node.attrib['Handle']
                    descriptor_handle = state_node.attrib['DescriptorHandle']
                    xml_entity = self._entities[descriptor_handle]
                    # modify state_node, but only in a deep copy
                    state_node = copy.deepcopy(state_node)  # noqa: PLW2901
                    state_node.tag = pm_qnames.State  # xml_entity.state.tag  # keep old tag

                    # replace state in parent
                    found = False
                    parent = self._md_state_node
                    for st in parent:
                        if st.get('Handle') == handle:
                            parent.replace(st, state_node)
                            found = True
                            break
                    if not found:
                        parent.append(state_node)

                    # replace or add in xml entity
                    xml_entity.states[handle] = state_node

                    handles.append(handle)
        self.context_handles = handles  # update observable

    def process_incoming_waveform_states(self, received_message_data: ReceivedMessage):
        """Check mdib_version_group and process report it if okay."""
        if not self._pre_check_report_ok(received_message_data,
                                         self._process_incoming_waveform_states):
            return
        with self.mdib_lock:
            self._process_incoming_waveform_states(received_message_data)

    def _process_incoming_waveform_states(self, received_message: ReceivedMessage):
        if self._can_accept_mdib_version(received_message.mdib_version_group.mdib_version, 'waveform states'):
            self._update_mdib_version_group(received_message.mdib_version_group)

        handles = []
        for state in received_message.p_msg.msg_node:
            handles.append(self._update_state(state,
                                              pm_qnames.RealTimeSampleArrayMetricState))  # replaces states in self._get_mdib_response_node
        self.waveform_handles = handles  # update observable

    def process_incoming_description_modification_report(self, received_message: ReceivedMessage):
        """Check mdib_version_group and process report it if okay."""
        if not self._pre_check_report_ok(received_message,
                                         self._process_incoming_metric_states_report):
            return
        with self.mdib_lock:
            self._process_incoming_description_modification_report(received_message)

    def _process_incoming_description_modification_report(self, received_message: ReceivedMessage):
        new_descriptors_handles = []
        updated_descriptors_handles = []
        deleted_descriptors_handles = []
        for report_part in received_message.p_msg.msg_node:
            parent_handle = report_part.attrib.get('ParentDescriptor')  # can be None in case of MDS, but that is ok
            source_mds_handle = report_part[0].text
            descriptors = [copy.deepcopy(e) for e in report_part if e.tag == msg_qnames.Descriptor]
            states = [copy.deepcopy(e) for e in report_part if e.tag == msg_qnames.State]
            modification_type = report_part.attrib.get('ModificationType', 'Upt')  # implied value is 'Upt'
            if modification_type == 'Upt':
                updated_descriptors_handles.extend(self._update_descriptors(parent_handle,
                                                                            source_mds_handle,
                                                                            descriptors,
                                                                            states))
            elif modification_type == 'Crt':
                new_descriptors_handles.extend(self._create_descriptors(parent_handle,
                                                                        source_mds_handle,
                                                                        descriptors,
                                                                        states))
            elif modification_type == 'Del':
                deleted_descriptors_handles.extend(self._delete_descriptors(descriptors))
            else:
                self.logger.error('Unknown modification type %r', modification_type)
        if updated_descriptors_handles:
            self.updated_descriptors_handles = updated_descriptors_handles
        if new_descriptors_handles:
            self.new_descriptors_handles = new_descriptors_handles
        if deleted_descriptors_handles:
            self.deleted_descriptors_handles = deleted_descriptors_handles

    def _update_descriptors(self,
                            parent_handle: str,
                            source_mds_handle: str,
                            descriptors: list[LxmlElement],
                            states: list[LxmlElement]) -> list[str]:
        handles = []
        for descriptor in descriptors:
            handle = descriptor.attrib['Handle']
            entity = self._entities.get(handle)
            if entity.parent_handle != parent_handle:
                self.logger.error('inconsistent parent handle "%s" for "%s"', handle, entity.parent_handle)
            if entity.source_mds != source_mds_handle:
                self.logger.error('inconsistent source mds handle "%s" for "%s"',
                                  source_mds_handle, entity.source_mds)
            if entity is None:
                self.logger.error('got descriptor update for not existing handle "%s"', handle)
                continue

            current_states = [s for s in states if s.attrib['DescriptorHandle'] == handle]
            self._update_descriptor_states(descriptor, current_states)
            handles.append(handle)
        return handles

    def _create_descriptors(self,
                            parent_handle: str,
                            source_mds_handle: str,
                            descriptors: list[LxmlElement],
                            states: list[LxmlElement]) -> list[str]:
        handles = []
        for descriptor in descriptors:
            xsi_type = get_xsi_type(descriptor)
            descriptor_handle = descriptor.attrib['Handle']
            current_states = [s for s in states if s.attrib['DescriptorHandle'] == descriptor_handle]

            # add states to parent (MdState node)
            for st in current_states:
                st.tag = pm_qnames.State
                self._md_state_node.append(st)

            xml_entity = self._entity_factory(descriptor, parent_handle, source_mds_handle)
            if xml_entity.is_multi_state:
                for st in current_states:
                    xml_entity.states[st.attrib['Handle']] = st
            elif len(current_states) != 1:
                self.logger.error('create descriptor: Expect one state, got %d', len(current_states))
                # Todo: what to do in this case? add entity without state?
            else:
                xml_entity.state = current_states[0]
            self._entities[descriptor_handle] = xml_entity
            handles.append(descriptor_handle)

            # add descriptor to parent
            parent_xml_entity = self._entities[parent_handle]
            if parent_xml_entity.node_type == pm_qnames.ChannelDescriptor:
                # channel children have same tag
                descriptor.tag = pm_qnames.Metric
                parent_xml_entity.descriptor.append(descriptor)
            elif parent_xml_entity.node_type == pm_qnames.VmdDescriptor:
                # vmd children have same tag
                descriptor.tag = pm_qnames.Channel
                parent_xml_entity.descriptor.append(descriptor)
            elif parent_xml_entity.node_type == pm_qnames.MdsDescriptor:
                # Mds children have different names.
                # child_order determines the tag of the element (first tuple position), and the corresponding type
                # (2nd position)
                child_order: Iterable[tuple[QName, QName]] = (
                    (pm_qnames.MetaData, pm_qnames.MetaData),  # optional member, no handle
                    (pm_qnames.SystemContext, pm_qnames.SystemContextDescriptor),
                    (pm_qnames.Clock, pm_qnames.ClockDescriptor),
                    (pm_qnames.Battery, pm_qnames.BatteryDescriptor),
                    (pm_qnames.ApprovedJurisdictions, pm_qnames.ApprovedJurisdictions),
                    # optional list, no handle
                    (pm_qnames.Vmd, pm_qnames.VmdDescriptor))
                # Insert at correct position with correct name!
                self._insert_child(descriptor, xsi_type,
                                   parent_xml_entity.descriptor, child_order)
        return handles

    @staticmethod
    def _insert_child(child_node: LxmlElement,
                      child_xsi_type: QName,
                      parent_node:
                      LxmlElement,
                      child_order: Iterable[tuple[QName, QName]]):
        """Rename child_node to correct name acc. to BICEPS schema and insert at correct position."""
        # rename child_node to correct name required by BICEPS schema
        add_before_q_names = []

        for i, entry in enumerate(child_order):
            schema_name, xsi_type = entry
            if xsi_type == child_xsi_type:
                child_node.tag = schema_name
                add_before_q_names.extend([x[0] for x in child_order[i + 1:]])
                break

        # find position
        existing_children = parent_node[:]
        if not existing_children or not add_before_q_names:
            parent_node.append(child_node)
            return
        for tmp_child_node in existing_children:
            if tmp_child_node.tag in add_before_q_names:
                tmp_child_node.addprevious(child_node)
                return
        raise RuntimeError('this should not happen')

    def _delete_descriptors(self, descriptors: list[LxmlElement]) -> list[str]:
        handles = []
        for descriptor in descriptors:
            handle = descriptor.attrib['Handle']
            entity = self._entities.get(handle)
            if entity is None:
                self.logger.error('shall delete descriptor "%s", but it is unknown', handle)
            else:
                self._delete_entity(entity, handles)
        return handles
        # Todo: update self._get_mdib_response_node

    def _delete_entity(self, entity: XmlEntity | XmlMultiStateEntity, deleted_handles: list[str]):
        """Recursive method to delete an entity and subtree."""
        parent = entity.descriptor.getparent()
        if parent is not None:
            parent.remove(entity.descriptor)
        states = entity.states.values() if entity.is_multi_state else [entity.state]
        for state in states:
            parent = state.getparent()
            if parent is not None:
                parent.remove(state)
        handle = entity.descriptor.get('Handle')
        del self._entities[handle]
        deleted_handles.append(handle)
        child_entities = [e for e in self._entities.values() if e.parent_handle == handle]
        for e in child_entities:
            self._delete_entity(e, deleted_handles)

    def _process_incoming_state_report(self, received_message: ReceivedMessage, expected_q_name: QName) -> list[str]:
        """Check mdib version.

        If okay:
         - update mdib.
         - update observable.
        Call this method only if mdib_lock is already acquired.
        """
        if self._can_accept_mdib_version(received_message.mdib_version_group.mdib_version, 'state'):
            self._update_mdib_version_group(received_message.mdib_version_group)

        handles = []
        for report_part in received_message.p_msg.msg_node:
            for state in report_part:
                if state.tag == expected_q_name:
                    handles.append(self._update_state(state))  # replace states in self._get_mdib_response_node
        return handles  # update observable

    def _update_state(self, state_node: LxmlElement, xsi_type: QName | None = None) -> str:
        """Replace state in DOM tree and entity."""
        descriptor_handle = state_node.attrib['DescriptorHandle']
        xml_entity = self._entities[descriptor_handle]
        state_node = copy.deepcopy(state_node)  # we modify state_node, but only in a deep copy
        state_node.tag = pm_qnames.State  # xml_entity.state.tag  # keep old tag
        if xsi_type:
            state_node.set(QN_TYPE, default_ns_helper.doc_name_from_qname(xsi_type))

        # replace state in parent
        parent = xml_entity.state.getparent()
        parent.replace(xml_entity.state, state_node)

        # replace in xml entity
        xml_entity.state = state_node
        return descriptor_handle

    def _update_descriptor_states(self, descriptor_node: LxmlElement, state_nodes: list[LxmlElement]) -> str:
        """Replace state in DOM tree and entity."""
        for state_node in state_nodes:
            state_node.tag = pm_qnames.State  # rename in order to have a valid tag acc. to participant model

        descriptor_handle = descriptor_node.attrib['Handle']
        xml_entity = self._entities[descriptor_handle]
        descriptor_node.tag = xml_entity.descriptor.tag  # keep old tag

        # move all children with a Handle from entity.descriptor to descriptor_node (at identical position )
        children = xml_entity.descriptor[:]
        for idx, child in enumerate(children):
            if 'Handle' in child.attrib:
                descriptor_node.insert(idx, child)

        # replace descriptor in parent
        descriptor_parent = xml_entity.descriptor.getparent()
        descriptor_parent.replace(xml_entity.descriptor, descriptor_node)

        # replace descriptor in xml_entity
        xml_entity.descriptor = descriptor_node

        if xml_entity.is_multi_state:
            # replace state_nodes in parent
            for state_node in state_nodes:
                state_parent = xml_entity.state.getparent()
                state_parent.replace(xml_entity.state, state_node)

            # replace state_nodes in xml_entity
            xml_entity.states = state_nodes
        elif len(state_nodes) != 1:
            self.logger.error('update descriptor: Expect one state, got %d', len(state_nodes))
            # Todo: what to do in this case? add entity without state?
        else:
            state_parent = xml_entity.state.getparent()
            state_parent.replace(xml_entity.state, state_nodes[0])
            xml_entity.state = state_nodes[0]
        return descriptor_handle

    def _pre_check_report_ok(self, received_message_data: ReceivedMessage,
                             handler: Callable) -> bool:
        """Check if the report can be added to mdib.

        The pre-check runs before the mdib lock is acquired.
        The report is buffered if state is 'initializing' and 'is_buffered_report' is False.
        :return: True if report can be added to mdib.
        """
        self._check_sequence_or_instance_id_changed(
            received_message_data.mdib_version_group)  # this might change self._state
        if self._state == ConsumerMdibState.invalid:
            # ignore report in these states
            return False
        if self._state == ConsumerMdibState.initializing:
            with self._buffered_notifications_lock:
                # check state again, it might have changed before lock was acquired
                if self._state == ConsumerMdibState.initializing:
                    self._buffered_notifications.append(_BufferedData(received_message_data, handler))
                    return False
        return True

    def _can_accept_mdib_version(self, new_mdib_version: int, log_prefix: str) -> bool:
        if self.MDIB_VERSION_CHECK_DISABLED:
            return True
        # log deviations from expected mdib version
        if new_mdib_version < self.mdib_version:
            self._logger.warning('{}: ignoring too old Mdib version, have {}, got {}',  # noqa: PLE1205
                                 log_prefix, self.mdib_version, new_mdib_version)
        elif (new_mdib_version - self.mdib_version) > 1:
            # This can happen if consumer did not subscribe to all notifications.
            # Still log a warning, because mdib is no longer a correct mirror of provider mdib.
            self._logger.warning('{}: expect mdib_version {}, got {}',  # noqa: PLE1205
                                 log_prefix, self.mdib_version + 1, new_mdib_version)
        # it is possible to receive multiple notifications with the same mdib version => compare ">="
        return new_mdib_version >= self.mdib_version

    def _check_sequence_or_instance_id_changed(self, mdib_version_group: MdibVersionGroup):
        """Check if sequence id and instance id are still the same.

        If not,
        - set state member to invalid
        - set the observable "sequence_or_instance_id_changed_event" in a thread.
          This allows to implement an observer that can directly call reload_all without blocking the consumer.
        """
        if mdib_version_group.sequence_id == self.sequence_id and mdib_version_group.instance_id == self.instance_id:
            return
        if self._state == ConsumerMdibState.initialized:
            if mdib_version_group.sequence_id != self.sequence_id:
                self.logger.warning('sequence id changed from "%s" to "%s"',
                                    self.sequence_id, mdib_version_group.sequence_id)
            if mdib_version_group.instance_id != self.instance_id:
                self.logger.warning('instance id changed from "%r" to "%r"',
                                    self.instance_id, mdib_version_group.instance_id)
            self.logger.warning('mdib is no longer valid!')

            self._state = ConsumerMdibState.invalid

            def _set_observable():
                self.sequence_or_instance_id_changed_event = True

            thr = Thread(target=_set_observable)
            thr.start()

    def _set_root_node(self, root_node: LxmlElement): # noqa: C901
        """Set member and create xml entities."""
        if root_node.tag != msg_qnames.GetMdibResponse:
            raise ValueError(f'root node must be {msg_qnames.GetMdibResponse!s}, got {root_node.tag!s}')
        self._get_mdib_response_node = root_node
        self._mdib_node = root_node[0]
        self._entities.clear()
        for child_element in self._mdib_node:  # MdDescription, MdState; both are optional
            if child_element.tag == pm_qnames.MdState:
                self._md_state_node = child_element
            if child_element.tag == pm_qnames.MdDescription:
                self._md_description_node = child_element

        def register_children_with_handle(parent_node: LxmlElement, source_mds: str | None = None):
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
