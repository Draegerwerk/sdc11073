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
from .xml_consumermdibxtra import XmlConsumerMdibMethods
from .xml_mdibbase import XmlMdibBase, XmlEntity, XmlMultiStateEntity, get_xsi_type

if TYPE_CHECKING:
    from sdc11073.consumer.consumerimpl import SdcConsumer
    from sdc11073.pysoap.msgreader import ReceivedMessage
    from lxml.etree import QName


@dataclass
class _BufferedData:
    received_message_data: ReceivedMessage
    handler: callable


class XmlConsumerMdib(XmlMdibBase):
    # sequence_or_instance_id_changed_event is set to True every time the sequence id changes.
    # It is not reset to False any time later.
    # It is in the responsibility of the application to react on a changed sequence id.
    # Observe this property and call "reload_all" in the observer code.
    sequence_or_instance_id_changed_event: bool = properties.ObservableProperty(
        default_value=False, fire_only_on_changed_value=False)

    MDIB_VERSION_CHECK_DISABLED = False  # for testing purpose you can disable checking of mdib version, so that every notification is accepted.

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
        self._sdc_client = sdc_client
        if extras_cls is None:
            extras_cls = XmlConsumerMdibMethods
        self._xtra = extras_cls(self, self._logger)
        self._state = ConsumerMdibState.invalid
        self.rt_buffers = {}  # key  is a handle, value is a ConsumerRtBuffer
        self._max_realtime_samples = max_realtime_samples
        self._last_wf_age_log = time.time()
        # a buffer for notifications that are received before initial get_mdib is done
        self._buffered_notifications = []
        self._buffered_notifications_lock = Lock()

    @property
    def xtra(self) -> Any:
        """Give access to extended functionality."""
        return self._xtra

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

            mdib_version_group = response.mdib_version_group
            self.mdib_version = mdib_version_group.mdib_version
            self._logger.info('setting initial mdib version to {}',
                              mdib_version_group.mdib_version)  # noqa: PLE1205
            self.sequence_id = mdib_version_group.sequence_id
            self._logger.info('setting initial sequence id to {}', mdib_version_group.sequence_id)  # noqa: PLE1205
            if mdib_version_group.instance_id != self.instance_id:
                self.instance_id = mdib_version_group.instance_id
            self._logger.info('setting initial instance id to {}', mdib_version_group.instance_id)  # noqa: PLE1205

            # Todo: Is this special handling still needed?
            # # retrieve context states only if there were none in mdib
            # if len(self.context_states.objects) == 0:
            #     self._get_context_states()
            # else:
            #     self._logger.info('found context states in GetMdib Result, will not call getContextStates')

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
        """Check mdib version, if okay:
         - update mdib.
         - update observable
        Call this method only if mdib_lock is already acquired."""
        if self._can_accept_mdib_version(received_message.mdib_version_group.mdib_version, 'metric states'):
            self._update_from_mdib_version_group(received_message.mdib_version_group)
        # Todo: replace states in self._get_mdib_response_node

        handles = []
        for report_part in received_message.p_msg.msg_node:
            for state in report_part:
                if state.tag == msg_qnames.MetricState:
                    handles.append(self._update_state(state))
        self.metric_handles = handles  # update observable

    def process_incoming_alert_states_report(self, received_message_data: ReceivedMessage):
        """Check mdib_version_group and process report it if okay."""
        if not self._pre_check_report_ok(received_message_data,
                                         self._process_incoming_metric_states_report):
            return
        with self.mdib_lock:
            self._process_incoming_alert_states_report(received_message_data)

    def _process_incoming_alert_states_report(self, received_message: ReceivedMessage):
        """Check mdib version, if okay:
         - update mdib.
         - update observable
        Call this method only if mdib_lock is already acquired."""
        if self._can_accept_mdib_version(received_message.mdib_version_group.mdib_version, 'alert states'):
            self._update_from_mdib_version_group(received_message.mdib_version_group)

        handles = []
        for report_part in received_message.p_msg.msg_node:
            for state in report_part:
                if state.tag == msg_qnames.AlertState:
                    handles.append(self._update_state(state))  # replace states in self._get_mdib_response_node
        self.alert_handles = handles  # update observable

    def process_incoming_waveform_states(self, received_message_data: ReceivedMessage):
        if not self._pre_check_report_ok(received_message_data,
                                         self._process_incoming_waveform_states):
            return
        with self.mdib_lock:
            self._process_incoming_waveform_states(received_message_data)

    def _process_incoming_waveform_states(self, received_message: ReceivedMessage):
        if self._can_accept_mdib_version(received_message.mdib_version_group.mdib_version, 'waveform states'):
            self._update_from_mdib_version_group(received_message.mdib_version_group)

        handles = []
        for state in received_message.p_msg.msg_node:
            handles.append(self._update_state(state,
                                              pm_qnames.RealTimeSampleArrayMetricState))  # replaces states in self._get_mdib_response_node

        self.waveform_handles = handles  # update observable

    def process_incoming_description_modification_report(self, received_message: ReceivedMessage):
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
                deleted_descriptors_handles.extend(self._delete_descriptors(parent_handle,
                                                                            source_mds_handle,
                                                                            descriptors, states))
            else:
                self.logger.error('Unknown modification type %r', modification_type)

    def _update_descriptors(self, parent_handle: str, source_mds_handle: str, descriptors: list, states: list):
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

    def _create_descriptors(self, parent_handle: str, source_mds_handle: str, descriptors: list, states: list) -> list[
        str]:
        handles = []
        for descriptor in descriptors:
            xsi_type = get_xsi_type(descriptor)
            handle = descriptor.attrib['Handle']
            current_states = [s for s in states if s.attrib['DescriptorHandle'] == handle]

            # add states to parent (MdState node)
            for st in current_states:
                st.tag = pm_qnames.State
                self._md_state_node.append(st)

            if xsi_type in self.multi_state_q_names:
                xml_entity = XmlMultiStateEntity(parent_handle, source_mds_handle, xsi_type, descriptor, current_states)
            else:
                if len(current_states) != 1:
                    self.logger.error('create descriptor: Expect one state, got %d', len(current_states))
                    # Todo: what to do in this case? add entity without state?
                    xml_entity = XmlEntity(parent_handle, source_mds_handle, xsi_type, descriptor, None)
                else:
                    xml_entity = XmlEntity(parent_handle, source_mds_handle, xsi_type, descriptor, current_states[0])
            self._entities[handle] = xml_entity
            handles.append(handle)

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
                child_order = ((pm_qnames.MetaData, pm_qnames.MetaData),  # optional member, no handle
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
    def _insert_child(child_node, child_xsi_type, parent_node, child_order: list[tuple[QName, QName]]):
        """Rename child_node to correct name acc. to BICEPS schema and insert at correct position."""
        # rename child_node to correct name required by BICEPS schema
        add_before_qnames = []

        for i, entry in enumerate(child_order):
            schema_name, xsi_type = entry
            if xsi_type == child_xsi_type:
                child_node.tag = schema_name
                add_before_qnames.extend([x[0] for x in child_order[i + 1:]])
                break

        # find position
        existing_children = parent_node[:]
        if not existing_children or not add_before_qnames:
            parent_node.append(child_node)
            return
        for i, tmp_child_node in enumerate(existing_children):
            if tmp_child_node.tag in add_before_qnames:
                tmp_child_node.addprevious(child_node)
                return
        raise RuntimeError('this should not happen')

    def _delete_descriptors(self, parent_handle: str, source_mds_handle: str, descriptors: list, states: list):
        handles = []
        for descriptor in descriptors:
            handle = descriptor.attrib['Handle']
            entity = self._entities.get(handle)
            if entity is None:
                self.logger.error('shall delete descriptor "%s", but it is unknown', handle)
            else:
                del self._entities[handle]
                handles.append(handle)
        return handles
        # Todo: update self._get_mdib_response_node

    def _update_state(self, state_node, xsi_type: QName | None = None) -> str:
        """Replace state in DOM tree and entity"""
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

    def _update_descriptor_states(self, descriptor_node, state_nodes) -> str:
        """Replace state in DOM tree and entity"""
        # state_nodes = [copy.deepcopy(s) for s in state_nodes] # we modify state_nodes, but only in a deep copy
        for state_node in state_nodes:
            state_node.tag = pm_qnames.State  # rename in order to have a valid tag acc. to participant model
        # descriptor_node = copy.deepcopy(descriptor_node)  # we modify descriptor_node, but only in a deep copy

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
        else:
            if len(state_nodes) != 1:
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
            print('_pre_check_report_ok: invalid')
            return False
        if self._state == ConsumerMdibState.initializing:
            print('_pre_check_report_ok: buffering')
            with self._buffered_notifications_lock:
                # check state again, it might have changed before lock was acquired
                if self._state == ConsumerMdibState.initializing:
                    self._buffered_notifications.append(_BufferedData(received_message_data, handler))
                    return False
        print('_pre_check_report_ok: ok')
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

    def _update_from_mdib_version_group(self, mdib_version_group):
        if mdib_version_group.mdib_version != self.mdib_version:
            self.mdib_version = mdib_version_group.mdib_version
        if mdib_version_group.sequence_id != self.sequence_id:
            self.sequence_id = mdib_version_group.sequence_id
        if mdib_version_group.instance_id != self.instance_id:
            self.instance_id = mdib_version_group.instance_id

    def _check_sequence_or_instance_id_changed(self, mdib_version_group):
        """Check if sequence id and instance id are still the same.

        If not,
        - set state member to invalid
        - set the observable "sequence_or_instance_id_changed_event" in a thread.
          This allows to implement an observer that can directly call reload_all without blocking the consumer."""
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
