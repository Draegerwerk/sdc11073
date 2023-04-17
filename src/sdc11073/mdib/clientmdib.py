import time
import traceback
from collections import deque
from dataclasses import dataclass
from threading import Lock
from typing import List, Any
from decimal import Decimal
from enum import Enum

from . import mdibbase
from .clientmdibxtra import ClientMdibMethods
from .. import loghelper
from ..exceptions import ApiUsageError


@dataclass
class RtSampleContainer:
    """Contains a single Value"""
    value_string: str
    determination_time: float
    validity: Enum
    annotations: list

    @property
    def value(self):
        return float(self.value_string)

    @property
    def dec_value(self):
        return Decimal(self.value_string)

    def __repr__(self):
        return f'RtSample value="{self.value_string}" validity="{self.validity}" time={self.determination_time}'


class ClientRtBuffer:
    """Collects data of one real time stream."""

    def __init__(self,
                 sample_period: float,
                 max_samples: int):
        """
        :param sample_period: float value, in seconds.
                              When an incoming real time sample array is split into single RtSampleContainers, this is used to calculate the individual time stamps.
                              Value can be zero if correct value is not known. In this case all Containers will have the observation time of the sample array.
        :param max_samples: integer, max. length of self.rt_data
        """
        self.rt_data = deque(maxlen=max_samples)
        self.sample_period = sample_period
        self._max_samples = max_samples
        self._logger = loghelper.get_logger_adapter('sdc.client.mdib.rt')
        self._lock = Lock()
        self.last_sc = None  # last state container that was handled

    def mk_rt_sample_containers(self, realtime_sample_array_container) -> List[RtSampleContainer]:
        """

        :param realtime_sample_array_container: a RealTimeSampleArrayMetricStateContainer instance
        :return: a list of RtSampleContainer
        """
        self.last_sc = realtime_sample_array_container
        metric_value = realtime_sample_array_container.MetricValue
        if metric_value is None:
            # this can happen if metric state is not activated.
            self._logger.debug('real time sample array "{} "has no metric value, ignoring it',
                               realtime_sample_array_container.DescriptorHandle)
            return []
        determination_time = metric_value.DeterminationTime
        annotations = metric_value.Annotations
        apply_annotations = metric_value.ApplyAnnotations
        rt_sample_containers = []
        if metric_value.Samples is not None:
            for i, sample in enumerate(metric_value.Samples):
                applied_annotations = []
                if apply_annotations is not None:
                    for apply_annotation in apply_annotations:
                        if apply_annotation.SampleIndex == i:
                            # there is an annotation for this sample:
                            ann_index = apply_annotation.AnnotationIndex
                            annotation = annotations[ann_index]  # index is zero-based
                            applied_annotations.append(annotation)
                rt_sample_time = determination_time + i * self.sample_period
                rt_sample_containers.append(RtSampleContainer(sample,
                                                             rt_sample_time,
                                                             metric_value.MetricQuality.Validity,
                                                             applied_annotations))
        return rt_sample_containers

    def add_rt_sample_containers(self, rt_sample_containers: List[RtSampleContainer]) -> None:
        """
        Updates self.rt_data with the new rt_sample_containers
        :param rt_sample_containers: a list of RtSampleContainer
        :return: None
        """
        if not rt_sample_containers:
            return
        with self._lock:
            self.rt_data.extend(rt_sample_containers)

    def read_rt_data(self) -> List[RtSampleContainer]:
        """ This read method consumes all currently buffered data.
        @return: a list of RtSampleContainer objects"""
        with self._lock:
            ret = list(self.rt_data)
            self.rt_data.clear()
        return ret


@dataclass
class _BufferedData:
    mdib_version_group: Any  # MdibVersionGroup
    data: list
    handler: callable


class ClientMdibContainer(mdibbase.MdibContainer):
    """ This mdib is meant to be read-only.
    Only update source is an SdcClient."""

    MDIB_VERSION_CHECK_DISABLED = False  # for testing purpose you can disable checking of mdib version, so that every notification is accepted.

    # INITIAL_NOTIFICATION_BUFFERING setting determines how incoming notifications are handled between start of
    # subscription ond handling of GetMib Response.
    # INITIAL_NOTIFICATION_BUFFERING = False: the response for the first incoming notification is answered after the get_mdib is done.
    # INITIAL_NOTIFICATION_BUFFERING = True:  responses are sent immediately and first notifications are buffered.
    INITIAL_NOTIFICATION_BUFFERING = True

    def __init__(self,
                 sdc_client,
                 extras_cls=None,
                 max_realtime_samples=100):
        """

        :param sdc_client: a SdcClient instance
        :param max_realtime_samples: determines how many real time samples are stored per RealtimeSampleArray
        """
        super().__init__(sdc_client.sdc_definitions)
        self._logger = loghelper.get_logger_adapter('sdc.client.mdib', sdc_client.log_prefix)
        self._sdc_client = sdc_client
        if extras_cls is None:
            extras_cls = ClientMdibMethods
        self._xtra = extras_cls(self, self._logger)
        self._is_initialized = False
        self.rt_buffers = {}  # key  is a handle, value is a ClientRtBuffer
        self._max_realtime_samples = max_realtime_samples
        self._last_wf_age_log = time.time()
        self._context_mdib_version = None
        # a buffer for notifications that are received before initial get_mdib is done
        self._buffered_notifications = []
        self._buffered_notifications_lock = Lock()
        self._sequence_id_changed_flag = False

    @property
    def xtra(self):
        return self._xtra

    @property
    def sdc_client(self):
        return self._sdc_client

    @property
    def is_initialized(self):
        return self._is_initialized

    def init_mdib(self):
        """
        Binds own notification handlers to observables of sdc client and calls GetMdib.
        Client mdib is initialized from GetMdibResponse, and from then on updated from incoming notifications.
        :return:
        """
        if self._is_initialized:
            raise ApiUsageError('ClientMdibContainer is already initialized')
        # first start receiving notifications, then call get_mdib.
        # Otherwise, we might miss notifications.
        self._xtra.bind_to_client_observables()
        self.reload_all()
        self._sdc_client.set_mdib(self)  # pylint: disable=protected-access
        self._logger.info('initializing mdib done')

    def reload_all(self):
        """
        Deletes all data and reloads everything. Useful e.g. after sequence id change.
        This is not called automatically, the application has to take care.
        :return:
        """
        self._is_initialized = False
        self._sequence_id_changed_flag = False
        self.descriptions.clear()
        self.clear_states()

        get_service = self._sdc_client.client('Get')
        self._logger.info('initializing mdib...')
        response = get_service.get_mdib()  # GetRequestResult
        self._logger.info('creating description containers...')
        descriptor_containers, state_containers = response.result
        self.add_description_containers(descriptor_containers)
        self._logger.info('creating state containers...')
        self.add_state_containers(state_containers)

        mdib_version_group = response.mdib_version_group
        if mdib_version_group.mdib_version is not None:
            self.mdib_version = mdib_version_group.mdib_version
            self._logger.info('setting initial mdib version to {}', mdib_version_group.mdib_version)
        else:
            self._logger.warn('found no mdib version in GetMdib response, assuming "0"')
            self.mdib_version = 0
        self.sequence_id = mdib_version_group.sequence_id
        self._logger.info('setting initial sequence id to {}', mdib_version_group.sequence_id)
        if mdib_version_group.instance_id != self.instance_id:
            self.instance_id = mdib_version_group.instance_id
        self._logger.info('setting initial instance id to {}', mdib_version_group.instance_id)

        # retrieve context states only if there were none in mdib
        if len(self.context_states.objects) == 0:
            self._get_context_states()
        else:
            self._logger.info('found context states in GetMdib Result, will not call getContextStates')

        # process buffered notifications
        with self._buffered_notifications_lock:
            for buffered_report in self._buffered_notifications:
                buffered_report.handler(buffered_report.mdib_version_group,
                                        buffered_report.data,
                                        is_buffered_report=True)
            del self._buffered_notifications[:]
            self._is_initialized = True

    def _buffer_data(self, mdib_version_group, data, func):
        """
        Write notification to a temporary buffer, as long as mdib is not initialized.
        :param mdib_version_group:
        :param data:
        :param func: the callable that shall be called later for delayed handling of report
        :return: True if buffered, False if report shall be processed immediately
        """
        if self._is_initialized:
            # no reason to buffer
            return False

        if not self.INITIAL_NOTIFICATION_BUFFERING:
            self._wait_until_initialized(func.__name__)
            return False

        # get lock and check if we need to write to buffer
        with self._buffered_notifications_lock:
            if not self._is_initialized:
                self._buffered_notifications.append(_BufferedData(mdib_version_group, data, func))
                return True
            return False

    def _get_context_states(self, handles=None):
        try:
            self._logger.debug('new Query, handles={}', handles)
            time.sleep(0.001)
            context_service = self._sdc_client.client('Context')
            self._logger.info('requesting context states...')
            response = context_service.get_context_states(handles)
            context_state_containers = response.result.ContextState

            self._context_mdib_version = response.mdib_version
            self._logger.debug('_get_context_states: setting _context_mdib_version to {}', self._context_mdib_version)

            self._logger.debug('got {} context states', len(context_state_containers))
            with self.context_states.lock:
                for state_container in context_state_containers:
                    old_state_containers = self.context_states.handle.get(state_container.Handle, [])
                    if len(old_state_containers) == 0:
                        self.context_states.add_object_no_lock(state_container)
                        self._logger.debug('new ContextState {}', state_container)
                    elif len(old_state_containers) == 1:
                        old_state_container = old_state_containers[0]
                        if old_state_container.StateVersion != state_container.StateVersion:
                            self._logger.debug('update {} ==> {}', old_state_container, state_container)
                            old_state_container.update_from_node(state_container.node)
                            self.context_states.update_object_no_lock(old_state_container)
                        else:
                            difference = state_container.diff(old_state_container)
                            if difference:
                                self._logger.error('no state version update but different!\n{ \n{}', difference)
                    else:
                        txt = ', '.join([str(x) for x in old_state_containers])
                        self._logger.error('found {} objects: {}', len(old_state_containers), txt)

        except Exception:
            self._logger.error(traceback.format_exc())
        finally:
            self._logger.info('_get_context_states done')

    def _can_accept_version(self, mdib_version, sequence_id, log_prefix):
        if not self._can_accept_mdib_version(log_prefix, mdib_version):
            return False
        if self._sequence_id_changed(sequence_id):
            return False
        return True

    def _can_accept_mdib_version(self, log_prefix, new_mdib_version):
        if self.MDIB_VERSION_CHECK_DISABLED:
            return True
        if new_mdib_version is None:
            self._logger.error('{}: could not check MdibVersion!', log_prefix)
        else:
            # log deviations from expected mdib version
            if new_mdib_version < self.mdib_version:
                self._logger.warn('{}: ignoring too old Mdib version, have {}, got {}', log_prefix, self.mdib_version,
                                  new_mdib_version)
            elif (new_mdib_version - self.mdib_version) > 1:
                if self._sdc_client.all_subscribed:
                    self._logger.warn('{}: expect mdib_version {}, got {}', log_prefix, self.mdib_version + 1,
                                      new_mdib_version)
            # it is possible to receive multiple notifications with the same mdib version => compare ">="
            if new_mdib_version >= self.mdib_version:
                return True
        return False

    def _sequence_id_changed(self, sequence_id: str) -> bool:
        if self.sequence_id != sequence_id:
            self._sequence_id_changed_flag = True
            self.sequence_id = sequence_id
        return self._sequence_id_changed_flag

    def _update_from_mdib_version_group(self, mdib_version_group):
        if mdib_version_group.mdib_version != self.mdib_version:
            self.mdib_version = mdib_version_group.mdib_version
        if mdib_version_group.sequence_id != self.sequence_id:
            self.sequence_id = mdib_version_group.sequence_id
        if mdib_version_group.instance_id != self.instance_id:
            self.instance_id = mdib_version_group.instance_id

    def _wait_until_initialized(self, log_prefix):
        show_success_log = False
        started = time.monotonic()
        while not self._is_initialized:
            delay = time.monotonic() - started
            if 3 >= delay > 1:
                show_success_log = True
                self._logger.warn('{}: _wait_until_initialized takes long...', log_prefix)
            elif delay > 10:
                raise RuntimeError('_wait_until_initialized failed')
            time.sleep(1)
        delay = time.monotonic() - started
        if show_success_log:
            self._logger.info('{}: _wait_until_initialized took {} seconds', log_prefix, delay)

    def _process_incoming_states(self, report_type: str, state_containers: list, is_buffered_report: bool) -> dict:
        """Update mdib with incoming states"""
        states_by_handle = {}
        for state_container in state_containers:
            if state_container.is_context_state:
                src = self.context_states
                old_state_container = src.handle.get_one(state_container.Handle,
                                                         allow_none=True)
            else:
                src = self.states
                old_state_container = src.descriptorHandle.get_one(state_container.DescriptorHandle,
                                                                   allow_none=True)
            if old_state_container is not None:
                if self._has_new_state_usable_state_version(old_state_container, state_container,
                                                            report_type,
                                                            is_buffered_report):
                    old_state_container.update_from_other_container(state_container)
                    src.update_object(old_state_container)
                    states_by_handle[old_state_container.DescriptorHandle] = old_state_container
            else:
                if state_container.is_context_state:
                    self._logger.info(
                        '{}: new context state handle = {} Descriptor Handle={} Assoc={}, Validators={}',
                        report_type, state_container.Handle, state_container.DescriptorHandle,
                        state_container.ContextAssociation, state_container.Validator)
                else:
                    self._logger.error('{}: got a new state {}',
                                       report_type,
                                       state_container.DescriptorHandle)
                self._set_descriptor_container_reference(state_container)
                src.add_object(state_container)
                states_by_handle[state_container.DescriptorHandle] = state_container
        return states_by_handle

    def _process_incoming_states_report(self, report_type: str, report, is_buffered_report: bool) -> dict:
        """Update mdib with incoming states"""
        states_by_handle = {}
        for report_part in report.ReportPart:
            for state_container in report_part.values_list:
                if state_container.is_context_state:
                    src = self.context_states
                    old_state_container = src.handle.get_one(state_container.Handle,
                                                             allow_none=True)
                else:
                    src = self.states
                    old_state_container = src.descriptorHandle.get_one(state_container.DescriptorHandle,
                                                                       allow_none=True)
                if old_state_container is not None:
                    if self._has_new_state_usable_state_version(old_state_container, state_container,
                                                                report_type,
                                                                is_buffered_report):
                        old_state_container.update_from_other_container(state_container)
                        src.update_object(old_state_container)
                        states_by_handle[old_state_container.DescriptorHandle] = old_state_container
                else:
                    if state_container.is_context_state:
                        self._logger.info(
                            '{}: new context state handle = {} Descriptor Handle={} Assoc={}, Validators={}',
                            report_type, state_container.Handle, state_container.DescriptorHandle,
                            state_container.ContextAssociation, state_container.Validator)
                    else:
                        self._logger.error('{}: got a new state {}',
                                           report_type,
                                           state_container.DescriptorHandle)
                    self._set_descriptor_container_reference(state_container)
                    src.add_object(state_container)
                    states_by_handle[state_container.DescriptorHandle] = state_container
        return states_by_handle

    def process_incoming_metric_states_report(self, mdib_version_group, report, is_buffered_report=False):
        if not is_buffered_report and self._buffer_data(mdib_version_group, report,
                                                        self.process_incoming_metric_states_report):
            return
        states_by_handle = {}
        try:
            with self.mdib_lock:
                if not self._can_accept_version(mdib_version_group.mdib_version, mdib_version_group.sequence_id, 'metric states'):
                    return
                self._update_from_mdib_version_group(mdib_version_group)
                states_by_handle = self._process_incoming_states_report(
                    'metric states', report, is_buffered_report)
        finally:
            self.metrics_by_handle = states_by_handle  # used by wait_metric_matches method

    def process_incoming_alert_states_report(self, mdib_version_group, report, is_buffered_report=False):
        if not is_buffered_report and self._buffer_data(mdib_version_group, report,
                                                        self.process_incoming_alert_states_report):
            return
        states_by_handle = {}
        try:
            with self.mdib_lock:
                if not self._can_accept_version(mdib_version_group.mdib_version, mdib_version_group.sequence_id, 'alert states'):
                    return
                self._update_from_mdib_version_group(mdib_version_group)
                states_by_handle = self._process_incoming_states_report(
                    'alert states', report, is_buffered_report)
        finally:
            self.alert_by_handle = states_by_handle  # used by wait_metric_matches method

    def process_incoming_operational_states_report(self, mdib_version_group, report,
                                            is_buffered_report=False):
        if not is_buffered_report and self._buffer_data(mdib_version_group, report,
                                                        self.process_incoming_operational_states_report):
            return
        states_by_handle = {}

        try:
            with self.mdib_lock:
                if not self._can_accept_version(mdib_version_group.mdib_version, mdib_version_group.sequence_id, 'operational states'):
                    return
                self._update_from_mdib_version_group(mdib_version_group)
                states_by_handle = self._process_incoming_states_report(
                    'operational states', report, is_buffered_report)
        finally:
            self.operation_by_handle = states_by_handle  # used by wait_metric_matches method

    def process_incoming_waveform_states(self, mdib_version_group, state_containers, is_buffered_report=False):
        if not is_buffered_report and self._buffer_data(mdib_version_group, state_containers,
                                                        self.process_incoming_waveform_states):
            return
        states_by_handle = {}
        try:
            with self.mdib_lock:
                if not self._can_accept_version(mdib_version_group.mdib_version, mdib_version_group.sequence_id, 'waveform states'):
                    return
                self._update_from_mdib_version_group(mdib_version_group)
                states_by_handle = self._process_incoming_states(
                    'waveform states', state_containers, is_buffered_report)

                # add to Waveform Buffer
                for state_container in states_by_handle.values():
                    descriptor_container = state_container.descriptor_container
                    d_handle = state_container.DescriptorHandle
                    rt_buffer = self.rt_buffers.get(d_handle)
                    if rt_buffer is None:
                        sample_period = 0  # default
                        if descriptor_container is not None:
                            # read sample period
                            sample_period = descriptor_container.SamplePeriod or 0
                        rt_buffer = ClientRtBuffer(sample_period=sample_period, max_samples=self._max_realtime_samples)
                        self.rt_buffers[d_handle] = rt_buffer
                    rt_sample_containers = rt_buffer.mk_rt_sample_containers(state_container)
                    rt_buffer.add_rt_sample_containers(rt_sample_containers)
        finally:
            if states_by_handle is not None:
                self.waveform_by_handle = states_by_handle
        return states_by_handle

    def process_incoming_context_states_report(self, mdib_version_group, report, is_buffered_report=False):
        if not is_buffered_report and self._buffer_data(mdib_version_group, report,
                                                        self.process_incoming_context_states_report):
            return

        try:
            with self.mdib_lock:
                if not self._can_accept_version(mdib_version_group.mdib_version, mdib_version_group.sequence_id, 'context states'):
                    return
                self._update_from_mdib_version_group(mdib_version_group)
                states_by_handle = self._process_incoming_states_report(
                    'context states', report, is_buffered_report)
        finally:
            self.context_by_handle = states_by_handle  # used by wait_metric_matches method

    def process_incoming_component_states_report(self, mdib_version_group, report, is_buffered_report=False):
        if not is_buffered_report and self._buffer_data(mdib_version_group, report,
                                                        self.process_incoming_component_states_report):
            return

        try:
            with self.mdib_lock:
                if not self._can_accept_version(mdib_version_group.mdib_version, mdib_version_group.sequence_id, 'component states'):
                    return
                self._update_from_mdib_version_group(mdib_version_group)
                states_by_handle = self._process_incoming_states_report(
                    'component states', report, is_buffered_report)
        finally:
            self.component_by_handle = states_by_handle  # used by wait_metric_matches method

    def process_incoming_description_modifications(self, mdib_version_group, report, is_buffered_report=False):
        if not is_buffered_report and self._buffer_data(mdib_version_group, report,
                                                        self.process_incoming_description_modifications):
            return

        def multi_key(st_container):
            if st_container.is_context_state:
                return self.context_states
            else:
                return self.states

        new_descriptor_by_handle = {}
        updated_descriptor_by_handle = {}
        try:
            DescriptionModificationType = self.sdc_definitions.data_model.msg_types.DescriptionModificationType
            with self.mdib_lock:
                if not self._can_accept_version(mdib_version_group.mdib_version, mdib_version_group.sequence_id, 'descriptors'):
                    return
                self._update_from_mdib_version_group(mdib_version_group)
                for report_part in report.ReportPart:
                    modification_type = report_part.ModificationType
                    if modification_type ==  DescriptionModificationType.CREATE:
                        for descriptor_container in report_part.Descriptor:
                            self.descriptions.add_object(descriptor_container)
                            self._logger.debug('process_incoming_descriptors: created description "{}" (parent="{}")',
                                               descriptor_container.Handle, descriptor_container.parent_handle)
                            new_descriptor_by_handle[descriptor_container.Handle] = descriptor_container
                        for state_container in report_part.State:
                            self._set_descriptor_container_reference(state_container)
                            multi_key(state_container).add_object_no_lock(state_container)
                    elif modification_type == DescriptionModificationType.UPDATE:
                        updated_descriptor_containers = report_part.Descriptor
                        updated_state_containers = report_part.State
                        for descriptor_container in updated_descriptor_containers:
                            self._logger.info('process_incoming_descriptors: update descriptor "{}" (parent="{}")',
                                              descriptor_container.Handle, descriptor_container.parent_handle)
                            old_container = self.descriptions.handle.get_one(descriptor_container.Handle,
                                                                             allow_none=True)
                            if old_container is None:
                                self._logger.error(
                                    'process_incoming_descriptors: got update of descriptor "{}", but it did not exist in mdib!',
                                    descriptor_container.Handle)
                            else:
                                old_container.update_from_other_container(descriptor_container)
                            updated_descriptor_by_handle[descriptor_container.Handle] = descriptor_container
                            # if this is a context descriptor, delete all associated states that are not in
                            # state_containers list
                            if descriptor_container.is_context_descriptor:
                                updated_handles = {s.Handle for s in updated_state_containers
                                                   if
                                                   s.descriptorHandle == descriptor_container.Handle}  # set comprehension
                                my_handles = {s.Handle for s in self.context_states.descriptorHandle.get(
                                    descriptor_container.Handle, [])}  # set comprehension
                                to_be_deleted = my_handles - updated_handles
                                for handle in to_be_deleted:
                                    state = self.context_states.handle.get_one(handle)
                                    self.context_states.remove_object_no_lock(state)
                        for state_container in updated_state_containers:
                            my_multi_key = multi_key(state_container)

                            if state_container.is_context_state:
                                old_state_container = my_multi_key.handle.get_one(state_container.Handle,
                                                                                  allow_none=True)
                            else:
                                old_state_container = my_multi_key.descriptorHandle.get_one(
                                    state_container.DescriptorHandle, allow_none=True)
                                if old_state_container is None:
                                    self._logger.error(
                                        'process_incoming_descriptors: got update of state "{}" , but it did not exist in mdib!',
                                        state_container.DescriptorHandle)
                            if old_state_container is not None:
                                old_state_container.update_from_other_container(state_container)
                                my_multi_key.update_object(old_state_container)

                    elif modification_type == DescriptionModificationType.DELETE:
                        deleted_descriptor_containers = report_part.Descriptor
                        deleted_state_containers = report_part.State
                        for descriptor_container in deleted_descriptor_containers:
                            self._logger.debug('process_incoming_descriptors: remove descriptor "{}" (parent="{}")',
                                               descriptor_container.Handle, descriptor_container.parent_handle)
                            self.rm_descriptor_by_handle(
                                descriptor_container.Handle)  # handling of self.deleted_descriptor_by_handle inside called method
                        for state_container in deleted_state_containers:
                            multi_key(state_container).remove_object_no_lock(state_container)
                    else:
                        raise ValueError(
                            f'unknown modification type {modification_type} in description modification report')

        finally:
            self.description_modifications = report  # update observable for complete report
            # update observables for every report part separately
            if new_descriptor_by_handle:
                self.new_descriptors_by_handle = new_descriptor_by_handle
            if updated_descriptor_by_handle:
                self.updated_descriptors_by_handle = updated_descriptor_by_handle


    def _has_new_state_usable_state_version(self, old_state_container, new_state_container,
                                            report_name, is_buffered_report):
        """
        compare state versions old vs new
        :param old_state_container:
        :param new_state_container:
        :param report_name: used for logging
        :return: True if new state is ok for mdib , otherwise False
        """
        diff = int(new_state_container.StateVersion) - int(old_state_container.StateVersion)
        # diff == 0 can happen if there is only a descriptor version update
        if diff == 1:  # this is the perfect version
            return True
        if diff > 1:
            self._logger.error('{}: missed {} states for state DescriptorHandle={} ({}->{})',
                               report_name,
                               diff - 1, old_state_container.DescriptorHandle,
                               old_state_container.StateVersion, new_state_container.StateVersion)
            return True  # the new version is newer, therefore it can be added to mdib
        if diff < 0:
            if not is_buffered_report:
                self._logger.error(
                    '{}: reduced state version for state DescriptorHandle={} ({}->{}) ',
                    report_name, old_state_container.DescriptorHandle,
                    old_state_container.StateVersion, new_state_container.StateVersion)
            return False
        diffs = old_state_container.diff(new_state_container)  # compares all xml attributes
        if diffs:
            self._logger.error(
                '{}: repeated state version {} for state {}, DescriptorHandle={}, but states have different data:{}',
                report_name, old_state_container.StateVersion, old_state_container.__class__.__name__,
                old_state_container.DescriptorHandle, diffs)
        return False
