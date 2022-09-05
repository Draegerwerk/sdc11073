import copy
import time
import traceback
from collections import deque
from collections import namedtuple
from concurrent import futures
from statistics import mean, stdev
from threading import Lock

from . import mdibbase
from .. import loghelper
from .. import namespaces
from .. import observableproperties as properties
from .. import pmtypes

_global_nsmap = namespaces.nsmap

PROFILING = False
if PROFILING:
    import cProfile
    import pstats
    from io import StringIO

LOG_WF_AGE_INTERVAL = 30  # how often a log message is written with mean and stdef of waveforms age
AGE_CALC_SAMPLES_COUNT = 100  # amount of data for wf mean age and stdev calculation

A_NO_LOG = 0
A_OUT_OF_RANGE = 1
A_STILL_OUT_OF_RANGE = 2
A_BACK_IN_RANGE = 3


class DeterminationTimeWarner:
    """A Helper to reduce log warnings regarding determination time."""
    ST_IN_RANGE = 0
    ST_OUT_OF_RANGE = 1
    result_lookup = {
        # (last, current) :  (action, shall_repeat)
        (ST_IN_RANGE, ST_IN_RANGE): (A_NO_LOG, False),
        (ST_IN_RANGE, ST_OUT_OF_RANGE): (A_OUT_OF_RANGE, False),
        (ST_OUT_OF_RANGE, ST_OUT_OF_RANGE): (A_STILL_OUT_OF_RANGE, True),
        (ST_OUT_OF_RANGE, ST_IN_RANGE): (A_BACK_IN_RANGE, False)
    }

    def __init__(self, repeat_period=30):
        self.repeat_period = repeat_period
        self._last_log_time = 0
        self.last_state = self.ST_IN_RANGE

    def get_out_of_determination_time_log_state(self, min_age, max_age, warn_limit):
        """
        @return: one of above constants
        """
        now = time.time()
        if min_age < -warn_limit or max_age > warn_limit:
            current_state = self.ST_OUT_OF_RANGE
        else:
            current_state = self.ST_IN_RANGE
        action, shall_repeat = self.result_lookup[(self.last_state, current_state)]
        if self.last_state != current_state:
            # a state transition
            self.last_state = current_state
            self._last_log_time = now
            return action
        # no state transition, but might need repeated logging
        if shall_repeat and now - self._last_log_time >= self.repeat_period:
            self._last_log_time = now
            return action
        return A_NO_LOG


_AgeData = namedtuple('_AgeData', 'mean_age stdev min_age max_age')


class ClientRtBuffer:
    """Collects data of one real time stream."""

    def __init__(self, sample_period, max_samples):
        """
        :param sample_period: float value, in seconds.
                              When an incoming real time sample array is split into single RtSampleContainers, this is used to calculate the individual time stamps.
                              Value can be zero if correct value is not known. In this case all Containers will have the observation time of the sample array.
        :param max_samples: integer, max. length of self.rtdata
        """
        self.rt_data = deque(maxlen=max_samples)
        self.sample_period = sample_period
        self._max_samples = max_samples
        self._logger = loghelper.get_logger_adapter('sdc.client.mdib.rt')
        self._lock = Lock()
        self.last_sc = None  # last statecontainer that was handled
        self._age_of_data_list = deque(
            maxlen=AGE_CALC_SAMPLES_COUNT)  # used to calculate average age of samples when received
        self._reported_min_age = None
        self._reported_max_age = None

    def mk_rtsample_containers(self, realtime_sample_array_container):
        """

        :param realtime_sample_array_container: a RealTimeSampleArrayMetricStateContainer instance
        :return: a list of mdibbase.RtSampleContainer
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
        rtsample_containers = []
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
                rtsample_containers.append(mdibbase.RtSampleContainer(sample, rt_sample_time,
                                                                      metric_value.MetricQuality.Validity,
                                                                      applied_annotations))
        return rtsample_containers

    def add_rt_sample_containers(self, rt_sample_containers):
        """
        Updates self.rt_data with the new rt_sample_containers
        :param rt_sample_containers: a list of mdibbase.RtSampleContainer
        :return: None
        """
        if not rt_sample_containers:
            return
        with self._lock:
            self.rt_data.extend(rt_sample_containers)
            # use time of youngest sample, this is the best value for indication of delays
            self._age_of_data_list.append(time.time() - rt_sample_containers[-1].determination_time)
        try:
            self._reported_min_age = min(self._age_of_data_list[-1], self._reported_min_age)
        except TypeError:
            self._reported_min_age = self._age_of_data_list[-1]
        try:
            self._reported_max_age = max(self._age_of_data_list[-1], self._reported_min_age)
        except TypeError:
            self._reported_max_age = self._age_of_data_list[-1]

    def read_rt_data(self):
        """ This read method consumes all data in buffer.
        @return: a list of RtSampleContainer objects"""
        with self._lock:
            ret = copy.copy(self.rt_data)
            self.rt_data.clear()
        return ret

    def get_age_stdev(self):
        with self._lock:
            min_value, self._reported_min_age = self._reported_min_age, None
            max_value, self._reported_max_age = self._reported_max_age, None
            mean_data = 0 if len(self._age_of_data_list) == 0 else mean(self._age_of_data_list)
            std_deviation = 0 if len(self._age_of_data_list) < 2 else stdev(self._age_of_data_list)
            return _AgeData(mean_data, std_deviation, min_value or 0, max_value or 0)


_BufferedNotification = namedtuple('_BufferedNotification', 'report handler')


class ClientMdibContainer(mdibbase.MdibContainer):
    """ This mdib is meant to be read-only.
    Only update source is an SdcClient."""

    DETERMINATIONTIME_WARN_LIMIT = 1.0  # in seconds
    MDIB_VERSION_CHECK_DISABLED = False  # for testing purpose you can disable checking of mdib version, so that every notification is accepted.

    # INITIAL_NOTIFICATION_BUFFERING setting determines how incoming notifications are handled between start of
    # subscription ond handling of GetMib Response.
    # INITIAL_NOTIFICATION_BUFFERING = False: the response for the first incoming notification is answered after the getmdib is done.
    # INITIAL_NOTIFICATION_BUFFERING = True:  responses are sent immediately and first notifications are buffered.
    INITIAL_NOTIFICATION_BUFFERING = True

    def __init__(self, sdc_client, max_realtime_samples=100):
        """

        :param sdc_client: a SdcClient instance
        :param max_realtime_samples: determines how many real time samples are stored per RealtimeSampleArray
        """
        super().__init__(sdc_client.sdc_definitions)
        self._logger = loghelper.get_logger_adapter('sdc.client.mdib', sdc_client.log_prefix)
        self._sdc_client = sdc_client
        self._is_initialized = False
        self.rt_buffers = {}  # key  is a handle, value is a ClientRtBuffer
        self._max_realtime_samples = max_realtime_samples
        self._last_wf_age_log = time.time()
        if PROFILING:
            self.prof = cProfile.Profile()

        self._context_mdib_version = None
        self._msg_reader = sdc_client.msg_reader
        # a buffer for notifications that are received before initial getmdib is done
        self._buffered_notifications = []
        self._buffered_notifications_lock = Lock()
        self.waveform_time_warner = DeterminationTimeWarner()
        self.metric_time_warner = DeterminationTimeWarner()
        self._sequence_id_changed_flag = False

    def init_mdib(self):
        """
        Binds own notification handlers to observables of sdc client and calls GetMdib.
        Client mdib is initialized from GetMdibResponse, and from then on updated from incoming notifications.
        :return:
        """
        if self._is_initialized:
            raise RuntimeError('ClientMdibContainer is already initialized')
        # first start receiving notifications, then call get_mdib.
        # Otherwise we might miss notifications.
        self._bind_to_client_observables()
        self.reload_all()
        self._sdc_client._register_mdib(self)  # pylint: disable=protected-access
        self._logger.info('initializing mdib done')

    def reload_all(self):
        """
        Deletes all data and reloads everything. Useful e.g. after sequence id change.
        THis is not called automatically, the application has to take care.
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

        mdib_version = response.mdib_version
        sequence_id = response.sequence_id
        if mdib_version is not None:
            self.mdib_version = mdib_version
            self._logger.info('setting initial mdib version to {}', mdib_version)
        else:
            self._logger.warn('found no mdib version in GetMdib response, assuming "0"')
            self.mdib_version = 0
        self.sequence_id = sequence_id
        self._logger.info('setting sequence Id to {}', sequence_id)

        # retrieve context states only if there were none in mdib
        if len(self.context_states.objects) == 0:
            self._get_context_states()
        else:
            self._logger.info('found context states in GetMdib Result, will not call getContextStates')

        # process buffered notifications
        with self._buffered_notifications_lock:
            for buffered_report in self._buffered_notifications:
                buffered_report.handler(buffered_report.report, is_buffered_report=True)
            del self._buffered_notifications[:]
            self._is_initialized = True

    def wait_metric_matches(self, handle, matches_func, timeout):
        """ wait until a matching metric has been received. The matching is defined by the handle of the metric
        and the result of a matching function. If the matching function returns true, this function returns.
        :param handle: The handle string of the metric of interest.
        :param matches_func: a callable, argument is the current state with matching handle. Can be None, in that case every state matches
        Example:
            expected = 42
            def isMatchingValue(state):
                if state.MetricValue is None:
                    return False

                found_value = state.MetricValue.Value
                return [expected] == found_value
        :param timeout: timeout in seconds
        @return: the matching state. In case of a timeout it raises a TimeoutError exception.
        """
        fut = futures.Future()

        # define a callback function that sets value of fut
        def on_metrics_by_handle(metrics_by_handle):
            metric = metrics_by_handle.get(handle)
            if metric is not None:
                if matches_func is None or matches_func(metric):
                    fut.set_result(metric)

        try:
            properties.bind(self, metrics_by_handle=on_metrics_by_handle)
            begin = time.monotonic()
            ret = fut.result(timeout)
            self._logger.debug('wait_metric_matches: got result after {:.2f} seconds', time.monotonic() - begin)
            return ret
        finally:
            properties.unbind(self, metrics_by_handle=on_metrics_by_handle)

    def mk_proposed_state(self, descriptor_handle, copy_current_state=True, handle=None):
        """ Create a new state that can be used as proposed state in according operations.
        The new state is not part of mdib!

        :param descriptor_handle: the descriptor
        :param copy_current_state: if True, all members of existing state will be copied to new state
        :param handle: if this is a multi state class, then this is the handle of the existing state that shall be used for copy.
        :return:
        """
        descr = self.descriptions.handle.get_one(descriptor_handle)
        new_state = self.mk_state_container_from_descriptor(descr)
        if copy_current_state:
            lookup = self.context_states if new_state.isContextState else self.states
            if new_state.isMultiState:
                if handle is None:  # new state
                    return new_state
                old_state = lookup.handle.get_one(handle)
            else:
                old_state = lookup.descriptorHandle.get_one(descriptor_handle)
            new_state.update_from_other_container(old_state)
        return new_state

    def _buffer_notification(self, report, func):
        """
        Write notification to a temporary buffer, as long as mdib is not initialized.
        :param report: the report
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
                self._buffered_notifications.append(_BufferedNotification(report, func))
                return True
            return False

    def _sync_context_states(self):
        """This method requests all context states from device and deletes all local context states that are not
        available in response from Device."""
        try:
            self._logger.info('_sync_context_states called')
            context_service = self._sdc_client.client('Context')
            # mdib_version, sequence_id, context_state_containers = context_service.get_context_states()
            response = context_service.get_context_states()
            context_state_containers = response.result

            devices_context_state_handles = [s.Handle for s in context_state_containers]
            with self.context_states.lock:
                for obj in self.context_states.objects:
                    if obj.Handle not in devices_context_state_handles:
                        self.context_states.remove_object_no_lock((obj))
        except Exception:
            self._logger.error(traceback.format_exc())

    def _get_context_states(self, handles=None):
        try:
            self._logger.debug('new Query, handles={}', handles)
            time.sleep(0.001)
            context_service = self._sdc_client.client('Context')
            self._logger.info('requesting context states...')
            # mdib_version, sequence_id, context_state_containers = context_service.get_context_states(handles)
            response = context_service.get_context_states(handles)
            mdib_version = response.mdib_version
            # sequence_id = response.sequence_id
            context_state_containers = response.result

            self._context_mdib_version = mdib_version
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

    def _bind_to_client_observables(self):
        # get notifications from sdcClient
        if PROFILING:
            properties.bind(self._sdc_client, waveform_report=self._on_waveform_report_profiled)
        else:
            properties.bind(self._sdc_client, waveform_report=self._on_waveform_report)
        properties.bind(self._sdc_client, episodic_metric_report=self._on_episodic_metric_report)
        properties.bind(self._sdc_client, episodic_alert_report=self._on_episodic_alert_report)
        properties.bind(self._sdc_client, episodic_context_report=self._on_episodic_context_report)
        properties.bind(self._sdc_client, episodic_component_report=self._on_episodic_component_report)
        properties.bind(self._sdc_client, description_modification_report=self._on_description_modification_report)
        properties.bind(self._sdc_client, episodic_operational_state_report=self._on_operational_state_report)

    def _can_accept_mdib_version(self, log_prefix, new_mdib_version):
        if self.MDIB_VERSION_CHECK_DISABLED:
            return True
        if new_mdib_version is None:
            self._logger.error('{}: could not check MdibVersion!', log_prefix)
        else:
            # log deviations from expected mdib versionb
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

    def _on_episodic_metric_report(self, received_message_data, is_buffered_report=False):
        if not is_buffered_report and self._buffer_notification(received_message_data, self._on_episodic_metric_report):
            return

        now = time.time()
        metrics_by_handle = {}
        max_age = 0
        min_age = 0
        state_containers = self._msg_reader.read_episodic_metric_report(received_message_data)
        try:
            with self.mdib_lock:
                new_mdib_version = received_message_data.mdib_version
                if not self._can_accept_mdib_version('_on_episodic_metric_report', new_mdib_version):
                    return
                if self._sequence_id_changed(received_message_data.sequence_id):
                    return
                self.mdib_version = new_mdib_version
                for state_container in state_containers:
                    desc_h = state_container.DescriptorHandle
                    old_state_container = self.states.descriptorHandle.get_one(desc_h, allow_none=True)
                    if old_state_container is not None:
                        if self._has_new_state_usable_state_version(old_state_container, state_container,
                                                                    'EpisodicMetricReport',
                                                                    is_buffered_report):
                            old_state_container.update_from_other_container(state_container)
                            self.states.update_object(old_state_container)
                            metrics_by_handle[desc_h] = old_state_container
                    else:
                        self._logger.error('_on_episodic_metric_report: got a new state {}',
                                           state_container.DescriptorHandle)
                        self._set_descriptor_container_reference(state_container)
                        self.states.add_object(state_container)
                        metrics_by_handle[desc_h] = state_container

                    if state_container.MetricValue is not None:
                        # BICEPS: While Validity is "Ong" or "NA", the enclosing METRIC value SHALL not possess a
                        # determined value.
                        # Also ignore determination time if measurement is invalid or not active.
                        if state_container.ActivationState == pmtypes.ComponentActivation.ON and \
                                state_container.MetricValue.MetricQuality.Validity not in [
                            pmtypes.MeasurementValidity.INVALID,
                            pmtypes.MeasurementValidity.NA,
                            pmtypes.MeasurementValidity.MEASUREMENT_ONGOING]:
                            determination_time = state_container.MetricValue.DeterminationTime
                            if determination_time is None:
                                self._logger.warn(
                                    '_on_episodic_metric_report: metric {} version {} has no DeterminationTime',
                                    desc_h, state_container.StateVersion)
                            else:
                                age = now - determination_time
                                min_age = min(min_age, age)
                                max_age = max(max_age, age)
            shall_log = self.metric_time_warner.get_out_of_determination_time_log_state(min_age, max_age,
                                                                                        self.DETERMINATIONTIME_WARN_LIMIT)
            if shall_log == A_OUT_OF_RANGE:
                self._logger.warn(
                    '_on_episodic_metric_report mdib_version {}: age of metrics outside limit of {} sec.: max, min = {:03f}, {:03f}',
                    new_mdib_version, self.DETERMINATIONTIME_WARN_LIMIT, max_age, min_age)
            elif shall_log == A_STILL_OUT_OF_RANGE:
                self._logger.warn(
                    '_on_episodic_metric_report mdib_version {}: age of metrics still outside limit of {} sec.: max, min = {:03f}, {:03f}',
                    new_mdib_version, self.DETERMINATIONTIME_WARN_LIMIT, max_age, min_age)
            elif shall_log == A_BACK_IN_RANGE:
                self._logger.info(
                    '_on_episodic_metric_report mdib_version {}: age of metrics back in limit of {} sec.: max, min = {:03f}, {:03f}',
                    new_mdib_version, self.DETERMINATIONTIME_WARN_LIMIT, max_age, min_age)
        finally:
            self.metrics_by_handle = metrics_by_handle  # used by wait_metric_matches method

    def _on_episodic_alert_report(self, received_message_data, is_buffered_report=False):
        if not is_buffered_report and self._buffer_notification(received_message_data, self._on_episodic_alert_report):
            return

        alert_by_handle = {}
        state_containers = self._msg_reader.read_episodic_alert_report(received_message_data)
        self._logger.debug('_on_episodic_alert_report: received {} alerts', len(state_containers))
        try:
            with self.mdib_lock:
                new_mdib_version = received_message_data.mdib_version
                if not self._can_accept_mdib_version('_on_episodic_alert_report', new_mdib_version):
                    return
                if self._sequence_id_changed(received_message_data.sequence_id):
                    return
                self.mdib_version = new_mdib_version
                for state_container in state_containers:
                    old_state_container = self.states.descriptorHandle.get_one(state_container.DescriptorHandle,
                                                                               allow_none=True)
                    if old_state_container is not None:
                        if self._has_new_state_usable_state_version(old_state_container, state_container,
                                                                    'EpisodicAlertReport',
                                                                    is_buffered_report):
                            old_state_container.update_from_other_container(state_container)
                            self.states.update_object(old_state_container)
                            alert_by_handle[old_state_container.DescriptorHandle] = old_state_container
                    else:
                        self._logger.error('_on_episodic_alert_report: got a new state {}',
                                           state_container.DescriptorHandle)
                        self._set_descriptor_container_reference(state_container)
                        self.states.add_object(state_container)
                        alert_by_handle[state_container.DescriptorHandle] = state_container
        finally:
            self.alert_by_handle = alert_by_handle  # update observable

    def _on_operational_state_report(self, received_message_data, is_buffered_report=False):
        if not is_buffered_report and self._buffer_notification(received_message_data,
                                                                self._on_operational_state_report):
            return
        operation_by_handle = {}
        all_operation_state_containers = self._msg_reader.read_operational_state_report(received_message_data)
        try:
            with self.mdib_lock:
                new_mdib_version = received_message_data.mdib_version
                if not self._can_accept_mdib_version('_on_operational_state_report', new_mdib_version):
                    return
                if self._sequence_id_changed(received_message_data.sequence_id):
                    return
                self.mdib_version = new_mdib_version
                for state_container in all_operation_state_containers:
                    old_state_container = self.states.descriptorHandle.get_one(state_container.descriptorHandle,
                                                                               allow_none=True)
                    if old_state_container is not None:
                        if self._has_new_state_usable_state_version(old_state_container, state_container,
                                                                    'OperationalStateReport', is_buffered_report):
                            old_state_container.update_from_other_container(state_container)
                            self.states.update_object(old_state_container)
                            operation_by_handle[old_state_container.descriptorHandle] = old_state_container
                    else:
                        self._logger.error('_on_operational_state_report: got a new state {}',
                                           state_container.DescriptorHandle)
                        self._set_descriptor_container_reference(state_container)
                        self.states.add_object(state_container)
                        operation_by_handle[state_container.descriptorHandle] = state_container
        finally:
            self.operation_by_handle = operation_by_handle

    def _on_waveform_report_profiled(self, report_node):
        self.prof.enable()
        self._on_waveform_report(report_node)
        self.prof.disable()
        str_io = StringIO()
        stats = pstats.Stats(self.prof, stream=str_io).sort_stats('cumulative')
        stats.print_stats(30)
        print(str_io.getvalue())
        print(f'total number of states: {len(self.states._objects)}')  # pylint:disable=protected-access
        print(f'total number of objIds: {len(self.states._object_ids)}')  # pylint:disable=protected-access
        for name, refs in self.states._object_ids.items():  # pylint:disable=protected-access
            if len(refs) > 50:
                print(f'object {name} has {len(refs)} idx references, {refs}')

    def _on_waveform_report(self, received_message_data, is_buffered_report=False):
        # pylint:disable=too-many-locals
        if not is_buffered_report and self._buffer_notification(received_message_data, self._on_waveform_report):
            return
        waveform_by_handle = {}
        waveform_age = {}  # collect age of all waveforms in this report, and make one report if age is above warn limit (instead of multiple)
        rt_sample_array_containers = self._msg_reader.read_waveform_report(received_message_data)
        self._logger.debug('_on_waveform_report: {} waveforms received', len(rt_sample_array_containers))
        try:
            with self.mdib_lock:
                new_mdib_version = received_message_data.mdib_version
                if not self._can_accept_mdib_version('_on_waveform_report', new_mdib_version):
                    return
                if self._sequence_id_changed(received_message_data.sequence_id):
                    return
                self.mdib_version = new_mdib_version
                for state_container in rt_sample_array_containers:
                    d_handle = state_container.DescriptorHandle
                    old_state_container = self.states.descriptorHandle.get_one(d_handle, allow_none=True)
                    if old_state_container is not None:
                        if self._has_new_state_usable_state_version(old_state_container, state_container,
                                                                    'WaveformReport', is_buffered_report):
                            # update old state container from new one
                            old_state_container.update_from_other_container(state_container)
                            self.states.update_object(old_state_container)
                            waveform_by_handle[d_handle] = old_state_container
                        descriptor_container = old_state_container.descriptor_container
                    else:
                        self._logger.error('_on_waveform_report: got a new state {}', state_container.DescriptorHandle)
                        self._set_descriptor_container_reference(state_container)
                        self.states.add_object(state_container)
                        waveform_by_handle[d_handle] = state_container
                        descriptor_container = state_container.descriptor_container
                    # add to Waveform Buffer
                    rt_buffer = self.rt_buffers.get(d_handle)
                    if rt_buffer is None:
                        sample_period = 0  # default
                        if descriptor_container is not None:
                            # read sample period
                            sample_period = descriptor_container.SamplePeriod or 0
                        rt_buffer = ClientRtBuffer(sample_period=sample_period, max_samples=self._max_realtime_samples)
                        self.rt_buffers[d_handle] = rt_buffer
                    state_containers = rt_buffer.mk_rtsample_containers(state_container)
                    rt_buffer.add_rt_sample_containers(state_containers)

                    # check age
                    if len(state_containers) > 0:
                        waveform_age[d_handle] = state_containers[-1].age

            if len(waveform_age) > 0:
                min_age = min(waveform_age.values())
                max_age = max(waveform_age.values())
                shall_log = self.waveform_time_warner.get_out_of_determination_time_log_state(
                    min_age, max_age, self.DETERMINATIONTIME_WARN_LIMIT)
                if shall_log != A_NO_LOG:
                    tmp = ', '.join(f'"{k}":{v:.3f}sec.' for k, v in waveform_age.items())
                    if shall_log == A_OUT_OF_RANGE:
                        self._logger.warn(
                            '_on_waveform_report mdib_version {}: age of samples outside limit of {} sec.: age={}!',
                            new_mdib_version, self.DETERMINATIONTIME_WARN_LIMIT, tmp)
                    elif shall_log == A_STILL_OUT_OF_RANGE:
                        self._logger.warn(
                            '_on_waveform_report mdib_version {}: age of samples still outside limit of {} sec.: age={}!',
                            new_mdib_version, self.DETERMINATIONTIME_WARN_LIMIT, tmp)
                    elif shall_log == A_BACK_IN_RANGE:
                        self._logger.info(
                            '_on_waveform_report mdib_version {}: age of samples back in limit of {} sec.: age={}',
                            new_mdib_version, self.DETERMINATIONTIME_WARN_LIMIT, tmp)
            if LOG_WF_AGE_INTERVAL:
                now = time.time()
                if now - self._last_wf_age_log >= LOG_WF_AGE_INTERVAL:
                    age_data = self.get_wf_age_stdev()
                    self._logger.info('waveform mean age={:.1f}ms., stdev={:.2f}ms. min={:.1f}ms., max={}',
                                      age_data.mean_age * 1000., age_data.stdev * 1000.,
                                      age_data.min_age * 1000., age_data.max_age * 1000.)
                    self._last_wf_age_log = now
        finally:
            self.waveform_by_handle = waveform_by_handle

    def _on_episodic_context_report(self, received_message_data, is_buffered_report=False):
        if not is_buffered_report and self._buffer_notification(received_message_data,
                                                                self._on_episodic_context_report):
            return
        context_by_handle = {}
        state_containers = self._msg_reader.read_episodic_context_report(received_message_data)
        try:
            with self.mdib_lock:
                new_mdib_version = received_message_data.mdib_version
                if not self._can_accept_mdib_version('_on_episodic_context_report', new_mdib_version):
                    return
                if self._sequence_id_changed(received_message_data.sequence_id):
                    return
                self.mdib_version = new_mdib_version
                for state_container in state_containers:
                    old_state_container = self.context_states.handle.get_one(state_container.Handle,
                                                                             allow_none=True)
                    if old_state_container is not None:
                        if self._has_new_state_usable_state_version(old_state_container, state_container,
                                                                    'EpisodicContextReport',
                                                                    is_buffered_report):
                            old_state_container.update_from_other_container(state_container)
                            self.context_states.update_object(old_state_container)
                            context_by_handle[old_state_container.Handle] = old_state_container
                    else:
                        self._set_descriptor_container_reference(state_container)
                        self.context_states.add_object(state_container)
                        self._logger.info(
                            '_on_episodic_context_report: new context state handle = {} Descriptor Handle={} Assoc={}, Validators={}',
                            state_container.Handle, state_container.DescriptorHandle,
                            state_container.ContextAssociation, state_container.Validator)
                        context_by_handle[state_container.Handle] = state_container
        finally:
            self.context_by_handle = context_by_handle

    def _on_episodic_component_report(self, received_message_data, is_buffered_report=False):
        """The EpisodicComponentReport is sent if at least one property of at least one component state has changed
        and SHOULD contain only the changed component states.
        Components are MDSs, VMDs, Channels. Not metrics and alarms
        """
        if not is_buffered_report and self._buffer_notification(received_message_data,
                                                                self._on_episodic_component_report):
            return
        component_by_handle = {}
        state_containers = self._msg_reader.read_episodic_component_report(received_message_data)
        try:
            with self.mdib_lock:
                new_mdib_version = received_message_data.mdib_version
                if not self._can_accept_mdib_version('_on_episodic_component_report', new_mdib_version):
                    return
                if self._sequence_id_changed(received_message_data.sequence_id):
                    return
                self.mdib_version = new_mdib_version
                for state_container in state_containers:
                    desc_h = state_container.DescriptorHandle
                    old_state_container = self.states.descriptorHandle.get_one(desc_h, allow_none=True)
                    if old_state_container is not None:
                        if self._has_new_state_usable_state_version(old_state_container, state_container,
                                                                    'EpisodicComponentReport', is_buffered_report):
                            self._logger.info(
                                '_on_episodic_component_report: updated component state, handle="{}" DescriptorVersion={}',
                                desc_h, state_container.DescriptorVersion)
                            old_state_container.update_from_other_container(state_container)
                            self.states.update_object(old_state_container)
                            component_by_handle[old_state_container.DescriptorHandle] = old_state_container
                    else:
                        self._logger.error('_on_episodic_component_report: got a new state {}',
                                           state_container.DescriptorHandle)
                        self._set_descriptor_container_reference(state_container)
                        self.states.add_object(state_container)
                        component_by_handle[state_container.descriptorHandle] = state_container
        finally:
            self.component_by_handle = component_by_handle

    def _on_description_modification_report(self, received_message_data, is_buffered_report=False):
        """The DescriptionModificationReport is sent if at least one Descriptor has been created, updated or deleted during runtime.
        It consists of 1...n DescriptionModificationReportParts.
        """
        if not is_buffered_report and self._buffer_notification(received_message_data,
                                                                self._on_description_modification_report):
            return
        descriptions_lookup_list = self._msg_reader.read_description_modification_report(received_message_data)
        with self.mdib_lock:
            new_mdib_version = received_message_data.mdib_version
            if not self._can_accept_mdib_version('_on_description_modification_report', new_mdib_version):
                return
            if self._sequence_id_changed(received_message_data.sequence_id):
                return
            self.mdib_version = new_mdib_version
            for descriptions_lookup in descriptions_lookup_list:
                new_descriptor_by_handle = {}
                updated_descriptor_by_handle = {}

                # -- new --
                new_descriptor_containers, new_state_containers = descriptions_lookup[
                    pmtypes.DescriptionModificationTypes.CREATE]
                for descriptor_container in new_descriptor_containers:
                    self.descriptions.add_object(descriptor_container)
                    self._logger.debug('_on_description_modification_report: created description "{}" (parent="{}")',
                                       descriptor_container.Handle, descriptor_container.parent_handle)
                    new_descriptor_by_handle[descriptor_container.Handle] = descriptor_container
                for state_container in new_state_containers:
                    self._set_descriptor_container_reference(state_container)
                    # determine multikey
                    if state_container.isContextState:
                        multikey = self.context_states
                    else:
                        multikey = self.states
                    multikey.add_object(state_container)

                # -- deleted --
                deleted_descriptor_containers, deleted_state_containers = descriptions_lookup[
                    pmtypes.DescriptionModificationTypes.DELETE]
                for descriptor_container in deleted_descriptor_containers:
                    self._logger.debug('_on_description_modification_report: remove descriptor "{}" (parent="{}")',
                                       descriptor_container.Handle, descriptor_container.parent_handle)
                    self.rm_descriptor_by_handle(
                        descriptor_container.Handle)  # handling of self.deleted_descriptor_by_handle inside called method

                # -- updated --
                updated_descriptor_containers, updated_state_containers = descriptions_lookup[
                    pmtypes.DescriptionModificationTypes.UPDATE]
                for descriptor_container in updated_descriptor_containers:
                    self._logger.info('_on_description_modification_report: update descriptor "{}" (parent="{}")',
                                      descriptor_container.Handle, descriptor_container.parent_handle)
                    old_container = self.descriptions.handle.get_one(descriptor_container.Handle, allow_none=True)
                    if old_container is None:
                        self._logger.error('got update of descriptor "{}" , but it did not exist in mdib!',
                                          descriptor_container.Handle)
                    else:
                        old_container.update_from_other_container(descriptor_container)
                    updated_descriptor_by_handle[descriptor_container.Handle] = descriptor_container
                    # if this is a context descriptor, delete all associated states that are not in
                    # state_containers list
                    if descriptor_container.is_context_descriptor:
                        updated_handles = {s.Handle for s in updated_state_containers
                                           if s.descriptorHandle == descriptor_container.Handle}  # set comprehension
                        my_handles = {s.Handle for s in self.context_states.descriptorHandle.get(
                            descriptor_container.Handle, [])}  # set comprehension
                        to_be_deleted = my_handles - updated_handles
                        for handle in to_be_deleted:
                            state = multikey.handle.get_one(handle)
                            self.context_states.remove_object_no_lock(state)
                for state_container in updated_state_containers:
                    # determine multikey
                    if state_container.isContextState:
                        multikey = self.context_states
                        old_state_container = multikey.handle.get_one(state_container.Handle, allow_none=True)
                    else:
                        multikey = self.states
                        old_state_container = multikey.descriptorHandle.get_one(
                            state_container.DescriptorHandle, allow_none=True)
                        if old_state_container is None:
                            self._logger.error('got update of state "{}" , but it did not exist in mdib!',
                                               state_container.DescriptorHandle)
                    if old_state_container is not None:
                        old_state_container.update_from_other_container(state_container)
                        multikey.update_object(old_state_container)

                self.description_modifications = descriptions_lookup_list # update observable for complete report
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
                               diff - 1, old_state_container.descriptorHandle,
                               old_state_container.StateVersion, new_state_container.StateVersion)
            return True  # the new version is newer, therefore it can be added to mdib
        if diff < 0:
            if not is_buffered_report:
                self._logger.error(
                    '{}: reduced state version for state DescriptorHandle={} ({}->{}) ',
                    report_name, old_state_container.descriptorHandle,
                    old_state_container.StateVersion, new_state_container.StateVersion)
            return False
        diffs = old_state_container.diff(new_state_container)  # compares all xml attributes
        if diffs:
            self._logger.error(
                '{}: repeated state version {} for state {}, DescriptorHandle={}, but states have different data:{}',
                report_name, old_state_container.StateVersion, old_state_container.__class__.__name__,
                old_state_container.descriptorHandle, diffs)
        return False

    def get_wf_age_stdev(self):
        means = []
        stdevs = []
        mins = []
        maxs = []
        for buf in self.rt_buffers.values():
            age_data = buf.get_age_stdev()
            means.append(age_data.mean_age)
            stdevs.append(age_data.stdev)
            mins.append(age_data.min_age)
            maxs.append(age_data.max_age)
        return _AgeData(mean(means), mean(stdevs), min(mins), max(maxs))
