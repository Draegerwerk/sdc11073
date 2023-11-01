from __future__ import annotations

import time
from collections import deque
from concurrent import futures
from dataclasses import dataclass
from enum import IntEnum
from statistics import mean, stdev
from threading import Lock
from typing import TYPE_CHECKING, Callable, ClassVar

from sdc11073 import observableproperties as properties
from sdc11073.exceptions import ApiUsageError

if TYPE_CHECKING:
    from sdc11073.loghelper import LoggerAdapter
    from sdc11073.pysoap.msgreader import ReceivedMessage

    from .consumermdib import ConsumerMdib
    from .statecontainers import AbstractMetricStateContainer, AbstractStateProtocol




PROFILING = False
if PROFILING:
    import cProfile
    import pstats
    from io import StringIO

class _WarningState(IntEnum):
    A_NO_LOG = 0
    A_OUT_OF_RANGE = 1
    A_STILL_OUT_OF_RANGE = 2
    A_BACK_IN_RANGE = 3

LOG_WF_AGE_INTERVAL = 30  # how often a log message is written with mean and stdev of waveforms age
AGE_CALC_SAMPLES_COUNT = 100  # amount of data for wf mean age and stdev calculation


class DeterminationTimeWarner:
    """A Helper to reduce log warnings regarding determination time."""

    ST_IN_RANGE = 0
    ST_OUT_OF_RANGE = 1
    result_lookup: ClassVar[dict[tuple[int, int], tuple[_WarningState, bool]]] = {
        (ST_IN_RANGE, ST_IN_RANGE): (_WarningState.A_NO_LOG, False),
        (ST_IN_RANGE, ST_OUT_OF_RANGE): (_WarningState.A_OUT_OF_RANGE, False),
        (ST_OUT_OF_RANGE, ST_OUT_OF_RANGE): (_WarningState.A_STILL_OUT_OF_RANGE, True),
        (ST_OUT_OF_RANGE, ST_IN_RANGE): (_WarningState.A_BACK_IN_RANGE, False),
    }

    def __init__(self, repeat_period: int=30):
        """Construct the DeterminationTimeWarner.

        :param repeat_period: period after which an existing warning condition the warning shall be repeated.
        """
        self.repeat_period = repeat_period
        self._last_log_time = 0
        self.last_state = self.ST_IN_RANGE

    def get_out_of_determination_time_log_state(self, min_age: float,
                                                max_age: float,
                                                warn_limit: float) -> _WarningState:
        """:return: one of above constants."""
        now = time.time()
        current_state = self.ST_OUT_OF_RANGE if min_age < -warn_limit or max_age > warn_limit else self.ST_IN_RANGE
        warning_state, shall_repeat = self.result_lookup[(self.last_state, current_state)]
        if self.last_state != current_state:
            # a state transition
            self.last_state = current_state
            self._last_log_time = now
            return warning_state
        # no state transition, but might need repeated logging
        if shall_repeat and now - self._last_log_time >= self.repeat_period:
            self._last_log_time = now
            return warning_state
        return _WarningState.A_NO_LOG


class AgeLogger:
    """A helper that write to lo  if incoming states are too old or in the future."""

    def __init__(self, metric_time_warner: DeterminationTimeWarner,
                 warn_limit: float,
                 log_prefix: str,
                 mdib_version: int):
        self._metric_time_warner = metric_time_warner
        self._warn_limit = warn_limit
        self._log_prefix = log_prefix
        self._mdib_version = mdib_version
        self.age_list: list[float] = []
        self.now = time.time()

    def add_determination_time(self, determination_time: float):
        """Make the determination time an age and add it to self.age_list."""
        self.age_list.append(self.now - determination_time)

    def log_age_warnings(self, logger: LoggerAdapter):
        """Write age warning to logger."""
        if len(self.age_list) == 0:
            return
        min_age = min(self.age_list)
        max_age = max(self.age_list)

        shall_log = self._metric_time_warner.get_out_of_determination_time_log_state(
            min_age, max_age, self._warn_limit)
        if shall_log == _WarningState.A_OUT_OF_RANGE:
            logger.warning(  # noqa: PLE1205
                '{} mdib_version {}: age of states outside limit of {} sec.: max, min = {:03f}, {:03f}',
                self._log_prefix, self._mdib_version, self._warn_limit, max_age, min_age)
        elif shall_log == _WarningState.A_STILL_OUT_OF_RANGE:
            logger.warning( # noqa: PLE1205
                '{} mdib_version {}: age of states still outside limit of {} sec.: max, min = {:03f}, {:03f}',
                self._log_prefix, self._mdib_version, self._warn_limit, max_age, min_age)
        elif shall_log == _WarningState.A_BACK_IN_RANGE:
            logger.info( # noqa: PLE1205
                '{} mdib_version {}: age of states back in limit of {} sec.: max, min = {:03f}, {:03f}',
                self._log_prefix, self._mdib_version, self._warn_limit, max_age, min_age)


@dataclass
class AgeData:
    """Container for some statistical age data."""

    mean_age: float
    stdev: float
    min_age: float
    max_age: float


class AgeStatistics:
    """Keep age data of a single state over time."""

    min_list_lenght_for_statistics = 3
    def __init__(self, entry_count: int | None = None):
        length = entry_count or AGE_CALC_SAMPLES_COUNT
        self._age_of_data_list = deque(
            maxlen=length)  # used to calculate average age of samples when received
        self._lock = Lock()

    def process_state(self, metric_state: AbstractMetricStateContainer):
        """Append age of metric_state to internal list."""
        try:
            age = time.time() - metric_state.MetricValue.DeterminationTime
            with self._lock:
                self._age_of_data_list.append(age)
        except AttributeError as ex:
            if not metric_state.is_metric_state:
                raise ApiUsageError(f'{self.__class__.__name__} can only handle metric states') from ex
            # or state.MetricValue is None or  DeterminationTime is None: ignore this

    def get_age_stdev(self) -> AgeData:
        """Calculate age statistics data."""
        if len(self._age_of_data_list) < self.min_list_lenght_for_statistics:
            return AgeData(0.0, 0.0, 0.0, 0.0)
        with self._lock:
            min_value = min(self._age_of_data_list)
            max_value = max(self._age_of_data_list)
            mean_data = mean(self._age_of_data_list)
            std_deviation = stdev(self._age_of_data_list)
            return AgeData(mean_data, std_deviation, min_value, max_value)


class ConsumerMdibMethods:
    """Extra methods for consumer mdib tht are not core functionality."""

    DETERMINATIONTIME_WARN_LIMIT = 1.0  # in seconds

    def __init__(self, consumer_mdib: ConsumerMdib, logger: LoggerAdapter):
        self._mdib = consumer_mdib
        self._sdc_client = consumer_mdib.sdc_client
        self._msg_reader = self._sdc_client.msg_reader
        self._logger = logger
        self.waveform_time_warner = DeterminationTimeWarner()
        self.metric_time_warner = DeterminationTimeWarner()
        self._last_wf_age_log = time.time()
        if PROFILING:
            self.prof = cProfile.Profile()
        self._age_statistics = {}
        self._calculate_wf_age_stats = False

    def set_calculate_wf_age_stats(self, shall_calculate: bool):
        """Switch calculation of statistice on or off."""
        self._calculate_wf_age_stats = shall_calculate

    def wait_metric_matches(self, handle: str,
                            matches_func: Callable[[AbstractMetricStateContainer], bool],
                            timeout: float) -> AbstractMetricStateContainer:
        """Wait until a matching metric has been received.

        The matching is defined by the handle of the metric
        and the result of a matching function. If the matching function returns true, this function returns.
        :param handle: The handle string of the metric of interest.
        :param matches_func: a callable, argument is the current state with matching handle.
                              Can be None, in that case every state matches
        Example:
            expected = 42
            def isMatchingValue(state):
                if state.MetricValue is None:
                    return False.

                found_value = state.MetricValue.Value
                return [expected] == found_value
        :param timeout: timeout in seconds
        :return: the matching state. In case of a timeout it raises a TimeoutError exception.
        """
        fut = futures.Future()

        # define a callback function that sets value of fut
        def on_metrics_by_handle(metrics_by_handle: dict[str, AbstractMetricStateContainer]):
            metric = metrics_by_handle.get(handle)
            if metric is not None:
                if matches_func is None or matches_func(metric):
                    fut.set_result(metric)

        try:
            properties.bind(self._mdib, metrics_by_handle=on_metrics_by_handle)
            begin = time.monotonic()
            ret = fut.result(timeout)
            self._logger.debug('wait_metric_matches: got result after {:.2f} seconds',  # noqa: PLE1205
                               time.monotonic() - begin)
            return ret
        finally:
            properties.unbind(self._mdib, metrics_by_handle=on_metrics_by_handle)

    def mk_proposed_state(self, descriptor_handle: str,
                          copy_current_state: bool = True,
                          handle: str | None = None) -> AbstractStateProtocol:
        """Create a new state that can be used as proposed state in according operations.

        The new state is not part of mdib!.

        :param descriptor_handle: the descriptor
        :param copy_current_state: if True, all members of existing state will be copied to new state
        :param handle: if this is a multi state class, and the handle is not None,
                      this is the handle of the existing state that shall be used for copy.
        :return: a new state container
        """
        descr = self._mdib.descriptions.handle.get_one(descriptor_handle)
        new_state = self._mdib.data_model.mk_state_container(descr)
        if copy_current_state:
            lookup = self._mdib.context_states if new_state.is_context_state else self._mdib.states
            if new_state.is_multi_state:
                if handle is None:  # new state
                    return new_state
                old_state = lookup.handle.get_one(handle)
            else:
                old_state = lookup.descriptor_handle.get_one(descriptor_handle)
            new_state.update_from_other_container(old_state)
        return new_state

    def sync_context_states(self):
        """Sync context states with provider.

        Requests all context states from device and deletes all local context states that are not
        available in response from Device.
        """
        self._logger.info('_sync_context_states called')
        context_service = self._sdc_client.client('Context')
        response = context_service.get_context_states()
        context_state_containers = response.result.ContextState

        devices_context_state_handles = [s.Handle for s in context_state_containers]
        with self._mdib.context_states.lock:
            for obj in self._mdib.context_states.objects:
                if obj.Handle not in devices_context_state_handles:
                    self._mdib.context_states.remove_object_no_lock(obj)

    def bind_to_client_observables(self):
        """Connect the mdib with the notifications from consumer."""
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

    def _on_episodic_metric_report(self, received_message_data: ReceivedMessage):
        model = self._mdib.data_model
        cls = model.msg_types.EpisodicMetricReport
        report = cls.from_node(received_message_data.p_msg.msg_node)
        self._mdib.process_incoming_metric_states_report(received_message_data.mdib_version_group, report)

        if not self._mdib.is_initialized:
            return

        # generate warnings if age of states is out of accepted range
        age_logger = AgeLogger(self.metric_time_warner, self.DETERMINATIONTIME_WARN_LIMIT,
                               'EpisodicMetricReport', self._mdib.mdib_version)
        for report_part in report.ReportPart:
            for state_container in report_part.values_list:
                desc_h = state_container.DescriptorHandle
                if state_container.MetricValue is not None:
                    # BICEPS: While Validity is "Ong" or "NA", the enclosing METRIC value SHALL not possess a
                    # determined value.
                    # Also ignore determination time if measurement is invalid or not active.
                    if state_container.ActivationState == model.pm_types.ComponentActivation.ON and \
                            state_container.MetricValue.MetricQuality.Validity not in [
                        model.pm_types.MeasurementValidity.INVALID,
                        model.pm_types.MeasurementValidity.NA,
                        model.pm_types.MeasurementValidity.MEASUREMENT_ONGOING]:
                        determination_time = state_container.MetricValue.DeterminationTime
                        if determination_time is None:
                            self._logger.warning(  # noqa: PLE1205
                                'EpisodicMetricReport: metric {} version {} has no DeterminationTime',
                                desc_h, state_container.StateVersion)
                        else:
                            age_logger.add_determination_time(determination_time)

        age_logger.log_age_warnings(self._logger)

    def _on_episodic_alert_report(self, received_message_data: ReceivedMessage):
        cls = self._mdib.data_model.msg_types.EpisodicAlertReport
        report = cls.from_node(received_message_data.p_msg.msg_node)
        self._mdib.process_incoming_alert_states_report(received_message_data.mdib_version_group, report)

    def _on_operational_state_report(self, received_message_data: ReceivedMessage):
        cls = self._mdib.data_model.msg_types.EpisodicOperationalStateReport
        report = cls.from_node(received_message_data.p_msg.msg_node)
        self._mdib.process_incoming_operational_states_report(received_message_data.mdib_version_group, report)

    def _on_waveform_report_profiled(self, received_message_data: ReceivedMessage):
        """Handle waveform report and print profiling information to stdout."""
        reference_count_print_limit = 50
        self.prof.enable()
        self._on_waveform_report(received_message_data)
        self.prof.disable()
        str_io = StringIO()
        stats = pstats.Stats(self.prof, stream=str_io).sort_stats('cumulative')
        stats.print_stats(30)
        print(str_io.getvalue())
        print(f'total number of states: {len(self._mdib.states.objects)}')
        print(f'total number of objIds: {len(self._mdib.states._object_ids)}')  # noqa: SLF001
        for name, refs in self._mdib.states._object_ids.items():  # noqa: SLF001
            if len(refs) > reference_count_print_limit:
                print(f'object {name} has {len(refs)} idx references, {refs}')

    def _on_waveform_report(self, received_message_data: ReceivedMessage):
        """Handle waveform report."""
        cls = self._mdib.data_model.msg_types.WaveformStream
        report = cls.from_node(received_message_data.p_msg.msg_node)
        if self._calculate_wf_age_stats:
            self._process_wf_age_statistics(report.State)
        accepted_states = self._mdib.process_incoming_waveform_states(received_message_data.mdib_version_group,
                                                                      report.State)

        if accepted_states is None or len(accepted_states) == 0 or not self._mdib.is_initialized:
            return

        waveform_age = {}  # collect age of all waveforms in this report, and make one report if age is above warn limit (instead of multiple)
        now = time.time()
        for state_container in accepted_states.values():
            rt_sample_containers = self._mdib.rt_buffers[state_container.DescriptorHandle].rt_data
            if len(rt_sample_containers) > 0:
                waveform_age[state_container.DescriptorHandle] = now - rt_sample_containers[-1].determination_time

        if len(waveform_age) > 0:
            min_age = min(waveform_age.values())
            max_age = max(waveform_age.values())
            shall_log = self.waveform_time_warner.get_out_of_determination_time_log_state(
                min_age, max_age, self.DETERMINATIONTIME_WARN_LIMIT)
            if shall_log != _WarningState.A_NO_LOG:
                tmp = ', '.join(f'"{k}": {v:.3f}sec.' for k, v in waveform_age.items())
                if shall_log == _WarningState.A_OUT_OF_RANGE:
                    self._logger.warning(  # noqa: PLE1205
                        '_on_waveform_report mdib_version {}: age of samples outside limit of {} sec.: {}',
                        self._mdib.mdib_version, self.DETERMINATIONTIME_WARN_LIMIT, tmp)
                elif shall_log == _WarningState.A_STILL_OUT_OF_RANGE:
                    self._logger.warning(  # noqa: PLE1205
                        '_on_waveform_report mdib_version {}: age of samples still outside limit of {} sec.: {}',
                        self._mdib.mdib_version, self.DETERMINATIONTIME_WARN_LIMIT, tmp)
                elif shall_log == _WarningState.A_BACK_IN_RANGE:
                    self._logger.info(  # noqa: PLE1205
                        '_on_waveform_report mdib_version {}: age of samples back in limit of {} sec.: {}',
                        self._mdib.mdib_version, self.DETERMINATIONTIME_WARN_LIMIT, tmp)
        if LOG_WF_AGE_INTERVAL:
            now = time.time()
            if now - self._last_wf_age_log >= LOG_WF_AGE_INTERVAL:
                age_data = self.get_wf_age_stdev()
                if age_data is not None:
                    self._logger.info(  # noqa: PLE1205
                        'waveform mean age={:.1f}ms., stdev={:.2f}ms. min={:.1f}ms., max={}',
                        age_data.mean_age * 1000., age_data.stdev * 1000.,
                        age_data.min_age * 1000., age_data.max_age * 1000.)
                self._last_wf_age_log = now

    def _on_episodic_context_report(self, received_message_data: ReceivedMessage):
        """Handle episodic context report."""
        cls = self._mdib.data_model.msg_types.EpisodicContextReport
        report = cls.from_node(received_message_data.p_msg.msg_node)
        self._mdib.process_incoming_context_states_report(received_message_data.mdib_version_group, report)

    def _on_episodic_component_report(self, received_message_data: ReceivedMessage):
        """Handle episodic component report.

        The EpisodicComponentReport is sent if at least one property of at least one component state has changed
        and SHOULD contain only the changed component states.
        Components are MDSs, VMDs, Channels. Not metrics and alarms.
        """
        cls = self._mdib.data_model.msg_types.EpisodicComponentReport
        report = cls.from_node(received_message_data.p_msg.msg_node)
        self._mdib.process_incoming_component_states_report(received_message_data.mdib_version_group, report)

    def _on_description_modification_report(self, received_message_data: ReceivedMessage):
        """Handle description modification report.

        The EpisodicComponentReport is sent if at least one property of at least one component state has changed
        and SHOULD contain only the changed component states.
        Components are MDSs, VMDs, Channels. Not metrics and alarms.
        """
        cls = self._mdib.data_model.msg_types.DescriptionModificationReport
        report = cls.from_node(received_message_data.p_msg.msg_node)
        self._mdib.process_incoming_description_modifications(received_message_data.mdib_version_group, report)

    def _process_wf_age_statistics(self, state_containers: list[AbstractMetricStateContainer]):
        for st in state_containers:
            age_stat = self._age_statistics.get(st.DescriptorHandle)
            if age_stat is None:
                age_stat = AgeStatistics()
                self._age_statistics[st.DescriptorHandle] = age_stat
            age_stat.process_state(st)

    def get_wf_age_stdev(self) -> AgeData:
        """Create some statistics data for age of waveform data when it arrived.

        Data is used for logging.
        """
        if len(self._age_statistics) < AgeStatistics.min_list_lenght_for_statistics:
            return AgeData(0.0, 0.0, 0.0, 0.0)

        means = []
        stdevs = []
        mins = []
        maxs = []
        for age_stat in self._age_statistics.values():
            age_data = age_stat.get_age_stdev()
            means.append(age_data.mean_age)
            stdevs.append(age_data.stdev)
            mins.append(age_data.min_age)
            maxs.append(age_data.max_age)
        return AgeData(mean(means), mean(stdevs), min(mins), max(maxs))
