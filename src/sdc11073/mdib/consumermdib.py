"""Consumer mdib implementation."""

from __future__ import annotations

import enum
import functools
import threading
import time
from collections import deque
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from sdc11073 import loghelper
from sdc11073 import observableproperties as properties
from sdc11073.exceptions import ApiUsageError
from sdc11073.mdib import mdibbase
from sdc11073.mdib.consumermdibxtra import ConsumerMdibMethods

if TYPE_CHECKING:
    from collections.abc import Callable
    from decimal import Decimal
    from enum import Enum

    from sdc11073.consumer.consumerimpl import SdcConsumer
    from sdc11073.mdib.entityprotocol import EntityGetterProtocol
    from sdc11073.mdib.statecontainers import (
        AbstractContextStateContainer,
        AbstractStateContainer,
        RealTimeSampleArrayMetricStateContainer,
    )
    from sdc11073.pysoap.msgreader import MdibVersionGroupReader
    from sdc11073.xml_types.msg_types import (
        AbstractReport,
        DescriptionModificationReport,
        EpisodicAlertReport,
        EpisodicComponentReport,
        EpisodicContextReport,
        EpisodicMetricReport,
        OperationInvokedReport,
    )


@dataclass
class RtSampleContainer:
    """RtSampleContainer contains a single value."""

    value: Decimal
    determination_time: float
    validity: Enum
    annotations: list

    @property
    def age(self) -> float:
        """Return the age of the Sample in seconds."""
        return time.time() - self.determination_time

    def __repr__(self) -> str:
        return f'RtSample value="{self.value}" validity="{self.validity}" time={self.determination_time}'


class ConsumerRtBuffer:
    """Collects data of one real time stream."""

    def __init__(self, sample_period: float, max_samples: int):
        """Construct a ConsumerRtBuffer.

        :param sample_period: float value, in seconds.
                              When an incoming real time sample array is split into single RtSampleContainers,
                              this is used to calculate the individual time stamps.
                              Value can be zero if correct value is not known.
                              In this case all Containers will have the observation time of the sample array.
        :param max_samples: integer, max. length of self.rt_data
        """
        self.rt_data = deque(maxlen=max_samples)
        self.sample_period = sample_period
        self._max_samples = max_samples
        self._logger = loghelper.get_logger_adapter('sdc.client.mdib.rt')
        self._lock = threading.Lock()
        self.last_sc = None  # last state container that was handled

    def mk_rt_sample_containers(
        self,
        realtime_sample_array_container: RealTimeSampleArrayMetricStateContainer,
    ) -> list[RtSampleContainer]:
        """Create a list of RtSampleContainer from a RealTimeSampleArrayMetricStateContainer.

        :param realtime_sample_array_container: a RealTimeSampleArrayMetricStateContainer instance
        :return: a list of RtSampleContainer
        """
        self.last_sc = realtime_sample_array_container
        metric_value = realtime_sample_array_container.MetricValue
        if metric_value is None:
            # this can happen if metric state is not activated.
            self._logger.debug(  # noqa: PLE1205
                'real time sample array "{}" has no metric value, ignoring it',
                realtime_sample_array_container.DescriptorHandle,
            )
            return []
        determination_time = metric_value.DeterminationTime
        annots = metric_value.Annotation
        apply_annotations = metric_value.ApplyAnnotation
        rt_sample_containers = []
        if metric_value.Samples is not None:
            for i, sample in enumerate(metric_value.Samples):
                applied_annotations = []
                if apply_annotations is not None:
                    for apply_annotation in apply_annotations:
                        if apply_annotation.SampleIndex == i:
                            # there is an annotation for this sample:
                            ann_index = apply_annotation.AnnotationIndex
                            annotation = annots[ann_index]  # index is zero-based
                            applied_annotations.append(annotation)
                rt_sample_time = determination_time + i * self.sample_period
                rt_sample_containers.append(
                    RtSampleContainer(sample, rt_sample_time, metric_value.MetricQuality.Validity, applied_annotations),
                )
        return rt_sample_containers

    def add_rt_sample_containers(self, rt_sample_containers: list[RtSampleContainer]) -> None:
        """Update self.rt_data with the new rt_sample_containers.

        :param rt_sample_containers: a list of RtSampleContainer
        :return: None
        """
        if not rt_sample_containers:
            return
        with self._lock:
            self.rt_data.extend(rt_sample_containers)

    def read_rt_data(self) -> list[RtSampleContainer]:
        """Consume all currently buffered data and return it.

        :return: a list of RtSampleContainer objects
        """
        with self._lock:
            ret = list(self.rt_data)
            self.rt_data.clear()
        return ret


@dataclass
class _BufferedData:
    mdib_version_group: MdibVersionGroupReader
    data: AbstractReport
    handler: callable


class ConsumerMdibState(enum.Enum):
    """ConsumerMdib can be in one of these states."""

    initializing = enum.auto()  # the state during reload_all()
    initialized = enum.auto()  # the state when mdib is in sync with provider
    invalid = enum.auto()  # the state when mdib is not in sync with provider


_MDIB_VERSION_NO_SYNC = '{}: MDIB not yet synchronized. This MdibVersion will not be processed, current {}, received {}'
_MDIB_VERSION_UNEXPECTED = '{}: unexpect MdibVersion, expected {}, received {}'


def _inconsistent_on_error(func: Callable[..., Any]) -> Callable[..., Any]:
    """Decorate a ConsumerMdib method that processes a received report.

    If the decorated method raises, the data of the mdib is no longer a correct mirror of the provider mdib:
    'status' is set to invalid, then the exception is passed on to the caller.
    """

    @functools.wraps(func)
    def wrapper(self: ConsumerMdib, *args: Any, **kwargs: Any) -> Any:
        try:
            return func(self, *args, **kwargs)
        except Exception:
            self.status = ConsumerMdibState.invalid
            raise

    return wrapper


class ConsumerMdib(mdibbase.MdibBase):
    """ConsumerMdib is a mirror of a provider mdib. Updates are performed by an SdcConsumer."""

    # for testing purpose you can disable checking of mdib version, so that every notification is accepted.
    MDIB_VERSION_CHECK_DISABLED = False

    # sequence_or_instance_id_changed_event is set to True every time the sequence id changes.
    # It is not reset to False any time later.
    # It is in the responsibility of the application to react on a changed sequence id.
    # Observe this property and call "reload_all" in the observer code.
    sequence_or_instance_id_changed_event: bool = properties.ObservableProperty(
        default_value=False,
        fire_only_on_changed_value=False,
    )

    # status tells whether the data in this mdib is a correct mirror of the provider mdib.
    # It is set to ConsumerMdibState.invalid if an error occurred while processing a received report.
    # Observe this property to be notified about such errors; the data can only be trusted again after reload_all.
    status = properties.ObservableProperty(default_value=ConsumerMdibState.invalid)

    def __init__(self, sdc_client: SdcConsumer, extras_cls: type | None = None, max_realtime_samples: int = 100):
        """Construct a ConsumerMdib instance.

        :param sdc_client: a SdcConsumer instance
        :param  extras_cls: extended functionality
        :param max_realtime_samples: determines how many real time samples are stored per RealtimeSampleArray
        """
        super().__init__(
            sdc_client.sdc_definitions,
            loghelper.get_logger_adapter('sdc.client.mdib', sdc_client.log_prefix),
        )
        self._synchronizedReports = threading.Event()
        self._sdc_client = sdc_client
        if extras_cls is None:
            extras_cls = ConsumerMdibMethods
        self._xtra = extras_cls(self, self._logger)
        self.rt_buffers = {}  # key  is a handle, value is a ConsumerRtBuffer
        self._max_realtime_samples = max_realtime_samples
        self._last_wf_age_log = time.time()
        # a buffer for notifications that are received before initial get_mdib is done
        self._buffered_notifications = []
        self._buffered_notifications_lock = threading.Lock()
        self.entities: EntityGetterProtocol = mdibbase.EntityGetter(self)

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
        return self.status == ConsumerMdibState.initialized

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
            self._synchronizedReports.clear()
            self.status = ConsumerMdibState.initializing  # notifications are now buffered
            self.descriptions.clear()
            self.clear_states()
            self.sequence_id = None
            self.instance_id = None
            self.mdib_version = None

            get_service = self._sdc_client.client('Get')
            self._logger.info('initializing mdib...')
            response = get_service.get_mdib()  # GetRequestResult
            self._logger.info('creating description containers...')
            descriptor_containers, state_containers = response.result
            self.add_description_containers(descriptor_containers)
            self._logger.info('creating state containers...')
            self.add_state_containers(state_containers)

            mdib_version_group = response.mdib_version_group
            self.mdib_version = mdib_version_group.mdib_version
            self._logger.info('setting initial mdib version to %d', mdib_version_group.mdib_version)
            self.sequence_id = mdib_version_group.sequence_id
            self._logger.info('setting initial sequence id to {}', mdib_version_group.sequence_id)  # noqa: PLE1205
            if mdib_version_group.instance_id != self.instance_id:
                self.instance_id = mdib_version_group.instance_id
            self._logger.info('setting initial instance id to {}', mdib_version_group.instance_id)  # noqa: PLE1205

            # retrieve context states only if there were none in mdib
            if len(self.context_states.objects) == 0:
                self._retrieve_context_states()
            else:
                self._logger.info('found context states in GetMdib Result, will not call getContextStates')

            # process buffered notifications
            with self._buffered_notifications_lock:
                self._logger.debug('got _buffered_notifications_lock')
                for buffered_report in self._buffered_notifications:
                    # buffered data might contain notifications that do not fit.
                    if buffered_report.mdib_version_group.sequence_id != self.sequence_id:
                        self.logger.debug(
                            'wrong sequence id "%s"; ignore buffered report',
                            buffered_report.mdib_version_group.sequence_id,
                        )
                        continue
                    if buffered_report.mdib_version_group.mdib_version <= self.mdib_version:
                        self.logger.debug(
                            'older mdib version "%d"; ignore buffered report',
                            buffered_report.mdib_version_group.mdib_version,
                        )
                        continue
                    buffered_report.handler(buffered_report.mdib_version_group, buffered_report.data)
                del self._buffered_notifications[:]
                # self.status could have been set to invalid by a notification handler.
                # In this case, we do not set it initialized.
                if self.status == ConsumerMdibState.initializing:
                    self.status = ConsumerMdibState.initialized
            self._logger.info('reload_all done')

    def _retrieve_context_states(self):
        """Only called when context states are not included in GetMdib result."""
        self._logger.info('requesting context states...')
        response = self._sdc_client.client('Context').get_context_states()
        context_state_containers = response.result.ContextState

        self._logger.debug('got {} context states', len(context_state_containers))  # noqa: PLE1205
        with self.context_states.lock:
            for state_container in context_state_containers:
                old_state_containers = self.context_states.handle.get(state_container.Handle, [])
                if not len(old_state_containers):
                    self.context_states.add_object_no_lock(state_container)
                    self._logger.debug('new ContextState {}', state_container)  # noqa: PLE1205
                else:
                    txt = ', '.join([str(x) for x in old_state_containers])
                    msg = (
                        f'Unexpected behavior. ContextState(s) are already included in the MDIB. '
                        f'Found {len(old_state_containers)} objects: {txt}'
                    )
                    raise RuntimeError(msg)

    def _can_accept_mdib_version(self, new_mdib_version: int, log_prefix: str) -> bool:  # noqa: C901
        if self.MDIB_VERSION_CHECK_DISABLED:
            return True

        if new_mdib_version <= 0:
            # It is not assumed that MDIB version within a notification is zero.
            # This is only possible in GetMdib response.
            msg = _MDIB_VERSION_UNEXPECTED.format(log_prefix, self.mdib_version + 1, new_mdib_version)
            self._logger.error(msg)
            raise ValueError(msg)

        # SDPi R1007 requires a strictly increasing msg:AbstractReport/@MdibVersion.
        # This prohibits decrementing version numbers within an MDIB sequence.
        if new_mdib_version < self.mdib_version:
            if self._synchronizedReports.is_set():
                msg = _MDIB_VERSION_UNEXPECTED.format(log_prefix, self.mdib_version + 1, new_mdib_version)
                self._logger.error(msg)
                raise ValueError(msg)

            self._logger.debug(_MDIB_VERSION_NO_SYNC, log_prefix, self.mdib_version, new_mdib_version)
            return False

        # SDPi R1007 requires a strictly increasing msg:AbstractReport/@MdibVersion.
        # This prohibits two reports with the same MDIB version.
        if new_mdib_version == self.mdib_version:
            if self._synchronizedReports.is_set():
                msg = _MDIB_VERSION_UNEXPECTED.format(log_prefix, self.mdib_version + 1, new_mdib_version)
                self._logger.error(msg)
                raise ValueError(msg)
            self._logger.debug(  # noqa: PLE1205
                '{}: received report with same mdib version {} as current mdib version. MDIB now in sync.',
                log_prefix,
                new_mdib_version,
            )
            self._synchronizedReports.set()
            return False

        if (new_mdib_version - self.mdib_version) > 1:
            # An error is logged in this case because this MDIB implementation cannot determine whether essential
            # information was missed or not received.
            msg = _MDIB_VERSION_UNEXPECTED.format(log_prefix, self.mdib_version + 1, new_mdib_version)
            self._logger.error(msg)
            if self._sdc_client.all_subscribed:
                raise ValueError(msg)

        if new_mdib_version > self.mdib_version:
            if not self._synchronizedReports.is_set():
                self._logger.debug(  # noqa: PLE1205
                    '{}: received report with mdib version {} greater than current mdib version {}. MDIB now in sync.',
                    log_prefix,
                    new_mdib_version,
                    self.mdib_version,
                )
                self._synchronizedReports.set()
            return True

        raise RuntimeError('THIS SHOULD NEVER HAPPEN!')

    def _check_sequence_or_instance_id_changed(self, mdib_version_group: mdibbase.MdibVersionGroup):
        """Check if sequence id and instance id are still the same.

        If not,
        - set state member to invalid
        - set the observable "sequence_or_instance_id_changed_event" in a thread.
          This allows to implement an observer that can directly call reload_all without blocking the consumer.
        """
        if mdib_version_group.sequence_id == self.sequence_id and mdib_version_group.instance_id == self.instance_id:
            return
        if self.status == ConsumerMdibState.initialized:
            if mdib_version_group.sequence_id != self.sequence_id:
                self.logger.warning(
                    'sequence id changed from "%s" to "%s"',
                    self.sequence_id,
                    mdib_version_group.sequence_id,
                )
            if mdib_version_group.instance_id != self.instance_id:
                self.logger.warning(
                    'instance id changed from "%r" to "%r"',
                    self.instance_id,
                    mdib_version_group.instance_id,
                )
            self.logger.warning('mdib is no longer valid!')

            self.status = ConsumerMdibState.invalid

            def _set_observable():
                self.sequence_or_instance_id_changed_event = True

            thr = threading.Thread(target=_set_observable)
            thr.start()

    def _update_from_mdib_version_group(self, mdib_version_group: MdibVersionGroupReader):
        if mdib_version_group.mdib_version != self.mdib_version:
            self.mdib_version = mdib_version_group.mdib_version
        if mdib_version_group.sequence_id != self.sequence_id:
            self.sequence_id = mdib_version_group.sequence_id
        if mdib_version_group.instance_id != self.instance_id:
            self.instance_id = mdib_version_group.instance_id

    def _update_from_states_report(
        self,
        report_type: str,
        report: EpisodicMetricReport | EpisodicAlertReport | OperationInvokedReport | EpisodicComponentReport,
    ) -> dict[str, AbstractStateContainer]:
        """Update mdib with incoming states."""
        states_by_handle = {}
        for report_part in report.ReportPart:
            for state_container in report_part.values_list:
                src = self.states
                old_state_container = src.descriptor_handle.get_one(state_container.DescriptorHandle, allow_none=True)
                if old_state_container is not None:
                    if self._has_new_state_usable_state_version(old_state_container, state_container, report_type):
                        old_state_container.update_from_other_container(state_container)
                        src.update_object(old_state_container)
                        states_by_handle[old_state_container.DescriptorHandle] = old_state_container
                else:
                    msg = f'Unknown state with DescriptorHandle "{state_container.DescriptorHandle}" received.'
                    self._logger.error(msg)
                    raise RuntimeError(msg)
        return states_by_handle

    def _update_from_context_states_report(
        self,
        report: EpisodicContextReport,
    ) -> dict[str, AbstractContextStateContainer]:
        """Update mdib with incoming states."""
        states_by_handle = {}
        for report_part in report.ReportPart:
            for state_container in report_part.values_list:
                src = self.context_states
                old_state_container = src.handle.get_one(state_container.Handle, allow_none=True)
                if old_state_container is not None:
                    if self._has_new_state_usable_state_version(old_state_container, state_container, 'context states'):
                        self._logger.info(  # noqa: PLE1205
                            'updated context state: handle = {} Descriptor Handle={} Assoc={}, Validators={}',
                            state_container.Handle,
                            state_container.DescriptorHandle,
                            state_container.ContextAssociation,
                            state_container.Validator,
                        )
                        old_state_container.update_from_other_container(state_container)
                        src.update_object(old_state_container)
                        states_by_handle[old_state_container.DescriptorHandle] = old_state_container
                else:
                    self._logger.info(  # noqa: PLE1205
                        'new context state: handle = {} Descriptor Handle={} Assoc={}, Validators={}',
                        state_container.Handle,
                        state_container.DescriptorHandle,
                        state_container.ContextAssociation,
                        state_container.Validator,
                    )
                    self._set_descriptor_container_reference(state_container)
                    src.add_object(state_container)
                    states_by_handle[state_container.Handle] = state_container
        return states_by_handle

    def _pre_check_report_ok(
        self,
        mdib_version_group: MdibVersionGroupReader,
        report: AbstractReport | list[RealTimeSampleArrayMetricStateContainer],
        handler: Callable,
    ) -> bool:
        """Check if the report can be added to mdib.

        The pre-check runs before the mdib lock is acquired.
        The report is buffered if state is 'initializing' and 'is_buffered_report' is False.
        :return: True if report can be added to mdib.
        """
        self._check_sequence_or_instance_id_changed(mdib_version_group)  # this might change self.status
        if self.status == ConsumerMdibState.invalid:
            # ignore report in these states
            return False
        if self.status == ConsumerMdibState.initializing:
            with self._buffered_notifications_lock:
                # check state again, it might have changed before lock was acquired
                if self.status == ConsumerMdibState.initializing:
                    self._buffered_notifications.append(_BufferedData(mdib_version_group, report, handler))
                    return False
                if self.status == ConsumerMdibState.invalid:
                    return False
        return True

    def process_incoming_metric_states_report(
        self,
        mdib_version_group: MdibVersionGroupReader,
        report: EpisodicMetricReport,
    ):
        """Check mdib_version_group and process report it if okay."""
        if not self._pre_check_report_ok(mdib_version_group, report, self._process_incoming_metric_states_report):
            return
        with self.mdib_lock:
            self._process_incoming_metric_states_report(mdib_version_group, report)

    @_inconsistent_on_error
    def _process_incoming_metric_states_report(
        self,
        mdib_version_group: MdibVersionGroupReader,
        report: EpisodicMetricReport,
    ):
        """Check mdib version.

        If okay:
         - update mdib.
         - update observable.
        Call this method only if mdib_lock is already acquired.
        """
        states_by_handle = {}
        if self._can_accept_mdib_version(mdib_version_group.mdib_version, 'metric states'):
            self._update_from_mdib_version_group(mdib_version_group)
            states_by_handle = self._update_from_states_report('metric states', report)
        self.metrics_by_handle = states_by_handle  # update observable

    def process_incoming_alert_states_report(
        self,
        mdib_version_group: MdibVersionGroupReader,
        report: EpisodicAlertReport,
    ):
        """Check mdib_version_group and process report it if okay."""
        if not self._pre_check_report_ok(mdib_version_group, report, self._process_incoming_alert_states_report):
            return
        with self.mdib_lock:
            self._process_incoming_alert_states_report(mdib_version_group, report)

    @_inconsistent_on_error
    def _process_incoming_alert_states_report(
        self,
        mdib_version_group: MdibVersionGroupReader,
        report: EpisodicAlertReport,
    ):
        """Check mdib version.

        If okay:
         - update mdib.
         - update observable.
        Call this method only if mdib_lock is already acquired.
        """
        states_by_handle = {}
        if self._can_accept_mdib_version(mdib_version_group.mdib_version, 'alert states'):
            self._update_from_mdib_version_group(mdib_version_group)
            states_by_handle = self._update_from_states_report('alert states', report)
        self.alert_by_handle = states_by_handle  # update observable

    def process_incoming_operational_states_report(
        self,
        mdib_version_group: MdibVersionGroupReader,
        report: OperationInvokedReport,
    ):
        """Check mdib_version_group and process report it if okay."""
        if not self._pre_check_report_ok(mdib_version_group, report, self._process_incoming_operational_states_report):
            return
        with self.mdib_lock:
            self._process_incoming_operational_states_report(mdib_version_group, report)

    @_inconsistent_on_error
    def _process_incoming_operational_states_report(
        self,
        mdib_version_group: MdibVersionGroupReader,
        report: OperationInvokedReport,
    ):
        """Check mdib version.

        If okay:
         - update mdib.
         - update observable.
        Call this method only if mdib_lock is already acquired.
        """
        states_by_handle = {}
        if self._can_accept_mdib_version(mdib_version_group.mdib_version, 'operational states'):
            self._update_from_mdib_version_group(mdib_version_group)
            states_by_handle = self._update_from_states_report('operational states', report)
        self.operation_by_handle = states_by_handle  # update observable

    def process_incoming_context_states_report(
        self,
        mdib_version_group: MdibVersionGroupReader,
        report: EpisodicContextReport,
    ):
        """Check mdib_version_group and process report it if okay."""
        if not self._pre_check_report_ok(mdib_version_group, report, self._process_incoming_context_states_report):
            return
        with self.mdib_lock:
            self._process_incoming_context_states_report(mdib_version_group, report)

    @_inconsistent_on_error
    def _process_incoming_context_states_report(
        self,
        mdib_version_group: MdibVersionGroupReader,
        report: EpisodicContextReport,
    ):
        """Check mdib version.

        If okay:
         - update mdib.
         - update observable.
        Call this method only if mdib_lock is already acquired.
        """
        states_by_handle = {}
        if self._can_accept_mdib_version(mdib_version_group.mdib_version, 'context states'):
            self._update_from_mdib_version_group(mdib_version_group)
            states_by_handle = self._update_from_context_states_report(report)
        self.context_by_handle = states_by_handle  # update observable

    def process_incoming_component_states_report(
        self,
        mdib_version_group: MdibVersionGroupReader,
        report: EpisodicComponentReport,
    ):
        """Check mdib_version_group and process report it if okay."""
        if not self._pre_check_report_ok(mdib_version_group, report, self._process_incoming_component_states_report):
            return
        with self.mdib_lock:
            self._process_incoming_component_states_report(mdib_version_group, report)

    @_inconsistent_on_error
    def _process_incoming_component_states_report(
        self,
        mdib_version_group: MdibVersionGroupReader,
        report: EpisodicComponentReport,
    ):
        """Check mdib version.

        If okay:
         - update mdib.
         - update observable.
        Call this method only if mdib_lock is already acquired.
        """
        states_by_handle = {}
        if self._can_accept_mdib_version(mdib_version_group.mdib_version, 'component states'):
            self._update_from_mdib_version_group(mdib_version_group)
            states_by_handle = self._update_from_states_report('component states', report)
        self.component_by_handle = states_by_handle  # update observable

    def process_incoming_waveform_states(
        self,
        mdib_version_group: MdibVersionGroupReader,
        state_containers: list[RealTimeSampleArrayMetricStateContainer],
    ) -> dict[str, RealTimeSampleArrayMetricStateContainer] | None:
        """Check mdib_version_group and process state_containers it if okay."""
        if not self._pre_check_report_ok(mdib_version_group, state_containers, self._process_incoming_waveform_states):
            return
        with self.mdib_lock:
            self._process_incoming_waveform_states(mdib_version_group, state_containers)

    @_inconsistent_on_error
    def _process_incoming_waveform_states(
        self,
        mdib_version_group: MdibVersionGroupReader,
        state_containers: list[RealTimeSampleArrayMetricStateContainer],
    ):
        """Check mdib version.

        If okay:
         - update mdib.
         - update observable.
        Call this method only if mdib_lock is already acquired.
        """
        states_by_handle = {}
        if self._can_accept_mdib_version(mdib_version_group.mdib_version, 'waveform states'):
            self._update_from_mdib_version_group(mdib_version_group)
            for state_container in state_containers:
                old_state_container = self.states.descriptor_handle.get_one(
                    state_container.DescriptorHandle,
                    allow_none=True,
                )
                if old_state_container is not None:
                    if self._has_new_state_usable_state_version(
                        old_state_container,
                        state_container,
                        'waveform states',
                    ):
                        old_state_container.update_from_other_container(state_container)
                        self.states.update_object(old_state_container)
                        states_by_handle[old_state_container.DescriptorHandle] = old_state_container
                else:
                    msg = f'Unknown state with DescriptorHandle "{state_container.DescriptorHandle}" received.'
                    self._logger.error(msg)
                    raise RuntimeError(msg)

            # add to Waveform Buffer
            for state_container in states_by_handle.values():
                state_container: RealTimeSampleArrayMetricStateContainer
                descriptor_container = state_container.descriptor_container
                d_handle = state_container.DescriptorHandle
                rt_buffer = self.rt_buffers.get(d_handle)
                if rt_buffer is None:
                    sample_period = 0  # default
                    if descriptor_container is not None:
                        # read sample period
                        sample_period = descriptor_container.SamplePeriod or 0
                    rt_buffer = ConsumerRtBuffer(
                        sample_period=sample_period,
                        max_samples=self._max_realtime_samples,
                    )
                    self.rt_buffers[d_handle] = rt_buffer
                rt_sample_containers = rt_buffer.mk_rt_sample_containers(state_container)
                rt_buffer.add_rt_sample_containers(rt_sample_containers)
        self.waveform_by_handle = states_by_handle  # update observable

    def process_incoming_description_modifications(
        self,
        mdib_version_group: MdibVersionGroupReader,
        report: DescriptionModificationReport,
    ):
        """Check mdib_version_group and process report it if okay."""
        if not self._pre_check_report_ok(mdib_version_group, report, self._process_incoming_description_modifications):
            return
        with self.mdib_lock:
            self._process_incoming_description_modifications(mdib_version_group, report)

    @_inconsistent_on_error
    def _process_incoming_description_modifications(  # noqa: PLR0915, PLR0912, C901
        self,
        mdib_version_group: MdibVersionGroupReader,
        report: DescriptionModificationReport,
    ):
        """Check mdib version.

        If okay:
         - update mdib.
         - update observables.
        Call this method only if mdib_lock is already acquired.
        """

        def multi_key(st_container: AbstractStateContainer) -> mdibbase.StatesLookup | mdibbase.MultiStatesLookup:
            return self.context_states if st_container.is_context_state else self.states

        new_descriptor_by_handle = {}
        updated_descriptor_by_handle = {}
        deleted_descriptor_by_handle = {}

        dmt = self.sdc_definitions.data_model.msg_types.DescriptionModificationType
        if self._can_accept_mdib_version(mdib_version_group.mdib_version, 'descriptors'):
            self._update_from_mdib_version_group(mdib_version_group)
            for report_part in report.ReportPart:
                modification_type = report_part.ModificationType
                if modification_type == dmt.CREATE:
                    for descriptor_container in report_part.Descriptor:
                        self.descriptions.add_object(descriptor_container)
                        self._logger.debug(  # noqa: PLE1205
                            'process_incoming_descriptors: created description "{}" (parent="{}")',
                            descriptor_container.Handle,
                            descriptor_container.parent_handle,
                        )
                        new_descriptor_by_handle[descriptor_container.Handle] = descriptor_container
                    for state_container in report_part.State:
                        self._set_descriptor_container_reference(state_container)
                        multi_key(state_container).add_object_no_lock(state_container)
                elif modification_type == dmt.UPDATE:
                    updated_descriptor_containers = report_part.Descriptor
                    updated_state_containers = report_part.State
                    for descriptor_container in updated_descriptor_containers:
                        self._logger.info(  # noqa: PLE1205
                            'process_incoming_descriptors: update descriptor "{}" (parent="{}")',
                            descriptor_container.Handle,
                            descriptor_container.parent_handle,
                        )
                        old_container = self.descriptions.handle.get_one(
                            descriptor_container.Handle,
                            allow_none=True,
                        )
                        if old_container is None:
                            msg = f'Update for unknown descriptor with handle "{descriptor_container.Handle}" received.'
                            self._logger.error(msg)
                            raise RuntimeError(msg)

                        old_container.update_from_other_container(descriptor_container)
                        updated_descriptor_by_handle[descriptor_container.Handle] = descriptor_container
                        # if this is a context descriptor, delete all associated states that are not in
                        # state_containers list
                        if descriptor_container.is_context_descriptor:
                            updated_handles = {
                                s.Handle
                                for s in updated_state_containers
                                if s.DescriptorHandle == descriptor_container.Handle
                            }
                            my_handles = {
                                s.Handle
                                for s in self.context_states.descriptor_handle.get(descriptor_container.Handle, [])
                            }  # set comprehension
                            to_be_deleted = my_handles - updated_handles
                            for handle in to_be_deleted:
                                state = self.context_states.handle.get_one(handle)
                                self.context_states.remove_object_no_lock(state)
                    for state_container in updated_state_containers:
                        my_multi_key = multi_key(state_container)

                        if state_container.is_context_state:
                            old_state_container = my_multi_key.handle.get_one(
                                state_container.Handle,
                                allow_none=True,
                            )
                        else:
                            old_state_container = my_multi_key.descriptor_handle.get_one(
                                state_container.DescriptorHandle,
                                allow_none=True,
                            )

                        if old_state_container is not None:
                            old_state_container.update_from_other_container(state_container)
                            my_multi_key.update_object(old_state_container)
                        elif state_container.is_context_state:
                            self._logger.info(  # noqa: PLE1205
                                'process_incoming_descriptors: new context state: handle = {} Descriptor Handle={} '
                                'Assoc={}, Validators={}',
                                state_container.Handle,
                                state_container.DescriptorHandle,
                                state_container.ContextAssociation,
                                state_container.Validator,
                            )
                            self._set_descriptor_container_reference(state_container)
                            my_multi_key.add_object_no_lock(state_container)
                        else:
                            msg = f'Unknown state with DescriptorHandle "{state_container.DescriptorHandle}" received.'
                            self._logger.error(msg)
                            raise RuntimeError(msg)

                elif modification_type == dmt.DELETE:
                    deleted_descriptor_containers = report_part.Descriptor
                    deleted_state_containers = report_part.State
                    for descriptor_container in deleted_descriptor_containers:
                        self._logger.debug(  # noqa: PLE1205
                            'process_incoming_descriptors: remove descriptor "{}" (parent="{}")',
                            descriptor_container.Handle,
                            descriptor_container.parent_handle,
                        )
                        self.rm_descriptor_by_handle(descriptor_container.Handle)
                        deleted_descriptor_by_handle[descriptor_container.Handle] = descriptor_container

                    # following is already handled in rm_descriptor_by_handle
                    for state_container in deleted_state_containers:
                        multi_key(state_container).remove_object_no_lock(state_container)
                else:
                    msg = f'unknown modification type {modification_type} in description modification report'
                    raise ValueError(msg)

        self.description_modifications = report  # update observable for complete report
        # update observables for every report part separately
        if new_descriptor_by_handle:
            self.new_descriptors_by_handle = new_descriptor_by_handle
        if updated_descriptor_by_handle:
            self.updated_descriptors_by_handle = updated_descriptor_by_handle
        if deleted_descriptor_by_handle:
            self.deleted_descriptor_by_handle = deleted_descriptor_by_handle

    def _has_new_state_usable_state_version(
        self,
        old_state_container: AbstractStateContainer,
        new_state_container: AbstractStateContainer,
        report_name: str,
    ) -> bool:
        """Compare state versions old vs new.

        :param old_state_container: old version of a state container in mdib
        :param new_state_container: new version of a state container in mdib
        :param report_name: used for logging
        :return: True if new state is ok for mdib, otherwise False.
        """
        diff = int(new_state_container.StateVersion) - int(old_state_container.StateVersion)
        if diff == 1:  # this is the perfect version
            return True
        if diff > 1:
            msg = (f'{report_name}: missed {diff - 1} states for state '
                   f'DescriptorHandle={old_state_container.DescriptorHandle} '
                   f'({old_state_container.StateVersion}->{new_state_container.StateVersion})')
        else:
            msg = (f'{report_name}: received older state for state '
                   f'DescriptorHandle={old_state_container.DescriptorHandle} '
                   f'({old_state_container.StateVersion}->{new_state_container.StateVersion})')
        self._logger.error(msg)
        raise ValueError(msg)
