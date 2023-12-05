from __future__ import annotations

import time
import traceback
from decimal import Context
from threading import Event, Thread
from typing import TYPE_CHECKING, Any, Protocol

from sdc11073 import loghelper
from sdc11073.intervaltimer import IntervalTimer

from . import waveforms
from .realtimesamples import Annotator, RtSampleArray

if TYPE_CHECKING:
    from sdc11073.definitions_base import AbstractDataModel
    from sdc11073.mdib import ProviderMdib
    from sdc11073.mdib.statecontainers import RealTimeSampleArrayMetricStateContainer
    from sdc11073.mdib.transactions import RtDataMdibUpdateTransaction
    from sdc11073.xml_types.pm_types import ComponentActivation

    from .realtimesamples import AnnotatorProtocol


class WaveformGeneratorProtocol(Protocol):
    """A waveform generator creates an infinite sequence of float values."""

    def __init__(self, min_value: float, max_value: float, waveform_period: float, sample_period: float):
        ...

    def next_samples(self, count: int) -> list[float]:
        """Get next values from generator."""
        ...

    sample_period: float


class _SampleArrayGenerator:
    """Wraps a waveform generator and makes RtSampleArray objects."""

    def __init__(self, model: AbstractDataModel,
                 descriptor_handle: str,
                 generator: WaveformGeneratorProtocol):
        self._model = model
        self._descriptor_handle = descriptor_handle
        self._last_timestamp = None
        self._activation_state = model.pm_types.ComponentActivation.ON
        self._generator = generator
        self.current_rt_sample_array = None

    def set_activation_state(self, component_activation_state: ComponentActivation):
        """Set activation state of generator.

        If component_activation_state is not "ON", the generator will not generate values.
        :param component_activation_state: one of pmtypes.ComponentActivation values.
        """
        self._activation_state = component_activation_state
        if component_activation_state == self._model.pm_types.ComponentActivation.ON:
            self._last_timestamp = time.time()

    def get_next_sample_array(self) -> RtSampleArray:
        """Read sample values from waveform generator and calculate determination time.

        If activation state is not 'On', the returned RtSampleArray contains no samples.
        """
        if self._activation_state != self._model.pm_types.ComponentActivation.ON:
            self.current_rt_sample_array = RtSampleArray(
                self._model, None, self._generator.sample_period, [], self._activation_state)
        else:
            now = time.time()
            observation_time = self._last_timestamp or now
            samples_count = int((now - observation_time) / self._generator.sample_period)
            samples = self._generator.next_samples(samples_count)
            self._last_timestamp = observation_time + self._generator.sample_period * samples_count
            self.current_rt_sample_array = RtSampleArray(
                self._model, observation_time, self._generator.sample_period, samples, self._activation_state)
        return self.current_rt_sample_array

    def set_waveform_generator(self, generator: WaveformGeneratorProtocol):
        self._generator = generator

    @property
    def is_active(self) -> bool:
        return self._activation_state == self._model.pm_types.ComponentActivation.ON


class GenericWaveformProvider:
    """Provide waveform data.

    - runs periodic job to send waveform data
    """

    DEFAULT_WORKER_THREAD_INTERVAL = 0.1  # seconds

    WARN_LIMIT_REALTIMESAMPLES_BEHIND_SCHEDULE = 0.2  # warn limit when real time samples cannot be sent in time
    WARN_RATE_REALTIMESAMPLES_BEHIND_SCHEDULE = 5  # max. every x seconds a message

    def __init__(self, mdib: ProviderMdib, log_prefix: str = ''):
        self._mdib = mdib
        self._logger = loghelper.get_logger_adapter(f'sdc.device.{self.__class__.__name__}', log_prefix)

        self._stop_worker = Event()
        self._worker_thread: Thread | None = None
        self.notifications_interval = self.DEFAULT_WORKER_THREAD_INTERVAL
        self._waveform_generators: dict[str, _SampleArrayGenerator] = {}
        self._annotators: dict[str, AnnotatorProtocol] = {}
        self._last_log_time = 0
        self._last_logged_delay = 0

    def register_waveform_generator(self, descriptor_handle: str, wf_generator: WaveformGeneratorProtocol):
        """Add wf_generator to waveform sources.

        :param descriptor_handle: the handle of the RealtimeSampleArray that shall accept this data
        :param wf_generator: a waveforms.WaveformGenerator instance
        """
        sample_period = wf_generator.sample_period
        descriptor_container = self._mdib.descriptions.handle.get_one(descriptor_handle)
        if descriptor_container.SamplePeriod != sample_period:
            # we must inform subscribers
            with self._mdib.transaction_manager() as mgr:
                descr = mgr.get_descriptor(descriptor_handle)
                descr.SamplePeriod = sample_period
        if descriptor_handle in self._waveform_generators:
            self._waveform_generators[descriptor_handle].set_waveform_generator(wf_generator)
        else:
            self._waveform_generators[descriptor_handle] = _SampleArrayGenerator(self._mdib.data_model,
                                                                                 descriptor_handle,
                                                                                 wf_generator)

    def add_annotation_generator(self,
                                 coded_value: Any,
                                 trigger_handle: str,
                                 annotated_handles: list[str]) -> AnnotatorProtocol:
        """Add annotator to list of annotators."""
        annotation = self._mdib.data_model.pm_types.AnnotationType(coded_value)
        annotator = Annotator(annotation, trigger_handle, annotated_handles)
        self._annotators[annotator.trigger_handle] = annotator
        return annotator

    def start(self):
        """Start worker thread."""
        self._worker_thread = Thread(target=self._worker_thread_loop)
        self._worker_thread.daemon = True
        self._worker_thread.start()

    def stop(self):
        """Stop worker thread."""
        if self._worker_thread is not None:
            self._stop_worker.set()
            self._worker_thread.join()
            self._worker_thread = None

    @property
    def is_running(self) -> bool:
        """Return True if worker thread is set."""
        return self._worker_thread is not None

    def set_activation_state(self, descriptor_handle: str, component_activation_state: ComponentActivation):
        """Set the activation state of waveform generator and of Metric state in mdib."""
        wf_generator = self._waveform_generators[descriptor_handle]
        wf_generator.set_activation_state(component_activation_state)
        with self._mdib.transaction_manager() as mgr:
            state = mgr.get_state(descriptor_handle)
            state.ActivationState = component_activation_state
            # if the generator is not active, there shall be no MetricValue
            if not wf_generator.is_active:
                state.MetricValue = None

    def update_all_realtime_samples(self, transaction: RtDataMdibUpdateTransaction):
        """Update all realtime sample states that have a waveform generator registered.

        On transaction commit the mdib will call the appropriate send method of the sdc device.
        """
        for descriptor_handle, wf_generator in self._waveform_generators.items():
            if wf_generator.is_active:
                state = transaction.get_real_time_sample_array_metric_state(descriptor_handle)
                self._update_rt_samples(state)
        self._add_all_annotations()

    def provide_waveforms(self,
                          generator_class: type[WaveformGeneratorProtocol] = waveforms.TriangleGenerator,
                          max_waveforms: int | None = None) -> list[str]:
        """Create waveform generators for waveforms in mdib.

        The values of descriptor.TechnicalRange and descriptor.SamplePeriod are used for the generator.

        :param generator_class: the generator to be instantiated.
        :param max_waveforms: limit number of waveforms.
        :return: list of handles of created generators
        """
        name = self._mdib.data_model.pm_names.RealTimeSampleArrayMetricDescriptor
        all_waveforms = self._mdib.descriptions.NODETYPE.get(name)
        if max_waveforms:
            all_waveforms = all_waveforms[:max_waveforms]
        for waveform in all_waveforms:
            min_value = 0
            max_value = 1
            sample_period = waveform.SamplePeriod if waveform.SamplePeriod > 0 else 0.01  # guarantee usable value
            try:
                tech_range = waveform.TechnicalRange[0]
            except IndexError:
                pass
            else:
                if tech_range.Lower is not None:
                    min_value = float(tech_range.Lower)
                if tech_range.Upper is not None:
                    max_value = float(tech_range.Upper)
                if max_value < min_value:
                    max_value, min_value = min_value, max_value  # swap values
                elif min_value == max_value:
                    max_value += 1
            generator = generator_class(min_value=min_value,
                                        max_value=max_value,
                                        waveform_period=2.0,
                                        sample_period=sample_period)
            self.register_waveform_generator(waveform.Handle, generator)
        return [waveform.Handle for waveform in all_waveforms]

    def _worker_thread_loop(self):
        timer = IntervalTimer(period_in_seconds=self.notifications_interval)
        try:
            while True:
                shall_stop = self._stop_worker.is_set()
                if shall_stop:
                    return
                behind_schedule_seconds = timer.wait_next_interval_begin()
                self._log_waveform_timing(behind_schedule_seconds)
                try:
                    with self._mdib.rt_sample_transaction() as transaction:
                        self.update_all_realtime_samples(transaction)
                    self._log_waveform_timing(behind_schedule_seconds)
                except Exception:  # noqa: BLE001
                    # catch all to keep loop running
                    self._logger.warning('could not update real time samples: %s', traceback.format_exc())
        finally:
            self._logger.info('rt_sample_sendloop end')

    def _update_rt_samples(self, state: RealTimeSampleArrayMetricStateContainer):
        """Update waveforms state from waveform generator (if available)."""
        wf_generator = self._waveform_generators.get(state.DescriptorHandle)
        if wf_generator:
            ctxt = Context(prec=10)
            rt_sample_array = wf_generator.get_next_sample_array()
            samples = [ctxt.create_decimal(s) for s in rt_sample_array.samples]
            if state.MetricValue is None:
                state.mk_metric_value()
            state.MetricValue.Samples = samples
            state.MetricValue.DeterminationTime = rt_sample_array.determination_time
            state.MetricValue.Annotation = rt_sample_array.annotations
            state.MetricValue.ApplyAnnotation = rt_sample_array.apply_annotations
            state.ActivationState = rt_sample_array.activation_state

    def _add_all_annotations(self):
        """Add annotations to all current RtSampleArrays."""
        rt_sample_arrays = {handle: g.current_rt_sample_array for (handle, g) in self._waveform_generators.items()}
        for src_handle, _annotator in self._annotators.items():
            if src_handle in rt_sample_arrays:
                timestamps = _annotator.get_annotation_timestamps(rt_sample_arrays[src_handle])
                if timestamps:
                    for dest_handle in _annotator.annotated_handles:
                        if dest_handle in rt_sample_arrays:
                            rt_sample_arrays[dest_handle].add_annotations_at(_annotator.annotation, timestamps)

    def _log_waveform_timing(self, behind_schedule_seconds: float):
        try:
            last_log_time = self._last_log_time
        except AttributeError:
            self._last_log_time = 0
            last_log_time = self._last_log_time
        try:
            last_logged_delay = self._last_logged_delay
        except AttributeError:
            self._last_logged_delay = 0
            last_logged_delay = self._last_logged_delay

        # max. one log per second
        now = time.monotonic()
        if now - last_log_time < self.WARN_RATE_REALTIMESAMPLES_BEHIND_SCHEDULE:
            return
        if last_logged_delay >= self.WARN_LIMIT_REALTIMESAMPLES_BEHIND_SCHEDULE > behind_schedule_seconds:
            self._logger.info('RealTimeSampleTimer delay is back inside limit of %2f seconds (mdib version=%d)',
                              self.WARN_LIMIT_REALTIMESAMPLES_BEHIND_SCHEDULE, self._mdib.mdib_version)
            self._last_logged_delay = behind_schedule_seconds
            self._last_log_time = now
        elif behind_schedule_seconds >= self.WARN_LIMIT_REALTIMESAMPLES_BEHIND_SCHEDULE:
            self._logger.warning('RealTimeSampleTimer is %.4f seconds behind schedule (mdib version=%d)',
                                 behind_schedule_seconds, self._mdib.mdib_version)
            self._last_logged_delay = behind_schedule_seconds
            self._last_log_time = now
