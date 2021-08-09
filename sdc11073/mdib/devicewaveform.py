"""
This module contains the default implementation for waveform handling of the device.
The sdc device periodically calls mdib.update_all_rt_samples method, which itself calls same method
of its waveform source. It is the responsibility of this method to update the RealtimeSampleArrayStates
of the mdib.
"""
import time
from abc import ABC, abstractmethod
from typing import Iterable, List
from .. import pmtypes
from ..sdcdevice.waveforms import _WaveformGeneratorBase

class RtSampleArray:
    """ This class contains a list of waveform values plus time stamps and annotations.
    It is used to create Waveform notifications."""
    def __init__(self, determination_time: float, sample_period: float,
                 samples: List[tuple], activation_state: pmtypes.ComponentActivation):
        """
        :param determination_time: the time stamp of the first value in samples
        :param sample_period: the time difference between two samples
        :param samples: a list of 2-tuples (value (float or int), flag annotation_trigger)
        :param activation_state: one of pmtypes.ComponentActivation values
        """
        self.determination_time = determination_time
        self.sample_period = sample_period
        self.samples = samples
        self.activation_state = activation_state
        self.annotations = []
        self.apply_annotations = []

    def _nearest_index(self, timestamp: float):
        # first check if timestamp is outside the range of this sample array. Accept 0.5*sample period as tolerance.
        if self.determination_time is None:  # when deactivated, determinationTime is None
            return None
        if timestamp < (self.determination_time - self.sample_period * 0.5):
            return None
        if timestamp >= self.determination_time + len(self.samples) * self.sample_period + self.sample_period * 0.5:
            return None
        pos = (timestamp - self.determination_time) / self.sample_period
        return int(pos) + 1 if pos % 1 >= 0.5 else int(pos)

    def get_annotation_trigger_timestamps(self):
        """ returns the time stamps of all samples annotation_trigger set"""
        return [self.determination_time + i * self.sample_period for i, sample in enumerate(self.samples) if sample[1]]

    def add_annotations_at(self, annotation: pmtypes.Annotation, timestamps: Iterable[float]):
        """
        @param annotation: a pmtypes.Annotation instance
        @param timestamps: a list of time stamps (time.time based)
        """
        applied = False
        annotation_index = len(self.annotations)  # Index is zero-based
        for timestamp in timestamps:
            i = self._nearest_index(timestamp)
            if i is not None:
                self.apply_annotations.append(pmtypes.ApplyAnnotation(annotation_index, i))
                applied = True
        if applied:
            self.annotations.append(annotation)


class _SampleArrayGenerator:
    """Wraps a waveform generator and makes RtSampleArray objects"""
    def __init__(self, descriptor_handle: str, generator: _WaveformGeneratorBase):
        self._descriptor_handle = descriptor_handle
        self._last_timestamp = None
        self._activation_state = pmtypes.ComponentActivation.ON
        self._generator = generator
        self.current_rt_sample_array = None

    def set_activation_state(self, component_activation_state: pmtypes.ComponentActivation):
        """
        @param component_activation_state: one of pmtypes.ComponentActivation values
        """
        self._activation_state = component_activation_state
        if component_activation_state == pmtypes.ComponentActivation.ON:
            self._last_timestamp = time.time()

    def get_next_sample_array(self) -> RtSampleArray:
        """ Read sample values from waveform generator and calculate determination time.
        If activation state is not 'On', no samples are returned.
        @return: RtSampleArray instance"""
        if self._activation_state != pmtypes.ComponentActivation.ON:
            self.current_rt_sample_array = RtSampleArray(None, self._generator.sampleperiod, [], self._activation_state)
        else:
            now = time.time()
            observation_time = self._last_timestamp or now
            samples_count = int((now - observation_time) / self._generator.sampleperiod)
            samples = self._generator.next_samples(samples_count)
            self._last_timestamp = observation_time + self._generator.sampleperiod * samples_count
            self.current_rt_sample_array = RtSampleArray(
                observation_time, self._generator.sampleperiod, samples, self._activation_state)
        return self.current_rt_sample_array

    def set_waveform_generator(self, generator):
        self._generator = generator


class AbstractWaveformSource(ABC):
    """The methods declared by this abstract class are used by mdib. """
    @abstractmethod
    def update_all_realtime_samples(self, transaction):
        pass

    @abstractmethod
    def register_waveform_generator(self, mdib, descriptor_handle, wf_generator):
        pass

    @abstractmethod
    def set_activation_state(self, mdib, descriptor_handle, component_activation_state):
        pass


class DefaultWaveformSource(AbstractWaveformSource):
    """ This is the basic mechanism that reads data from waveform sources and applies it to mdib
    via real time transaction.
    Method 'update_all_realtime_samples' must be called periodically."""
    def __init__(self):
        self._waveform_generators = {}
        self._annotators = {}

    def update_all_realtime_samples(self, transaction):
        """ update all realtime sample states that have a waveform generator registered.
        On transaction commit the mdib will call the corresponding send method of the sdc device."""
        for descriptor_handle in self._waveform_generators:  # iterate over keys
            state = transaction.get_real_time_sample_array_metric_state(descriptor_handle)
            self._update_rt_samples(state)
        self._add_all_annotations()

    def register_waveform_generator(self, mdib, descriptor_handle, wf_generator):
        """
        param mdib: a device mdib instance
        @param descriptor_handle: the handle of the RealtimeSampelArray that shall accept this data
        @param wf_generator: a waveforms.WaveformGenerator instance
        """
        sample_period = wf_generator.sampleperiod
        descriptor_container = mdib.descriptions.handle.getOne(descriptor_handle)
        if descriptor_container.SamplePeriod != sample_period:
            # we must inform subscribers
            with mdib.transaction_manager() as trns:
                descr = trns.get_descriptor(descriptor_handle)
                descr.SamplePeriod = sample_period
        if descriptor_handle in self._waveform_generators:
            self._waveform_generators[descriptor_handle].set_waveform_generator(wf_generator)
        else:
            self._waveform_generators[descriptor_handle] = _SampleArrayGenerator(descriptor_handle, wf_generator)

    def set_activation_state(self, mdib, descriptor_handle, component_activation_state):
        """
        param mdib: a device mdib instance
        @param descriptorHandle: a handle string
        @param componentActivation: one of pmtypes.ComponentActivation values
        """
        self._waveform_generators[descriptor_handle].set_activation_state(component_activation_state)
        with mdib.transaction_manager() as trns:
            state = trns.get_state(descriptor_handle)
            state.ActivationState = component_activation_state

    def register_annotation_generator(self, annotator, trigger_handle, annotated_handles):
        """
        :param annotator: a pmtypes.Annotation instance
        :param trigger_handle: The handle of the waveform that triggers the annotator ( trigger = start of a waveform cycle)
        :param annotated_handles: the handles of the waveforms that shall be annotated.
        """
        self._annotators[trigger_handle] = (annotator, annotated_handles)

    def _update_rt_samples(self, state):
        """ update waveforms state from waveform generator (if available)"""
        wf_generator = self._waveform_generators.get(state.descriptorHandle)
        if wf_generator:
            rt_sample = wf_generator.get_next_sample_array()
            samples = [s[0] for s in rt_sample.samples]  # only the values without the 'start of cycle' flags
            if state.metricValue is None:
                state.mk_metric_value()
            state.metricValue.Samples = samples
            state.metricValue.DeterminationTime = rt_sample.determination_time
            state.metricValue.Annotations = rt_sample.annotations
            state.metricValue.ApplyAnnotations = rt_sample.apply_annotations
            state.ActivationState = rt_sample.activation_state

    def _add_all_annotations(self):
        """ add annotations to all current RtSampleArrays """
        rt_sample_arrays = {handle: g.current_rt_sample_array for (handle, g) in self._waveform_generators.items()}
        for src_handle, _annotator in self._annotators.items():
            if src_handle in rt_sample_arrays:
                annotation, dest_handles = _annotator
                timestamps = rt_sample_arrays[src_handle].get_annotation_trigger_timestamps()
                if timestamps:
                    for dest_handle in dest_handles:
                        if dest_handle in rt_sample_arrays:
                            rt_sample_arrays[dest_handle].add_annotations_at(annotation, timestamps)
