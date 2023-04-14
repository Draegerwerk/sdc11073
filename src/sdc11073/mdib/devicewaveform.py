"""
This module contains the default implementation for waveform handling of the device.
The sdc device periodically calls mdib.update_all_rt_samples method, which itself calls same method
of its waveform source. It is the responsibility of this method to update the RealtimeSampleArrayStates
of the mdib.
"""
import time
from abc import ABC, abstractmethod
from decimal import Context
from typing import Iterable, List, Type, Union

from ..sdcdevice.waveforms import WaveformGeneratorBase


class RtSampleArray:
    """ This class contains a list of waveform values plus time stamps and annotations.
    It is used to create Waveform notifications."""

    def __init__(self, model, determination_time: Union[float, None], sample_period: float,
                 samples: List[float], activation_state):
        """
        :param determination_time: the time stamp of the first value in samples, can be None if not active
        :param sample_period: the time difference between two samples
        :param samples: a list of 2-tuples (value (float or int), flag annotation_trigger)
        :param activation_state: one of pmtypes.ComponentActivation values
        """
        self._model = model
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

    def add_annotations_at(self, annotation, timestamps: Iterable[float]):
        """
        :param annotation: a pmtypes.Annotation instance
        :param timestamps: a list of time stamps (time.time based)
        """
        applied = False
        annotation_index = len(self.annotations)  # Index is zero-based
        for timestamp in timestamps:
            i = self._nearest_index(timestamp)
            if i is not None:
                self.apply_annotations.append(self._model.pm_types.ApplyAnnotation(annotation_index, i))
                applied = True
        if applied:
            self.annotations.append(annotation)


class AbstractAnnotator(ABC):
    def __init__(self, annotation, trigger_handle: str, annotated_handles: List[str]):
        """

        :param annotation:  a pmtypes.Annotation instance
        :param trigger_handle: the handle of the state that triggers an annotation
        :param annotated_handles: list of handles that get annotated
        """
        self.annotation = annotation
        self.trigger_handle = trigger_handle
        self.annotated_handles = annotated_handles

    @abstractmethod
    def get_annotation_timestamps(self, rt_sample_array: RtSampleArray) -> List[float]:
        """
        Analyzes the rt_sample_array and returns timestamps for annotations
        :param rt_sample_array: the RtSampleArray that is checked
        :return: list of timestamps, can be empty
        """
        pass


class Annotator(AbstractAnnotator):
    """
    This is sample of how to apply annotations. This annotator triggers an annotation when the value
    changes from <= 0 to > 0.
    """

    def __init__(self, annotation, trigger_handle: str, annotated_handles: List[str]):
        """

        :param annotation:: pmtypes.Annotation
        :param trigger_handle: the handle of the state that triggers an annotation
        :param annotated_handles: list of handles that get annotated
        """
        super().__init__(annotation, trigger_handle, annotated_handles)
        self._last_value = 0.0

    def get_annotation_timestamps(self, rt_sample_array: RtSampleArray) -> List[float]:
        """

        :param rt_sample_array:
        :return:
        """
        ret = []
        for i, rt_sample in enumerate(rt_sample_array.samples):
            if self._last_value <= 0 and rt_sample > 0:
                ret.append(rt_sample_array.determination_time + i * rt_sample_array.sample_period)
            self._last_value = rt_sample
        return ret


class _SampleArrayGenerator:
    """Wraps a waveform generator and makes RtSampleArray objects"""

    def __init__(self, model, descriptor_handle: str, generator: WaveformGeneratorBase):
        self._model = model
        self._descriptor_handle = descriptor_handle
        self._last_timestamp = None
        self._activation_state = model.pm_types.ComponentActivation.ON
        self._generator = generator
        self.current_rt_sample_array = None

    def set_activation_state(self, component_activation_state):
        """
        :param component_activation_state: one of pmtypes.ComponentActivation values
        """
        self._activation_state = component_activation_state
        if component_activation_state == self._model.pm_types.ComponentActivation.ON:
            self._last_timestamp = time.time()

    def get_next_sample_array(self) -> RtSampleArray:
        """ Read sample values from waveform generator and calculate determination time.
        If activation state is not 'On', no samples are returned.
        @return: RtSampleArray instance"""
        if self._activation_state != self._model.pm_types.ComponentActivation.ON:
            self.current_rt_sample_array = RtSampleArray(
                self._model, None, self._generator.sampleperiod, [], self._activation_state)
        else:
            now = time.time()
            observation_time = self._last_timestamp or now
            samples_count = int((now - observation_time) / self._generator.sampleperiod)
            samples = self._generator.next_samples(samples_count)
            self._last_timestamp = observation_time + self._generator.sampleperiod * samples_count
            self.current_rt_sample_array = RtSampleArray(
                self._model, observation_time, self._generator.sampleperiod, samples, self._activation_state)
        return self.current_rt_sample_array

    def set_waveform_generator(self, generator):
        self._generator = generator

    @property
    def is_active(self):
        return self._activation_state == self._model.pm_types.ComponentActivation.ON


class AbstractWaveformSource(ABC):
    """The methods declared by this abstract class are used by mdib. """

    def __init__(self, mdib):
        self._mdib = mdib

    @abstractmethod
    def update_all_realtime_samples(self, transaction):
        pass

    @abstractmethod
    def register_waveform_generator(self, descriptor_handle, wf_generator):
        pass

    @abstractmethod
    def set_activation_state(self, descriptor_handle, component_activation_state):
        pass


class DefaultWaveformSource(AbstractWaveformSource):
    """ This is the basic mechanism that reads data from waveform sources and applies it to mdib
    via real time transaction.
    Method 'update_all_realtime_samples' must be called periodically."""

    def __init__(self, mdib):
        super().__init__(mdib)
        self._waveform_generators = {}
        self._annotators = {}

    def update_all_realtime_samples(self, transaction):
        """ update all realtime sample states that have a waveform generator registered.
        On transaction commit the mdib will call the appropriate send method of the sdc device."""
        for descriptor_handle, wf_generator in self._waveform_generators.items():
            if wf_generator.is_active:
                state = transaction.get_real_time_sample_array_metric_state(descriptor_handle)
                self._update_rt_samples(state)
        self._add_all_annotations()

    def register_waveform_generator(self, descriptor_handle, wf_generator):
        """
        :param descriptor_handle: the handle of the RealtimeSampleArray that shall accept this data
        :param wf_generator: a waveforms.WaveformGenerator instance
        """
        sample_period = wf_generator.sampleperiod
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

    def set_activation_state(self, descriptor_handle, component_activation_state):
        """
        :param descriptor_handle: a handle string
        :param component_activation_state: one of pmtypes.ComponentActivation values
        """
        wf_generator = self._waveform_generators[descriptor_handle]
        wf_generator.set_activation_state(component_activation_state)
        with self._mdib.transaction_manager() as mgr:
            state = mgr.get_state(descriptor_handle)
            state.ActivationState = component_activation_state
            # if the generator is not active, there shall be no MetricValue
            if not wf_generator.is_active:
                state.MetricValue = None

    def register_annotation_generator(self, annotator: Type[AbstractAnnotator]):
        self._annotators[annotator.trigger_handle] = annotator

    def _update_rt_samples(self, state):
        """ update waveforms state from waveform generator (if available)"""
        ctxt = Context(prec=10)
        wf_generator = self._waveform_generators.get(state.DescriptorHandle)
        if wf_generator:
            rt_sample_array = wf_generator.get_next_sample_array()
            samples = [ctxt.create_decimal(s) for s in rt_sample_array.samples]
            if state.MetricValue is None:
                state.mk_metric_value()
            state.MetricValue.Samples = samples
            state.MetricValue.DeterminationTime = rt_sample_array.determination_time
            state.MetricValue.Annotations = rt_sample_array.annotations
            state.MetricValue.ApplyAnnotations = rt_sample_array.apply_annotations
            state.ActivationState = rt_sample_array.activation_state

    def _add_all_annotations(self):
        """ add annotations to all current RtSampleArrays """
        rt_sample_arrays = {handle: g.current_rt_sample_array for (handle, g) in self._waveform_generators.items()}
        for src_handle, _annotator in self._annotators.items():
            if src_handle in rt_sample_arrays:
                timestamps = _annotator.get_annotation_timestamps(rt_sample_arrays[src_handle])
                if timestamps:
                    for dest_handle in _annotator.annotated_handles:
                        if dest_handle in rt_sample_arrays:
                            rt_sample_arrays[dest_handle].add_annotations_at(_annotator.annotation, timestamps)
