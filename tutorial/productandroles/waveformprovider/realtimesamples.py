"""RtSampleArray and Annotator implementation."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sdc11073.provider.protocols.waveformprotocol import AnnotatorProtocol, RtSampleArrayProtocol

if TYPE_CHECKING:
    from collections.abc import Iterable

    from sdc11073.definitions_base import AbstractDataModel
    from sdc11073.xml_types.pm_types import Annotation, ComponentActivation


class RtSampleArray(RtSampleArrayProtocol):
    """RtSampleArray contains a list of waveform values plus time stamps and annotations.

    It is the output of the waveform provider and contains all data that is needed to create waveform notifications.
    """

    def __init__(
        self,
        model: AbstractDataModel,
        determination_time: float | None,
        sample_period: float,
        samples: list[float],
        activation_state: ComponentActivation,
    ):
        """Construct a RtSampleArray.

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

    def _nearest_index(self, timestamp: float) -> int | None:
        """Find the realtime sample that's determination time is nearest to given timestamp."""
        if self.determination_time is None:  # when deactivated, determinationTime is None
            return None
        # check if timestamp is too old. Accept 0.5*sample period as tolerance.
        # self.determination_time is the timestamp of the first element in the array
        delta = self.sample_period * 0.5
        if timestamp < (self.determination_time - delta):
            return None  # timestamp too small (== to old)
        youngest_determination_time = self.determination_time + len(self.samples) * self.sample_period
        # check if timestamp is too young. Accept 0.5*sample period as tolerance.
        if timestamp >= youngest_determination_time + delta:
            return None  # timestamp too big (== to young)
        # calculate the position
        pos = (timestamp - self.determination_time) / self.sample_period
        return int(pos) + 1 if pos % 1 >= 0.5 else int(pos)  # return the nearest element  # noqa: PLR2004

    def add_annotations_at(self, annotation: Annotation, timestamps: Iterable[float]):
        """Add annotation at the waveform samples nearest to timestamps."""
        applied = False
        annotation_index = len(self.annotations)  # Index is zero-based
        for timestamp in timestamps:
            i = self._nearest_index(timestamp)
            if i is not None:
                self.apply_annotations.append(self._model.pm_types.ApplyAnnotation(annotation_index, i))
                applied = True
        if applied:
            self.annotations.append(annotation)


class Annotator(AnnotatorProtocol):
    """Annotator is a sample of how to apply annotations.

    This annotator triggers an annotation when the value changes from <= 0 to > 0.
    """

    def __init__(self, annotation: Annotation, trigger_handle: str, annotated_handles: list[str]) -> None:
        """Construct an annotator.

        :param annotation: Annotation
        :param trigger_handle: the handle of the state that triggers an annotation
        :param annotated_handles: list of handles that get annotated
        """
        self.annotation = annotation
        self.trigger_handle = trigger_handle
        self.annotated_handles = annotated_handles
        self._last_value = 0.0

    def get_annotation_timestamps(self, rt_sample_array: RtSampleArrayProtocol) -> list[float]:
        """Analyze the rt_sample_array and return timestamps for annotations."""
        ret = []
        for i, rt_sample in enumerate(rt_sample_array.samples):
            if self._last_value <= 0 < rt_sample:
                ret.append(rt_sample_array.determination_time + i * rt_sample_array.sample_period)
            self._last_value = rt_sample
        return ret
