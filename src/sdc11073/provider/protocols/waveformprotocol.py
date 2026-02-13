"""Protocols for waveform providers."""

from collections.abc import Callable, Iterable
from typing import Any, Protocol

from sdc11073.definitions_base import AbstractDataModel
from sdc11073.mdib.providermdibprotocol import ProviderMdibProtocol
from sdc11073.xml_types.pm_types import Annotation, ComponentActivation

CurveGeneratorCallable = Callable[[float, float, int], list[float]]


class WaveformGeneratorProtocol(Protocol):
    """Generator of infinite curve, data is provided by a curve generator."""

    def __init__(
        self,
        values_generator: CurveGeneratorCallable,
        min_value: float,
        max_value: float,
        waveform_period: float,
        sample_period: float,
    ):
        """Initialize waveform generator."""
        ...

    def next_samples(self, count: int) -> list[float]:
        """Get next values from generator."""
        ...


class RtSampleArrayProtocol(Protocol):
    """RtSampleArray contains a list of waveform values plus time stamps and annotations.

    It is the output of the waveform provider and contains all data that is needed to create waveform notifications.
    """

    determination_time: float | None
    sample_period: float
    samples: list[float]
    activation_state: ComponentActivation

    def __init__(
        self,
        model: AbstractDataModel,
        determination_time: float | None,
        sample_period: float,
        samples: list[float],
        activation_state: ComponentActivation,
    ) -> None:
        """Construct a RtSampleArray."""
        ...

    def add_annotations_at(self, annotation: Annotation, timestamps: Iterable[float]):
        """Add annotations to the RtSampleArray at given timestamps."""
        ...


class AnnotatorProtocol(Protocol):
    """An Annotator adds Annotations to waveforms. It mimics things like start of inspiration cycle etc."""

    annotation: Annotation
    trigger_handle: str
    annotated_handles: list[str]

    def __init__(self, annotation: Annotation, trigger_handle: str, annotated_handles: list[str]) -> None:
        """Construct an annotator."""
        ...

    def get_annotation_timestamps(self, rt_sample_array: RtSampleArrayProtocol) -> list[float]:
        """Analyze the rt_sample_array and return timestamps for annotations.

        Return a list of timestamps, can be empty.
        """
        ...


class WaveformProviderProtocol(Protocol):
    """WaveformProvider is not a role provider of a product, it is separate.

    The SdcProvider expects this interface.
    """

    is_running: bool

    def __init__(self, mdib: ProviderMdibProtocol, log_prefix: str): ...

    def register_waveform_generator(self, descriptor_handle: str, wf_generator: WaveformGeneratorProtocol):
        """Add waveform generator to waveform sources."""
        ...

    def add_annotation_generator(
        self,
        coded_value: Any,
        trigger_handle: str,
        annotated_handles: list[str],
    ) -> AnnotatorProtocol:
        """Add annotator to list of annotators."""
        ...

    def start(self):
        """Start whatever needs to be started in implementation."""
        ...

    def stop(self):
        """Stop whatever needs to be stopped in implementation."""
        ...

    def set_activation_state(self, descriptor_handle: str, component_activation_state: ComponentActivation):
        """Set the activation state of waveform generator and of Metric state in mdib."""
        ...

    def update_all_realtime_samples(self):
        """Update all realtime sample states that have a waveform generator registered."""
        ...
