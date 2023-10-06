import itertools
import math
from typing import Callable

CurveGenerator = Callable[[float, float, int], list[float]]


def sinus(min_value: float, max_value: float, samples: int) -> list[float]:
    """Return a list of values for one sinus curve period."""
    delta = 2 * math.pi / samples
    _values = [math.sin(i * delta) for i in range(samples)]  # -1 ... +1
    return [(n + 1) / 2.0 * (max_value - min_value) + min_value for n in _values]  # min ... max


def sawtooth(min_value: float, max_value: float, samples: int) -> list[float]:
    """Return a list of values for one sawtooth curve period."""
    delta = (max_value - min_value) / float(samples)
    return [min_value + i * delta for i in range(samples)]


def triangle(min_value: float, max_value: float, samples: int) -> list[float]:
    """Return a list of values for one triangle curve period."""
    min_value = float(min_value)
    max_value = float(max_value)
    delta = (max_value - min_value) / float(samples) * 2
    samples_cnt = int(samples / 2)
    return [min_value + i * delta for i in range(samples_cnt)] + [max_value - i * delta for i in range(samples_cnt)]


class WaveformGeneratorBase:
    """Generator of infinite curve, data is provided by a curve generator."""

    def __init__(self,
                 values_generator: CurveGenerator,
                 min_value: float,
                 max_value: float,
                 waveform_period: float,
                 sample_period: float):
        if sample_period >= waveform_period:
            raise ValueError(
                f'Choose a waveformperiod > sampleperiod. currently have wp={waveform_period}, sp={sample_period}')
        if sample_period <= 0 or waveform_period <= 0:
            raise ValueError('no values <= 0 allowed for sample_period and waveform_period')
        self.sample_period = sample_period
        samples = int(waveform_period / sample_period)
        self._values = values_generator(min_value, max_value, samples)
        self._generator = itertools.cycle(self._values)

    def next_samples(self, count: int) -> list[float]:
        """Get next values from generator."""
        return [next(self._generator) for _ in range(count)]


class TriangleGenerator(WaveformGeneratorBase):
    """Generator of infinite triangle curve."""

    def __init__(self, min_value: float, max_value: float, waveform_period: float, sample_period: float):
        super().__init__(triangle, min_value, max_value, waveform_period, sample_period)


class SawtoothGenerator(WaveformGeneratorBase):
    """Generator of infinite saw tooth curve."""

    def __init__(self, min_value: float, max_value: float, waveform_period: float, sample_period: float):
        super().__init__(sawtooth, min_value, max_value, waveform_period, sample_period)


class SinusGenerator(WaveformGeneratorBase):
    """Generator of infinite sinus curve."""

    def __init__(self, min_value: float, max_value: float, waveform_period: float, sample_period: float):
        super().__init__(sinus, min_value, max_value, waveform_period, sample_period)
