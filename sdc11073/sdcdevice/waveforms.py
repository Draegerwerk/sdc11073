import itertools
import math
import time
import threading
import traceback
from . import intervaltimer


def sinus(min_value, max_value, samples):
    delta = 2 * math.pi / samples
    _values = [math.sin(i * delta) for i in range(samples)]  # -1 ... +1
    values = [(n + 1) / 2.0 * (max_value - min_value) + min_value for n in _values]  # min ... max
    return values


def sawtooth(min_value, max_value, samples):
    delta = (max_value - min_value) / float(samples)
    values = [min_value + i * delta for i in range(samples)]
    return values


def triangle(min_value, max_value, samples):
    min_value = float(min_value)
    max_value = float(max_value)
    delta = (max_value - min_value) / float(samples) * 2
    samples_cnt = int(samples / 2)
    values = [min_value + i * delta for i in range(samples_cnt)] + [max_value - i * delta for i in range(samples_cnt)]
    return values


class WaveformGeneratorBase:
    def __init__(self, values_generator, min_value, max_value, waveformperiod, sampleperiod):
        if sampleperiod >= waveformperiod:
            raise ValueError(
                f'please choose a waveformperiod >> sampleperiod. currently use have wp={waveformperiod}, sp={sampleperiod}')
        self.sampleperiod = sampleperiod
        samples = int(waveformperiod / sampleperiod)
        self._values = values_generator(min_value, max_value, samples)
        self._generator = itertools.cycle(self._values)

    def next_samples(self, count):
        return [next(self._generator) for _ in range(count)]


class TriangleGenerator(WaveformGeneratorBase):
    def __init__(self, min_value, max_value, waveformperiod, sampleperiod):
        super().__init__(triangle, min_value, max_value, waveformperiod, sampleperiod)


class SawtoothGenerator(WaveformGeneratorBase):
    def __init__(self, min_value, max_value, waveformperiod, sampleperiod):
        super().__init__(sawtooth, min_value, max_value, waveformperiod, sampleperiod)


class SinusGenerator(WaveformGeneratorBase):
    def __init__(self, min_value, max_value, waveformperiod, sampleperiod):
        super().__init__(sinus, min_value, max_value, waveformperiod, sampleperiod)


class WaveformSender:
    WARN_LIMIT_REALTIMESAMPLES_BEHIND_SCHEDULE = 0.2  # warn limit when real time samples cannot be sent in time (typically because receiver is too slow)
    WARN_RATE_REALTIMESAMPLES_BEHIND_SCHEDULE = 5  # max. every x seconds a message

    def __init__(self, mdib, logger, collect_rt_samples_period):
        """"""
        self._mdib = mdib
        self._logger = logger
        self.collect_rt_samples_period = collect_rt_samples_period
        self._run_loop = False
        self._thread = None
        self._last_log_time = 0
        self._last_logged_delay = 0

    def start(self):
        self._run_loop = True
        self._thread = threading.Thread(target=self._rt_sample_send_loop,
                                        name='DevPeriodicSendLoop')
        self._thread.daemon = True
        self._thread.start()

    def stop(self):
        self._run_loop = False

    def _rt_sample_send_loop(self):
        """Periodically send waveform samples."""
        # start delayed in order to have a fully initialized device when waveforms start
        # (otherwise timing issues might happen)
        time.sleep(0.1)
        timer = intervaltimer.IntervalTimer(period_in_seconds=self.collect_rt_samples_period)
        try:
            while self._run_loop:
                behind_schedule_seconds = timer.wait_next_interval_begin()
                try:
                    self._mdib.xtra.update_all_rt_samples()  # update from waveform generators
                    self._log_waveform_timing(behind_schedule_seconds)
                except Exception:
                    self._logger.warn(' could not update real time samples: {}', traceback.format_exc())
            self._logger.info('_run_rt_sample_thread = False')
        finally:
            self._logger.info('rt_sample_sendloop end')

    def _log_waveform_timing(self, behind_schedule_seconds):
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
        #if last_logged_delay >= self.WARN_LIMIT_REALTIMESAMPLES_BEHIND_SCHEDULE and behind_schedule_seconds < self.WARN_LIMIT_REALTIMESAMPLES_BEHIND_SCHEDULE:
        if last_logged_delay >= self.WARN_LIMIT_REALTIMESAMPLES_BEHIND_SCHEDULE > behind_schedule_seconds:
            self._logger.info('RealTimeSampleTimer delay is back inside limit of {:.2f} seconds (mdib version={}',
                              self.WARN_LIMIT_REALTIMESAMPLES_BEHIND_SCHEDULE, self._mdib.mdib_version)
            self._last_logged_delay = behind_schedule_seconds
            self._last_log_time = now
        elif behind_schedule_seconds >= self.WARN_LIMIT_REALTIMESAMPLES_BEHIND_SCHEDULE:
            self._logger.warn('RealTimeSampleTimer is {:.4f} seconds behind schedule (mdib version={})',
                              behind_schedule_seconds, self._mdib.mdib_version)
            self._last_logged_delay = behind_schedule_seconds
            self._last_log_time = now
