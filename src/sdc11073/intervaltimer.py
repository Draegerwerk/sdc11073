from time import perf_counter, sleep


class IntervalTimer:
    """IntervalTimer is a timer that does not drift (but is has jitter)."""

    def __init__(self, period_in_seconds: float):
        self._period = period_in_seconds
        self._next_interval_start = perf_counter() + self._period

    def set_period(self, period: float):
        """Set timer period."""
        self._period = period

    def reset(self):
        """Reset timer."""
        self._next_interval_start = perf_counter() + self._period

    def wait_next_interval_begin(self) -> float:
        """Return when enough time is elapsed.

        :return: 0.0 if timer is in scheduled plan, otherwise seconds how far timer is behind schedule.
        """
        behind_schedule = 0.0
        dt_remaining = self.remaining_time()
        if dt_remaining <= 0:
            behind_schedule = abs(dt_remaining)
        else:
            sleep(dt_remaining)
        self._next_interval_start += self._period
        return behind_schedule

    def remaining_time(self) -> float:
        """Return remaining time until next period."""
        return self._next_interval_start - perf_counter()
