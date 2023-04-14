from time import monotonic, sleep


class IntervalTimer:
    """ this is a timer that does not drift (but is has jitter). """

    def __init__(self, period_in_seconds, activeWaitLimit=0.0):
        self._period = period_in_seconds
        self._active_wait_limit = activeWaitLimit
        self._next_interval_start = monotonic() + self._period

    def set_period(self, period):
        self._period = period

    def reset(self):
        self._next_interval_start = monotonic() + self._period

    def wait_next_interval_begin(self):
        """
        :return: 0.0 if timer is in scheduled plan, otherwise seconds how far timer is behind schedule
        """
        behind_schedule = 0.0
        dt_remaining = self.remaining_time()
        if dt_remaining <= 0:
            behind_schedule = abs(dt_remaining)
        elif dt_remaining > self._active_wait_limit:
            # normal sleep
            sleep(dt_remaining)
        else:
            # active wait, time is too short for a sleep call
            while dt_remaining > 0:
                dt_remaining = self.remaining_time()
        self._next_interval_start += self._period
        return behind_schedule

    def remaining_time(self):
        return self._next_interval_start - monotonic()
