from time import monotonic, sleep

class IntervalTimer(object):
    ''' this is a timer that does not drift (but is has jitter). '''
    VERBOSE = 0                    
    def __init__(self, periodInSeconds, activeWaitLimit=0.0):
        self._period = periodInSeconds
        self._activeWaitLimit = activeWaitLimit
        self._nextIntervalStart = monotonic() + self._period

    
    def setPeriod(self, period):
        self._period = period

        
    def reset(self):
        self._nextIntervalStart = monotonic() + self._period

    
    def waitForNextIntervalBegin(self):
        '''
        @param return: 0.0 if timer is in scheduled plan, otherwise seconds how far timer is behind schedule
        '''
        behindSchedule = 0.0
        now = monotonic()
        dt = self._nextIntervalStart - now
        if dt <= 0:
            behindSchedule = abs(dt)
        elif dt > self._activeWaitLimit:
            # normal sleep
            sleep(dt)
        else:
            # active wait, time is too short for a sleep call
            while dt > 0:
                dt = self._nextIntervalStart - monotonic()
        self._nextIntervalStart += self._period 
        return behindSchedule
    sleep = waitForNextIntervalBegin  # alias
    
