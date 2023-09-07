import threading

from .observables import bind, unbind


class Error(Exception):
    """Base class for all ValuesCollector-related exceptions."""


class CancelledError(Error):
    """The ValuesCollector was cancelled."""


class CollectTimeoutError(Error):
    """The operation exceeded the given deadline."""


class SingleValueCollector:
    """collects next data item from an observable.
    usage:
    assuming myObj has an ObservableProperty named 'myProperty':
    collector = SingleValueCollector(myObj, 'myProperty') # collector will now retrieve and store the value of the next set value of observable property
    result = collector.result(timeout=0.1) # wait until result is available (or timeout
        some other thread:     myObj.myProperty = 42
    => now call of  collector.result() returns, result = 42.
    """

    # Possible states
    PENDING = 'PENDING'
    FINISHED = 'FINISHED'
    CLOSED = 'CLOSED'

    def __init__(self, obj, propName):
        self._obj = obj
        self._prop_name = propName
        self._cond = threading.Condition()
        bind(obj, **{propName: self._on_data})
        self._state = self.PENDING
        self._result = None

    def _on_data(self, data):
        if self._state == self.CLOSED:
            return
        with self._cond:
            self._result = data
            self._state = self.FINISHED
            unbind(self._obj, **{self._prop_name: self._on_data})
            self._cond.notify_all()

    def result(self, timeout=None):
        if self._state == self.CLOSED:
            raise RuntimeError('SingleValueCollector is already closed')
        with self._cond:
            if self._state == self.FINISHED:
                self._state = self.CLOSED
                return self._result

            self._cond.wait(timeout)

            if self._state == self.FINISHED:
                self._state = self.CLOSED
                return self._result
            unbind(self._obj, **{self._prop_name: self._on_data})
            self._state = self.CLOSED
            raise CollectTimeoutError

    def restart(self):
        """Start to capture another value."""
        if self._state != self.CLOSED:
            raise RuntimeError('SingleValueCollector is still active')
        bind(self._obj, **{self._prop_name: self._on_data})
        self._state = self.PENDING
        self._result = None


class ValuesCollector(SingleValueCollector):
    """collects multiple data from an observable.
    usage:
    assuming myObj has an ObservableProperty named 'myProperty':
    collector = ValuesCollector(myObj, 'myProperty', 2) # collector will now retrieve and store the value of the next 2 set value of observable property
    result = collector.result(timeout=0.1) # wait until result is available (or timeout
        some other thread:     myObj.myProperty = 42
                               myObj.myProperty = 43
    => now call of  collector.result() returns, result = [42, 43].
    """

    def __init__(self, obj, propName, n):
        super().__init__(obj, propName)
        self._n = n
        self._result = []

    def _on_data(self, data):
        if self._state == self.CLOSED:
            return
        with self._cond:
            if self._state == self.FINISHED:
                return
            self._result.append(data)
            if len(self._result) >= self._n:
                self._state = self.FINISHED
                unbind(self._obj, **{self._prop_name: self._on_data})
                self._cond.notify_all()
