import unittest
from unittest import mock

from sdc11073.observableproperties import observables, valuecollector


class TestValueCollector(unittest.TestCase):
    class _Mocked:
        mock = observables.ObservableProperty()

    def test_raise_if_closed(self):
        collector = valuecollector.SingleValueCollector(self._Mocked(), 'mock')
        with mock.patch.object(collector, '_state', collector.CLOSED):
            self.assertRaises(RuntimeError, collector.result)

    def test_error_on_timeout(self):
        collector = valuecollector.SingleValueCollector(self._Mocked(), 'mock')
        self.assertRaises(valuecollector.CollectTimeoutError, collector.result, 0.01)

    def test_restart(self):
        mocked = self._Mocked()
        collector = valuecollector.SingleValueCollector(mocked, 'mock')
        self.assertRaises(RuntimeError, collector.restart)
        mocked.mock = 1
        self.assertEqual(collector.result(0.01), mocked.mock)
        collector.restart()
        self.assertRaises(RuntimeError, collector.restart)
