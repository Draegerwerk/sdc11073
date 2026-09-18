"""Tests for SDC Consumer functionality."""

import threading
import unittest
from unittest import mock

from sdc11073.consumer.consumerimpl import SdcConsumer, default_components_factory
from sdc11073.definitions_sdc import SdcV1Definitions
from sdc11073.dispatch import RequestDispatcher
from sdc11073.httpserver.httpserverimpl import HttpServerThreadBase

LOG_PREFIX = '_test_consumer_worker'
# other tests may leave a consumer running, so only threads of the consumer under test are relevant here
WORKER_NAME = f'ConsumerNotificationsWorker{LOG_PREFIX}'


def _own_worker_threads() -> list[threading.Thread]:
    """Return the living notification worker threads of the consumer under test."""
    return [t for t in threading.enumerate() if t.name == WORKER_NAME]


class TestConsumerWithFailure(unittest.TestCase):
    def test_fail_on_startup(self):
        """Test that starting services fails with RuntimeError if HTTP server does not start in time."""
        with mock.patch.object(HttpServerThreadBase, 'start'):
            sdc_consumer = SdcConsumer('http', SdcV1Definitions, None)
            sdc_consumer._network_adapter = mock.Mock()
            sdc_consumer._network_adapter.ip = '123.456.789.000'

            with self.assertRaises(RuntimeError) as context:
                sdc_consumer._start_event_sink(shared_http_server=None, http_server_start_timeout=0)
            self.assertIn('Http server could not be started', str(context.exception))


class TestConsumerNotificationWorker(unittest.TestCase):
    """Verify that the worker thread of the deferred request handler is not leaked."""

    def test_construction_starts_no_worker_thread(self):
        """Verify that constructing a consumer does not start a notification worker thread."""
        consumer = SdcConsumer('http', SdcV1Definitions, None, log_prefix=LOG_PREFIX)
        try:
            self.assertIsNone(consumer._services_dispatcher._worker)
            self.assertEqual(_own_worker_threads(), [])
        finally:
            consumer.stop_all()

    def test_stop_all_stops_dispatcher(self):
        """Verify that stop_all ends the worker thread of the deferred request handler."""
        consumer = SdcConsumer('http', SdcV1Definitions, None, log_prefix=LOG_PREFIX)
        consumer._services_dispatcher.start()
        worker = consumer._services_dispatcher._worker
        self.assertTrue(worker.is_alive())
        consumer.stop_all()
        self.assertFalse(worker.is_alive())
        self.assertIsNone(consumer._services_dispatcher._worker)
        self.assertEqual(_own_worker_threads(), [])

    def test_stop_all_twice(self):
        """Verify that stop_all can be called twice, as some tests do in their tearDown."""
        consumer = SdcConsumer('http', SdcV1Definitions, None, log_prefix=LOG_PREFIX)
        consumer._services_dispatcher.start()
        consumer.stop_all()
        consumer.stop_all()
        self.assertIsNone(consumer._services_dispatcher._worker)

    def test_stop_all_with_non_deferred_dispatcher(self):
        """Verify that stop_all also works if the action dispatcher handles requests inline."""
        components = default_components_factory()
        components.action_dispatcher_class = RequestDispatcher
        consumer = SdcConsumer('http', SdcV1Definitions, None, components=components)
        self.assertIsInstance(consumer._services_dispatcher, RequestDispatcher)
        consumer.stop_all()  # must not raise
