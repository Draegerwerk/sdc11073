"""Tests for SDC Consumer functionality."""

import threading
import unittest
from unittest import mock

from sdc11073.consumer.consumerimpl import SdcConsumer, default_components_factory
from sdc11073.consumer.subscription import ConsumerSubscriptionManager
from sdc11073.definitions_sdc import SdcV1Definitions
from sdc11073.dispatch import RequestDispatcher
from sdc11073.dispatch.pathelementregistry import PathElementRegistry
from sdc11073.exceptions import ApiUsageError
from sdc11073.httpserver import httpserverimpl
from sdc11073.httpserver.httpserverimpl import HttpServerThreadBase
from tests import utils

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

    def test_failing_stop_step_does_not_skip_the_others(self):
        """Verify that stop_all executes all steps if one fails, and raises the first failure afterwards."""
        consumer = SdcConsumer('http', SdcV1Definitions, None, log_prefix=LOG_PREFIX)
        consumer._services_dispatcher.start()
        subscription_mgr = mock.Mock()
        subscription_mgr.unsubscribe_all.side_effect = KeyError('unsubscribe failed')
        subscription_mgr.stop.side_effect = ValueError('stop failed')
        consumer._subscription_mgr = subscription_mgr
        soap_client = mock.Mock()
        consumer._soap_clients = {'key': soap_client}
        with self.assertRaises(KeyError):
            consumer.stop_all()
        subscription_mgr.stop.assert_called_once()
        self.assertIsNone(consumer._services_dispatcher._worker)
        self.assertEqual(_own_worker_threads(), [])
        soap_client.close.assert_called_once()
        self.assertEqual(consumer._soap_clients, {})


class TestConsumerStartFailure(unittest.TestCase):
    """Verify that a start_all that fails after the first thread was started leaves no thread behind."""

    def setUp(self):
        self.consumer = SdcConsumer('http://127.0.0.1:1/', SdcV1Definitions, None, log_prefix=LOG_PREFIX)
        # replace everything that would talk to a provider, start_all then fails while starting the event sink
        soap_client = mock.Mock(sock_name=('127.0.0.1', 0))
        for name, value in (
            ('_connect', None),
            ('_get_metadata', mock.Mock()),
            ('_mk_hosted_services', None),
            ('get_soap_client', soap_client),
        ):
            patcher = mock.patch.object(self.consumer, name, return_value=value)
            patcher.start()
            self.addCleanup(patcher.stop)

    def tearDown(self):
        self.consumer.stop_all()

    def test_http_server_start_timeout_stops_worker_and_http_server(self):
        """Verify that the worker and the http server are stopped if the internal http server does not start in time.

        The http server is still initializing when it is stopped, so it must not start serving afterwards.
        """
        release_init = threading.Event()
        original_init = httpserverimpl._ThreadingHTTPServer.__init__

        def delayed_init(server, *args, **kwargs):  # noqa: ANN001, ANN002, ANN003
            release_init.wait(timeout=10)
            original_init(server, *args, **kwargs)

        with mock.patch.object(httpserverimpl._ThreadingHTTPServer, '__init__', delayed_init):
            try:
                with self.assertRaises(RuntimeError) as context:
                    self.consumer.start_all(check_get_service=False, http_server_start_timeout=0.01)
            finally:
                release_init.set()
            http_server = self.consumer._http_server
            self.assertTrue(utils.wait_for(lambda: not http_server.is_alive(), timeout=10))
        self.assertIn('Http server could not be started', str(context.exception))
        self.assertFalse(http_server.started_evt.is_set())
        self.assertEqual(http_server.httpd.socket.fileno(), -1)  # the listening socket is closed
        self.assertIsNone(self.consumer._services_dispatcher._worker)
        self.assertEqual(_own_worker_threads(), [])

    def test_subscription_manager_start_failure_stops_worker_and_http_server(self):
        """Verify that the worker and the http server are stopped if the subscription manager cannot be started.

        The thread of the subscription manager was never started, so stopping it must not fail the cleanup.
        """
        with (
            mock.patch.object(ConsumerSubscriptionManager, 'start', side_effect=RuntimeError("can't start new thread")),
            self.assertRaises(RuntimeError) as context,
        ):
            self.consumer.start_all(check_get_service=False)
        self.assertIn("can't start new thread", str(context.exception))
        http_server = self.consumer._http_server
        self.assertTrue(utils.wait_for(lambda: not http_server.is_alive(), timeout=10))
        self.assertIsNone(self.consumer._services_dispatcher._worker)
        self.assertEqual(_own_worker_threads(), [])

    def test_failed_registration_stops_worker_and_http_server(self):
        """Verify that the worker and the internal http server are stopped if the registration fails.

        This is what happens if another consumer with the same path prefix already uses a shared http server.
        """
        with (
            mock.patch.object(PathElementRegistry, 'register_instance', side_effect=ApiUsageError('duplicate')),
            self.assertRaises(ApiUsageError),
        ):
            self.consumer.start_all(check_get_service=False)
        http_server = self.consumer._http_server
        self.assertIsNotNone(http_server)
        self.assertTrue(utils.wait_for(lambda: not http_server.is_alive(), timeout=10))
        self.assertIsNone(self.consumer._services_dispatcher._worker)
        self.assertEqual(_own_worker_threads(), [])
