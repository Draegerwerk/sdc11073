"""Tests for the deferred request handler of the SDC consumer."""

import queue
import threading
import time
import unittest
from unittest import mock

from sdc11073.consumer.request_handler_deferred import DispatchKeyRegistryDeferred, EmptyResponse
from sdc11073.dispatch import RequestData
from sdc11073.dispatch.dispatchkey import DispatchKey

LOG_PREFIX = '_test_deferred'
# other tests may leave a consumer running, so only threads of the dispatcher under test are relevant here
WORKER_NAME = f'ConsumerNotificationsWorker{LOG_PREFIX}'
ACTION = 'my_action'


def _mk_request(action: str = ACTION) -> RequestData:
    """Return a request that is dispatched to the handler registered for DispatchKey(action, None)."""
    request_data = RequestData({}, '/foo', 'peer')
    request_data.message_data = mock.Mock(action=action, q_name=None)
    return request_data


def _own_worker_threads() -> list[threading.Thread]:
    """Return the living worker threads of the dispatcher under test."""
    return [t for t in threading.enumerate() if t.name == WORKER_NAME]


class TestDispatchKeyRegistryDeferred(unittest.TestCase):
    def setUp(self):
        self.dispatcher = DispatchKeyRegistryDeferred(LOG_PREFIX)
        self.handled = []
        self.dispatcher.register_post_handler(DispatchKey(ACTION, None), self._handler)

    def tearDown(self):
        self.dispatcher.stop()

    def _handler(self, request_data: RequestData) -> EmptyResponse:
        self.handled.append(request_data)
        return EmptyResponse()

    def test_no_worker_thread_after_construction(self):
        """Verify that constructing the dispatcher does not create a worker thread."""
        self.assertIsNone(self.dispatcher._worker)
        self.assertEqual(_own_worker_threads(), [])

    def test_start_creates_named_daemon_thread(self):
        """Verify that start creates a running, named daemon thread."""
        self.dispatcher.start()
        worker = self.dispatcher._worker
        self.assertTrue(worker.is_alive())
        self.assertTrue(worker.daemon)
        self.assertEqual(worker.name, WORKER_NAME)

    def test_stop_ends_worker_thread(self):
        """Verify that stop ends the worker thread and forgets it."""
        self.dispatcher.start()
        worker = self.dispatcher._worker
        self.dispatcher.stop()
        self.assertFalse(worker.is_alive())
        self.assertIsNone(self.dispatcher._worker)
        self.assertEqual(_own_worker_threads(), [])

    def test_stop_twice_does_nothing(self):
        """Verify that stop can be called twice without an error."""
        self.dispatcher.start()
        self.dispatcher.stop()
        self.dispatcher.stop()
        self.assertIsNone(self.dispatcher._worker)

    def test_stop_without_start_does_nothing(self):
        """Verify that stop can be called if start was never called."""
        self.dispatcher.stop()
        self.assertIsNone(self.dispatcher._worker)

    def test_start_after_stop_revives_worker(self):
        """Verify that a dispatcher can be restarted, as needed by SdcConsumer.restart."""
        self.dispatcher.start()
        self.dispatcher.stop()
        self.dispatcher.start()
        worker = self.dispatcher._worker
        self.assertTrue(worker.is_alive())
        self.dispatcher.on_post(_mk_request())
        self.dispatcher.stop()
        self.assertEqual(len(self.handled), 1)

    def test_queued_requests_are_handled_before_stop_returns(self):
        """Verify that requests that are already queued are handled before the worker thread ends."""
        expected = 20
        blocker = threading.Event()

        def slow_handler(request_data: RequestData) -> EmptyResponse:
            blocker.wait(timeout=10)
            self.handled.append(request_data)
            return EmptyResponse()

        self.dispatcher.register_post_handler(DispatchKey(ACTION, None), slow_handler)
        self.dispatcher.start()
        for _ in range(expected):
            self.dispatcher.on_post(_mk_request())
        blocker.set()  # let the worker drain the queue
        self.dispatcher.stop()
        self.assertEqual(len(self.handled), expected)

    def test_request_after_stop_is_discarded(self):
        """Verify that a request that arrives after stop is answered with an empty response and discarded."""
        self.dispatcher.start()
        self.dispatcher.stop()
        response = self.dispatcher.on_post(_mk_request())
        self.assertIsInstance(response, EmptyResponse)
        self.assertEqual(response.serialize(), b'')
        self.assertEqual(self.handled, [])

    def test_request_before_start_is_discarded(self):
        """Verify that a request that arrives before start is answered with an empty response and discarded."""
        response = self.dispatcher.on_post(_mk_request())
        self.assertIsInstance(response, EmptyResponse)
        self.assertEqual(self.handled, [])

    def test_stop_from_handler_does_not_raise(self):
        """Verify that a handler can call stop, although the worker thread cannot join itself."""
        errors = []

        def stopping_handler(request_data: RequestData) -> EmptyResponse:  # noqa: ARG001
            try:
                self.dispatcher.stop()
            except BaseException as ex:  # noqa: BLE001
                errors.append(ex)
            return EmptyResponse()

        self.dispatcher.register_post_handler(DispatchKey(ACTION, None), stopping_handler)
        self.dispatcher.start()
        worker = self.dispatcher._worker
        self.dispatcher.on_post(_mk_request())
        worker.join(timeout=10)
        self.assertEqual(errors, [])
        self.assertFalse(worker.is_alive())
        self.assertIsNone(self.dispatcher._worker)

    def test_on_post_does_not_block_if_queue_is_full(self):
        """Verify that on_post returns instead of blocking forever if no free queue slot is available."""
        blocker = threading.Event()

        def blocking_handler(request_data: RequestData) -> EmptyResponse:  # noqa: ARG001
            blocker.wait(timeout=10)
            return EmptyResponse()

        self.dispatcher.register_post_handler(DispatchKey(ACTION, None), blocking_handler)
        self.dispatcher._queue = queue.Queue(1)
        self.dispatcher.start()
        try:
            with mock.patch.object(DispatchKeyRegistryDeferred, 'PUT_TIMEOUT', 0.01):
                for _ in range(5):  # more requests than the worker and the queue can hold
                    begin = time.monotonic()
                    self.dispatcher.on_post(_mk_request())
                    self.assertLess(time.monotonic() - begin, 5)
        finally:
            blocker.set()

    def test_stop_returns_if_queue_is_full(self):
        """Verify that stop returns instead of blocking forever if the stop request cannot be queued."""
        blocker = threading.Event()

        def blocking_handler(request_data: RequestData) -> EmptyResponse:  # noqa: ARG001
            blocker.wait(timeout=10)
            return EmptyResponse()

        self.dispatcher.register_post_handler(DispatchKey(ACTION, None), blocking_handler)
        self.dispatcher._queue = queue.Queue(1)
        self.dispatcher.start()
        try:
            with mock.patch.object(DispatchKeyRegistryDeferred, 'PUT_TIMEOUT', 0.01):
                for _ in range(3):
                    self.dispatcher.on_post(_mk_request())  # worker is blocked, queue runs full
            with mock.patch.object(DispatchKeyRegistryDeferred, 'STOP_TIMEOUT', 0.01):
                begin = time.monotonic()
                self.dispatcher.stop()
                self.assertLess(time.monotonic() - begin, 5)
        finally:
            blocker.set()
