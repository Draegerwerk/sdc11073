"""Tests for the deferred request handler of the SDC consumer."""

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

    def test_start_twice_keeps_the_same_worker(self):
        """Verify that start is idempotent, as required by RequestDispatcherProtocol."""
        self.dispatcher.start()
        worker = self.dispatcher._worker
        self.dispatcher.start()
        self.assertIs(self.dispatcher._worker, worker)
        self.assertEqual(len(_own_worker_threads()), 1)

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

    def test_on_post_is_not_delayed_by_an_idle_worker(self):
        """Verify that a worker thread waiting for the next request does not delay on_post.

        Regression test: the worker used to hold the queue lock while it waited for the next request, which starved
        the http thread that delivers requests for seconds.
        """
        count = 20
        self.dispatcher.start()
        begin = time.monotonic()
        for _ in range(count):
            self.dispatcher.on_post(_mk_request())
        duration = time.monotonic() - begin
        # a starving on_post waits at least GET_TIMEOUT per request, usually a multiple of it
        self.assertLess(duration, count * DispatchKeyRegistryDeferred.GET_TIMEOUT)
        self.assertLess(duration, 1.0)

    def test_request_after_stop_is_rejected(self):
        """Verify that a request that arrives after stop is rejected instead of being silently dropped."""
        self.dispatcher.start()
        self.dispatcher.stop()
        with self.assertRaises(RuntimeError):
            self.dispatcher.on_post(_mk_request())
        self.assertEqual(self.handled, [])

    def test_request_before_start_is_rejected(self):
        """Verify that a request that arrives before start is rejected instead of being silently dropped."""
        with self.assertRaises(RuntimeError):
            self.dispatcher.on_post(_mk_request())
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

    def test_concurrent_stop_does_not_raise(self):
        """Verify that stop can be called from several threads at once."""
        errors = []

        def call_stop():
            try:
                self.dispatcher.stop()
            except BaseException as ex:  # noqa: BLE001
                errors.append(ex)

        self.dispatcher.start()
        threads = [threading.Thread(target=call_stop) for _ in range(5)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)
        self.assertEqual(errors, [])
        self.assertIsNone(self.dispatcher._worker)
        self.assertEqual(_own_worker_threads(), [])

    def test_on_post_does_not_block_if_queue_is_full(self):
        """Verify that on_post rejects a request instead of blocking forever if no free queue slot is available."""
        blocker = threading.Event()
        rejected = 0

        def blocking_handler(request_data: RequestData) -> EmptyResponse:  # noqa: ARG001
            blocker.wait(timeout=10)
            return EmptyResponse()

        self.dispatcher.register_post_handler(DispatchKey(ACTION, None), blocking_handler)
        try:
            with mock.patch.object(DispatchKeyRegistryDeferred, 'QUEUE_SIZE', 1):
                self.dispatcher.start()
            with mock.patch.object(DispatchKeyRegistryDeferred, 'PUT_TIMEOUT', 0.01):
                for _ in range(5):  # more requests than the worker and the queue can hold
                    begin = time.monotonic()
                    try:
                        self.dispatcher.on_post(_mk_request())
                    except RuntimeError:
                        rejected += 1
                    self.assertLess(time.monotonic() - begin, 5)
        finally:
            blocker.set()
        self.assertGreater(rejected, 0)

    def test_stop_returns_if_a_handler_blocks(self):
        """Verify that stop returns within STOP_TIMEOUT instead of waiting for a blocked handler.

        The queued requests are discarded in this case, so that the worker of the stopped dispatcher cannot keep
        handling requests while a restarted dispatcher is already running.
        """
        blocker = threading.Event()
        handler_entered = threading.Event()

        def blocking_handler(request_data: RequestData) -> EmptyResponse:  # noqa: ARG001
            handler_entered.set()
            blocker.wait(timeout=10)
            return EmptyResponse()

        self.dispatcher.register_post_handler(DispatchKey(ACTION, None), blocking_handler)
        self.dispatcher.start()
        worker = self.dispatcher._worker
        try:
            self.dispatcher.on_post(_mk_request())
            self.assertTrue(handler_entered.wait(timeout=10))
            self.dispatcher.on_post(_mk_request())  # waits in the queue, will be discarded
            with mock.patch.object(DispatchKeyRegistryDeferred, 'STOP_TIMEOUT', 0.01):
                begin = time.monotonic()
                self.dispatcher.stop()
                self.assertLess(time.monotonic() - begin, 5)
            self.assertTrue(worker.is_alive())  # it is still blocked in the handler
        finally:
            blocker.set()
        worker.join(timeout=10)
        self.assertFalse(worker.is_alive())  # it ends on its own, without a further stop

    def test_start_after_a_timed_out_stop_uses_a_new_worker(self):
        """Verify that a worker that outlived stop neither is revived nor reads the queue of the new worker."""
        blocker = threading.Event()
        handler_entered = threading.Event()

        def blocking_handler(request_data: RequestData) -> EmptyResponse:
            handler_entered.set()
            blocker.wait(timeout=10)
            self.handled.append(request_data)
            return EmptyResponse()

        self.dispatcher.register_post_handler(DispatchKey(ACTION, None), blocking_handler)
        self.dispatcher.start()
        old_worker = self.dispatcher._worker
        try:
            self.dispatcher.on_post(_mk_request())
            self.assertTrue(handler_entered.wait(timeout=10))
            with mock.patch.object(DispatchKeyRegistryDeferred, 'STOP_TIMEOUT', 0.01):
                self.dispatcher.stop()
            self.assertTrue(old_worker.is_alive())

            self.dispatcher.start()  # the old worker must not be reused and must not be revived
            new_worker = self.dispatcher._worker
            self.assertIsNot(new_worker, old_worker)
            self.assertTrue(new_worker.is_alive())
            self.dispatcher.on_post(_mk_request())
        finally:
            blocker.set()
        old_worker.join(timeout=10)
        self.assertFalse(old_worker.is_alive())
        self.dispatcher.stop()
        # each worker handled exactly the one request it had accepted
        self.assertEqual(len(self.handled), 2)
        self.assertEqual(_own_worker_threads(), [])
