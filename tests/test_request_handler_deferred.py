"""Tests for the deferred request handler of the SDC consumer."""

import threading
import time
import unittest
from unittest import mock

from sdc11073.consumer.request_handler_deferred import (
    DispatchKeyRegistryDeferred,
    EmptyResponse,
    _ClosableQueue,
    _ClosedError,
)
from sdc11073.dispatch import RequestData
from sdc11073.dispatch.dispatchkey import DispatchKey
from tests import utils

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
        self.assertTrue(utils.wait_for(lambda: len(self.handled) == 1, timeout=10))

    def test_queued_requests_are_discarded_on_stop(self):
        """Verify that stop finishes the request that is being handled, but discards the queued ones."""
        blocker = threading.Event()
        handler_entered = threading.Event()

        def slow_handler(request_data: RequestData) -> EmptyResponse:
            handler_entered.set()
            blocker.wait(timeout=10)
            self.handled.append(request_data)
            return EmptyResponse()

        self.dispatcher.register_post_handler(DispatchKey(ACTION, None), slow_handler)
        self.dispatcher.start()
        worker = self.dispatcher._worker
        request_queue = self.dispatcher._queue
        self.dispatcher.on_post(_mk_request())
        self.assertTrue(handler_entered.wait(timeout=10))
        for _ in range(20):
            self.dispatcher.on_post(_mk_request())
        stopper = threading.Thread(target=self.dispatcher.stop)
        stopper.start()
        try:
            # release the handler only after stop() discarded the queue, otherwise the worker takes the next request
            self.assertTrue(utils.wait_for(lambda: request_queue._closed, timeout=10))
            self.assertEqual(request_queue.qsize(), 0)
        finally:
            blocker.set()
        stopper.join(timeout=10)
        self.assertFalse(stopper.is_alive())
        self.assertFalse(worker.is_alive())
        self.assertEqual(len(self.handled), 1)

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

    def test_request_racing_with_stop_is_rejected(self):
        """Verify that a request is rejected if stop is executed after on_post read the queue, but before it put.

        Such a request must not be answered as accepted, because no worker thread handles it any more.
        """
        self.dispatcher.start()
        request_queue = self.dispatcher._queue
        original_put = request_queue.put

        def put_after_stop(*args, **kwargs):  # noqa: ANN002, ANN003
            self.dispatcher.stop()
            original_put(*args, **kwargs)

        with (
            mock.patch.object(request_queue, 'put', put_after_stop),
            self.assertRaises(RuntimeError) as context,
        ):
            self.dispatcher.on_post(_mk_request())
        self.assertIsInstance(context.exception.__cause__, _ClosedError)
        self.assertIsNone(self.dispatcher._worker)
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

    def test_stop_from_handler_discards_queued_requests(self):
        """Verify that requests queued behind a handler that calls stop are not handled."""
        queued = threading.Event()

        def stopping_handler(request_data: RequestData) -> EmptyResponse:
            queued.wait(timeout=10)
            self.handled.append(request_data)
            self.dispatcher.stop()
            return EmptyResponse()

        self.dispatcher.register_post_handler(DispatchKey(ACTION, None), stopping_handler)
        self.dispatcher.start()
        worker = self.dispatcher._worker
        self.dispatcher.on_post(_mk_request())
        self.dispatcher.on_post(_mk_request())
        queued.set()
        worker.join(timeout=10)
        self.assertFalse(worker.is_alive())
        self.assertEqual(len(self.handled), 1)

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
        """Verify that stop returns within STOP_TIMEOUT instead of waiting for a blocked handler."""
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

    def _start_blocked_worker(self, blocker: threading.Event) -> threading.Thread:
        """Start the dispatcher, let its worker block in a handler and stop it with a timeout.

        Return the worker thread, which is still alive until blocker is set.
        """
        handler_entered = threading.Event()

        def blocking_handler(request_data: RequestData) -> EmptyResponse:
            handler_entered.set()
            blocker.wait(timeout=10)
            self.handled.append(request_data)
            return EmptyResponse()

        self.dispatcher.register_post_handler(DispatchKey(ACTION, None), blocking_handler)
        self.dispatcher.start()
        worker = self.dispatcher._worker
        self.dispatcher.on_post(_mk_request())
        self.assertTrue(handler_entered.wait(timeout=10))
        with mock.patch.object(DispatchKeyRegistryDeferred, 'STOP_TIMEOUT', 0.01):
            self.dispatcher.stop()
        self.assertTrue(worker.is_alive())
        return worker

    def test_start_waits_for_a_worker_that_outlived_stop(self):
        """Verify that start waits for the worker of a timed-out stop before it starts a new one.

        Otherwise both workers would handle requests at the same time, and an old report could be handled after a
        newer one.
        """
        blocker = threading.Event()
        try:
            old_worker = self._start_blocked_worker(blocker)
            releaser = threading.Timer(0.2, blocker.set)
            releaser.start()
            self.dispatcher.start()
            self.assertFalse(old_worker.is_alive())
            new_worker = self.dispatcher._worker
            self.assertIsNot(new_worker, old_worker)
            self.assertTrue(new_worker.is_alive())
        finally:
            blocker.set()
        self.dispatcher.on_post(_mk_request())
        self.assertTrue(utils.wait_for(lambda: len(self.handled) == 2, timeout=10))
        self.dispatcher.stop()
        self.assertEqual(_own_worker_threads(), [])

    def test_start_raises_if_a_worker_that_outlived_stop_still_runs(self):
        """Verify that start raises instead of starting a second worker if the previous one does not end in time."""
        blocker = threading.Event()
        try:
            old_worker = self._start_blocked_worker(blocker)
            with (
                mock.patch.object(DispatchKeyRegistryDeferred, 'STOP_TIMEOUT', 0.01),
                self.assertRaises(RuntimeError),
            ):
                self.dispatcher.start()
            self.assertIsNone(self.dispatcher._worker)
            self.assertEqual(_own_worker_threads(), [old_worker])
            with self.assertRaises(RuntimeError):
                self.dispatcher.on_post(_mk_request())  # still stopped
        finally:
            blocker.set()
        old_worker.join(timeout=10)
        self.assertFalse(old_worker.is_alive())
        self.dispatcher.start()  # works again as soon as the previous worker ended
        self.dispatcher.on_post(_mk_request())
        self.assertTrue(utils.wait_for(lambda: len(self.handled) == 2, timeout=10))

    def test_restart_from_handler(self):
        """Verify that a handler can stop and start the dispatcher, e.g. in an observer that restarts the consumer.

        The worker thread cannot wait for itself, and it handles no further request after the handler returned.
        """
        errors = []
        restarted = threading.Event()

        def restarting_handler(request_data: RequestData) -> EmptyResponse:  # noqa: ARG001
            try:
                self.dispatcher.stop()
                self.dispatcher.start()
            except BaseException as ex:  # noqa: BLE001
                errors.append(ex)
            restarted.set()
            return EmptyResponse()

        self.dispatcher.register_post_handler(DispatchKey(ACTION, None), restarting_handler)
        self.dispatcher.start()
        old_worker = self.dispatcher._worker
        self.dispatcher.on_post(_mk_request())
        self.assertTrue(restarted.wait(timeout=10))
        old_worker.join(timeout=10)
        self.assertEqual(errors, [])
        self.assertFalse(old_worker.is_alive())
        new_worker = self.dispatcher._worker
        self.assertIsNot(new_worker, old_worker)
        self.assertTrue(new_worker.is_alive())
        self.dispatcher.register_post_handler(DispatchKey(ACTION, None), self._handler)
        self.dispatcher.on_post(_mk_request())
        self.assertTrue(utils.wait_for(lambda: len(self.handled) == 1, timeout=10))


class TestClosableQueue(unittest.TestCase):
    def test_put_and_get_while_open(self):
        """Verify that an open queue behaves like a normal queue."""
        request_queue = _ClosableQueue(10)
        request_queue.put(1)
        self.assertEqual(request_queue.get_nowait(), 1)

    def test_put_after_close_raises(self):
        """Verify that a closed queue rejects new items."""
        request_queue = _ClosableQueue(10)
        request_queue.close_and_discard()
        with self.assertRaises(_ClosedError):
            request_queue.put(1)
        with self.assertRaises(_ClosedError):
            request_queue.put_nowait(1)
        self.assertEqual(request_queue.qsize(), 0)

    def test_close_and_discard_returns_number_of_discarded_items(self):
        """Verify that closing removes all items and reports how many there were."""
        request_queue = _ClosableQueue(10)
        for i in range(3):
            request_queue.put(i)
        self.assertEqual(request_queue.close_and_discard(), 3)
        self.assertEqual(request_queue.qsize(), 0)
        self.assertEqual(request_queue.close_and_discard(), 0)  # a second close finds nothing

    def test_close_rejects_a_producer_that_waits_for_a_free_slot(self):
        """Verify that a producer blocked on a full queue is rejected by close instead of waiting for its timeout."""
        request_queue = _ClosableQueue(1)
        request_queue.put(0)
        errors = []

        def producer():
            try:
                request_queue.put(1, timeout=10)
            except BaseException as ex:  # noqa: BLE001
                errors.append(ex)

        thread = threading.Thread(target=producer)
        thread.start()
        # not_full._waiters is a CPython detail, it only makes sure that the producer really waits
        self.assertTrue(utils.wait_for(lambda: len(request_queue.not_full._waiters) > 0, timeout=10))
        begin = time.monotonic()
        self.assertEqual(request_queue.close_and_discard(), 1)
        thread.join(timeout=10)
        self.assertFalse(thread.is_alive())
        self.assertLess(time.monotonic() - begin, 5)
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], _ClosedError)
        self.assertEqual(request_queue.qsize(), 0)
