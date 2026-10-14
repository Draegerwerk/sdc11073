"""Implementation of a request dispatcher that handles incoming notifications in a separate thread."""

from __future__ import annotations

import queue
import threading
from typing import TYPE_CHECKING

from sdc11073.dispatch import RequestData, RequestDispatcher
from sdc11073.exceptions import InvalidActionError
from sdc11073.pysoap.msgfactory import CreatedMessage
from sdc11073.pysoap.soapenvelope import Fault, faultcodeEnum

if TYPE_CHECKING:
    from sdc11073.consumer.manipulator import RequestManipulatorProtocol
    from sdc11073.dispatch.dispatchkey import OnPostHandler

    _QueueItem = tuple[OnPostHandler, RequestData, str | None]


class EmptyResponse(CreatedMessage):
    """EmptyResponse is a response with no content."""

    def __init__(self):
        super().__init__(None, None)

    def serialize(
        self,
        pretty: bool = False,  # noqa: ARG002
        request_manipulator: RequestManipulatorProtocol | None = None,  # noqa: ARG002
        validate: bool = True,  # noqa: ARG002
    ) -> bytes:
        """Return bytes of len 0."""
        return b''


class _ClosedError(Exception):
    """Raised by _ClosableQueue.put after the queue was closed."""


class _ClosableQueue(queue.Queue):
    """A queue that rejects new items once it is closed.

    Closing and putting are atomic, because both happen while self.mutex is held: queue.Queue.put calls _put with
    the mutex held. So an item is either in the queue when it is closed, and is discarded, or it is rejected.
    """

    def __init__(self, maxsize: int):
        super().__init__(maxsize)
        self._closed = False

    def _put(self, item: _QueueItem):
        if self._closed:
            raise _ClosedError
        super()._put(item)

    def close_and_discard(self) -> int:
        """Close the queue, remove all items and return how many were removed.

        Producers that wait for a free slot are woken up, so that they are rejected without waiting for the timeout.
        """
        with self.mutex:
            self._closed = True
            discarded = self._qsize()
            self.queue.clear()
            self.not_full.notify_all()
            return discarded


class DispatchKeyRegistryDeferred(RequestDispatcher):
    """A middleware that splits request processing into two parts.

    It writes the request to a queue and returns immediately. A worker thread is responsible for the further handling.
    This allows a faster response.
    The worker thread is created by start() and ended by stop(). As long as no worker thread is running, on_post
    rejects incoming requests with a RuntimeError.

    Every worker thread owns the queue and the stop flag that it works with; both are passed to it as arguments.
    Nothing is shared with the thread that writes to the queue, so no lock is needed to read them. That is essential:
    a lock that is held while the worker waits for the next request would starve the http thread that delivers it.
    """

    QUEUE_SIZE = 1000  # maximum number of requests waiting for the worker thread
    PUT_TIMEOUT = 1.0  # max. seconds that an incoming request waits for a free slot in the queue
    STOP_TIMEOUT = 5.0  # max. seconds that stop() waits for the worker thread to finish the current request
    GET_TIMEOUT = 0.1  # granularity with which an idle worker thread looks for a stop request, must be << STOP_TIMEOUT

    def __init__(self, log_prefix: str):
        super().__init__(log_prefix)
        self._log_prefix = log_prefix or ''
        # _worker, _queue and _stop_requested belong together and are only ever replaced as a triple, while
        # _lifecycle_lock is held. Two rules keep this deadlock free and starvation free: _lifecycle_lock is never
        # held while blocking (join, get, put), and it is a plain Lock, so that a reentrant use cannot creep in.
        self._lifecycle_lock = threading.Lock()
        self._worker: threading.Thread | None = None
        self._queue: _ClosableQueue | None = None
        self._stop_requested: threading.Event | None = None

    def start(self):
        """See documentation in RequestDispatcherProtocol."""
        with self._lifecycle_lock:
            if self._worker is not None:
                return  # already started
            request_queue = _ClosableQueue(self.QUEUE_SIZE)
            stop_requested = threading.Event()
            worker = threading.Thread(
                target=self._read_queue,
                args=(request_queue, stop_requested),
                name=f'ConsumerNotificationsWorker{self._log_prefix}',
                daemon=True,
            )
            # The thread is started before it is published, so that a failing start cannot leave a queue behind that
            # no thread reads, and so that stop() can never see a thread that was not started yet. Thread.start only
            # waits for the new thread to come up, and the new thread never needs _lifecycle_lock.
            worker.start()
            self._queue = request_queue  # on_post accepts requests from now on
            self._stop_requested = stop_requested
            self._worker = worker

    def stop(self):
        """See documentation in RequestDispatcherProtocol.

        Requests that are still queued are discarded, and their number is logged. A request whose handling has
        already begun is finished, because a thread cannot be interrupted. stop() waits at most STOP_TIMEOUT for that;
        it does not wait at all if it was called from a handler, because a thread cannot join itself.
        """
        with self._lifecycle_lock:
            worker, request_queue, stop_requested = self._worker, self._queue, self._stop_requested
            self._worker = self._queue = self._stop_requested = None  # on_post rejects requests from now on
        if worker is None or request_queue is None or stop_requested is None:
            return  # never started, or already stopped
        stop_requested.set()  # the worker thread handles no further request
        # a request that on_post puts after this is rejected instead of being lost
        discarded = request_queue.close_and_discard()
        if discarded > 0:
            # these requests were answered, but they are not handled
            self._logger.info(  # noqa: PLE1205
                'worker thread "{}" stopped, {} queued request(s) discarded',
                worker.name,
                discarded,
            )
        if worker is threading.current_thread():
            return  # stop() was called from a handler; the worker thread ends as soon as the handler returns
        worker.join(timeout=self.STOP_TIMEOUT)
        if worker.is_alive():
            self._logger.warning(  # noqa: PLE1205
                'worker thread "{}" is still handling a request after {} seconds, it ends when the request is done',
                worker.name,
                self.STOP_TIMEOUT,
            )

    def on_post(self, request_data: RequestData) -> CreatedMessage:
        """See documentation in RequestHandlerProtocol."""
        request_queue = self._queue  # read once, stop() may replace it with None at any time
        if request_queue is None:
            raise RuntimeError('Consumer already stopped, cannot handle request')
        action = request_data.message_data.action
        func = self._get_post_handler(request_data)
        if func is None:
            fault = Fault()
            fault.Code.Value = faultcodeEnum.SENDER
            fault.add_reason_text(f'invalid action {action}')

            raise InvalidActionError(fault)
        try:
            request_queue.put((func, request_data, action), timeout=self.PUT_TIMEOUT)
        except queue.Full as e:
            # deliberately raised and not only logged: a silently dropped notification is much harder to diagnose
            msg = f'Consumer unable to handle request {action}, queue is full'
            raise RuntimeError(msg) from e
        except _ClosedError as e:
            # stop() was called after the queue was read above
            msg = f'Consumer already stopped, cannot handle request {action}'
            raise RuntimeError(msg) from e
        return EmptyResponse()

    def _read_queue(self, request_queue: _ClosableQueue, stop_requested: threading.Event):
        """Handle queued requests until a stop is requested.

        The stop flag is checked again after every get, so a request that is taken from the queue after the stop was
        requested is not handled. The other queued requests are discarded by stop().
        The queue and the stop flag are arguments instead of attributes because they belong to this thread alone.
        No lock is needed to read them, and a worker of a previous start cannot interfere with the current one.
        """
        while not stop_requested.is_set():
            try:
                func, request, action = request_queue.get(timeout=self.GET_TIMEOUT)
            except queue.Empty:
                continue
            if stop_requested.is_set():
                # this request was answered, but it is not handled
                self._logger.info(  # noqa: PLE1205
                    'worker thread "{}" stopped, request with action "{}" discarded',
                    threading.current_thread().name,
                    action,
                )
                return
            try:
                func(request)
            except Exception:
                # catch all to keep thread alive
                self._logger.exception(  # noqa: PLE1205
                    'method {} for action "{}" failed',
                    getattr(func, '__name__', func),
                    action,
                )
