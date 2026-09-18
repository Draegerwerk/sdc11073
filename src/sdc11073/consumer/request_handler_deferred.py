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
    STOP_TIMEOUT = 5.0  # max. seconds that stop() waits for the worker thread to handle the queued requests
    GET_TIMEOUT = 0.1  # granularity with which an idle worker thread looks for a stop request, must be << STOP_TIMEOUT

    def __init__(self, log_prefix: str):
        super().__init__(log_prefix)
        self._log_prefix = log_prefix or ''
        # _worker, _queue and _stop_requested belong together and are only ever replaced as a triple, while
        # _lifecycle_lock is held. Two rules keep this deadlock free and starvation free: _lifecycle_lock is never
        # held while blocking (join, get, put), and it is a plain Lock, so that a reentrant use cannot creep in.
        self._lifecycle_lock = threading.Lock()
        self._worker: threading.Thread | None = None
        self._queue: queue.Queue[_QueueItem] | None = None
        self._stop_requested: threading.Event | None = None

    def start(self):
        """See documentation in RequestDispatcherProtocol."""
        with self._lifecycle_lock:
            if self._worker is not None:
                return  # already started
            request_queue: queue.Queue[_QueueItem] = queue.Queue(self.QUEUE_SIZE)
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

        Requests that are already queued are handled before the worker thread ends.
        If the worker thread cannot be joined, the queued requests are discarded instead. This happens if it did not
        end within STOP_TIMEOUT, or if stop() was called from a handler, because a thread cannot join itself. Then
        at most the request that is currently being handled is still handled, so that stop() stays bounded and a
        worker of a stopped dispatcher cannot handle requests while a restarted dispatcher is already running.
        """
        with self._lifecycle_lock:
            worker, request_queue, stop_requested = self._worker, self._queue, self._stop_requested
            self._worker = self._queue = self._stop_requested = None  # on_post rejects requests from now on
        if worker is None or request_queue is None or stop_requested is None:
            return  # never started, or already stopped
        stop_requested.set()  # the worker thread ends as soon as its queue is empty
        if worker is threading.current_thread():
            # stop() was called from a handler, which is executed in the worker thread; it cannot join itself.
            discarded = self._discard(request_queue)
            self._logger.info(  # noqa: PLE1205
                'stop() was called from worker thread "{}", it ends after the current request, '
                '{} queued request(s) discarded',
                worker.name,
                discarded,
            )
            return
        worker.join(timeout=self.STOP_TIMEOUT)
        if worker.is_alive():
            discarded = self._discard(request_queue)
            self._logger.warning(  # noqa: PLE1205
                'worker thread "{}" did not end within {} seconds, {} queued request(s) discarded',
                worker.name,
                self.STOP_TIMEOUT,
                discarded,
            )
        elif (left_over := request_queue.qsize()) > 0:
            # a request that was accepted while the worker was ending; it was answered, but it is not handled
            self._logger.warning(  # noqa: PLE1205
                'worker thread "{}" ended, {} request(s) that arrived during the stop are discarded',
                worker.name,
                left_over,
            )

    @staticmethod
    def _discard(request_queue: queue.Queue[_QueueItem]) -> int:
        """Remove all queued requests and return how many were removed."""
        discarded = 0
        while True:
            try:
                request_queue.get_nowait()
            except queue.Empty:
                return discarded
            discarded += 1

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
        return EmptyResponse()

    def _read_queue(self, request_queue: queue.Queue[_QueueItem], stop_requested: threading.Event):
        """Handle queued requests until a stop is requested and the queue is empty.

        The queue and the stop flag are arguments instead of attributes because they belong to this thread alone.
        No lock is needed to read them, and a worker of a previous start cannot interfere with the current one.
        """
        while True:
            try:
                func, request, action = request_queue.get(timeout=self.GET_TIMEOUT)
            except queue.Empty:
                if stop_requested.is_set():
                    return  # the queue is drained and a stop was requested
                continue
            try:
                func(request)
            except Exception:
                # catch all to keep thread alive
                self._logger.exception(  # noqa: PLE1205
                    'method {} for action "{}" failed',
                    getattr(func, '__name__', func),
                    action,
                )
