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
    from sdc11073.dispatch.dispatchkey import OnPostHandler

    from .manipulator import RequestManipulatorProtocol


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


class _StopWorker:
    """Sentinel that is put into the queue in order to end the worker thread.

    The worker thread only ends if a stop is still requested when it reads the sentinel. A sentinel of a stop
    request that has been superseded by a later start is ignored.
    """


_STOP_WORKER = _StopWorker()

if TYPE_CHECKING:
    _QueueItem = tuple[OnPostHandler, RequestData, str | None] | _StopWorker


class DispatchKeyRegistryDeferred(RequestDispatcher):
    """A middleware that splits request processing into two parts.

    It writes the request to a queue and returns immediately. A worker thread is responsible for the further handling.
    This allows a faster response.
    The worker thread is created by start() and ended by stop(). As long as no worker thread is running, incoming
    requests are discarded.
    """

    QUEUE_SIZE = 1000  # maximum number of requests waiting for the worker thread
    PUT_TIMEOUT = 1.0  # max. seconds that an incoming request waits for a free slot in the queue
    STOP_TIMEOUT = 1.0  # max. seconds that stop() waits for the queue and for the worker thread

    def __init__(self, log_prefix: str):
        super().__init__(log_prefix)
        self._log_prefix = log_prefix or ''
        self._queue: queue.Queue[_QueueItem] = queue.Queue(self.QUEUE_SIZE)
        self._worker_lock = threading.Lock()  # protects _worker and _stop_requested
        self._worker: threading.Thread | None = None
        self._stop_requested = True  # there is no worker thread before start() was called

    def start(self):
        """See documentation in RequestDispatcherProtocol."""
        with self._worker_lock:
            self._stop_requested = False
            if self._worker is not None and self._worker.is_alive():
                return  # the worker of a previous start did not end yet, it keeps handling the queue
            left_over = self._queue.qsize()
            if left_over:
                self._logger.info('{} message(s) left over from a previous run', left_over)  # noqa: PLE1205
            self._worker = threading.Thread(
                target=self._read_queue,
                name=f'ConsumerNotificationsWorker{self._log_prefix}',
                daemon=True,
            )
            self._worker.start()

    def stop(self):
        """See documentation in RequestDispatcherProtocol.

        Requests that are already queued are handled before the worker thread ends.
        """
        with self._worker_lock:
            self._stop_requested = True  # on_post discards incoming requests from now on
            worker = self._worker
        if worker is None:
            return  # never started or already stopped
        try:
            self._queue.put(_STOP_WORKER, timeout=self.STOP_TIMEOUT)
        except queue.Full:
            self._logger.warning(  # noqa: PLE1205
                'queue is full, could not stop worker thread "{}"; it ends with the process',
                worker.name,
            )
            return
        if worker is threading.current_thread():
            # stop() was called from a handler that is executed in the worker thread, it cannot join itself.
            # The worker ends as soon as it reads the stop request from the queue.
            self._logger.info(  # noqa: PLE1205
                'stop() was called from worker thread "{}", it ends after the queued requests',
                worker.name,
            )
            return
        worker.join(timeout=self.STOP_TIMEOUT)
        if worker.is_alive():
            self._logger.warning(  # noqa: PLE1205
                'worker thread "{}" did not end within {} seconds, it keeps handling the queued requests',
                worker.name,
                self.STOP_TIMEOUT,
            )

    def on_post(self, request_data: RequestData) -> CreatedMessage:
        """See documentation in RequestHandlerProtocol."""
        action = request_data.message_data.action
        if self._stop_requested:
            self._logger.warning('no worker thread, ignoring request with action "{}"', action)  # noqa: PLE1205
            return EmptyResponse()
        func = self._get_post_handler(request_data)
        if func is None:
            fault = Fault()
            fault.Code.Value = faultcodeEnum.SENDER
            fault.add_reason_text(f'invalid action {action}')

            raise InvalidActionError(fault)
        try:
            self._queue.put((func, request_data, action), timeout=self.PUT_TIMEOUT)
        except queue.Full:
            self._logger.warning(  # noqa: PLE1205
                'queue is full ({} requests), ignoring request with action "{}"',
                self._queue.qsize(),
                action,
            )
        return EmptyResponse()

    def _read_queue(self):
        while True:
            item = self._queue.get()
            if isinstance(item, _StopWorker):
                with self._worker_lock:
                    if self._stop_requested:
                        self._worker = None
                        return
                continue  # start() was called after this stop request was queued, keep on working
            func, request, action = item
            try:
                func(request)
            except Exception:
                # catch all to keep thread alive
                self._logger.exception('method {} for action "{}" failed', func.__name__, action)  # noqa: PLE1205
