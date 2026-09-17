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
    The worker thread is created by start() and ended by stop(). As long as no worker thread is running, incoming
    requests are discarded.
    """

    QUEUE_SIZE = 1000  # maximum number of requests waiting for the worker thread
    PUT_TIMEOUT = 1.0  # max. seconds that an incoming request waits for a free slot in the queue
    STOP_TIMEOUT = 5.0  # max. seconds that stop() waits for the queue and for the worker thread
    GET_TIMEOUT = 2.0  # has to be short than STOP_TIMEOUT

    def __init__(self, log_prefix: str):
        super().__init__(log_prefix)
        self._log_prefix = log_prefix or ''
        self._queue: queue.Queue[tuple[OnPostHandler, RequestData, str | None]] | None = None
        self._worker: threading.Thread | None = None
        self._stop_requested = threading.Event()
        self._queue_lock = threading.RLock()

    def start(self):
        """See documentation in RequestDispatcherProtocol."""
        self._stop_requested.clear()
        if self._worker is not None:
            return  # the worker of a previous start did not end yet, it keeps handling the queue
        with self._queue_lock:
            self._queue = queue.Queue(self.QUEUE_SIZE)
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
        if self._worker is None:
            return
        self._stop_requested.set()
        self._worker.join(timeout=self.STOP_TIMEOUT)
        if self._worker.is_alive():
            self._logger.warning(  # noqa: PLE1205
                'worker thread "{}" did not end within {} seconds, it keeps handling the queued requests',
                self._worker.name,
                self.STOP_TIMEOUT,
            )
        self._worker = None
        with self._queue_lock:
            self._queue = None

    def on_post(self, request_data: RequestData) -> CreatedMessage:
        """See documentation in RequestHandlerProtocol."""
        if self._stop_requested.is_set():
            raise RuntimeError('Consumer already stopped, cannot handle request')
        action = request_data.message_data.action
        func = self._get_post_handler(request_data)
        if func is None:
            fault = Fault()
            fault.Code.Value = faultcodeEnum.SENDER
            fault.add_reason_text(f'invalid action {action}')

            raise InvalidActionError(fault)
        try:
            with self._queue_lock:
                if self._queue is not None:
                    self._queue.put((func, request_data, action), timeout=self.PUT_TIMEOUT)
        except queue.Full as e:
            msg = f'Consumer unable to handle request {action}, queue is full'
            raise RuntimeError(msg) from e
        return EmptyResponse()

    def _read_queue(self):
        while not self._stop_requested.is_set():
            try:
                with self._queue_lock:
                    if self._queue is None:
                        break
                    func, request, action = self._queue.get(timeout=self.GET_TIMEOUT)
            except queue.Empty:
                continue
            try:
                func(request)
            except Exception:
                # catch all to keep thread alive
                self._logger.exception('method {} for action "{}" failed', func.__name__, action)  # noqa: PLE1205
