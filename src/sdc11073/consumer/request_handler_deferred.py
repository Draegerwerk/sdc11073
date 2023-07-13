from __future__ import annotations

import queue
import threading
import traceback
from typing import TYPE_CHECKING

from sdc11073.dispatch import RequestData, RequestDispatcher
from sdc11073.exceptions import InvalidActionError
from sdc11073.pysoap.msgfactory import CreatedMessage
from sdc11073.pysoap.soapenvelope import Fault, faultcodeEnum

if TYPE_CHECKING:
    from .manipulator import RequestManipulatorProtocol


class EmptyResponse(CreatedMessage):
    """EmptyResponse is a response with no content."""

    def __init__(self):
        super().__init__(None, None)

    def serialize(self, pretty: bool = False,  # noqa: ARG002
                  request_manipulator: RequestManipulatorProtocol | None = None,  # noqa: ARG002
                  validate: bool = True) -> bytes:  # noqa: ARG002
        """Return bytes of len 0."""
        return b''


class DispatchKeyRegistryDeferred(RequestDispatcher):
    """A middleware that splits request processing into two parts.

    It writes the request to a queue and returns immediately. A worker thread is responsible for the further handling.
    This allows a faster response.
    """

    def __init__(self, log_prefix: str):
        super().__init__(log_prefix)
        self._queue = queue.Queue(1000)
        self._worker = threading.Thread(target=self._read_queue)
        self._worker.daemon = True
        self._worker.start()

    def on_post(self, request_data: RequestData) -> CreatedMessage:
        """See documentation in RequestHandlerProtocol."""
        action = request_data.message_data.action
        func = self._get_post_handler(request_data)
        if func is None:
            fault = Fault()
            fault.Code.Value = faultcodeEnum.SENDER
            fault.add_reason_text(f'invalid action {action}')

            raise InvalidActionError(fault)
        self._queue.put((func, request_data, action))
        return EmptyResponse()

    def _read_queue(self):
        while True:
            func, request, action = self._queue.get()
            try:
                func(request)
            except Exception:  # noqa: BLE001
                # catch all to keep thread alive
                self._logger.error('method {} for action "{}" failed:{}',  # noqa: PLE1205
                                   func.__name__, action, traceback.format_exc())
