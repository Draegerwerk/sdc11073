import queue
import threading
import traceback

from sdc11073.dispatch import RequestData
from sdc11073.dispatch import DispatchKeyRegistry
from ..exceptions import InvalidActionError
from ..pysoap.msgfactory import CreatedMessage
from ..pysoap.soapenvelope import Fault, faultcodeEnum


class _DispatchError(Exception):
    def __init__(self, http_error_code, error_text):
        super().__init__()
        self.http_error_code = http_error_code
        self.error_text = error_text


class EmptyResponse(CreatedMessage):
    def __init__(self):
        super().__init__(None, None)

    def serialize(self, pretty=False, request_manipulator=None, validate=True) -> bytes:
        return b''


class DispatchKeyRegistryDeferred(DispatchKeyRegistry):
    """This middleware splits request processing into two parts. It writes
    the request to a queue and returns immediately. A worker thread is responsible for the further handling.
    This allows a faster response."""
    def __init__(self, log_prefix):
        super().__init__(log_prefix)
        self._queue = queue.Queue(1000)
        self._worker = threading.Thread(target=self._read_queue)
        self._worker.daemon = True
        self._worker.start()

    def on_post(self, request_data: RequestData) -> CreatedMessage:
        action = request_data.message_data.action
        func = self.get_post_handler(request_data)
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
            except Exception:
                self._logger.error(
                    f'method {func.__name__} for action "{action}" failed:{traceback.format_exc()}')
