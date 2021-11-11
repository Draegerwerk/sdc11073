import queue
import threading
import time
import traceback

from .. import commlog
from .. import loghelper
from ..httprequesthandler import HTTPRequestHandler, HttpServerThreadBase, RequestData


class _DispatchError(Exception):
    def __init__(self, http_error_code, error_text):
        super().__init__()
        self.http_error_code = http_error_code
        self.error_text = error_text


class ReceivedNotification(RequestData):
    def __init__(self, http_header, path, request=None):
        super().__init__(http_header, path, None, request)
        self.message_data = None


class SOAPNotificationsDispatcher:
    """ receiver of all notifications"""

    def __init__(self, log_prefix):
        self._logger = loghelper.get_logger_adapter('sdc.client.notif_dispatch', log_prefix)
        self.log_prefix = log_prefix
        self.methods = {}

    def register_function(self, action, func):
        self.methods[action] = func

    def on_post(self, request_data: ReceivedNotification) -> [str, None]:
        return self._dispatch(request_data)

    def on_get(self, path: str, headers) -> str:  # pylint: disable=unused-argument, no-self-use
        return ''

    def _dispatch(self, request_data):
        start = time.time()
        action = request_data.message_data.action
        self._logger.debug('received notification path={}, action = {}', request_data.path_elements, action)

        try:
            func = self.methods[action]
        except KeyError as ex:
            self._logger.error(f'action "{action}" not registered. Known:{self.methods.keys()}')
            raise _DispatchError(404, 'action not registered') from ex

        func(request_data)
        duration = time.time() - start
        if duration > 0.005:
            self._logger.debug('action {}: duration = {:.4f}sec', action, duration)
        return ''


class SOAPNotificationsDispatcherThreaded(SOAPNotificationsDispatcher):

    def __init__(self, ident):
        super().__init__(ident)
        self._queue = queue.Queue(1000)
        self._worker = threading.Thread(target=self._read_queue)
        self._worker.daemon = True
        self._worker.start()

    def _dispatch(self, request_data):
        action = request_data.message_data.action
        try:
            func = self.methods[action]
        except KeyError as ex:
            self._logger.error(
                f'action "{action}" not registered. Known:{self.methods.keys()}')
            raise _DispatchError(404, 'action not registered') from ex
        self._queue.put((func, request_data, action))
        return ''

    def _read_queue(self):
        while True:
            func, request, action = self._queue.get()
            try:
                func(request)
            except Exception:
                self._logger.error(
                    f'method {func.__name__} for action "{action}" failed:{traceback.format_exc()}')


class SOAPNotificationsHandler(HTTPRequestHandler):
    disable_nagle_algorithm = True
    wbufsize = 0xffff  # 64k buffer to prevent tiny packages
    RESPONSE_COMPRESS_MINSIZE = 256  # bytes, compress response it it is larger than this value (and other side supports compression)

    def do_POST(self):  # pylint: disable=invalid-name
        """SOAP POST gateway"""
        self.server.logger.debug('notification do_POST incoming')  # pylint: disable=protected-access
        dispatcher = self.server.dispatcher
        response_string = ''
        if dispatcher is None:
            # close this connection
            self.close_connection = True  # pylint: disable=attribute-defined-outside-init
            self.server.logger.warn(
                'received a POST request, but no dispatcher => returning 404 ')  # pylint:disable=protected-access
            self.send_response(404)  # not found
        else:
            request_bytes = self._read_request()

            self.server.logger.debug('notification {} bytes', request_bytes)  # pylint: disable=protected-access
            # execute the method
            commlog.get_communication_logger().log_soap_subscription_msg_in(request_bytes)
            try:
                request_data = ReceivedNotification(self.headers, self.path, request_bytes)
                request_data.message_data = self.server.msg_reader.read_received_message(request_bytes)
                response_string = self.server.dispatcher.on_post(request_data)
                if response_string is None:
                    response_string = ''
                self.send_response(202, b'Accepted')
            except _DispatchError as ex:
                self.server.logger.error('received a POST request, but got _DispatchError => returning {}',
                                         ex.http_error_code)  # pylint:disable=protected-access
                self.send_response(ex.http_error_code, ex.error_text)
            except Exception as ex:
                self.server.logger.error(
                    'received a POST request, but got Exception "{}"=> returning {}\n{}', ex, 500,
                    traceback.format_exc())  # pylint:disable=protected-access
                self.send_response(500, b'server error in dispatch')
        response_bytes = response_string.encode('utf-8')
        if len(response_bytes) > self.RESPONSE_COMPRESS_MINSIZE:
            response_bytes = self._compress_if_required(response_bytes)

        self.send_header("Content-Type", "application/soap+xml; charset=utf-8")
        self.send_header("Content-Length", len(response_bytes))  # this is necessary for correct keep-alive handling!
        self.end_headers()
        self.wfile.write(response_bytes)


class NotificationsReceiver(HttpServerThreadBase):
    def __init__(self, my_ipaddress, ssl_context, log_prefix, msg_reader, msg_factory,
                 supported_encodings,
                 notifications_handler_class, async_dispatch=True):
        """
        This thread receives all notifications from the connected device.
        :param my_ipaddress:
        :param ssl_context:
        :param log_prefix:
        :param sdc_definitions:
        :param supported_encodings:
        :param soap_notifications_handler_class:
        :param async_dispatch:
        """
        logger = loghelper.get_logger_adapter('sdc.client.notif_dispatch', log_prefix)
        request_handler = notifications_handler_class
        if async_dispatch:
            dispatcher = SOAPNotificationsDispatcherThreaded(log_prefix)
        else:
            dispatcher = SOAPNotificationsDispatcher(log_prefix)
        super().__init__(my_ipaddress, ssl_context, supported_encodings,
                         request_handler, dispatcher, msg_reader, msg_factory,
                         logger, chunked_responses=False)
