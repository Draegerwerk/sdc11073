import socket
import threading
import traceback
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler
from http.server import HTTPServer
from io import BytesIO

from .compression import CompressionHandler


class HTTPRequestHandlingError(Exception):
    """ This class is used to communicate errors from http request handlers back to http server."""

    def __init__(self, status, reason, soap_fault):
        """
        :param status: integer, e.g. 404
        :param reason: the provided human readable text
        :param soap_fault: soapenvelope.SoapFault instance
        """
        super().__init__()
        self.status = status
        self.reason = reason
        self.soap_fault = soap_fault

    def __repr__(self):
        if self.soap_fault:
            return f'{self.__class__.__name__}(status={self.status}, reason={self.soap_fault})'
        return f'{self.__class__.__name__}(status={self.status}, reason={self.reason})'


class FunctionNotImplementedError(HTTPRequestHandlingError):
    def __init__(self, soap_fault):
        """
        :param soap_fault: soapenvelope.SoapFault instance
        """
        super().__init__(500, 'not implemented', soap_fault)


class InvalidActionError(HTTPRequestHandlingError):
    def __init__(self, soap_fault):
        """
        :param soap_fault: soapenvelope.SoapFault instance
        """
        super().__init__(400, 'Bad Request', soap_fault)


class InvalidPathError(HTTPRequestHandlingError):
    def __init__(self, soap_fault):
        """
        :param soap_fault: soapenvelope.SoapFault instance
        """
        super().__init__(400, 'Bad Request', soap_fault)


class DechunkError(Exception):
    """Raised when could not de-chunk stream.
    """


class DecompressError(Exception):
    """Raised when could not de-compress stream.
    """


def mk_chunks(body, chunk_size=512):
    """
    convert plain body bytes to chunked bytes
    :param body: bytes
    :param chunk_size: size of chunks
    :return: body converted to chunks (but still as single bytes array)
    """
    data = BytesIO()
    tail = body
    while True:
        head, tail = tail[:chunk_size], tail[chunk_size:]
        data.write(f'{len(head):x}\r\n'.encode('utf-8'))
        data.write(head)
        data.write(b'\r\n')
        if not head:
            return data.getvalue()


CR_LF = b'\r\n'


class HTTPReader:
    """ Base class that implements decoding of incoming http requests.
    Supported features:
    - read data by content-length
    - handle chunk-encoding
    - handle compression
    """

    @classmethod
    def _read_dechunk(cls, stream):
        """De-chunk HTTP body stream.
        :param file stream: readable file-like object.
        :rtype: bytes
        :raise: DechunkError
        """
        body = []
        while True:
            chunk_header = cls._read_until(stream, CR_LF)
            chunk_headers = chunk_header.split(b';')  # length + optional chunk-extensions (name=value pairs)
            chunk_len, _ = chunk_headers[0], chunk_headers[1:]  # we do nothing with chunk-extensions...
            if chunk_len is None:
                raise DechunkError(
                    'Could not extract chunk size: unexpected end of data.')

            try:
                chunk_len = int(chunk_len.strip(), 16)
            except (ValueError, TypeError) as err:
                raise DechunkError('Could not parse chunk size:') from err

            bytes_to_read = chunk_len
            while bytes_to_read:
                chunk = stream.read(bytes_to_read)
                bytes_to_read -= len(chunk)
                body.append(chunk)

            # chunk ends with \r\n
            cr_lf = stream.read(2)
            if cr_lf != CR_LF:
                raise DechunkError('No CR+LF at the end of chunk!')
            if chunk_len == 0:  # len == 0 indicates end of data
                break
        return b''.join(body)

    @staticmethod
    def _read_until(stream, delimiter, max_bytes=16):
        """Read until we have found the given delimiter.
        :param file stream: readable file-like object.
        :param bytes delimiter: delimiter string.
        :param int max_bytes: maximum bytes to read.
        :rtype: bytes|None
        """

        buf = bytearray()
        delim_len = len(delimiter)

        while len(buf) < max_bytes:
            char = stream.read(1)

            if not char:
                break

            buf += char
            if buf[-delim_len:] == delimiter:
                return bytes(buf[:-delim_len])

    @classmethod
    def read_request_body(cls, http_message, supported_encodings=None):
        """ checks header for content-length, chunk-encoding and compression entries.
        Handles incoming bytes correspondingly.
        @http_message: a http request or response read from network
        :return: bytes
        """
        http_body = None
        transfer_encoding = http_message.headers.get('transfer-encoding')
        if transfer_encoding is not None and transfer_encoding.lower() == 'chunked':
            http_body = cls._read_dechunk(http_message.rfile)
        else:
            cl_string = http_message.headers.get('content-length')
            if cl_string:
                try:
                    content_length = int(cl_string)
                    http_body = http_message.rfile.read(content_length)
                except TypeError:
                    http_body = http_message.rfile.read()

        # if we get compressed content then we check against server setting
        # if it matches continue and decompress
        # if current server setting is any, use whatever client has provided in content-encoding header
        actual_enc = http_message.headers.get('content-encoding')
        if actual_enc:
            supported_encs = supported_encodings or CompressionHandler.available_encodings
            if actual_enc in supported_encs:
                http_body = CompressionHandler.decompress_payload(actual_enc, http_body)
            else:
                raise DecompressError(f'content-encoding "{actual_enc}" is not supported', )
        return http_body

    @classmethod
    def read_response_body(cls, http_response, supported_encodings=None):
        """ checks header for content-length, chunk-encoding and compression entries.
        Handles incoming bytes correspondingly.
        :http_response: a http response read from network
        :supported_encodings: if given, only these encodings may be used.
        :return: bytes
        """
        http_body = None
        cl_string = http_response.getheader('content-length')
        if cl_string:
            try:
                content_length = int(cl_string)
                http_body = http_response.read(content_length)
            except TypeError:
                http_body = http_response.read()
        if http_body is None:
            transfer_encoding = http_response.getheader('transfer-encoding')
            if transfer_encoding is not None and transfer_encoding.lower() == 'chunked':
                # de-chunking is done by http client. we just need to read until no more data available
                http_body = []
                tmp = http_response.read()
                while tmp:
                    http_body.append(tmp)
                    tmp = http_response.read()
                http_body = b''.join(http_body)
        if http_body is None:
            http_body = http_response.read()

        # if we get compressed content then we check against server setting
        # if it matches continue and decompress
        # if current server setting is any, use whatever client has provided in content-encoding header
        actual_enc = http_response.getheader('content-encoding')
        if actual_enc:
            supported_encs = supported_encodings or CompressionHandler.available_encodings
            if actual_enc in supported_encs:
                http_body = CompressionHandler.decompress_payload(actual_enc, http_body)
            else:
                raise DecompressError(f'content-encoding "{actual_enc}" is not supported')
        return http_body


class HTTPRequestHandler(BaseHTTPRequestHandler):
    """ Base class that implements decoding of incoming http requests.
    Supported features:
    - read data by content-length
    - handle chunk-encoding
    - handle compression
    """
    protocol_version = "HTTP/1.1"  # this enables keep-alive

    def _read_request(self):
        """ checks header for content-length, chunk-encoding and compression entries.
        Handles incoming bytes correspondingly.
        :return: http body as bytes
        """
        return HTTPReader.read_request_body(self)

    def _compress_if_supported(self, response_bytes):
        """Compress response if header of request indicates that other side
        accepts one of our supported compression encodings"""
        accepted_enc = CompressionHandler.parse_header(self.headers.get('accept-encoding'))
        for enc in accepted_enc:
            if enc in self.server.supported_encodings:
                response_bytes = CompressionHandler.compress_payload(enc, response_bytes)
                self.send_header('Content-Encoding', enc)
                break
        return response_bytes

    def log_request(self, code='-', size='-'):
        pass  # suppress printing of every request to stderr


@dataclass(frozen=True)
class _ThreadInfo:
    thread: threading.Thread
    request: socket.socket
    client_address: tuple


class ThreadingHTTPServer(HTTPServer):
    """ Each request is handled in a thread.
    """

    def __init__(self, logger, server_address, RequestHandlerClass, dispatcher,
                 msg_reader, msg_factory, chunked_responses, supported_encodings):
        super().__init__(server_address, RequestHandlerClass)
        self.daemon_threads = True
        self.threads = []
        self.logger = logger
        self.dispatcher = dispatcher
        self.msg_reader = msg_reader
        self.msg_factory = msg_factory
        self.chunked_response = chunked_responses
        self.supported_encodings = supported_encodings

    def process_request_thread(self, request, client_address):
        """Same as in BaseServer but as a thread.
        """
        try:
            self.finish_request(request, client_address)
        except (ConnectionResetError, ConnectionAbortedError) as ex:
            self.logger.warn('Connection reset by {}: {}', client_address, ex)
        except Exception:
            self.handle_error(request, client_address)
        finally:
            self.shutdown_request(request)
            # this thread will close after return from this method, it can already be removed from self.threads
            for thread_info in self.threads:
                if thread_info.request == request:
                    self.threads.remove(thread_info)

    def process_request(self, request, client_address):
        """Start a new thread to process the request."""
        thread = threading.Thread(target=self.process_request_thread,
                                  args=(request, client_address),
                                  name=f'SubscrRecv{client_address}')
        thread.daemon = True
        self.threads.append(_ThreadInfo(thread, request, client_address))
        thread.start()

    def server_close(self):
        super().server_close()
        if self.dispatcher is not None:
            self.dispatcher.methods = {}
            self.dispatcher = None  # this leads to a '503' reaction in SOAPNotificationsHandler
        for thread_info in self.threads:
            if thread_info.thread.is_alive():
                try:
                    thread_info.request.shutdown(socket.SHUT_RDWR)
                    thread_info.request.close()
                    self.logger.info('closed socket for notifications from {}', thread_info.client_address)
                except OSError:
                    # the connection is already closed
                    continue
                except Exception as ex:
                    self.logger.warn('error closing socket for notifications from {}: {}', thread_info.client_address,
                                     ex)


class HttpServerThreadBase(threading.Thread):

    def __init__(self, my_ipaddress, ssl_context, supported_encodings,
                 request_handler_cls, dispatcher, msg_reader, msg_factory,
                 logger, chunked_responses=False):
        """
        Runs a ThreadingHTTPServer in a thread, so that it can be stopped without blocking.
        Handling of requests happens in two stages:
        - the http server instantiates a request handler with the request
        - the request handler forwards the handling itself to a dispatcher (due to the dynamic nature of the handling).
        :param my_ipaddress: The ip address that the http server shall bind to (no port!)
        :param ssl_context: a ssl.SslContext instance or None
        :param supported_encodings: a list of strings
        :param request_handler_cls: a class derived from HTTPRequestHandler
        :param dispatcher: a Dispatcher instance
        :param msg_reader: MessageReader instance
        :param msg_factory: MessageFactory instance
        :param logger: a python logger
        :param chunked_responses: boolean
        """
        super().__init__(name='Dev_SdcHttpServerThread')
        self.daemon = True

        self._my_ipaddress = my_ipaddress
        self._ssl_context = ssl_context
        self.my_port = None
        self.httpd = None
        self.supported_encodings = supported_encodings
        self.msg_reader = msg_reader
        self.msg_factory = msg_factory
        self.logger = logger
        self.chunked_responses = chunked_responses
        self._request_handler_cls = request_handler_cls
        # create and set up the dispatcher for all incoming requests
        self.dispatcher = dispatcher
        self.started_evt = threading.Event()  # helps to wait until thread has initialised is variables
        self._stop_requested = False
        self.base_url = None

    def run(self):
        self._stop_requested = False
        try:
            myport = 0  # zero means that OS selects a free port
            self.httpd = ThreadingHTTPServer(self.logger,
                                             (self._my_ipaddress, myport),
                                             self._request_handler_cls,
                                             self.dispatcher,
                                             self.msg_reader, self.msg_factory,
                                             self.chunked_responses,
                                             self.supported_encodings)
            self.my_port = self.httpd.server_port
            self.logger.info('starting http server on {}:{}', self._my_ipaddress, self.my_port)
            if self._ssl_context:
                self.httpd.socket = self._ssl_context.wrap_socket(self.httpd.socket)
                self.base_url = f'https://{self._my_ipaddress}:{self.my_port}/'
            else:
                self.base_url = f'http://{self._my_ipaddress}:{self.my_port}/'

            self.started_evt.set()
            self.httpd.serve_forever()
        except Exception:
            if not self._stop_requested:
                self.logger.error(
                    f'Unhandled Exception at thread runtime. Thread will abort! {traceback.format_exc()}')
            raise
        finally:
            self.logger.info('http server stopped.')

    def stop(self):
        self._stop_requested = True
        self.httpd.shutdown()
        self.httpd.server_close()
        for thread_info in self.httpd.threads:
            if thread_info.thread.is_alive():
                thread_info.thread.join(1)
            if thread_info.thread.is_alive():
                self.logger.warn('could not end client thread for notifications from {}', thread_info.client_address)
        del self.httpd.threads[:]


class RequestData:
    """This class holds all information about the processing of a http request together"""

    def __init__(self, http_header, path, peer_name, request=None):
        self.http_header = http_header
        self.request = request
        self.peer_name = peer_name  # for logging
        self.consumed_path_elements = []
        self.path = path
        if path.startswith('/'):
            path = path[1:]
        self.path_elements = path.split('/')
        self.message_data = None

    def consume_current_path_element(self):
        if len(self.path_elements) == 0:
            return None
        self.consumed_path_elements.append(self.path_elements[0])
        self.path_elements = self.path_elements[1:]
        return self.consumed_path_elements[-1]

    @property
    def current(self):
        return self.path_elements[0] if len(self.path_elements) > 0 else None
