from __future__ import annotations

import logging
import socket
import time
import traceback
from http.client import (
    HTTPConnection,
    HTTPException,
    HTTPResponse,
    HTTPSConnection,
    NotConnected,
    UnknownTransferEncoding,
)
from threading import Lock
from typing import TYPE_CHECKING, Protocol

from lxml.etree import XMLSyntaxError

from sdc11073 import commlog, observableproperties
from sdc11073.httpserver.compression import CompressionHandler
from sdc11073.httpserver.httpreader import HTTPReader, mk_chunks
from sdc11073.namespaces import default_ns_helper as ns_hlp
from sdc11073.pysoap.soapenvelope import Fault

if TYPE_CHECKING:
    from collections.abc import Iterable
    from ssl import SSLContext

    from sdc11073.consumer.manipulator import RequestManipulatorProtocol
    from sdc11073.definitions_base import BaseDefinitions
    from sdc11073.loghelper import LoggerAdapter
    from sdc11073.pysoap.msgfactory import CreatedMessage
    from sdc11073.pysoap.msgreader import MessageReader, ReceivedMessage


class HTTPConnectionNoDelay(HTTPConnection):
    """Connect method sets specific socket options."""

    def connect(self):
        """Connect to a host on a given port."""
        super().connect()
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, True)


class HTTPSConnectionNoDelay(HTTPSConnection):
    """Connect method sets specific socket options."""

    def connect(self):
        """Connect to a host on a given SSL port."""
        super().connect()
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, True)


class HTTPReturnCodeError(HTTPException):
    """HTTPReturnCodeError is used to map http return codes to Python exceptions."""

    def __init__(self, status: int, reason: str, soap_fault: Fault | None):
        super().__init__()
        self.status = status
        self.reason = reason
        self.soap_fault = soap_fault

    def __repr__(self) -> str:
        return f'HTTPReturnCodeError(status={self.status}, reason={self.reason} fault={self.soap_fault}'


class SoapClientProtocol(Protocol):
    """The expected interface of a soap client."""

    sock_name: tuple[str, int] | None

    def __init__(self,
                 netloc: str,
                 socket_timeout: int | float,
                 logger: LoggerAdapter,
                 ssl_context: SSLContext | None,
                 sdc_definitions: type[BaseDefinitions],
                 msg_reader: MessageReader,
                 supported_encodings: Iterable[str] | str = None,
                 request_encodings: Iterable[str] | str = None,
                 chunk_size: int = 0):
        ...

    def post_message_to(self, hosted_service_path: str,
                        message: CreatedMessage,
                        msg: str = '',
                        request_manipulator: RequestManipulatorProtocol | None = None,
                        validate: bool = True,
                        ) -> ReceivedMessage | None:
        """Send the message and return None if the response is empty else the received response."""
        ...

    def get_from_url(self, url: str, msg: str) -> bytes:
        """Send a GET request and return content of response."""
        ...

    def is_closed(self) -> bool:
        """Return True if connection is closed."""

    def connect(self):
        """Connect to net location."""

    def close(self):
        """Close connection."""

    @property
    def sock(self) -> socket.SocketType | None:
        """Return used socket."""


class SoapClient:
    """SOAP Client wraps a http connection. It can send / receive SoapEnvelopes."""

    _used_soap_clients = 0

    roundtrip_time = observableproperties.ObservableProperty()

    def __init__(self,
                 netloc: str,
                 socket_timeout: int | float,
                 logger: LoggerAdapter,
                 ssl_context: SSLContext | None,
                 sdc_definitions: type[BaseDefinitions],
                 msg_reader: MessageReader,
                 supported_encodings: Iterable[str] | str = None,
                 request_encodings: Iterable[str] | str = None,
                 chunk_size: int = 0):
        """Connect to one url.

        :param netloc: the location of the service (domain name:port) ###url of the service
        :param logger: a python logger instance
        :param ssl_context: an optional sll.SSLContext instance
        :param sdc_definitions: needed to normalize and de-normalize xml text.
        :param supported_encodings: Configured set of encodings that can be used.
                                    If None, all available encodings are used.
                                    This used for decompression of received responses.
                                    If this is an empty list, no compression is supported.
        :param request_encodings: An optional list of encodings that the other side accepts.
                                  It is used to compress requests.
                                  If not set, requests will not be compressed.
                                  If set, then the http request will be compressed using this method
        :param chunk_size: if value > 0, message is split into chunks of this size.
        """
        self._log = logger
        self._ssl_context = ssl_context
        self._sdc_definitions = sdc_definitions
        self._msg_reader = msg_reader
        self._netloc = netloc
        self._socket_timeout = socket_timeout
        self._http_connection = None  # connect later on demand
        self.__class__._used_soap_clients += 1  # noqa: SLF001
        self._client_number = self.__class__._used_soap_clients  # noqa: SLF001
        self._log.info('created soap client No. {} for {}', self._client_number, netloc)
        self.supported_encodings = supported_encodings if supported_encodings is not None \
            else CompressionHandler.available_encodings
        # request_encodings contains the compression algorithms that the other side accepts ( set at runtime)
        self.request_encodings = request_encodings if request_encodings is not None else []
        self._get_headers = self._make_get_headers()
        self._lock = Lock()
        self._chunk_size = chunk_size
        self._has_connection_error = False  # used to avoid implicit connects after an error
        self.sock_name: tuple[str, int] | None = None

    @property
    def netloc(self) -> str:
        """Return location, e.g.127.0.0.1:9999."""
        return self._netloc

    @property
    def sock(self) -> socket.SocketType | None:
        """Return used socket."""
        return None if self._http_connection is None else self._http_connection.sock

    def _mk_http_connection(self) -> [HTTPSConnectionNoDelay, HTTPConnectionNoDelay]:
        """Establish connection.

        Soap client never sends very large requests, the largest packages are notifications.
        We can use TCP_NODELAY for a little faster transmission.
        """
        if self._ssl_context is not None:
            conn = HTTPSConnectionNoDelay(self._netloc, context=self._ssl_context, timeout=self._socket_timeout)
        else:
            conn = HTTPConnectionNoDelay(self._netloc, timeout=self._socket_timeout)
        return conn

    def connect(self):
        """Connect to netloc."""
        self._has_connection_error = False
        self._http_connection = self._mk_http_connection()
        self._http_connection.connect()  # connect now so that we have own address and port for logging
        self.sock_name = self._http_connection.sock.getsockname()
        self._log.info('soap client No. {} uses connection={}:{}',
                       self._client_number, self.sock_name[0], self.sock_name[1])

    def close(self):
        """Close connection."""
        with self._lock:
            self._close_without_lock()

    def _close_without_lock(self):
        self.sock_name = None
        if self._http_connection is not None:
            self._log.info('closing soapClientNo {} for {}', self._client_number, self._netloc)
            self._http_connection.close()
            self._http_connection = None

    def is_closed(self) -> bool:
        """Return True if connection is closed."""
        return self._http_connection is None or self._http_connection.sock is None

    @staticmethod
    def _prepare_message(created_message: CreatedMessage,
                         request_manipulator: RequestManipulatorProtocol | None,
                         validate: bool) -> bytes:
        if hasattr(request_manipulator, 'manipulate_soapenvelope'):
            tmp = request_manipulator.manipulate_soapenvelope(created_message.p_msg)
            if tmp:
                created_message.p_msg = tmp
                # in this case do not validate , because the manipulator might intentionally have created invalid xml.
                validate = False
        xml_request = created_message.serialize(request_manipulator=request_manipulator,
                                                validate=validate)

        if hasattr(request_manipulator, 'manipulate_string'):
            tmp = request_manipulator.manipulate_string(xml_request)
            if tmp:
                xml_request = tmp
        return xml_request

    def post_message_to(self, path: str,
                        created_message: CreatedMessage,
                        msg: str = '',
                        request_manipulator: RequestManipulatorProtocol | None = None,
                        validate: bool = True,
                        ) -> ReceivedMessage | None:
        """Post created message to netloc/path.

        :param path: url path component
        :param created_message: The message that shall be sent
        :param msg: used in logs, helps to identify the context in which the method was called
        :param request_manipulator: see documentation of RequestManipulatorProtocol
        :param validate: set to False if no schema validation shall be done
        """
        if self.is_closed() and not self._has_connection_error:
            # implicit connect
            self.connect()
        if self.is_closed():
            raise NotConnected
        xml_request = self._prepare_message(created_message, request_manipulator, validate)
        started = time.perf_counter()
        try:
            with self._lock:
                http_response, xml_response = self._send_soap_request(path, xml_request, msg)
        finally:
            self.roundtrip_time = time.perf_counter() - started  # set roundtrip time even if method raises an exception
        if not xml_response:  # empty response
            return None

        message_data = self._msg_reader.read_received_message(xml_response)
        if message_data.action == f'{ns_hlp.WSA.namespace}/fault':
            soap_fault = Fault.from_node(message_data.p_msg.msg_node)
            raise HTTPReturnCodeError(http_response.status, http_response.reason, soap_fault)
        return message_data

    def _send_soap_request(self, path: str, xml: bytes, log_msg: str) -> tuple[HTTPResponse, bytes]:
        """Send SOAP request."""
        logging.getLogger(commlog.SOAP_REQUEST_OUT).debug(xml, extra={'http_method': 'POST'})
        self._log.debug("{}:POST to netloc='{}' path='{}'", log_msg, self._netloc, path)

        headers = {
            'Content-type': 'application/soap+xml; charset=utf-8',
            'user_agent': 'pysoap',
            'Connection': 'keep-alive',
        }
        # set accepted encodings
        if self.supported_encodings:
            headers['Accept-Encoding'] = ','.join(self.supported_encodings)
        # if possible encode ( compress) xml data
        if self.request_encodings:
            for compr in self.request_encodings:
                if compr in self.supported_encodings:
                    xml = CompressionHandler.compress_payload(compr, xml)
                    headers['Content-Encoding'] = compr
                    break
        # split message into chunks?
        if self._chunk_size > 0:
            headers['transfer-encoding'] = "chunked"
            xml = mk_chunks(xml, self._chunk_size)
        else:
            headers['Content-Length'] = str(len(xml))

        # send the request
        try:
            self._http_connection.request('POST', path, body=xml, headers=headers)
        except HTTPException as ex:
            self._log.warn("{}: could not send request, http exception = {!r}", log_msg, ex)
            self._close_without_lock()
            self._has_connection_error = True
            raise NotConnected from ex
        except OSError as ex:
            if ex.errno in (10053, 10054):
                self._log.warn("{}: could not send request to {}, OSError={!r}", log_msg, self.netloc, ex)
            else:
                self._log.warn("{}: could not send request to {}, OSError={}", log_msg, self.netloc,
                               traceback.format_exc())
            self._has_connection_error = True
            self._close_without_lock()
            raise NotConnected from ex
        except Exception as ex:  # noqa: BLE001
            self._log.warn("{}: POST to netloc='{}' path='{}': could not send request, error={!r}\n{}", log_msg,
                           self._netloc, path, ex, traceback.format_exc())
            self._has_connection_error = True
            self._close_without_lock()
            raise NotConnected from ex

        # read the response
        try:
            response = self._http_connection.getresponse()
        except HTTPException as ex:
            self._log.warn("{}: could not receive response, http exception = {!r} ", log_msg, ex)
            self._has_connection_error = True
            self._close_without_lock()
            raise NotConnected from ex
        except OSError as ex:
            if ex.errno in (10053, 10054):
                self._log.warn("{}: could not receive response, OSError={!r}", log_msg, ex)
            else:
                self._log.warn("{}: could not receive response, OSError={!r} ({!r})\n{}", log_msg, ex.errno,
                               ex, traceback.format_exc())
            self._has_connection_error = True
            self._close_without_lock()
            raise NotConnected from ex
        except Exception as ex:  # noqa: BLE001
            self._log.warn("{}: POST to netloc='{}' path='{}': could not receive response, error={!r}\n{}",
                           log_msg, self._netloc, path, ex, traceback.format_exc())
            self._has_connection_error = True
            self._close_without_lock()
            raise NotConnected from ex

        content = HTTPReader.read_response_body(response)

        if response.status >= 300:  # noqa: PLR2004
            self._log.error(
                "{}: POST to netloc='{}' path='{}': could not send request, HTTP response={}\ncontent='{}'", log_msg,
                self._netloc, path, response.status, content.decode('utf-8'))
            try:
                tmp = self._msg_reader.read_received_message(content)
            except XMLSyntaxError as ex:
                raise HTTPReturnCodeError(response.status, response.reason, None) from ex
            else:
                soap_fault = Fault.from_node(tmp.p_msg.msg_node)
                raise HTTPReturnCodeError(response.status, response.reason, soap_fault)

        response_headers = {k.lower(): v for k, v in response.getheaders()}

        self._log.debug('{}: response:{}; content has {} Bytes ', log_msg, response_headers, len(content))
        logging.getLogger(commlog.SOAP_RESPONSE_IN).debug(content, extra={'http_method': 'POST'})
        return response, content

    def _make_get_headers(self) -> dict[str, str]:
        headers = {
            'user_agent': 'pysoap',
            'Connection': 'keep-alive',
        }
        if self.supported_encodings:
            headers['Accept-Encoding'] = ', '.join(self.supported_encodings)
        return headers

    def get_from_url(self, url: str, msg: str) -> bytes:
        """Send a GET request and return content of response."""
        if self.is_closed() and not self._has_connection_error:
            # implicit connect
            self.connect()
        if self.is_closed():
            raise NotConnected

        if not url.startswith('/'):
            url = '/' + url
        self._log.debug("{} Get {}/{}", msg, self._netloc, url)
        with self._lock:
            self._http_connection.request('GET', url, headers=self._get_headers)
            response = self._http_connection.getresponse()
            headers = {k.lower(): v for k, v in response.getheaders()}
            _content = response.read()
            if 'content-encoding' in headers:
                enc = headers['content-encoding']
                if enc in self.supported_encodings:
                    content = CompressionHandler.decompress_payload(enc, _content)
                else:
                    self._log.warn("{}: unsupported compression ", headers['content-encoding'])
                    raise UnknownTransferEncoding
            else:
                content = _content
        return content
