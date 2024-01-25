from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING

from aiohttp.client import ClientSession, ClientTimeout, TCPConnector

from sdc11073 import commlog, observableproperties
from sdc11073.httpserver.compression import CompressionHandler
from sdc11073.httpserver.httpreader import mk_chunks
from sdc11073.namespaces import default_ns_helper as ns_hlp
from sdc11073.pysoap.soapenvelope import Fault

from .soapclient import HTTPReturnCodeError

if TYPE_CHECKING:
    from ssl import SSLContext

    from sdc11073.consumer.manipulator import RequestManipulatorProtocol
    from sdc11073.definitions_base import BaseDefinitions
    from sdc11073.loghelper import LoggerAdapter
    from sdc11073.pysoap.msgfactory import CreatedMessage
    from sdc11073.pysoap.msgreader import MessageReader, ReceivedMessage


class SoapClientAsync:
    """SOAP Client wraps an http connection. It can send / receive SoapEnvelopes."""

    _used_soap_clients = 0

    roundtrip_time = observableproperties.ObservableProperty()

    def __init__(self,
                 netloc: str,
                 socket_timeout: int | float,
                 logger: LoggerAdapter,
                 ssl_context: [SSLContext, None],
                 sdc_definitions: BaseDefinitions,
                 msg_reader: MessageReader,
                 supported_encodings: list[str] | None = None,
                 request_encodings: list[str] | None = None,
                 chunk_size: int = 0):
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
        # these compression alg's does the other side accept ( set at runtime):
        self.request_encodings = request_encodings if request_encodings is not None else []
        self._get_headers = self._make_get_headers()
        self._chunk_size = chunk_size
        self._netloc = netloc

    @property
    def netloc(self) -> str:
        """Return location, e.g.127.0.0.1:9999."""
        return self._netloc

    def is_closed(self) -> bool:
        """Return True if connection is closed."""
        return self._http_connection is None

    async def _mk_http_connection(self) -> ClientSession:
        """TCP_NODELAY is set by default in asyncio create_connection."""
        if self._ssl_context is not None:
            connector = TCPConnector(ssl=self._ssl_context)
            base_url = f'https://{self._netloc}/'
        else:
            connector = TCPConnector()
            base_url = f'http://{self._netloc}/'

        return ClientSession(base_url, connector=connector, timeout=ClientTimeout(self._socket_timeout))

    async def async_connect(self):
        """Connect to netloc."""
        self._http_connection = await self._mk_http_connection()

    def close(self):
        """Close connection."""
        # ToDo: run async_close in event loop
        self._http_connection = None

    async def async_close(self):
        """Close connection."""
        if self._http_connection is not None:
            self._log.info('closing soapClientNo {} for {}', self._client_number, self._netloc)
            await self._http_connection.close()
            self._http_connection = None

    async def async_post_message_to(self, path: str,
                                    created_message: CreatedMessage,
                                    request_manipulator: RequestManipulatorProtocol | None = None) \
            -> ReceivedMessage | None:
        """Send the message and return None if the response is empty else the received response.

        :param path: url path component
        :param created_message: The message that shall be sent
        :param request_manipulator: can manipulate data before sending
        """
        if self.is_closed():
            self._http_connection = await self._mk_http_connection()

        if hasattr(request_manipulator, 'manipulate_soapenvelope'):
            tmp = request_manipulator.manipulate_soapenvelope(created_message.p_msg)
            if tmp:
                created_message.p_msg = tmp
        xml_request = created_message.serialize(request_manipulator=request_manipulator)

        assert b'utf-8' in xml_request[:100].lower()
        if hasattr(request_manipulator, 'manipulate_string'):
            tmp = request_manipulator.manipulate_string(xml_request)
            if tmp:
                xml_request = tmp

        started = time.perf_counter()
        try:
            headers = {
                'Content-type': 'application/soap+xml; charset=utf-8',
                'user_agent': 'pysoap',
                'Connection': 'keep-alive',
            }
            logging.getLogger(commlog.SOAP_REQUEST_OUT).debug(xml_request, extra={'http_method': 'POST'})

            if self.supported_encodings:
                headers['Accept-Encoding'] = ','.join(self.supported_encodings)
            if self.request_encodings:
                for compr in self.request_encodings:
                    if compr in self.supported_encodings:
                        xml_request = CompressionHandler.compress_payload(compr, xml_request)
                        headers['Content-Encoding'] = compr
                        break
            if self._chunk_size > 0:
                headers['transfer-encoding'] = "chunked"
                xml_request = mk_chunks(xml_request, chunk_size=self._chunk_size)
            else:
                headers['Content-Length'] = str(len(xml_request))

            async with self._http_connection.post(path, data=xml_request, headers=headers) as resp:
                xml_response = await resp.text()

        finally:
            self.roundtrip_time = time.perf_counter() - started  # set roundtrip time even if method raises an exception
        if not xml_response:  # empty response
            return None

        message_data = self._msg_reader.read_received_message(xml_response.encode('utf-8'))
        if message_data.action == f'{ns_hlp.WSA.namespace}/fault':
            soap_fault = Fault.from_node(message_data.p_msg.msg_node)
            raise HTTPReturnCodeError(resp.status, resp.reason, soap_fault)
        return message_data

    def _make_get_headers(self) -> dict[str, str]:
        headers = {
            'user_agent': 'pysoap',
            'Connection': 'keep-alive',
        }
        if self.supported_encodings:
            headers['Accept-Encoding'] = ', '.join(self.supported_encodings)
        return headers
