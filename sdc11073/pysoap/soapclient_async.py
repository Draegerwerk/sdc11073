from aiohttp.client import ClientSession, TCPConnector
import asyncio
import socket
import sys
import time
import traceback
from threading import Lock

from .. import commlog
from .. import observableproperties
from ..compression import CompressionHandler
from ..httprequesthandler import HTTPReader, mkchunks
from ..namespaces import Prefixes
from .soapclient import SoapClient, HTTPReturnCodeError


class AioSoapClient(SoapClient):
    """SOAP Client wraps an http connection. It can send / receive SoapEnvelopes."""
    _usedSoapClients = 0
    SOCKET_TIMEOUT = 5 if sys.gettrace() is None else 1000  # higher timeout for debugging

    roundtrip_time = observableproperties.ObservableProperty()

    # def __init__(self, netloc, logger, ssl_context, sdc_definitions, msg_reader, supported_encodings=None,
    #              request_encodings=None, chunked_requests=False):
    #     """ Connects to one url
    #     :param netloc: the location of the service (domainname:port) ###url of the service
    #     :param logger: a python logger instance
    #     :param ssl_context: an optional sll.SSLContext instance
    #     :param sdc_definitions: needed to normalize and de-normalize xml text.
    #     :param supported_encodings: configured set of encodings that can be used. If None, all available encodings are used.
    #                             This used for decompression of received responses.
    #                             If this is an empty list, no compression is supported.
    #     :param request_encodings: an optional list of encodings that the other side accepts. It is used to compress requests.
    #                             If not set, requests will not be commpressed.
    #                             If set, then the http request will be compressed using this method
    #     :param chunked_requests: it True, requests are chunk-encoded
    #     """
    #     self._log = logger
    #     self._ssl_context = ssl_context
    #     self._sdc_definitions = sdc_definitions
    #     self._msg_reader = msg_reader
    #     self._netloc = f'http://{netloc}/'
    #     self._http_connection = None  # connect later on demand
    #     self.__class__._usedSoapClients += 1  # pylint: disable=protected-access
    #     self._client_number = self.__class__._usedSoapClients  # pylint: disable=protected-access
    #     self._log.info('created soap client No. {} for {}', self._client_number, netloc)
    #     self.supported_encodings = supported_encodings if supported_encodings is not None else CompressionHandler.available_encodings
    #     self.request_encodings = request_encodings if request_encodings is not None else []  # these compression alg's does the other side accept ( set at runtime)
    #     self._get_headers = self._make_get_headers()
    #     self._lock = Lock()
    #     self._chunked_requests = chunked_requests
    #     self._async_loop = None

    def __init__(self, netloc, logger, ssl_context, sdc_definitions, msg_reader, supported_encodings=None,
                 request_encodings=None, chunked_requests=False):
        super().__init__( netloc, logger, ssl_context, sdc_definitions, msg_reader, supported_encodings,
                          request_encodings, chunked_requests)
        self._async_loop = None
        self._netloc = f'http://{netloc}/'


    async def _mk_http_connection(self) -> ClientSession:
        """ Soap client never sends very large requests, the largest packages are notifications.
         Therefore we can use TCP_NODELAY for a little faster transmission.
        (Otherwise there would be a chance that receivers windows size decreases, which would result in smaller
        packages and therefore higher network load."""
        if self._ssl_context is not None:
            # conn = HTTPSConnectionNoDelay(self._netloc, context=self._ssl_context, timeout=self.SOCKET_TIMEOUT)
            connector = TCPConnector(ssl=self._ssl_context)
        else:
            # conn = HTTPConnectionNoDelay(self._netloc, timeout=self.SOCKET_TIMEOUT)
            connector = TCPConnector()
        conn = ClientSession(self._netloc, connector=connector)
        #asyncio.get_event_loop().run_forever()
        return conn

    def connect(self):
        try:
            self._async_loop = asyncio.get_event_loop()
        except RuntimeError: # there is no current event loop
            pass
        if self._async_loop is None:
            self._async_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._async_loop)
        self._http_connection = self._async_loop.run_until_complete(self._mk_http_connection())

    # def close(self):
    #     with self._lock:
    #         if self._http_connection is not None:
    #             self._log.info('closing soapClientNo {} for {}', self._client_number, self._netloc)
    #             self._http_connection.close()
    #             self._http_connection = None

    # def post_message_to(self, path, created_message, msg='', request_manipulator=None):
    #     # if asyncio.get_event_loop().is_closed():
    #     #     print(f'post_message_to {path} new event loop')
    #     #     loop = asyncio.new_event_loop()
    #     #     asyncio.set_event_loop(loop)
    #     print(f'post_message_to {path}')
    #     loop = asyncio.new_event_loop()
    #     result = loop.run(self.async_post_message_to(path, created_message, msg, request_manipulator))
    #     print(f'post_message_to {path} done {result}')
    #     loop = asyncio.new_event_loop()
    #     asyncio.set_event_loop(loop)
    #     return result



    # def get_url(self, url, msg):
    #     if not url.startswith('/'):
    #         url = '/' + url
    #     self._log.debug("{} Get {}/{}", msg, self._netloc, url)
    #     with self._lock:
    #         self._http_connection.request('GET', url, headers=self._get_headers)
    #         response = self._http_connection.getresponse()
    #         headers = {k.lower(): v for k, v in response.getheaders()}
    #         _content = response.read()
    #         if 'content-encoding' in headers:
    #             enc = headers['content-encoding']
    #             if enc in self.supported_encodings:
    #                 content = CompressionHandler.decompress_payload(enc, _content)
    #             else:
    #                 self._log.warn("{}: unsupported compression ", headers['content-encoding'])
    #                 raise httplib.UnknownTransferEncoding
    #         else:
    #             content = _content
    #     return content

    def get_url(self, url, msg):
        if not url.startswith('/'):
            url = '/' + url
        self._log.debug("{} Get {}/{}", msg, self._netloc, url)
        with self._lock:
            result = self._async_loop.run_until_complete(self.async_get_url(url, msg))
            #print(f'post_message_to {url} done {result}')
            return result

    async def async_get_url(self, url, msg):
        async with self._http_connection.get(url, headers=self._get_headers) as response:
            assert response.status == 200
            xml_response = await response.text()
            headers = {k.lower(): v for k, v in response.headers.items()}
            return xml_response
            # if 'content-encoding' in headers:
            #     enc = headers['content-encoding']
            #     if enc in self.supported_encodings:
            #         content = CompressionHandler.decompress_payload(enc, xml_response.encode('utf-8'))
            #     else:
            #         self._log.warn("{}: unsupported compression ", headers['content-encoding'])
            #         raise httplib.UnknownTransferEncoding
            # else:
            #     content = xml_response
            # return content

    def post_message_to(self, path, created_message, msg='', request_manipulator=None):
#        if self.is_closed():
#            self.connect()
        #print(f'post_message_to {path}')
        with self._lock:
            result = self._async_loop.run_until_complete(self.async_post_message_to(path, created_message, msg, request_manipulator))
        #print(f'post_message_to {path} done {result}')
        return result

    async def async_post_message_to(self, path, created_message, msg='', request_manipulator=None):
        """
        :param path: url path component
        :param created_message: The message that shall be sent
        :param response_factory: a callable that creates a response object from received xml. If None, a ReceivedSoap12Envelope will be created
        :param schema: If given, the request is validated against this schema
        :param msg: used in logs, helps to identify the context in which the method was called
        """
        if self.is_closed():
            self._http_connection = await self._mk_http_connection()

        if hasattr(request_manipulator, 'manipulate_soapenvelope'):
            tmp = request_manipulator.manipulate_soapenvelope(created_message.p_msg)
            if tmp:
                created_message.p_msg = tmp
        xml_request = created_message.serialize_message(pretty=False, normalized=False,
                                                        request_manipulator=request_manipulator)

        # MDPWS:R0007 A text SOAP envelope shall be serialized using utf-8 character encoding
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
            commlog.get_communication_logger().log_soap_request_out(xml_request, 'POST')

            if self.supported_encodings:
                headers['Accept-Encoding'] = ','.join(self.supported_encodings)
            if self.request_encodings:
                for compr in self.request_encodings:
                    if compr in self.supported_encodings:
                        xml_request = CompressionHandler.compress_payload(compr, xml_request)
                        headers['Content-Encoding'] = compr
                        break
            if self._chunked_requests:
                headers['transfer-encoding'] = "chunked"
                xml_request = mkchunks(xml_request)
            else:
                headers['Content-Length'] = str(len(xml_request))

            async with self._http_connection.post(path, data=xml_request, headers=headers) as resp:
                #assert resp.status == 200
                xml_response = await resp.text()

        finally:
            self.roundtrip_time = time.perf_counter() - started  # set roundtrip time even if method raises an exception
        if not xml_response:  # empty response
            return None

        message_data = self._msg_reader.read_received_message(xml_response.encode('utf-8'))
        if message_data.action == f'{Prefixes.WSA.namespace}/fault':
            soap_fault = self._msg_reader.read_fault_message(message_data)
            raise HTTPReturnCodeError(resp.status, resp.reason, soap_fault)
        return message_data

    # def _send_soap_request(self, path, xml, msg) ->(httplib.HTTPResponse, str):
    #     """Send SOAP request using HTTP"""
    #     if not isinstance(xml, bytes):
    #         xml = xml.encode('utf-8')
    #
    #     headers = {
    #         'Content-type': 'application/soap+xml; charset=utf-8',
    #         'user_agent': 'pysoap',
    #         'Connection': 'keep-alive',
    #     }
    #     commlog.get_communication_logger().log_soap_request_out(xml, 'POST')
    #
    #     if self.supported_encodings:
    #         headers['Accept-Encoding'] = ','.join(self.supported_encodings)
    #     if self.request_encodings:
    #         for compr in self.request_encodings:
    #             if compr in self.supported_encodings:
    #                 xml = CompressionHandler.compress_payload(compr, xml)
    #                 headers['Content-Encoding'] = compr
    #                 break
    #     if self._chunked_requests:
    #         headers['transfer-encoding'] = "chunked"
    #         xml = mkchunks(xml)
    #     else:
    #         headers['Content-Length'] = str(len(xml))
    #
    #     xml = bytearray(xml)  # cast to bytes, required to bypass httplib checks for is str
    #
    #     self._log.debug("{}:POST to netloc='{}' path='{}'", msg, self._netloc, path)
    #     response = None
    #
    #     def send_request():
    #         do_reopen = False
    #         try:
    #             # self._http_connection.request('POST', path, body=xml, headers=headers)
    #             resp = self._http_connection.post(path, data=xml, headers=headers)
    #             return True, do_reopen  # success = True
    #         except httplib.CannotSendRequest as ex:
    #             # for whatever reason the response of the previous call was not read. read it and try again
    #             self._log.warn(
    #                 "{}: could not send request, got error '{}'. Will read response and retry", msg, ex)
    #             tmp = self._http_connection.getresponse()
    #             tmp.read()
    #         except OSError as ex:
    #             if ex.errno in (10053, 10054):
    #                 self._log.warn("{}: could not send request to {}, OSError={!r}", msg, self.netloc, ex)
    #             else:
    #                 self._log.warn("{}: could not send request to {}, OSError={}", msg, self.netloc,
    #                                traceback.format_exc())
    #             do_reopen = True
    #         except Exception as ex:
    #             self._log.warn("{}: POST to netloc='{}' path='{}': could not send request, error={!r}\n{}", msg,
    #                            self._netloc, path, ex, traceback.format_exc())
    #         return False, do_reopen  # success = False
    #
    #     def get_response() -> httplib.HTTPResponse:
    #         try:
    #             return self._http_connection.getresponse()
    #         except httplib.BadStatusLine as ex:
    #             self._log.warn("{}: invalid http response, error= {!r} ", msg, ex)
    #             raise
    #         except OSError as ex:
    #             if ex.errno in (10053, 10054):
    #                 self._log.warn("{}: could not receive response, OSError={!r}", msg, ex)
    #             else:
    #                 self._log.warn("{}: could not receive response, OSError={} ({!r})\n{}", msg, ex.errno,
    #                                ex, traceback.format_exc())
    #             raise httplib.NotConnected()
    #         except Exception as ex:
    #             self._log.warn("{}: POST to netloc='{}' path='{}': could not receive response, error={!r}\n{}",
    #                            msg, self._netloc, path, ex, traceback.format_exc())
    #             raise httplib.NotConnected()
    #
    #     def reopen_http_connection():
    #         self._log.info("{}: will close and reopen the connection and then try again", msg)
    #         self._http_connection.close()
    #         try:
    #             self._http_connection.connect()
    #         except Exception as ex:
    #             self._log.error("{}: could not reopen the connection, error={!r}\n{}\ncall-stack ={}",
    #                             msg, ex, traceback.format_exc(), ''.join(traceback.format_stack()))
    #             self._http_connection.close()
    #             raise httplib.NotConnected()
    #
    #     with self._lock:
    #         _retry_send = 2  # ugly construct that allows to retry sending the request once
    #         while _retry_send > 0:
    #             _retry_send -= 1
    #             success, _do_reopen = send_request()
    #             if not success:
    #                 if _do_reopen:
    #                     reopen_http_connection()
    #                 else:
    #                     raise httplib.NotConnected()
    #             else:
    #                 try:
    #                     response = get_response()
    #                     _retry_send = -1  # -1 == SUCCESS
    #                 except httplib.NotConnected:
    #                     self._log.info("{}: will reopen after get_response error", msg)
    #                     reopen_http_connection()
    #
    #         if _retry_send != -1:
    #             raise httplib.NotConnected()
    #
    #         content = HTTPReader.read_response_body(response)
    #
    #         if response.status >= 300:
    #             self._log.error(
    #                 "{}: POST to netloc='{}' path='{}': could not send request, HTTP response={}\ncontent='{}'", msg,
    #                 self._netloc, path, response.status, content)
    #             tmp = self._msg_reader.read_received_message(content)
    #             soap_fault = self._msg_reader.read_fault_message(tmp)
    #             raise HTTPReturnCodeError(response.status, response.reason, soap_fault)
    #
    #         response_headers = {k.lower(): v for k, v in response.getheaders()}
    #
    #         self._log.debug('{}: response:{}; content has {} Bytes ', msg, response_headers, len(content))
    #         commlog.get_communication_logger().log_soap_response_in(content, 'POST')
    #         return response, content

    def _make_get_headers(self):
        headers = {
            'user_agent': 'pysoap',
            'Connection': 'keep-alive'
        }
        if self.supported_encodings:
            headers['Accept-Encoding'] = ', '.join(self.supported_encodings)
        return headers

