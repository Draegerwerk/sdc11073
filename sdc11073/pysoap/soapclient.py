#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Pythonic simple SOAP Client implementation
Using lxml based SoapEnvelope."""
import http.client as httplib
import socket
import sys
import time
import traceback
from threading import Lock

from lxml.etree import XMLSyntaxError  # pylint: disable=no-name-in-module

from . import soapenvelope
from .. import commlog
from .. import observableproperties
from ..compression import CompressionHandler
from ..httprequesthandler import HTTPReader, mkchunks


class HTTPConnectionNoDelay(httplib.HTTPConnection):
    def connect(self):
        httplib.HTTPConnection.connect(self)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, True)


class HTTPSConnectionNoDelay(httplib.HTTPSConnection):
    def connect(self):
        httplib.HTTPSConnection.connect(self)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, True)


class HTTPReturnCodeError(httplib.HTTPException):
    ''' THis class is used to map http return codes to Python exceptions.'''

    def __init__(self, status, reason, soapfault):
        '''
        @param status: integer, e.g. 404
        param reason: the provided human readable text
        '''
        super().__init__()
        self.status = status
        self.reason = reason
        self.soapfault = soapfault

    def __repr__(self):
        if self.soapfault:
            return 'HTTPReturnCodeError(status={}, reason={}'.format(self.status, self.soapfault)
        return 'HTTPReturnCodeError(status={}, reason={}'.format(self.status, self.reason)


class SoapClient:
    _usedSoapClients = 0
    SOCKET_TIMEOUT = 10 if sys.gettrace() is None else 1000  # higher timeout for debugging

    """SOAP Client"""
    roundtrip_time = observableproperties.ObservableProperty()

    def __init__(self, netloc, logger, ssl_context, sdc_definitions, supported_encodings=None,
                 request_encodings=None, chunked_requests=False):
        ''' Connects to one url
        @param netloc: the location of the service (domainname:port) ###url of the service
        @param ssl_context: an optional sll.SSLContext instance
        @param biceps_schema:
        @param supported_encodings: configured set of encodings that can be used. If None, all available encodings are used.
                                This used for decompression of received responses.
                                If this is an empty list, no compression is supported.
        @param request_encodings: an optional list of encodings that the other side accepts. It is used to compress requests.
                                If not set, requests will not be commpressed.
                                If set, then the http request will be compressed using this method
        '''
        self._log = logger
        self._ssl_context = ssl_context
        self._sdc_definitions = sdc_definitions
        self._netloc = netloc
        self._http_connection = None  # connect later on demand
        self.__class__._usedSoapClients += 1  # pylint: disable=protected-access
        self._client_number = self.__class__._usedSoapClients  # pylint: disable=protected-access
        self._log.info('created soap client No. {} for {}', self._client_number, netloc)
        self.supported_encodings = supported_encodings if supported_encodings is not None else CompressionHandler.available_encodings
        self.request_encodings = request_encodings if request_encodings is not None else []  # these compression alg's does the other side accept ( set at runtime)
        self._get_headers = self._make_get_headers()
        self._lock = Lock()
        self._chunked_requests = chunked_requests

    @property
    def netloc(self):
        return self._netloc

    @property
    def sock(self):
        return None if self._http_connection is None else self._http_connection.sock

    def _mk_http_connection(self):
        ''' Soap client never sends very large requests, the largest packages are notifications.
         Therefore we can use TCP_NODELAY for a little faster transmission.
        (Otherwise there would be a chance that receivers windows size decreases, which would result in smaller
        packages and therefore higher network load.'''
        if self._ssl_context is not None:
            conn = HTTPSConnectionNoDelay(self._netloc, context=self._ssl_context, timeout=self.SOCKET_TIMEOUT)
        else:
            conn = HTTPConnectionNoDelay(self._netloc, timeout=self.SOCKET_TIMEOUT)
        return conn

    def connect(self):
        self._http_connection = self._mk_http_connection()
        self._http_connection.connect()  # connect now so that we have own address and port for logging
        my_addr = self._http_connection.sock.getsockname()
        self._log.info('soap client No. {} uses connection={}:{}', self._client_number, my_addr[0], my_addr[1])

    def close(self):
        with self._lock:
            if self._http_connection is not None:
                self._log.info('closing soapClientNo {} for {}', self._client_number, self._netloc)
                self._http_connection.close()
                self._http_connection = None

    def is_closed(self):
        return self._http_connection is None

    def post_soap_envelope_to(self, path, soap_envelope, response_factory=None, schema=None, msg='',
                              request_manipulator=None):
        '''
        @param path: url path component
        @param soapEnvelopeRequest: The soap envelope that shall be sent
        @param response_factory: a callable that creates a response object from received xml. If None, a ReceivedSoap12Envelope will be created
        @param schema: If given, the request is validated against this schema
        @param msg: used in logs, helps to identify the context in which the method was called
        '''
        if self.is_closed():
            self.connect()
        return self.__post_soap_envelope(soap_envelope, response_factory, schema, path, msg, request_manipulator)

    def __post_soap_envelope(self, soap_envelope, response_factory, schema, path, msg, request_manipulator):
        if schema is not None:
            soap_envelope.validate_body(schema)
        if hasattr(request_manipulator, 'manipulate_soapenvelope'):
            tmp = request_manipulator.manipulate_soapenvelope(soap_envelope)
            if tmp:
                soap_envelope = tmp
        normalized_xml_request = soap_envelope.as_xml(request_manipulator=request_manipulator)
        xml_request = self._sdc_definitions.denormalize_xml_text(normalized_xml_request)

        # MDPWS:R0007 A text SOAP envelope shall be serialized using utf-8 character encoding
        assert b'utf-8' in xml_request[:100].lower()
        if hasattr(request_manipulator, 'manipulate_string'):
            tmp = request_manipulator.manipulate_string(xml_request)
            if tmp:
                xml_request = tmp

        started = time.perf_counter()
        try:
            xml_response = self._send_soap_request(path, xml_request, msg)
        finally:
            self.roundtrip_time = time.perf_counter() - started  # set roundtrip time even if method raises an exception
        normalized_xml_response = self._sdc_definitions.normalize_xml_text(xml_response)
        my_response_factory = response_factory or soapenvelope.ReceivedSoap12Envelope.from_xml_string
        try:
            return my_response_factory(normalized_xml_response, schema)
        except XMLSyntaxError as ex:
            self._log.error('{} XMLSyntaxError in string: "{}"', msg, normalized_xml_response)
            raise RuntimeError('{} in "{}"'.format(ex, normalized_xml_response))

    def _send_soap_request(self, path, xml, msg):
        """Send SOAP request using HTTP"""
        if not isinstance(xml, bytes):
            xml = xml.encode('utf-8')

        headers = {
            'Content-type': 'application/soap+xml; charset=utf-8',
            'user_agent': 'pysoap',
            'Connection': 'keep-alive',
        }
        commlog.get_communication_logger().log_soap_request_out(xml, 'POST')

        if self.supported_encodings:
            headers['Accept-Encoding'] = ','.join(self.supported_encodings)
        if self.request_encodings:
            for compr in self.request_encodings:
                if compr in self.supported_encodings:
                    xml = CompressionHandler.compress_payload(compr, xml)
                    headers['Content-Encoding'] = compr
                    break
        if self._chunked_requests:
            headers['transfer-encoding'] = "chunked"
            xml = mkchunks(xml)
        else:
            headers['Content-Length'] = str(len(xml))

        xml = bytearray(xml)  # cast to bytes, required to bypass httplib checks for is str

        self._log.debug("{}:POST to netloc='{}' path='{}'", msg, self._netloc, path)
        response = None
        content = None

        def send_request():
            do_reopen = False
            success = False
            try:
                self._http_connection.request('POST', path, body=xml, headers=headers)
                return True, do_reopen  # success = True
            except httplib.CannotSendRequest as ex:
                # for whatever reason the response of the previous call was not read. read it and try again
                self._log.warn(
                    "{}: could not send request, got httplib.CannotSendRequest Error. Will read response and retry",
                    msg)
                tmp = self._http_connection.getresponse()
                tmp.read()
            except OSError as ex:
                if ex.errno in (10053, 10054):
                    self._log.warn("{}: could not send request to {}, OSError={!r}", msg, self.netloc, ex)
                else:
                    self._log.warn("{}: could not send request to {}, OSError={}", msg, self.netloc, traceback.format_exc())
                do_reopen = True
            except Exception as ex:
                self._log.warn("{}: POST to netloc='{}' path='{}': could not send request, error={!r}\n{}", msg,
                               self._netloc, path, ex, traceback.format_exc())
            return success, do_reopen

        def get_response():
            try:
                return self._http_connection.getresponse()
            except httplib.BadStatusLine as ex:
                self._log.warn("{}: invalid http response, error= {!r} ", msg, ex)
                raise
            except OSError as ex:
                if ex.errno in (10053, 10054):
                    self._log.warn("{}: could not receive response, OSError={!r}", msg, ex)
                else:
                    self._log.warn("{}: could not receive response, OSError={} ({!r})\n{}", msg, ex.errno,
                                   ex, traceback.format_exc())
                raise httplib.NotConnected()
            except Exception as ex:
                self._log.warn("{}: POST to netloc='{}' path='{}': could not receive response, error={!r}\n{}",
                               msg, self._netloc, path, ex, traceback.format_exc())
                raise httplib.NotConnected()

        def reopen_http_connection():
            self._log.info("{}: will close and reopen the connection and then try again", msg)
            self._http_connection.close()
            try:
                self._http_connection.connect()
            except Exception as ex:
                self._log.error("{}: could not reopen the connection, error={!r}\n{}\ncall-stack ={}",
                                msg, ex, traceback.format_exc(), ''.join(traceback.format_stack()))
                self._http_connection.close()
                raise httplib.NotConnected()

        with self._lock:
            _retry_send = 2  # ugly construct that allows to retry sending the request once
            while _retry_send > 0:
                _retry_send -= 1
                success, _do_reopen = send_request()
                if not success:
                    if _do_reopen:
                        reopen_http_connection()
                    else:
                        raise httplib.NotConnected()
                else:
                    try:
                        response = get_response()
                        _retry_send = -1  # -1 == SUCCESS
                    except httplib.NotConnected:
                        self._log.info("{}: will reopen after get_response error", msg)
                        reopen_http_connection()

            if _retry_send != -1:
                raise httplib.NotConnected()

            content = HTTPReader.read_response_body(response)

            if response.status >= 300:
                self._log.error(
                    "{}: POST to netloc='{}' path='{}': could not send request, HTTP response={}\ncontent='{}'", msg,
                    self._netloc, path, response.status, content)
                soapfault = soapenvelope.ReceivedSoapFault.from_xml_string(content)

                raise HTTPReturnCodeError(response.status, content, soapfault)

            response_headers = {k.lower(): v for k, v in response.getheaders()}

            self._log.debug('{}: response:{}; content has {} Bytes ', msg, response_headers, len(content))
            commlog.get_communication_logger().log_soap_response_in(content, 'POST')
            return content

    def _make_get_headers(self):
        headers = {
            'user_agent': 'pysoap',
            'Connection': 'keep-alive'
        }
        if self.supported_encodings:
            headers['Accept-Encoding'] = ', '.join(self.supported_encodings)
        return headers

    def get_url(self, url, msg):
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
                    raise httplib.UnknownTransferEncoding
            else:
                content = _content
        return content
