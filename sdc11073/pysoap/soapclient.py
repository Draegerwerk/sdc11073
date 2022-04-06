#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Pythonic simple SOAP Client implementation
Using lxml based SoapEnvelope."""
import sys
import traceback
from threading import Lock
import socket
import time
import http.client as httplib
from lxml import etree as etree_

from .. import observableproperties
from .. import commlog
from ..compression import CompressionHandler
from . import soapenvelope
from ..httprequesthandler import HTTPReader, mkchunks

class HTTPConnection_NODELAY(httplib.HTTPConnection):
    def connect(self):
        httplib.HTTPConnection.connect(self)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, True)



class HTTPSConnection_NODELAY(httplib.HTTPSConnection):
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
        super(HTTPReturnCodeError, self).__init__()
        self.status = status
        self.reason = reason
        self.soapfault = soapfault

    def __repr__(self):
        if self.soapfault:
            return 'HTTPReturnCodeError(status={}, reason={}'.format(self.status, self.soapfault)
        else:
            return 'HTTPReturnCodeError(status={}, reason={}'.format(self.status, self.reason)



class SoapClient(CompressionHandler):
    _usedSoapClients = 0
    SOCKET_TIMEOUT = 10 if sys.gettrace() is None else 1000 # higher timeout for debugging

    """SOAP Client"""
    roundtrip_time = observableproperties.ObservableProperty()
    def __init__(self, netloc, logger, sslContext, sdc_definitions, supportedEncodings=None,
                 requestEncodings=None, chunked_requests=False, xml_validator=None):
        ''' Connects to one url
        @param netloc: the location of the service (domainname:port) ###url of the service
        @param sslContext: an optional sll.SSLContext instance
        @param supportedEncodings: configured set of encodings that can be used. If None, all available encodings are used.
                                This used for decompression of received responses.
                                If this is an empty list, no compression is supported.
        @param requestEncodings: an optional list of encodings that the other side accepts. It is used to compress requests.
                                If not set, requests will not be commpressed.
                                If set, then the http request will be compressed using this method
        @param xml_validator: optional etree.XMLSchema instance
        '''
        self._log = logger
        self._sslContext = sslContext
        self._sdc_definitions = sdc_definitions
        self._netloc = netloc
        self._httpConnection = None # connect later on demand
        self.__class__._usedSoapClients += 1   #pylint: disable=protected-access
        self._clientNo = self.__class__._usedSoapClients   #pylint: disable=protected-access
        self._log.info('created soapClient No. {} for {}', self._clientNo, netloc)
        self.supportedEncodings = supportedEncodings if supportedEncodings is not None else self.available_encodings
        self.requestEncodings = requestEncodings  if requestEncodings is not None else [] # these compression alg's does the other side accept ( set at runtime)
        self._xml_validator = xml_validator
        self._makeGetHeaders()
        self._lock = Lock()
        self._is_closed = True
        self._chunked_requests = chunked_requests
        self.connect()

    @property
    def netloc(self):
        return self._netloc

    @property
    def sock(self):
        return None if self._httpConnection is None else self._httpConnection.sock

    def _mkHttpConnection(self):
        ''' Soap client never sends very large requests, the largest packages are notifications.
         Therefore we can use TCP_NODELAY for a little faster transmission.
        (Otherwise there would be a chance that receivers windows size decreases, which would result in smaller
        packages and therefore higher network load.'''
        if self._sslContext is not None:
            conn = HTTPSConnection_NODELAY(self._netloc, context=self._sslContext, timeout=self.SOCKET_TIMEOUT)
        else:
            conn =  HTTPConnection_NODELAY(self._netloc, timeout=self.SOCKET_TIMEOUT)
        return conn

    def connect(self):
        with self._lock:
            if self._httpConnection is None:
                self._httpConnection = self._mkHttpConnection()
                self._httpConnection.connect() # connect now so that we have own address and port for logging
                my_addr = self._httpConnection.sock.getsockname()
                self._log.info('soapClient No. {} uses connection={}:{}', self._clientNo, my_addr[0], my_addr[1])
                self._is_closed = False

    def close(self):
        with self._lock:
            if self._httpConnection is not None:
                self._log.info('closing soapClientNo {} for {}', self._clientNo, self._netloc)
                self._httpConnection.close()
                self._httpConnection = None
                self._is_closed = True
    
    def isClosed(self):
        return self._is_closed

    def postSoapEnvelopeTo(self, path,
                           soapEnvelopeRequest,
                           responseFactory=soapenvelope.ReceivedSoap12Envelope.fromXMLString,
                           msg='',
                           request_manipulator=None):
        '''
        @param path: url path component
        @param soapEnvelopeRequest: The soap envelope that shall be sent
        @param responseFactory: a callable that creates a response object from received xml. If None, a ReceivedSoap12Envelope will be created
        @param schema: If given, the request is validated against this schema
        @param msg: used in logs, helps to identify the context in which the method was called
        '''
        if self.isClosed():
            raise httplib.NotConnected('call connect before posting!')
        return self.__postSoapEnvelope(soapEnvelopeRequest, responseFactory, path, msg, request_manipulator)

    def __postSoapEnvelope(self, soapEnvelopeRequest, responseFactory, path, msg, request_manipulator):
        if self._xml_validator is not None:
            soapEnvelopeRequest.validate_envelope(self._xml_validator)
        if hasattr(request_manipulator, 'manipulate_soapenvelope'):
            tmp = request_manipulator.manipulate_soapenvelope(soapEnvelopeRequest)
            if tmp:
                soapEnvelopeRequest = tmp
        normalized_xml_request = soapEnvelopeRequest.as_xml(request_manipulator=request_manipulator)
        xml_request = self._sdc_definitions.denormalizeXMLText(normalized_xml_request)

        assert (b'utf-8' in xml_request[:100].lower())  # MDPWS:R0007 A text SOAP envelope shall be serialized using utf-8 character encoding
        if hasattr(request_manipulator, 'manipulate_string'):
            tmp = request_manipulator.manipulate_string(xml_request)
            if tmp:
                xml_request = tmp

        started = time.perf_counter()
        try:
            xml_response = self._sendSoapRequest(path, xml_request, msg)
        finally:
            self.roundtrip_time = time.perf_counter() - started # set roundtrip time even if method raises an exception
        normalized_xml_response = self._sdc_definitions.normalizeXMLText(xml_response)

        if responseFactory is not None:
            try:
                response_envelope = responseFactory(normalized_xml_response)
            except etree_.XMLSyntaxError as ex:
                self._log.error('{} XMLSyntaxError in string: "{}"', msg, normalized_xml_response)
                raise RuntimeError('{} in "{}"'.format(ex, normalized_xml_response))
            if self._xml_validator is not None:
                response_envelope.validate_envelope(self._xml_validator)
            return response_envelope
        return None

    def validate_envelope(self, envelope):
        if self._xml_validator is None:
            return
        root = envelope.buildDoc()
        doc = etree_.ElementTree(element=root)
        self._xml_validator.assertValid(doc)

    def _sendSoapRequest(self, path, xml, msg):
        """Send SOAP request using HTTP"""
        if not isinstance(xml, bytes):
            xml = xml.encode('utf-8')

        headers = {
            'Content-type': 'application/soap+xml; charset=utf-8',
            'user_agent': 'pysoap',
            'Connection': 'keep-alive',
        }
        commlog.defaultLogger.logSoapReqOut(xml, 'POST')

        if self.supportedEncodings:
            headers['Accept-Encoding'] = ','.join(self.supportedEncodings)
        if self.requestEncodings:
            for compr in self.requestEncodings:
                if compr in self.supportedEncodings:
                    xml = self.compressPayload(compr, xml)
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
                self._httpConnection.request('POST', path, body=xml, headers=headers)
                return True, do_reopen # success = True
            except httplib.CannotSendRequest as ex:
                # for whatever reason the response of the previous call was not read. read it and try again
                self._log.warn("{}: could not send request, got httplib.CannotSendRequest Error. Will read response and retry", msg)
                tmp = self._httpConnection.getresponse()
                tmp.read()
            except OSError as ex:
                if ex.errno in (10053, 10054):
                    self._log.warn("{}: could not send request, OSError={!r}", msg, ex)
                else:
                    self._log.warn("{}: could not send request, OSError={}", msg, traceback.format_exc())
                do_reopen = True
            except socket.error as ex:
                self._log.warn("{}: could not send request, socket error={!r}", msg, ex)
                do_reopen = True
            except Exception as ex:
                self._log.warn("{}: POST to netloc='{}' path='{}': could not send request, error={!r}\n{}", msg,
                               self._netloc, path, ex, traceback.format_exc())
            return success, do_reopen

        def get_response():
            try:
                return self._httpConnection.getresponse()
            except OSError as ex:
                if ex.errno in (10053, 10054):
                    self._log.warn("{}: could not receive response, OSError={!r}", msg, ex)
                else:
                    self._log.warn("{}: could not receive response, OSError={} ({!r})\n{}", msg, ex.errno,
                                   ex, traceback.format_exc())
                raise httplib.NotConnected()
            except socket.error as ex:
                self._log.warn("{}: could not receive response, socket error={!r}", msg, ex)
                raise httplib.NotConnected()
            except httplib.BadStatusLine as ex:
                self._log.warn("{}: invalid http response, error= {!r} ", msg, ex)
                raise
            except Exception as ex:
                self._log.warn("{}: POST to netloc='{}' path='{}': could not receive response, error={!r}\n{}",
                               msg, self._netloc, path, ex, traceback.format_exc())
                raise httplib.NotConnected()

        def reopen_http_connection():
            self._log.info("{}: will close and reopen the connection and then try again", msg)
            self._httpConnection.close()
            try:
                self._httpConnection.connect()
            except Exception as ex:
                self._log.error("{}: could not reopen the connection, error={!r}\n{}\ncall-stack ={}",
                                msg, ex, traceback.format_exc(), ''.join(traceback.format_stack()))
                self._httpConnection.close()
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
                soapfault = soapenvelope.ReceivedSoapFault.fromXMLString(content)

                raise HTTPReturnCodeError(response.status, content, soapfault)

            responseHeaders = {k.lower(): v for k, v in response.getheaders()}

            self._log.debug('{}: response:{}; content has {} Bytes ', msg, responseHeaders, len(content))
            commlog.defaultLogger.logSoapRespIn(content, 'POST')
            return content

    def _makeGetHeaders(self):
        self._getHeaders = {
            'user_agent': 'pysoap',
            'Connection': 'keep-alive'
        }
        if sys.version < '3':
            # Ensure http_method, location and all headers are binary to prevent
            # UnicodeError inside httplib.HTTPConnection._send_output.

            # httplib in python3 do the same inside itself, don't need to convert it here
            self._getHeaders = dict((str(k), str(v)) for k, v in self._getHeaders.items())

        if self.supportedEncodings:
            self._getHeaders['Accept-Encoding'] = ', '.join(self.supportedEncodings)

    def getUrl(self, url, msg):
        if not url.startswith('/'):
            url = '/' + url
        self._log.debug("{} Get {}/{}", msg, self._netloc, url)
        with self._lock:
            self._httpConnection.request('GET', url, headers=self._getHeaders)
            response = self._httpConnection.getresponse()
            headers = {k.lower(): v for k, v in response.getheaders()}
            _content = response.read()
            if 'content-encoding' in headers:
                enc = headers['content-encoding']
                if enc in self.supportedEncodings:
                    content = self.decompress(_content, enc)
                else:
                    self._log.warn("{}: unsupported compression ", headers['content-encoding'])
                    raise httplib.UnknownTransferEncoding
            else:
                content = _content
        return content
