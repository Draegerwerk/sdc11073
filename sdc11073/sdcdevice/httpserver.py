import threading
import time
import traceback
import socket
import urllib
from http.server import HTTPServer
from .exceptions import HTTPRequestHandlingError, InvalidPathError, InvalidActionError
from .. import pysoap
from .. import commlog
from .. import loghelper
from ..httprequesthandler import HTTPRequestHandler, mkchunks


MULTITHREADED = True


class MyThreadingMixIn(object):

    def process_request_thread(self, request, client_address):
        """Same as in BaseServer but as a thread.

        In addition, exception handling is done here.

        """
        try:
            self.finish_request(request, client_address)
            self.shutdown_request(request)
        except Exception as ex:
            if self.dispatcher is not None:
                # only 
                self.handle_error(request, client_address)
            self.shutdown_request(request)


    def process_request(self, request, client_address):
        """Start a new thread to process the request."""
        t = threading.Thread(target = self.process_request_thread,
                             args = (request, client_address),
                             name='SubscrRecv{}'.format(client_address))
        t.daemon = True
        t.start()
        self.threads.append((t,request, client_address))


if MULTITHREADED:
    class MyHTTPServer(MyThreadingMixIn, HTTPServer):
        ''' Each request is handled in a thread.
        Following receipe from https://pymotw.com/2/BaseHTTPServer/index.html#module-BaseHTTPServer 
        '''
        def __init__(self, *args, **kwargs):
            HTTPServer. __init__(self, *args, **kwargs)
            self.daemon_threads = True
            self.threads = []
            self.dispatcher = None
else:
    MyHTTPServer = HTTPServer # single threaded, sequential operation

class DevicesDispatcher(object):
    """ Dispatch to one of the registered devices, based on url"""
    def __init__(self, logger):
        self._logger = logger
        self.deviceByUrl = {}       # lookup for requests

    def register_device_dispatcher(self, path, dispatcher):
        if path.startswith('/'):
            path = path[1:]
        if path.endswith('/'):
            path = path[:-1]
        if path in self.deviceByUrl:
            raise RuntimeError('Path "{}" already registered'.format(path))
        self.deviceByUrl[path] = dispatcher

    def get_device_dispatcher(self, path):
        _path = path[1:] if path.startswith('/') else path
        for url, dispatcher in self.deviceByUrl.items():
            if _path.startswith(url):
                return dispatcher
        raise HTTPRequestHandlingError(status=404, reason='not found', soapfault=b'client error')

    def on_post(self, path, headers, request):
        return self.get_device_dispatcher(path).on_post(path, headers, request)

    def on_get(self, path, headers):
        return self.get_device_dispatcher(path).on_get(path, headers)


class HostedServiceDispatcher(object):
    ''' receiver of all messages'''

    def __init__(self, sdc_definitions, logger):
        self.sdc_definitions = sdc_definitions
        self._logger = logger
        self.hostedServiceByUrl = {}       # lookup for requests
        self.hostedServices = []

    def register_hosted_service(self, hosted_service):
        path = hosted_service.epr
        if path.endswith('/'):
            path = path[:-1]
        if path in self.hostedServiceByUrl:
            raise RuntimeError('Path "{}" already registered'.format(path))
        self.hostedServiceByUrl[path] = hosted_service
        self.hostedServices.append(hosted_service)

    def on_post(self, path, headers, request):
        """Method converts the http request into a soap envelope and calls dispatchSoapRequest.
           Return of dispatchSoapRequest (soap envelope) is converted back to a string."""
        commlog.defaultLogger.logSoapReqIn(request, 'POST')
        normalizedRequest = self.sdc_definitions.normalizeXMLText(request)
        # execute the method
        soapEnvelope = pysoap.soapenvelope.ReceivedSoap12Envelope.fromXMLString(normalizedRequest)
        response = self._dispatchSoapRequest(path, headers, soapEnvelope)
        normalized_response_xml_string = response.as_xml()
        return self.sdc_definitions.denormalizeXMLText(normalized_response_xml_string)

    def _dispatchSoapRequest(self, path, header, soapEnvelope):
        # path is a string like /0105a018-8f4c-4199-9b04-aff4835fd8e9/StateEvent, without http:/servername:port
        hostedService = self.hostedServiceByUrl.get(path)
        if not hostedService:
            raise InvalidPathError(soapEnvelope, path)
        try:
            return hostedService.dispatchSoapRequest( path, header, soapEnvelope)
        except InvalidActionError as ex:
            # error: no handler for this action; log this error with all known pathes, the re-raise
            all_actions = []
            for dispatcher in self.hostedServices:
                all_actions.extend(', '.join([dispatcher.epr, k]) for k in dispatcher.getActions())

            txt = 'HostedServiceDispatcher.dispatchSoapRequest: {} , known=\n{}'.format(ex, '\n'.join(all_actions))
            self._logger.error(txt)
            raise

    def on_get(self, path, httpHeaders):
        """ Get Requests are handled as they are, no soap envelopes"""
        response_string = self._dispatchGetRequest(path, httpHeaders)
        return  self.sdc_definitions.denormalizeXMLText(response_string)

    def _dispatchGetRequest(self, path, httpHeaders):
        parsedPath = urllib.parse.urlparse(path)
        p = parsedPath.path
        if p.endswith('/'):
            p = p[:-1]
        dispatcher = self.hostedServiceByUrl.get(p)
        if dispatcher is None:
            raise KeyError(
                'HostedServiceDispatcher.dispatchGetRequest: unknown path "{}", known = {}'.format(p, self.hostedServiceByUrl.keys()))
        response_string = dispatcher.dispatchGetRequest(parsedPath, httpHeaders)
        return  self.sdc_definitions.denormalizeXMLText(response_string)


class _SdcServerRequestHandler(HTTPRequestHandler):
    protocol_version = "HTTP/1.1"  # this enables keep-alive
    # This server does NOT disable nagle algorithm. It sends Large responses,
    # and network efficiency is more important tahn short latencies.
    disable_nagle_algorithm = False

    def do_POST(self):
        """SOAP POST gateway"""
        try:
            devices_dispatcher = self.server.dispatcher
            if devices_dispatcher is None:
                # close this connection
                self.close_connection = 1
                response_xml_string = b'received a POST request, but have no dispatcher'
                self.send_response(404)  # not found
            else:
                request = self._read_request()
                commlog.defaultLogger.logSoapReqIn(request, 'POST')
                try:
                    #delegate handling to on_post method of dispatcher
                    response_xml_string = devices_dispatcher.on_post(self.path, self.headers, request)
                    http_status = 200
                    http_reason = 'Ok'
                    # MDPWS:R0007 A text SOAP envelope shall be serialized using utf-8 character encoding
                    assert (b'utf-8' in response_xml_string[:100].lower())
                except HTTPRequestHandlingError as ex:
                    response_xml_string = ex.soapfault
                    http_status = ex.status
                    http_reason = ex.reason

                commlog.defaultLogger.logSoapRespOut(response_xml_string, 'POST')
                self.send_response(http_status, http_reason)
            response_xml_string = self._compressIfRequired(response_xml_string)
            self.send_header("Content-type", "application/soap+xml; charset=utf-8")
            if self.server.chunked_response:
                self.send_header("transfer-encoding", "chunked")
                self.end_headers()
                self.wfile.write(mkchunks(response_xml_string))
            else:
                self.send_header("Content-length", len(response_xml_string))
                self.end_headers()
                self.wfile.write(response_xml_string)
        except Exception as ex:
            # make an error 500 response with the soap fault as content
            self.server.logger.error(traceback.format_exc())
            # we must create a soapEnvelope in order to generate a SoapFault
            dev_dispatcher = devices_dispatcher.get_device_dispatcher(self.path)
            normalizedRequest = dev_dispatcher.sdc_definitions.normalizeXMLText(request)
            soapEnvelope = pysoap.soapenvelope.ReceivedSoap12Envelope.fromXMLString(normalizedRequest)

            response = pysoap.soapenvelope.SoapFault(soapEnvelope, code=pysoap.soapenvelope.SoapFaultCode.SENDER,
                                                          reason=str(ex))
            normalized_response_xml_string = response.as_xml()
            response_xml_string = dev_dispatcher.sdc_definitions.denormalizeXMLText(normalized_response_xml_string)
            self.send_response(500)
            self.send_header("Content-type", "application/soap+xml; charset=utf-8")
            self.send_header("Content-length", len(response_xml_string))
            self.end_headers()
            self.wfile.write(response_xml_string)

    def do_GET(self):
        parsedPath = urllib.parse.urlparse(self.path)
        try:
            commlog.defaultLogger.logSoapReqIn('', 'GET') # GET has no content, log it to document duration of processing
            response_string = self.server.dispatcher.on_get(self.path, self.headers)
            self.send_response(200, 'Ok')
            response_string = self._compressIfRequired(response_string)
            commlog.defaultLogger.logSoapRespOut(response_string, 'GET')
            if parsedPath.query == 'wsdl':
                content_type = "text/xml; charset=utf-8"
            else:
                content_type = "application/soap+xml; charset=utf-8"
        except Exception as ex:
            self.send_response(500)
            response_string = str(ex)
            content_type = "text"

        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", len(response_string))
        self.end_headers()
        self.wfile.write(response_string)


class HttpServerThread(threading.Thread):
    
    def __init__(self, my_ipaddress, sslContext, supportedEncodings, log_prefix=None, chunked_responses=False):
        '''
        :param my_ipaddress:
        :param sslContext:
        :param supportedEncodings: a list od strings
        '''
        super(HttpServerThread, self).__init__(name='Dev_SdcHttpServerThread')
        self.daemon = True

        self._my_ipaddress = my_ipaddress
        self._sslContext = sslContext
        self.my_port = None
        self.httpd = None
        self.supportedEncodings = supportedEncodings

        self._logger = loghelper.getLoggerAdapter('sdc.device.httpsrv', log_prefix)
        self.chunked_responses = chunked_responses
        # create and set up the dispatcher for all incoming requests
        self.devices_dispatcher = DevicesDispatcher(self._logger)
        self.started_evt = threading.Event() # helps to wait until thread has initialised is variables


    def run(self):
        try:
            myport = 0  # zero means that OS selects a free port
            self.httpd = MyHTTPServer((self._my_ipaddress, myport), _SdcServerRequestHandler)
            self.httpd.chunked_response = self.chunked_responses # monkey-patching, value needed by _SdcServerRequestHandler
            # add use compression flag to the server
            setattr(self.httpd, 'supportedEncodings', self.supportedEncodings)
            self.httpd.logger = self._logger # add logger by monkey-pathing
            if self._sslContext is not None:
                self.httpd.socket = self._sslContext.wrap_socket(self.httpd.socket, do_handshake_on_connect=False)
            self.my_port = self.httpd.server_port
            self.httpd.dispatcher = self.devices_dispatcher

            self.started_evt.set()
            self.httpd.serve_forever()
        except Exception:
            self._logger.error('Unhandled Exception at thread runtime. Thread will abort! {}'.format(traceback.format_exc()))
            raise

    def setCompressionFlag(self, useCompression):
        '''Sets use compression attribute on the http server to be used in handler
        @param useCompression: bool flag 
        '''
        self.httpd.useCompression = useCompression
   
    def stop(self, closeAllConnections=True):
        self.httpd.shutdown()
        self.join(timeout=5)
        self.httpd.socket.close()
        if closeAllConnections:
            if self.httpd.dispatcher is not None:
                self.httpd.dispatcher = None # this leads to a '503' reaction in SOAPNotificationsHandler
            for thr in self.httpd.threads:
                thread, request, client_addr = thr
                if thread.is_alive():
                    try:
                        request.shutdown(socket.SHUT_RDWR)
                        request.close()
                        self._logger.info('closed socket from {}', client_addr)
                    except OSError as ex:
                        # the connection is already closed
                        continue
                    except Exception as ex:
                        self._logger.warn ('error closing socket from {}: {}', client_addr, ex)
            time.sleep(0.1)
            for thr in self.httpd.threads:
                thread, request, client_addr = thr
                if thread.is_alive():
                    thread.join(1)
                if thread.is_alive():
                    self._logger.warn('could not end client thread connected from {}', client_addr)
            del self.httpd.threads[:]
