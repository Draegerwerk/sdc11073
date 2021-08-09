import socket
import threading
import time
import traceback
import urllib
from http.server import HTTPServer

from .exceptions import HTTPRequestHandlingError, InvalidPathError, InvalidActionError
from .. import commlog
from .. import loghelper
from .. import pysoap
from ..httprequesthandler import HTTPRequestHandler, mkchunks

MULTITHREADED = True


class MyThreadingMixIn:

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
            else:
                print("don't care error:{}".format(ex))
            self.shutdown_request(request)

    def process_request(self, request, client_address):
        """Start a new thread to process the request."""
        t = threading.Thread(target=self.process_request_thread,
                             args=(request, client_address),
                             name='SubscrRecv{}'.format(client_address))
        t.daemon = True
        t.start()
        self.threads.append((t, request, client_address))


if MULTITHREADED:
    class MyHTTPServer(MyThreadingMixIn, HTTPServer):
        ''' Each request is handled in a thread.
        Following receipe from https://pymotw.com/2/BaseHTTPServer/index.html#module-BaseHTTPServer 
        '''

        def __init__(self, *args, **kwargs):
            HTTPServer.__init__(self, *args, **kwargs)
            self.daemon_threads = True
            self.threads = []
            self.dispatcher = None
else:
    MyHTTPServer = HTTPServer  # single threaded, sequential operation


class DevicesDispatcher:
    """ Dispatch to one of the registered devices, based on url"""

    def __init__(self, logger):
        self._logger = logger
        self.device_by_url = {}  # lookup for requests

    def register_device_dispatcher(self, path, dispatcher):
        if path.startswith('/'):
            path = path[1:]
        if path.endswith('/'):
            path = path[:-1]
        if path in self.device_by_url:
            raise RuntimeError('Path "{}" already registered'.format(path))
        self.device_by_url[path] = dispatcher

    def get_device_dispather(self, path):
        _path = path[1:] if path.startswith('/') else path
        for url, dispatcher in self.device_by_url.items():
            if _path.startswith(url):
                return dispatcher
        raise HTTPRequestHandlingError(status=404, reason='not found', soapfault=b'client error')

    def on_post(self, path, headers, request):
        return self.get_device_dispather(path).on_post(path, headers, request)

    def on_get(self, path, headers):
        return self.get_device_dispather(path).on_get(path, headers)


class HostedServiceDispatcher:
    ''' receiver of all messages'''

    def __init__(self, sdc_definitions, logger):
        self.sdc_definitions = sdc_definitions
        self._logger = logger
        self.hosted_service_by_url = {}  # lookup for requests
        self.hosted_services = []

    def register_hosted_service(self, hosted_service):
        path = hosted_service.epr
        if path.endswith('/'):
            path = path[:-1]
        if path in self.hosted_service_by_url:
            raise RuntimeError('Path "{}" already registered'.format(path))
        self.hosted_service_by_url[path] = hosted_service
        self.hosted_services.append(hosted_service)

    def on_post(self, path, headers, request):
        """Method converts the http request into a soap envelope and calls dispatch_soap_request.
           Return of dispatch_soap_request (soap envelope) is converted back to a string."""
        commlog.get_communication_logger().log_soap_request_in(request, 'POST')
        normalized_request = self.sdc_definitions.normalize_xml_text(request)
        # execute the method
        envelope = pysoap.soapenvelope.ReceivedSoap12Envelope.from_xml_string(normalized_request)
        response = self._dispatch_soap_request(path, headers, envelope)
        normalized_response_xml_string = response.as_xml()
        return self.sdc_definitions.denormalize_xml_text(normalized_response_xml_string)

    def _dispatch_soap_request(self, path, header, envelope):
        # path is a string like /0105a018-8f4c-4199-9b04-aff4835fd8e9/StateEvent, without http:/servername:port
        hosted_service = self.hosted_service_by_url.get(path)
        if not hosted_service:
            raise InvalidPathError(envelope, path)
        try:
            return hosted_service.dispatch_soap_request(path, header, envelope)
        except InvalidActionError as ex:
            # error: no handler for this action; log this error with all known pathes, the re-raise
            all_actions = []
            for dispatcher in self.hosted_services:
                all_actions.extend(', '.join([dispatcher.epr, k]) for k in dispatcher.get_actions())

            txt = 'HostedServiceDispatcher.dispatch_soap_request: {} , known=\n{}'.format(ex, '\n'.join(all_actions))
            self._logger.error(txt)
            raise

    def on_get(self, path, http_headers):
        """ Get Requests are handled as they are, no soap envelopes"""
        response_string = self._dispatch_get_request(path, http_headers)
        return self.sdc_definitions.denormalize_xml_text(response_string)

    def _dispatch_get_request(self, path, http_headers):
        parsed_path = urllib.parse.urlparse(path)
        _path = parsed_path.path
        if _path.endswith('/'):
            _path = _path[:-1]
        dispatcher = self.hosted_service_by_url.get(_path)
        if dispatcher is None:
            raise KeyError(
                'HostedServiceDispatcher.dispatch_get_request: unknown path "{}", known = {}'.format(_path,
                                                                                                     self.hosted_service_by_url.keys()))
        response_string = dispatcher.dispatch_get_request(parsed_path, http_headers)
        return self.sdc_definitions.denormalize_xml_text(response_string)


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
                self.close_connection = 1  # pylint: disable=attribute-defined-outside-init
                response_xml_string = b'received a POST request, but have no dispatcher'
                self.send_response(404)  # not found
            else:
                request = self._read_request()
                commlog.get_communication_logger().log_soap_request_in(request, 'POST')
                try:
                    # delegate handling to on_post method of dispatcher
                    response_xml_string = devices_dispatcher.on_post(self.path, self.headers, request)
                    http_status = 200
                    http_reason = 'Ok'
                    # MDPWS:R0007 A text SOAP envelope shall be serialized using utf-8 character encoding
                    assert (b'utf-8' in response_xml_string[:100].lower())
                except HTTPRequestHandlingError as ex:
                    response_xml_string = ex.soapfault
                    http_status = ex.status
                    http_reason = ex.reason

                commlog.get_communication_logger().log_soap_response_out(response_xml_string, 'POST')
                self.send_response(http_status, http_reason)
            response_xml_string = self._compress_if_required(response_xml_string)
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
            dev_dispatcher = devices_dispatcher.get_device_dispather(self.path)
            normalized_request = dev_dispatcher.sdc_definitions.normalize_xml_text(request)
            envelope = pysoap.soapenvelope.ReceivedSoap12Envelope.from_xml_string(normalized_request)

            response = pysoap.soapenvelope.SoapFault(envelope, code=pysoap.soapenvelope.SoapFaultCode.SENDER,
                                                     reason=str(ex))
            normalized_response_xml_string = response.as_xml()
            response_xml_string = dev_dispatcher.sdc_definitions.denormalize_xml_text(normalized_response_xml_string)
            self.send_response(500)
            self.send_header("Content-type", "application/soap+xml; charset=utf-8")
            self.send_header("Content-length", len(response_xml_string))
            self.end_headers()
            self.wfile.write(response_xml_string)

    def do_GET(self):
        parsed_path = urllib.parse.urlparse(self.path)
        try:
            commlog.get_communication_logger().log_soap_request_in('',
                                                                   'GET')  # GET has no content, log it to document duration of processing
            response_string = self.server.dispatcher.on_get(self.path, self.headers)
            self.send_response(200, 'Ok')
            response_string = self._compress_if_required(response_string)
            commlog.get_communication_logger().log_soap_response_out(response_string, 'GET')
            if parsed_path.query == 'wsdl':
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

    def __init__(self, my_ipaddress, ssl_context, supported_encodings, log_prefix=None, chunked_responses=False):
        '''
        :param my_ipaddress:
        :param ssl_context:
        :param supported_encodings: a list od strings
        '''
        super().__init__(name='Dev_SdcHttpServerThread')
        self.daemon = True

        self._my_ipaddress = my_ipaddress
        self._ssl_context = ssl_context
        self.my_port = None
        self.httpd = None
        self.supported_encodings = supported_encodings

        self._logger = loghelper.get_logger_adapter('sdc.device.httpsrv', log_prefix)
        self.chunked_responses = chunked_responses
        # create and set up the dispatcher for all incoming requests
        self.devices_dispatcher = DevicesDispatcher(self._logger)
        self.started_evt = threading.Event()  # helps to wait until thread has initialised is variables

    def run(self):
        try:
            myport = 0  # zero means that OS selects a free port
            self.httpd = MyHTTPServer((self._my_ipaddress, myport), _SdcServerRequestHandler)
            self.httpd.chunked_response = self.chunked_responses  # pylint: disable=attribute-defined-outside-init
            # add use compression flag to the server
            setattr(self.httpd, 'supported_encodings', self.supported_encodings)
            self.httpd.logger = self._logger  # pylint: disable=attribute-defined-outside-init
            if self._ssl_context is not None:
                self.httpd.socket = self._ssl_context.wrap_socket(self.httpd.socket)
            self.my_port = self.httpd.server_port
            self.httpd.dispatcher = self.devices_dispatcher

            self.started_evt.set()
            self.httpd.serve_forever()
        except Exception:
            self._logger.error(
                'Unhandled Exception at thread runtime. Thread will abort! {}'.format(traceback.format_exc()))
            raise

    def set_compression_flag(self, use_compression):
        '''Sets use compression attribute on the http server to be used in handler
        @param use_compression: bool flag
        '''
        self.httpd.use_compression = use_compression  # pylint: disable=attribute-defined-outside-init

    def stop(self, close_all_connections=True):
        self.httpd.shutdown()
        self.join(timeout=5)
        self.httpd.socket.close()
        if close_all_connections:
            if self.httpd.dispatcher is not None:
                self.httpd.dispatcher = None  # this leads to a '503' reaction in SOAPNotificationsHandler
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
                        self._logger.warn('error closing socket from {}: {}', client_addr, ex)
            time.sleep(0.1)
            for thr in self.httpd.threads:
                thread, request, client_addr = thr
                if thread.is_alive():
                    thread.join(1)
                if thread.is_alive():
                    self._logger.warn('could not end client thread connected from {}', client_addr)
            del self.httpd.threads[:]
