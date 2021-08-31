import traceback
import urllib

from .exceptions import HTTPRequestHandlingError, InvalidPathError, InvalidActionError
from .. import commlog
from .. import loghelper
from .. import pysoap
from ..httprequesthandler import HTTPRequestHandler, mkchunks
from ..httprequesthandler import HttpServerThreadBase, AbstractDispatcher


class DevicesDispatcher(AbstractDispatcher):
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
        raise HTTPRequestHandlingError(status=404, reason='not found', soap_fault=b'client error')

    def on_post(self, path: str, headers, request: str) -> [str, None]:
        return self.get_device_dispather(path).on_post(path, headers, request)

    def on_get(self, path: str, headers) -> str:
        return self.get_device_dispather(path).on_get(path, headers)


class HostedServiceDispatcher(AbstractDispatcher):
    """ receiver of all messages"""

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

    def on_post(self, path: str, headers, request: str) -> [str, None]:
        """Method converts the http request into a soap envelope and calls dispatch_soap_request.
           Return of dispatch_soap_request (soap envelope) is converted back to a string."""
        commlog.get_communication_logger().log_soap_request_in(request, 'POST')
        normalized_request = self.sdc_definitions.normalize_xml_text(request)
        # execute the method
        envelope = pysoap.soapenvelope.ReceivedSoap12Envelope(normalized_request)
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

    def on_get(self, path: str, headers) -> str:
        """ Get Requests are handled as they are, no soap envelopes"""
        response_string = self._dispatch_get_request(path, headers)
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

    def do_POST(self):  # pylint: disable=invalid-name
        """SOAP POST gateway"""
        try:
            devices_dispatcher = self.server.dispatcher
            if devices_dispatcher is None:
                # close this connection
                self.close_connection = True  # pylint: disable=attribute-defined-outside-init
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
                    assert b'utf-8' in response_xml_string[:100].lower()
                except HTTPRequestHandlingError as ex:
                    response_xml_string = ex.soap_fault
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
            envelope = pysoap.soapenvelope.ReceivedSoap12Envelope(normalized_request)

            response = pysoap.soapenvelope.SoapFault(envelope, code=pysoap.soapenvelope.SoapFaultCode.SENDER,
                                                     reason=str(ex))
            normalized_response_xml_string = response.as_xml()
            response_xml_string = dev_dispatcher.sdc_definitions.denormalize_xml_text(normalized_response_xml_string)
            self.send_response(500)
            self.send_header("Content-type", "application/soap+xml; charset=utf-8")
            self.send_header("Content-length", len(response_xml_string))
            self.end_headers()
            self.wfile.write(response_xml_string)

    def do_GET(self):  # pylint: disable=invalid-name
        parsed_path = urllib.parse.urlparse(self.path)
        try:
            # GET has no content, log it to document duration of processing
            commlog.get_communication_logger().log_soap_request_in('', 'GET')
            response_string = self.server.dispatcher.on_get(self.path, self.headers)
            self.send_response(200, 'Ok')
            response_string = self._compress_if_required(response_string)
            commlog.get_communication_logger().log_soap_response_out(response_string, 'GET')
            if parsed_path.query == 'wsdl':
                content_type = "text/xml; charset=utf-8"
            else:
                content_type = "application/soap+xml; charset=utf-8"
        except Exception as ex:
            self.server.logger.error(traceback.format_exc())
            self.send_response(500)
            response_string = str(ex).encode('utf-8')
            content_type = "text"

        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", len(response_string))
        self.end_headers()
        self.wfile.write(response_string)


class DeviceHttpServerThread(HttpServerThreadBase):

    def __init__(self, my_ipaddress, ssl_context, supported_encodings, log_prefix=None, chunked_responses=False):
        """
        :param my_ipaddress:
        :param ssl_context:
        :param supported_encodings: a list od strings
        """
        logger = loghelper.get_logger_adapter('sdc.device.httpsrv', log_prefix)
        request_handler = _SdcServerRequestHandler
        dispatcher = DevicesDispatcher(logger)
        super().__init__(my_ipaddress, ssl_context, supported_encodings,
                         request_handler, dispatcher,
                         logger, chunked_responses=chunked_responses)
