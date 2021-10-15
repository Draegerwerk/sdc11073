import traceback
import urllib

from .exceptions import HTTPRequestHandlingError, InvalidPathError, InvalidActionError
from .. import commlog
from .. import loghelper
from .. import pysoap
from ..httprequesthandler import HTTPRequestHandler, mkchunks
from ..httprequesthandler import HttpServerThreadBase
from ..httprequesthandler import RequestData


class PathElementDispatcher:
    """ Dispatch to one of the registered sub_dispatchers, based on path element"""

    def __init__(self, logger):
        self._logger = logger
        self.sub_dispatchers = {}  # lookup for requests

    def register_dispatcher(self, path_element, dispatcher):
        if path_element in self.sub_dispatchers:
            raise RuntimeError('Path-element "{}" already registered'.format(path_element))
        self.sub_dispatchers[path_element] = dispatcher

    def get_dispatcher(self, path_element):
        dispatcher = self.sub_dispatchers.get(path_element)
        if dispatcher is None:
            raise HTTPRequestHandlingError(status=404, reason='not found', soap_fault=b'client error')
        return dispatcher

    def on_post(self, request_data) -> [str, None]:
        dispatcher = self.get_dispatcher(request_data.consume_current_path_element())
        return dispatcher.on_post(request_data)

    def on_get(self, request_data) -> str:
        dispatcher = self.get_dispatcher(request_data.consume_current_path_element())
        return dispatcher.on_get(request_data)


class HostedServiceDispatcher(PathElementDispatcher):
    """ receiver of all messages"""

    def __init__(self, msg_reader, logger):
        super().__init__(logger)
        self._msg_reader = msg_reader
        self.sdc_definitions = msg_reader.sdc_definitions
        self._hosted_services = []

    def register_hosted_service(self, hosted_service):
        path_element = hosted_service.path_element
        self.register_dispatcher(path_element, hosted_service)
        self._hosted_services.append(hosted_service)

    def on_post(self, request_data) -> [str, None]:
        """Method converts the http request into a soap envelope and calls on_post.
           Returned soap envelope is converted back to a string."""
        commlog.get_communication_logger().log_soap_request_in(request_data.request, 'POST')
        request_data.message_data = self._msg_reader.read_received_message(request_data.request)
        response_envelope = self._dispatch_post_request(request_data)
        normalized_response_xml_string = response_envelope.as_xml()
        return self.sdc_definitions.denormalize_xml_text(normalized_response_xml_string)

    def _dispatch_post_request(self, request_data):
        hosted_service = self.get_dispatcher(request_data.consume_current_path_element())
        if not hosted_service:
            raise InvalidPathError(request_data.request_data.envelope, request_data.consumed_path_elements)
        try:
            return hosted_service.on_post(request_data)
        except InvalidActionError as ex:
            # error: no handler for this action; log this error with all known pathes, the re-raise
            all_actions = []
            for dispatcher in self._hosted_services:
                all_actions.extend(', '.join([dispatcher.path_element or '', str(k)]) for k in dispatcher.get_keys())

            txt = 'HostedServiceDispatcher.on_post: {} , known=\n{}'.format(ex, '\n'.join(all_actions))
            self._logger.error(txt)
            raise

    def on_get(self, request_data) -> str:
        """ Get Requests are handled as they are, no soap envelopes"""
        response_string = self._dispatch_get_request(request_data)
        return self.sdc_definitions.denormalize_xml_text(response_string)

    def _dispatch_get_request(self, request_data):
        dispatcher = self.get_dispatcher(request_data.consume_current_path_element())
        if dispatcher is None:
            raise KeyError(
                'HostedServiceDispatcher.on_get: unknown path "{}", known = {}'.format(
                    request_data.consumed_path_elements, self.sub_dispatchers.keys()))
        response_string = dispatcher.on_get(request_data)
        return self.sdc_definitions.denormalize_xml_text(response_string)


class _SdcServerRequestHandler(HTTPRequestHandler):
    protocol_version = "HTTP/1.1"  # this enables keep-alive
    # This server does NOT disable nagle algorithm. It sends Large responses,
    # and network efficiency is more important tahn short latencies.
    disable_nagle_algorithm = False

    def do_POST(self):  # pylint: disable=invalid-name
        """SOAP POST gateway"""
        request = self._read_request()
        request_data = RequestData(self.headers, self.path, self.connection.getpeername(), request)
        try:
            devices_dispatcher = self.server.dispatcher
            if devices_dispatcher is None:
                # close this connection
                self.close_connection = True  # pylint: disable=attribute-defined-outside-init
                response_xml_string = b'received a POST request, but have no dispatcher'
                self.send_response(404)  # not found
            else:
                commlog.get_communication_logger().log_soap_request_in(request, 'POST')
                try:
                    # delegate handling to on_post method of dispatcher
                    response_xml_string = devices_dispatcher.on_post(request_data)
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
            dev_dispatcher = devices_dispatcher.get_dispatcher(request_data.current)
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
        request_data = RequestData(self.headers, self.path, self.connection.getpeername())
        try:
            # GET has no content, log it to document duration of processing
            commlog.get_communication_logger().log_soap_request_in('', 'GET')
            response_string = self.server.dispatcher.on_get(request_data)
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
        dispatcher = PathElementDispatcher(logger)
        super().__init__(my_ipaddress, ssl_context, supported_encodings,
                         request_handler, dispatcher,
                         logger, chunked_responses=chunked_responses)
