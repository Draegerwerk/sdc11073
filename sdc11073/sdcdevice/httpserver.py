from typing import Any, Union
from urllib.parse import urlparse

from .. import loghelper
from ..exceptions import ApiUsageError
from ..httprequesthandler import HTTPRequestHandler, mk_chunks
from ..httprequesthandler import HTTPRequestHandlingError
from ..httprequesthandler import HttpServerThreadBase
from ..pysoap.soapenvelope import SoapFault, FaultCodeEnum


class PathElementRegistry:
    """ Dispatch to one of the registered sub_dispatchers, based on path element"""

    def __init__(self):
        self._instances = {}

    def register_instance(self, path_element: Union[str, None], instance: Any):
        if path_element in self._instances:
            raise ApiUsageError(f'Path-element "{path_element}" already registered')
        self._instances[path_element] = instance

    def get_instance(self, path_element: Union[str, None]) -> Any:
        instance = self._instances.get(path_element)
        if instance is None:
            soap_fault = SoapFault(code=FaultCodeEnum.SENDER, reason=f'invalid path {path_element}')
            raise HTTPRequestHandlingError(status=404, reason='not found', soap_fault=soap_fault)
        return instance


class _SdcServerRequestHandler(HTTPRequestHandler):
    protocol_version = "HTTP/1.1"  # this enables keep-alive
    # This server does NOT disable nagle algorithm. It sends Large responses,
    # and network efficiency is more important than short latencies.
    disable_nagle_algorithm = False

    def get_first_path_element(self):
        parsed_path = urlparse(self.path)
        path_elements = parsed_path.path.split('/')
        if len(path_elements[0]) > 0:
            return path_elements[0]
        return path_elements[1]

    def do_POST(self):  # pylint: disable=invalid-name
        request_bytes = self._read_request()
        devices_dispatcher = self.server.dispatcher
        if devices_dispatcher is None:
            # close this connection
            self.close_connection = True  # pylint: disable=attribute-defined-outside-init
            response_xml_string = 'received a POST request, but have no dispatcher'
            self.send_response(404, response_xml_string)  # not found
            return

        device_dispatcher = devices_dispatcher.get_instance(self.get_first_path_element())
        peer_name = self.connection.getpeername()
        result = device_dispatcher.do_post(self.headers, self.path, peer_name, request_bytes)
        http_status, http_reason, response_xml_string = result

        self.send_response(http_status, http_reason)
        response_xml_string = self._compress_if_supported(response_xml_string)
        self.send_header("Content-type", "application/soap+xml; charset=utf-8")
        if self.server.chunked_response:
            self.send_header("transfer-encoding", "chunked")
            self.end_headers()
            self.wfile.write(mk_chunks(response_xml_string))
        else:
            self.send_header("Content-length", str(len(response_xml_string)))
            self.end_headers()
            self.wfile.write(response_xml_string)

    def do_GET(self):  # pylint: disable=invalid-name
        parsed_path = urlparse(self.path)
        devices_dispatcher = self.server.dispatcher
        if devices_dispatcher is None:
            # close this connection
            self.close_connection = True  # pylint: disable=attribute-defined-outside-init
            response_xml_string = 'received a POST request, but have no dispatcher'
            self.send_response(404, response_xml_string)  # not found
            return

        device_dispatcher = devices_dispatcher.get_instance(self.get_first_path_element())

        peer_name = self.connection.getpeername()
        result = device_dispatcher.do_get(self.headers, self.path, peer_name)
        http_status, http_reason, response_xml_string, content_type = result

        self.send_response(http_status, http_reason)
        response_xml_string = self._compress_if_supported(response_xml_string)
        self.send_header("Content-type", content_type)
        if self.server.chunked_response:
            self.send_header("transfer-encoding", "chunked")
            self.end_headers()
            self.wfile.write(mk_chunks(response_xml_string))
        else:
            self.send_header("Content-length", str(len(response_xml_string)))
            self.end_headers()
            self.wfile.write(response_xml_string)


class DeviceHttpServerThread(HttpServerThreadBase):

    def __init__(self, my_ipaddress, ssl_context, supported_encodings,
                 msg_reader, msg_factory, log_prefix=None, chunked_responses=False):
        """
        :param my_ipaddress:
        :param ssl_context:
        :param supported_encodings: a list od strings
        """
        logger = loghelper.get_logger_adapter('sdc.device.httpsrv', log_prefix)
        request_handler = _SdcServerRequestHandler
        dispatcher = PathElementRegistry()
        super().__init__(my_ipaddress, ssl_context, supported_encodings,
                         request_handler, dispatcher, msg_reader, msg_factory,
                         logger, chunked_responses=chunked_responses)
