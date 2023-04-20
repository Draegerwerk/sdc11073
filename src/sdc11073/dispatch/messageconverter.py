from __future__ import annotations

import traceback
from typing import TYPE_CHECKING
from urllib.parse import urlparse

from .request import RequestData
from .. import commlog
from ..exceptions import HTTPRequestHandlingError
from ..pysoap.soapenvelope import Fault, faultcodeEnum
from ..xml_types.addressing_types import HeaderInformationBlock

if TYPE_CHECKING:
    from .dispatchkey import RequestHandlerProtocol


class MessageConverterMiddleware:
    """ Converts between http server message format and internal format
    http server is strings, internal is RequestData."""

    def __init__(self, msg_reader, msg_factory, logger, dispatcher: RequestHandlerProtocol):
        self._logger = logger
        self._msg_reader = msg_reader
        self._msg_factory = msg_factory
        self._dispatcher: RequestHandlerProtocol = dispatcher

    def do_post(self, headers: dict, path: str, peer_name: str, request_bytes: bytes) -> (int, str, str):
        http_status = 200
        http_reason = 'Ok'
        response_xml_string = 'not set yet'
        commlog.get_communication_logger().log_soap_request_in(request_bytes, 'POST')

        # try to read the request
        fault = None
        message_data = None
        try:
            message_data = self._msg_reader.read_received_message(request_bytes)
        except HTTPRequestHandlingError as ex:
            self._logger.warning('could not read message: {}', str(ex))
            fault = ex.soap_fault
            http_status = ex.status
            http_reason = ex.reason
        except Exception as ex:
            http_status = 500
            http_reason = 'exception'
            self._logger.warning(traceback.format_exc())
            fault = Fault()
            fault.Code.Value = faultcodeEnum.SENDER
            fault.add_reason_text(str(ex))

        if fault is not None:
            inf = HeaderInformationBlock(action=fault.action, addr_to=None)
            response = self._msg_factory.mk_soap_message(inf, payload=fault)
            response_xml_string = response.serialize()
            commlog.get_communication_logger().log_soap_response_out(response_xml_string, 'POST')
            return http_status, http_reason, response_xml_string

        # handle the request
        try:
            request_data = RequestData(headers, path, peer_name, request_bytes, message_data)
            request_data.consume_current_path_element()  # uuid is already used
            response = self._dispatcher.on_post(request_data)
            response_xml_string = response.serialize()
        except HTTPRequestHandlingError as ex:
            message_data = self._msg_reader.read_received_message(request_bytes, validate=False)
            request_data = RequestData(headers, path, peer_name, request_bytes, message_data)
            response = self._msg_factory.mk_reply_soap_message(request_data, ex.soap_fault)
            response_xml_string = response.serialize()
            http_status = ex.status
            http_reason = ex.reason
        except Exception as ex:
            # make an error 500 response with the soap fault as content
            self._logger.error(traceback.format_exc())
            message_data = self._msg_reader.read_received_message(request_bytes, validate=False)
            request_data = RequestData(headers, path, peer_name, request_bytes, message_data)
            fault = Fault()
            fault.Code.Value = faultcodeEnum.SENDER
            fault.add_reason_text(str(ex))
            response = self._msg_factory.mk_reply_soap_message(request_data, fault)
            response_xml_string = response.serialize()
            http_status = 500
            http_reason = 'exception'
        finally:
            commlog.get_communication_logger().log_soap_response_out(response_xml_string, 'POST')
            return http_status, http_reason, response_xml_string

    def do_get(self, headers: dict, path: str, peer_name: str) -> (int, str, str, str):
        parsed_path = urlparse(path)
        try:
            # GET has no content, log it to document duration of processing
            commlog.get_communication_logger().log_soap_request_in('', 'GET')
            request_data = RequestData(headers, path, peer_name)
            request_data.consume_current_path_element()  # uuid is already used
            response_string = self._dispatcher.on_get(request_data)
            commlog.get_communication_logger().log_soap_response_out(response_string, 'GET')
            if parsed_path.query == 'wsdl':
                content_type = "text/xml; charset=utf-8"
            else:
                content_type = "application/soap+xml; charset=utf-8"
            return 200, 'Ok', response_string, content_type
        except Exception as ex:
            self._logger.error(traceback.format_exc())
            response_string = str(ex).encode('utf-8')
            content_type = "text"
            return 500, 'Exception', response_string, content_type
