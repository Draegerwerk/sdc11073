from __future__ import annotations

from typing import TYPE_CHECKING

from sdc11073.namespaces import PrefixesEnum
from sdc11073.xml_types.addressing_types import HeaderInformationBlock

from .serviceclientbase import GetRequestResult, HostedServiceClient

if TYPE_CHECKING:
    from sdc11073.consumer.manipulator import RequestManipulatorProtocol


class GetServiceClient(HostedServiceClient):
    """Client for GetService."""

    port_type_name = PrefixesEnum.SDC.tag('GetService')

    def get_mdib(self, request_manipulator: RequestManipulatorProtocol | None = None) -> GetRequestResult:
        """Send a GetMdib request."""
        data_model = self._sdc_definitions.data_model
        request = data_model.msg_types.GetMdib()
        inf = HeaderInformationBlock(action=request.action, addr_to=self.endpoint_reference.Address)
        message = self._msg_factory.mk_soap_message(inf, payload=request)
        received_message_data = self.post_message(message, request_manipulator=request_manipulator)
        result = received_message_data.msg_reader.read_get_mdib_response(received_message_data)
        return GetRequestResult(received_message_data, result)

    def get_md_description(self, requested_handles: list[str] | None = None,
                           request_manipulator: RequestManipulatorProtocol | None = None) -> GetRequestResult:
        """Send a GetMdDescription request.

        :param requested_handles: None if all states shall be requested, otherwise a list of handles
        :param request_manipulator: see documentation of RequestManipulatorProtocol
        """
        data_model = self._sdc_definitions.data_model
        request = data_model.msg_types.GetMdDescription()
        if requested_handles is not None:
            request.HandleRef.extend(requested_handles)
        inf = HeaderInformationBlock(action=request.action, addr_to=self.endpoint_reference.Address)
        message = self._msg_factory.mk_soap_message(inf, payload=request)
        received_message_data = self.post_message(message, request_manipulator=request_manipulator)
        cls = data_model.msg_types.GetMdDescriptionResponse
        report = cls.from_node(received_message_data.p_msg.msg_node)

        return GetRequestResult(received_message_data, report)

    def get_md_state(self, requested_handles: list[str] | None = None,
                     request_manipulator: RequestManipulatorProtocol | None = None) -> GetRequestResult:
        """Send a GetMdState request.

        :param requested_handles: None if all states shall be requested, otherwise a list of handles
        :param request_manipulator: see documentation of RequestManipulatorProtocol
        """
        data_model = self._sdc_definitions.data_model
        request = data_model.msg_types.GetMdState()
        if requested_handles is not None:
            request.HandleRef.extend(requested_handles)
        inf = HeaderInformationBlock(action=request.action, addr_to=self.endpoint_reference.Address)
        message = self._msg_factory.mk_soap_message(inf, payload=request)
        received_message_data = self.post_message(message, request_manipulator=request_manipulator)
        cls = data_model.msg_types.GetMdStateResponse
        report = cls.from_node(received_message_data.p_msg.msg_node)
        return GetRequestResult(received_message_data, report)
