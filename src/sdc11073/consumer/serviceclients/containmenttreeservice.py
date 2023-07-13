from __future__ import annotations

from typing import TYPE_CHECKING

from sdc11073.namespaces import PrefixesEnum
from sdc11073.xml_types.addressing_types import HeaderInformationBlock

from .serviceclientbase import GetRequestResult, HostedServiceClient

if TYPE_CHECKING:
    from sdc11073.consumer.manipulator import RequestManipulatorProtocol
    from collections.abc import Iterable

class CTreeServiceClient(HostedServiceClient):
    """Client for ContainmentTreeService."""

    port_type_name = PrefixesEnum.SDC.tag('ContainmentTreeService')

    def get_descriptor(self, handles: Iterable[str],
                       request_manipulator: RequestManipulatorProtocol | None = None) -> GetRequestResult:
        """Send a GetDescriptor request to provider."""
        data_model = self._sdc_definitions.data_model
        request = data_model.msg_types.GetDescriptor()
        request.HandleRef.extend(handles)
        inf = HeaderInformationBlock(action=request.action, addr_to=self.endpoint_reference.Address)
        message = self._msg_factory.mk_soap_message(inf, payload=request)

        received_message_data = self.post_message(message, request_manipulator=request_manipulator)
        cls = data_model.msg_types.GetDescriptorResponse
        report = cls.from_node(received_message_data.p_msg.msg_node)
        return GetRequestResult(received_message_data, report)

    def get_containment_tree(self, handles: Iterable[str],
                             request_manipulator: RequestManipulatorProtocol | None = None) -> GetRequestResult:
        """Send a GetContainmentTree request to provider."""
        data_model = self._sdc_definitions.data_model
        request = data_model.msg_types.GetContainmentTree()
        request.HandleRef.extend(handles)
        inf = HeaderInformationBlock(action=request.action, addr_to=self.endpoint_reference.Address)
        message = self._msg_factory.mk_soap_message(inf, payload=request)

        received_message_data = self.post_message(message, request_manipulator=request_manipulator)
        cls = data_model.msg_types.GetContainmentTreeResponse
        report = cls.from_node(received_message_data.p_msg.msg_node)
        return GetRequestResult(received_message_data, report)
