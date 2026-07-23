"""Implementation of the SDC ContainmentTreeService port type."""

from __future__ import annotations

from typing import TYPE_CHECKING, NoReturn

from sdc11073.dispatch import DispatchKey
from sdc11073.exceptions import FunctionNotImplementedError
from sdc11073.namespaces import PrefixesEnum
from sdc11073.provider.porttypes.porttypebase import (
    DPWSPortTypeBase,
    WSDLMessageDescription,
    WSDLOperationBinding,
    mk_wsdl_two_way_operation,
    msg_prefix,
)
from sdc11073.pysoap.soapenvelope import Fault, faultcodeEnum

if TYPE_CHECKING:
    from sdc11073 import xml_utils
    from sdc11073.dispatch.request import RequestData
    from sdc11073.provider.dpwshostedservice import DPWSHostedService


class ContainmentTreeService(DPWSPortTypeBase):
    """Provider port type for the SDC ContainmentTreeService."""

    port_type_name = PrefixesEnum.SDC.tag('ContainmentTreeService')
    WSDLMessageDescriptions = (
        WSDLMessageDescription('GetDescriptor', (f'{msg_prefix}:GetDescriptor',)),
        WSDLMessageDescription('GetDescriptorResponse', (f'{msg_prefix}:GetDescriptorResponse',)),
        WSDLMessageDescription('GetContainmentTree', (f'{msg_prefix}:GetContainmentTreeResponse',)),
        WSDLMessageDescription('GetContainmentTreeResponse', (f'{msg_prefix}:GetContainmentTreeResponse',)),
    )
    WSDLOperationBindings = (
        WSDLOperationBinding('GetDescriptor', 'literal', 'literal'),
        WSDLOperationBinding('GetContainmentTree', 'literal', 'literal'),
    )

    def register_hosting_service(self, hosting_service: DPWSHostedService):
        """Register the GetContainmentTree and GetDescriptor post handlers in the hosting service."""
        super().register_hosting_service(hosting_service)
        actions = self._mdib.sdc_definitions.Actions
        msg_names = self._mdib.sdc_definitions.data_model.msg_names
        hosting_service.register_post_handler(
            DispatchKey(actions.GetContainmentTree, msg_names.GetContainmentTree), self._on_get_containment_tree
        )
        hosting_service.register_post_handler(
            DispatchKey(actions.GetDescriptor, msg_names.GetDescriptor), self._on_get_descriptor
        )

    def _on_get_containment_tree(self, request_data: RequestData) -> NoReturn:  # noqa: ARG002
        """Handle a GetContainmentTree request by raising a not-implemented SOAP fault."""
        # TODO(#498): implement, currently method only raises a soap fault  # noqa: FIX002
        fault = Fault()
        fault.Code.Value = faultcodeEnum.RECEIVER
        fault.add_reason_text('not implemented')

        raise FunctionNotImplementedError(fault)

    def _on_get_descriptor(self, request_data: RequestData) -> NoReturn:  # noqa: ARG002
        """Handle a GetDescriptor request by raising a not-implemented SOAP fault."""
        # TODO(#499): implement, currently method only raises a soap fault  # noqa: FIX002
        fault = Fault()
        fault.Code.Value = faultcodeEnum.RECEIVER
        fault.add_reason_text('not implemented')
        raise FunctionNotImplementedError(fault)

    def add_wsdl_port_type(self, parent_node: xml_utils.LxmlElement) -> None:
        """Add the ContainmentTreeService wsdl:portType node with its operations to parent_node."""
        port_type = self._mk_port_type_node(parent_node)
        mk_wsdl_two_way_operation(port_type, operation_name='GetDescriptor')
        mk_wsdl_two_way_operation(port_type, operation_name='GetContainmentTree')
