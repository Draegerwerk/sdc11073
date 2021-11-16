from .servicesbase import DPWSPortTypeImpl, WSDLMessageDescription, WSDLOperationBinding, mk_wsdl_two_way_operation
from .servicesbase import msg_prefix
from ...pysoap.soapenvelope import SoapFault, SoapFaultCode
from ...httprequesthandler import FunctionNotImplementedError



class ContainmentTreeService(DPWSPortTypeImpl):
    WSDLMessageDescriptions = (WSDLMessageDescription('GetDescriptor',
                                                      (f'{msg_prefix}:GetDescriptor',)),
                               WSDLMessageDescription('GetDescriptorResponse',
                                                      (f'{msg_prefix}:GetDescriptorResponse',)),
                               WSDLMessageDescription('GetContainmentTree',
                                                      (f'{msg_prefix}:GetContainmentTreeResponse',)),
                               WSDLMessageDescription('GetContainmentTreeResponse',
                                                      (f'{msg_prefix}:GetContainmentTreeResponse',)),
                               )
    WSDLOperationBindings = (WSDLOperationBinding('GetDescriptor', 'literal', 'literal'),
                             WSDLOperationBinding('GetContainmentTree', 'literal', 'literal'))

    def register_handlers(self, hosting_service):
        super().register_handlers(hosting_service)
        actions = self._mdib.sdc_definitions.Actions
        hosting_service.register_post_handler(actions.GetContainmentTree, self._on_get_containment_tree)
        hosting_service.register_post_handler(actions.GetDescriptor, self._on_get_descriptor)
        hosting_service.register_post_handler('GetContainmentTree', self._on_get_containment_tree)
        hosting_service.register_post_handler('GetDescriptor', self._on_get_descriptor)

    def _on_get_containment_tree(self, request_data):  # pylint: disable=no-self-use
        # ToDo: implement, currently method only raises a soap fault
        fault = SoapFault(code=SoapFaultCode.RECEIVER, reason='not implemented', details='sorry!')
        fault_message = self._sdc_device.msg_factory.mk_fault_message(request_data.message_data, fault)
        fault_xml = fault_message.serialize_message()
        raise FunctionNotImplementedError(request_data.message_data.p_msg, fault_xml)

    def _on_get_descriptor(self, request_data):  # pylint: disable=no-self-use
        # ToDo: implement, currently method only raises a soap fault
        fault = SoapFault(code=SoapFaultCode.RECEIVER, reason='not implemented', details='sorry!')
        fault_message = self._sdc_device.msg_factory.mk_fault_message(request_data.message_data, fault)
        fault_xml = fault_message.serialize_message()
        raise FunctionNotImplementedError(request_data.message_data.p_msg, fault_xml)

    def add_wsdl_port_type(self, parent_node):
        port_type = self._mk_port_type_node(parent_node)
        mk_wsdl_two_way_operation(port_type, operation_name='GetDescriptor')
        mk_wsdl_two_way_operation(port_type, operation_name='GetContainmentTree')
