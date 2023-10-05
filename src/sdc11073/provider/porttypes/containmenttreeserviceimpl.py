from .porttypebase import DPWSPortTypeBase, WSDLMessageDescription, WSDLOperationBinding, mk_wsdl_two_way_operation
from .porttypebase import msg_prefix
from sdc11073.dispatch import DispatchKey
from sdc11073.exceptions import FunctionNotImplementedError
from sdc11073.namespaces import PrefixesEnum
from sdc11073.pysoap.soapenvelope import Fault, faultcodeEnum


class ContainmentTreeService(DPWSPortTypeBase):
    port_type_name = PrefixesEnum.SDC.tag('ContainmentTreeService')
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

    def register_hosting_service(self, hosting_service):
        super().register_hosting_service(hosting_service)
        actions = self._mdib.sdc_definitions.Actions
        msg_names = self._mdib.sdc_definitions.data_model.msg_names
        hosting_service.register_post_handler(DispatchKey(actions.GetContainmentTree, msg_names.GetContainmentTree),
                                              self._on_get_containment_tree)
        hosting_service.register_post_handler(DispatchKey(actions.GetDescriptor, msg_names.GetDescriptor),
                                              self._on_get_descriptor)

    def _on_get_containment_tree(self, request_data):  # pylint: disable=no-self-use
        # ToDo: implement, currently method only raises a soap fault
        fault = Fault()
        fault.Code.Value = faultcodeEnum.RECEIVER
        fault.add_reason_text('not implemented')

        raise FunctionNotImplementedError(fault)

    def _on_get_descriptor(self, request_data):  # pylint: disable=no-self-use
        # ToDo: implement, currently method only raises a soap fault
        fault = Fault()
        fault.Code.Value = faultcodeEnum.RECEIVER
        fault.add_reason_text('not implemented')
        raise FunctionNotImplementedError(fault)

    def add_wsdl_port_type(self, parent_node):
        port_type = self._mk_port_type_node(parent_node)
        mk_wsdl_two_way_operation(port_type, operation_name='GetDescriptor')
        mk_wsdl_two_way_operation(port_type, operation_name='GetContainmentTree')
