from .servicesbase import DPWSPortTypeImpl, WSDLMessageDescription, WSDLOperationBinding, _mk_wsdl_one_way_operation
from .servicesbase import msg_prefix


class DescriptionEventService(DPWSPortTypeImpl):
    WSDLMessageDescriptions = (
        WSDLMessageDescription('DescriptionModificationReport',
                               (f'{msg_prefix}:DescriptionModificationReport',)),
    )
    WSDLOperationBindings = (WSDLOperationBinding('DescriptionModificationReport', None, 'literal'),
                             )

    def add_wsdl_port_type(self, parent_node):
        port_type = self._mk_port_type_node(parent_node, True)
        _mk_wsdl_one_way_operation(port_type, operation_name='DescriptionModificationReport')
