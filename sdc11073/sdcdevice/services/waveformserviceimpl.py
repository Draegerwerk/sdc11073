from .servicesbase import DPWSPortTypeImpl, WSDLMessageDescription, WSDLOperationBinding, _mk_wsdl_one_way_operation
from .servicesbase import msg_prefix

class WaveformService(DPWSPortTypeImpl):
    WSDLMessageDescriptions = (WSDLMessageDescription('Waveform',
                                                      (f'{msg_prefix}:WaveformStreamReport',)),)
    WSDLOperationBindings = (WSDLOperationBinding('Waveform', None, 'literal'),)

    def add_wsdl_port_type(self, parent_node):
        port_type = self._mk_port_type_node(parent_node, True)
        _mk_wsdl_one_way_operation(port_type, operation_name='Waveform')

    def _mk_offered_subscriptions(self):
        # unclear if this is needed, it seems wsdl uses Waveform name, action uses WaveformStream
        return  [self._sdc_device.mdib.sdc_definitions.Actions.Waveform]
