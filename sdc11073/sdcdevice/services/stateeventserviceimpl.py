from .servicesbase import DPWSPortTypeImpl, WSDLMessageDescription, WSDLOperationBinding, _mk_wsdl_one_way_operation
from .servicesbase import msg_prefix


class StateEventService(DPWSPortTypeImpl):
    WSDLMessageDescriptions = (
        WSDLMessageDescription('EpisodicAlertReport',
                               (f'{msg_prefix}:EpisodicAlertReport',)),
        WSDLMessageDescription('SystemErrorReport',
                               (f'{msg_prefix}:SystemErrorReport',)),
        WSDLMessageDescription('PeriodicAlertReport',
                               (f'{msg_prefix}:PeriodicAlertReport',)),
        WSDLMessageDescription('EpisodicComponentReport',
                               (f'{msg_prefix}:EpisodicComponentReport',)),
        WSDLMessageDescription('PeriodicOperationalStateReport',
                               (f'{msg_prefix}:PeriodicOperationalStateReport',)),
        WSDLMessageDescription('PeriodicComponentReport',
                               (f'{msg_prefix}:PeriodicComponentReport',)),
        WSDLMessageDescription('EpisodicOperationalStateReport',
                               (f'{msg_prefix}:EpisodicOperationalStateReport',)),
        WSDLMessageDescription('PeriodicMetricReport',
                               (f'{msg_prefix}:PeriodicMetricReport',)),
        WSDLMessageDescription('EpisodicMetricReport',
                               (f'{msg_prefix}:EpisodicMetricReport',)),
    )

    WSDLOperationBindings = (WSDLOperationBinding('EpisodicAlertReport', None, 'literal'),
                             WSDLOperationBinding('SystemErrorReport', None, 'literal'),
                             WSDLOperationBinding('PeriodicAlertReport', None, 'literal'),
                             WSDLOperationBinding('EpisodicComponentReport', None, 'literal'),
                             WSDLOperationBinding('PeriodicOperationalStateReport', None, 'literal'),
                             WSDLOperationBinding('PeriodicComponentReport', None, 'literal'),
                             WSDLOperationBinding('EpisodicOperationalStateReport', None, 'literal'),
                             WSDLOperationBinding('PeriodicMetricReport', None, 'literal'),
                             WSDLOperationBinding('EpisodicMetricReport', None, 'literal'),
                             )

    def add_wsdl_port_type(self, parent_node):
        port_type = self._mk_port_type_node(parent_node, True)
        _mk_wsdl_one_way_operation(port_type, operation_name='EpisodicAlertReport')
        _mk_wsdl_one_way_operation(port_type, operation_name='SystemErrorReport')
        _mk_wsdl_one_way_operation(port_type, operation_name='PeriodicAlertReport')
        _mk_wsdl_one_way_operation(port_type, operation_name='EpisodicComponentReport')
        _mk_wsdl_one_way_operation(port_type, operation_name='PeriodicOperationalStateReport')
        _mk_wsdl_one_way_operation(port_type, operation_name='PeriodicComponentReport')
        _mk_wsdl_one_way_operation(port_type, operation_name='EpisodicOperationalStateReport')
        _mk_wsdl_one_way_operation(port_type, operation_name='PeriodicMetricReport')
        _mk_wsdl_one_way_operation(port_type, operation_name='EpisodicMetricReport')
