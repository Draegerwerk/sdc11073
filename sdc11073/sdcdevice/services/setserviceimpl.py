from .servicesbase import ServiceWithOperations, WSDLMessageDescription, WSDLOperationBinding
from .servicesbase import mk_wsdl_two_way_operation, _mk_wsdl_one_way_operation, msg_prefix


class SetService(ServiceWithOperations):
    WSDLMessageDescriptions = (WSDLMessageDescription('Activate',
                                                      (f'{msg_prefix}:Activate',)),
                               WSDLMessageDescription('ActivateResponse',
                                                      (f'{msg_prefix}:ActivateResponse',)),
                               WSDLMessageDescription('SetString',
                                                      (f'{msg_prefix}:SetString',)),
                               WSDLMessageDescription('SetStringResponse',
                                                      (f'{msg_prefix}:SetStringResponse',)),
                               WSDLMessageDescription('SetComponentState',
                                                      (f'{msg_prefix}:SetComponentState',)),
                               WSDLMessageDescription('SetComponentStateResponse',
                                                      (f'{msg_prefix}:SetComponentStateResponse',)),
                               WSDLMessageDescription('SetAlertState',
                                                      (f'{msg_prefix}:SetAlertState',)),
                               WSDLMessageDescription('SetAlertStateResponse',
                                                      (f'{msg_prefix}:SetAlertStateResponse',)),
                               WSDLMessageDescription('SetMetricState',
                                                      (f'{msg_prefix}:SetMetricState',)),
                               WSDLMessageDescription('SetMetricStateResponse',
                                                      (f'{msg_prefix}:SetMetricStateResponse',)),
                               WSDLMessageDescription('SetValue',
                                                      (f'{msg_prefix}:SetValue',)),
                               WSDLMessageDescription('SetValueResponse',
                                                      (f'{msg_prefix}:SetValueResponse',)),
                               WSDLMessageDescription('OperationInvokedReport',
                                                      (f'{msg_prefix}:OperationInvokedReport',)),
                               )
    WSDLOperationBindings = (WSDLOperationBinding('Activate', 'literal', 'literal'),  # fault?
                             WSDLOperationBinding('SetString', 'literal', 'literal'),  # fault?
                             WSDLOperationBinding('SetComponentState', 'literal', 'literal'),  # fault?
                             WSDLOperationBinding('SetAlertState', 'literal', 'literal'),  # fault?
                             WSDLOperationBinding('SetMetricState', 'literal', 'literal'),  # fault?
                             WSDLOperationBinding('SetValue', 'literal', 'literal'),  # fault?
                             WSDLOperationBinding('OperationInvokedReport', None, 'literal'),
                             )

    def register_handlers(self, hosting_service):
        super().register_handlers(hosting_service)
        actions = self._mdib.sdc_definitions.Actions
        hosting_service.register_post_handler(actions.Activate, self._on_activate)
        hosting_service.register_post_handler(actions.SetValue, self._on_set_value)
        hosting_service.register_post_handler(actions.SetString, self._on_set_string)
        hosting_service.register_post_handler(actions.SetMetricState, self._on_set_metric_state)
        hosting_service.register_post_handler(actions.SetAlertState, self._on_set_alert_state)
        hosting_service.register_post_handler(actions.SetComponentState, self._on_set_component_state)
        hosting_service.register_post_handler('Activate', self._on_activate)
        hosting_service.register_post_handler('SetValue', self._on_set_value)
        hosting_service.register_post_handler('SetString', self._on_set_string)
        hosting_service.register_post_handler('SetMetricState', self._on_set_metric_state)
        hosting_service.register_post_handler('SetAlertState', self._on_set_alert_state)
        hosting_service.register_post_handler('SetComponentState', self._on_set_component_state)

    def _on_activate(self, request_data):  # pylint:disable=unused-argument
        """Handler for Active calls.
        It enques an operation and generates the expected operation invoked report. """
        operation_request = self._sdc_device.msg_reader.read_activate_request(request_data.message_data)
        operation_descriptor = self._mdib.descriptions.handle.get_one(operation_request.operation_handle)
        # convert arguments to python types; need operation descriptor for this.
        operation_request = self._sdc_device.msg_reader.convert_activate_arguments(operation_descriptor,
                                                                                   operation_request)
        return self._handle_operation_request(request_data.message_data, 'ActivateResponse',
                                              operation_request)

    def _on_set_value(self, request_data):  # pylint:disable=unused-argument
        """Handler for SetValue calls.
        It enqueues an operation and generates the expected operation invoked report. """
        self._logger.debug('_on_set_value')
        operation_request = self._sdc_device.msg_reader.read_set_value_request(request_data.message_data)
        ret = self._handle_operation_request(request_data.message_data, 'SetValueResponse', operation_request)
        return ret

    def _on_set_string(self, request_data):  # pylint:disable=unused-argument
        """Handler for SetString calls.
        It enqueues an operation and generates the expected operation invoked report."""
        self._logger.debug('_on_set_string')
        operation_request = self._sdc_device.msg_reader.read_set_string_request(request_data.message_data)
        return self._handle_operation_request(request_data.message_data, 'SetStringResponse',
                                              operation_request)

    def _on_set_metric_state(self, request_data):  # pylint:disable=unused-argument
        """Handler for SetMetricState calls.
        It enqueues an operation and generates the expected operation invoked report."""
        self._logger.debug('_on_set_metric_state')
        operation_request = self._sdc_device.msg_reader.read_set_metric_state_request(request_data.message_data)
        return self._handle_operation_request(request_data.message_data,
                                              'SetMetricStateResponse',
                                              operation_request)

    def _on_set_alert_state(self, request_data):  # pylint:disable=unused-argument
        """Handler for SetMetricState calls.
        It enqueues an operation and generates the expected operation invoked report."""
        self._logger.debug('_on_set_alert_state')
        operation_request = self._sdc_device.msg_reader.read_set_alert_state_request(request_data.message_data)
        return self._handle_operation_request(request_data.message_data,
                                              'SetAlertStateResponse',
                                              operation_request)

    def _on_set_component_state(self, request_data):  # pylint:disable=unused-argument
        """Handler for SetComponentState calls.
        It enqueues an operation and generates the expected operation invoked report."""
        self._logger.debug('_on_set_component_state')
        operation_request = self._sdc_device.msg_reader.read_set_component_state_request(request_data.message_data)
        return self._handle_operation_request(request_data.message_data,
                                              'SetComponentStateResponse',
                                              operation_request)

    def add_wsdl_port_type(self, parent_node):
        port_type = self._mk_port_type_node(parent_node, True)
        mk_wsdl_two_way_operation(port_type, operation_name='Activate')
        mk_wsdl_two_way_operation(port_type, operation_name='SetString')
        mk_wsdl_two_way_operation(port_type, operation_name='SetComponentState')
        mk_wsdl_two_way_operation(port_type, operation_name='SetAlertState')
        mk_wsdl_two_way_operation(port_type, operation_name='SetMetricState')
        mk_wsdl_two_way_operation(port_type, operation_name='SetValue')
        _mk_wsdl_one_way_operation(port_type, operation_name='OperationInvokedReport')
