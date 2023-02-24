from concurrent.futures import Future

from .serviceclientbase import HostedServiceClient


class SetServiceClient(HostedServiceClient):
    subscribeable_actions = ('OperationInvokedReport',)

    def set_numeric_value(self, operation_handle, requested_numeric_value, request_manipulator=None) -> Future:
        """ call SetNumericValue Method of device
        :param operation_handle: a string
        :param requested_numeric_value: decimal , int, float or a string representing a decimal number
        @return a Future object
        """
        data_model = self._sdc_definitions.data_model
        self._logger.info('set_numeric_value operation_handle={} requested_numeric_value={}',
                          operation_handle, requested_numeric_value)
        request = data_model.msg_types.SetValue()
        request.OperationHandleRef = operation_handle
        request.RequestedNumericValue = requested_numeric_value
        message = self._msg_factory.mk_soap_message(self.endpoint_reference.Address, request)
        return self._call_operation(message, request_manipulator=request_manipulator)

    def set_string(self, operation_handle, requested_string, request_manipulator=None) -> Future:
        """ call SetString Method of device
        :param operation_handle: a string
        :param requested_string: a string
        @return a Future object
        """
        data_model = self._sdc_definitions.data_model
        self._logger.info('set_string operation_handle={} requested_string={}',
                          operation_handle, requested_string)
        request = data_model.msg_types.SetString()
        request.OperationHandleRef = operation_handle
        request.RequestedStringValue = requested_string
        message = self._msg_factory.mk_soap_message(self.endpoint_reference.Address, request)
        return self._call_operation(message, request_manipulator=request_manipulator)

    def set_alert_state(self, operation_handle, proposed_alert_state, request_manipulator=None) -> Future:
        """The SetAlertState method corresponds to the SetAlertStateOperation objects in the MDIB and allows the modification of an alert.
        It can handle a single proposed AlertState as argument (only for backwards compatibility) and a list of them.
        :param operation_handle: handle name as string
        :param proposed_alert_state: domainmodel.AbstractAlertState instance or a list of them
        """
        data_model = self._sdc_definitions.data_model
        self._logger.info('set_alert_state operation_handle={} requestedAlertState={}',
                          operation_handle, proposed_alert_state)
        request = data_model.msg_types.SetAlertState()
        request.OperationHandleRef = operation_handle
        request.ProposedAlertState = proposed_alert_state
        message = self._msg_factory.mk_soap_message(self.endpoint_reference.Address, request)
        return self._call_operation(message, request_manipulator=request_manipulator)

    def set_metric_state(self, operation_handle, proposed_metric_states, request_manipulator=None) -> Future:
        """The SetMetricState method corresponds to the SetMetricStateOperation objects in the MDIB and allows the modification of metric states.
        :param operation_handle: handle name as string
        :param proposed_metric_states: a list of domainmodel.AbstractMetricState instance or derived class
        """
        data_model = self._sdc_definitions.data_model
        self._logger.info('set_metric_state operation_handle={} requestedMetricState={}',
                          operation_handle, proposed_metric_states)
        request = data_model.msg_types.SetMetricState()
        request.OperationHandleRef = operation_handle
        request.ProposedMetricState.extend(proposed_metric_states)
        message = self._msg_factory.mk_soap_message(self.endpoint_reference.Address, request)
        return self._call_operation(message, request_manipulator=request_manipulator)

    def activate(self, operation_handle, arguments=None, request_manipulator=None) -> Future:
        """ an activate call does not return the result of the operation directly. Instead you get an transaction id,
        and will receive the status of this transaction as notification ("OperationInvokedReport").
        This method returns a "future" object. The future object has a result as soon as a final transaction state is received.
        :param operation_handle: a string
        :param arguments: a list of strings or None
        :return: a concurrent.futures.Future object
        """
        data_model = self._sdc_definitions.data_model
        self._logger.info('activate handle={} arguments={}', operation_handle, arguments)
        request = data_model.msg_types.Activate()
        request.OperationHandleRef = operation_handle
        if arguments is not None:
            for arg_value in arguments:
                request.add_argument(arg_value)
        message = self._msg_factory.mk_soap_message(self.endpoint_reference.Address, request)
        return self._call_operation(message, request_manipulator=request_manipulator)

    def set_component_state(self, operation_handle, proposed_component_states, request_manipulator=None) -> Future:
        """
        The set_component_state method corresponds to the SetComponentStateOperation objects in the MDIB and allows to insert or modify context states.
        :param operation_handle: handle name as string
        :param proposed_component_states: a list of domainmodel.AbstractDeviceComponentState instances or derived class
        :return: a concurrent.futures.Future
        """
        data_model = self._sdc_definitions.data_model
        tmp = ', '.join([f'{st.__class__.__name__} (DescriptorHandle={st.DescriptorHandle})'
                         for st in proposed_component_states])
        self._logger.info('set_component_state {}', tmp)
        request = data_model.msg_types.SetComponentState()
        request.OperationHandleRef = operation_handle
        request.ProposedComponentState.extend(proposed_component_states)
        message = self._msg_factory.mk_soap_message(self.endpoint_reference.Address, request)
        self._logger.debug('set_component_state sends {}', lambda: message.serialize_message(pretty=True))
        return self._call_operation(message, request_manipulator=request_manipulator)
