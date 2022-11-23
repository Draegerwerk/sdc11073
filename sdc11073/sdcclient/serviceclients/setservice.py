from concurrent.futures import Future

from .serviceclientbase import HostedServiceClient


class SetServiceClient(HostedServiceClient):
    subscribeable_actions = ('OperationInvokedReport',)

    def set_numeric_value(self, operation_handle, requested_numeric_value, request_manipulator=None) -> Future:
        """ call SetNumericValue Method of device
        :param operation_handle: a string
        :param requested_numeric_value: int or float or a string representing a decimal number
        @return a Future object
        """
        self._logger.info('set_numeric_value operation_handle={} requested_numeric_value={}',
                          operation_handle, requested_numeric_value)
        message = self._mk_set_numeric_value_message(operation_handle, requested_numeric_value)
        return self._call_operation(message, request_manipulator=request_manipulator)

    def set_string(self, operation_handle, requested_string, request_manipulator=None) -> Future:
        """ call SetString Method of device
        :param operation_handle: a string
        :param requested_string: a string
        @return a Future object
        """
        self._logger.info('set_string operation_handle={} requested_string={}',
                          operation_handle, requested_string)
        envelope = self._mk_set_string_message(operation_handle, requested_string)
        return self._call_operation(envelope, request_manipulator=request_manipulator)

    def set_alert_state(self, operation_handle, proposed_alert_state, request_manipulator=None) -> Future:
        """The SetAlertState method corresponds to the SetAlertStateOperation objects in the MDIB and allows the modification of an alert.
        It can handle a single proposed AlertState as argument (only for backwards compatibility) and a list of them.
        :param operation_handle: handle name as string
        :param proposed_alert_state: domainmodel.AbstractAlertState instance or a list of them
        """
        self._logger.info('set_alert_state operation_handle={} requestedAlertState={}',
                          operation_handle, proposed_alert_state)
        if hasattr(proposed_alert_state, 'NODETYPE'):
            # this is a state container. make it a list
            proposed_alert_state = [proposed_alert_state]
        envelope = self._mk_set_alert_message(operation_handle, proposed_alert_state)
        return self._call_operation(envelope, request_manipulator=request_manipulator)

    def set_metric_state(self, operation_handle, proposed_metric_states, request_manipulator=None) -> Future:
        """The SetMetricState method corresponds to the SetMetricStateOperation objects in the MDIB and allows the modification of metric states.
        :param operation_handle: handle name as string
        :param proposed_metric_states: a list of domainmodel.AbstractMetricState instance or derived class
        """
        self._logger.info('set_metric_state operation_handle={} requestedMetricState={}',
                          operation_handle, proposed_metric_states)
        envelope = self._mk_set_metric_state_message(operation_handle, proposed_metric_states)
        return self._call_operation(envelope, request_manipulator=request_manipulator)

    def activate(self, operation_handle, arguments=None, request_manipulator=None) -> Future:
        """ an activate call does not return the result of the operation directly. Instead you get an transaction id,
        and will receive the status of this transaction as notification ("OperationInvokedReport").
        This method returns a "future" object. The future object has a result as soon as a final transaction state is received.
        :param operation_handle: a string
        :param arguments: a list of strings or None
        :return: a concurrent.futures.Future object
        """
        # make message body
        self._logger.info('activate handle={} arguments={}', operation_handle, arguments)
        message = self._msg_factory.mk_activate_message(self.endpoint_reference.address,
                                                        operation_handle,
                                                        arguments)
        return self._call_operation(message, request_manipulator=request_manipulator)

    def set_component_state(self, operation_handle, proposed_component_states, request_manipulator=None) -> Future:
        """
        The set_component_state method corresponds to the SetComponentStateOperation objects in the MDIB and allows to insert or modify context states.
        :param operation_handle: handle name as string
        :param proposed_component_states: a list of domainmodel.AbstractDeviceComponentState instances or derived class
        :return: a concurrent.futures.Future
        """
        tmp = ', '.join([f'{st.__class__.__name__} (DescriptorHandle={st.DescriptorHandle})'
                         for st in proposed_component_states])
        self._logger.info('set_component_state {}', tmp)
        message = self._msg_factory.mk_set_component_state_message(self.endpoint_reference.address,
                                                                   operation_handle, proposed_component_states)
        self._logger.debug('set_component_state sends {}', lambda: message.serialize_message(pretty=True))
        return self._call_operation(message, request_manipulator=request_manipulator)

    def _mk_set_numeric_value_message(self, operation_handle, requested_numeric_value):
        """create message, but do not send it. Used for unit testing"""
        return self._msg_factory.mk_set_value_message(
            self.endpoint_reference.address, operation_handle, requested_numeric_value)

    def _mk_set_string_message(self, operation_handle, requested_string):
        """create message, but do not send it. Used for unit testing"""
        return self._msg_factory.mk_set_string_message(
            self.endpoint_reference.address, operation_handle, requested_string)

    def _mk_set_alert_message(self, operation_handle, proposed_alert_states):
        return self._msg_factory.mk_set_alert_message(self.endpoint_reference.address, operation_handle,
                                                      proposed_alert_states)

    def _mk_set_metric_state_message(self, operation_handle, proposed_metric_states):
        """create message, but do not send it. Used for unit testing
        :param proposed_metric_states: a list of AbstractMetricStateContainer or derived classes """
        return self._msg_factory.mk_set_metric_state_message(self.endpoint_reference.address,
                                                             operation_handle, proposed_metric_states)
