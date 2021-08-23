from .providerbase import ProviderRole
from ..namespaces import domTag
from ..pmtypes import ComponentActivation


class GenericMetricProvider(ProviderRole):
    """ Always added operations: None
    This is a generic Handler for
    - SetValueOperation on numeric metrics
    - SetStringOperation on (enum) string metrics
    """

    def __init__(self, activation_state_can_remove_metric_value=True, log_prefix=None):
        '''

        :param activation_state_can_remove_metric_value: if True, SF717 is handled
               SF717: A Metric Provider shall not provide a MetricValue if the ActivationState = Shtdn|Off|Fail.
        '''
        super().__init__(log_prefix)
        self.activation_state_can_remove_metric_value = activation_state_can_remove_metric_value

    def make_operation_instance(self, operation_descriptor_container, operations_factory):
        ''' Can handle following cases:
        SetValueOperation, target = NumericMetricDescriptor: => handler = _set_numeric_value
        SetStringOperation, target = (Enum)StringMetricDescriptor: => handler = _set_string
        SetMetricStateOperationDescriptor, target = any subclass of AbstractMetricDescriptor: => handler = _set_metric_state
        '''
        operation_target_handle = operation_descriptor_container.OperationTarget
        op_target_descriptor_container = self._mdib.descriptions.handle.get_one(operation_target_handle)

        if op_target_descriptor_container.NODETYPE not in (domTag('StringMetricDescriptor'),
                                                           domTag('EnumStringMetricDescriptor'),
                                                           domTag('NumericMetricDescriptor'),
                                                           domTag('RealTimeSampleArrayMetricDescriptor')):
            return None  # this is not metric provider role

        if operation_descriptor_container.NODETYPE == domTag('SetValueOperationDescriptor'):
            if op_target_descriptor_container.NODETYPE == domTag('NumericMetricDescriptor'):
                op_cls = operations_factory(domTag('SetValueOperationDescriptor'))
                return self._mk_operation(op_cls,
                                          handle=operation_descriptor_container.handle,
                                          operation_target_handle=operation_target_handle,
                                          coded_value=operation_descriptor_container.Type,
                                          current_argument_handler=self._set_numeric_value)
            return None
        if operation_descriptor_container.NODETYPE == domTag('SetStringOperationDescriptor'):
            if op_target_descriptor_container.NODETYPE in (domTag('StringMetricDescriptor'),
                                                           domTag('EnumStringMetricDescriptor')):
                op_cls = operations_factory(domTag('SetStringOperationDescriptor'))
                return self._mk_operation(op_cls,
                                          handle=operation_descriptor_container.handle,
                                          operation_target_handle=operation_target_handle,
                                          coded_value=operation_descriptor_container.Type,
                                          current_argument_handler=self._set_string)
            return None
        if operation_descriptor_container.NODETYPE == domTag('SetMetricStateOperationDescriptor'):
            op_cls = operations_factory(domTag('SetMetricStateOperationDescriptor'))
            operation = self._mk_operation(op_cls,
                                           handle=operation_descriptor_container.handle,
                                           operation_target_handle=operation_target_handle,
                                           coded_value=operation_descriptor_container.Type,
                                           current_argument_handler=self._set_metric_state)
            return operation
        return None

    def _set_metric_state(self, operation_instance, value):
        '''

        :param operation_instance: the operation
        :param value: a list of proposed metric states
        :return:
        '''
        # ToDo: consider ModifiableDate attribute
        operation_instance.current_value = value
        with self._mdib.transaction_manager() as mgr:
            for proposed_state in value:
                # state = mgr.getMetricState(proposedMetricState.descriptorHandle)
                state = mgr.get_state(proposed_state.descriptorHandle)
                if state.isMetricState:
                    self._logger.info('updating {} with proposed metric state', state)
                    state.update_from_other_container(proposed_state,
                                                      skipped_properties=['StateVersion', 'DescriptorVersion'])
                else:
                    self._logger.warn('_set_metric_state operation: ignore invalid referenced type {} in operation',
                                      state.NODETYPE)

    def on_pre_commit(self, mdib, transaction):
        if not self.activation_state_can_remove_metric_value:
            return
        if transaction.metric_state_updates:
            self._handle_metrics_component_activation(transaction.metric_state_updates.values())
        if transaction.rt_sample_state_updates:
            self._handle_metrics_component_activation(transaction.rt_sample_state_updates.values())

    def _handle_metrics_component_activation(self, metric_state_updates):
        # check if MetricValue shall be removed
        for _, new_state in metric_state_updates:
            if new_state is None or not new_state.isMetricState:
                continue
            # SF717: check if MetricValue shall be automatically removed
            if new_state.ActivationState in (ComponentActivation.OFF,
                                             ComponentActivation.SHUTDOWN,
                                             ComponentActivation.FAILURE):
                if new_state.MetricValue is not None:
                    # remove metric value
                    self._logger.info('{}: remove metric value because ActivationState="{}", handle="{}"',
                                      self.__class__.__name__, new_state.ActivationState, new_state.descriptorHandle)
                    new_state.MetricValue = None
