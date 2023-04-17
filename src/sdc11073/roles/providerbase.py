from functools import partial

from .. import loghelper
from .. import observableproperties as properties


class ProviderRole:
    def __init__(self, mdib, log_prefix):
        self._mdib = mdib
        self._logger = loghelper.get_logger_adapter(f'sdc.device.{self.__class__.__name__}', log_prefix)

    def stop(self):
        """ if provider uses worker threads, implement stop method"""

    def init_operations(self, sco):
        pass

    def make_operation_instance(self, operation_descriptor_container,  # pylint: disable=unused-argument,no-self-use
                                operation_cls_getter):  # pylint: disable=unused-argument
        """returns a callable for this operation or None.
        If a mdib already has operations defined, this method can connect a handler to a given operation descriptor.
        Use case: initialization from an existing mdib"""
        return None

    def make_missing_operations(self, sco):  # pylint: disable=unused-argument, no-self-use
        """
        This method is called after all existing operations from mdib have been registered.
        If a role provider needs to add operations beyond that, it can do it here.
        :return: []
        """
        return []

    def on_pre_commit(self, mdib, transaction):
        pass

    def on_post_commit(self, mdib, transaction):
        pass

    def _set_numeric_value(self, operation_instance, value):
        """ sets a numerical metric value"""
        pm_types = self._mdib.data_model.pm_types
        operation_target_handle = self._get_operation_target_handle(operation_instance)
        self._logger.info('set value of {} via {} from {} to {}', operation_target_handle, operation_instance.handle,
                          operation_instance.current_value, value)
        operation_instance.current_value = value
        with self._mdib.transaction_manager() as mgr:
            # state = mgr.getMetricState(operation_target_handle)
            state = mgr.get_state(operation_target_handle)
            if state.MetricValue is None:
                state.mk_metric_value()
            state.MetricValue.Value = value
            # SF1823: For Metrics with the MetricCategory = Set|Preset that are being modified as a result of a
            # SetValue or SetString operation a Metric Provider shall set the MetricQuality / Validity = Vld.
            metric_descriptor_container = self._mdib.descriptions.handle.get_one(operation_target_handle)
            if metric_descriptor_container.MetricCategory in (pm_types.MetricCategory.SETTING,
                                                              pm_types.MetricCategory.PRESETTING):
                state.MetricValue.Validity = pm_types.MeasurementValidity.VALID

    def _set_string(self, operation_instance, value):
        """ sets a string value"""
        pm_types = self._mdib.data_model.pm_types
        operation_target_handle = self._get_operation_target_handle(operation_instance)
        self._logger.info('set value {} from {} to {}', operation_target_handle, operation_instance.current_value,
                          value)
        operation_instance.current_value = value
        with self._mdib.transaction_manager() as mgr:
            # state = mgr.getMetricState(operation_target_handle)
            state = mgr.get_state(operation_target_handle)
            if state.MetricValue is None:
                state.mk_metric_value()
            state.MetricValue.Value = value
            # SF1823: For Metrics with the MetricCategory = Set|Preset that are being modified as a result of a
            # SetValue or SetString operation a Metric Provider shall set the MetricQuality / Validity = Vld.
            metric_descriptor_container = self._mdib.descriptions.handle.get_one(operation_target_handle)
            if metric_descriptor_container.MetricCategory in (pm_types.MetricCategory.SETTING,
                                                              pm_types.MetricCategory.PRESETTING):
                state.MetricValue.Validity = pm_types.MeasurementValidity.VALID

    def _mk_operation_from_operation_descriptor(self, operation_descriptor_container,
                                                operation_cls_getter,
                                                current_argument_handler=None,
                                                current_request_handler=None,
                                                timeout_handler=None):
        """
        :param operation_descriptor_container: the operation container for which this operation Handler shall be created
        :param current_argument_handler: the handler that shall be called by operation
        :param current_request_handler: the handler that shall be called by operation
        :para timeout_handler: callable when timeout is detected (InvocationEffectiveTimeout)
        :return: instance of cls
        """
        cls = operation_cls_getter(operation_descriptor_container.NODETYPE)
        operation = self._mk_operation(cls,
                                       operation_descriptor_container.Handle,
                                       operation_descriptor_container.OperationTarget,
                                       operation_descriptor_container.coding,
                                       current_argument_handler,
                                       current_request_handler,
                                       timeout_handler)
        return operation

    def _mk_operation(self, cls, handle, operation_target_handle, # pylint: disable=no-self-use
                      coded_value, current_argument_handler=None,
                      current_request_handler=None, timeout_handler=None):
        """

        :param cls: one of the Operations defined in sdcdevice.sco
        :param handle: the handle of this operation
        :param operation_target_handle: the handle of the operation target
        :param coded_value: the CodedValue for the Operation ( can be None)
        :param current_argument_handler: the handler that shall be called by operation
        :param current_request_handler: the handler that shall be called by operation
        :return: instance of cls
        """
        operation = cls(handle=handle,
                        operation_target_handle=operation_target_handle,
                        coded_value=coded_value)
        if current_argument_handler:
            # bind method to current_argument
            properties.strongbind(operation, current_argument=partial(current_argument_handler, operation))
        if current_request_handler:
            # bind method to current_request
            properties.strongbind(operation, current_request=partial(current_request_handler, operation))
        if timeout_handler:
            # bind method to current_request
            properties.strongbind(operation, on_timeout=partial(timeout_handler, operation))
        return operation

    def _get_operation_target_handle(self, operation_instance):
        operation_descriptor_handle = operation_instance.handle
        operation_descriptor_container = self._mdib.descriptions.handle.get_one(operation_descriptor_handle)
        return operation_descriptor_container.OperationTarget

    @staticmethod
    def _create_descriptor_container(container_cls, handle, parent_handle, coded_value, safety_classification):
        obj = container_cls(handle=handle, parent_handle=parent_handle)
        obj.SafetyClassification = safety_classification
        obj.Type = coded_value
        return obj
