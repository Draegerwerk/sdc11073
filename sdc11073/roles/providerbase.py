from functools import partial
from .. import observableproperties as properties
from .. import loghelper
from .. import pmtypes
from .. import sdcdevice



class ProviderRole(object):
    def __init__(self, log_prefix):
        self._mdib = None
        self._logger = loghelper.getLoggerAdapter('sdc.device.{}'.format(self.__class__.__name__), log_prefix)

    def stop(self):
        """ if provider uses worker threads, implement stop method"""
        pass

    def initOperations(self, mdib):
        self._mdib = mdib

    def makeOperationInstance(self, operationDescriptorContainer): #pylint: disable=unused-argument
        """returns a callable for this operation or None.
        If a mdib already has operations defined, this method can connect a handler to a given operation descriptor.
        Use case: initialization from an existing mdib"""
        return None

    def makeMissingOperations(self):
        """
        This method is called after all existing operations from mdib have been registered.
        If a role provider needs to add operations beyond that, it can do it here.
        :return: []
        """
        return []

    def onPreCommit(self, mdib, transaction):
        pass

    def onPostCommit(self, mdib, transaction):
        pass


    def _setNumericValue(self, operationInstance, value):
        """ sets a numerical metric value"""
        operationDescriptorHandle = operationInstance.handle
        operationDescriptorContainer = self._mdib.descriptions.handle.getOne(operationDescriptorHandle)
        operationTargetHandle = operationDescriptorContainer.OperationTarget
        self._logger.info('set value of {} via {} from {} to {}', operationTargetHandle, operationDescriptorHandle,
                          operationInstance.currentValue, value)
        operationInstance.currentValue = value
        with self._mdib.mdibUpdateTransaction() as mgr:
            state = mgr.getMetricState(operationTargetHandle)
            if state.metricValue is None:
                state.mkMetricValue()
            state.metricValue.Value = value
            #SF1823: For Metrics with the MetricCategory = Set|Preset that are being modified as a result of a
            # SetValue or SetString operation a Metric Provider shall set the MetricQuality / Validity = Vld.
            metricDescriptorContainer = self._mdib.descriptions.handle.getOne(operationTargetHandle)
            if metricDescriptorContainer.MetricCategory in (pmtypes.MetricCategory.SETTING,
                                                            pmtypes.MetricCategory.PRESETTING):
                state.metricValue.Validity = pmtypes.MeasurementValidity.VALID


    def _setString(self, operationInstance, value):
        """ sets a string value"""
        operationDescriptorHandle = operationInstance.handle
        operationDescriptorContainer = self._mdib.descriptions.handle.getOne(operationDescriptorHandle)
        operationTargetHandle = operationDescriptorContainer.OperationTarget
        self._logger.info('set value {} from {} to {}', operationTargetHandle, operationInstance.currentValue,
                          value)
        operationInstance.currentValue = value
        with self._mdib.mdibUpdateTransaction() as mgr:
            state = mgr.getMetricState(operationTargetHandle)
            if state.metricValue is None:
                state.mkMetricValue()
            state.metricValue.Value = value
            #SF1823: For Metrics with the MetricCategory = Set|Preset that are being modified as a result of a
            # SetValue or SetString operation a Metric Provider shall set the MetricQuality / Validity = Vld.
            metricDescriptorContainer = self._mdib.descriptions.handle.getOne(operationTargetHandle)
            if metricDescriptorContainer.MetricCategory in (pmtypes.MetricCategory.SETTING,
                                                            pmtypes.MetricCategory.PRESETTING):
                state.metricValue.Validity = pmtypes.MeasurementValidity.VALID


    def _mkOperationFromOperationDescriptor(self, operationDescriptorContainer,
                                            currentArgumentHandler=None, currentRequestHandler=None,
                                            timeoutHandler=None):
        """
        :param operationDescriptorContainer: the operation container for which this operation Handler shall be created
        :param currentArgumentHandler: the handler that shall be called by operation
        :param currentRequestHandler: the handler that shall be called by operation
        :param timeoutHandler: callable when timeout is detected (InvocationEffectiveTimeout)
        :return: instance of cls
        """
        cls = sdcdevice.sco.getOperationClass(operationDescriptorContainer.NODETYPE)
        op = self._mkOperation(cls,
                               operationDescriptorContainer.handle,
                               operationDescriptorContainer.OperationTarget,
                               operationDescriptorContainer.coding,
                               currentArgumentHandler,
                               currentRequestHandler,
                               timeoutHandler)
        op.safetyReq = operationDescriptorContainer.SafetyReq
        return op

    def _mkOperation(self, cls, handle, operationTargetHandle, codedValue,
                     currentArgumentHandler=None, currentRequestHandler=None, timeoutHandler=None):
        """

        :param cls: one of the Operations defined in sdcdevice.sco
        :param handle: the handle of this operation
        :param operationTargetHandle: the handle of the operation target
        :param codedValue: the CodedValue for the Operation ( can be None)
        :param currentArgumentHandler: the handler that shall be called by operation
        :param currentRequestHandler: the handler that shall be called by operation
        :return: instance of cls
        """
        operation = cls(handle=handle,
                        operationTarget=operationTargetHandle,
                        codedValue=codedValue)
        if currentArgumentHandler:
            # bind method to currentArgument
            properties.strongbind(operation, currentArgument=partial(currentArgumentHandler, operation))
        if currentRequestHandler:
            # bind method to currentRequest
            properties.strongbind(operation, currentRequest=partial(currentRequestHandler, operation))
        if timeoutHandler:
            # bind method to onTimeout
            properties.strongbind(operation, onTimeout=partial(timeoutHandler, operation))
        return operation