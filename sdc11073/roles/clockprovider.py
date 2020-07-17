from .. import namespaces
from .. import sdcdevice
from .. import pmtypes
from ..nomenclature import NomenclatureCodes as nc
from . import providerbase

# coded values for SDC ntp and time zone
MDC_OP_SET_TIME_SYNC_REF_SRC = pmtypes.CodedValue(nc.MDC_OP_SET_TIME_SYNC_REF_SRC)
MDC_ACT_SET_TIME_ZONE = pmtypes.CodedValue(nc.MDC_ACT_SET_TIME_ZONE)

OP_SET_NTP = pmtypes.CodedValue(nc.OP_SET_NTP)
OP_SET_TZ = pmtypes.CodedValue(nc.OP_SET_TZ)

class GenericSDCClockProvider(providerbase.ProviderRole):
    """ Handles operations for setting ntp server and time zone.
    It guarantees that mdib has a clock descriptor and that there operations for setting
    ReferenceSource and Timezone of clock state."""

    def __init__(self, log_prefix):
        super(GenericSDCClockProvider, self).__init__(log_prefix)
        self._set_ntp_operations = []
        self._set_tz_operations = []

    def initOperations(self, mdib):
        super(GenericSDCClockProvider, self).initOperations(mdib)
        # create a clock descriptor and state if they do not exist in mdib
        clockDescriptor = self._mdib.descriptions.NODETYPE.getOne(namespaces.domTag('ClockDescriptor'), allowNone=True)
        if clockDescriptor is None:
            mdsContainer = self._mdib.descriptions.NODETYPE.getOne(namespaces.domTag('MdsDescriptor'))
            clock_descr_handle = 'clock_' + mdsContainer.handle
            self._logger.info('creating a clock descriptor, handle={}'.format(clock_descr_handle))
            clockDescriptor = self._mdib.createClockDescriptorContainer(handle=clock_descr_handle,
                                                                        parentHandle=mdsContainer.handle,
                                                                        codedValue=pmtypes.CodedValue(123),
                                                                        safetyClassification=pmtypes.SafetyClassification.INF)

        clockState = self._mdib.states.descriptorHandle.getOne(clockDescriptor.handle, allowNone = True)
        if clockState is None:
            clockState = self._mdib.mkStateContainerFromDescriptor(clockDescriptor)
            self._mdib.addState(clockState)


    def makeOperationInstance(self, operationDescriptorContainer):
        if operationDescriptorContainer.coding in (MDC_OP_SET_TIME_SYNC_REF_SRC.coding, OP_SET_NTP.coding):
            self._logger.info('instantiating "set ntp server" operation from existing descriptor handle={}'.format(
                operationDescriptorContainer.handle))
            set_ntp_operation = self._mkOperationFromOperationDescriptor(operationDescriptorContainer,
                                                                         currentArgumentHandler=self._setNTPString)
            self._set_ntp_operations.append(set_ntp_operation)
            return set_ntp_operation
        elif operationDescriptorContainer.coding in (MDC_ACT_SET_TIME_ZONE.coding, OP_SET_TZ.coding):
            self._logger.info('instantiating "set time zone" operation from existing descriptor handle={}'.format(
                operationDescriptorContainer.handle))
            set_tz_operation = self._mkOperationFromOperationDescriptor(operationDescriptorContainer,
                                                                        currentArgumentHandler=self._setTZString)
            self._set_tz_operations.append(set_tz_operation)
            return set_tz_operation
        return None  # ?

    def makeMissingOperations(self):
        ops = []
        mdsContainer = self._mdib.descriptions.NODETYPE.getOne(namespaces.domTag('MdsDescriptor'))
        clockDescriptor = self._mdib.descriptions.NODETYPE.getOne(namespaces.domTag('ClockDescriptor'), allowNone=True)
        if not self._set_ntp_operations:
            self._logger.info('adding "set ntp server" operation, code = {}'.format(nc.MDC_OP_SET_TIME_SYNC_REF_SRC))
            set_ntp_operation = self._mkOperation(sdcdevice.sco.SetStringOperation,
                                                 handle='SET_NTP_SRV_'+ mdsContainer.handle,
                                                 operationTargetHandle=clockDescriptor.handle,
                                                 codedValue=MDC_OP_SET_TIME_SYNC_REF_SRC,
                                                 currentArgumentHandler=self._setNTPString)
            self._set_ntp_operations.append(set_ntp_operation)
            ops.append(set_ntp_operation)
        if not self._set_tz_operations:
            self._logger.info('adding "set time zone" operation, code = {}'.format(nc.MDC_ACT_SET_TIME_ZONE))
            set_tz_operation = self._mkOperation(sdcdevice.sco.SetStringOperation,
                                                 handle='SET_TZONE_'+ mdsContainer.handle,
                                                 operationTargetHandle=clockDescriptor.handle,
                                                 codedValue=MDC_ACT_SET_TIME_ZONE,
                                                 currentArgumentHandler=self._setTZString)
            self._set_tz_operations.append(set_tz_operation)
            ops.append(set_tz_operation)
        return ops


    def _setNTPString(self, operationInstance, value):
        '''This is the handler for the set ntp server operation.
         It sets the ReferenceSource value of clock state'''
        operationDescriptorHandle = operationInstance.handle
        operationDescriptorContainer = self._mdib.descriptions.handle.getOne(operationDescriptorHandle)
        operationTargetHandle = operationDescriptorContainer.OperationTarget
        self._logger.info('set value {} from {} to {}', operationTargetHandle, operationInstance.currentValue,
                          value)
        with self._mdib.mdibUpdateTransaction() as mgr:
            state = mgr.getComponentState(operationTargetHandle)
            if state.NODETYPE == namespaces.domTag('MdsState'):
                mdsHandle = state.descriptorHandle
                mgr.ungetState(state)
                # look for the ClockState child
                clockDescriptors = self._mdib.descriptions.NODETYPE.get(namespaces.domTag('ClockDescriptor'),[])
                clockDescriptors = [ c for c in clockDescriptors if c.parentHandle == mdsHandle]
                if len(clockDescriptors) == 1:
                    state = mgr.getComponentState(clockDescriptors[0].handle)
            if state.NODETYPE != namespaces.domTag('ClockState'):
                raise RuntimeError('_setNTPString: expected ClockState, got {}'.format(state.NODETYPE.localname))
            state.ReferenceSource = [pmtypes.ElementWithTextOnly(value)]


    def _setTZString(self, operationInstance, value):
        '''This is the handler for the set time zone operation.
         It sets the TimeZone value of clock state.'''
        operationDescriptorHandle = operationInstance.handle
        operationDescriptorContainer = self._mdib.descriptions.handle.getOne(operationDescriptorHandle)
        operationTargetHandle = operationDescriptorContainer.OperationTarget
        self._logger.info('set value {} from {} to {}', operationTargetHandle, operationInstance.currentValue,
                          value)
        with self._mdib.mdibUpdateTransaction() as mgr:
            state = mgr.getComponentState(operationTargetHandle)
            if state.NODETYPE == namespaces.domTag('MdsState'):
                mdsHandle = state.descriptorHandle
                mgr.ungetState(state)
                # look for the ClockState child
                clockDescriptors = self._mdib.descriptions.NODETYPE.get(namespaces.domTag('ClockDescriptor'),[])
                clockDescriptors = [ c for c in clockDescriptors if c.parentHandle == mdsHandle]
                if len(clockDescriptors) == 1:
                    state = mgr.getComponentState(clockDescriptors[0].handle)

            if state.NODETYPE != namespaces.domTag('ClockState'):
                raise RuntimeError('_setNTPString: expected Clockstate, got {}'.format(state.NODETYPE.localname))
            state.TimeZone = value
