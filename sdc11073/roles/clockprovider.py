from . import providerbase
from .nomenclature import NomenclatureCodes as nc
from .. import namespaces
from .. import pmtypes

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
        super().__init__(log_prefix)
        self._set_ntp_operations = []
        self._set_tz_operations = []

    def init_operations(self, mdib):
        super().init_operations(mdib)
        # create a clock descriptor and state if they do not exist in mdib
        clock_descriptor = self._mdib.descriptions.NODETYPE.get_one(namespaces.domTag('ClockDescriptor'),
                                                                    allow_none=True)
        if clock_descriptor is None:
            mds_container = self._mdib.descriptions.NODETYPE.get_one(namespaces.domTag('MdsDescriptor'))
            clock_descr_handle = 'clock_' + mds_container.Handle
            self._logger.info(f'creating a clock descriptor, handle={clock_descr_handle}')
            clock_descriptor = self._mdib.descriptor_factory.create_clock_descriptor_container(
                handle=clock_descr_handle,
                parent_handle=mds_container.Handle,
                coded_value=pmtypes.CodedValue(123),
                safety_classification=pmtypes.SafetyClassification.INF)

        clock_state = self._mdib.states.descriptorHandle.get_one(clock_descriptor.Handle, allow_none=True)
        if clock_state is None:
            clock_state = self._mdib.mk_state_container_from_descriptor(clock_descriptor)
            self._mdib.add_state(clock_state)

    def make_operation_instance(self, operation_descriptor_container, operation_cls_getter):
        if operation_descriptor_container.coding in (MDC_OP_SET_TIME_SYNC_REF_SRC.coding, OP_SET_NTP.coding):
            self._logger.info(f'instantiating "set ntp server" operation from existing descriptor handle={operation_descriptor_container.Handle}')
            set_ntp_operation = self._mk_operation_from_operation_descriptor(operation_descriptor_container,
                                                                             operation_cls_getter,
                                                                             current_argument_handler=self._set_ntp_string)
            self._set_ntp_operations.append(set_ntp_operation)
            return set_ntp_operation
        if operation_descriptor_container.coding in (MDC_ACT_SET_TIME_ZONE.coding, OP_SET_TZ.coding):
            self._logger.info(f'instantiating "set time zone" operation from existing descriptor handle={operation_descriptor_container.Handle}')
            set_tz_operation = self._mk_operation_from_operation_descriptor(operation_descriptor_container,
                                                                            operation_cls_getter,
                                                                            current_argument_handler=self._set_tz_string)
            self._set_tz_operations.append(set_tz_operation)
            return set_tz_operation
        return None  # ?

    def _set_ntp_string(self, operation_instance, value):
        """This is the handler for the set ntp server operation.
         It sets the ReferenceSource value of clock state"""
        operation_target_handle = self._get_operation_target_handle(operation_instance)
        self._logger.info('set value {} from {} to {}', operation_target_handle, operation_instance.current_value,
                          value)
        with self._mdib.transaction_manager() as mgr:
            # state = mgr.getComponentState(operation_target_handle)
            state = mgr.get_state(operation_target_handle)
            if state.NODETYPE == namespaces.domTag('MdsState'):
                mds_handle = state.descriptorHandle
                mgr.unget_state(state)
                # look for the ClockState child
                clock_descriptors = self._mdib.descriptions.NODETYPE.get(namespaces.domTag('ClockDescriptor'), [])
                clock_descriptors = [c for c in clock_descriptors if c.parent_handle == mds_handle]
                if len(clock_descriptors) == 1:
                    # state = mgr.getComponentState(clock_descriptors[0].handle)
                    state = mgr.get_state(clock_descriptors[0].handle)
            if state.NODETYPE != namespaces.domTag('ClockState'):
                raise RuntimeError(f'_set_ntp_string: expected ClockState, got {state.NODETYPE.localname}')
            state.ReferenceSource = [pmtypes.ElementWithTextOnly(value)]

    def _set_tz_string(self, operation_instance, value):
        """This is the handler for the set time zone operation.
         It sets the TimeZone value of clock state."""
        operation_target_handle = self._get_operation_target_handle(operation_instance)
        self._logger.info(f'set value {operation_target_handle} from {operation_instance.current_value} to {value}')
        with self._mdib.transaction_manager() as mgr:
            state = mgr.get_state(operation_target_handle)
            if state.NODETYPE == namespaces.domTag('MdsState'):
                mds_handle = state.descriptorHandle
                mgr.unget_state(state)
                # look for the ClockState child
                clock_descriptors = self._mdib.descriptions.NODETYPE.get(namespaces.domTag('ClockDescriptor'), [])
                clock_descriptors = [c for c in clock_descriptors if c.parent_handle == mds_handle]
                if len(clock_descriptors) == 1:
                    state = mgr.get_state(clock_descriptors[0].handle)

            if state.NODETYPE != namespaces.domTag('ClockState'):
                raise RuntimeError(f'_set_ntp_string: expected ClockState, got {state.NODETYPE.localname}')
            state.TimeZone = value


class SDCClockProvider(GenericSDCClockProvider):
    """This Implementation adds operations to mdib if they do not exist."""

    def make_missing_operations(self, operation_cls_getter):
        ops = []
        mds_container = self._mdib.descriptions.NODETYPE.get_one(namespaces.domTag('MdsDescriptor'))
        clock_descriptor = self._mdib.descriptions.NODETYPE.get_one(namespaces.domTag('ClockDescriptor'),
                                                                    allow_none=True)
        set_string_op_cls = operation_cls_getter(namespaces.domTag('SetStringOperationDescriptor'))

        if not self._set_ntp_operations:
            self._logger.info(f'adding "set ntp server" operation, code = {nc.MDC_OP_SET_TIME_SYNC_REF_SRC}')
            set_ntp_operation = self._mk_operation(set_string_op_cls,
                                                   handle='SET_NTP_SRV_' + mds_container.handle,
                                                   operation_target_handle=clock_descriptor.handle,
                                                   coded_value=MDC_OP_SET_TIME_SYNC_REF_SRC,
                                                   current_argument_handler=self._set_ntp_string)
            self._set_ntp_operations.append(set_ntp_operation)
            ops.append(set_ntp_operation)
        if not self._set_tz_operations:
            self._logger.info(f'adding "set time zone" operation, code = {nc.MDC_ACT_SET_TIME_ZONE}')
            set_tz_operation = self._mk_operation(set_string_op_cls,
                                                  handle='SET_TZONE_' + mds_container.handle,
                                                  operation_target_handle=clock_descriptor.handle,
                                                  coded_value=MDC_ACT_SET_TIME_ZONE,
                                                  current_argument_handler=self._set_tz_string)
            self._set_tz_operations.append(set_tz_operation)
            ops.append(set_tz_operation)
        return ops
