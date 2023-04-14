from . import providerbase
from .nomenclature import NomenclatureCodes as nc


class GenericSDCClockProvider(providerbase.ProviderRole):
    """ Handles operations for setting ntp server and time zone.
    It guarantees that mdib has a clock descriptor and that there operations for setting
    ReferenceSource and Timezone of clock state."""

    def __init__(self, mdib, log_prefix):
        super().__init__(mdib, log_prefix)
        self._set_ntp_operations = []
        self._set_tz_operations = []
        pm_types = self._mdib.data_model.pm_types

        self.MDC_OP_SET_TIME_SYNC_REF_SRC = pm_types.CodedValue(nc.MDC_OP_SET_TIME_SYNC_REF_SRC)
        self.MDC_ACT_SET_TIME_ZONE = pm_types.CodedValue(nc.MDC_ACT_SET_TIME_ZONE)

        self.OP_SET_NTP = pm_types.CodedValue(nc.OP_SET_NTP)
        self.OP_SET_TZ = pm_types.CodedValue(nc.OP_SET_TZ)

    def init_operations(self, sco):
        super().init_operations(sco)
        pm_types = self._mdib.data_model.pm_types
        pm_names = self._mdib.data_model.pm_names
        # create a clock descriptor and state if they do not exist in mdib
        clock_descriptor = self._mdib.descriptions.NODETYPE.get_one(pm_names.ClockDescriptor,
                                                                    allow_none=True)
        if clock_descriptor is None:
            mds_container = self._mdib.descriptions.NODETYPE.get_one(pm_names.MdsDescriptor)
            clock_descr_handle = 'clock_' + mds_container.Handle
            self._logger.debug(f'creating a clock descriptor, handle={clock_descr_handle}')
            clock_descriptor = self._create_clock_descriptor_container(
                handle=clock_descr_handle,
                parent_handle=mds_container.Handle,
                coded_value=pm_types.CodedValue('123'),
                safety_classification=pm_types.SafetyClassification.INF)
            self._mdib.descriptions.add_object(clock_descriptor)
        clock_state = self._mdib.states.descriptorHandle.get_one(clock_descriptor.Handle, allow_none=True)
        if clock_state is None:
            clock_state = self._mdib.data_model.mk_state_container(clock_descriptor)
            self._mdib.states.add_object(clock_state)

    def make_operation_instance(self, operation_descriptor_container, operation_cls_getter):
        if operation_descriptor_container.coding in (self.MDC_OP_SET_TIME_SYNC_REF_SRC.coding, self.OP_SET_NTP.coding):
            self._logger.debug(
                f'instantiating "set ntp server" operation from existing descriptor handle={operation_descriptor_container.Handle}')
            set_ntp_operation = self._mk_operation_from_operation_descriptor(operation_descriptor_container,
                                                                             operation_cls_getter,
                                                                             current_argument_handler=self._set_ntp_string)
            self._set_ntp_operations.append(set_ntp_operation)
            return set_ntp_operation
        if operation_descriptor_container.coding in (self.MDC_ACT_SET_TIME_ZONE.coding, self.OP_SET_TZ.coding):
            self._logger.debug(
                f'instantiating "set time zone" operation from existing descriptor handle={operation_descriptor_container.Handle}')
            set_tz_operation = self._mk_operation_from_operation_descriptor(operation_descriptor_container,
                                                                            operation_cls_getter,
                                                                            current_argument_handler=self._set_tz_string)
            self._set_tz_operations.append(set_tz_operation)
            return set_tz_operation
        return None  # ?

    def _set_ntp_string(self, operation_instance, value):
        """This is the handler for the set ntp server operation.
         It sets the ReferenceSource value of clock state"""
        pm_types = self._mdib.data_model.pm_types
        pm_names = self._mdib.data_model.pm_names
        operation_target_handle = self._get_operation_target_handle(operation_instance)
        self._logger.info('set value {} from {} to {}', operation_target_handle, operation_instance.current_value,
                          value)
        with self._mdib.transaction_manager() as mgr:
            # state = mgr.getComponentState(operation_target_handle)
            state = mgr.get_state(operation_target_handle)
            if state.NODETYPE == pm_names.MdsState:
                mds_handle = state.DescriptorHandle
                mgr.unget_state(state)
                # look for the ClockState child
                clock_descriptors = self._mdib.descriptions.NODETYPE.get(pm_names.ClockDescriptor, [])
                clock_descriptors = [c for c in clock_descriptors if c.parent_handle == mds_handle]
                if len(clock_descriptors) == 1:
                    state = mgr.get_state(clock_descriptors[0].handle)
            if state.NODETYPE != pm_names.ClockState:
                raise ValueError(f'_set_ntp_string: expected ClockState, got {state.NODETYPE.localname}')
            state.ReferenceSource = [value]

    def _set_tz_string(self, operation_instance, value):
        """This is the handler for the set time zone operation.
         It sets the TimeZone value of clock state."""
        pm_names = self._mdib.data_model.pm_names
        operation_target_handle = self._get_operation_target_handle(operation_instance)
        self._logger.info(f'set value {operation_target_handle} from {operation_instance.current_value} to {value}')
        with self._mdib.transaction_manager() as mgr:
            state = mgr.get_state(operation_target_handle)
            if state.NODETYPE == pm_names.MdsState:
                mds_handle = state.DescriptorHandle
                mgr.unget_state(state)
                # look for the ClockState child
                clock_descriptors = self._mdib.descriptions.NODETYPE.get(pm_names.ClockDescriptor, [])
                clock_descriptors = [c for c in clock_descriptors if c.parent_handle == mds_handle]
                if len(clock_descriptors) == 1:
                    state = mgr.get_state(clock_descriptors[0].handle)

            if state.NODETYPE != pm_names.ClockState:
                raise ValueError(f'_set_ntp_string: expected ClockState, got {state.NODETYPE.localname}')
            state.TimeZone = value

    def _create_clock_descriptor_container(self, handle: str,
                                           parent_handle: str,
                                           coded_value,
                                           safety_classification):
        """
        This method creates a ClockDescriptorContainer with the given properties.
        :param handle: Handle of the new container
        :param parent_handle: Handle of the parent
        :param coded_value: a pmtypes.CodedValue instance that defines what this onject represents in medical terms.
        :param safety_classification: a pmtypes.SafetyClassification value
        :return: the created object
        """
        model = self._mdib.data_model
        cls = model.get_descriptor_container_class(model.pm_names.ClockDescriptor)
        return self._create_descriptor_container(cls, handle, parent_handle, coded_value, safety_classification)


class SDCClockProvider(GenericSDCClockProvider):
    """This Implementation adds operations to mdib if they do not exist."""

    def make_missing_operations(self, sco):
        pm_names = self._mdib.data_model.pm_names
        ops = []
        operation_cls_getter = sco.operation_cls_getter

        mds_container = self._mdib.descriptions.NODETYPE.get_one(pm_names.MdsDescriptor)
        clock_descriptor = self._mdib.descriptions.NODETYPE.get_one(pm_names.ClockDescriptor,
                                                                    allow_none=True)
        set_string_op_cls = operation_cls_getter(pm_names.SetStringOperationDescriptor)

        if not self._set_ntp_operations:
            self._logger.debug(f'adding "set ntp server" operation, code = {nc.MDC_OP_SET_TIME_SYNC_REF_SRC}')
            set_ntp_operation = self._mk_operation(set_string_op_cls,
                                                   handle='SET_NTP_SRV_' + mds_container.handle,
                                                   operation_target_handle=clock_descriptor.handle,
                                                   coded_value=self.MDC_OP_SET_TIME_SYNC_REF_SRC,
                                                   current_argument_handler=self._set_ntp_string)
            self._set_ntp_operations.append(set_ntp_operation)
            ops.append(set_ntp_operation)
        if not self._set_tz_operations:
            self._logger.debug(f'adding "set time zone" operation, code = {nc.MDC_ACT_SET_TIME_ZONE}')
            set_tz_operation = self._mk_operation(set_string_op_cls,
                                                  handle='SET_TZONE_' + mds_container.handle,
                                                  operation_target_handle=clock_descriptor.handle,
                                                  coded_value=self.MDC_ACT_SET_TIME_ZONE,
                                                  current_argument_handler=self._set_tz_string)
            self._set_tz_operations.append(set_tz_operation)
            ops.append(set_tz_operation)
        return ops

