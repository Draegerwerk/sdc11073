import uuid
import time

from ..msgtypes import RetrievabilityMethod
from .devicewaveform import DefaultWaveformSource

class DeviceMdibMethods:
    waveform_provider_cls = DefaultWaveformSource
    def __init__(self, device_mdib):
        self._mdib = device_mdib
        self.waveform_provider = self.waveform_provider_cls(device_mdib)
        self.descriptor_factory = DescriptorFactory(device_mdib)
        self.default_instance_identifiers = (device_mdib.data_model.pmtypes.InstanceIdentifier(
            root='rootWithNoMeaning', extension_string='System'),)

    def ensure_location_context_descriptor(self):
        """Create a LocationContextDescriptor if there is none in mdib."""
        mdib = self._mdib
        pm = mdib.data_model.pm_names
        location_context_container = mdib.descriptions.NODETYPE.get_one(pm.LocationContextDescriptor, allow_none=True)
        if location_context_container is None:
            system_context_container = mdib.descriptions.NODETYPE.get_one(pm.SystemContextDescriptor)
            descr_cls = mdib.data_model.get_descriptor_container_class(pm.LocationContextDescriptor)
            descr_container = descr_cls(handle=uuid.uuid4().hex, parent_handle=system_context_container.Handle)
            descr_container.SafetyClassification = mdib.data_model.pmtypes.SafetyClassification.INF
            mdib.descriptions.add_object(descr_container)

    def ensure_patient_context_descriptor(self):
        """Create PatientContextDescriptor if there is none in mdib."""
        mdib = self._mdib
        pm = mdib.data_model.pm_names
        patient_context_container = mdib.descriptions.NODETYPE.get_one(pm.PatientContextDescriptor, allow_none=True)
        if patient_context_container is None:
            system_context_container = mdib.descriptions.NODETYPE.get_one(pm.SystemContextDescriptor)
            descr_cls = mdib.data_model.get_descriptor_container_class(pm.PatientContextDescriptor)
            descr_container = descr_cls(handle=uuid.uuid4().hex, parent_handle=system_context_container.Handle)
            descr_container.SafetyClassification =  mdib.data_model.pmtypes.SafetyClassification.INF
            mdib.descriptions.add_object(descr_container)

    def set_location(self, sdc_location, validators=None, set_associated=True):
        """
        This method updates only the mdib internal data!
        use the SdcDevice.set_location method if you want to publish the address on the network.
        :param sdc_location: a pysdc.location.SdcLocation instance
        :param validators: a list of pysdc.pmtypes.InstanceIdentifier objects or None
        :param set_associated: if True, DindingTime, BindingMdibVersion and ContextAssociation are set
        """
        mdib = self._mdib
        pm = mdib.data_model.pm_names
        with mdib.transaction_manager() as mgr:
            all_location_contexts = mdib.context_states.NODETYPE.get(pm.LocationContextState, [])
            # set all to currently associated Locations to Disassociated
            associated_locations = [l for l in all_location_contexts if
                                    l.ContextAssociation ==  mdib.data_model.pmtypes.ContextAssociation.ASSOCIATED]
            for location in associated_locations:
                location_context = mgr.get_context_state(location.Handle)
                location_context.ContextAssociation =  mdib.data_model.pmtypes.ContextAssociation.DISASSOCIATED
                # UnbindingMdibVersion is the first version in which it is no longer bound ( == this version)
                location_context.UnbindingMdibVersion = mdib.mdib_version + 1
            descriptor_container = mdib.descriptions.NODETYPE.get_one(pm.LocationContextDescriptor)

            mdib._current_location = mgr.mk_context_state(
                descriptor_container.Handle, set_associated=set_associated)  # this creates a new location state
            mdib._current_location.update_from_sdc_location(sdc_location)
            if validators is not None:
                mdib._current_location.Validator = validators

    def mk_state_containers_for_all_descriptors(self):
        """The model requires that there is a state for every descriptor (exception: multi-states)
        Call this method to create missing states
        :return:
        """
        mdib = self._mdib
        pm = mdib.data_model.pm_names
        for descr in mdib.descriptions.objects:
            if descr.Handle not in mdib.states.descriptorHandle and descr.Handle not in mdib.context_states.descriptorHandle:
                state_cls = mdib.data_model.get_state_class_for_descriptor(descr)
                if state_cls.is_multi_state:
                    pass  # nothing to do, it is allowed to have no state
                else:
                    state = state_cls(descr)
                    # add some initial values where needed
                    if state.is_alert_condition:
                        state.DeterminationTime = time.time()
                    elif state.NODETYPE == pm.AlertSystemState:
                        state.LastSelfCheck = time.time()
                        state.SelfCheckCount = 1
                    elif state.NODETYPE == pm.ClockState:
                        state.LastSet = time.time()
                    if mdib._current_transaction is not None:
                        mdib._current_transaction.add_state(state)
                    else:
                        mdib.states.add_object(state)

    def update_retrievability_lists(self):
        """This method updates internal lists, based on current mdib descriptors. """
        mdib = self._mdib
        with mdib.mdib_lock:
            del mdib._retrievability_episodic[:]
            mdib.retrievability_periodic.clear()
            for descr in mdib.descriptions.objects:
                if descr.retrievability is not None:
                    for r_by in descr.retrievability.By:
                        if r_by.Method == RetrievabilityMethod.EPISODIC:
                            mdib._retrievability_episodic.append(descr.Handle)
                        elif r_by.Method == RetrievabilityMethod.PERIODIC:
                            period_float = r_by.UpdatePeriod
                            period_ms = int(period_float * 1000.0)
                            mdib.retrievability_periodic[period_ms].append(descr.Handle)

    def update_all_rt_samples(self):
        if self.waveform_provider is None:
            return
        with self._mdib._rt_sample_transaction() as transaction:
            self.waveform_provider.update_all_realtime_samples(transaction)


class DescriptorFactory:
    def __init__(self, mdib):
        self._mdib = mdib

    @staticmethod
    def _create_descriptor_container(container_cls, handle, parent_handle, coded_value, safety_classification):
        obj = container_cls(handle=handle, parent_handle=parent_handle)
        obj.SafetyClassification = safety_classification
        obj.Type = coded_value
        return obj

    def create_vmd_descriptor_container(self,
                                        handle: str,
                                        parent_handle: str,
                                        coded_value,
                                        safety_classification):
        """
        This method creates an VmdDescriptorContainer with the given properties.
        :param handle: Handle of the new container
        :param parent_handle: Handle of the parent
        :param coded_value: a pmtypes.CodedValue instance that defines what this onject represents in medical terms.
        :param safety_classification: a pmtypes.SafetyClassification value
        :return: the created object
        """
        model = self._mdib.data_model
        cls = model.get_descriptor_container_class(model.pm_names.VmdDescriptor)
        return self._create_descriptor_container(cls, handle, parent_handle, coded_value, safety_classification)

    def create_channel_descriptor_container(self,
                                            handle: str,
                                            parent_handle: str,
                                            coded_value,
                                            safety_classification):
        """
        This method creates a ChannelDescriptorContainer with the given properties.
        :param handle: Handle of the new container.
        :param parent_handle: Handle of the parent.
        :param coded_value: a pmtypes.CodedValue instance that defines what this onject represents in medical terms.
        :param safety_classification: a pmtypes.SafetyClassification value.
        :return: the created object.
        """
        model = self._mdib.data_model
        cls = model.get_descriptor_container_class(model.pm_names.ChannelDescriptor)
        return self._create_descriptor_container(cls, handle, parent_handle, coded_value, safety_classification)

    def create_string_metric_descriptor_container(self,
                                                  handle: str,
                                                  parent_handle: str,
                                                  coded_value,
                                                  safety_classification,
                                                  unit,
                                                  metric_availability,
                                                  metric_category):
        """
        This method creates a StringMetricDescriptorContainer with the given properties.
        :param handle: Handle of the new container
        :param parent_handle: Handle of the parent
        :param coded_value: a pmtypes.CodedValue instance that defines what this onject represents in medical terms.
        :param safety_classification: a pmtypes.SafetyClassification value
        :param unit: a CodedValue
        :param metric_availability: pmtypes.MetricAvailability
        :param metric_category: pmtypes.MetricCategory
        :return: the created object
        """
        model = self._mdib.data_model
        cls = model.get_descriptor_container_class(model.pm_names.StringMetricDescriptor)
        obj = self._create_descriptor_container(cls, handle, parent_handle, coded_value, safety_classification)
        obj.Unit = unit
        obj.MetricAvailability = metric_availability
        obj.MetricCategory = metric_category
        return obj

    def create_enum_string_metric_descriptor_container(self,
                                                       handle: str,
                                                       parent_handle: str,
                                                       coded_value,
                                                       safety_classification,
                                                       unit,
                                                       allowed_values,
                                                       metric_availability,
                                                       metric_category):
        """
        This method creates an EnumStringMetricDescriptorContainer with the given properties.
        :param handle: Handle of the new container
        :param parent_handle: Handle of the parent
        :param coded_value: a pmtypes.CodedValue instance that defines what this onject represents in medical terms.
        :param safety_classification: a pmtypes.SafetyClassification value
        :param unit: pmtypes.CodedValue
        :param allowed_values:
        :param metric_availability: pmtypes.MetricAvailability
        :param metric_category: pmtypes.MetricCategory
        :return: the created object
        """
        model = self._mdib.data_model
        cls = model.get_descriptor_container_class(model.pm_names.EnumStringMetricDescriptor)
        obj = self._create_descriptor_container(cls, handle, parent_handle, coded_value, safety_classification)
        obj.Unit = unit
        obj.MetricAvailability = metric_availability
        obj.MetricCategory = metric_category
        obj.AllowedValue = allowed_values
        return obj

    def create_clock_descriptor_container(self,
                                          handle: str,
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
