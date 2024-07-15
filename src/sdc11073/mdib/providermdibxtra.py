from __future__ import annotations

import time
import uuid
from typing import TYPE_CHECKING, Any

from sdc11073.xml_types.pm_types import RetrievabilityMethod

if TYPE_CHECKING:
    from sdc11073.location import SdcLocation
    from sdc11073.xml_types.pm_types import (
        CodedValue,
        InstanceIdentifier,
        MetricAvailability,
        MetricCategory,
        SafetyClassification,
    )

    from .descriptorcontainers import AbstractDescriptorProtocol
    from .providermdib import ProviderMdib
    from .statecontainers import AbstractStateProtocol


class ProviderMdibMethods:
    """Extra methods for provider mdib tht are not core functionality."""

    def __init__(self, provider_mdib: ProviderMdib):
        self._mdib = provider_mdib
        self.descriptor_factory = DescriptorFactory(provider_mdib)
        self.default_validators = (provider_mdib.data_model.pm_types.InstanceIdentifier(
            root='rootWithNoMeaning', extension_string='System'),)

    def ensure_location_context_descriptor(self):
        """Create a LocationContextDescriptor if there is none in mdib."""
        mdib = self._mdib
        pm = mdib.data_model.pm_names
        system_context_descriptors = mdib.descriptions.NODETYPE.get(pm.SystemContextDescriptor, [])
        location_context_descriptors = mdib.descriptions.NODETYPE.get(pm.LocationContextDescriptor, [])

        for system_context_descriptor in system_context_descriptors:
            child_location_descriptors = [d for d in location_context_descriptors
                                          if d.parent_handle == system_context_descriptor.Handle
                                          and d.NODETYPE == pm.LocationContextDescriptor]
            if not child_location_descriptors:
                descr_cls = mdib.data_model.get_descriptor_container_class(pm.LocationContextDescriptor)
                descr_container = descr_cls(handle=uuid.uuid4().hex, parent_handle=system_context_descriptor.Handle)
                descr_container.SafetyClassification = mdib.data_model.pm_types.SafetyClassification.INF
                mdib.descriptions.add_object(descr_container)

    def ensure_patient_context_descriptor(self):
        """Create a PatientContextDescriptor if there is none in mdib."""
        mdib = self._mdib
        pm = mdib.data_model.pm_names
        system_context_descriptors = mdib.descriptions.NODETYPE.get(pm.SystemContextDescriptor, [])
        patient_context_descriptors = mdib.descriptions.NODETYPE.get(pm.PatientContextDescriptor, [])

        for system_context_descriptor in system_context_descriptors:
            child_location_descriptors = [d for d in patient_context_descriptors
                                          if d.parent_handle == system_context_descriptor.Handle
                                          and d.NODETYPE == pm.PatientContextDescriptor]
            if not child_location_descriptors:
                descr_cls = mdib.data_model.get_descriptor_container_class(pm.PatientContextDescriptor)
                descr_container = descr_cls(handle=uuid.uuid4().hex, parent_handle=system_context_descriptor.Handle)
                descr_container.SafetyClassification = mdib.data_model.pm_types.SafetyClassification.INF
                mdib.descriptions.add_object(descr_container)

    def set_location(self, sdc_location: SdcLocation,
                     validators: list[InstanceIdentifier] | None = None,
                     location_context_descriptor_handle: str | None = None):
        """Create a location context state. The new state will be the associated state.

        This method updates only the mdib data!
        Use the SdcProvider.set_location method if you want to publish the address on the network.
        :param sdc_location: a sdc11073.location.SdcLocation instance
        :param validators: a list of InstanceIdentifier objects or None
               If None, self.default_validators is used.
        :param location_context_descriptor_handle: Only needed if the mdib contains more than one
               LocationContextDescriptor. Then this defines the descriptor for which a new LocationContextState
               shall be created.
        """
        mdib = self._mdib
        pm = mdib.data_model.pm_names

        if location_context_descriptor_handle is None:
            # assume there is only one descriptor in mdib, user has not provided a handle.
            descriptor_container = mdib.descriptions.NODETYPE.get_one(pm.LocationContextDescriptor)
        else:
            descriptor_container = mdib.descriptions.handle.get_one(location_context_descriptor_handle)

        with mdib.context_state_transaction() as mgr:
            mgr.disassociate_all(descriptor_container.Handle)

            new_location = mgr.mk_context_state(descriptor_container.Handle, set_associated=True)
            new_location.update_from_sdc_location(sdc_location)
            if validators is None:
                new_location.Validator = self.default_validators
            else:
                new_location.Validator = validators

    def mk_state_containers_for_all_descriptors(self):
        """Create a state container for every descriptor that is missing a state in mdib.

        The model requires that there is a state for every descriptor (exception: multi-states)
        """
        mdib = self._mdib
        pm = mdib.data_model.pm_names
        for descr in mdib.descriptions.objects:
            if descr.Handle not in mdib.states.descriptor_handle \
                    and descr.Handle not in mdib.context_states.descriptor_handle:
                state_cls = mdib.data_model.get_state_class_for_descriptor(descr)
                if state_cls.is_multi_state:
                    pass  # nothing to do, it is allowed to have no state
                else:
                    state = state_cls(descr)
                    # add some initial values where needed
                    if state.is_alert_condition:
                        state.DeterminationTime = time.time()
                    elif state.NODETYPE == pm.AlertSystemState:  # noqa: SIM300
                        state.LastSelfCheck = time.time()
                        state.SelfCheckCount = 1
                    elif state.NODETYPE == pm.ClockState:  # noqa: SIM300
                        state.LastSet = time.time()
                    if mdib.current_transaction is not None:
                        mdib.current_transaction.add_state(state)
                    else:
                        mdib.states.add_object(state)

    def update_retrievability_lists(self):
        """Update internal lists, based on current mdib descriptors."""
        mdib = self._mdib
        with mdib.mdib_lock:
            del mdib._retrievability_episodic[:]  # noqa: SLF001
            mdib.retrievability_periodic.clear()
            for descr in mdib.descriptions.objects:
                for r in descr.get_retrievability():
                    for r_by in r.By:
                        if r_by.Method == RetrievabilityMethod.EPISODIC:
                            mdib._retrievability_episodic.append(descr.Handle)  # noqa: SLF001
                        elif r_by.Method == RetrievabilityMethod.PERIODIC:
                            period_float = r_by.UpdatePeriod
                            period_ms = int(period_float * 1000.0)
                            mdib.retrievability_periodic[period_ms].append(descr.Handle)

    def set_all_source_mds(self):
        """Set the source mds member of all descriptors.

        This makes handling of SourceMds separation in messages less demanding on CPU.
        """
        pm_qnames = self._mdib.data_model.pm_names
        all_mds_descriptors = self._mdib.descriptions.NODETYPE.get(pm_qnames.MdsDescriptor)
        for mds_descriptor in all_mds_descriptors:
            for descr in self._mdib.get_all_descriptors_in_subtree(mds_descriptor):
                descr.set_source_mds(mds_descriptor.Handle)

    def get_mds_descriptor(self, container: AbstractDescriptorProtocol | AbstractStateProtocol) \
            -> AbstractDescriptorProtocol | None:
        """Get the parent mds descriptor for a given descriptor or state."""
        tmp = container
        if tmp.is_state_container:
            tmp = self._mdib.descriptions.handle.get_one(tmp.DescriptorHandle)
        mds_descr = None
        expected_type = self._mdib.data_model.pm_names.MdsDescriptor
        while mds_descr is None:
            if tmp.NODETYPE == expected_type:  # noqa: SIM300
                return tmp
            parent_handle = tmp.parent_handle
            tmp = self._mdib.descriptions.handle.get_one(parent_handle, allow_none=True)
            if tmp is None:
                if self._mdib.current_transaction:
                    tmp = self._mdib.current_transaction.actual_descriptor(parent_handle)
            if tmp is None:
                raise KeyError(f'could not find mds descriptor for handle {container.Handle}')
        return None

    def set_source_mds(self, descriptor_container: AbstractDescriptorProtocol):
        """Find the parent mds for descriptor_container and set source mds of descriptor_container."""
        mds = self.get_mds_descriptor(descriptor_container)
        descriptor_container.set_source_mds(mds.Handle)


class DescriptorFactory:
    """DescriptorFactory provides some methods to make creation of descriptors easier."""

    def __init__(self, mdib: ProviderMdib):
        self._mdib = mdib

    @staticmethod
    def _create_descriptor_container(container_cls: type,
                                     handle: str,
                                     parent_handle: str,
                                     coded_value: CodedValue,
                                     safety_classification: SafetyClassification) -> AbstractDescriptorProtocol:
        obj = container_cls(handle=handle, parent_handle=parent_handle)
        obj.SafetyClassification = safety_classification
        obj.Type = coded_value
        return obj

    def create_vmd_descriptor_container(self,
                                        handle: str,
                                        parent_handle: str,
                                        coded_value: CodedValue,
                                        safety_classification: SafetyClassification) -> AbstractDescriptorProtocol:
        """Create an VmdDescriptorContainer with the given properties.

        :param handle: Handle of the new container
        :param parent_handle: Handle of the parent
        :param coded_value: a pmtypes.CodedValue instance that defines what this object represents in medical terms.
        :param safety_classification: a pmtypes.SafetyClassification value
        :return: the created object
        """
        model = self._mdib.data_model
        cls = model.get_descriptor_container_class(model.pm_names.VmdDescriptor)
        return self._create_descriptor_container(cls, handle, parent_handle, coded_value, safety_classification)

    def create_channel_descriptor_container(self,
                                            handle: str,
                                            parent_handle: str,
                                            coded_value: CodedValue,
                                            safety_classification: SafetyClassification) -> AbstractDescriptorProtocol:
        """Create a ChannelDescriptorContainer with the given properties.

        :param handle: Handle of the new container.
        :param parent_handle: Handle of the parent.
        :param coded_value: a pmtypes.CodedValue instance that defines what this object represents in medical terms.
        :param safety_classification: a pmtypes.SafetyClassification value.
        :return: the created object.
        """
        model = self._mdib.data_model
        cls = model.get_descriptor_container_class(model.pm_names.ChannelDescriptor)
        return self._create_descriptor_container(cls, handle, parent_handle, coded_value, safety_classification)

    def create_string_metric_descriptor_container(self,  # noqa: PLR0913
                                                  handle: str,
                                                  parent_handle: str,
                                                  coded_value: CodedValue,
                                                  safety_classification: SafetyClassification,
                                                  unit: CodedValue,
                                                  metric_availability: MetricAvailability,
                                                  metric_category: MetricCategory) -> AbstractDescriptorProtocol:
        """Create a StringMetricDescriptorContainer with the given properties.

        :param handle: Handle of the new container
        :param parent_handle: Handle of the parent
        :param coded_value: a pmtypes.CodedValue instance that defines what this object represents in medical terms.
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

    def create_enum_string_metric_descriptor_container(self,  # noqa: PLR0913
                                                       handle: str,
                                                       parent_handle: str,
                                                       coded_value: CodedValue,
                                                       safety_classification: SafetyClassification,
                                                       unit: CodedValue,
                                                       allowed_values: Any,
                                                       metric_availability: MetricAvailability,
                                                       metric_category: MetricCategory) -> AbstractDescriptorProtocol:
        """Create an EnumStringMetricDescriptorContainer with the given properties.

        :param handle: Handle of the new container
        :param parent_handle: Handle of the parent
        :param coded_value: a pmtypes.CodedValue instance that defines what this object represents in medical terms.
        :param safety_classification: a pmtypes.SafetyClassification value
        :param unit: pmtypes.CodedValue
        :param allowed_values:
        :param metric_availability: pmtypes.MetricAvailability
        :param metric_category: pmtypes.MetricCategory
        :return: the created object.
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
                                          coded_value: CodedValue,
                                          safety_classification: SafetyClassification) -> AbstractDescriptorProtocol:
        """Create a ClockDescriptorContainer with the given properties.

        :param handle: Handle of the new container
        :param parent_handle: Handle of the parent
        :param coded_value: a pmtypes.CodedValue instance that defines what this object represents in medical terms.
        :param safety_classification: a pmtypes.SafetyClassification value
        :return: the created object.
        """
        model = self._mdib.data_model
        cls = model.get_descriptor_container_class(model.pm_names.ClockDescriptor)
        return self._create_descriptor_container(cls, handle, parent_handle, coded_value, safety_classification)
