from __future__ import annotations

import time
import uuid
from collections import defaultdict
from contextlib import contextmanager
from threading import Lock
from typing import List, Type, TYPE_CHECKING, Optional

from . import mdibbase
from .devicewaveform import AbstractWaveformSource
from .devicewaveform import DefaultWaveformSource, AbstractAnnotator
from .transactions import RtDataMdibUpdateTransaction, MdibUpdateTransaction, TransactionProcessor
from .. import loghelper
from .. import pmtypes
from ..definitions_base import ProtocolsRegistry
from ..definitions_sdc import SDC_v1_Definitions
from ..msgtypes import RetrievabilityMethod
from ..namespaces import domTag
from ..pysoap.msgreader import MessageReaderDevice
from ..observableproperties import ObservableProperty

if TYPE_CHECKING:
    from ..definitions_base import BaseDefinitions


class DeviceMdibContainer(mdibbase.MdibContainer):
    """Device side implementation of an mdib.
     Do not modify containers directly, use transactions for that purpose.
     Transactions keep track of changes and initiate sending of update notifications to clients."""
    transaction = ObservableProperty(fire_only_on_changed_value=False)
    rt_updates = ObservableProperty(fire_only_on_changed_value=False) # different observable for performance

    def __init__(self, sdc_definitions: Optional[Type[BaseDefinitions]] = None,
                 log_prefix: Optional[str] = None,
                 waveform_source: Optional[AbstractWaveformSource] = None,
                 transaction_proc_cls: Optional[Type[TransactionProcessor]] = TransactionProcessor):
        """
        :param sdc_definitions: defaults to sdc11073.definitions_sdc.SDC_v1_Definitions
        :param log_prefix: a string
        :param waveform_source: an instance of an object that implements devicewaveform.AbstractWaveformSource
        :param transaction_proc_cls: runs the transaction
        """
        if sdc_definitions is None:
            sdc_definitions = SDC_v1_Definitions
        super().__init__(sdc_definitions)
        self._logger = loghelper.get_logger_adapter('sdc.device.mdib', log_prefix)
        self._tr_lock = Lock()  # transaction lock

        self.sequence_id = uuid.uuid4().urn  # this uuid identifies this mdib instance

        self._current_location = None
        self._annotators = {}
        self._current_transaction = None

        self.pre_commit_handler = None  # pre_commit_handler can modify transaction if needed before it is committed
        self.post_commit_handler = None  # post_commit_handler can modify mdib if needed after it is committed
        self._waveform_source = waveform_source or DefaultWaveformSource()
        self._transaction_proc_cls = transaction_proc_cls
        self._retrievability_episodic = []  # a list of handles
        self.retrievability_periodic = defaultdict(list)
        self.descriptor_factory = DescriptorFactory(self)

    @contextmanager
    def transaction_manager(self, set_determination_time=True):
        # pylint: disable=protected-access
        with self._tr_lock:
            with self.mdib_lock:
                try:
                    self._current_transaction = MdibUpdateTransaction(self)
                    yield self._current_transaction
                    if callable(self.pre_commit_handler):
                        self.pre_commit_handler(self, self._current_transaction)  # pylint: disable=not-callable
                    if self._current_transaction._error:
                        self._logger.info('transaction_manager: transaction without updates!')
                    else:
                        processor = self._transaction_proc_cls(self, self._current_transaction,
                                                               set_determination_time, self._logger)
                        processor.process_transaction()
                        self.transaction = processor
                        self._current_transaction.mdib_version = self.mdib_version

                        if callable(self.post_commit_handler):
                            self.post_commit_handler(self, self._current_transaction)  # pylint: disable=not-callable
                finally:
                    self._current_transaction = None

    @contextmanager
    def _rt_sample_transaction(self):
        with self._tr_lock:
            with self.mdib_lock:
                try:
                    self._current_transaction = RtDataMdibUpdateTransaction(self)
                    yield self._current_transaction
                    if callable(self.pre_commit_handler):
                        self.pre_commit_handler(self, self._current_transaction)  # pylint: disable=not-callable
                    if self._current_transaction.error:
                        self._logger.info('_rtsampleTransaction: transaction without updates!')
                    else:
                        self._process_internal_rt_transaction()
                        if callable(self.post_commit_handler):
                            self.post_commit_handler(self, self._current_transaction)  # pylint: disable=not-callable
                finally:
                    self._current_transaction = None

    def _process_internal_rt_transaction(self):
        mgr = self._current_transaction
        # handle real time samples
        if len(mgr.rt_sample_state_updates) > 0:
            self.mdib_version += 1
            updates = []
            self._logger.debug('transaction_manager: rtSample updates = {}', mgr.rt_sample_state_updates)
            for transaction_item in mgr.rt_sample_state_updates.values():
                updates.append(transaction_item.new)
            # makes copies of all states for sending, so that they can't be affected by transactions after this one
            updates = [s.mk_copy(copy_node=False) for s in updates]
            self.rt_updates = updates
        mgr.mdib_version = self.mdib_version

    def set_location(self, sdc_location, validators=None):
        """
        This method updates only the mdib internal data!
        use the SdcDevice.set_location method if you want to publish the address an the network.
        :param sdc_location: a pysdc.location.SdcLocation instance
        :param validators: a list of pysdc.pmtypes.InstanceIdentifier objects or None
        """
        with self.transaction_manager() as mgr:
            all_location_contexts = self.context_states.NODETYPE.get(domTag('LocationContextState'), [])
            # set all to currently associated Locations to Disassociated
            associated_locations = [l for l in all_location_contexts if
                                    l.ContextAssociation == pmtypes.ContextAssociation.ASSOCIATED]
            for location in associated_locations:
                location_context = mgr.get_context_state(location.DescriptorHandle, location.Handle)
                location_context.ContextAssociation = pmtypes.ContextAssociation.DISASSOCIATED
                # UnbindingMdibVersion is the first version in which it is no longer bound ( == this version)
                location_context.UnbindingMdibVersion = self.mdib_version + 1
            descriptor_container = self.descriptions.NODETYPE.get_one(domTag('LocationContextDescriptor'))

            self._current_location = mgr.mk_context_state(
                descriptor_container.Handle)  # this creates a new location state
            self._current_location.update_from_sdc_location(sdc_location)
            if validators is not None:
                self._current_location.Validator = validators

    def ensure_location_context_descriptor(self):
        """Create a LocationContextDescriptor if there is none in mdib."""
        system_context_container = self.descriptions.NODETYPE.get_one(domTag('SystemContextDescriptor'))
        children = self.descriptions.parent_handle.get(system_context_container.handle)
        child_node_types = [ch.NODETYPE for ch in children]
        q_name = domTag('LocationContextDescriptor')
        if q_name not in child_node_types:
            self._logger.info('creating a LocationContextDescriptor')
            descr_cls = self.sdc_definitions.get_descriptor_container_class(q_name)
            descr_container = descr_cls(handle=uuid.uuid4().hex, parent_handle=system_context_container.handle)
            descr_container.SafetyClassification = pmtypes.SafetyClassification.INF
            self.add_descriptor(descr_container)

    def ensure_patient_context_descriptor(self):
        """Create PatientContextDescriptor if there is none in mdib."""
        system_context_container = self.descriptions.NODETYPE.get_one(domTag('SystemContextDescriptor'))
        children = self.descriptions.parent_handle.get(system_context_container.handle)
        child_node_types = [ch.NODETYPE for ch in children]
        q_name = domTag('PatientContextDescriptor')
        if q_name not in child_node_types:
            self._logger.info('creating a PatientContextDescriptor')
            descr_cls = self.sdc_definitions.get_descriptor_container_class(q_name)
            descr_container = descr_cls(self.nsmapper,
                                        handle=uuid.uuid4().hex, parent_handle=system_context_container.handle)
            descr_container.SafetyClassification = pmtypes.SafetyClassification.INF
            self.add_descriptor(descr_container)

    # real time data handling
    def register_waveform_generator(self, descriptor_handle: str, wf_generator):
        self._waveform_source.register_waveform_generator(self, descriptor_handle, wf_generator)

    def set_waveform_generator_activation_state(self, descriptor_handle: str,
                                                component_activation: pmtypes.ComponentActivation):
        self._waveform_source.set_activation_state(self, descriptor_handle, component_activation)

    def register_annotation_generator(self, annotator: Type[AbstractAnnotator]):
        self._waveform_source.register_annotation_generator(annotator)

    def update_all_rt_samples(self):
        with self._rt_sample_transaction() as transaction:
            self._waveform_source.update_all_realtime_samples(transaction)

    def mk_state_containers_for_all_descriptors(self):
        """The model requires that there is a state for every descriptor (exception: multi-states)
        Call this method to create missing states
        :return:
        """
        for descr in self.descriptions.objects:
            if descr.Handle not in self.states.descriptorHandle and descr.Handle not in self.context_states.descriptorHandle:
                state_cls = self.get_state_class_for_descriptor(descr)
                if state_cls.isMultiState:
                    pass  # nothing to do, it is allowed to have no state
                else:
                    state = state_cls(descr)
                    # add some initial values where needed
                    if state.isAlertCondition:
                        state.DeterminationTime = time.time()
                    elif state.NODETYPE == domTag('AlertSystemState'):
                        state.LastSelfCheck = time.time()
                        state.SelfCheckCount = 1
                    elif state.NODETYPE == domTag('ClockState'):
                        state.LastSet = time.time()
                    if self._current_transaction is not None:
                        self._current_transaction.add_state(state)
                    else:
                        self.states.add_object(state)

    def update_retrievability_lists(self):
        """This method updates internal lists, based on current mdib descriptors. """
        with self.mdib_lock:
            del self._retrievability_episodic[:]
            self.retrievability_periodic.clear()
            for descr in self.descriptions.objects:
                if descr.retrievability is not None:
                    for r_by in descr.retrievability.By:
                        if r_by.Method == RetrievabilityMethod.EPISODIC:
                            self._retrievability_episodic.append(descr.Handle)
                        elif r_by.Method == RetrievabilityMethod.PERIODIC:
                            period_float = r_by.UpdatePeriod
                            period_ms = int(period_float * 1000.0)
                            self.retrievability_periodic[period_ms].append(descr.Handle)

    @classmethod
    def from_mdib_file(cls,
                       path: str,
                       protocol_definition: Optional[Type[BaseDefinitions]] = None,
                       xml_reader_class: Optional[Type[MessageReaderDevice]] = MessageReaderDevice,
                       log_prefix: Optional[str] = None):
        """
        An alternative constructor for the class
        :param path: the input file path for creating the mdib
        :param protocol_definition: an optional object derived from BaseDefinitions, forces usage of this definition
        :param xml_reader_class: class that is used to read mdib xml file
        :param log_prefix: a string or None
        :return: instance
        """
        with open(path, 'rb') as the_file:
            xml_text = the_file.read()
        return cls.from_string(xml_text,
                               protocol_definition,
                               xml_reader_class,
                               log_prefix)

    @classmethod
    def from_string(cls,
                    xml_text: bytes,
                    protocol_definition: Optional[Type[BaseDefinitions]] = None,
                    xml_reader_class: Optional[Type[MessageReaderDevice]] = MessageReaderDevice,
                    log_prefix: Optional[str] = None):
        """
        An alternative constructor for the class
        :param xml_text: the input string for creating the mdib
        :param protocol_definition: an optional object derived from BaseDefinitions, forces usage of this definition
        :param xml_reader_class: class that is used to read mdib xml file
        :param log_prefix: a string or None
        :return: instance
        """
        # get protocol definition that matches xml_text
        if protocol_definition is None:
            for definition_cls in ProtocolsRegistry.protocols:
                if definition_cls.ParticipantModelNamespace is not None and definition_cls.ParticipantModelNamespace.encode(
                        'utf-8') in xml_text:
                    protocol_definition = definition_cls
                    break
        if protocol_definition is None:
            raise ValueError('cannot create instance, no known BICEPS schema version identified')
        # xml_reader_class = protocol_definition.DefaultSdcDeviceComponents.xml_reader_class
        mdib = cls(protocol_definition, log_prefix=log_prefix)

        xml_msg_reader = xml_reader_class(protocol_definition, mdib._logger, log_prefix)
        message_data = xml_msg_reader.read_payload_data(xml_text)
        descriptor_containers, state_containers = xml_msg_reader.read_get_mdib_response(message_data)

        mdib.add_description_containers(descriptor_containers)
        mdib.add_state_containers(state_containers)
        mdib.mk_state_containers_for_all_descriptors()
        mdib.update_retrievability_lists()
        return mdib


class DescriptorFactory:
    def __init__(self, mdib):
        self._mdib = mdib

    @staticmethod
    def _create_descriptor_container(container_cls, handle, parent_handle, coded_value, safety_classification):
        obj = container_cls(handle=handle, parent_handle=parent_handle)
        obj.SafetyClassification = safety_classification
        obj.Type = coded_value
        return obj

    def create_vmd_descriptor_container(self, handle: str, parent_handle: str, coded_value: pmtypes.CodedValue,
                                        safety_classification: pmtypes.SafetyClassification):
        """
        This method creates an VmdDescriptorContainer with the given properties.
        :param handle: Handle of the new container
        :param parent_handle: Handle of the parent
        :param coded_value: a pmtypes.CodedValue instance that defines what this onject represents in medical terms.
        :param safety_classification: a pmtypes.SafetyClassification value
        :return: the created object
        """
        cls = self._mdib.sdc_definitions.get_descriptor_container_class(domTag('VmdDescriptor'))
        return self._create_descriptor_container(cls, handle, parent_handle, coded_value, safety_classification)

    def create_channel_descriptor_container(self, handle: str, parent_handle: str, coded_value: pmtypes.CodedValue,
                                            safety_classification: pmtypes.SafetyClassification):
        """
        This method creates a ChannelDescriptorContainer with the given properties.
        :param handle: Handle of the new container
        :param parent_handle: Handle of the parent
        :param coded_value: a pmtypes.CodedValue instance that defines what this onject represents in medical terms.
        :param safety_classification: a pmtypes.SafetyClassification value
        :return: the created object
        """
        cls = self._mdib.sdc_definitions.get_descriptor_container_class(domTag('ChannelDescriptor'))
        return self._create_descriptor_container(cls, handle, parent_handle, coded_value, safety_classification)

    def create_string_metric_descriptor_container(
            self, handle: str, parent_handle: str, coded_value: pmtypes.CodedValue,
            safety_classification: pmtypes.SafetyClassification, unit: pmtypes.CodedValue,
            metric_availability: pmtypes.MetricAvailability = pmtypes.MetricAvailability.INTERMITTENT,
            metric_category: pmtypes.MetricCategory = pmtypes.MetricCategory.UNSPECIFIED):
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
        cls = self._mdib.sdc_definitions.get_descriptor_container_class(domTag('StringMetricDescriptor'))
        obj = self._create_descriptor_container(cls, handle, parent_handle, coded_value, safety_classification)
        obj.Unit = unit
        obj.MetricAvailability = metric_availability
        obj.MetricCategory = metric_category
        return obj

    def create_enum_string_metric_descriptor_container(
            self, handle: str, parent_handle: str, coded_value: pmtypes.CodedValue,
            safety_classification: pmtypes.SafetyClassification, unit: pmtypes.CodedValue,
            allowed_values: List[str],
            metric_availability: pmtypes.MetricAvailability = pmtypes.MetricAvailability.INTERMITTENT,
            metric_category: pmtypes.MetricCategory = pmtypes.MetricCategory.UNSPECIFIED):
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
        cls = self._mdib.sdc_definitions.get_descriptor_container_class(domTag('EnumStringMetricDescriptor'))
        obj = self._create_descriptor_container(cls, handle, parent_handle, coded_value, safety_classification)
        obj.Unit = unit
        obj.MetricAvailability = metric_availability
        obj.MetricCategory = metric_category
        obj.AllowedValue = allowed_values
        return obj

    def create_clock_descriptor_container(self, handle: str, parent_handle: str, coded_value: pmtypes.CodedValue,
                                          safety_classification: pmtypes.SafetyClassification):
        """
        This method creates a ClockDescriptorContainer with the given properties.
        :param handle: Handle of the new container
        :param parent_handle: Handle of the parent
        :param coded_value: a pmtypes.CodedValue instance that defines what this onject represents in medical terms.
        :param safety_classification: a pmtypes.SafetyClassification value
        :return: the created object
        """
        cls = self._mdib.sdc_definitions.get_descriptor_container_class(domTag('ClockDescriptor'))
        return self._create_descriptor_container(cls, handle, parent_handle, coded_value, safety_classification)
