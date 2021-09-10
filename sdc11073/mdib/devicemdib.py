import time
import uuid
from collections import defaultdict
from contextlib import contextmanager
from threading import Lock
from typing import List

from . import mdibbase
from .devicewaveform import AbstractWaveformSource
from .devicewaveform import DefaultWaveformSource
from .transactions import RtDataMdibUpdateTransaction, MdibUpdateTransaction, TrItem
from .. import loghelper
from .. import pmtypes
from ..definitions_base import ProtocolsRegistry, BaseDefinitions
from ..definitions_sdc import SDC_v1_Definitions
from ..etc import apply_map
from ..msgtypes import RetrievabilityMethod
from ..namespaces import domTag
from ..pysoap.msgreader import MessageReader

class DeviceMdibContainer(mdibbase.MdibContainer):
    """Device side implementation of an mdib.
     Do not modify containers directly, use transactions for that purpose.
     Transactions keep track of changes and initiate sending of update notifications to clients."""

    def __init__(self, sdc_definitions: [BaseDefinitions, None],
                 log_prefix: [str, None] = None,
                 waveform_source: [AbstractWaveformSource, None] = None):
        """
        :param sdc_definitions: defaults to sdc11073.definitions_sdc.SDC_v1_Definitions
        :param log_prefix: a string
        :param waveform_source: an instance of an object that implements devicewaveform.AbstractWaveformSource
        """
        if sdc_definitions is None:
            sdc_definitions = SDC_v1_Definitions
        super().__init__(sdc_definitions)
        self._logger = loghelper.get_logger_adapter('sdc.device.mdib', log_prefix)
        self._sdc_device = None
        self._tr_lock = Lock()  # transaction lock

        self.sequence_id = uuid.uuid4().urn  # this uuid identifies this mdib instance

        self._current_location = None  # or a SdcLocation instance
        self._annotators = {}
        self._current_transaction = None

        self.pre_commit_handler = None  # pre_commit_handler can modify transaction if needed before it is committed
        self.post_commit_handler = None  # post_commit_handler can modify mdib if needed after it is committed
        self._waveform_source = waveform_source or DefaultWaveformSource()
        self._retrievability_episodic = []  # a list of handles
        self.retrievability_periodic = defaultdict(list)
        self.descriptor_factory = DescriptorFactory(self)

    @contextmanager
    def transaction_manager(self, set_determination_time=True):
        # pylint: disable=protected-access
        with self._tr_lock:
            try:
                self._current_transaction = MdibUpdateTransaction(self)
                yield self._current_transaction
                if callable(self.pre_commit_handler):
                    self.pre_commit_handler(self, self._current_transaction)  # pylint: disable=not-callable
                if self._current_transaction._error:
                    self._logger.info('transaction_manager: transaction without updates!')
                else:
                    self._process_transaction(set_determination_time)
                    if callable(self.post_commit_handler):
                        self.post_commit_handler(self, self._current_transaction)  # pylint: disable=not-callable
            finally:
                self._current_transaction = None

    mdibUpdateTransaction = transaction_manager  # backwards compatibility

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

    def _process_transaction(self, set_determination_time):
        mgr = self._current_transaction
        now = time.time()
        increment_mdib_version = False

        descr_updated = []
        descr_created = []
        descr_deleted = []
        descr_updated_states = []
        metric_updates = []
        alert_updates = []
        comp_updates = []
        ctxt_updates = []
        op_updates = []
        rt_updates = []

        # BICEPS: The version number is incremented by one every time the descriptive part changes
        if len(mgr.descriptor_updates) > 0:
            self.mddescription_version += 1
            increment_mdib_version = True

        # BICEPS: The version number is incremented by one every time the state part changes.
        if sum([len(mgr.metric_state_updates), len(mgr.alert_state_updates),
                len(mgr.component_state_updates), len(mgr.context_state_updates),
                len(mgr.operational_state_updates), len(mgr.rt_sample_state_updates)]
               ) > 0:
            self.mdstate_version += 1
            increment_mdib_version = True

        if increment_mdib_version:
            self.mdib_version += 1

        # handle descriptors
        if len(mgr.descriptor_updates) > 0:
            # need to know all to be deleted and to be created descriptors
            to_be_deleted = [old for old, new in mgr.descriptor_updates.values() if new is None]
            to_be_created = [new for old, new in mgr.descriptor_updates.values() if old is None]
            to_be_deleted_handles = [d.handle for d in to_be_deleted]
            to_be_created_handles = [d.handle for d in to_be_created]
            with self.mdib_lock:

                def _update_corresponding_state(descriptor_container):
                    # add state to updated_states list and to corresponding notifications input
                    # => the state is always sent twice, a) in the description modification report and b)
                    # in the specific state update notification.
                    if descriptor_container.isAlertDescriptor:
                        update_dict = mgr.alert_state_updates
                    elif descriptor_container.isComponentDescriptor:
                        update_dict = mgr.component_state_updates
                    elif descriptor_container.isContextDescriptor:
                        update_dict = mgr.context_state_updates
                    elif descriptor_container.isRealtimeSampleArrayMetricDescriptor:
                        update_dict = mgr.rt_sample_state_updates
                    elif descriptor_container.isMetricDescriptor:
                        update_dict = mgr.metric_state_updates
                    elif descriptor_container.isOperationalDescriptor:
                        update_dict = mgr.operational_state_updates
                    else:
                        raise RuntimeError(f'do not know how to handle {descriptor_container.__class__.__name__}')
                    if descriptor_container.isContextDescriptor:
                        update_dict = mgr.context_state_updates
                        all_context_states = self.context_states.descriptorHandle.get(descriptor_container.handle, [])
                        for context_states in all_context_states:
                            key = (descriptor_container.handle, context_states.handle)
                            # check if state is already present in this transaction
                            state_update = update_dict.get(key)
                            if state_update is not None:
                                # the state has also been updated directly in transaction.
                                # update descriptor version
                                old_state, new_state = state_update
                            else:
                                old_state = context_states
                                new_state = old_state.mk_copy()
                                update_dict[key] = TrItem(old_state, new_state)
                            new_state.descriptor_container = descriptor_container
                            new_state.increment_state_version()
                            new_state.update_descriptor_version()
                            descr_updated_states.append(new_state)
                    else:
                        # check if state is already present in this transaction
                        state_update = update_dict.get(descriptor_container.handle)
                        new_state = None
                        if state_update is not None:
                            # the state has also been updated directly in transaction.
                            # update descriptor version
                            old_state, new_state = state_update
                            if new_state is None:
                                raise ValueError(
                                    f'state deleted? that should not be possible! handle = {descriptor_container.handle}')
                            new_state.descriptor_container = descriptor_container
                            new_state.update_descriptor_version()
                        else:
                            old_state = self.states.descriptorHandle.get_one(descriptor_container.handle,
                                                                             allow_none=True)
                            if old_state is not None:
                                new_state = old_state.mk_copy()
                                new_state.descriptor_container = descriptor_container
                                new_state.increment_state_version()
                                new_state.update_descriptor_version()
                                update_dict[descriptor_container.handle] = TrItem(old_state, new_state)
                        if new_state is not None:
                            descr_updated_states.append(new_state)

                def _increment_parent_descriptor_version(descriptor_container):
                    parent_descriptor_container = self.descriptions.handle.get_one(descriptor_container.parent_handle)
                    parent_descriptor_container.increment_descriptor_version()
                    descr_updated.append(parent_descriptor_container)
                    _update_corresponding_state(parent_descriptor_container)

                # handling only updated states here: If a descriptor is created, I assume that the application also creates the state in an transaction.
                # The state will then be transported via that notification report.
                # Maybe this needs to be reworked, but at the time of this writing it seems fine.
                for tr_item in mgr.descriptor_updates.values():
                    orig_descriptor, new_descriptor = tr_item.old, tr_item.new
                    if new_descriptor is not None:
                        # DescriptionModificationReport also contains the states that are related to the descriptors.
                        # => if there is one, update its DescriptorVersion and add it to list of states that shall be sent
                        # (Assuming that context descriptors (patient, location) are never changed,
                        #  additional check for states in self.context_states is not needed.
                        #  If this assumption is wrong, that functionality must be added!)
                        _update_corresponding_state(new_descriptor)
                    if orig_descriptor is None:
                        # this is a create operation
                        self._logger.debug('transaction_manager: new descriptor Handle={}, DescriptorVersion={}',
                                           new_descriptor.handle, new_descriptor.DescriptorVersion)
                        descr_created.append(new_descriptor.mk_copy())
                        self.descriptions.add_object_no_lock(new_descriptor)
                        # R0033: A SERVICE PROVIDER SHALL increment pm:AbstractDescriptor/@DescriptorVersion by one if a direct child descriptor is added or deleted.
                        if new_descriptor.parent_handle is not None and new_descriptor.parent_handle not in to_be_created_handles:
                            # only update parent if it is not also created in this transaction
                            _increment_parent_descriptor_version(new_descriptor)
                    elif new_descriptor is None:
                        # this is a delete operation
                        self._logger.debug('transaction_manager: rm descriptor Handle={}, DescriptorVersion={}',
                                           orig_descriptor.handle, orig_descriptor.DescriptorVersion)
                        all_descriptors = self.get_all_descriptors_in_subtree(orig_descriptor)
                        self._rm_descriptors_and_states(all_descriptors)
                        descr_deleted.extend(all_descriptors)
                        # R0033: A SERVICE PROVIDER SHALL increment pm:AbstractDescriptor/@DescriptorVersion by one if a direct child descriptor is added or deleted.
                        if orig_descriptor.parent_handle is not None and orig_descriptor.parent_handle not in to_be_deleted_handles:
                            # only update parent if it is not also deleted in this transaction
                            _increment_parent_descriptor_version(orig_descriptor)
                    else:
                        # this is an update operation
                        descr_updated.append(new_descriptor)
                        self._logger.debug('transaction_manager: update descriptor Handle={}, DescriptorVersion={}',
                                           new_descriptor.handle, new_descriptor.DescriptorVersion)
                        self.descriptions.replace_object_no_lock(new_descriptor)

        # handle metric states
        if len(mgr.metric_state_updates) > 0:
            with self.mdib_lock:
                # self.mdib_version += 1
                self._logger.debug('transaction_manager: mdib version={}, metric updates = {}',
                                   self.mdib_version,
                                   mgr.metric_state_updates)
                for value in mgr.metric_state_updates.values():
                    oldstate, newstate = value.old, value.new
                    try:
                        if set_determination_time and newstate.MetricValue is not None:
                            newstate.MetricValue.DeterminationTime = now
                        # replace the old container with the new one
                        self.states.remove_object_no_lock(oldstate)
                        self.states.add_object_no_lock(newstate)
                        metric_updates.append(newstate)
                    except RuntimeError:
                        self._logger.warn('transaction_manager: {} did not exist before!! really??', newstate)
                        raise

        # handle alert states
        if len(mgr.alert_state_updates) > 0:
            with self.mdib_lock:
                self._logger.debug('transaction_manager: alert State updates = {}', mgr.alert_state_updates)
                for value in mgr.alert_state_updates.values():
                    oldstate, newstate = value.old, value.new
                    try:
                        if set_determination_time and newstate.isAlertCondition:
                            newstate.DeterminationTime = time.time()
                        newstate.set_node_member()
                        # replace the old container with the new one
                        self.states.remove_object_no_lock(oldstate)
                        self.states.add_object_no_lock(newstate)
                        alert_updates.append(newstate)
                    except RuntimeError:
                        self._logger.warn('transaction_manager: {} did not exist before!! really??', newstate)
                        raise

            # handle component state states
        if len(mgr.component_state_updates) > 0:
            with self.mdib_lock:
                # self.mdib_version += 1
                self._logger.debug('transaction_manager: component State updates = {}', mgr.component_state_updates)
                for value in mgr.component_state_updates.values():
                    oldstate, newstate = value.old, value.new
                    try:
                        newstate.set_node_member()
                        # replace the old container with the new one
                        self.states.remove_object_no_lock(oldstate)
                        self.states.add_object_no_lock(newstate)
                        comp_updates.append(newstate)
                    except RuntimeError:
                        self._logger.warn('transaction_manager: {} did not exist before!! really??', newstate)
                        raise

        # handle context states
        if len(mgr.context_state_updates) > 0:
            with self.mdib_lock:
                self._logger.debug('transaction_manager: contextState updates = {}', mgr.context_state_updates)
                for value in mgr.context_state_updates.values():
                    oldstate, newstate = value.old, value.new
                    try:
                        ctxt_updates.append(newstate)
                        # replace the old container with the new one
                        self.context_states.remove_object_no_lock(oldstate)
                        self.context_states.add_object_no_lock(newstate)
                        newstate.set_node_member()
                    except RuntimeError:
                        self._logger.warn('transaction_manager: {} did not exist before!! really??', newstate)
                        raise

        # handle operational states
        if len(mgr.operational_state_updates) > 0:
            with self.mdib_lock:
                self._logger.debug('transaction_manager: operationalState updates = {}',
                                   mgr.operational_state_updates)
                for value in mgr.operational_state_updates.values():
                    oldstate, newstate = value.old, value.new
                    try:
                        newstate.set_node_member()
                        self.states.remove_object_no_lock(oldstate)
                        self.states.add_object_no_lock(newstate)
                        op_updates.append(newstate)
                    except RuntimeError:
                        self._logger.warn('transaction_manager: {} did not exist before!! really??', newstate)
                        raise

        # handle real time samples
        # important note: this transaction does not pull values from registered waveform providers!
        # Application is responsible for providing data.
        if len(mgr.rt_sample_state_updates) > 0:
            with self.mdib_lock:
                self._logger.debug('transaction_manager: rtSample updates = {}', mgr.rt_sample_state_updates)
                for value in mgr.rt_sample_state_updates.values():
                    oldstate, newstate = value.old, value.new
                    try:
                        newstate.set_node_member()
                        self.states.remove_object_no_lock(oldstate)
                        self.states.add_object_no_lock(newstate)
                        rt_updates.append(newstate)
                    except RuntimeError:
                        self._logger.warn('transaction_manager: {} did not exist before!! really??', newstate)
                        raise

        mdib_version = self.mdib_version
        if self._sdc_device is not None:
            if len(mgr.descriptor_updates) > 0:
                updated = [d.mk_copy() for d in descr_updated]
                created = [d.mk_copy() for d in descr_created]
                deleted = [d.mk_copy() for d in descr_deleted]
                updated_states = [s.mk_copy() for s in descr_updated_states]
                self._sdc_device.send_descriptor_updates(mdib_version, updated=updated, created=created,
                                                         deleted=deleted,
                                                         states=updated_states)
            if len(metric_updates) > 0:
                updates = [s.mk_copy() for s in metric_updates]
                self._sdc_device.send_metric_state_updates(mdib_version, updates)
            if len(alert_updates) > 0:
                updates = [s.mk_copy() for s in alert_updates]
                self._sdc_device.send_alert_state_updates(mdib_version, updates)
            if len(comp_updates) > 0:
                updates = [s.mk_copy() for s in comp_updates]
                self._sdc_device.send_component_state_updates(mdib_version, updates)
            if len(ctxt_updates) > 0:
                updates = [s.mk_copy() for s in ctxt_updates]
                self._sdc_device.send_context_state_updates(mdib_version, updates)
            if len(op_updates) > 0:
                updates = [s.mk_copy() for s in op_updates]
                self._sdc_device.send_operational_state_updates(mdib_version, updates)
            if len(rt_updates) > 0:
                updates = [s.mk_copy() for s in rt_updates]
                self._sdc_device.send_realtime_samples_state_updates(mdib_version, updates)
        mgr.mdib_version = self.mdib_version

    def _process_internal_rt_transaction(self):
        mgr = self._current_transaction
        # handle real time samples
        if len(mgr.rt_sample_state_updates) > 0:
            self.mdib_version += 1
            updates = []
            self._logger.debug('transaction_manager: rtSample updates = {}', mgr.rt_sample_state_updates)
            for value in mgr.rt_sample_state_updates.values():
                try:
                    value.new.set_node_member()
                    updates.append(value.new)
                except RuntimeError:
                    self._logger.warn('transaction_manager: {} did not exist before!! really??', value.new)
                    raise
            # makes copies of all states for sending, so that they can't be affected by transactions after this one
            updates = [s.mk_copy() for s in updates]
            if self._sdc_device is not None:
                self._sdc_device.send_realtime_samples_state_updates(self.mdib_version, updates)

        mgr.mdib_version = self.mdib_version

    def set_sdc_device(self, sdc_device):
        self._sdc_device = sdc_device

    @property
    def msg_reader(self):
        return None if self._sdc_device is None else self._sdc_device.msg_reader

    def set_location(self, sdc_location, validators=None):
        """
        This method updates only the mdib internal data!
        use the SdcDevice.set_location method if you want to publish the address an the network.
        :param sdc_location: a pysdc.location.SdcLocation instance
        :param validators: a list of pysdc.pmtypes.InstanceIdentifier objects or None
        """
        all_location_contexts = self.context_states.NODETYPE.get(domTag('LocationContextState'), [])
        with self.transaction_manager() as mgr:
            # set all to currently associated Locations to Disassociated
            associated_locations = [l for l in all_location_contexts if
                                    l.ContextAssociation == pmtypes.ContextAssociation.ASSOCIATED]
            for location in associated_locations:
                location_context = mgr.get_state(location.descriptorHandle, location.Handle)
                location_context.ContextAssociation = pmtypes.ContextAssociation.DISASSOCIATED
                # UnbindingMdibVersion is the first version in which it is no longer bound ( == this version)
                location_context.UnbindingMdibVersion = self.mdib_version
            descriptor_container = self.descriptions.NODETYPE.get_one(domTag('LocationContextDescriptor'))

            self._current_location = mgr.get_state(descriptor_container.handle)  # this creates a new location state
            self._current_location.update_from_sdc_location(sdc_location)
            if validators is not None:
                self._current_location.Validator = validators

    def add_descriptor(self, descriptor_container, adjust_state_version=True):
        """Add descriptor to mdib.
        If method is called within an transaction, the created object is added to transaction and clients will be
        notified. Otherwise the object is only added to mdib without sending notifications to clients!
        :param descriptor_container: a descriptor container container instance
        :param adjust_state_version: if True, and an object with the same handle was already in this mdib,
           the descriptor version is set to last version + 1.
        """
        if self._current_transaction is not None:
            self._current_transaction.add_descriptor(descriptor_container, adjust_state_version)
        else:
            self.descriptions.add_object(descriptor_container)
        return descriptor_container

    def add_state(self, state_container, adjust_state_version=True):
        """Add state to mdib.
        If method is called within an transaction, the created object is added to transaction and clients will be
        notified. Otherwise the object is only added to mdib without sending notifications to clients!
        :param state_container: a state container instance
        :param adjust_state_version: if True, and an object with the same handle was already in this mdib,
           the state version is set to last version + 1.
        """
        if self._current_transaction is not None:
            self._current_transaction.add_state(state_container, adjust_state_version)
        else:
            if state_container.isContextState:
                if state_container.Handle in self.context_states.handle:
                    raise ValueError('context state Handle {} already in mdib!'.format(state_container.Handle))
                table = self.context_states
            else:
                if state_container.descriptorHandle in self.states.descriptorHandle:
                    raise ValueError(
                        'state descriptorHandle {} already in mdib!'.format(state_container.descriptorHandle))
                table = self.states
            if adjust_state_version:
                table.set_version(state_container)
            table.add_object(state_container)

    def add_mds_node(self, mds_node):
        """
        This method creates DescriptorContainers and StateContainers from the provided dom tree.
        If it is called within an transaction, the created objects are added to transaction and clients will be notified.
        Otherwise the objects are only added to mdib without sending notifications to clients!
        :param mds_node: a node representing data of a complete mds
        :return: None
        """
        descriptor_containers = self.msg_reader.read_mddescription(mds_node, self)
        apply_map(self.add_descriptor, descriptor_containers)

        state_containers = self.msg_reader.read_mdstate(
            mds_node, self, additional_descriptor_containers=descriptor_containers)
        apply_map(self.add_state, state_containers)
        self.mk_state_containers_for_all_descriptors()

    def ensure_location_context_descriptor(self):
        """Create a LocationContextDescriptor if there is none in mdib."""
        system_context_container = self.descriptions.NODETYPE.get_one(domTag('SystemContextDescriptor'))
        children = self.descriptions.parent_handle.get(system_context_container.handle)
        child_node_types = [ch.NODETYPE for ch in children]
        q_name = domTag('LocationContextDescriptor')
        if q_name not in child_node_types:
            self._logger.info('creating a LocationContextDescriptor')
            descr_cls = self.sdc_definitions.get_descriptor_container_class(q_name)
            descr_container = descr_cls(self.nsmapper,
                                        handle=uuid.uuid4().hex, parent_handle=system_context_container.handle)
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

    def register_annotation_generator(self, annotation: pmtypes.Annotation, trigger_handle: str,
                                      annotated_handles: List[str]):
        self._waveform_source.register_annotation_generator(annotation, trigger_handle, annotated_handles)

    def update_all_rt_samples(self):
        with self._rt_sample_transaction() as transaction:
            self._waveform_source.update_all_realtime_samples(transaction)

    def mk_state_containers_for_all_descriptors(self):
        """The model requires that there is a state for every descriptor (exception: multi-states)
        Call this method to create missing states
        :return:
        """
        for descr in self.descriptions.objects:
            if descr.handle not in self.states.descriptorHandle and descr.handle not in self.context_states.descriptorHandle:
                state_cls = self.get_state_class_for_descriptor(descr)
                if state_cls.isMultiState:
                    pass  # nothing to do, it is allowed to have no state
                else:
                    state = state_cls(self.nsmapper, descr)
                    # add some initial values where needed
                    if state.isAlertCondition:
                        state.DeterminationTime = time.time()
                    elif state.NODETYPE == domTag('AlertSystemState'):
                        state.LastSelfCheck = time.time()
                        state.SelfCheckCount = 1
                    elif state.NODETYPE == domTag('ClockState'):
                        state.LastSet = time.time()
                    state.set_node_member()
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
                            self._retrievability_episodic.append(descr.handle)
                        elif r_by.Method == RetrievabilityMethod.PERIODIC:
                            period_float = r_by.UpdatePeriod
                            period_ms = int(period_float * 1000.0)
                            self.retrievability_periodic[period_ms].append(descr.handle)

    @classmethod
    def from_mdib_file(cls, path,
                       protocol_definition=None,
                       log_prefix=None):
        """
        An alternative constructor for the class
        :param path: the input file path for creating the mdib
        :param protocol_definition: an optional object derived from BaseDefinitions, forces usage of this definition
        :param log_prefix: a string or None
        :return: instance
        """
        with open(path, 'rb') as the_file:
            xml_text = the_file.read()
        return cls.from_string(xml_text,
                               protocol_definition,
                               log_prefix)

    @classmethod
    def from_string(cls, xml_text,
                    protocol_definition=None,
                    log_prefix=None):
        """
        An alternative constructor for the class
        :param xml_text: the input string for creating the mdib
        :param protocol_definition: an optional object derived from BaseDefinitions, forces usage of this definition
        :param log_prefix: a string or None
        :return: instance
        """
        # get protocol definition that matches xml_text
        protocol_definition = None
        if protocol_definition is None:
            for definition_cls in ProtocolsRegistry.protocols:
                if definition_cls.ParticipantModelNamespace is not None and definition_cls.ParticipantModelNamespace.encode(
                        'utf-8') in xml_text:
                    protocol_definition = definition_cls
                    break
        if protocol_definition is None:
            raise ValueError('cannot create instance, no known BICEPS schema version identified')
        msg_reader_cls = MessageReader  # use soap message reader
        mdib = cls(protocol_definition, log_prefix=log_prefix)
        root = msg_reader_cls.get_mdib_root_node(mdib.sdc_definitions, xml_text)
        mdib.biceps_schema.message_schema.assertValid(root)

        mdib.nsmapper.use_doc_prefixes(root.nsmap)
        msg_reader = msg_reader_cls(mdib._logger)
        # first make descriptions and add them to mdib, and then make states (they need already existing descriptions)
        descriptor_containers = msg_reader.read_mddescription(root, mdib)
        mdib.add_description_containers(descriptor_containers)
        state_containers = msg_reader.read_mdstate(root, mdib)
        mdib.add_state_containers(state_containers)
        mdib.mk_state_containers_for_all_descriptors()
        mdib.update_retrievability_lists()
        return mdib


class DescriptorFactory:
    def __init__(self, mdib):
        self._mdib = mdib

    def _create_descriptor_container(self, cls, handle, parent_handle, coded_value, safety_classification):
        obj = cls(nsmapper=self._mdib.nsmapper,
                  handle=handle,
                  parent_handle=parent_handle,
                  )
        obj.SafetyClassification = safety_classification
        obj.Type = coded_value
        return obj

    def create_vmd_descriptor_container(self, handle: str, parent_handle: str, coded_value: pmtypes.CodedValue,
                                        safety_classification: pmtypes.SafetyClassification,
                                        add_to_mdib: bool = True):
        """
        This method creates an VmdDescriptorContainer with the given properties.
        If it is called within an transaction, the created object is added to transaction and clients will be notified.
        Otherwise the object is only added to mdib without sending notifications to clients!
        :param handle: Handle of the new container
        :param parent_handle: Handle of the parent
        :param coded_value: a pmtypes.CodedValue instance that defines what this onject represents in medical terms.
        :param safety_classification: a pmtypes.SafetyClassification value
        :param add_to_mdib:
        :return: the created object
        """
        cls = self._mdib.sdc_definitions.get_descriptor_container_class(domTag('VmdDescriptor'))
        obj = self._create_descriptor_container(cls, handle, parent_handle, coded_value, safety_classification)
        if add_to_mdib:
            self._mdib.add_descriptor(obj)
        return obj

    def create_channel_descriptor_container(self, handle: str, parent_handle: str, coded_value: pmtypes.CodedValue,
                                            safety_classification: pmtypes.SafetyClassification,
                                            add_to_mdib: bool = True):
        """
        This method creates a ChannelDescriptorContainer with the given properties and optionally adds it to the mdib.
        If it is called within an transaction, the created object is added to transaction and clients will be notified.
        Otherwise the object is only added to mdib without sending notifications to clients!
        :param handle: Handle of the new container
        :param parent_handle: Handle of the parent
        :param coded_value: a pmtypes.CodedValue instance that defines what this onject represents in medical terms.
        :param safety_classification: a pmtypes.SafetyClassification value
        :param add_to_mdib:
        :return: the created object
        """
        cls = self._mdib.sdc_definitions.get_descriptor_container_class(domTag('ChannelDescriptor'))
        obj = self._create_descriptor_container(cls, handle, parent_handle, coded_value, safety_classification)
        if add_to_mdib:
            self._mdib.add_descriptor(obj)
        return obj

    def create_string_metric_descriptor_container(
            self, handle: str, parent_handle: str, coded_value: pmtypes.CodedValue,
            safety_classification: pmtypes.SafetyClassification, unit: pmtypes.CodedValue,
            metric_availability: pmtypes.MetricAvailability = pmtypes.MetricAvailability.INTERMITTENT,
            metric_category: pmtypes.MetricCategory = pmtypes.MetricCategory.UNSPECIFIED,
            add_to_mdib: bool = True):
        """
        This method creates a StringMetricDescriptorContainer with the given properties and optionally adds it to the mdib.
        If it is called within an transaction, the created object is added to transaction and clients will be notified.
        Otherwise the object is only added to mdib without sending notifications to clients!
        :param handle: Handle of the new container
        :param parent_handle: Handle of the parent
        :param coded_value: a pmtypes.CodedValue instance that defines what this onject represents in medical terms.
        :param safety_classification: a pmtypes.SafetyClassification value
        :param unit: a CodedValue
        :param metric_availability: pmtypes.MetricAvailability
        :param metric_category: pmtypes.MetricCategory
        :param add_to_mdib:
        :return: the created object
        """
        cls = self._mdib.sdc_definitions.get_descriptor_container_class(domTag('StringMetricDescriptor'))
        obj = self._create_descriptor_container(cls, handle, parent_handle, coded_value, safety_classification)
        obj.Unit = unit
        obj.MetricAvailability = metric_availability
        obj.MetricCategory = metric_category
        if add_to_mdib:
            self._mdib.add_descriptor(obj)
        return obj

    def create_enum_string_metric_descriptor_container(
            self, handle: str, parent_handle: str, coded_value: pmtypes.CodedValue,
            safety_classification: pmtypes.SafetyClassification, unit: pmtypes.CodedValue,
            allowed_values: List[str],
            metric_availability: pmtypes.MetricAvailability = pmtypes.MetricAvailability.INTERMITTENT,
            metric_category: pmtypes.MetricCategory = pmtypes.MetricCategory.UNSPECIFIED,
            add_to_mdib: bool = True):
        """
        This method creates an EnumStringMetricDescriptorContainer with the given properties and optionally adds it
        to the mdib.
        If it is called within an transaction, the created object is added to transaction and clients will be notified.
        Otherwise the object is only added to mdib without sending notifications to clients!
        :param handle: Handle of the new container
        :param parent_handle: Handle of the parent
        :param coded_value: a pmtypes.CodedValue instance that defines what this onject represents in medical terms.
        :param safety_classification: a pmtypes.SafetyClassification value
        :param unit: pmtypes.CodedValue
        :param allowed_values:
        :param metric_availability: pmtypes.MetricAvailability
        :param metric_category: pmtypes.MetricCategory
        :param add_to_mdib:
        :return: the created object
        """
        cls = self._mdib.sdc_definitions.get_descriptor_container_class(domTag('EnumStringMetricDescriptor'))
        obj = self._create_descriptor_container(cls, handle, parent_handle, coded_value, safety_classification)
        obj.Unit = unit
        obj.MetricAvailability = metric_availability
        obj.MetricCategory = metric_category
        obj.AllowedValue = allowed_values
        if add_to_mdib:
            self._mdib.add_descriptor(obj)
        return obj

    def create_clock_descriptor_container(self, handle: str, parent_handle: str, coded_value: pmtypes.CodedValue,
                                          safety_classification: pmtypes.SafetyClassification,
                                          add_to_mdib: bool = True):
        """
        This method creates a ClockDescriptorContainer with the given properties.
        If it is called within an transaction, the created object is added to transaction and clients will be notified.
        Otherwise the object is only added to mdib without sending notifications to clients!
        :param handle: Handle of the new container
        :param parent_handle: Handle of the parent
        :param coded_value: a pmtypes.CodedValue instance that defines what this onject represents in medical terms.
        :param safety_classification: a pmtypes.SafetyClassification value
        :param add_to_mdib:
        :return: the created object
        """
        cls = self._mdib.sdc_definitions.get_descriptor_container_class(domTag('ClockDescriptor'))
        obj = self._create_descriptor_container(cls, handle, parent_handle, coded_value, safety_classification)
        if add_to_mdib:
            self._mdib.add_descriptor(obj)
        return obj
