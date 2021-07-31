from contextlib import contextmanager
import uuid
import time
from threading import Lock
from collections import defaultdict
from . import mdibbase
from ..namespaces import domTag
from .. import loghelper
from .. import pmtypes
from ..definitions_base import ProtocolsRegistry
from .devicewaveform import DefaultWaveformSource
from ..definitions_sdc import SDC_v1_Definitions
from .transactions import RtDataMdibUpdateTransaction, MdibUpdateTransaction, TrItem
from ..msgtypes import RetrievabilityMethod


class DeviceMdibContainer(mdibbase.MdibContainer):
    """Device side implementation of an mdib.
     Do not modify containers directly, use transactions for that purpose.
     Transactions keep track of changes and initiate sending of update notifications to clients."""
    def __init__(self, sdc_definitions, log_prefix=None, waveform_source=None):
        """
        :param sdc_definitions: defaults to sdc11073.definitions_sdc.SDC_v1_Definitions
        :param log_prefix: a string
        :param waveform_source: an instance of an object that implements devicewaveform.AbstractWaveformSource
        """
        if sdc_definitions is None:
            sdc_definitions = SDC_v1_Definitions
        super().__init__(sdc_definitions)
        self._logger = loghelper.get_logger_adapter('sdc.device.mdib', log_prefix)
        self._sdcDevice = None
        self._trLock = Lock() # transaction lock

        self.sequence_id = uuid.uuid4().urn # this uuid identifies this mdib instance
        
        self._currentLocation = None  # or a SdcLocation instance
        self._annotators = {}
        self._current_transaction = None

        self.preCommitHandler = None # preCommitHandler can modify transaction if needed before it is committed
        self.postCommitHandler = None # postCommitHandler can modify mdib if needed after it is committed
        self._waveform_source = waveform_source or DefaultWaveformSource()
        self._msg_reader = None
        self._retrievability_episodic = []  # a list of handles
        self._retrievability_periodic = defaultdict(list)

    @contextmanager
    def mdibUpdateTransaction(self, setDeterminationTime=True):
        #pylint: disable=protected-access
        with self._trLock:
            try:
                self._current_transaction = MdibUpdateTransaction(self)
                yield self._current_transaction
                if callable(self.preCommitHandler):
                    self.preCommitHandler(self, self._current_transaction) #pylint: disable=not-callable
                if self._current_transaction._error:
                    self._logger.info('mdibUpdateTransaction: transaction without updates!')
                else:
                    self._process_transaction(setDeterminationTime)
                    if callable(self.postCommitHandler):
                        self.postCommitHandler(self, self._current_transaction)  #pylint: disable=not-callable
            finally:
                self._current_transaction = None

    @contextmanager
    def _rt_sample_transaction(self):
        with self._trLock:
            with self.mdib_lock:
                try:
                    self._current_transaction = RtDataMdibUpdateTransaction(self)
                    yield self._current_transaction
                    if callable(self.preCommitHandler):
                        self.preCommitHandler(self, self._current_transaction) #pylint: disable=not-callable
                    if self._current_transaction._error:
                        self._logger.info('_rtsampleTransaction: transaction without updates!')
                    else:
                        self._process_internal_rt_transaction()
                        if callable(self.postCommitHandler):
                            self.postCommitHandler(self, self._current_transaction)  #pylint: disable=not-callable
                finally:
                    self._current_transaction = None

    def _process_transaction(self, setDeterminationTime):
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
        if len(mgr.metric_state_updates) > 0 or len(mgr.alert_state_updates) > 0 \
                or len(mgr.component_state_updates) > 0 or len(mgr.context_state_updates) > 0 \
                or len(mgr.operational_state_updates) > 0 or len(mgr.rt_sample_state_updates) > 0:
            self.mdstate_version += 1
            increment_mdib_version = True

        if increment_mdib_version:
            self.mdib_version += 1

        # handle descriptors
        if len(mgr.descriptor_updates) > 0:
            #need to know all to be deleted and to be created descriptors
            to_be_deleted = [old for old, new in mgr.descriptor_updates.values() if new is None]
            to_be_created = [new for old, new in mgr.descriptor_updates.values() if old is None]
            to_be_deleted_handles = [d.handle for d in to_be_deleted]
            to_be_created_handles = [d.handle for d in to_be_created]
            with self.mdib_lock:

                def _updateCorrespondingState(descriptorContainer):
                    # add state to updated_states list and to corresponding notifications input
                    # => the state is always sent twice, a) in the description modification report and b)
                    # in the specific state update notification.
                    if descriptorContainer.isAlertDescriptor:
                        update_dict = mgr.alert_state_updates
                    elif descriptorContainer.isComponentDescriptor:
                        update_dict = mgr.component_state_updates
                    elif descriptorContainer.isContextDescriptor:
                        update_dict = mgr.context_state_updates
                    elif descriptorContainer.isRealtimeSampleArrayMetricDescriptor:
                        update_dict = mgr.rt_sample_state_updates
                    elif descriptorContainer.isMetricDescriptor:
                        update_dict = mgr.metric_state_updates
                    elif descriptorContainer.isOperationalDescriptor:
                        update_dict = mgr.operational_state_updates
                    else:
                        raise RuntimeError(f'do not know how to handle {descriptorContainer.__class__.__name__}')
                    if descriptorContainer.isContextDescriptor:
                        update_dict = mgr.context_state_updates
                        all_states = self.context_states.descriptorHandle.get(descriptorContainer.handle, [])
                        for st in all_states:
                            key = (descriptorContainer.handle, st.handle)
                            # check if state is already present in this transaction
                            state_update = update_dict.get(key)
                            if state_update is not None:
                                # the state has also been updated directly in transaction.
                                # update descriptor version
                                old_state, new_state = state_update
                            else:
                                old_state = st
                                new_state = old_state.mk_copy()
                                update_dict[key] = TrItem(old_state, new_state)
                            new_state.descriptorContainer = descriptorContainer
                            new_state.increment_state_version()
                            new_state.update_descriptor_version()
                            descr_updated_states.append(new_state)
                    else:
                        # check if state is already present in this transaction
                        state_update = update_dict.get(descriptorContainer.handle)
                        new_state = None
                        if state_update is not None:
                            # the state has also been updated directly in transaction.
                            # update descriptor version
                            old_state, new_state = state_update
                            if new_state is None:
                                raise ValueError(f'state deleted? that should not be possible! handle = {descriptorContainer.handle}')
                            new_state.descriptorContainer = descriptorContainer
                            new_state.update_descriptor_version()
                        else:
                            old_state = self.states.descriptorHandle.getOne(descriptorContainer.handle, allowNone=True)
                            if old_state is not None:
                                new_state = old_state.mk_copy()
                                new_state.descriptorContainer = descriptorContainer
                                new_state.increment_state_version()
                                new_state.update_descriptor_version()
                                update_dict[descriptorContainer.handle] = TrItem(old_state, new_state)
                        if new_state is not None:
                            descr_updated_states.append(new_state)

                def _incrementParentDescriptorVersion(descriptorContainer):
                    parentDescriptorContainer = self.descriptions.handle.getOne(descriptorContainer.parent_handle)
                    parentDescriptorContainer.increment_descriptor_version()
                    descr_updated.append(parentDescriptorContainer)
                    _updateCorrespondingState(parentDescriptorContainer)

                # handling only updated states here: If a descriptor is created, I assume that the application also creates the state in an transaction.
                # The state will then be transported via that notification report.
                # Maybe this needs to be reworked, but at the time of this writing it seems fine.
                for tr_item in mgr.descriptor_updates.values():
                    origDescriptor, newDescriptor = tr_item.old, tr_item.new
                    if newDescriptor is not None:
                        # DescriptionModificationReport also contains the states that are related to the descriptors.
                        # => if there is one, update its DescriptorVersion and add it to list of states that shall be sent
                        # (Assuming that context descriptors (patient, location) are never changed,
                        #  additional check for states in self.context_states is not needed.
                        #  If this assumption is wrong, that functionality must be added!)
                        _updateCorrespondingState(newDescriptor)
                    if origDescriptor is None:
                        # this is a create operation
                        self._logger.debug('mdibUpdateTransaction: new descriptor Handle={}, DescriptorVersion={}', newDescriptor.handle, newDescriptor.DescriptorVersion)
                        descr_created.append(newDescriptor.mk_copy())
                        self.descriptions.add_object_no_lock(newDescriptor)
                        # R0033: A SERVICE PROVIDER SHALL increment pm:AbstractDescriptor/@DescriptorVersion by one if a direct child descriptor is added or deleted.
                        if newDescriptor.parent_handle is not None and  newDescriptor.parent_handle not in to_be_created_handles:
                            # only update parent if it is not also created in this transaction
                            _incrementParentDescriptorVersion(newDescriptor)
                    elif newDescriptor is None:
                        # this is a delete operation
                        self._logger.debug('mdibUpdateTransaction: rm descriptor Handle={}, DescriptorVersion={}', origDescriptor.handle, origDescriptor.DescriptorVersion)
                        all_descriptors = self.get_all_descriptors_in_subtree(origDescriptor)
                        self._rm_descriptors_and_states(all_descriptors)
                        descr_deleted.extend(all_descriptors)
                        # R0033: A SERVICE PROVIDER SHALL increment pm:AbstractDescriptor/@DescriptorVersion by one if a direct child descriptor is added or deleted.
                        if origDescriptor.parent_handle is not None and  origDescriptor.parent_handle not in to_be_deleted_handles:
                            # only update parent if it is not also deleted in this transaction
                            _incrementParentDescriptorVersion(origDescriptor)
                    else:
                        # this is an update operation
                        descr_updated.append(newDescriptor)
                        self._logger.debug('mdibUpdateTransaction: update descriptor Handle={}, DescriptorVersion={}', newDescriptor.handle, newDescriptor.DescriptorVersion)
                        self.descriptions.replace_object_no_lock(newDescriptor)

        # handle metric states
        if len(mgr.metric_state_updates) > 0:
            with self.mdib_lock:
                # self.mdib_version += 1
                self._logger.debug('mdibUpdateTransaction: mdib version={}, metric updates = {}',
                                   self.mdib_version,
                                   mgr.metric_state_updates)
                for value in mgr.metric_state_updates.values():
                    oldstate, newstate = value.old, value.new
                    try:
                        if setDeterminationTime and newstate.metricValue is not None:
                            newstate.metricValue.DeterminationTime = now
                        # replace the old container with the new one
                        self.states.remove_object_no_lock(oldstate)
                        self.states.add_object_no_lock(newstate)
                        metric_updates.append(newstate)
                    except RuntimeError:
                        self._logger.warn('mdibUpdateTransaction: {} did not exist before!! really??', newstate)
                        raise

        # handle alert states
        if len(mgr.alert_state_updates) > 0:
            with self.mdib_lock:
                self._logger.debug('mdibUpdateTransaction: alert State updates = {}', mgr.alert_state_updates)
                for value in mgr.alert_state_updates.values():
                    oldstate, newstate = value.old, value.new
                    try:
                        if setDeterminationTime and newstate.isAlertCondition:
                            newstate.DeterminationTime = time.time()
                        newstate.set_node_member()
                        # replace the old container with the new one
                        self.states.remove_object_no_lock(oldstate)
                        self.states.add_object_no_lock(newstate)
                        alert_updates.append(newstate)
                    except RuntimeError:
                        self._logger.warn('mdibUpdateTransaction: {} did not exist before!! really??', newstate)
                        raise

            # handle component state states
        if len(mgr.component_state_updates) > 0:
            with self.mdib_lock:
                # self.mdib_version += 1
                self._logger.debug('mdibUpdateTransaction: component State updates = {}', mgr.component_state_updates)
                for value in mgr.component_state_updates.values():
                    oldstate, newstate = value.old, value.new
                    try:
                        newstate.set_node_member()
                        # replace the old container with the new one
                        self.states.remove_object_no_lock(oldstate)
                        self.states.add_object_no_lock(newstate)
                        comp_updates.append(newstate)
                    except RuntimeError:
                        self._logger.warn('mdibUpdateTransaction: {} did not exist before!! really??', newstate)
                        raise

        # handle context states
        if len(mgr.context_state_updates) > 0:
            with self.mdib_lock:
                self._logger.debug('mdibUpdateTransaction: contextState updates = {}', mgr.context_state_updates)
                for value in mgr.context_state_updates.values():
                    oldstate, newstate = value.old, value.new
                    try:
                        ctxt_updates.append(newstate)
                        # replace the old container with the new one
                        self.context_states.remove_object_no_lock(oldstate)
                        self.context_states.add_object_no_lock(newstate)
                        newstate.set_node_member()
                    except RuntimeError:
                        self._logger.warn('mdibUpdateTransaction: {} did not exist before!! really??', newstate)
                        raise

        # handle operational states
        if len(mgr.operational_state_updates) > 0:
            with self.mdib_lock:
                self._logger.debug('mdibUpdateTransaction: operationalState updates = {}', mgr.operational_state_updates)
                for value in mgr.operational_state_updates.values():
                    oldstate, newstate = value.old, value.new
                    try:
                        newstate.set_node_member()
                        self.states.remove_object_no_lock(oldstate)
                        self.states.add_object_no_lock(newstate)
                        op_updates.append(newstate)
                    except RuntimeError:
                        self._logger.warn('mdibUpdateTransaction: {} did not exist before!! really??', newstate)
                        raise

        # handle real time samples
        # important note: this transaction does not pull values from registered waveform providers!
        # Application is responsible for providing data.
        if len(mgr.rt_sample_state_updates) > 0:
            with self.mdib_lock:
                self._logger.debug('mdibUpdateTransaction: rtSample updates = {}', mgr.rt_sample_state_updates)
                for value in mgr.rt_sample_state_updates.values():
                    oldstate, newstate = value.old, value.new
                    try:
                        newstate.set_node_member()
                        self.states.remove_object_no_lock(oldstate)
                        self.states.add_object_no_lock(newstate)
                        rt_updates.append(newstate)
                    except RuntimeError:
                        self._logger.warn('mdibUpdateTransaction: {} did not exist before!! really??', newstate)
                        raise
                    except:
                        raise

        mdib_version = self.mdib_version
        if self._sdcDevice is not None:
            if len(mgr.descriptor_updates) > 0:
                updated = [d.mk_copy() for d in descr_updated]
                created = [d.mk_copy() for d in descr_created]
                deleted = [d.mk_copy() for d in descr_deleted]
                updated_states = [s.mk_copy() for s in descr_updated_states]
                self._sdcDevice.sendDescriptorUpdates(mdib_version, updated=updated, created=created, deleted=deleted, updated_states=updated_states)
            if len(metric_updates) > 0:
                updates = [s.mk_copy() for s in metric_updates]
                self._sdcDevice.sendMetricStateUpdates(mdib_version, updates)
            if len(alert_updates) > 0:
                updates = [s.mk_copy() for s in alert_updates]
                self._sdcDevice.sendAlertStateUpdates(mdib_version, updates)
            if len(comp_updates) > 0:
                updates = [s.mk_copy() for s in comp_updates]
                self._sdcDevice.sendComponentStateUpdates(mdib_version, updates)
            if len(ctxt_updates) > 0:
                updates = [s.mk_copy() for s in ctxt_updates]
                self._sdcDevice.sendContextStateUpdates(mdib_version, updates)
            if len(op_updates) > 0:
                updates = [s.mk_copy() for s in op_updates]
                self._sdcDevice.sendOperationalStateUpdates(mdib_version, updates)
            if len(rt_updates) > 0:
                updates = [s.mk_copy() for s in rt_updates]
                self._sdcDevice.sendRealtimeSamplesStateUpdates(mdib_version, updates)
        mgr.mdib_version = self.mdib_version

    def _process_internal_rt_transaction(self):
        mgr = self._current_transaction
        # handle real time samples
        if len(mgr.rt_sample_state_updates) > 0:
            self.mdib_version += 1
            updates = []
            self._logger.debug('mdibUpdateTransaction: rtSample updates = {}', mgr.rt_sample_state_updates)
            for value in mgr.rt_sample_state_updates.values():
                oldstate, newstate = value.old, value.new
                try:
                    newstate.set_node_member()
                    updates.append(newstate)
                except RuntimeError:
                    self._logger.warn('mdibUpdateTransaction: {} did not exist before!! really??', newstate)
                    raise
            # makes copies of all states for sending, so that they can't be affected by transactions after this one
            updates = [s.mk_copy() for s in updates]
            if self._sdcDevice is not None:
                self._sdcDevice.sendRealtimeSamplesStateUpdates(self.mdib_version, updates)

        mgr.mdib_version = self.mdib_version

    def setSdcDevice(self, sdcDevice):
        self._sdcDevice = sdcDevice
        self._msg_reader = sdcDevice.msg_reader

    def setLocation(self, sdcLocation, validators=None):
        """
        This method updates only the mdib internal data!
        use the SdcDevice.setLocation method if you want to publish the address an the network.
        @param sdcLocation: a pysdc.location.SdcLocation instance
        @param validators: a list of pysdc.pmtypes.InstanceIdentifier objects or None
        """
        allLocationContexts = self.context_states.NODETYPE.get(domTag('LocationContextState'), [])
        with self.mdibUpdateTransaction() as mgr:
            # set all to currently associated Locations to Disassociated
            associatedLocations = [l for l in allLocationContexts if l.ContextAssociation == pmtypes.ContextAssociation.ASSOCIATED]
            for l in associatedLocations:
                locationContext = mgr.get_state(l.descriptorHandle, l.Handle)
                locationContext.ContextAssociation = pmtypes.ContextAssociation.DISASSOCIATED
                # UnbindingMdibVersion is the first version in which it is no longer bound ( == this version)
                locationContext.UnbindingMdibVersion = self.mdib_version
            descriptorContainer = self.descriptions.NODETYPE.getOne(domTag('LocationContextDescriptor'))
                    
            self._currentLocation = mgr.get_state(descriptorContainer.handle) # this creates a new location state
            self._currentLocation.update_from_sdc_location(sdcLocation)
            if validators is not None:
                self._currentLocation.Validator = validators

    def _createDescriptorContainer(self, cls, handle, parent_handle, coded_value, safety_classification):
        obj = cls(nsmapper=self.nsmapper, 
                  handle=handle,
                  parent_handle=parent_handle,
                  )
        obj.SafetyClassification = safety_classification
        obj.Type = coded_value
        return obj

    def createVmdDescriptorContainer(self, handle, parent_handle, coded_value, safetyClassification):
        """
        This method creates an VmdDescriptorContainer with the given properties.
        If it is called within an transaction, the created object is added to transaction and clients will be notified.
        Otherwise the object is only added to mdib without sending notifications to clients!
        :param handle: Handle of the new container
        :param parent_handle: Handle of the parent
        :param coded_value: a pmtypes.CodedValue instance that defines what this onject represents in medical terms.
        :param safetyClassification: a pmtypes.SafetyClassification value
        :return: the created object
        """
        qNameTag = domTag('Vmd')
        cls = self.get_descriptor_container_class(qNameTag)
        obj = self._createDescriptorContainer(cls, handle, parent_handle, coded_value, safetyClassification)
        if self._current_transaction is not None:
            self._current_transaction.add_descriptor(obj)
        else:
            self.descriptions.add_object(obj)
        return obj

    def createChannelDescriptorContainer(self, handle, parent_handle, codedValue, safetyClassification):
        """
        This method creates a ChannelDescriptorContainer with the given properties and optionally adds it to the mdib.
        If it is called within an transaction, the created object is added to transaction and clients will be notified.
        Otherwise the object is only added to mdib without sending notifications to clients!
        :param handle: Handle of the new container
        :param parent_handle: Handle of the parent
        :param codedValue: a pmtypes.CodedValue instance that defines what this onject represents in medical terms.
        :param safetyClassification: a pmtypes.SafetyClassification value
        :return: the created object
        """
        qNameTag = domTag('Channel')
        cls = self.get_descriptor_container_class(qNameTag)
        obj = self._createDescriptorContainer(cls, handle, parent_handle, codedValue, safetyClassification)
        if self._current_transaction is not None:
            self._current_transaction.add_descriptor(obj)
        else:
            self.descriptions.add_object(obj)
        return obj

    def createStringMetricDescriptorContainer(self, handle, parent_handle, codedValue, safetyClassification, unit,
                                              metricAvailability=pmtypes.MetricAvailability.INTERMITTENT,
                                              metricCategory=pmtypes.MetricCategory.UNSPECIFIED):
        """
        This method creates a StringMetricDescriptorContainer with the given properties and optionally adds it to the mdib.
        If it is called within an transaction, the created object is added to transaction and clients will be notified.
        Otherwise the object is only added to mdib without sending notifications to clients!
        :param handle: Handle of the new container
        :param parent_handle: Handle of the parent
        :param codedValue: a pmtypes.CodedValue instance that defines what this onject represents in medical terms.
        :param safetyClassification: a pmtypes.SafetyClassification value
        :param unit: a CodedValue
        :param metricAvailability: pmtypes.MetricAvailability
        :param metricCategory: pmtypes.MetricCategory
        :return: the created object
        """
        qNameTag = domTag('Metric')
        qNameType = domTag('StringMetricDescriptor')
        cls = self.get_descriptor_container_class(qNameType)
        obj = self._createDescriptorContainer(cls, handle, parent_handle, codedValue, safetyClassification)
        obj.Unit = unit
        obj.MetricAvailability = metricAvailability
        obj.MetricCategory = metricCategory
        if self._current_transaction is not None:
            self._current_transaction.add_descriptor(obj)
        else:
            self.descriptions.add_object(obj)
        return obj

    def createEnumStringMetricDescriptorContainer(self, handle, parent_handle, codedValue, safetyClassification,
                                                  unit,  allowedValues,
                                                  metricAvailability=pmtypes.MetricAvailability.INTERMITTENT,
                                                  metricCategory=pmtypes.MetricCategory.UNSPECIFIED):
        """
        This method creates an EnumStringMetricDescriptorContainer with the given properties and optionally adds it
        to the mdib.
        If it is called within an transaction, the created object is added to transaction and clients will be notified.
        Otherwise the object is only added to mdib without sending notifications to clients!
        :param handle: Handle of the new container
        :param parent_handle: Handle of the parent
        :param codedValue: a pmtypes.CodedValue instance that defines what this onject represents in medical terms.
        :param safetyClassification: a pmtypes.SafetyClassification value
        :param unit: pmtypes.CodedValue
        :param allowedValues:
        :param metricAvailability: pmtypes.MetricAvailability
        :param metricCategory: pmtypes.MetricCategory
        :return: the created object
        """
        qNameTag = domTag('Metric')
        qNameType = domTag('EnumStringMetricDescriptor')
        cls = self.get_descriptor_container_class(qNameType)
        obj = self._createDescriptorContainer(cls, handle, parent_handle, codedValue, safetyClassification)
        obj.Unit = unit
        obj.MetricAvailability = metricAvailability
        obj.MetricCategory = metricCategory
        obj.AllowedValue = allowedValues
        if self._current_transaction is not None:
            self._current_transaction.add_descriptor(obj)
        else:
            self.descriptions.add_object(obj)
        return obj

    def createClockDescriptorContainer(self, handle, parent_handle, coded_value, safety_classification):
        """
        This method creates a ClockDescriptorContainer with the given properties.
        If it is called within an transaction, the created object is added to transaction and clients will be notified.
        Otherwise the object is only added to mdib without sending notifications to clients!
        :param handle: Handle of the new container
        :param parent_handle: Handle of the parent
        :param codedValue: a pmtypes.CodedValue instance that defines what this onject represents in medical terms.
        :param safetyClassification: a pmtypes.SafetyClassification value
        :return: the created object
        """
        cls = self.get_descriptor_container_class(domTag('ClockDescriptor'))
        obj = self._createDescriptorContainer(cls, handle, parent_handle, coded_value, safety_classification)
        if self._current_transaction is not None:
            self._current_transaction.add_descriptor(obj)
        else:
            self.descriptions.add_object(obj)
        return obj

    def addState(self, stateContainer, adjustStateVersion=True):
        """Add state to mdib.
        If method is called within an transaction, the created object is added to transaction and clients will be
        notified. Otherwise the object is only added to mdib without sending notifications to clients!
        :param stateContainer: a state container instance
        :param adjustStateVersion: if True, and an object with the same handle was already in this mdib,
           the state version is set to last version + 1.
        """
        if self._current_transaction is not None:
            self._current_transaction.add_state(stateContainer, adjustStateVersion)
        else:
            if stateContainer.isContextState:
                if stateContainer.Handle in self.context_states.handle:
                    raise ValueError('context state Handle {} already in mdib!'.format(stateContainer.Handle))
                table = self.context_states
            else:
                if stateContainer.descriptorHandle in self.states.descriptorHandle:
                    raise ValueError('state descriptorHandle {} already in mdib!'.format(stateContainer.descriptorHandle))
                table = self.states
            if adjustStateVersion:
                table.set_version(stateContainer)
            table.add_object(stateContainer)

    def addMdsNode(self, mdsNode):
        """
        This method creates DescriptorContainers and StateContainers from the provided dom tree.
        If it is called within an transaction, the created objects are added to transaction and clients will be notified.
        Otherwise the objects are only added to mdib without sending notifications to clients!
        :param mdsNode: a node representing data of a complete mds
        :return: None
        """
        descriptorContainers = self._msg_reader.read_mddescription(mdsNode, self)
        if self._current_transaction is not None:
            for descr in descriptorContainers:
                self._current_transaction.add_descriptor(descr)
        else:
            for descr in descriptorContainers:
                self.descriptions.add_object(descr)

        stateContainers = self._msg_reader.read_mdstate(
            mdsNode, self, additional_descriptor_containers=descriptorContainers)
        for s in stateContainers:
            self.addState(s)
        self.mkStateContainersforAllDescriptors()

    # real time data handling
    def register_waveform_generator(self, descriptor_handle, wf_generator):
        self._waveform_source.register_waveform_generator(self, descriptor_handle, wf_generator)

    def setWaveformGeneratorActivationState(self, descriptorHandle, componentActivation):
        self._waveform_source.set_activation_state(self, descriptorHandle, componentActivation)

    def registerAnnotationGenerator(self, annotator, triggerHandle, annotatedHandles):
        self._waveform_source.register_annotation_generator(annotator, triggerHandle, annotatedHandles)

    def update_all_rt_samples(self):
        with self._rt_sample_transaction() as tr:
            self._waveform_source.update_all_realtime_samples(tr)

    def mkStateContainersforAllDescriptors(self):
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
                    st = state_cls(self.nsmapper, descr)
                    # add some initial values where needed
                    if st.isAlertCondition:
                        st.DeterminationTime = time.time()
                    elif st.NODETYPE == domTag('AlertSystemState'):
                        st.LastSelfCheck = time.time()
                        st.SelfCheckCount = 1
                    elif st.NODETYPE == domTag('ClockState'):
                        st.LastSet = time.time()
                    st.set_node_member()
                    if self._current_transaction is not None:
                        self._current_transaction.add_state(st)
                    else:
                        self.states.add_object(st)

    def update_retrievability_lists(self):
        """This method updates internal lists, based on current mdib descriptors. """
        del self._retrievability_episodic[:]
        self._retrievability_periodic.clear()
        for d in self.descriptions.objects:
            r = d.retrievability
            if r is None:
                return
            for ri in r.By:
                if ri.Method == RetrievabilityMethod.EPISODIC:
                    self._retrievability_episodic.append(d.handle)
                elif ri.Method == RetrievabilityMethod.PERIODIC:
                    period_float = ri.UpdatePeriod
                    period_ms = int(period_float*1000.0)
                    self._retrievability_periodic[period_ms].append(d.handle)


    @classmethod
    def fromMdibFile(cls, path, createLocationContextDescr=True, createPatientContextDescr=True,
                     protocol_definition=None, log_prefix=None):
        """
        An alternative constructor for the class
        :param path: the input file path for creating the mdib
        :param createLocationContextDescr: same as in fromString method
        :param createPatientContextDescr: same as in fromString method
        :param protocol_definition: an optional object derived from BaseDefinitions, forces usage of this definition
        :param log_prefix: a string or None
        :return: instance
        """
        with open(path, 'rb') as f:
            xml_text = f.read()
        return DeviceMdibContainer.fromString(xml_text, createLocationContextDescr, createPatientContextDescr,
                                              protocol_definition, log_prefix)


    @classmethod
    def fromString(cls, xml_text, createLocationContextDescr=True, createPatientContextDescr=True,
                   protocol_definition=None, log_prefix=None):
        """
        An alternative constructor for the class
        :param xml_text: the input string for creating the mdib
        :param createLocationContextDescr: if True, and the mdib does not contain a LocationContextDescriptor, it adds one
        :param createPatientContextDescr: if True, and the mdib does not contain a PatientContextDescriptor, it adds one
        :param protocol_definition: an optional object derived from BaseDefinitions, forces usage of this definition
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
        MsgReaderCls = protocol_definition.DefaultSdcDeviceComponents['MsgReaderClass']
        mdib = cls(protocol_definition, log_prefix=log_prefix)
        root =  MsgReaderCls.get_mdib_root_node(mdib.sdc_definitions, xml_text)
        mdib.biceps_schema.message_schema.assertValid(root)

        mdib.nsmapper.use_doc_prefixes(root.nsmap)
        msg_reader = MsgReaderCls(mdib._logger)
        # first make descriptions and add them to mdib, and then make states (they need already existing descriptions)
        descriptorContainers = msg_reader.read_mddescription(root, mdib)
        mdib.add_description_containers(descriptorContainers)
        stateContainers = msg_reader.read_mdstate(root, mdib)
        mdib.add_state_containers(stateContainers)

        if createLocationContextDescr or createPatientContextDescr:
            # make sure we have exactly one PatientContext and one LocationContext Descriptor, depending of flags
            systemContextContainer = mdib.descriptions.NODETYPE.getOne(domTag('SystemContextDescriptor'))
            children = mdib.descriptions.parent_handle.get(systemContextContainer.handle)
            childdrenNodeNames = [ch.NODETYPE for ch in children]
            if createLocationContextDescr:
                qn = domTag('LocationContextDescriptor')
                if qn not in childdrenNodeNames:
                    mdib._logger.info('creating a LocationContextDescriptor')
                    descr_cls = mdib.get_descriptor_container_class(qn)
                    lc = descr_cls(mdib.nsmapper,
                                   handle=uuid.uuid4().hex, parent_handle=systemContextContainer.handle)
                    lc.SafetyClassification = pmtypes.SafetyClassification.INF
                    mdib.descriptions.add_object(lc)
            if createPatientContextDescr:
                qn = domTag('PatientContextDescriptor')
                if qn not in childdrenNodeNames:
                    mdib._logger.info('creating a PatientContextDescriptor')
                    descr_cls = mdib.get_descriptor_container_class(qn)
                    pc = descr_cls(mdib.nsmapper,
                                   handle=uuid.uuid4().hex, parent_handle=systemContextContainer.handle)
                    pc.SafetyClassification = pmtypes.SafetyClassification.INF
                    mdib.descriptions.add_object(pc)
        mdib.mkStateContainersforAllDescriptors()
        mdib.update_retrievability_lists()
        return mdib
