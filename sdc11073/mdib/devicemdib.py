from contextlib import contextmanager
import uuid
import time
from collections import OrderedDict, namedtuple
from threading import Lock
from . import mdibbase
from . import msgreader
from .. import xmlparsing
from ..namespaces import domTag
from .. import loghelper
from .. import pmtypes
from ..definitions_base import ProtocolsRegistry

class SampleArraySource(object):
    def __init__(self, descriptorHandle, generator):
        self._descriptorHandle = descriptorHandle 
        self._generator = generator 
        self._lastTimeStamp = None
        self._activationState = pmtypes.ComponentActivation.ON # 'One of pmtypes.ComponentActivation values ('On', 'NotRdy, 'Standby', 'Off', 'Shtdn', 'Fail')


    def setActivationState(self, componentActivation):
        '''
        @param componentActivation: one of pmtypes.ComponentActivation values
        '''   
        self._activationState =  componentActivation    
        if componentActivation == pmtypes.ComponentActivation.ON:
            self._lastTimeStamp = time.time()
        
            
    def getNextSampleArray(self): 
        if self._activationState != pmtypes.ComponentActivation.ON:
            return (None, self._generator.sampleperiod, [], self._activationState)
        
        now = time.time()
        observationTime = self._lastTimeStamp or now
        samples_count = int((now - observationTime)/self._generator.sampleperiod)
        samples = self._generator.nextSamples(samples_count)
        self._lastTimeStamp = observationTime + self._generator.sampleperiod*samples_count
        return observationTime, self._generator.sampleperiod, samples, self._activationState


    def setWfGenerator(self, generator):
        self._generator = generator 


class RtSampleArray(object):
    ''' This class contains a list of waveform values plus time stamps and annotations.
    It is used to create Waveform notifications.'''  
    def __init__(self, determinationTime, sampleperiod, samples, activationState):
        '''
        @param determinationTime: the time stamp of the first value in samples
        @param sampleperiod: the time difference between two samples
        @param samples: a list of values (float or int)
        @param acticationState: one of pmtypes.ComponentActivation values
        '''
        self.determinationTime = determinationTime
        self._sampleperiod = sampleperiod
        self.samples = samples
        self.activationState = activationState
        self.annotations = []
        self.applyAnnotations = []
        
        
    def _nearestIndex(self, timestamp):
        # first check if timestamp is outside the range of this samplearray. Accept 0.5*sampleperiod as tolerance.
        if self.determinationTime is None: # when deactivated, determinationTime is None
            return None
        if timestamp < (self.determinationTime - self._sampleperiod*0.5):
            return None 
        elif timestamp >  self.determinationTime + len(self.samples)*self._sampleperiod + self._sampleperiod*0.5:
            return None
        n = (timestamp - self.determinationTime) / self._sampleperiod 
        return  int(n)+1 if n%1 > 0.5 else int(n) 


    def getAnnotationTriggerTimestamps(self):
        ''' returns the time stamps of all samples that mark the beginning of a period'''
        return [self.determinationTime + i*self._sampleperiod for i, sample in enumerate(self.samples) if sample[1]]
        

    def addAnnotationsAt(self, annotation, timestamps):
        '''
        @param annotation: a pmtypes.Annotation instance
        @param timestamps: a list of time stamps (time.time based)
        '''
        annotationIndex = len(self.annotations) # Index is zero-based
        self.annotations.append(annotation)
        for t in timestamps:
            i = self._nearestIndex(t)
            if i is not None:
                self.applyAnnotations.append(pmtypes.ApplyAnnotation(int(annotationIndex), i))

_TrItem = namedtuple('_TrItem', 'old new') # a named tuple for better readability of code

class _MdibUpdateTransaction(object):
    #pylint: disable=protected-access
    ''' a helper class that collects multiple updates into one transaction.
    Used by contextmanager DeviceMdibContainer.mdibUpdateTransaction '''
    def __init__(self, deviceMdibContainer):
        self._deviceMdibContainer = deviceMdibContainer
        
        self.descriptorUpdates = OrderedDict()
        self.metricStateUpdates = OrderedDict()
        self.alertStateUpdates = OrderedDict()
        self.componentStateUpdates = OrderedDict()
        self.contextStateUpdates = OrderedDict()
        self.operationalStateUpdates = OrderedDict()
        self.rtSampleStateUpdates = dict()   # unordered dict for performance
        self._error = False
        self._closed = False


    def addDescriptor(self, descriptorContainer, adjustDescriptorVersion=True):
        '''

        :param descriptorContainer: the object that shall be added to mdib
        :param adjustDescriptorVersion: if True, and a descriptor with this handle does not exist, but was already present in this mdib before,
          the DescriptorVersion of descriptorContainer is set to last known version for this handle +1
        :return: None
        '''
        descriptorHandle = descriptorContainer.handle
        if adjustDescriptorVersion:
            self._deviceMdibContainer.descriptions.setVersion(descriptorContainer)
        if self._closed:
            raise RuntimeError('This _MdibUpdateTransaction is closed!')
        if self._error:
            raise RuntimeError('This _MdibUpdateTransaction failed due to an previous error!')
        try:
            if descriptorHandle in self.descriptorUpdates:
                raise ValueError('descriptorHandle {} already in updated set!'.format(descriptorHandle))
            if descriptorHandle in self._deviceMdibContainer.descriptions.handle.keys():
                raise ValueError('cannot create descriptorHandle {}, it already exists!'.format(descriptorHandle))
            self.descriptorUpdates[descriptorHandle] = _TrItem(None, descriptorContainer)
        except:
            self._error = True
            raise

    createDescriptor = addDescriptor

    def removeDescriptor(self, descriptorHandle):
        if self._closed:
            raise RuntimeError('This _MdibUpdateTransaction is closed!')
        if self._error:
            raise RuntimeError('This _MdibUpdateTransaction failed due to an previous error!')
        try:
            if descriptorHandle in self.descriptorUpdates:
                raise ValueError('descriptorHandle {} already in updated set!'.format(descriptorHandle))
            origDescriptorContainer = self._deviceMdibContainer.descriptions.handle.getOne(descriptorHandle)
            self.descriptorUpdates[descriptorHandle] = _TrItem(origDescriptorContainer, None)
        except:
            self._error = True
            raise


    def getDescriptor(self, descriptorHandle):
        ''' Update a Descriptor.
        When the transaction is committed, the modifications to the copy will be applied to the original version, 
        and notification messages will be sent to clients.
        @return: a copy of the state.
        '''
        if self._closed:
            raise RuntimeError('This _MdibUpdateTransaction is closed!')
        if self._error:
            raise RuntimeError('This _MdibUpdateTransaction failed due to an previous error!')
        try:
            if descriptorHandle in self.descriptorUpdates:
                raise ValueError('descriptorHandle {} already in updated set!'.format(descriptorHandle))
            origDescriptorContainer = self._deviceMdibContainer.descriptions.handle.getOne(descriptorHandle)
            descriptorContainer = origDescriptorContainer.mkCopy()
            descriptorContainer.incrementDescriptorVersion()
            self.descriptorUpdates[descriptorHandle] = _TrItem(origDescriptorContainer, descriptorContainer)
            return descriptorContainer
        except:
            self._error = True
            raise


    def hasState(self, descriptorHandle):
        ''' check if transaction has a state with given handle '''
        return self.getStateTransactionItem(descriptorHandle) is not None

    def getStateTransactionItem(self, descriptorHandle):
        ''' if transaction has a state with given handle, return the transaction-item, otherwise None.  '''
        for lookup in (self.metricStateUpdates,
                       self.alertStateUpdates,
                       self.componentStateUpdates,
                       self.contextStateUpdates,
                       self.operationalStateUpdates,
                       self.rtSampleStateUpdates):
            if descriptorHandle in lookup:
                return lookup[descriptorHandle]

    def addState(self, stateContainer, adjustStateVersion=True):
        if self._closed:
            raise RuntimeError('This _MdibUpdateTransaction is closed!')
        if self._error:
            raise RuntimeError('This _MdibUpdateTransaction failed due to an previous error!')
        my_multikey = self._deviceMdibContainer.states
        if stateContainer.isMetricState:
            my_updates = self.metricStateUpdates
        elif stateContainer.isSystemContextState:
            my_updates = self.metricStateUpdates
        elif stateContainer.isAlertState:
            my_updates = self.alertStateUpdates
        elif stateContainer.isComponentState:
            my_updates = self.componentStateUpdates
        elif stateContainer.isOperationalState:
            my_updates = self.operationalStateUpdates
        elif stateContainer.isContextState:
            my_updates = self.contextStateUpdates
            my_multikey = self._deviceMdibContainer.contextStates
        elif stateContainer.isRealtimeSampleArrayMetricState:
            my_updates = self.rtSampleStateUpdates
        elif stateContainer.NODETYPE == domTag('ScoState'):
            #special case ScoState Draft6: cannot notify updates, it is a category of its own that does not fit anywhere
            # This is a bug in the spec, not in this implementation!
            return

        descriptorHandle = stateContainer.descriptorHandle
        my_handle = stateContainer.Handle if stateContainer.isContextState else stateContainer.descriptorHandle
        if my_handle in my_updates:
            raise ValueError('descriptorHandle {} already in updated set!'.format(descriptorHandle))

        if adjustStateVersion:
            my_multikey.setVersion(stateContainer)
        my_updates[descriptorHandle] = _TrItem(None, stateContainer) # old, new

    def ungetState(self, stateContainer):
        ''' forget a state that was provided before by a getXXXState call'''
        for lookup in (self.alertStateUpdates, self.componentStateUpdates, self.contextStateUpdates,
                       self.metricStateUpdates, self.operationalStateUpdates, self.rtSampleStateUpdates):
            if stateContainer.descriptorHandle in lookup:
                del lookup[stateContainer.descriptorHandle]


    def getMetricState(self, descriptorHandle, adjustStateVersion=True):
        ''' Update a MetricState.
        When the transaction is committed, the modifications to the copy will be applied to the original version, 
        and notification messages will be sent to clients.
        :param descriptorHandle: the descriptorHandle of the object that shall be read
        :param adjustStateVersion: if True, and a state with this handle does not exist, but was already present in this mdib before,
          the StateVersion of descriptorContainer is set to last known version for this handle +1
        @return: a copy of the state.
        '''
        if self._closed:
            raise RuntimeError('This _MdibUpdateTransaction is closed!')
        if self._error:
            raise RuntimeError('This _MdibUpdateTransaction failed due to an previous error!')
        try:
            if descriptorHandle in self.metricStateUpdates:
                raise ValueError('descriptorHandle {} already in updated set!'.format(descriptorHandle))
            old_state, new_state  = self._get_or_mk_StateContainer(descriptorHandle, adjustStateVersion)
            if not new_state.isMetricState:
                raise ValueError('descriptorHandle {} does not reference a metric state'.format(descriptorHandle))
            self.metricStateUpdates[descriptorHandle] = _TrItem(old_state, new_state)
            return new_state
        except:
            self._error = True
            raise


    def getComponentState(self, descriptorHandle, adjustStateVersion=True):
        ''' Update a ComponentState.
        When the transaction is committed, the modifications to the copy will be applied to the original version, 
        and notification messages will be sent to clients.
        :param descriptorHandle: the descriptorHandle of the object that shall be read
        :param adjustStateVersion: if True, and a state with this handle does not exist, but was already present in this mdib before,
          the StateVersion of descriptorContainer is set to last known version for this handle +1
        @return: a copy of the state.
        '''
        if self._closed:
            raise RuntimeError('This _MdibUpdateTransaction is closed!')
        if self._error:
            raise RuntimeError('This _MdibUpdateTransaction failed due to an previous error!')
        try:
            if descriptorHandle in self.componentStateUpdates:
                raise ValueError('descriptorHandle {} already in updated set!'.format(descriptorHandle))
            old_state, new_state  = self._get_or_mk_StateContainer(descriptorHandle, adjustStateVersion)
            if not new_state.isComponentState:
                raise ValueError('descriptorHandle {} does not reference a component state'.format(descriptorHandle))
            self.componentStateUpdates[descriptorHandle] = _TrItem(old_state, new_state)
            return new_state
        except:
            self._error = True
            raise


    def getAlertState(self, descriptorHandle, adjustStateVersion=True):
        ''' Update AlertConditionState or AlertSignalState node
        When the transaction is committed, the modifications to the copy will be applied to the original version, 
        and notification messages will be sent to clients.
        :param descriptorHandle: the descriptorHandle of the object that shall be read
        :param adjustStateVersion: if True, and a state with this handle does not exist, but was already present in this mdib before,
          the StateVersion of descriptorContainer is set to last known version for this handle +1
        @return: a copy of the state.
        '''
        if self._closed:
            raise RuntimeError('This _MdibUpdateTransaction is closed!')
        if self._error:
            raise RuntimeError('This _MdibUpdateTransaction failed due to an previous error!')
        try:
            if descriptorHandle in self.alertStateUpdates:
                raise ValueError('descriptorHandle {} already in updated set!'.format(descriptorHandle))
            old_state, new_state  = self._get_or_mk_StateContainer(descriptorHandle, adjustStateVersion)
            if not new_state.isAlertState:
                raise ValueError('descriptorHandle {} does not reference an alert state'.format(descriptorHandle))
            self.alertStateUpdates[descriptorHandle] = _TrItem(old_state, new_state)
            return new_state
        except:
            self._error = True
            raise

    
    def getContextState(self, descriptorHandle, contextStateHandle=None, adjustStateVersion=True):
        ''' Create or Update a ContextState.
        If contextStateHandle is None, a new Context State will be created and returned.
        Otherwise an the existing contextState with that handle will be returned or a new one created.
        When the transaction is committed, the modifications to the copy will be applied to the original version, 
        and notification messages will be sent to clients.
        :param descriptorHandle: the descriptorHandle of the object that shall be read
        :param contextStateHandle: If None, a new Context State will be created and returned.
            Otherwise an existing contextState with that handle will be returned or a new one created.
        :param adjustStateVersion: if True, and a state with this handle does not exist, but was already present in this mdib before,
          the StateVersion of descriptorContainer is set to last known version for this handle +1
        @return: a copy of the state.
        '''
        if self._closed:
            raise RuntimeError('This _MdibUpdateTransaction is closed!')
        if self._error:
            raise RuntimeError('This _MdibUpdateTransaction failed due to an previous error!')
        
        lookup_key = (descriptorHandle, contextStateHandle)
        try:
            if lookup_key in self.contextStateUpdates:
                raise ValueError('descriptorHandle {} already in updated set!'.format(lookup_key))
            descriptorContainer = self._getDescriptorInTransaction(descriptorHandle)
            if contextStateHandle is None:
                oldStateContainer = None
                newStateContainer = self._deviceMdibContainer.mkStateContainerFromDescriptor(descriptorContainer)
                newStateContainer.BindingMdibVersion = self._deviceMdibContainer.mdibVersion # auto-set this Attribute
                newStateContainer.BindingStartTime = time.time() # auto-set this Attribute
            else:
                oldStateContainer = self._deviceMdibContainer.contextStates.handle.getOne(contextStateHandle, allowNone=True)
                if oldStateContainer is not None:
                    newStateContainer = oldStateContainer.mkCopy()
                    newStateContainer.incrementState()
                else:
                    newStateContainer = self._deviceMdibContainer.mkStateContainerFromDescriptor(descriptorContainer)
                    newStateContainer.BindingMdibVersion = self._deviceMdibContainer.mdibVersion  # auto-set this Attribute
                    newStateContainer.BindingStartTime = time.time()  # auto-set this Attribute
                    newStateContainer.Handle = contextStateHandle
                    if adjustStateVersion:
                        self._deviceMdibContainer.contextStates.setVersion(newStateContainer)
            self.contextStateUpdates[lookup_key] = _TrItem(oldStateContainer, newStateContainer)
            return newStateContainer
        except:
            self._error = True
            raise

    def addContextState(self, contextStateContainer, adjustStateVersion=True):
        ''' Add a new ContextState.
        :param contextStateContainer: a ContextStateContainer instance
        :param adjustStateVersion: if True, and a state with this handle does not exist, but was already present in this mdib before,
          the StateVersion of descriptorContainer is set to last known version for this handle +1
        '''
        if self._closed:
            raise RuntimeError('This _MdibUpdateTransaction is closed!')
        if self._error:
            raise RuntimeError('This _MdibUpdateTransaction failed due to an previous error!')

        lookup_key = (contextStateContainer.descriptorHandle, contextStateContainer.Handle)
        try:
            if lookup_key in self.contextStateUpdates:
                raise ValueError('descriptorHandle {} already in updated set!'.format(lookup_key))
            if adjustStateVersion:
                self._deviceMdibContainer.contextStates.setVersion(contextStateContainer)
            self.contextStateUpdates[lookup_key] = _TrItem(None, contextStateContainer)
        except:
            self._error = True
            raise

    def getOperationalState(self, descriptorHandle, adjustStateVersion=True):
        ''' Update an OperationalState.
        When the transaction is committed, the modifications to the copy will be applied to the original version, 
        and notification messages will be sent to clients.
        :param descriptorHandle: the descriptorHandle of the object that shall be read
        :param adjustStateVersion: if True, and a state with this handle does not exist, but was already present in this mdib before,
          the StateVersion of descriptorContainer is set to last known version for this handle +1
        @return: a copy of the state.
        '''
        if self._closed:
            raise RuntimeError('This _MdibUpdateTransaction is closed!')
        if self._error:
            raise RuntimeError('This _MdibUpdateTransaction failed due to an previous error!')
        try:
            if descriptorHandle in self.operationalStateUpdates:
                raise ValueError('descriptorHandle {} already in updated set!'.format(descriptorHandle))
            old_state, new_state  = self._get_or_mk_StateContainer(descriptorHandle, adjustStateVersion)
            if not new_state.isOperationalState:
                raise ValueError('descriptorHandle {} does not reference an operational state ({})'.format(descriptorHandle, new_state.__class__.__name__))
            self.operationalStateUpdates[descriptorHandle] = _TrItem(old_state, new_state)
            return new_state
        except:
            self._error = True
            raise


    def getRealTimeSampleArrayMetricState(self, descriptorHandle):
        # for performance reasons, this method does not return a deep copy of the original object.
        # This means no rollback possible. 
        if self._closed:
            raise RuntimeError('This _MdibUpdateTransaction is closed!')
        if self._error:
            raise RuntimeError('This _MdibUpdateTransaction failed due to an previous error!')
        try:
            if descriptorHandle in self.rtSampleStateUpdates:
                raise ValueError('descriptorHandle {} already in updated set!'.format(descriptorHandle))
            stateContainer = self._deviceMdibContainer.states.descriptorHandle.getOne(descriptorHandle, allowNone=True)
            if stateContainer is None:
                descriptorContainer = self._getDescriptorInTransaction(descriptorHandle)
                stateContainer = self._deviceMdibContainer.mkStateContainerFromDescriptor(descriptorContainer)
                self._deviceMdibContainer.states.addObject(stateContainer)
            else:
                stateContainer.incrementState()

            if not stateContainer.isRealtimeSampleArrayMetricState:
                raise ValueError('descriptorHandle {} does not reference a RealTimeSampleArrayMetricState'.format(descriptorHandle))
            newstate = stateContainer
            self.rtSampleStateUpdates[descriptorHandle] = _TrItem(stateContainer, newstate)
            return newstate
        except:
            self._error = True
            raise

    def _getDescriptorInTransaction(self, descriptorHandle):
        ''' looks for descriptor in current transaction and in mdib'''
        tr_containers = self.descriptorUpdates.get(descriptorHandle)
        if tr_containers is not None:
            old, new = tr_containers
            if new is None: # descriptor is dwlwted in this transaction!
                raise RuntimeError('The descriptor {} is going to be deleted'.format(descriptorHandle))
            else:
                return new
        else:
            return self._deviceMdibContainer.descriptions.handle.getOne(descriptorHandle)

    def _get_or_mk_StateContainer(self, descriptorHandle, adjustStateVersion=True):
        ''' returns oldContainer, newContainer'''
        descriptorContainer = self._getDescriptorInTransaction(descriptorHandle)
        old_stateContainer = self._deviceMdibContainer.states.descriptorHandle.getOne(descriptorContainer.handle, allowNone=True)
        if old_stateContainer is None:
            # create a new state object
            new_stateContainer = self._deviceMdibContainer.mkStateContainerFromDescriptor(descriptorContainer)
            if adjustStateVersion:
                self._deviceMdibContainer.states.setVersion(new_stateContainer)
        else:
            new_stateContainer = old_stateContainer.mkCopy()
            new_stateContainer.incrementState()
        return old_stateContainer, new_stateContainer



class DeviceMdibContainer(mdibbase.MdibContainer):
    ''' update source is the users program.'''
#    def __init__(self, bicepsSchemaInstance=None, log_prefix=None):
    def __init__(self, sdc_definitions, log_prefix=None):
        super(DeviceMdibContainer, self).__init__(sdc_definitions)
        self._logger = loghelper.getLoggerAdapter('sdc.device.mdib', log_prefix)
        self._sdcDevice = None
        self._trLock = Lock() # transaction lock

        self.sequenceId = uuid.uuid4().urn # this uuid identifies this mdib instance
        
        self._currentLocation = None  # or a SdcLocation instance

        self._waveformGenerators = {}
        self._annotators = {}
        self._current_transaction = None

        self.preCommitHandler = None # preCommitHandler can modify transaction if needed before it is committed
        self.postCommitHandler = None # postCommitHandler can modify mdib if needed after it is committed

    @contextmanager
    def mdibUpdateTransaction(self, setDeterminationTime=True):
        #pylint: disable=protected-access
        with self._trLock:
            try:
                self._current_transaction = _MdibUpdateTransaction(self)
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


    def _process_transaction(self, setDeterminationTime):
        mgr = self._current_transaction
        now = time.time()

        if len(mgr.descriptorUpdates) > 0:
            self.mdDescriptionVersion +=1 # BICEPS: The version number is incremented by one every time the descriptive part changes

        if len(mgr.metricStateUpdates) > 0 or len(mgr.alertStateUpdates) > 0 \
                or len(mgr.componentStateUpdates) > 0 or len(mgr.contextStateUpdates) > 0 \
                or len(mgr.operationalStateUpdates) > 0 or len(mgr.rtSampleStateUpdates) > 0:
            self.mdStateVersion +=1 # BICEPS: The version number is incremented by one every time the state part changes.

        # handle descriptors
        if len(mgr.descriptorUpdates) > 0:
            #need to know all to be deleted and to be created descriptors
            to_be_deleted = [old for old, new in mgr.descriptorUpdates.values() if new is None]
            to_be_created = [new for old, new in mgr.descriptorUpdates.values() if old is None]
            to_be_deleted_handles = [d.handle for d in to_be_deleted]
            to_be_created_handles = [d.handle for d in to_be_created]
            with self.mdibLock:
                self.mdibVersion += 1
                updated = []
                created = []
                deleted = []
                updated_states = []

                def _updateCorrespondingState(descriptorContainer):
                    #look for corresponding state in transaction.
                    # if it is modified there, then move it from other lookup into descriptor modification report.
                    corresponding_state = None
                    for update_dict in (mgr.metricStateUpdates, mgr.alertStateUpdates, mgr.componentStateUpdates,
                                        mgr.contextStateUpdates, mgr.operationalStateUpdates):
                        update =  update_dict.get(descriptorContainer.handle)
                        if update is not None:
                            oldstate, newstate = update
                            if newstate is not None: # this is a create or update operation
                                corresponding_state = newstate
                                corresponding_state.descriptorContainer = descriptorContainer
                                corresponding_state.updateDescriptorVersion()
                                self._logger.info(
                                    'mdibUpdateTransaction: Descriptor update move state update to descriptor update:{}',
                                    newstate)
                                del update_dict[descriptorContainer.handle]
                                multikey_instance = self.contextStates if newstate.isContextState else self.states
                                if oldstate:
                                    multikey_instance.removeObjectNoLock(oldstate)
                                multikey_instance.addObjectNoLock(newstate)
                            break

                    if corresponding_state is None:
                        corresponding_state = self.states.descriptorHandle.getOne(descriptorContainer.handle,
                                                                                  allowNone=True)
                        if corresponding_state is not None:
                            corresponding_state.descriptorContainer = descriptorContainer
                            corresponding_state.incrementState()
                            corresponding_state.updateDescriptorVersion()
                    if corresponding_state is not None:
                        updated_states.append(corresponding_state)

                def _incrementParentDescriptorVersion(descriptorContainer):
                    parentDescriptorContainer = self.descriptions.handle.getOne(descriptorContainer.parentHandle)
                    parentDescriptorContainer.incrementDescriptorVersion()
                    updated.append(parentDescriptorContainer)
                    _updateCorrespondingState(parentDescriptorContainer)

                # handling only updated states here: If a descriptor is created, I assume that the application also creates the state in an transaction.
                # The state will then be transported via that notification report.
                # Maybe this needs to be reworked, but at the time of this writing it seems fine.
                for value in mgr.descriptorUpdates.values():
                    origDescriptor, newDescriptor = value.old, value.new
                    if newDescriptor is not None:
                        newDescriptor.updateNode(setXsiType=True)
                        # DescriptionModificationReport also contains the states that are related to the descriptors.
                        # => if there is one, update its DescriptorVersion and add it to list of states that shall be sent
                        # (Assuming that context descriptors (patient, location) are never changed,
                        #  additional check for states in self.contextStates is not needed.
                        #  If this assumption is wrong, that functionality must be added!)
                        _updateCorrespondingState(newDescriptor)
                    try:
                        if origDescriptor is None:
                            # this is a create operation
                            self._logger.debug('mdibUpdateTransaction: new descriptor Handle={}, DescriptorVersion={}', newDescriptor.handle, newDescriptor.DescriptorVersion)
                            created.append(newDescriptor.mkCopy())
                            self.descriptions.addObjectNoLock(newDescriptor)
                            # R0033: A SERVICE PROVIDER SHALL increment pm:AbstractDescriptor/@DescriptorVersion by one if a direct child descriptor is added or deleted.
                            if newDescriptor.parentHandle is not None and  newDescriptor.parentHandle not in to_be_created_handles:
                                # only update parent if it is not also created in this transaction
                                _incrementParentDescriptorVersion(newDescriptor)
                        elif newDescriptor is None:
                            # this is a delete operation
                            self._logger.debug('mdibUpdateTransaction: rm descriptor Handle={}, DescriptorVersion={}', origDescriptor.handle, origDescriptor.DescriptorVersion)
                            all_descriptors = self.getAllDescriptorsInSubTree(origDescriptor)
                            self._rmDescriptorsAndStates(all_descriptors)
                            deleted.extend(all_descriptors)
                            # R0033: A SERVICE PROVIDER SHALL increment pm:AbstractDescriptor/@DescriptorVersion by one if a direct child descriptor is added or deleted.
                            if origDescriptor.parentHandle is not None and  origDescriptor.parentHandle not in to_be_deleted_handles:
                                # only update parent if it is not also deleted in this transaction
                                _incrementParentDescriptorVersion(origDescriptor)
                        else:
                            # this is an update operation
                            updated.append(newDescriptor)
                            self._logger.debug('mdibUpdateTransaction: update descriptor Handle={}, DescriptorVersion={}', newDescriptor.handle, newDescriptor.DescriptorVersion)
                            self.descriptions.replaceObjectNoLock(newDescriptor)
                    except RuntimeError:
                        self._logger.error('mdibUpdateTransaction: Descriptor Handle={} did not exist before!! really??', origDescriptor.handle)
                        raise
                mdibVersion = self.mdibVersion
                # makes copies of all descriptors and states for sending, so that they can't be affected by transactions after this one
                updated = [d.mkCopy() for d in updated]
                created = [d.mkCopy() for d in created]
                deleted = [d.mkCopy() for d in deleted]
                updated_states = [s.mkCopy() for s in updated_states]
            self._sdcDevice.sendDescriptorUpdates(mdibVersion, updated=updated, created=created, deleted=deleted, updated_states=updated_states)

        # handle metric states
        if len(mgr.metricStateUpdates) > 0:
            with self.mdibLock:
                self.mdibVersion += 1
                updates = []
                self._logger.debug('mdibUpdateTransaction: mdib version={}, metric updates = {}',
                                   self.mdibVersion,
                                   mgr.metricStateUpdates)
                for value in mgr.metricStateUpdates.values():
                    oldstate, newstate = value.old, value.new
                    try:
                        if setDeterminationTime and newstate.metricValue is not None:
                            newstate.metricValue.DeterminationTime = now
                        # replace the old container with the new one
                        self.states.removeObjectNoLock(oldstate)
                        self.states.addObjectNoLock(newstate)
                        updates.append(newstate)
                    except RuntimeError:
                        self._logger.warn('mdibUpdateTransaction: {} did not exist before!! really??', newstate)
                        raise
                mdibVersion = self.mdibVersion
                # makes copies of all states for sending, so that they can't be affected by transactions after this one
                updates = [s.mkCopy() for s in updates]
            self._sdcDevice.sendMetricStateUpdates(mdibVersion, updates)

        # handle alert states
        if len(mgr.alertStateUpdates) > 0:
            with self.mdibLock:
                self.mdibVersion += 1
                updates = []
                self._logger.debug('mdibUpdateTransaction: alert State updates = {}', mgr.alertStateUpdates)
                for value in mgr.alertStateUpdates.values():
                    oldstate, newstate = value.old, value.new
                    try:
                        if setDeterminationTime and newstate.isAlertCondition:
                            newstate.DeterminationTime = time.time()
                        newstate.updateNode()
                        # replace the old container with the new one
                        self.states.removeObjectNoLock(oldstate)
                        self.states.addObjectNoLock(newstate)
                        updates.append(newstate)
                    except RuntimeError:
                        self._logger.warn('mdibUpdateTransaction: {} did not exist before!! really??', newstate)
                        raise
                mdibVersion = self.mdibVersion
                # makes copies of all states for sending, so that they can't be affected by transactions after this one
                updates = [s.mkCopy() for s in updates]
            self._sdcDevice.sendAlertStateUpdates(mdibVersion, updates)

            # handle component state states
        if len(mgr.componentStateUpdates) > 0:
            with self.mdibLock:
                self.mdibVersion += 1
                updates = []
                self._logger.debug('mdibUpdateTransaction: component State updates = {}', mgr.componentStateUpdates)
                for value in mgr.componentStateUpdates.values():
                    oldstate, newstate = value.old, value.new
                    try:
                        newstate.updateNode()
                        # replace the old container with the new one
                        self.states.removeObjectNoLock(oldstate)
                        self.states.addObjectNoLock(newstate)
                        updates.append(newstate)
                    except RuntimeError:
                        self._logger.warn('mdibUpdateTransaction: {} did not exist before!! really??', newstate)
                        raise
                mdibVersion = self.mdibVersion
                # makes copies of all states for sending, so that they can't be affected by transactions after this one
                updates = [s.mkCopy() for s in updates]
            self._sdcDevice.sendComponentStateUpdates(mdibVersion, updates)

        # handle context states
        if len(mgr.contextStateUpdates) > 0:
            with self.mdibLock:
                self.mdibVersion += 1
                updates = []
                self._logger.debug('mdibUpdateTransaction: contextState updates = {}', mgr.contextStateUpdates)
                for value in mgr.contextStateUpdates.values():
                    oldstate, newstate = value.old, value.new
                    try:
                        updates.append(newstate)
                        # replace the old container with the new one
                        self.contextStates.removeObjectNoLock(oldstate)
                        self.contextStates.addObjectNoLock(newstate)
                        newstate.updateNode()
                    except RuntimeError:
                        self._logger.warn('mdibUpdateTransaction: {} did not exist before!! really??', newstate)
                        raise
                mdibVersion = self.mdibVersion
                # makes copies of all tates for sending, so that they can't be affected by transactions after this one
                updates = [s.mkCopy() for s in updates]
            self._sdcDevice.sendContextStateUpdates(mdibVersion, updates)

        # handle operational states
        if len(mgr.operationalStateUpdates) > 0:
            with self.mdibLock:
                self.mdibVersion += 1
                updates = []
                self._logger.debug('mdibUpdateTransaction: operationalState updates = {}', mgr.operationalStateUpdates)
                for value in mgr.operationalStateUpdates.values():
                    oldstate, newstate = value.old, value.new
                    try:
                        newstate.updateNode()
                        self.states.removeObjectNoLock(oldstate)
                        self.states.addObjectNoLock(newstate)
                        updates.append(newstate)
                    except RuntimeError:
                        self._logger.warn('mdibUpdateTransaction: {} did not exist before!! really??', newstate)
                        raise
                mdibVersion = self.mdibVersion
                # makes copies of all states for sending, so that they can't be affected by transactions after this one
                updates = [s.mkCopy() for s in updates]
            self._sdcDevice.sendOperationalStateUpdates(mdibVersion, updates)

        # handle real time samples
        if len(mgr.rtSampleStateUpdates) > 0:
            with self.mdibLock:
                self.mdibVersion += 1
                updates = []
                self._logger.debug('mdibUpdateTransaction: rtSample updates = {}', mgr.rtSampleStateUpdates)
                for value in mgr.rtSampleStateUpdates.values():
                    oldstate, newstate = value.old, value.new
                    try:
                        newstate.updateNode()
                        updates.append(newstate)
                    except RuntimeError:
                        self._logger.warn('mdibUpdateTransaction: {} did not exist before!! really??', newstate)
                        raise
                mdibVersion = self.mdibVersion
                # makes copies of all states for sending, so that they can't be affected by transactions after this one
                updates = [s.mkCopy() for s in updates]
            self._sdcDevice.sendRealtimeSamplesStateUpdates(mdibVersion, updates)
    
    
    def setSdcDevice(self, sdcDevice):
        self._sdcDevice = sdcDevice


    def setLocation(self, sdcLocation, validators=None):
        '''
        This method updates only the mdib internal data! 
        use the SdcDevice.setLocation method if you want to publish the address an the network.
        @param sdcLocation: a pysdc.location.SdcLocation instance
        @param validator: a list of pysdc.pmtypes.InstanceIdentifier objects or None
        '''
        allLocationContexts = self.contextStates.NODETYPE.get(domTag('LocationContextState'), [])
        with self.mdibUpdateTransaction() as mgr:
            # set all to currently associated Locations to Disassociated
            associatedLocations = [l for l in allLocationContexts if l.ContextAssociation == pmtypes.ContextAssociation.ASSOCIATED]
            for l in associatedLocations:
                locationContext = mgr.getContextState(l.descriptorHandle, l.Handle)
                locationContext.ContextAssociation = pmtypes.ContextAssociation.DISASSOCIATED
                locationContext.UnbindingMdibVersion = self.mdibVersion # UnbindingMdibVersion is the first version in which it is no longer bound ( == this version)
            descriptorContainer = self.descriptions.NODETYPE.getOne(domTag('LocationContextDescriptor'))
                    
            self._currentLocation = mgr.getContextState(descriptorContainer.handle) # this creates a new location state
            self._currentLocation.updateFromSdcLocation(sdcLocation, self.bicepsSchema)
            if validators is not None:
                self._currentLocation.Validator = validators
  

    def _createDescriptorContainer(self, cls, nodeName, handle, parentHandle, codedValue, safetyClassification):
        obj = cls(nsmapper=self.nsmapper, 
                  nodeName=nodeName, 
                  handle=handle, 
                  parentHandle=parentHandle,
                  )
        obj.SafetyClassification = safetyClassification
        obj.Type = codedValue
        obj.updateNode()
        return obj


    def createVmdDescriptorContainer(self, handle, parentHandle, codedValue, safetyClassification):
        '''
        This method creates an VmdDescriptorContainer with the given properties.
        If it is called within an transaction, the created object is added to transaction and clients will be notified.
        Otherwise the object is only added to mdib without sending notifications to clients!
        :param handle: Handle of the new container
        :param parentHandle: Handle of the parent
        :param codedValue: a pmtypes.CodedValue instance that defines what this onject represents in medical terms.
        :param safetyClassification: a pmtypes.SafetyClassification value
        :return: the created object
        '''
        qNameTag = domTag('Vmd')
        cls = self.getDescriptorContainerClass(qNameTag)
        obj = self._createDescriptorContainer(cls, qNameTag, handle, parentHandle, codedValue, safetyClassification)
        if self._current_transaction is not None:
            self._current_transaction.createDescriptor(obj)
        else:
            self.descriptions.addObject(obj)
        return obj


    def createChannelDescriptorContainer(self, handle, parentHandle, codedValue, safetyClassification):
        '''
        This method creates a ChannelDescriptorContainer with the given properties and optionally adds it to the mdib.
        If it is called within an transaction, the created object is added to transaction and clients will be notified.
        Otherwise the object is only added to mdib without sending notifications to clients!
        :param handle: Handle of the new container
        :param parentHandle: Handle of the parent
        :param codedValue: a pmtypes.CodedValue instance that defines what this onject represents in medical terms.
        :param safetyClassification: a pmtypes.SafetyClassification value
        :return: the created object
        '''
        qNameTag = domTag('Channel')
        cls = self.getDescriptorContainerClass(qNameTag)
        obj = self._createDescriptorContainer(cls, qNameTag, handle, parentHandle, codedValue, safetyClassification)
        if self._current_transaction is not None:
            self._current_transaction.createDescriptor(obj)
        else:
            self.descriptions.addObject(obj)
        return obj
    

    def createStringMetricDescriptorContainer(self, handle, parentHandle, codedValue, safetyClassification, unit,  metricAvailability='Intr', metricCategory='Unspec'):
        '''
        This method creates a StringMetricDescriptorContainer with the given properties and optionally adds it to the mdib.
        If it is called within an transaction, the created object is added to transaction and clients will be notified.
        Otherwise the object is only added to mdib without sending notifications to clients!
        :param handle: Handle of the new container
        :param parentHandle: Handle of the parent
        :param codedValue: a pmtypes.CodedValue instance that defines what this onject represents in medical terms.
        :param safetyClassification: a pmtypes.SafetyClassification value
        :return: the created object
        '''
        qNameTag = domTag('Metric')
        qNameType = domTag('StringMetricDescriptor')
        cls = self.getDescriptorContainerClass(qNameType)
        obj = self._createDescriptorContainer(cls, qNameTag, handle, parentHandle, codedValue, safetyClassification)
        obj.Unit = unit
        obj.MetricAvailability = metricAvailability
        obj.MetricCategory = metricCategory
        obj.updateNode()
        if self._current_transaction is not None:
            self._current_transaction.createDescriptor(obj)
        else:
            self.descriptions.addObject(obj)
        return obj


    def createEnumStringMetricDescriptorContainer(self, handle, parentHandle, codedValue, safetyClassification, unit,  allowedValues, metricAvailability='Intr', metricCategory='Unspec'):
        '''
        This method creates an EnumStringMetricDescriptorContainer with the given properties and optionally adds it to the mdib.
        If it is called within an transaction, the created object is added to transaction and clients will be notified.
        Otherwise the object is only added to mdib without sending notifications to clients!
        :param handle: Handle of the new container
        :param parentHandle: Handle of the parent
        :param codedValue: a pmtypes.CodedValue instance that defines what this onject represents in medical terms.
        :param safetyClassification: a pmtypes.SafetyClassification value
        :return: the created object
        '''
        qNameTag = domTag('Metric')
        qNameType = domTag('EnumStringMetricDescriptor')
        cls = self.getDescriptorContainerClass(qNameType)
        obj = self._createDescriptorContainer(cls, qNameTag, handle, parentHandle, codedValue, safetyClassification)
        obj.Unit = unit
        obj.MetricAvailability = metricAvailability
        obj.MetricCategory = metricCategory
        obj.AllowedValue = allowedValues
        obj.updateNode()
        if self._current_transaction is not None:
            self._current_transaction.createDescriptor(obj)
        else:
            self.descriptions.addObject(obj)
        return obj


    def createClockDescriptorContainer(self, handle, parentHandle, codedValue, safetyClassification):
        '''
        This method creates a ClockDescriptorContainer with the given properties.
        If it is called within an transaction, the created object is added to transaction and clients will be notified.
        Otherwise the object is only added to mdib without sending notifications to clients!
        :param handle: Handle of the new container
        :param parentHandle: Handle of the parent
        :param codedValue: a pmtypes.CodedValue instance that defines what this onject represents in medical terms.
        :param safetyClassification: a pmtypes.SafetyClassification value
        :return: the created object
        '''
        cls = self.getDescriptorContainerClass( domTag('ClockDescriptor'))
        obj = self._createDescriptorContainer(cls, domTag('Clock'), handle, parentHandle, codedValue, safetyClassification)
        if self._current_transaction is not None:
            self._current_transaction.createDescriptor(obj)
        else:
            self.descriptions.addObject(obj)
        return obj


    def addState(self, stateContainer, adjustStateVersion=True):
        if self._current_transaction is not None:
            self._current_transaction.addState(stateContainer, adjustStateVersion)
        else:
            if stateContainer.isContextState:
                if stateContainer.Handle in self.contextStates.handle:
                    raise ValueError('context state Handle {} already in mdib!'.format(stateContainer.Handle))
                if adjustStateVersion:
                    self.contextStates.setVersion(stateContainer)
            else:
                if stateContainer.descriptorHandle in self.states.descriptorHandle:
                    raise ValueError('state descriptorHandle {} already in mdib!'.format(stateContainer.descriptorHandle))
                if adjustStateVersion:
                    self.states.setVersion(stateContainer)

    def addMdsNode(self, mdsNode):
        '''
        This method creates DescriptorContainers and StateContainers from the provided dom tree.
        If it is called within an transaction, the created objects are added to transaction and clients will be notified.
        Otherwise the objects are only added to mdib without sending notifications to clients!
        :param mdsNode: a node representing data of a complete mds
        :return: None
        '''
        msg_reader = msgreader.MessageReader(self)
        descriptorContainers = msg_reader.readMdDescription(mdsNode)
        if self._current_transaction is not None:
            for descr in descriptorContainers:
                self._current_transaction.createDescriptor(descr)
        else:
            for descr in descriptorContainers:
                self.descriptions.addObject(descr)

        stateContainers = msg_reader.readMdState(mdsNode, additionalDescriptorContainers=descriptorContainers)
        for s in stateContainers:
            self.addState(s)
        self.mkStateContainersforAllDescriptors()

    # real time data handling
    def registerWaveformGenerator(self, descriptorHandle, wfGenerator):
        '''
        @param descriptorHandle: the handle of the RealtimeSampelArray that shall accept this data
        @param wfGenerator: a waveforms.WaveformGenerator instance
        '''
        sampleperiod = wfGenerator.sampleperiod
        descriptorContainer = self.descriptions.handle.getOne(descriptorHandle)
        if descriptorContainer.SamplePeriod != sampleperiod:
            if self._sdcDevice is not None:
                # we must inform subscribers 
                with self.mdibUpdateTransaction() as tr:
                    descr = tr.getDescriptor(descriptorHandle)
                    descr.SamplePeriod = sampleperiod
            else:
                # we are at initialization time, this is a local operation. 
                descriptorContainer.SamplePeriod = sampleperiod
        if descriptorHandle in self._waveformGenerators:
            self._waveformGenerators[descriptorHandle].setWfGenerator(wfGenerator)
        else:
            self._waveformGenerators[descriptorHandle] = (SampleArraySource(descriptorHandle, wfGenerator)) 


    def setWaveformGeneratorActivationState(self, descriptorHandle, componentActivation):
        '''
        @param componentActivation: one of pmtypes.ComponentActivation values
        '''   
        self._waveformGenerators[descriptorHandle].setActivationState(componentActivation)


    def registerAnnotationGenerator(self, annotator, triggerHandle, annotatedHandles):
        '''
        @param annotator: a pmtypes.Annotation instance
        @param triggerHandle: The handle of the waveform that triggers the annotator ( trigger = start of a waveform cycle)
        @param annotatedHandles: the handles of the waveforms that shall be annotated.
        '''
        self._annotators[triggerHandle] = (annotator, annotatedHandles)


    def _getNextRealtimeSample(self, descriptorHandle):
        '''
        @return : timestamp, sampleperiod, list of values, activationState
        '''
        return self._waveformGenerators[descriptorHandle].getNextSampleArray()


    def getUpdatedDeviceRtSamples(self):
        rtsamples = {}
        # look for all waveforms that have new sample data or a changed activation state
        for handle, source in self._waveformGenerators.items():
            oldRtSampleStateContainer = self.states.descriptorHandle.getOne(handle, allowNone=True)
            determinationTime, sampleperiod, samples, activationState = self._getNextRealtimeSample(handle)
            if len(samples) > 0 or (oldRtSampleStateContainer is not None and oldRtSampleStateContainer.ActivationState != activationState):
                rtsamples[handle] = RtSampleArray(determinationTime, sampleperiod, samples, activationState)
        
        # add annotations
        for srchandle, _annotator in self._annotators.items():
            annotation, destHandles = _annotator
            if srchandle in rtsamples:
                samples = rtsamples[srchandle].samples
                timestamps = rtsamples[srchandle].getAnnotationTriggerTimestamps()
                if timestamps:
                    for destHandle in destHandles:
                        if destHandle in rtsamples:
                            rtsamples[destHandle].addAnnotationsAt(annotation, timestamps)
        return rtsamples        


    def mkStateContainersforAllDescriptors(self):
        '''The model requires that there is a state for every descriptor (exception: multi-states)
        Call this method to create missing states
        :return:
        '''
        for descr in self.descriptions.objects:
            if descr.handle not in self.states.descriptorHandle and descr.handle not in self.contextStates.descriptorHandle:
                state_cls = self.getStateClsForDescriptor(descr)
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
                    st.updateNode()
                    if self._current_transaction is not None:
                        self._current_transaction.addState(st)
                    else:
                        self.states.addObject(st)


    @classmethod
    def fromMdibFile(cls, path, createLocationContextDescr=True, createPatientContextDescr=True,
                     protocol_definition=None, log_prefix=None):
        """
        An alternative constructor for the class
        :param path: the input file path for creating the mdib
        :param createLocationContextDescr: same as in fromString method
        :param createPatientContextDescr: same as in fromString method
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

        mdib = cls(protocol_definition, log_prefix=log_prefix)
        root =  msgreader.MessageReader.getMdibRootNode(mdib.sdc_definitions, xml_text)
        mdib.bicepsSchema.bmmSchema.assertValid(root)

        mdib.nsmapper.useDocPrefixes(root.nsmap)
        msg_reader = msgreader.MessageReader(mdib)
        # first make descriptions and add them to mdib, and then make states (they need already existing descriptions)
        descriptorContainers = msg_reader.readMdDescription(root)
        mdib.addDescriptionContainers(descriptorContainers)
        stateContainers = msg_reader.readMdState(root)
        mdib.addStateContainers(stateContainers)

        if createLocationContextDescr or createPatientContextDescr:
            # make sure we have exactly one PatientContext and one LocationContext Descriptor, depending of flags
            systemContextContainer = mdib.descriptions.NODETYPE.getOne(domTag('SystemContextDescriptor'))
            children = mdib.descriptions.parentHandle.get(systemContextContainer.handle)
            childdrenNodeNames = [ch.NODETYPE for ch in children]
            if createLocationContextDescr:
                qn = domTag('LocationContextDescriptor')
                if qn not in childdrenNodeNames:
                    mdib._logger.info('creating a LocationContextDescriptor')
                    cls = mdib.getDescriptorContainerClass(qn)
                    lc = cls(mdib.nsmapper, nodeName=domTag('LocationContext'),
                             handle=uuid.uuid4().hex, parentHandle=systemContextContainer.handle)
                    lc.SafetyClassification = pmtypes.SafetyClassification.INF
                    mdib.descriptions.addObject(lc)
            if createPatientContextDescr:
                qn = domTag('PatientContextDescriptor')
                if qn not in childdrenNodeNames:
                    mdib._logger.info('creating a PatientContextDescriptor')
                    cls = mdib.getDescriptorContainerClass(qn)
                    pc = cls(mdib.nsmapper, nodeName=domTag('PatientContext'),
                             handle=uuid.uuid4().hex, parentHandle=systemContextContainer.handle)
                    pc.SafetyClassification = pmtypes.SafetyClassification.INF
                    mdib.descriptions.addObject(pc)
        mdib.mkStateContainersforAllDescriptors()
        return mdib
