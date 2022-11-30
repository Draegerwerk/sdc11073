import time
import uuid
from collections import OrderedDict
from collections import namedtuple
from contextlib import contextmanager
from functools import wraps
from threading import Lock

from lxml import etree as etree_

from . import mdibbase
from . import msgreader
from .devicewaveform import DefaultWaveformSource
from .. import loghelper
from .. import pmtypes
from ..definitions_base import ProtocolsRegistry
from ..definitions_sdc import SDC_v1_Definitions
from ..namespaces import domTag
from ..namespaces import extTag

_TrItem = namedtuple('_TrItem', 'old new')  # a named tuple for better readability of code


class _TransactionBase(object):
    def __init__(self, device_mdib_container):
        self._deviceMdibContainer = device_mdib_container
        self.descriptorUpdates = OrderedDict()
        self.metricStateUpdates = OrderedDict()
        self.alertStateUpdates = OrderedDict()
        self.componentStateUpdates = OrderedDict()
        self.contextStateUpdates = OrderedDict()
        self.operationalStateUpdates = OrderedDict()
        self.rtSampleStateUpdates = dict()  # unordered dict for performance
        self._error = False
        self._closed = False
        self.mdib_version = None

    def _getDescriptorInTransaction(self, descriptorHandle):
        """ looks for new or updated descriptor in current transaction and in mdib"""
        tr_containers = self.descriptorUpdates.get(descriptorHandle)
        if tr_containers is not None:
            old, new = tr_containers
            if new is None:  # descriptor is deleted in this transaction!
                raise RuntimeError('The descriptor {} is going to be deleted'.format(descriptorHandle))
            else:
                return new
        else:
            return self._deviceMdibContainer.descriptions.handle.getOne(descriptorHandle)

    def _get_or_mk_StateContainer(self, descriptorHandle, adjustStateVersion=True):
        """ returns oldContainer, newContainer"""
        descriptorContainer = self._getDescriptorInTransaction(descriptorHandle)
        old_stateContainer = self._deviceMdibContainer.states.descriptorHandle.getOne(descriptorContainer.handle,
                                                                                      allowNone=True)
        if old_stateContainer is None:
            # create a new state object
            new_stateContainer = self._deviceMdibContainer.mkStateContainerFromDescriptor(descriptorContainer)
            if adjustStateVersion:
                self._deviceMdibContainer.states.setVersion(new_stateContainer)
        else:
            new_stateContainer = old_stateContainer.mkCopy()
            new_stateContainer.incrementState()
        return old_stateContainer, new_stateContainer


def tr_method_wrapper(method):
    """a decorator for consistency checks and error handling"""

    @wraps(method)
    def wrapper(self, *args, **kwargs):
        if self._closed:
            raise RuntimeError('This Transaction is closed!')
        if self._error:
            raise RuntimeError('This Transaction failed due to an previous error!')
        try:
            return method(self, *args, **kwargs)
        except:
            self._error = True
            raise

    return wrapper


class _RtDataMdibUpdateTransaction(_TransactionBase):
    """This transaction is only used internally to periodically send waveform notifications.
    It handles this specific purpose with less overhead compared to regular transaction."""

    def __init__(self, device_mdib_container):
        super().__init__(device_mdib_container)

    @tr_method_wrapper
    def getRealTimeSampleArrayMetricState(self, descriptorHandle):
        # for performance reasons, this method does not return a copy of the original object.
        # This means no rollback possible.
        if descriptorHandle in self.rtSampleStateUpdates:
            raise ValueError('descriptorHandle {} already in updated set!'.format(descriptorHandle))
        stateContainer = self._deviceMdibContainer.states.descriptorHandle.getOne(descriptorHandle, allowNone=True)
        if stateContainer is None:
            raise ValueError('state {} not found!'.format(descriptorHandle))

        if not stateContainer.isRealtimeSampleArrayMetricState:
            raise ValueError('descriptorHandle {} does not reference a RealTimeSampleArrayMetricState'.format(
                descriptorHandle))
        stateContainer.incrementState()
        new_state = stateContainer  # supply old and new state; although identical, just do not break interface
        self.rtSampleStateUpdates[descriptorHandle] = _TrItem(stateContainer, new_state)
        return new_state


class _MdibUpdateTransaction(_TransactionBase):
    # pylint: disable=protected-access
    """ a helper class that collects multiple updates into one transaction.
    Used by contextmanager DeviceMdibContainer.mdibUpdateTransaction """

    def __init__(self, device_mdib_container):
        super().__init__(device_mdib_container)

    @tr_method_wrapper
    def addDescriptor(self, descriptorContainer, adjustDescriptorVersion=True):
        """
        :param descriptorContainer: the object that shall be added to mdib
        :param adjustDescriptorVersion: if True, and a descriptor with this handle does not exist, but was already present in this mdib before,
          the DescriptorVersion of descriptorContainer is set to last known version for this handle +1
        :return: None
        """
        descriptorHandle = descriptorContainer.handle
        if adjustDescriptorVersion:
            self._deviceMdibContainer.descriptions.setVersion(descriptorContainer)
        if descriptorHandle in self.descriptorUpdates:
            raise ValueError('descriptorHandle {} already in updated set!'.format(descriptorHandle))
        if descriptorHandle in self._deviceMdibContainer.descriptions.handle.keys():
            raise ValueError('cannot create descriptorHandle {}, it already exists!'.format(descriptorHandle))
        self.descriptorUpdates[descriptorHandle] = _TrItem(None, descriptorContainer)

    createDescriptor = addDescriptor

    @tr_method_wrapper
    def removeDescriptor(self, descriptorHandle):
        if descriptorHandle in self.descriptorUpdates:
            raise ValueError('descriptorHandle {} already in updated set!'.format(descriptorHandle))
        origDescriptorContainer = self._deviceMdibContainer.descriptions.handle.getOne(descriptorHandle)
        self.descriptorUpdates[descriptorHandle] = _TrItem(origDescriptorContainer, None)

    @tr_method_wrapper
    def getDescriptor(self, descriptorHandle):
        """ Update a descriptor.
        When the transaction is committed, the modifications to the copy will be applied to the original version,
        and notification messages will be sent to clients.
        @return: a copy of the state.
        """
        if descriptorHandle in self.descriptorUpdates:
            raise ValueError('descriptorHandle {} already in updated set!'.format(descriptorHandle))
        origDescriptorContainer = self._deviceMdibContainer.descriptions.handle.getOne(descriptorHandle)
        descriptorContainer = origDescriptorContainer.mkCopy()
        descriptorContainer.incrementDescriptorVersion()
        self.descriptorUpdates[descriptorHandle] = _TrItem(origDescriptorContainer, descriptorContainer)
        return descriptorContainer

    def hasState(self, descriptorHandle):
        """ check if transaction has a state with given handle """
        return self.getStateTransactionItem(descriptorHandle) is not None

    def getStateTransactionItem(self, descriptorHandle):
        """ if transaction has a state with given handle, return the transaction-item, otherwise None.  """
        for lookup in (self.metricStateUpdates,
                       self.alertStateUpdates,
                       self.componentStateUpdates,
                       self.contextStateUpdates,
                       self.operationalStateUpdates,
                       self.rtSampleStateUpdates):
            if descriptorHandle in lookup:
                return lookup[descriptorHandle]

    @tr_method_wrapper
    def addState(self, stateContainer, adjustStateVersion=True):
        my_multi_key = self._deviceMdibContainer.states
        my_updates = []
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
            my_multi_key = self._deviceMdibContainer.contextStates
        elif stateContainer.isRealtimeSampleArrayMetricState:
            my_updates = self.rtSampleStateUpdates
        elif stateContainer.NODETYPE == domTag('ScoState'):
            # special case ScoState Draft6: cannot notify updates, it is a category of its own that does not fit anywhere
            # This is a bug in the spec, not in this implementation!
            return

        descriptorHandle = stateContainer.descriptorHandle
        my_handle = stateContainer.Handle if stateContainer.isContextState else stateContainer.descriptorHandle
        if my_handle in my_updates:
            raise ValueError('descriptorHandle {} already in updated set!'.format(descriptorHandle))

        if adjustStateVersion:
            my_multi_key.setVersion(stateContainer)
        my_updates[descriptorHandle] = _TrItem(None, stateContainer)  # old, new

    def ungetState(self, stateContainer):
        """ forget a state that was provided before by a getXXXState call"""
        for lookup in (self.alertStateUpdates, self.componentStateUpdates, self.contextStateUpdates,
                       self.metricStateUpdates, self.operationalStateUpdates, self.rtSampleStateUpdates):
            if stateContainer.descriptorHandle in lookup:
                del lookup[stateContainer.descriptorHandle]

    @tr_method_wrapper
    def getMetricState(self, descriptorHandle, adjustStateVersion=True):
        """ Update a MetricState.
        When the transaction is committed, the modifications to the copy will be applied to the original version,
        and notification messages will be sent to clients.
        :param descriptorHandle: the descriptorHandle of the object that shall be read
        :param adjustStateVersion: if True, and a state with this handle does not exist, but was already present in this mdib before,
          the StateVersion of descriptorContainer is set to last known version for this handle +1
        @return: a copy of the state.
        """
        if descriptorHandle in self.metricStateUpdates:
            raise ValueError('descriptorHandle {} already in updated set!'.format(descriptorHandle))
        old_state, new_state = self._get_or_mk_StateContainer(descriptorHandle, adjustStateVersion)
        if not new_state.isMetricState:
            raise ValueError('descriptorHandle {} does not reference a metric state'.format(descriptorHandle))
        self.metricStateUpdates[descriptorHandle] = _TrItem(old_state, new_state)
        return new_state

    @tr_method_wrapper
    def getComponentState(self, descriptorHandle, adjustStateVersion=True):
        """ Update a ComponentState.
        When the transaction is committed, the modifications to the copy will be applied to the original version,
        and notification messages will be sent to clients.
        :param descriptorHandle: the descriptorHandle of the object that shall be read
        :param adjustStateVersion: if True, and a state with this handle does not exist, but was already present in this mdib before,
          the StateVersion of descriptorContainer is set to last known version for this handle +1
        @return: a copy of the state.
        """
        if descriptorHandle in self.componentStateUpdates:
            raise ValueError('descriptorHandle {} already in updated set!'.format(descriptorHandle))
        old_state, new_state = self._get_or_mk_StateContainer(descriptorHandle, adjustStateVersion)
        if not new_state.isComponentState:
            raise ValueError('descriptorHandle {} does not reference a component state'.format(descriptorHandle))
        self.componentStateUpdates[descriptorHandle] = _TrItem(old_state, new_state)
        return new_state

    @tr_method_wrapper
    def getAlertState(self, descriptorHandle, adjustStateVersion=True):
        """ Update AlertConditionState or AlertSignalState node
        When the transaction is committed, the modifications to the copy will be applied to the original version,
        and notification messages will be sent to clients.
        :param descriptorHandle: the descriptorHandle of the object that shall be read
        :param adjustStateVersion: if True, and a state with this handle does not exist, but was already present
          in this mdib before,
          the StateVersion of descriptorContainer is set to last known version for this handle +1
        @return: a copy of the state.
        """
        if descriptorHandle in self.alertStateUpdates:
            raise ValueError('descriptorHandle {} already in updated set!'.format(descriptorHandle))
        old_state, new_state = self._get_or_mk_StateContainer(descriptorHandle, adjustStateVersion)
        if not new_state.isAlertState:
            raise ValueError('descriptorHandle {} does not reference an alert state'.format(descriptorHandle))
        self.alertStateUpdates[descriptorHandle] = _TrItem(old_state, new_state)
        return new_state

    @tr_method_wrapper
    def getContextState(self, descriptorHandle, contextStateHandle=None, adjustStateVersion=True):
        """ Create or Update a ContextState.
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
        """
        lookup_key = (descriptorHandle, contextStateHandle)
        if lookup_key in self.contextStateUpdates:
            raise ValueError('descriptorHandle {} already in updated set!'.format(lookup_key))
        descriptorContainer = self._getDescriptorInTransaction(descriptorHandle)
        if contextStateHandle is None:
            oldStateContainer = None
            newStateContainer = self._deviceMdibContainer.mkStateContainerFromDescriptor(descriptorContainer)
            newStateContainer.BindingMdibVersion = self._deviceMdibContainer.mdibVersion  # auto-set this Attribute
            newStateContainer.BindingStartTime = time.time()  # auto-set this Attribute
        else:
            oldStateContainer = self._deviceMdibContainer.contextStates.handle.getOne(contextStateHandle,
                                                                                      allowNone=True)
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

    @tr_method_wrapper
    def addContextState(self, contextStateContainer, adjustStateVersion=True):
        """ Add a new ContextState.
        :param contextStateContainer: a ContextStateContainer instance
        :param adjustStateVersion: if True, and a state with this handle does not exist,
          but was already present in this mdib before,
          the StateVersion of descriptorContainer is set to last known version for this handle +1
        """
        lookup_key = (contextStateContainer.descriptorHandle, contextStateContainer.Handle)
        if lookup_key in self.contextStateUpdates:
            raise ValueError('descriptorHandle {} already in updated set!'.format(lookup_key))
        if adjustStateVersion:
            self._deviceMdibContainer.contextStates.setVersion(contextStateContainer)
        self.contextStateUpdates[lookup_key] = _TrItem(None, contextStateContainer)

    @tr_method_wrapper
    def getOperationalState(self, descriptorHandle, adjustStateVersion=True):
        """ Update an OperationalState.
        When the transaction is committed, the modifications to the copy will be applied to the original version,
        and notification messages will be sent to clients.
        :param descriptorHandle: the descriptorHandle of the object that shall be read
        :param adjustStateVersion: if True, and a state with this handle does not exist, but was already present in this mdib before,
          the StateVersion of descriptorContainer is set to last known version for this handle +1
        @return: a copy of the state.
        """
        if descriptorHandle in self.operationalStateUpdates:
            raise ValueError('descriptorHandle {} already in updated set!'.format(descriptorHandle))
        old_state, new_state = self._get_or_mk_StateContainer(descriptorHandle, adjustStateVersion)
        if not new_state.isOperationalState:
            raise ValueError('descriptorHandle {} does not reference an operational state '
                             '({})'.format(descriptorHandle, new_state.__class__.__name__))
        self.operationalStateUpdates[descriptorHandle] = _TrItem(old_state, new_state)
        return new_state

    @tr_method_wrapper
    def getRealTimeSampleArrayMetricState(self, descriptorHandle):
        if descriptorHandle in self.rtSampleStateUpdates:
            raise ValueError('descriptorHandle {} already in updated set!'.format(descriptorHandle))
        state_container = self._deviceMdibContainer.states.descriptorHandle.getOne(descriptorHandle, allowNone=True)
        if state_container is None:
            descriptorContainer = self._getDescriptorInTransaction(descriptorHandle)
            new_state = self._deviceMdibContainer.mkStateContainerFromDescriptor(descriptorContainer)
            if not new_state.isRealtimeSampleArrayMetricState:
                raise ValueError(
                    'descriptorHandle {} does not reference a RealTimeSampleArrayMetricState'.format(descriptorHandle))
            self._deviceMdibContainer.states.addObject(state_container)
        else:
            if not state_container.isRealtimeSampleArrayMetricState:
                raise ValueError(
                    'descriptorHandle {} does not reference a RealTimeSampleArrayMetricState'.format(descriptorHandle))
            new_state = state_container.mkCopy(copy_node=False)
            new_state.incrementState()
        self.rtSampleStateUpdates[descriptorHandle] = _TrItem(state_container, new_state)
        return new_state


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
        super(DeviceMdibContainer, self).__init__(sdc_definitions)
        self._logger = loghelper.getLoggerAdapter('sdc.device.mdib', log_prefix)
        self._sdcDevice = None
        self._trLock = Lock()  # transaction lock

        self.sequenceId = uuid.uuid4().urn  # this uuid identifies this mdib instance

        self._currentLocation = None  # or a SdcLocation instance
        self._annotators = {}
        self._current_transaction = None

        self.preCommitHandler = None  # preCommitHandler can modify transaction if needed before it is committed
        self.postCommitHandler = None  # postCommitHandler can modify mdib if needed after it is committed
        self._waveform_source = waveform_source or DefaultWaveformSource()

    @contextmanager
    def mdibUpdateTransaction(self, setDeterminationTime=True):
        # pylint: disable=protected-access
        with self._trLock:
            try:
                self._current_transaction = _MdibUpdateTransaction(self)
                yield self._current_transaction
                if callable(self.preCommitHandler):
                    self.preCommitHandler(self, self._current_transaction)  # pylint: disable=not-callable
                if self._current_transaction._error:
                    self._logger.info('mdibUpdateTransaction: transaction without updates!')
                else:
                    self._process_transaction(setDeterminationTime)
                    if callable(self.postCommitHandler):
                        self.postCommitHandler(self, self._current_transaction)  # pylint: disable=not-callable
            finally:
                self._current_transaction = None

    @contextmanager
    def _rt_sample_transaction(self):
        with self._trLock:
            with self.mdibLock:
                try:
                    self._current_transaction = _RtDataMdibUpdateTransaction(self)
                    yield self._current_transaction
                    if callable(self.preCommitHandler):
                        self.preCommitHandler(self, self._current_transaction)  # pylint: disable=not-callable
                    if self._current_transaction._error:
                        self._logger.info('_rtsampleTransaction: transaction without updates!')
                    else:
                        self._process_internal_rt_transaction()
                        if callable(self.postCommitHandler):
                            self.postCommitHandler(self, self._current_transaction)  # pylint: disable=not-callable
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
        if len(mgr.descriptorUpdates) > 0:
            self.mdDescriptionVersion += 1
            increment_mdib_version = True

        # BICEPS: The version number is incremented by one every time the state part changes.
        if len(mgr.metricStateUpdates) > 0 or len(mgr.alertStateUpdates) > 0 \
                or len(mgr.componentStateUpdates) > 0 or len(mgr.contextStateUpdates) > 0 \
                or len(mgr.operationalStateUpdates) > 0 or len(mgr.rtSampleStateUpdates) > 0:
            self.mdStateVersion += 1
            increment_mdib_version = True

        if increment_mdib_version:
            self.mdibVersion += 1

        # handle descriptors
        if len(mgr.descriptorUpdates) > 0:
            # need to know all to be deleted and to be created descriptors
            to_be_deleted = [old for old, new in mgr.descriptorUpdates.values() if new is None]
            to_be_created = [new for old, new in mgr.descriptorUpdates.values() if old is None]
            to_be_deleted_handles = [d.handle for d in to_be_deleted]
            to_be_created_handles = [d.handle for d in to_be_created]
            with self.mdibLock:

                def _updateCorrespondingState(descriptorContainer):
                    # add state to updated_states list and to corresponding notifications input
                    # => the state is always sent twice, a) in the description modification report and b)
                    # in the specific state update notification.
                    if descriptorContainer.isAlertDescriptor:
                        update_dict = mgr.alertStateUpdates
                    elif descriptorContainer.isComponentDescriptor:
                        update_dict = mgr.componentStateUpdates
                    elif descriptorContainer.isContextDescriptor:
                        update_dict = mgr.contextStateUpdates
                    elif descriptorContainer.isRealtimeSampleArrayMetricDescriptor:
                        update_dict = mgr.rtSampleStateUpdates
                    elif descriptorContainer.isMetricDescriptor:
                        update_dict = mgr.metricStateUpdates
                    elif descriptorContainer.isOperationalDescriptor:
                        update_dict = mgr.operationalStateUpdates
                    else:
                        raise RuntimeError(f'do not know how to handle {descriptorContainer.__class__.__name__}')
                    if descriptorContainer.isContextDescriptor:
                        update_dict = mgr.contextStateUpdates
                        all_states = self.contextStates.descriptorHandle.get(descriptorContainer.handle, [])
                        for st in all_states:
                            key = (descriptorContainer.Handle, st.Handle)
                            # check if state is already present in this transaction
                            state_update = update_dict.get(key)
                            if state_update is not None:
                                # the state has also been updated directly in transaction.
                                # update descriptor version
                                old_state, new_state = state_update
                            else:
                                old_state = st
                                new_state = old_state.mkCopy()
                                update_dict[key] = _TrItem(old_state, new_state)
                            new_state.descriptorContainer = descriptorContainer
                            new_state.incrementState()
                            new_state.updateDescriptorVersion()
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
                                raise ValueError(f'state deleted? that should not be possible! handle = '
                                                 f'{descriptorContainer.handle}')
                            new_state.descriptorContainer = descriptorContainer
                            new_state.updateDescriptorVersion()
                        else:
                            old_state = self.states.descriptorHandle.getOne(descriptorContainer.handle, allowNone=True)
                            if old_state is not None:
                                new_state = old_state.mkCopy()
                                new_state.descriptorContainer = descriptorContainer
                                new_state.incrementState()
                                new_state.updateDescriptorVersion()
                                update_dict[descriptorContainer.handle] = _TrItem(old_state, new_state)
                        if new_state is not None:
                            descr_updated_states.append(new_state)

                def _incrementParentDescriptorVersion(descriptorContainer):
                    parentDescriptorContainer = self.descriptions.handle.getOne(descriptorContainer.parentHandle)
                    parentDescriptorContainer.incrementDescriptorVersion()
                    descr_updated.append(parentDescriptorContainer)
                    _updateCorrespondingState(parentDescriptorContainer)

                # handling only updated states here: If a descriptor is created, I assume that the application also creates the state in an transaction.
                # The state will then be transported via that notification report.
                # Maybe this needs to be reworked, but at the time of this writing it seems fine.
                for tr_item in mgr.descriptorUpdates.values():
                    origDescriptor, newDescriptor = tr_item.old, tr_item.new
                    if newDescriptor is not None:
                        newDescriptor.updateNode(setXsiType=True)
                        # DescriptionModificationReport also contains the states that are related to the descriptors.
                        # => if there is one, update its DescriptorVersion and add it to list of states that shall be sent
                        # (Assuming that context descriptors (patient, location) are never changed,
                        #  additional check for states in self.contextStates is not needed.
                        #  If this assumption is wrong, that functionality must be added!)
                        _updateCorrespondingState(newDescriptor)
                    if origDescriptor is None:
                        # this is a create operation
                        self._logger.debug('mdibUpdateTransaction: new descriptor Handle={}, DescriptorVersion={}',
                                           newDescriptor.handle, newDescriptor.DescriptorVersion)
                        descr_created.append(newDescriptor.mkCopy())
                        self.descriptions.addObjectNoLock(newDescriptor)
                        # R0033: A SERVICE PROVIDER SHALL increment pm:AbstractDescriptor/@DescriptorVersion by one if a direct child descriptor is added or deleted.
                        if newDescriptor.parentHandle is not None and \
                                newDescriptor.parentHandle not in to_be_created_handles:
                            # only update parent if it is not also created in this transaction
                            _incrementParentDescriptorVersion(newDescriptor)
                    elif newDescriptor is None:
                        # this is a delete operation
                        self._logger.debug('mdibUpdateTransaction: rm descriptor Handle={}, DescriptorVersion={}',
                                           origDescriptor.handle, origDescriptor.DescriptorVersion)
                        all_descriptors = self.getAllDescriptorsInSubTree(origDescriptor)
                        self._rmDescriptorsAndStates(all_descriptors)
                        descr_deleted.extend(all_descriptors)
                        # R0033: A SERVICE PROVIDER SHALL increment pm:AbstractDescriptor/@DescriptorVersion by one if a direct child descriptor is added or deleted.
                        if origDescriptor.parentHandle is not None and \
                                origDescriptor.parentHandle not in to_be_deleted_handles:
                            # only update parent if it is not also deleted in this transaction
                            _incrementParentDescriptorVersion(origDescriptor)
                    else:
                        # this is an update operation
                        descr_updated.append(newDescriptor)
                        self._logger.debug('mdibUpdateTransaction: update descriptor Handle={}, DescriptorVersion={}',
                                           newDescriptor.handle, newDescriptor.DescriptorVersion)
                        self.descriptions.replaceObjectNoLock(newDescriptor)

        # handle metric states
        if len(mgr.metricStateUpdates) > 0:
            with self.mdibLock:
                # self.mdibVersion += 1
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
                        metric_updates.append(newstate)
                    except RuntimeError:
                        self._logger.warn('mdibUpdateTransaction: {} did not exist before!! really??', newstate)
                        raise

        # handle alert states
        if len(mgr.alertStateUpdates) > 0:
            with self.mdibLock:
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
                        alert_updates.append(newstate)
                    except RuntimeError:
                        self._logger.warn('mdibUpdateTransaction: {} did not exist before!! really??', newstate)
                        raise

            # handle component state states
        if len(mgr.componentStateUpdates) > 0:
            with self.mdibLock:
                # self.mdibVersion += 1
                self._logger.debug('mdibUpdateTransaction: component State updates = {}', mgr.componentStateUpdates)
                for value in mgr.componentStateUpdates.values():
                    oldstate, newstate = value.old, value.new
                    try:
                        newstate.updateNode()
                        # replace the old container with the new one
                        self.states.removeObjectNoLock(oldstate)
                        self.states.addObjectNoLock(newstate)
                        comp_updates.append(newstate)
                    except RuntimeError:
                        self._logger.warn('mdibUpdateTransaction: {} did not exist before!! really??', newstate)
                        raise

        # handle context states
        if len(mgr.contextStateUpdates) > 0:
            with self.mdibLock:
                self._logger.debug('mdibUpdateTransaction: contextState updates = {}', mgr.contextStateUpdates)
                for value in mgr.contextStateUpdates.values():
                    oldstate, newstate = value.old, value.new
                    try:
                        ctxt_updates.append(newstate)
                        # replace the old container with the new one
                        self.contextStates.removeObjectNoLock(oldstate)
                        self.contextStates.addObjectNoLock(newstate)
                        newstate.updateNode()
                    except RuntimeError:
                        self._logger.warn('mdibUpdateTransaction: {} did not exist before!! really??', newstate)
                        raise

        # handle operational states
        if len(mgr.operationalStateUpdates) > 0:
            with self.mdibLock:
                self._logger.debug('mdibUpdateTransaction: operationalState updates = {}', mgr.operationalStateUpdates)
                for value in mgr.operationalStateUpdates.values():
                    oldstate, newstate = value.old, value.new
                    try:
                        newstate.updateNode()
                        self.states.removeObjectNoLock(oldstate)
                        self.states.addObjectNoLock(newstate)
                        op_updates.append(newstate)
                    except RuntimeError:
                        self._logger.warn('mdibUpdateTransaction: {} did not exist before!! really??', newstate)
                        raise

        # handle real time samples
        # important note: this transaction does not pull values from registered waveform providers!
        # Application is responsible for providing data.
        if len(mgr.rtSampleStateUpdates) > 0:
            with self.mdibLock:
                self._logger.debug('mdibUpdateTransaction: rtSample updates = {}', mgr.rtSampleStateUpdates)
                for value in mgr.rtSampleStateUpdates.values():
                    oldstate, newstate = value.old, value.new
                    try:
                        newstate.updateNode()
                        self.states.removeObjectNoLock(oldstate)
                        self.states.addObjectNoLock(newstate)
                        rt_updates.append(newstate)
                    except RuntimeError:
                        self._logger.warn('mdibUpdateTransaction: {} did not exist before!! really??', newstate)
                        raise
                    except:
                        raise

        mdib_version_grp = self.mdib_version_group
        if self._sdcDevice is not None:
            if len(mgr.descriptorUpdates) > 0:
                updated = [d.mkCopy() for d in descr_updated]
                created = [d.mkCopy() for d in descr_created]
                deleted = [d.mkCopy() for d in descr_deleted]
                updated_states = [s.mkCopy() for s in descr_updated_states]
                self._sdcDevice.sendDescriptorUpdates(mdib_version_grp, updated=updated, created=created, deleted=deleted,
                                                      updated_states=updated_states)
            if len(metric_updates) > 0:
                updates = [s.mkCopy() for s in metric_updates]
                self._sdcDevice.sendMetricStateUpdates(mdib_version_grp, updates)
            if len(alert_updates) > 0:
                updates = [s.mkCopy() for s in alert_updates]
                self._sdcDevice.sendAlertStateUpdates(mdib_version_grp, updates)
            if len(comp_updates) > 0:
                updates = [s.mkCopy() for s in comp_updates]
                self._sdcDevice.sendComponentStateUpdates(mdib_version_grp, updates)
            if len(ctxt_updates) > 0:
                updates = [s.mkCopy() for s in ctxt_updates]
                self._sdcDevice.sendContextStateUpdates(mdib_version_grp, updates)
            if len(op_updates) > 0:
                updates = [s.mkCopy() for s in op_updates]
                self._sdcDevice.sendOperationalStateUpdates(mdib_version_grp, updates)
            if len(rt_updates) > 0:
                updates = [s.mkCopy() for s in rt_updates]
                self._sdcDevice.sendRealtimeSamplesStateUpdates(mdib_version_grp, updates)
        mgr.mdib_version = self.mdibVersion

    def _process_internal_rt_transaction(self):
        mgr = self._current_transaction
        # handle real time samples
        if len(mgr.rtSampleStateUpdates) > 0:
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
            # makes copies of all states for sending, so that they can't be affected by transactions after this one
            updates = [s.mkCopy() for s in updates]
            if self._sdcDevice is not None:
                self._sdcDevice.sendRealtimeSamplesStateUpdates(self.mdib_version_group, updates)

        mgr.mdib_version = self.mdibVersion

    def setSdcDevice(self, sdcDevice):
        self._sdcDevice = sdcDevice

    def setLocation(self, sdcLocation, validators=None):
        """
        This method updates only the mdib internal data!
        use the SdcDevice.setLocation method if you want to publish the address an the network.
        @param sdcLocation: a pysdc.location.SdcLocation instance
        @param validators: a list of pysdc.pmtypes.InstanceIdentifier objects or None
        """
        allLocationContexts = self.contextStates.NODETYPE.get(domTag('LocationContextState'), [])
        with self.mdibUpdateTransaction() as mgr:
            # set all to currently associated Locations to Disassociated
            associatedLocations = [l for l in allLocationContexts if
                                   l.ContextAssociation == pmtypes.ContextAssociation.ASSOCIATED]
            for l in associatedLocations:
                locationContext = mgr.getContextState(l.descriptorHandle, l.Handle)
                locationContext.ContextAssociation = pmtypes.ContextAssociation.DISASSOCIATED
                # UnbindingMdibVersion is the first version in which it is no longer bound ( == this version)
                locationContext.UnbindingMdibVersion = self.mdibVersion
            descriptorContainer = self.descriptions.NODETYPE.getOne(domTag('LocationContextDescriptor'))

            self._currentLocation = mgr.getContextState(descriptorContainer.handle)  # this creates a new location state
            self._currentLocation.updateFromSdcLocation(sdcLocation)
            if validators is not None:
                self._currentLocation.Validator = validators

    def _createDescriptorContainer(self, cls, nodeName, handle, parentHandle, codedValue, safetyClassification,
                                   ext_extension=None):
        obj = cls(nsmapper=self.nsmapper,
                  nodeName=nodeName,
                  handle=handle,
                  parentHandle=parentHandle,
                  )
        obj.SafetyClassification = safetyClassification
        obj.Type = codedValue
        if ext_extension:
            obj.ext_Extension = etree_.Element(extTag('Extension'))
            for node in ext_extension:
                obj.ext_Extension.append(node)
        obj.updateNode()
        return obj

    def createVmdDescriptorContainer(self, handle, parentHandle, codedValue, safetyClassification, ext_extension=None):
        """
        This method creates an VmdDescriptorContainer with the given properties.
        If it is called within an transaction, the created object is added to transaction and clients will be notified.
        Otherwise the object is only added to mdib without sending notifications to clients!
        :param handle: Handle of the new container
        :param parentHandle: Handle of the parent
        :param codedValue: a pmtypes.CodedValue instance that defines what this object represents in medical terms.
        :param safetyClassification: a pmtypes.SafetyClassification value
        :param ext_extension: list of etree.Element elements to be added to the Extension element
        :return: the created object
        """
        qNameTag = domTag('Vmd')
        cls = self.getDescriptorContainerClass(qNameTag)
        obj = self._createDescriptorContainer(cls, qNameTag, handle, parentHandle, codedValue, safetyClassification,
                                              ext_extension)
        if self._current_transaction is not None:
            self._current_transaction.createDescriptor(obj)
        else:
            self.descriptions.addObject(obj)
        return obj

    def createChannelDescriptorContainer(self, handle, parentHandle, codedValue, safetyClassification,
                                         ext_extension=None):
        """
        This method creates a ChannelDescriptorContainer with the given properties and optionally adds it to the mdib.
        If it is called within an transaction, the created object is added to transaction and clients will be notified.
        Otherwise the object is only added to mdib without sending notifications to clients!
        :param handle: Handle of the new container
        :param parentHandle: Handle of the parent
        :param codedValue: a pmtypes.CodedValue instance that defines what this object represents in medical terms.
        :param safetyClassification: a pmtypes.SafetyClassification value
        :param ext_extension: list of etree.Element elements to be added to the Extension element
        :return: the created object
        """
        qNameTag = domTag('Channel')
        cls = self.getDescriptorContainerClass(qNameTag)
        obj = self._createDescriptorContainer(cls, qNameTag, handle, parentHandle, codedValue, safetyClassification,
                                              ext_extension)
        if self._current_transaction is not None:
            self._current_transaction.createDescriptor(obj)
        else:
            self.descriptions.addObject(obj)
        return obj

    def createStringMetricDescriptorContainer(self, handle, parentHandle, codedValue, safetyClassification, unit,
                                              metricAvailability=pmtypes.MetricAvailability.INTERMITTENT,
                                              metricCategory=pmtypes.MetricCategory.UNSPECIFIED, ext_extension=None):
        """
        This method creates a StringMetricDescriptorContainer with the given properties and optionally adds it to the mdib.
        If it is called within an transaction, the created object is added to transaction and clients will be notified.
        Otherwise the object is only added to mdib without sending notifications to clients!
        :param handle: Handle of the new container
        :param parentHandle: Handle of the parent
        :param codedValue: a pmtypes.CodedValue instance that defines what this object represents in medical terms.
        :param safetyClassification: a pmtypes.SafetyClassification value
        :param unit: a CodedValue
        :param metricAvailability: pmtypes.MetricAvailability
        :param metricCategory: pmtypes.MetricCategory
        :param ext_extension: list of etree.Element elements to be added to the Extension element
        :return: the created object
        """
        qNameTag = domTag('Metric')
        qNameType = domTag('StringMetricDescriptor')
        cls = self.getDescriptorContainerClass(qNameType)
        obj = self._createDescriptorContainer(cls, qNameTag, handle, parentHandle, codedValue, safetyClassification,
                                              ext_extension)
        obj.Unit = unit
        obj.MetricAvailability = metricAvailability
        obj.MetricCategory = metricCategory
        obj.updateNode()
        if self._current_transaction is not None:
            self._current_transaction.createDescriptor(obj)
        else:
            self.descriptions.addObject(obj)
        return obj

    def createEnumStringMetricDescriptorContainer(self, handle, parentHandle, codedValue, safetyClassification,
                                                  unit, allowedValues,
                                                  metricAvailability=pmtypes.MetricAvailability.INTERMITTENT,
                                                  metricCategory=pmtypes.MetricCategory.UNSPECIFIED,
                                                  ext_extension=None):
        """
        This method creates an EnumStringMetricDescriptorContainer with the given properties and optionally adds it
        to the mdib.
        If it is called within an transaction, the created object is added to transaction and clients will be notified.
        Otherwise the object is only added to mdib without sending notifications to clients!
        :param handle: Handle of the new container
        :param parentHandle: Handle of the parent
        :param codedValue: a pmtypes.CodedValue instance that defines what this object represents in medical terms.
        :param safetyClassification: a pmtypes.SafetyClassification value
        :param unit: pmtypes.CodedValue
        :param allowedValues:
        :param metricAvailability: pmtypes.MetricAvailability
        :param metricCategory: pmtypes.MetricCategory
        :param ext_extension: list of etree.Element elements to be added to the Extension element
        :return: the created object
        """
        qNameTag = domTag('Metric')
        qNameType = domTag('EnumStringMetricDescriptor')
        cls = self.getDescriptorContainerClass(qNameType)
        obj = self._createDescriptorContainer(cls, qNameTag, handle, parentHandle, codedValue, safetyClassification,
                                              ext_extension)
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

    def createClockDescriptorContainer(self, handle, parentHandle, codedValue, safetyClassification,
                                       ext_extension=None):
        """
        This method creates a ClockDescriptorContainer with the given properties.
        If it is called within an transaction, the created object is added to transaction and clients will be notified.
        Otherwise the object is only added to mdib without sending notifications to clients!
        :param handle: Handle of the new container
        :param parentHandle: Handle of the parent
        :param codedValue: a pmtypes.CodedValue instance that defines what this object represents in medical terms.
        :param safetyClassification: a pmtypes.SafetyClassification value
        :param ext_extension: list of etree.Element elements to be added to the Extension element
        :return: the created object
        """
        cls = self.getDescriptorContainerClass(domTag('ClockDescriptor'))
        obj = self._createDescriptorContainer(cls, domTag('Clock'), handle, parentHandle, codedValue,
                                              safetyClassification, ext_extension)
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
                    raise ValueError(
                        'state descriptorHandle {} already in mdib!'.format(stateContainer.descriptorHandle))
                if adjustStateVersion:
                    self.states.setVersion(stateContainer)

    def addMdsNode(self, mdsNode):
        """
        This method creates DescriptorContainers and StateContainers from the provided dom tree.
        If it is called within an transaction, the created objects are added to transaction and clients will be notified.
        Otherwise the objects are only added to mdib without sending notifications to clients!
        :param mdsNode: a node representing data of a complete mds
        :return: None
        """
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
        self._waveform_source.register_waveform_generator(self, descriptorHandle, wfGenerator)

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

        mdib = cls(protocol_definition, log_prefix=log_prefix)
        root = msgreader.MessageReader.getMdibRootNode(mdib.sdc_definitions, xml_text)
        mdib.sdc_definitions.xml_validator.assertValid(root)
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
                    descr_cls = mdib.getDescriptorContainerClass(qn)
                    lc = descr_cls(mdib.nsmapper, nodeName=domTag('LocationContext'),
                                   handle=uuid.uuid4().hex, parentHandle=systemContextContainer.handle)
                    lc.SafetyClassification = pmtypes.SafetyClassification.INF
                    mdib.descriptions.addObject(lc)
            if createPatientContextDescr:
                qn = domTag('PatientContextDescriptor')
                if qn not in childdrenNodeNames:
                    mdib._logger.info('creating a PatientContextDescriptor')
                    descr_cls = mdib.getDescriptorContainerClass(qn)
                    pc = descr_cls(mdib.nsmapper, nodeName=domTag('PatientContext'),
                                   handle=uuid.uuid4().hex, parentHandle=systemContextContainer.handle)
                    pc.SafetyClassification = pmtypes.SafetyClassification.INF
                    mdib.descriptions.addObject(pc)
        mdib.mkStateContainersforAllDescriptors()
        return mdib
