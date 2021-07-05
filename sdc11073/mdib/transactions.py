import time
from collections import OrderedDict, namedtuple
from functools import wraps
from ..namespaces import domTag

TrItem = namedtuple('TrItem', 'old new') # a named tuple for better readability of code


class _TransactionBase(object):
    def __init__(self, device_mdib_container):
        self._deviceMdibContainer = device_mdib_container
        self.descriptorUpdates = OrderedDict()
        self.metricStateUpdates = OrderedDict()
        self.alertStateUpdates = OrderedDict()
        self.componentStateUpdates = OrderedDict()
        self.contextStateUpdates = OrderedDict()
        self.operationalStateUpdates = OrderedDict()
        self.rtSampleStateUpdates = dict()   # unordered dict for performance
        self._error = False
        self._closed = False
        self.mdib_version = None

    def _get_descriptor_in_transaction(self, descriptorHandle):
        """ looks for new or updated descriptor in current transaction and in mdib"""
        tr_containers = self.descriptorUpdates.get(descriptorHandle)
        if tr_containers is not None:
            old, new = tr_containers
            if new is None: # descriptor is deleted in this transaction!
                raise RuntimeError('The descriptor {} is going to be deleted'.format(descriptorHandle))
            else:
                return new
        else:
            return self._deviceMdibContainer.descriptions.handle.getOne(descriptorHandle)

    def _get_or_mk_state_container(self, descriptorHandle, adjustStateVersion=True):
        """ returns oldContainer, newContainer"""
        descriptorContainer = self._get_descriptor_in_transaction(descriptorHandle)
        old_stateContainer = self._deviceMdibContainer.states.descriptorHandle.getOne(descriptorContainer.handle, allowNone=True)
        if old_stateContainer is None:
            # create a new state object
            new_stateContainer = self._deviceMdibContainer.mkStateContainerFromDescriptor(descriptorContainer)
            if adjustStateVersion:
                self._deviceMdibContainer.states.setVersion(new_stateContainer)
        else:
            new_stateContainer = old_stateContainer.mkCopy()
            new_stateContainer.increment_state_version()
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


class RtDataMdibUpdateTransaction(_TransactionBase):
    """This transaction is only used internally to periodically send waveform notifications.
    It handles this specific purpose with less overhead compared to regular transaction."""
    def __init__(self, device_mdib_container):
        super().__init__(device_mdib_container)

    @tr_method_wrapper
    def get_real_time_sample_array_metric_state(self, descriptorHandle):
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
        stateContainer.increment_state_version()
        new_state = stateContainer  # supply old and new state; although identical, just do not break interface
        self.rtSampleStateUpdates[descriptorHandle] = TrItem(stateContainer, new_state)
        return new_state


class MdibUpdateTransaction(_TransactionBase):
    #pylint: disable=protected-access
    """ a helper class that collects multiple updates into one transaction.
    Used by contextmanager DeviceMdibContainer.mdibUpdateTransaction """
    def __init__(self, device_mdib_container):
        super().__init__(device_mdib_container)

    @tr_method_wrapper
    def add_descriptor(self, descriptorContainer, adjustDescriptorVersion=True):
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
        self.descriptorUpdates[descriptorHandle] = TrItem(None, descriptorContainer)

    createDescriptor = add_descriptor

    @tr_method_wrapper
    def remove_descriptor(self, descriptorHandle):
        if descriptorHandle in self.descriptorUpdates:
            raise ValueError('descriptorHandle {} already in updated set!'.format(descriptorHandle))
        origDescriptorContainer = self._deviceMdibContainer.descriptions.handle.getOne(descriptorHandle)
        self.descriptorUpdates[descriptorHandle] = TrItem(origDescriptorContainer, None)

    @tr_method_wrapper
    def get_descriptor(self, descriptorHandle):
        """ Update a descriptor.
        When the transaction is committed, the modifications to the copy will be applied to the original version,
        and notification messages will be sent to clients.
        @return: a copy of the state.
        """
        if descriptorHandle in self.descriptorUpdates:
            raise ValueError('descriptorHandle {} already in updated set!'.format(descriptorHandle))
        origDescriptorContainer = self._deviceMdibContainer.descriptions.handle.getOne(descriptorHandle)
        descriptorContainer = origDescriptorContainer.mkCopy()
        descriptorContainer.increment_descriptor_version()
        self.descriptorUpdates[descriptorHandle] = TrItem(origDescriptorContainer, descriptorContainer)
        return descriptorContainer
    # getDescriptor = get_descriptor

    def has_state(self, descriptorHandle):
        """ check if transaction has a state with given handle """
        return self.get_state_transaction_item(descriptorHandle) is not None

    def get_state_transaction_item(self, descriptorHandle):
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
    def add_state(self, stateContainer, adjustStateVersion=True):
        my_multi_key = self._deviceMdibContainer.states
        my_updates = []
        if stateContainer.isRealtimeSampleArrayMetricState:
            my_updates = self.rtSampleStateUpdates
        elif stateContainer.isMetricState:
            my_updates = self.metricStateUpdates
        elif stateContainer.isAlertState:
            my_updates = self.alertStateUpdates
        elif stateContainer.isComponentState:
            my_updates = self.componentStateUpdates
        elif stateContainer.isSystemContextState:
            my_updates = self.componentStateUpdates
        elif stateContainer.isOperationalState:
            my_updates = self.operationalStateUpdates
        elif stateContainer.isContextState:
            my_updates = self.contextStateUpdates
            my_multi_key = self._deviceMdibContainer.contextStates
        elif stateContainer.NODETYPE == domTag('ScoState'):
            #special case ScoState Draft6: cannot notify updates, it is a category of its own that does not fit anywhere
            # This is a bug in the spec, not in this implementation!
            return

        descriptorHandle = stateContainer.descriptorHandle
        my_handle = stateContainer.Handle if stateContainer.isContextState else stateContainer.descriptorHandle
        if my_handle in my_updates:
            raise ValueError('descriptorHandle {} already in updated set!'.format(descriptorHandle))

        if adjustStateVersion:
            my_multi_key.setVersion(stateContainer)
        my_updates[descriptorHandle] = TrItem(None, stateContainer) # old, new

    def unget_state(self, stateContainer):
        """ forget a state that was provided before by a getXXXState call"""
        for lookup in (self.alertStateUpdates, self.componentStateUpdates, self.contextStateUpdates,
                       self.metricStateUpdates, self.operationalStateUpdates, self.rtSampleStateUpdates):
            if stateContainer.descriptorHandle in lookup:
                del lookup[stateContainer.descriptorHandle]

    @tr_method_wrapper
    def get_state(self, descriptorHandle, handle=None, adjustStateVersion=True):
        descriptor_container =self._get_descriptor_in_transaction(descriptorHandle)
        if descriptor_container.isContextDescriptor:
            return self._get_context_state(descriptorHandle, handle, adjustStateVersion)
        if descriptor_container.isRealtimeSampleArrayMetricDescriptor:
            return self._get_real_time_sample_array_metric_state(descriptorHandle)

        if descriptor_container.isMetricDescriptor:
            updates = self.metricStateUpdates
        elif descriptor_container.isOperationalDescriptor:
            updates = self.operationalStateUpdates
        elif descriptor_container.isComponentDescriptor:
            updates = self.componentStateUpdates
        elif descriptor_container.isAlertDescriptor:
            updates =  self.alertStateUpdates
        else:
            raise RuntimeError(f'unhandled case {descriptor_container}')
        if descriptorHandle in updates:
            raise ValueError('descriptorHandle {} already in updated set!'.format(descriptorHandle))
        old_state, new_state = self._get_or_mk_state_container(descriptorHandle, adjustStateVersion)
        updates[descriptorHandle] = TrItem(old_state, new_state)
        return new_state

    def _get_context_state(self, descriptorHandle, contextStateHandle=None, adjustStateVersion=True):
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
        descriptorContainer = self._get_descriptor_in_transaction(descriptorHandle)
        if contextStateHandle is None:
            oldStateContainer = None
            newStateContainer = self._deviceMdibContainer.mkStateContainerFromDescriptor(descriptorContainer)
            newStateContainer.BindingMdibVersion = self._deviceMdibContainer.mdibVersion # auto-set this Attribute
            newStateContainer.BindingStartTime = time.time() # auto-set this Attribute
        else:
            oldStateContainer = self._deviceMdibContainer.contextStates.handle.getOne(contextStateHandle, allowNone=True)
            if oldStateContainer is not None:
                newStateContainer = oldStateContainer.mkCopy()
                newStateContainer.increment_state_version()
            else:
                newStateContainer = self._deviceMdibContainer.mkStateContainerFromDescriptor(descriptorContainer)
                newStateContainer.BindingMdibVersion = self._deviceMdibContainer.mdibVersion  # auto-set this Attribute
                newStateContainer.BindingStartTime = time.time()  # auto-set this Attribute
                newStateContainer.Handle = contextStateHandle
                if adjustStateVersion:
                    self._deviceMdibContainer.contextStates.setVersion(newStateContainer)
        self.contextStateUpdates[lookup_key] = TrItem(oldStateContainer, newStateContainer)
        return newStateContainer

    def _get_real_time_sample_array_metric_state(self, descriptorHandle):
        if descriptorHandle in self.rtSampleStateUpdates:
            raise ValueError('descriptorHandle {} already in updated set!'.format(descriptorHandle))
        state_container = self._deviceMdibContainer.states.descriptorHandle.getOne(descriptorHandle, allowNone=True)
        if state_container is None:
            descriptorContainer = self._get_descriptor_in_transaction(descriptorHandle)
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
            new_state.increment_state_version()
        self.rtSampleStateUpdates[descriptorHandle] = TrItem(state_container, new_state)
        return new_state
