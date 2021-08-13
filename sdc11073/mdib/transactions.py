import time
from collections import OrderedDict, namedtuple
from functools import wraps

from ..namespaces import domTag

TrItem = namedtuple('TrItem', 'old new')  # a named tuple for better readability of code


class _TransactionBase:
    def __init__(self, device_mdib_container):
        self._device_mdib_container = device_mdib_container
        self.descriptor_updates = OrderedDict()
        self.metric_state_updates = OrderedDict()
        self.alert_state_updates = OrderedDict()
        self.component_state_updates = OrderedDict()
        self.context_state_updates = OrderedDict()
        self.operational_state_updates = OrderedDict()
        self.rt_sample_state_updates = dict()  # unordered dict for performance
        self._error = False
        self._closed = False
        self.mdib_version = None

    def _get_descriptor_in_transaction(self, descriptor_handle):
        """ looks for new or updated descriptor in current transaction and in mdib"""
        tr_containers = self.descriptor_updates.get(descriptor_handle)
        if tr_containers is not None:
            _, new = tr_containers
            if new is None:  # descriptor is deleted in this transaction!
                raise RuntimeError('The descriptor {} is going to be deleted'.format(descriptor_handle))
            return new
        return self._device_mdib_container.descriptions.handle.get_one(descriptor_handle)

    def _get_or_mk_state_container(self, descriptor_handle, adjust_state_version=True):
        """ returns oldContainer, newContainer"""
        descriptor_container = self._get_descriptor_in_transaction(descriptor_handle)
        old_state_container = self._device_mdib_container.states.descriptorHandle.get_one(descriptor_container.handle,
                                                                                          allow_none=True)
        if old_state_container is None:
            # create a new state object
            new_state_container = self._device_mdib_container.mk_state_container_from_descriptor(descriptor_container)
            if adjust_state_version:
                self._device_mdib_container.states.set_version(new_state_container)
        else:
            new_state_container = old_state_container.mk_copy()
            new_state_container.increment_state_version()
        return old_state_container, new_state_container

    @property
    def error(self):
        return self._error

def tr_method_wrapper(method):
    """a decorator for consistency checks and error handling"""

    @wraps(method)
    def wrapper(self, *args, **kwargs):
        # pylint: disable=protected-access
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

    @tr_method_wrapper
    def get_real_time_sample_array_metric_state(self, descriptor_handle):
        # for performance reasons, this method does not return a copy of the original object.
        # This means no rollback possible.
        if descriptor_handle in self.rt_sample_state_updates:
            raise ValueError(f'descriptorHandle {descriptor_handle} already in updated set!')
        state_container = self._device_mdib_container.states.descriptorHandle.get_one(descriptor_handle,
                                                                                      allow_none=True)
        if state_container is None:
            raise ValueError(f'state {descriptor_handle} not found!')

        if not state_container.isRealtimeSampleArrayMetricState:
            raise ValueError(
                f'descriptorHandle {descriptor_handle} does not reference a RealTimeSampleArrayMetricState')
        state_container.increment_state_version()
        new_state = state_container  # supply old and new state; although identical, just do not break interface
        self.rt_sample_state_updates[descriptor_handle] = TrItem(state_container, new_state)
        return new_state


class MdibUpdateTransaction(_TransactionBase):
    # pylint: disable=protected-access
    """ a helper class that collects multiple updates into one transaction.
    Used by contextmanager DeviceMdibContainer.transaction_manager """

    @tr_method_wrapper
    def add_descriptor(self, descriptor_container, adjust_descriptor_version=True):
        """
        :param descriptor_container: the object that shall be added to mdib
        :param adjust_descriptor_version: if True, and a descriptor with this handle does not exist, but was already present in this mdib before,
          the DescriptorVersion of descriptor_container is set to last known version for this handle +1
        :return: None
        """
        descriptor_handle = descriptor_container.handle
        if adjust_descriptor_version:
            self._device_mdib_container.descriptions.set_version(descriptor_container)
        if descriptor_handle in self.descriptor_updates:
            raise ValueError('descriptorHandle {} already in updated set!'.format(descriptor_handle))
        if descriptor_handle in self._device_mdib_container.descriptions.handle.keys():
            raise ValueError('cannot create descriptorHandle {}, it already exists!'.format(descriptor_handle))
        self.descriptor_updates[descriptor_handle] = TrItem(None, descriptor_container)

    createDescriptor = add_descriptor

    @tr_method_wrapper
    def remove_descriptor(self, descriptor_handle):
        if descriptor_handle in self.descriptor_updates:
            raise ValueError('descriptorHandle {} already in updated set!'.format(descriptor_handle))
        orig_descriptor_container = self._device_mdib_container.descriptions.handle.get_one(descriptor_handle)
        self.descriptor_updates[descriptor_handle] = TrItem(orig_descriptor_container, None)

    @tr_method_wrapper
    def get_descriptor(self, descriptor_handle):
        """ Update a descriptor.
        When the transaction is committed, the modifications to the copy will be applied to the original version,
        and notification messages will be sent to clients.
        @return: a copy of the state.
        """
        if descriptor_handle in self.descriptor_updates:
            raise ValueError('descriptorHandle {} already in updated set!'.format(descriptor_handle))
        orig_descriptor_container = self._device_mdib_container.descriptions.handle.get_one(descriptor_handle)
        descriptor_container = orig_descriptor_container.mk_copy()
        descriptor_container.increment_descriptor_version()
        self.descriptor_updates[descriptor_handle] = TrItem(orig_descriptor_container, descriptor_container)
        return descriptor_container

    # getDescriptor = get_descriptor

    def has_state(self, descriptor_handle):
        """ check if transaction has a state with given handle """
        return self.get_state_transaction_item(descriptor_handle) is not None

    def get_state_transaction_item(self, descriptor_handle):
        """ if transaction has a state with given handle, return the transaction-item, otherwise None.  """
        for lookup in (self.metric_state_updates,
                       self.alert_state_updates,
                       self.component_state_updates,
                       self.context_state_updates,
                       self.operational_state_updates,
                       self.rt_sample_state_updates):
            if descriptor_handle in lookup:
                return lookup[descriptor_handle]
        return None

    @tr_method_wrapper
    def add_state(self, state_container, adjust_state_version=True):
        my_multi_key = self._device_mdib_container.states
        my_updates = []
        if state_container.isRealtimeSampleArrayMetricState:
            my_updates = self.rt_sample_state_updates
        elif state_container.isMetricState:
            my_updates = self.metric_state_updates
        elif state_container.isAlertState:
            my_updates = self.alert_state_updates
        elif state_container.isComponentState:
            my_updates = self.component_state_updates
        elif state_container.isSystemContextState:
            my_updates = self.component_state_updates
        elif state_container.isOperationalState:
            my_updates = self.operational_state_updates
        elif state_container.isContextState:
            my_updates = self.context_state_updates
            my_multi_key = self._device_mdib_container.context_states
        elif state_container.NODETYPE == domTag('ScoState'):
            # special case ScoState Draft6: cannot notify updates, it is a category of its own that does not fit anywhere
            # This is a bug in the spec, not in this implementation!
            return

        descriptor_handle = state_container.descriptorHandle
        my_handle = state_container.Handle if state_container.isContextState else state_container.descriptorHandle
        if my_handle in my_updates:
            raise ValueError('descriptorHandle {} already in updated set!'.format(descriptor_handle))

        if adjust_state_version:
            my_multi_key.set_version(state_container)
        my_updates[descriptor_handle] = TrItem(None, state_container)  # old, new

    def unget_state(self, state_container):
        """ forget a state that was provided before by a getXXXState call"""
        for lookup in (self.alert_state_updates, self.component_state_updates, self.context_state_updates,
                       self.metric_state_updates, self.operational_state_updates, self.rt_sample_state_updates):
            if state_container.descriptorHandle in lookup:
                del lookup[state_container.descriptorHandle]

    @tr_method_wrapper
    def get_state(self, descriptor_handle, handle=None, adjust_state_version=True):
        descriptor_container = self._get_descriptor_in_transaction(descriptor_handle)
        if descriptor_container.isContextDescriptor:
            return self._get_context_state(descriptor_handle, handle, adjust_state_version)
        if descriptor_container.isRealtimeSampleArrayMetricDescriptor:
            return self._get_real_time_sample_array_metric_state(descriptor_handle)

        if descriptor_container.isMetricDescriptor:
            updates = self.metric_state_updates
        elif descriptor_container.isOperationalDescriptor:
            updates = self.operational_state_updates
        elif descriptor_container.isComponentDescriptor:
            updates = self.component_state_updates
        elif descriptor_container.isAlertDescriptor:
            updates = self.alert_state_updates
        else:
            raise RuntimeError(f'unhandled case {descriptor_container}')
        if descriptor_handle in updates:
            raise ValueError('descriptorHandle {} already in updated set!'.format(descriptor_handle))
        old_state, new_state = self._get_or_mk_state_container(descriptor_handle, adjust_state_version)
        updates[descriptor_handle] = TrItem(old_state, new_state)
        return new_state

    def _get_context_state(self, descriptor_handle, context_state_handle=None, adjust_state_version=True):
        """ Create or Update a ContextState.
        If contextStateHandle is None, a new Context State will be created and returned.
        Otherwise an the existing contextState with that handle will be returned or a new one created.
        When the transaction is committed, the modifications to the copy will be applied to the original version,
        and notification messages will be sent to clients.
        :param descriptor_handle: the descriptorHandle of the object that shall be read
        :param context_state_handle: If None, a new Context State will be created and returned.
            Otherwise an existing contextState with that handle will be returned or a new one created.
        :param adjust_state_version: if True, and a state with this handle does not exist, but was already present in this mdib before,
          the StateVersion of descriptor_container is set to last known version for this handle +1
        @return: a copy of the state.
        """
        lookup_key = (descriptor_handle, context_state_handle)
        if lookup_key in self.context_state_updates:
            raise ValueError('descriptorHandle {} already in updated set!'.format(lookup_key))
        descriptor_container = self._get_descriptor_in_transaction(descriptor_handle)
        if context_state_handle is None:
            old_state_container = None
            new_state_container = self._device_mdib_container.mk_state_container_from_descriptor(descriptor_container)
            new_state_container.BindingMdibVersion = self._device_mdib_container.mdib_version  # auto-set this Attribute
            new_state_container.BindingStartTime = time.time()  # auto-set this Attribute
        else:
            old_state_container = self._device_mdib_container.context_states.handle.get_one(context_state_handle,
                                                                                            allow_none=True)
            if old_state_container is not None:
                new_state_container = old_state_container.mk_copy()
                new_state_container.increment_state_version()
            else:
                new_state_container = self._device_mdib_container.mk_state_container_from_descriptor(
                    descriptor_container)
                new_state_container.BindingMdibVersion = self._device_mdib_container.mdib_version  # auto-set this Attribute
                new_state_container.BindingStartTime = time.time()  # auto-set this Attribute
                new_state_container.Handle = context_state_handle
                if adjust_state_version:
                    self._device_mdib_container.context_states.set_version(new_state_container)
        self.context_state_updates[lookup_key] = TrItem(old_state_container, new_state_container)
        return new_state_container

    def _get_real_time_sample_array_metric_state(self, descriptor_handle):
        if descriptor_handle in self.rt_sample_state_updates:
            raise ValueError('descriptorHandle {} already in updated set!'.format(descriptor_handle))
        state_container = self._device_mdib_container.states.descriptorHandle.get_one(descriptor_handle,
                                                                                      allow_none=True)
        if state_container is None:
            descriptor_container = self._get_descriptor_in_transaction(descriptor_handle)
            new_state = self._device_mdib_container.mk_state_container_from_descriptor(descriptor_container)
            if not new_state.isRealtimeSampleArrayMetricState:
                raise ValueError(
                    'descriptorHandle {} does not reference a RealTimeSampleArrayMetricState'.format(descriptor_handle))
            self._device_mdib_container.states.add_object(state_container)
        else:
            if not state_container.isRealtimeSampleArrayMetricState:
                raise ValueError(
                    'descriptorHandle {} does not reference a RealTimeSampleArrayMetricState'.format(descriptor_handle))
            new_state = state_container.mk_copy(copy_node=False)
            new_state.increment_state_version()
        self.rt_sample_state_updates[descriptor_handle] = TrItem(state_container, new_state)
        return new_state
