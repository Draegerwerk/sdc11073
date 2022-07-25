import time
from collections import OrderedDict, namedtuple
from functools import wraps
from typing import Optional
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
        self.rt_sample_state_updates = {}  # unordered dict for performance
        self._error = False
        self._closed = False
        self.mdib_version = None

    def _get_descriptor_in_transaction(self, descriptor_handle):
        """ looks for new or updated descriptor in current transaction and in mdib"""
        tr_containers = self.descriptor_updates.get(descriptor_handle)
        if tr_containers is not None:
            _, new = tr_containers
            if new is None:  # descriptor is deleted in this transaction!
                raise RuntimeError(f'The descriptor {descriptor_handle} is going to be deleted')
            return new
        return self._device_mdib_container.descriptions.handle.get_one(descriptor_handle)

    # def _get_or_mk_state_container(self, descriptor_handle, adjust_state_version=True):
    #     """ returns oldContainer, newContainer"""
    #     descriptor_container = self._get_descriptor_in_transaction(descriptor_handle)
    #     old_state_container = self._device_mdib_container.states.descriptorHandle.get_one(descriptor_container.handle,
    #                                                                                       allow_none=True)
    #     if old_state_container is None:
    #         # create a new state object
    #         new_state_container = self._device_mdib_container.mk_state_container_from_descriptor(descriptor_container)
    #         if adjust_state_version:
    #             self._device_mdib_container.states.set_version(new_state_container)
    #     else:
    #         new_state_container = old_state_container.mk_copy()
    #         new_state_container.increment_state_version()
    #     return old_state_container, new_state_container

    def _get_state_container(self, descriptor_handle):
        """ returns oldContainer, newContainer"""
        descriptor_container = self._get_descriptor_in_transaction(descriptor_handle)
        old_state_container = self._device_mdib_container.states.descriptorHandle.get_one(descriptor_container.handle,
                                                                                          allow_none=False)
        new_state_container = old_state_container.mk_copy()
        new_state_container.increment_state_version()
        return old_state_container, new_state_container

    def _mk_state_container(self, descriptor_handle, adjust_state_version=True):
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
            raise ValueError(f'state with handle={descriptor_handle} already exists')
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
    """ A mdib transaction is the central mechanism to modify mdib data on the provider side.
    Use the transaction object to read and modify data of the mdib. On commit, modified data is written back to mdib
    and notifications about the modifications are sent to all consumers.
    A transaction is created by the device mdib like this:
    with mdib.transaction_manager as mgr:  # mgr is an instance of MdibUpdateTransaction
        state = mgr.get_state...   # call methods, modify objects
    # at the end of the with-statement, the transaction is committed.
    """

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
            raise ValueError(f'descriptorHandle {descriptor_handle} already in updated set!')
        if descriptor_handle in self._device_mdib_container.descriptions.handle.keys():
            raise ValueError(f'cannot create descriptorHandle {descriptor_handle}, it already exists!')
        self.descriptor_updates[descriptor_handle] = TrItem(None, descriptor_container)

    createDescriptor = add_descriptor

    @tr_method_wrapper
    def remove_descriptor(self, descriptor_handle):
        if descriptor_handle in self.descriptor_updates:
            raise ValueError(f'descriptorHandle {descriptor_handle} already in updated set!')
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
            raise ValueError(f'descriptorHandle {descriptor_handle} already in updated set!')
        orig_descriptor_container = self._device_mdib_container.descriptions.handle.get_one(descriptor_handle)
        descriptor_container = orig_descriptor_container.mk_copy()
        descriptor_container.increment_descriptor_version()
        self.descriptor_updates[descriptor_handle] = TrItem(orig_descriptor_container, descriptor_container)
        return descriptor_container

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
        """
        Add a state to mdib.
        This method should not be used directly by the application, because mdib takes care that states exists for all
        descriptors that are not for multi states (context states).
        If you want to add a context state, use mk_context_state instead.
        :param state_container:
        :param adjust_state_version:
        :return: None
        """
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
            raise ValueError(f'descriptorHandle {descriptor_handle} already in updated set!')

        if adjust_state_version:
            my_multi_key.set_version(state_container)
        my_updates[descriptor_handle] = TrItem(None, state_container)  # old, new

    def unget_state(self, state_container):
        """ forget a state that was provided before by a get_state call"""
        for lookup in (self.alert_state_updates, self.component_state_updates, self.context_state_updates,
                       self.metric_state_updates, self.operational_state_updates, self.rt_sample_state_updates):
            if state_container.descriptorHandle in lookup:
                del lookup[state_container.descriptorHandle]

    @tr_method_wrapper
    def get_state(self, descriptor_handle: str):  #, handle=None, adjust_state_version=True):
        """
        This method reads a state from mdib and adds it to the transaction.
        If there is no state with the given handle in the mdib, a ValueError is thrown.
        If this state is already part of the transaction (get_state has been called before with same arguments),
        a ValueError is thrown.
        When the transaction is committed, the modifications to the copy will be applied to the original version,
        and notification messages will be sent to clients.

        :param descriptor_handle: a string
        :return: a copy of the state
        """
        descriptor_container = self._get_descriptor_in_transaction(descriptor_handle)
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
        elif descriptor_container.isContextDescriptor:
            raise RuntimeError('for context states use get_context_state method!')
        else:
            raise RuntimeError(f'unhandled case {descriptor_container}')
        if descriptor_handle in updates:
            raise ValueError(f'descriptorHandle {descriptor_handle} already in updated set!')
        mdib_state, copied_state = self._get_state_container(descriptor_handle)
        updates[descriptor_handle] = TrItem(mdib_state, copied_state)
        return copied_state

    def get_context_state(self, descriptor_handle, context_state_handle):
        """ Reads a ContextState from mdib with given descriptor_handle and state handle.
        If there is no state with the given handle in the mdib, a ValueError is thrown.
        If this state is already part of the transaction (get_context_state has been called before with same arguments),
        a ValueError is thrown.

        When the transaction is committed, the modifications to the copy will be applied to the original version,
        and notification messages will be sent to clients.
        :param descriptor_handle: the descriptorHandle of the object that shall be read
        :param context_state_handle: the handle of the object that shall be read.
        @return: a copy of the state.
        """
        if context_state_handle is None:
            raise ValueError('no handle for context state specified')
        lookup_key = (descriptor_handle, context_state_handle)
        if lookup_key in self.context_state_updates:
            raise ValueError(f'descriptorHandle {lookup_key} already in updated set!')

        mdib_state = self._device_mdib_container.context_states.handle.get_one(context_state_handle,
                                                                                        allow_none=False)
        copied_state = mdib_state.mk_copy()
        copied_state.increment_state_version()
        self.context_state_updates[lookup_key] = TrItem(mdib_state, copied_state)
        return copied_state

    def mk_context_state(self, descriptor_handle:str,
                         context_state_handle:Optional[str]=None,
                         adjust_state_version:Optional[bool]=True):
        """ Create a new ContextState.
        If context_state_handle is None, a unique handle will be created.
        if context_state_handle is not None and it already exists in mdib, a ValueError will be thrown.
        When the transaction is committed, the new state will be added to the mdib,
        and notification messages will be sent to clients.
        :param descriptor_handle: the descriptorHandle of the object that shall be read
        :param context_state_handle: the handle for the new state or None.
        :param adjust_state_version: if True, and a state with this handle does not exist, but was already present
           in this mdib before, the StateVersion of descriptor_container is set to last known version for this handle +1
        @return: the new state.
        """
        descriptor_container = self._get_descriptor_in_transaction(descriptor_handle)
        if context_state_handle is None:
            old_state_container = None
        else:
            lookup_key = (descriptor_handle, context_state_handle)
            if lookup_key in self.context_state_updates:
                raise ValueError(f'descriptorHandle {lookup_key} already in updated set!')
            old_state_container = self._device_mdib_container.context_states.handle.get_one(context_state_handle,
                                                                                            allow_none=True)
            if old_state_container is not None:
                raise ValueError(f'ContextState with handle={context_state_handle} already exists')

        new_state_container = self._device_mdib_container.mk_state_container_from_descriptor(descriptor_container)
        new_state_container.BindingMdibVersion = self._device_mdib_container.mdib_version  # auto-set this attribute
        new_state_container.BindingStartTime = time.time()  # auto-set this attribute
        if context_state_handle is not None:
            new_state_container.Handle = context_state_handle
            if adjust_state_version:
                self._device_mdib_container.context_states.set_version(new_state_container)

        lookup_key = (descriptor_handle, new_state_container.Handle)
        self.context_state_updates[lookup_key] = TrItem(old_state_container, new_state_container)
        return new_state_container

    def _get_real_time_sample_array_metric_state(self, descriptor_handle):
        if descriptor_handle in self.rt_sample_state_updates:
            raise ValueError(f'descriptorHandle {descriptor_handle} already in updated set!')
        state_container = self._device_mdib_container.states.descriptorHandle.get_one(descriptor_handle,
                                                                                      allow_none=True)
        if state_container is None:
            descriptor_container = self._get_descriptor_in_transaction(descriptor_handle)
            new_state = self._device_mdib_container.mk_state_container_from_descriptor(descriptor_container)
            if not new_state.isRealtimeSampleArrayMetricState:
                raise ValueError(
                    f'descriptorHandle {descriptor_handle} does not reference a RealTimeSampleArrayMetricState')
            self._device_mdib_container.states.add_object(state_container)
        else:
            if not state_container.isRealtimeSampleArrayMetricState:
                raise ValueError(
                    f'descriptorHandle {descriptor_handle} does not reference a RealTimeSampleArrayMetricState')
            new_state = state_container.mk_copy(copy_node=False)
            new_state.increment_state_version()
        self.rt_sample_state_updates[descriptor_handle] = TrItem(state_container, new_state)
        return new_state


class TransactionProcessor:
    """The transaction processor, used internally by device mdib  """
    def __init__(self, mdib, transaction, set_determination_time, logger):
        self._mdib = mdib
        self._mgr = transaction
        self._logger = logger
        self._set_determination_time = set_determination_time
        self._now = None

        # states and descriptors that were modified are stored here:
        self.descr_updated = []
        self.descr_created = []
        self.descr_deleted = []
        self.descr_updated_states = []
        self.metric_updates = []
        self.alert_updates = []
        self.comp_updates = []
        self.ctxt_updates = []
        self.op_updates = []
        self.rt_updates = []
        self.has_descriptor_updates = False  # for easier handling

    def process_transaction(self):
        self._now = time.time()
        increment_mdib_version = False

        mgr = self._mgr

        # BICEPS: The version number is incremented by one every time the descriptive part changes
        if len(mgr.descriptor_updates) > 0:
            self._mdib.mddescription_version += 1
            increment_mdib_version = True

        # BICEPS: The version number is incremented by one every time the state part changes.
        if sum([len(mgr.metric_state_updates), len(mgr.alert_state_updates),
                len(mgr.component_state_updates), len(mgr.context_state_updates),
                len(mgr.operational_state_updates), len(mgr.rt_sample_state_updates)]
               ) > 0:
            self._mdib.mdstate_version += 1
            increment_mdib_version = True

        if increment_mdib_version:
            self._mdib.mdib_version += 1

        self._handle_descriptors()
        self._handle_metric_states()
        self._handle_alert_updates()
        self._handle_component_states()
        self._handle_context_state_updates()
        self._handle_operational_state_updates()
        self._handle_rt_value_updates()

    def _handle_descriptors(self):
        # handle descriptors
        mgr = self._mgr
        if len(mgr.descriptor_updates) > 0:
            self.has_descriptor_updates = True
            # need to know all to be deleted and to be created descriptors
            to_be_deleted = [old for old, new in mgr.descriptor_updates.values() if new is None]
            to_be_created = [new for old, new in mgr.descriptor_updates.values() if old is None]
            to_be_deleted_handles = [d.handle for d in to_be_deleted]
            to_be_created_handles = [d.handle for d in to_be_created]
            with self._mdib.mdib_lock:
                # handling only updated states here: If a descriptor is created, it can be assumed that the
                # application also creates the state in an transaction.
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
                        self._update_corresponding_state(new_descriptor)
                    if orig_descriptor is None:
                        # this is a create operation
                        self._logger.debug('transaction_manager: new descriptor Handle={}, DescriptorVersion={}',
                                           new_descriptor.handle, new_descriptor.DescriptorVersion)
                        self.descr_created.append(new_descriptor.mk_copy())
                        self._mdib.descriptions.add_object_no_lock(new_descriptor)
                        # R0033: A SERVICE PROVIDER SHALL increment pm:AbstractDescriptor/@DescriptorVersion by one if a direct child descriptor is added or deleted.
                        if new_descriptor.parent_handle is not None and new_descriptor.parent_handle not in to_be_created_handles:
                            # only update parent if it is not also created in this transaction
                            self._increment_parent_descriptor_version(new_descriptor)
                    elif new_descriptor is None:
                        # this is a delete operation
                        self._logger.debug('transaction_manager: rm descriptor Handle={}, DescriptorVersion={}',
                                           orig_descriptor.handle, orig_descriptor.DescriptorVersion)
                        all_descriptors = self._mdib.get_all_descriptors_in_subtree(orig_descriptor)
                        self._mdib.rm_descriptors_and_states(all_descriptors)
                        self.descr_deleted.extend([d.mk_copy() for d in all_descriptors])
                        # R0033: A SERVICE PROVIDER SHALL increment pm:AbstractDescriptor/@DescriptorVersion by one if a direct child descriptor is added or deleted.
                        if orig_descriptor.parent_handle is not None and orig_descriptor.parent_handle not in to_be_deleted_handles:
                            # only update parent if it is not also deleted in this transaction
                            self._increment_parent_descriptor_version(orig_descriptor)
                    else:
                        # this is an update operation
                        self.descr_updated.append(new_descriptor)
                        self._logger.debug('transaction_manager: update descriptor Handle={}, DescriptorVersion={}',
                                           new_descriptor.handle, new_descriptor.DescriptorVersion)
                        self._mdib.descriptions.replace_object_no_lock(new_descriptor)

    def _handle_metric_states(self):
        if len(self._mgr.metric_state_updates) > 0:
            with self._mdib.mdib_lock:
                self._logger.debug('transaction_manager: mdib version={}, metric updates = {}',
                                   self._mdib.mdib_version,
                                   self._mgr.metric_state_updates)
                for value in self._mgr.metric_state_updates.values():
                    old_state, new_state = value.old, value.new
                    try:
                        if self._set_determination_time and new_state.MetricValue is not None:
                            new_state.MetricValue.DeterminationTime = self._now
                        # replace the old container with the new one
                        self._mdib.states.remove_object_no_lock(old_state)
                        self._mdib.states.add_object_no_lock(new_state)
                        self.metric_updates.append(new_state.mk_copy())
                    except RuntimeError:
                        self._logger.warn('transaction_manager: {} did not exist before!! really??', new_state)
                        raise

    def _handle_alert_updates(self):
        if len(self._mgr.alert_state_updates) > 0:
            with self._mdib.mdib_lock:
                self._logger.debug('transaction_manager: alert State updates = {}', self._mgr.alert_state_updates)
                for value in self._mgr.alert_state_updates.values():
                    old_state, new_state = value.old, value.new
                    try:
                        if self._set_determination_time and new_state.isAlertCondition:
                            new_state.DeterminationTime = time.time()
                        new_state.set_node_member(self._mdib.nsmapper)
                        # replace the old container with the new one
                        self._mdib.states.remove_object_no_lock(old_state)
                        self._mdib.states.add_object_no_lock(new_state.mk_copy())
                        self.alert_updates.append(new_state)
                    except RuntimeError:
                        self._logger.warn('transaction_manager: {} did not exist before!! really??', new_state)
                        raise

    def _handle_component_states(self):
        if len(self._mgr.component_state_updates) > 0:
            with self._mdib.mdib_lock:
                self._logger.debug('transaction_manager: component State updates = {}',
                                   self._mgr.component_state_updates)
                for value in self._mgr.component_state_updates.values():
                    old_state, new_state = value.old, value.new
                    try:
                        new_state.set_node_member(self._mdib.nsmapper)
                        # replace the old container with the new one
                        self._mdib.states.remove_object_no_lock(old_state)
                        self._mdib.states.add_object_no_lock(new_state)
                        self.comp_updates.append(new_state.mk_copy())
                    except RuntimeError:
                        self._logger.warn('transaction_manager: {} did not exist before!! really??', new_state)
                        raise

    def _handle_context_state_updates(self):
        if len(self._mgr.context_state_updates) > 0:
            with self._mdib.mdib_lock:
                self._logger.debug('transaction_manager: contextState updates = {}', self._mgr.context_state_updates)
                for value in self._mgr.context_state_updates.values():
                    old_state, new_state = value.old, value.new
                    try:
                        self.ctxt_updates.append(new_state.mk_copy())
                        # replace the old container with the new one
                        self._mdib.context_states.remove_object_no_lock(old_state)
                        self._mdib.context_states.add_object_no_lock(new_state)
                        new_state.set_node_member(self._mdib.nsmapper)
                    except RuntimeError:
                        self._logger.warn('transaction_manager: {} did not exist before!! really??', new_state)
                        raise

    def _handle_operational_state_updates(self):
        if len(self._mgr.operational_state_updates) > 0:
            with self._mdib.mdib_lock:
                self._logger.debug('transaction_manager: operationalState updates = {}',
                                   self._mgr.operational_state_updates)
                for value in self._mgr.operational_state_updates.values():
                    old_state, new_state = value.old, value.new
                    try:
                        new_state.set_node_member(self._mdib.nsmapper)
                        self._mdib.states.remove_object_no_lock(old_state)
                        self._mdib.states.add_object_no_lock(new_state)
                        self.op_updates.append(new_state.mk_copy())
                    except RuntimeError:
                        self._logger.warn('transaction_manager: {} did not exist before!! really??', new_state)
                        raise

    def _handle_rt_value_updates(self):
        if len(self._mgr.rt_sample_state_updates) > 0:
            with self._mdib.mdib_lock:
                self._logger.debug('transaction_manager: rtSample updates = {}', self._mgr.rt_sample_state_updates)
                for value in self._mgr.rt_sample_state_updates.values():
                    old_state, new_state = value.old, value.new
                    try:
                        new_state.set_node_member(self._mdib.nsmapper)
                        self._mdib.states.remove_object_no_lock(old_state)
                        self._mdib.states.add_object_no_lock(new_state)
                        self.rt_updates.append(new_state.mk_copy())
                    except RuntimeError:
                        self._logger.warn('transaction_manager: {} did not exist before!! really??', new_state)
                        raise

    def _update_corresponding_state(self, descriptor_container):
        # add state to updated_states list and to corresponding notifications input
        # => the state is always sent twice, a) in the description modification report and b)
        # in the specific state update notification.
        if descriptor_container.isAlertDescriptor:
            update_dict = self._mgr.alert_state_updates
        elif descriptor_container.isComponentDescriptor:
            update_dict = self._mgr.component_state_updates
        elif descriptor_container.isContextDescriptor:
            update_dict = self._mgr.context_state_updates
        elif descriptor_container.isRealtimeSampleArrayMetricDescriptor:
            update_dict = self._mgr.rt_sample_state_updates
        elif descriptor_container.isMetricDescriptor:
            update_dict = self._mgr.metric_state_updates
        elif descriptor_container.isOperationalDescriptor:
            update_dict = self._mgr.operational_state_updates
        else:
            raise RuntimeError(f'do not know how to handle {descriptor_container.__class__.__name__}')
        if descriptor_container.isContextDescriptor:
            update_dict = self._mgr.context_state_updates
            all_context_states = self._mdib.context_states.descriptorHandle.get(
                descriptor_container.handle, [])
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
                self.descr_updated_states.append(new_state.mk_copy())
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
                new_state.set_descriptor_container(descriptor_container)
                new_state.update_descriptor_version()
            else:
                old_state = self._mdib.states.descriptorHandle.get_one(
                    descriptor_container.handle, allow_none=True)
                if old_state is not None:
                    new_state = old_state.mk_copy()
                    new_state.set_descriptor_container(descriptor_container)
                    new_state.increment_state_version()
                    update_dict[descriptor_container.handle] = TrItem(old_state, new_state)
            if new_state is not None:
                self.descr_updated_states.append(new_state.mk_copy())

    def _increment_parent_descriptor_version(self, descriptor_container):
        parent_descriptor_container = self._mdib.descriptions.handle.get_one(
            descriptor_container.parent_handle)
        parent_descriptor_container.increment_descriptor_version()
        self.descr_updated.append(parent_descriptor_container.mk_copy())
        self._update_corresponding_state(parent_descriptor_container)
