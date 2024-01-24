"""The transaction implementation in this module is deprecated.

It is only available as a fallback for a short time if the new modular implementation of transactions has problems.
"""

from __future__ import annotations

import time
import uuid
from functools import wraps
from typing import TYPE_CHECKING, Any, Callable

from sdc11073.exceptions import ApiUsageError

from .transactionsprotocol import TransactionItem, TransactionType

if TYPE_CHECKING:
    from sdc11073.loghelper import LoggerAdapter
    from sdc11073.multikey import MultiKeyLookup

    from .descriptorcontainers import AbstractDescriptorProtocol
    from .providermdib import ProviderMdib
    from .statecontainers import (
        AbstractMultiStateProtocol,
        AbstractStateContainer,
        AbstractStateProtocol,
    )
    from .transactionsprotocol import AnyTransactionManagerProtocol, TransactionResultProtocol


class _TransactionBase:
    def __init__(self,
                 device_mdib_container: ProviderMdib,
                 transaction_type: TransactionType,
                 logger: LoggerAdapter):
        self._device_mdib_container = device_mdib_container
        self._transaction_type = transaction_type
        self._logger = logger
        self.descriptor_updates = {}
        self.metric_state_updates = {}
        self.alert_state_updates = {}
        self.component_state_updates = {}
        self.context_state_updates = {}
        self.operational_state_updates = {}
        self.rt_sample_state_updates = {}
        self._error = False
        self._closed = False
        self.mdib_version = None
        self._current_update_dict = None  # used to check for data type

    @property
    def is_descriptor_update(self) -> bool:
        return self._transaction_type == TransactionType.descriptor

    def actual_descriptor(self, descriptor_handle: str) -> AbstractDescriptorProtocol:
        """Look for new or updated descriptor in current transaction and in mdib."""
        tr_container = self.descriptor_updates.get(descriptor_handle)
        if tr_container is not None:
            if tr_container.new is None:  # descriptor is deleted in this transaction!
                raise ValueError(f'The descriptor {descriptor_handle} is going to be deleted')
            return tr_container.new
        return self._device_mdib_container.descriptions.handle.get_one(descriptor_handle)

    def _get_state_container(self, descriptor_handle: str) -> tuple[AbstractStateProtocol, AbstractStateProtocol]:
        """Return old state, new state."""
        descriptor_container = self.actual_descriptor(descriptor_handle)
        old_state_container = self._device_mdib_container.states.descriptor_handle.get_one(descriptor_container.Handle,
                                                                                           allow_none=False)
        new_state_container = old_state_container.mk_copy()
        new_state_container.increment_state_version()
        return old_state_container, new_state_container

    def _mk_state_container(self, descriptor_handle: str, adjust_state_version: bool = True) \
            -> tuple[AbstractStateProtocol | None, AbstractStateProtocol | None]:
        """Return old container, new container."""
        descriptor_container = self.actual_descriptor(descriptor_handle)
        old_state_container = self._device_mdib_container.states.descriptor_handle.get_one(descriptor_container.Handle,
                                                                                           allow_none=True)
        if old_state_container is None:
            # create a new state object
            new_state_container = self._device_mdib_container.data_model.mk_state_container(descriptor_container)
            if adjust_state_version:
                self._device_mdib_container.states.set_version(new_state_container)
        else:
            raise ValueError(f'state with handle={descriptor_handle} already exists')
        return old_state_container, new_state_container

    def _verify_correct_update_dict(self, update_dict: dict):
        """Check that update_dict contains only data that are allowed in a single transaction.

        Mix of data types that result in different notifications is not allowed,
        because this would result in more than one notification with the same mdib version.
        """
        if self._current_update_dict is None:
            self._current_update_dict = update_dict
        elif self.is_descriptor_update:
            # in description modification reports it is allowed to update states as well
            return
        elif self._current_update_dict is not update_dict:
            raise ApiUsageError('Mix of data types in transaction is not allowed!')

    def _verify_correct_state_type(self, state: AbstractStateProtocol):
        # description modification report can contain any type of state, everything else must match exactly
        if self._transaction_type == TransactionType.descriptor or \
                (state.is_context_state and self._transaction_type == TransactionType.context) or \
                (state.is_realtime_sample_array_metric_state and self._transaction_type == TransactionType.rt_sample) or \
                (state.is_component_state and self._transaction_type == TransactionType.component) or \
                (state.is_operational_state and self._transaction_type == TransactionType.operational) or \
                (state.is_alert_state and self._transaction_type == TransactionType.alert) or \
                (state.is_metric_state and self._transaction_type == TransactionType.metric):
            return
        raise ApiUsageError('Mix of data types in transaction is not allowed!')

    def _verify_correct_descr_type(self, descriptor: AbstractDescriptorProtocol):
        # description modification report can contain any type of state, everything else must match exactly
        if self._transaction_type == TransactionType.descriptor or \
                (descriptor.is_context_descriptor and self._transaction_type == TransactionType.context) or \
                (
                        descriptor.is_realtime_sample_array_metric_descriptor and self._transaction_type == TransactionType.rt_sample) or \
                (descriptor.is_component_descriptor and self._transaction_type == TransactionType.component) or \
                (descriptor.is_operational_descriptor and self._transaction_type == TransactionType.operational) or \
                (descriptor.is_alert_descriptor and self._transaction_type == TransactionType.alert) or \
                (descriptor.is_metric_descriptor and self._transaction_type == TransactionType.metric):
            return
        raise ApiUsageError('Mix of data types in transaction is not allowed!')

    def _get_states_update(self, container: AbstractStateProtocol | AbstractDescriptorProtocol) -> dict:
        if getattr(container, 'is_realtime_sample_array_metric_state', False):
            return self.rt_sample_state_updates
        if getattr(container, 'is_metric_state', False) or getattr(container, 'is_metric_descriptor', False):
            return self.metric_state_updates
        if getattr(container, 'is_alert_state', False) or getattr(container, 'is_alert_descriptor', False):
            return self.alert_state_updates
        if getattr(container, 'is_component_state', False) or getattr(container, 'is_component_descriptor', False):
            return self.component_state_updates
        if getattr(container, 'is_operational_state', False) or getattr(container, 'is_operational_descriptor', False):
            return self.operational_state_updates
        if getattr(container, 'is_context_state', False) or getattr(container, 'is_context_descriptor', False):
            return self.context_state_updates
        raise NotImplementedError(f'unhandled case {container}')

    def _get_states_storage(self, state_container: AbstractStateProtocol) -> MultiKeyLookup:
        if state_container.is_context_state:
            return self._device_mdib_container.context_states
        return self._device_mdib_container.states

    @property
    def error(self) -> bool:
        return self._error


def tr_method_wrapper(method: Callable) -> Callable:
    """Wrap a method (Decorator) for consistency checks and error handling."""

    @wraps(method)
    def wrapper(self: _TransactionBase, *args: Any, **kwargs) -> Any:  # noqa: ANN003
        # pylint: disable=protected-access
        if self._closed:
            raise ApiUsageError('This Transaction is closed!')
        if self._error:
            raise ApiUsageError('This Transaction failed due to a previous error!')
        try:
            return method(self, *args, **kwargs)
        except:
            self._error = True
            raise

    return wrapper


class RtDataMdibUpdateTransaction(_TransactionBase):
    """RtDataMdibUpdateTransaction is only used internally to periodically send waveform notifications.

    It handles this specific purpose with less overhead compared to regular transaction.
    """

    def __init__(self,
                 device_mdib_container: ProviderMdib,
                 logger: LoggerAdapter):
        super().__init__(device_mdib_container, TransactionType.rt_sample, logger)

    @tr_method_wrapper
    def get_real_time_sample_array_metric_state(self, descriptor_handle: str) -> AbstractStateContainer:
        """Return the descriptor for given handle.

        For performance reasons, this method does not return a copy of the original object.
        This means no rollback is possible.
        """
        if descriptor_handle in self.rt_sample_state_updates:
            raise ValueError(f'DescriptorHandle {descriptor_handle} already in updated set!')
        state_container = self._device_mdib_container.states.descriptor_handle.get_one(descriptor_handle,
                                                                                       allow_none=True)
        if state_container is None:
            raise ValueError(f'state {descriptor_handle} not found!')

        if not state_container.is_realtime_sample_array_metric_state:
            raise ValueError(
                f'DescriptorHandle {descriptor_handle} does not reference a RealTimeSampleArrayMetricState')
        state_container.increment_state_version()
        new_state = state_container  # supply old and new state; although identical, just do not break interface
        self.rt_sample_state_updates[descriptor_handle] = TransactionItem(state_container, new_state)
        return new_state


class MdibUpdateTransaction(_TransactionBase):
    """A mdib transaction is the central mechanism to modify mdib data on the provider side.

    Implements DescriptorTransactionManagerProtocol, StateTransactionManagerProtocol and
    ContextStateTransactionManagerProtocol.

    Use the transaction object to read and modify data of the mdib. On commit, modified data is written back to mdib
    and notifications about the modifications are sent to all consumers.
    A transaction is created by the device mdib like this:
    with mdib.transaction_manager as mgr:  # mgr is an instance of MdibUpdateTransaction
        state = mgr.get_state...   # call methods, modify objects
    At the end of the with-statement, the transaction is committed.
    A transaction allows only one type of data , e.g. only metric updates or descriptor updates...
    It raises a TypeError if you try to add a different kind.
    Reason: Mdib handles each notification as a transaction (except for description modification reports,
    this can have multiple notifications with the same mdib version.).
    """

    def __init__(self, device_mdib_container: ProviderMdib,
                 transaction_type: TransactionType,
                 logger: LoggerAdapter):
        super().__init__(device_mdib_container, transaction_type, logger)
        # lookups for states that are modified due to descriptor changes
        self.descriptor_state_new = {}
        self.descriptor_state_upd = {}
        self.descriptor_state_del = {}
        self.new_descriptors = []  # handles

    @tr_method_wrapper
    def add_descriptor(self,
                       descriptor_container: AbstractDescriptorProtocol,
                       adjust_descriptor_version: bool = True,
                       state_container: AbstractStateProtocol | None = None):
        """Add a new descriptor to mdib.

        :param descriptor_container: the object that shall be added to mdib
        :param adjust_descriptor_version: if True, and a descriptor with this handle does not exist,
            but was already present in this mdib before,
            the DescriptorVersion of descriptor_container is set to last known version for this handle +1
        :param state_container: optional state container for descriptor_container
        :return: None
        """
        if self._transaction_type != TransactionType.descriptor:
            raise ApiUsageError('Mix of data types in transaction is not allowed!')

        descriptor_handle = descriptor_container.Handle
        if descriptor_handle in self.descriptor_updates:
            raise ValueError(f'Descriptor {descriptor_handle} already in updated set!')
        if descriptor_handle in self._device_mdib_container.descriptions.handle:
            raise ValueError(f'cannot create Descriptor {descriptor_handle}, it already exists!')
        if adjust_descriptor_version:
            self._device_mdib_container.descriptions.set_version(descriptor_container)
        if descriptor_container.source_mds is None:
            self._device_mdib_container.xtra.set_source_mds(descriptor_container)
        self.descriptor_updates[descriptor_handle] = TransactionItem(None, descriptor_container)
        self.new_descriptors.append(descriptor_handle)
        if state_container is not None:
            if state_container.DescriptorHandle != descriptor_handle:
                raise ValueError(f'State {state_container.DescriptorHandle} != {descriptor_handle}!')
            self.add_state(state_container)

    @tr_method_wrapper
    def remove_descriptor(self, descriptor_handle: str):
        """Remove existing descriptor from mdib."""
        if self._transaction_type != TransactionType.descriptor:
            raise ApiUsageError('Mix of data types in transaction is not allowed!')
        if descriptor_handle in self.descriptor_updates:
            raise ValueError(f'DescriptorHandle {descriptor_handle} already in updated set!')
        orig_descriptor_container = self._device_mdib_container.descriptions.handle.get_one(descriptor_handle)
        self.descriptor_updates[descriptor_handle] = TransactionItem(orig_descriptor_container, None)

    @tr_method_wrapper
    def get_descriptor(self, descriptor_handle: str) -> AbstractDescriptorProtocol:
        """Get a descriptor from mdib.

        When the transaction is committed, the modifications to the copy will be applied to the original version,
        and notification messages will be sent to clients.
        @return: a copy of the state.
        """
        if self._transaction_type != TransactionType.descriptor:
            raise ApiUsageError('Mix of data types in transaction is not allowed!')
        if descriptor_handle in self.descriptor_updates:
            raise ValueError(f'DescriptorHandle {descriptor_handle} already in updated set!')
        orig_descriptor_container = self._device_mdib_container.descriptions.handle.get_one(descriptor_handle)
        descriptor_container = orig_descriptor_container.mk_copy()
        descriptor_container.increment_descriptor_version()
        self.descriptor_updates[descriptor_handle] = TransactionItem(orig_descriptor_container, descriptor_container)
        return descriptor_container

    def has_state(self, descriptor_handle: str) -> bool:
        """Determine if transaction has a state with given handle."""
        return self.get_state_transaction_item(descriptor_handle) is not None

    def get_state_transaction_item(self, handle: str) -> TransactionItem | None:
        """If transaction has a state with given handle, return the transaction-item, otherwise None.

        :param handle: the Handle of a context state or the DescriptorHandle in all other cases
        """
        for lookup in (self.metric_state_updates,
                       self.alert_state_updates,
                       self.component_state_updates,
                       self.context_state_updates,
                       self.operational_state_updates,
                       self.rt_sample_state_updates):
            if handle in lookup:
                return lookup[handle]
        return None

    @tr_method_wrapper
    def add_state(self,
                  state_container: AbstractStateProtocol | AbstractMultiStateProtocol,
                  adjust_state_version: bool = True):
        """Add a new state to mdib.

        This method should not be used directly by the application, because mdib takes care that states exists for all
        descriptors that are not for multi states (context states).
        If you want to add a context state, use mk_context_state instead
        :param state_container:
        :param adjust_state_version:
        :return: None
        """
        self._verify_correct_state_type(state_container)

        my_multi_key = self._get_states_storage(state_container)
        my_updates = self._get_states_update(state_container)

        descriptor_handle = state_container.DescriptorHandle
        if self.is_descriptor_update:
            # check that the descriptor is also new
            if descriptor_handle not in self.new_descriptors:
                raise ApiUsageError('This is a transaction for descriptor modifications, this state does not match')

        my_handle = state_container.Handle if state_container.is_context_state else descriptor_handle
        if my_handle in my_updates:
            raise ValueError(f'State {descriptor_handle} already in updated set!')

        if state_container.descriptor_container is None:
            descr = self.actual_descriptor(descriptor_handle)
            state_container.descriptor_container = descr
        if adjust_state_version:
            my_multi_key.set_version(state_container)
        my_updates[my_handle] = TransactionItem(None, state_container)

    def unget_state(self, state_container: AbstractStateProtocol | AbstractMultiStateProtocol):
        """Forget a state that was provided before by a get_state or add_state call."""
        updates_dict = self._get_states_update(state_container)
        if state_container.is_context_state:
            if state_container.Handle in self.context_state_updates:
                del updates_dict[state_container.Handle]
        elif state_container.DescriptorHandle in updates_dict:
            del updates_dict[state_container.DescriptorHandle]

    @tr_method_wrapper
    def get_state(self, descriptor_handle: str) -> AbstractStateProtocol:
        """Read a state from mdib and add it to the transaction.

        If there is no state with the given handle in the mdib, a ValueError is thrown.
        If this state is already part of the transaction (get_state has been called before with same arguments),
        a ValueError is thrown.
        When the transaction is committed, the modifications to the copy will be applied to the original version,
        and notification messages will be sent to clients
        :param descriptor_handle: a string
        :return: a copy of the state
        """
        descriptor_container = self.actual_descriptor(descriptor_handle)
        self._verify_correct_descr_type(descriptor_container)

        if descriptor_container.is_realtime_sample_array_metric_descriptor:
            return self._get_real_time_sample_array_metric_state(descriptor_container)
        if descriptor_container.is_context_descriptor:
            raise ApiUsageError('for context states use get_context_state method!')
        updates_dict = self._get_states_update(descriptor_container)
        if descriptor_handle in updates_dict:
            raise ValueError(f'State {descriptor_handle} already in updated set!')
        self._verify_correct_update_dict(updates_dict)
        if self.is_descriptor_update and descriptor_handle not in self.descriptor_updates:
            raise ApiUsageError('this is a descriptor update transaction, state does not match!')
        mdib_state, copied_state = self._get_state_container(descriptor_handle)
        updates_dict[descriptor_handle] = TransactionItem(mdib_state, copied_state)
        return copied_state

    def get_context_state(self, context_state_handle: str) -> AbstractMultiStateProtocol:
        """Read a ContextState from mdib with given state handle.

        If there is no state with the given handle in the mdib, a ValueError is thrown.
        If this state is already part of the transaction (get_context_state has been called before with same arguments),
        a ValueError is thrown.

        When the transaction is committed, the modifications to the copy will be applied to the original version,
        and notification messages will be sent to clients
        :param context_state_handle: the handle of the object that shall be read.
        @return: a copy of the state.
        """
        if self._transaction_type != TransactionType.context:
            raise ApiUsageError('Mix of data types in transaction is not allowed!')

        if context_state_handle is None:
            raise ValueError('no handle for context state specified')
        if context_state_handle in self.context_state_updates:
            raise ValueError(f'DescriptorHandle {context_state_handle} already in updated set!')

        mdib_state = self._device_mdib_container.context_states.handle.get_one(context_state_handle,
                                                                               allow_none=False)
        copied_state = mdib_state.mk_copy()
        copied_state.increment_state_version()
        self.context_state_updates[context_state_handle] = TransactionItem(mdib_state, copied_state)
        return copied_state

    def mk_context_state(self, descriptor_handle: str,
                         context_state_handle: str | None = None,
                         adjust_state_version: bool = True,
                         set_associated: bool = False) -> AbstractStateProtocol:
        """Create a new ContextStateContainer.

        If context_state_handle is None, a unique handle will be created.
        if context_state_handle is not None, and it already exists in mdib, a ValueError will be thrown.
        When the transaction is committed, the new state will be added to the mdib,
        and notification messages will be sent to clients
        :param descriptor_handle: the DescriptorHandle of the object that shall be read
        :param context_state_handle: the handle for the new state or None
        :param adjust_state_version: if True, and a state with this handle does not exist, but was already present
           in this mdib before, the StateVersion of descriptor_container is set to last known version for this handle +1
           :param set_associated: if True, BindingMdibVersion, BindingStartTime and ContextAssociation are set.
        :return: the new state.
        """
        if self._transaction_type != TransactionType.context:
            raise ApiUsageError('Mix of data types in transaction is not allowed!')
        descriptor_container = self.actual_descriptor(descriptor_handle)
        if context_state_handle is None:
            old_state_container = None
        else:
            if context_state_handle in self.context_state_updates:
                raise ValueError(f'DescriptorHandle {context_state_handle} already in updated set!')
            old_state_container = self._device_mdib_container.context_states.handle.get_one(context_state_handle,
                                                                                            allow_none=True)
            if old_state_container is not None:
                raise ValueError(f'ContextState with handle={context_state_handle} already exists')

        new_state_container = self._device_mdib_container.data_model.mk_state_container(descriptor_container)
        new_state_container.Handle = context_state_handle or uuid.uuid4().hex
        if set_associated:
            # bind to mdib version AFTER this transaction
            new_state_container.BindingMdibVersion = self._device_mdib_container.mdib_version + 1
            new_state_container.BindingStartTime = time.time()
            new_state_container.ContextAssociation = \
                self._device_mdib_container.data_model.pm_types.ContextAssociation.ASSOCIATED
        if context_state_handle is not None and adjust_state_version:
            self._device_mdib_container.context_states.set_version(new_state_container)

        self.context_state_updates[new_state_container.Handle] = TransactionItem(old_state_container,
                                                                                 new_state_container)
        return new_state_container

    def process_transaction(self, set_determination_time: bool) -> TransactionResultProtocol:
        """Process the transaction."""
        processor = ProcessTransactionResult(self._device_mdib_container,
                                             self,
                                             set_determination_time,
                                             self._logger)
        processor.process_transaction()
        return processor

    def _get_real_time_sample_array_metric_state(
            self,
            descriptor_container: AbstractDescriptorProtocol,
    ) -> AbstractStateProtocol:
        if self._transaction_type != TransactionType.rt_sample:
            raise ApiUsageError('Mix of data types in transaction is not allowed!')

        descriptor_handle = descriptor_container.Handle
        if descriptor_handle in self.rt_sample_state_updates:
            raise ValueError(f'DescriptorHandle {descriptor_handle} already in updated set!')
        state_container = self._device_mdib_container.states.descriptor_handle.get_one(descriptor_handle,
                                                                                       allow_none=True)
        if state_container is None:
            descriptor_container = self.actual_descriptor(descriptor_handle)
            new_state = self._device_mdib_container.data_model.mk_state_container(descriptor_container)
            if not new_state.is_realtime_sample_array_metric_state:
                raise ValueError(
                    f'DescriptorHandle {descriptor_handle} does not reference a RealTimeSampleArrayMetricState')
            self._device_mdib_container.states.add_object(state_container)
        else:
            if not state_container.is_realtime_sample_array_metric_state:
                raise ValueError(
                    f'DescriptorHandle {descriptor_handle} does not reference a RealTimeSampleArrayMetricState')
            new_state = state_container.mk_copy(copy_node=False)
            new_state.increment_state_version()
        self.rt_sample_state_updates[descriptor_handle] = TransactionItem(state_container, new_state)
        return new_state


class ProcessTransactionResult:
    """The transaction processor, used internally by device mdib.

    Implements TransactionResultProtocol.
    """

    def __init__(self, mdib: ProviderMdib,
                 transaction: MdibUpdateTransaction,
                 set_determination_time: bool,
                 logger: LoggerAdapter):
        self._mdib = mdib
        self._mgr = transaction
        self._logger = logger
        self._set_determination_time = set_determination_time
        self._now = None

        # states and descriptors that were modified are stored here:
        self.descr_updated = []
        self.descr_created = []
        self.descr_deleted = []
        self.metric_updates = []
        self.alert_updates = []
        self.comp_updates = []
        self.ctxt_updates = []
        self.op_updates = []
        self.rt_updates = []
        self.has_descriptor_updates = False  # for easier handling

    def all_states(self) -> list[AbstractStateProtocol]:
        """Return all states in this transaction."""
        return self.metric_updates + self.alert_updates + self.comp_updates + self.ctxt_updates \
               + self.op_updates + self.rt_updates

    def process_transaction(self):
        """Run transaction."""
        self._now = time.time()
        increment_mdib_version = False

        mgr = self._mgr

        # BICEPS: The version number is incremented by one every time the descriptive part changes
        if mgr.descriptor_updates:
            self._mdib.mddescription_version += 1
            increment_mdib_version = True

        # BICEPS: The version number is incremented by one every time the state part changes.
        if mgr.metric_state_updates or mgr.alert_state_updates or mgr.component_state_updates \
                or mgr.context_state_updates or mgr.operational_state_updates or mgr.rt_sample_state_updates:
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
        if mgr.descriptor_updates:
            self.has_descriptor_updates = True
            # need to know all to be deleted and to be created descriptors
            to_be_deleted_handles = [tr_item.old.Handle for tr_item in mgr.descriptor_updates.values()
                                     if tr_item.new is None and tr_item.old is not None]
            to_be_created_handles = [tr_item.new.Handle for tr_item in mgr.descriptor_updates.values()
                                     if tr_item.old is None and tr_item.new is not None]
            # handling only updated states here: If a descriptor is created, it can be assumed that the
            # application also creates the state in a transaction.
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
                else:  # descriptor delete
                    self._remove_corresponding_state(orig_descriptor)
                if orig_descriptor is None:
                    # this is a create operation
                    self._logger.debug(  # noqa: PLE1205
                        'transaction_manager: new descriptor Handle={}, DescriptorVersion={}',
                        new_descriptor.Handle, new_descriptor.DescriptorVersion)
                    self.descr_created.append(new_descriptor.mk_copy())
                    self._mdib.descriptions.add_object_no_lock(new_descriptor)
                    # increment DescriptorVersion if a child descriptor is added or deleted.
                    if new_descriptor.parent_handle is not None \
                            and new_descriptor.parent_handle not in to_be_created_handles:
                        # only update parent if it is not also created in this transaction
                        self._increment_parent_descriptor_version(new_descriptor)
                elif new_descriptor is None:
                    # this is a delete operation
                    self._logger.debug(  # noqa: PLE1205
                        'transaction_manager: rm descriptor Handle={}, DescriptorVersion={}',
                        orig_descriptor.Handle, orig_descriptor.DescriptorVersion)
                    all_descriptors = self._mdib.get_all_descriptors_in_subtree(orig_descriptor)
                    self._mdib.rm_descriptors_and_states(all_descriptors)
                    self.descr_deleted.extend([d.mk_copy() for d in all_descriptors])
                    # increment DescriptorVersion if a child descriptor is added or deleted.
                    if orig_descriptor.parent_handle is not None \
                            and orig_descriptor.parent_handle not in to_be_deleted_handles:
                        # only update parent if it is not also deleted in this transaction
                        self._increment_parent_descriptor_version(orig_descriptor)
                else:
                    # this is an update operation
                    self.descr_updated.append(new_descriptor)
                    self._logger.debug(  # noqa: PLE1205
                        'transaction_manager: update descriptor Handle={}, DescriptorVersion={}',
                        new_descriptor.Handle, new_descriptor.DescriptorVersion)
                    self._mdib.descriptions.replace_object_no_lock(new_descriptor)

    def _handle_metric_states(self):
        if self._mgr.metric_state_updates:
            self._logger.debug(  # noqa: PLE1205
                'transaction_manager: mdib version={}, metric updates = {}',
                self._mdib.mdib_version,
                self._mgr.metric_state_updates)
            if self._set_determination_time:
                for tr_item in self._mgr.metric_state_updates.values():
                    state = tr_item.new
                    if state is None:
                        continue
                    if state.MetricValue is not None:
                        state.MetricValue.DeterminationTime = time.time()
            self.metric_updates.extend(self._handle_updates(self._mgr.metric_state_updates))

    def _handle_alert_updates(self):
        if self._mgr.alert_state_updates:
            self._logger.debug(  # noqa: PLE1205
                'transaction_manager: alert State updates = {}', self._mgr.alert_state_updates)
            if self._set_determination_time:
                for tr_item in self._mgr.alert_state_updates.values():
                    new_state = tr_item.new
                    if new_state is None or not hasattr(new_state, 'Presence'):
                        continue
                    if tr_item.old is None:
                        if new_state.Presence:
                            new_state.DeterminationTime = time.time()
                    elif new_state.is_alert_condition and new_state.Presence != tr_item.old.Presence:
                        new_state.DeterminationTime = time.time()
            self.alert_updates.extend(self._handle_updates(self._mgr.alert_state_updates))

    def _handle_component_states(self):
        if self._mgr.component_state_updates:
            self._logger.debug(  # noqa: PLE1205
                'transaction_manager: component State updates = {}',
                self._mgr.component_state_updates)
            self.comp_updates.extend(self._handle_updates(self._mgr.component_state_updates))

    def _handle_context_state_updates(self):
        if self._mgr.context_state_updates:
            self._logger.debug(  # noqa: PLE1205
                'transaction_manager: contextState updates = {}', self._mgr.context_state_updates)
            self.ctxt_updates.extend(self._handle_updates(self._mgr.context_state_updates, True))

    def _handle_operational_state_updates(self):
        if self._mgr.operational_state_updates:
            self._logger.debug(  # noqa: PLE1205
                'transaction_manager: operationalState updates = {}',
                self._mgr.operational_state_updates)
            self.op_updates.extend(self._handle_updates(self._mgr.operational_state_updates))

    def _handle_rt_value_updates(self):
        if self._mgr.rt_sample_state_updates:
            self._logger.debug(  # noqa: PLE1205
                'transaction_manager: rtSample updates = {}', self._mgr.rt_sample_state_updates)
            self.rt_updates.extend(self._handle_updates(self._mgr.rt_sample_state_updates))

    def _handle_updates(self, mgr_state_updates_dict: dict, is_context_states_update: bool = False) -> list[
        TransactionItem]:
        """Update mdib table and return a list of states to be sent.

        :param mgr_state_updates_dict: updates in transaction
        :param is_context_states_update: bool
        :return: list of states to be sent in notification
        """
        table = self._mdib.context_states if is_context_states_update else self._mdib.states
        updates_list = []
        for transaction_item in mgr_state_updates_dict.values():
            if transaction_item.old is not None:
                table.remove_object_no_lock(transaction_item.old)
            table.add_object_no_lock(transaction_item.new)
            updates_list.append(transaction_item.new.mk_copy(copy_node=False))
        return updates_list

    def _update_corresponding_state(self, descriptor_container: AbstractDescriptorProtocol):
        """Add state to updated_states list and to corresponding notifications input.

        The state is always sent twice, (a) in the description modification report and (b)
        in the specific state update notification.
        """
        if descriptor_container.is_alert_descriptor:
            update_dict = self._mgr.alert_state_updates
        elif descriptor_container.is_component_descriptor:
            update_dict = self._mgr.component_state_updates
        elif descriptor_container.is_context_descriptor:
            update_dict = self._mgr.context_state_updates
        elif descriptor_container.is_realtime_sample_array_metric_descriptor:
            update_dict = self._mgr.rt_sample_state_updates
        elif descriptor_container.is_metric_descriptor:
            update_dict = self._mgr.metric_state_updates
        elif descriptor_container.is_operational_descriptor:
            update_dict = self._mgr.operational_state_updates
        else:
            raise NotImplementedError(f'do not know how to handle {descriptor_container.__class__.__name__}')
        if descriptor_container.is_context_descriptor:
            update_dict = self._mgr.context_state_updates
            all_context_states = self._mdib.context_states.descriptor_handle.get(
                descriptor_container.Handle, [])
            for context_state in all_context_states:
                key = (descriptor_container.Handle, context_state.Handle)
                # check if state is already present in this transaction
                state_update = update_dict.get(key)
                if state_update is not None:
                    # the state has also been updated directly in transaction.
                    # update descriptor version
                    old_state, new_state = state_update
                else:
                    old_state = context_state
                    new_state = old_state.mk_copy()
                    update_dict[key] = TransactionItem(old_state, new_state)
                new_state.descriptor_container = descriptor_container
                new_state.increment_state_version()
                new_state.update_descriptor_version()
        else:
            # check if state is already present in this transaction
            tr_item = update_dict.get(descriptor_container.Handle)
            if tr_item is not None:
                # the state has also been updated directly in transaction.
                # update descriptor version
                if tr_item.new is None:
                    raise ValueError(
                        f'state deleted? that should not be possible! handle = {descriptor_container.Handle}')
                tr_item.new.update_descriptor_version()
            else:
                old_state = self._mdib.states.descriptor_handle.get_one(
                    descriptor_container.Handle, allow_none=True)
                if old_state is not None:
                    new_state = old_state.mk_copy()
                    new_state.descriptor_container = descriptor_container  #
                    new_state.DescriptorVersion = descriptor_container.DescriptorVersion
                    new_state.increment_state_version()
                    update_dict[descriptor_container.Handle] = TransactionItem(old_state, new_state)

    def _increment_parent_descriptor_version(self, descriptor_container: AbstractDescriptorProtocol):
        parent_descriptor_container = self._mdib.descriptions.handle.get_one(
            descriptor_container.parent_handle, allow_none=True)
        if parent_descriptor_container is not None:
            parent_descriptor_container.increment_descriptor_version()
            self.descr_updated.append(parent_descriptor_container.mk_copy())
            self._update_corresponding_state(parent_descriptor_container)

    def _remove_corresponding_state(self, descriptor_container: AbstractDescriptorProtocol):
        if descriptor_container.is_context_descriptor:
            for state in self._mdib.context_states.descriptor_handle.get(descriptor_container.Handle, [])[:]:
                self._mdib.context_states.remove_object_no_lock(state)
        else:
            state = self._mdib.states.descriptor_handle.get_one(descriptor_container.Handle, allow_none=True)
            self._mdib.states.remove_object_no_lock(state)


def mk_transaction(provider_mdib: ProviderMdib,
                   transaction_type: TransactionType,
                   logger: LoggerAdapter) -> AnyTransactionManagerProtocol:
    """Create a transaction according to transaction_type."""
    return MdibUpdateTransaction(provider_mdib, transaction_type, logger)
