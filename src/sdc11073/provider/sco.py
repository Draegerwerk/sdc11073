"""
This Module contains code handles Service Controller operations (sco).
All remote control commands of a client are executed by sco instances.

These operations share a common behavior:
A remote control command is executed async. The response to such soap request contains a state (typically 'wait') and a transaction id.
The progress of the transaction is reported with an OperationInvokedReport.
A client must subscribe to the OperationInvokeReport Event of the 'Set' service, otherwise it would not get informed about progress.
"""
from __future__ import annotations

import queue
import threading
import time
import traceback
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Callable, Optional

from .. import loghelper
from .. import observableproperties as properties
from ..exceptions import ApiUsageError

if TYPE_CHECKING:
    from .porttypes.setserviceimpl import SetServiceProtocol
    from lxml.etree import QName


class OperationDefinition:
    """ This is the base class of all provided operations.
    An operation is a point for remote control over the network."""
    current_value = properties.ObservableProperty(fire_only_on_changed_value=False)
    current_request = properties.ObservableProperty(fire_only_on_changed_value=False)
    current_argument = properties.ObservableProperty(fire_only_on_changed_value=False)
    on_timeout = properties.ObservableProperty(fire_only_on_changed_value=False)
    OP_DESCR_QNAME = None
    OP_STATE_QNAME = None
    OP_QNAME = None

    def __init__(self, handle: str,
                 operation_target_handle: str,
                 coded_value=None,
                 log_prefix: Optional[str] = None):
        """
        :param handle: the handle of the operation itself.
        :param operation_target_handle: the handle of the modified data (MdDescription)
        :param coded_value: a pmtypes.CodedValue instance
        """
        self._logger = loghelper.get_logger_adapter(f'sdc.device.op.{self.__class__.__name__}', log_prefix)
        self._mdib = None
        self._descriptor_container = None
        self._operation_state_container = None
        self._operation_target_container = None
        self._handle = handle
        self._operation_target_handle = operation_target_handle
        # documentation of operation_target_handle:
        # A HANDLE reference this operation is targeted to. In case of a single state this is the HANDLE of the descriptor.
        # In case that multiple states may belong to one descriptor (pm:AbstractMultiState), OperationTarget is the HANDLE
        # of one of the state instances (if the state is modified by the operation).
        self._coded_value = coded_value
        self.calls = []  # record when operation was called
        self.last_called_time = None

    @property
    def handle(self):
        return self._handle

    @property
    def operation_target_handle(self):
        return self._operation_target_handle

    @property
    def operation_target_storage(self):
        return self._mdib.states

    @property
    def descriptor_container(self):
        return self._descriptor_container

    def execute_operation(self, request, operation_request):  # pylint: disable=unused-argument
        """ This is the code that executes the operation itself.
        A handler that executes the operation must be bound to observable "current_request"."""
        self.calls.append((time.time(), request))
        self.current_request = request
        self.current_argument = operation_request.argument
        self.last_called_time = time.time()

    def check_timeout(self):
        if self.last_called_time is None:
            return
        if self._descriptor_container.InvocationEffectiveTimeout is None:
            return
        age = time.time() - self.last_called_time
        if age < self._descriptor_container.InvocationEffectiveTimeout:
            return
        self.on_timeout = True  # let observable fire

    def set_mdib(self, mdib, parent_descriptor_container):
        """ The operation needs to know the mdib that it operates on.
        This is called by SubscriptionManager on registration.
        Needs to be implemented by derived classes if specific things have to be initialized."""
        if self._mdib is not None:
            raise ApiUsageError('Mdib is already set')
        self._mdib = mdib
        self._logger.log_prefix = mdib.log_prefix  # use same prefix as mdib for logging
        self._descriptor_container = self._mdib.descriptions.handle.get_one(self._handle, allow_none=True)
        if self._descriptor_container is not None:
            # there is already a descriptor
            self._logger.debug('descriptor for operation "{}" is already present, re-using it', self._handle)
        else:
            cls = mdib.data_model.get_descriptor_container_class(self.OP_DESCR_QNAME)
            self._descriptor_container = cls(self._handle, parent_descriptor_container.Handle)
            self._init_operation_descriptor_container()
            # ToDo: transaction context for flexibility to add operations at runtime
            mdib.descriptions.add_object(self._descriptor_container)

        self._operation_state_container = self._mdib.states.descriptorHandle.get_one(self._handle, allow_none=True)
        if self._operation_state_container is not None:
            self._logger.debug('operation state for operation "{}" is already present, re-using it', self._handle)
        else:
            cls = mdib.data_model.get_state_container_class(self.OP_STATE_QNAME)
            self._operation_state_container = cls(self._descriptor_container)
            mdib.states.add_object(self._operation_state_container)

        # now add the object that is target of operation
        self._init_operation_target_container()

    def _init_operation_descriptor_container(self):
        self._descriptor_container.OperationTarget = self._operation_target_handle
        if self._coded_value is not None:
            self._descriptor_container.Type = self._coded_value

    def _init_operation_target_container(self):
        """ Create the object that is manipulated by the operation"""
        operation_target_descriptor = self._mdib.descriptions.handle.get_one(self._operation_target_handle)
        self._operation_target_container = self._mdib.states.descriptorHandle.get_one(self._operation_target_handle,
                                                                                      allow_none=True)  # pylint:disable=protected-access
        if self._operation_target_container is not None:
            self._logger.debug('operation target state for operation "{}" is already present, re-using it',
                              self._operation_target_handle)
        else:
            self._operation_target_container = self._mdib.data_model.mk_state_container(operation_target_descriptor)
            self._logger.info('creating {} DescriptorHandle = {}', self._operation_target_container.__class__.__name__,
                              self._operation_target_handle)
            if self._operation_target_container is not None:
                storage = self._mdib.context_states if self._operation_target_container.is_multi_state else self._mdib.states
                storage.add_object(self._operation_target_container)

    def set_operating_mode(self, mode):
        """ Mode is one of En, Dis, NA"""
        with self._mdib.transaction_manager() as mgr:
            state = mgr.get_state(self._handle)
            state.OperatingMode = mode

    def collect_values(self, number_of_values=None):
        """ Async way to retrieve next value(s):
        Returns a Future-like object that has a result() method.
        For details see properties.SingleValueCollector and propertiesValuesCollector documentation.
        """
        if number_of_values is None:
            return properties.SingleValueCollector(self, 'current_value')
        return properties.ValuesCollector(self, 'current_value', number_of_values)

    def __str__(self):
        code = '?' if self._descriptor_container is None else self._descriptor_container.Type
        return f'{self.__class__.__name__} handle={self._handle} code={code} operation-target={self._operation_target_handle}'


class _OperationsWorker(threading.Thread):
    """ Thread that enqueues and processes all operations.
    It manages transaction ids for all operations.
    Progress notifications are sent via subscription manager."""

    def __init__(self, operations_registry, set_service: SetServiceProtocol, mdib, log_prefix):
        """
        :param set_service: set_service.notify_operation is called in order to notify all subscribers of OperationInvokeReport Events
        """
        super().__init__(name='DeviceOperationsWorker')
        self.daemon = True
        self._operations_registry = operations_registry
        self._set_service: SetServiceProtocol = set_service
        self._mdib = mdib
        self._operations_queue = queue.Queue(10)  # spooled operations
        self._transaction_id = 1
        self._transaction_id_lock = threading.Lock()
        self._logger = loghelper.get_logger_adapter('sdc.device.op_worker', log_prefix)

    def enqueue_operation(self, operation, request, operation_request):
        """ enqueues operation "operation".
        :param operation: a callable with signature operation(request, mdib)
        :param request: the soapEnvelope of the request
        :param argument: parsed argument for the operation handler
        @return: a transaction Identifier
        """
        with self._transaction_id_lock:
            transaction_id = self._transaction_id
            self._transaction_id += 1
        self._operations_queue.put((transaction_id, operation, request, operation_request), timeout=1)
        return transaction_id

    def run(self):
        data_model = self._mdib.data_model
        InvocationState = data_model.msg_types.InvocationState
        InvocationError = data_model.msg_types.InvocationError
        while True:
            try:
                try:
                    from_queue = self._operations_queue.get(timeout=1.0)
                except queue.Empty:
                    self._operations_registry.check_invocation_timeouts()
                else:
                    if from_queue == 'stop_sco':
                        self._logger.info('stop request found. Terminating now.')
                        return
                    tr_id, operation, request, operation_request = from_queue  # unpack tuple
                    time.sleep(0.001)
                    self._logger.info('{}: starting operation "{}" argument={}',
                                      operation.__class__.__name__, operation.handle, operation_request.argument)
                    # duplicate the WAIT response to the operation request as notification. Standard requires this.
                    self._set_service.notify_operation(
                        operation, tr_id, InvocationState.WAIT, self._mdib.mdib_version_group)
                    time.sleep(0.001)  # not really necessary, but in real world there might also be some delay.
                    self._set_service.notify_operation(
                        operation, tr_id, InvocationState.START, self._mdib.mdib_version_group)
                    try:
                        operation.execute_operation(request, operation_request)
                        self._logger.info('{}: successfully finished operation "{}"', operation.__class__.__name__,
                                          operation.handle)
                        self._set_service.notify_operation(
                            operation, tr_id, InvocationState.FINISHED, self._mdib.mdib_version_group)
                    except Exception as ex:
                        self._logger.error('{}: error executing operation "{}": {}', operation.__class__.__name__,
                                           operation.handle, traceback.format_exc())
                        self._set_service.notify_operation(
                            operation, tr_id, InvocationState.FAILED, self._mdib.mdib_version_group,
                            error=InvocationError.OTHER, error_message=repr(ex))
            except Exception:
                self._logger.error('{}: unexpected error while handling operation: {}',
                                   self.__class__.__name__, traceback.format_exc())

    def stop(self):
        self._operations_queue.put('stop_sco')  # a dummy request to stop the thread
        self.join(timeout=1)


class AbstractScoOperationsRegistry(ABC):

    def __init__(self, set_service: SetServiceProtocol,
                 operation_cls_getter: Callable[[QName], type],
                 mdib,
                 sco_descriptor_container,
                 log_prefix=None):
        self._worker = None
        self._set_service: SetServiceProtocol = set_service
        self.operation_cls_getter = operation_cls_getter
        self._mdib = mdib
        self.sco_descriptor_container = sco_descriptor_container
        self._log_prefix = log_prefix
        self._logger = loghelper.get_logger_adapter('sdc.device.op_reg', log_prefix)
        self._registered_operations = {}  # lookup by handle

    def check_invocation_timeouts(self):
        for op in self._registered_operations.values():
            op.check_timeout()

    @abstractmethod
    def register_operation(self, operation: OperationDefinition, sco_descriptor_container=None) -> None:
        """

        :param operation: OperationDefinition
        :param sco_descriptor_container: a descriptor container
        :return:
        """

    @abstractmethod
    def unregister_operation_by_handle(self, operation_handle: str) -> None:
        """

        :param operation_handle:
        :return:
        """

    @abstractmethod
    def get_operation_by_handle(self, operation_handle: str) -> OperationDefinition:
        """

        :param operation_handle:
        :return:
        """

    @abstractmethod
    def enqueue_operation(self, operation: OperationDefinition, request, argument):
        """ enqueues operation "operation".
        :param operation: a callable with signature operation(request, mdib)
        :param request: the soapEnvelope of the request
        @return: a transaction Id
        """

    @abstractmethod
    def start_worker(self):
        """ start worker thread"""

    @abstractmethod
    def stop_worker(self):
        """ stop worker thread"""


class ScoOperationsRegistry(AbstractScoOperationsRegistry):
    """ Registry for Sco operations.
    from BICEPS:
    A service control object to define remote control operations. Any pm:AbstractOperationDescriptor/@OperationTarget
    within this SCO SHALL only reference this or child descriptors within the CONTAINMENT TREE.
    NOTE - In modular systems, dynamically plugged-in modules would typically be modeled as VMDs.
    Such VMDs potentially have their own SCO. In every other case, SCO operations are modeled in pm:MdsDescriptor/pm:Sco.
    """

    def register_operation(self, operation: OperationDefinition, sco_descriptor_container=None):
        if operation.handle in self._registered_operations:
            self._logger.debug('handle {} is already registered, will re-use it', operation.handle)
        parent_container = sco_descriptor_container or self.sco_descriptor_container
        operation.set_mdib(self._mdib, parent_container)
        self._logger.info('register operation "{}"', operation)
        self._registered_operations[operation.handle] = operation

    def unregister_operation_by_handle(self, operation_handle: str):
        del self._registered_operations[operation_handle]

    def get_operation_by_handle(self, operation_handle: str) -> OperationDefinition:
        return self._registered_operations.get(operation_handle)

    def enqueue_operation(self, operation: OperationDefinition, request, operation_request):
        """ enqueues operation "operation".
        :param operation: a callable with signature operation(request, mdib)
        :param request: the soapEnvelope of the request
        @return: a transaction Id
        """
        return self._worker.enqueue_operation(operation, request, operation_request)

    def start_worker(self):
        if self._worker is not None:
            raise ApiUsageError('SCO worker is already running')
        self._worker = _OperationsWorker(self, self._set_service, self._mdib, self._log_prefix)
        self._worker.start()

    def stop_worker(self):
        if self._worker is not None:
            self._worker.stop()
            self._worker = None
