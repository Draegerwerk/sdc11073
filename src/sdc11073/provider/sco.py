"""Sco Module implements Service Controller Operations(sco) functionality.

All remote control commands of a client are executed by sco instances.

These operations share a common behavior:
A remote control command is executed. The response to such soap request contains a state
(typically 'wait') and a transaction id.
The progress of the transaction is reported with an OperationInvokedReport.
A client must subscribe to the OperationInvokeReport Event of the 'Set' service,
otherwise it would not get informed about progress.
"""
from __future__ import annotations

import queue
import threading
import time
import traceback
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from sdc11073 import loghelper
from sdc11073.exceptions import ApiUsageError

if TYPE_CHECKING:
    from enum import Enum

    from sdc11073.mdib.descriptorcontainers import AbstractDescriptorProtocol
    from sdc11073.mdib.providermdib import ProviderMdib
    from sdc11073.pysoap.soapenvelope import ReceivedSoapMessage
    from sdc11073.xml_types.msg_types import AbstractSet
    from sdc11073.roles.providerbase import OperationClassGetter
    from .operations import OperationDefinitionBase
    from .porttypes.setserviceimpl import SetServiceProtocol


class _OperationsWorker(threading.Thread):
    """Thread that enqueues and processes all operations.

    Progress notifications are sent via subscription manager.
    """

    def __init__(self,
                 operations_registry: AbstractScoOperationsRegistry,
                 set_service: SetServiceProtocol,
                 mdib: ProviderMdib,
                 log_prefix: str):
        super().__init__(name='DeviceOperationsWorker')
        self.daemon = True
        self._operations_registry = operations_registry
        self._set_service: SetServiceProtocol = set_service
        self._mdib = mdib
        self._operations_queue = queue.Queue(10)  # spooled operations
        self._logger = loghelper.get_logger_adapter('sdc.device.op_worker', log_prefix)

    def enqueue_operation(self, operation: OperationDefinitionBase,
                          request: ReceivedSoapMessage,
                          operation_request: AbstractSet,
                          transaction_id: int):
        """Enqueue operation."""
        self._operations_queue.put((transaction_id, operation, request, operation_request), timeout=1)

    def run(self):
        data_model = self._mdib.data_model
        InvocationState = data_model.msg_types.InvocationState  # noqa: N806
        InvocationError = data_model.msg_types.InvocationError  # noqa: N806
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
                    self._logger.info('%s: starting operation "%s" argument=%r',
                                      operation.__class__.__name__, operation.handle, operation_request.argument)
                    # duplicate the WAIT response to the operation request as notification. Standard requires this.
                    self._set_service.notify_operation(
                        operation, tr_id, InvocationState.WAIT, self._mdib.mdib_version_group)
                    time.sleep(0.001)  # not really necessary, but in real world there might also be some delay.
                    self._set_service.notify_operation(
                        operation, tr_id, InvocationState.START, self._mdib.mdib_version_group)
                    try:
                        execute_result = operation.execute_operation(request, operation_request)
                        self._logger.info('%s: successfully finished operation "%s"',
                                          operation.__class__.__name__, operation.handle)
                        self._set_service.notify_operation(
                            operation, tr_id, execute_result.invocation_state,
                            self._mdib.mdib_version_group, execute_result.operation_target_handle)
                    except Exception as ex:
                        self._logger.error('%s: error executing operation "%s": %s', operation.__class__.__name__,
                                           operation.handle, traceback.format_exc())
                        self._set_service.notify_operation(
                            operation, tr_id, InvocationState.FAILED, self._mdib.mdib_version_group,
                            error=InvocationError.OTHER, error_message=repr(ex))
            except Exception:
                self._logger.error('%s: unexpected error while handling operation: %s',
                                   self.__class__.__name__, traceback.format_exc())

    def stop(self):
        self._operations_queue.put('stop_sco')  # a dummy request to stop the thread
        self.join(timeout=1)


class AbstractScoOperationsRegistry(ABC):
    """Base class for a Sco."""

    def __init__(self, set_service: SetServiceProtocol,
                 operation_cls_getter: OperationClassGetter,
                 mdib: ProviderMdib,
                 sco_descriptor_container: AbstractDescriptorProtocol,
                 log_prefix: str | None = None):
        self._worker = None
        self._set_service: SetServiceProtocol = set_service
        self.operation_cls_getter = operation_cls_getter
        self._mdib = mdib
        self.sco_descriptor_container = sco_descriptor_container
        self._log_prefix = log_prefix
        self._logger = loghelper.get_logger_adapter('sdc.device.op_reg', log_prefix)
        self._registered_operations = {}  # lookup by handle

    def check_invocation_timeouts(self):
        """Call check_timeout of all registered operations."""
        for op in self._registered_operations.values():
            op.check_timeout()

    @abstractmethod
    def register_operation(self, operation: OperationDefinitionBase) -> None:
        """Register the operation."""

    @abstractmethod
    def unregister_operation_by_handle(self, operation_handle: str) -> None:
        """Un-register the operation."""

    @abstractmethod
    def get_operation_by_handle(self, operation_handle: str) -> OperationDefinitionBase:
        """Get OperationDefinition for given handle."""

    @abstractmethod
    def handle_operation_request(self, operation: OperationDefinitionBase,
                                 request: ReceivedSoapMessage,
                                 operation_request: AbstractSet,
                                 transaction_id: int) -> Enum:
        """Handle operation "operation"."""

    @abstractmethod
    def start_worker(self):
        """Start worker thread."""

    @abstractmethod
    def stop_worker(self):
        """Stop worker thread."""


class ScoOperationsRegistry(AbstractScoOperationsRegistry):
    """Registry for Sco operations.

    from BICEPS:
    A service control object to define remote control operations. Any pm:AbstractOperationDescriptor/@OperationTarget
    within this SCO SHALL only reference this or child descriptors within the CONTAINMENT TREE.
    NOTE - In modular systems, dynamically plugged-in modules would typically be modeled as VMDs.
    Such VMDs potentially have their own SCO. In every other case, SCO operations are modeled in pm:MdsDescriptor/pm:Sco.
    """

    def register_operation(self, operation: OperationDefinitionBase):
        """Register the operation."""
        if operation.handle in self._registered_operations:
            self._logger.debug('handle %s is already registered, will re-use it', operation.handle)
        operation.set_mdib(self._mdib, self.sco_descriptor_container.Handle)
        self._logger.info('register operation "%s"', operation)
        self._registered_operations[operation.handle] = operation

    def unregister_operation_by_handle(self, operation_handle: str):
        """Un-register the operation."""
        del self._registered_operations[operation_handle]

    def get_operation_by_handle(self, operation_handle: str) -> OperationDefinitionBase:
        """Get OperationDefinition for given handle."""
        return self._registered_operations.get(operation_handle)

    def handle_operation_request(self, operation: OperationDefinitionBase,
                                 request: ReceivedSoapMessage,
                                 operation_request: AbstractSet,
                                 transaction_id: int) -> Enum:
        """Handle operation immediately or delayed in worker thread, depending on operation.delayed_processing."""
        InvocationState = self._mdib.data_model.msg_types.InvocationState  # noqa: N806

        if operation.delayed_processing:
            self._worker.enqueue_operation(operation, request, operation_request, transaction_id)
            return InvocationState.WAIT
        try:
            execute_result = operation.execute_operation(request, operation_request)
            self._logger.info('%s: successfully finished operation "%s"', operation.__class__.__name__,
                              operation.handle)
            self._set_service.notify_operation(operation, transaction_id, execute_result.invocation_state,
                                               self._mdib.mdib_version_group, execute_result.operation_target_handle)
            self._logger.debug('notifications for operation %s sent', operation.handle)
            return InvocationState.FINISHED
        except Exception as ex:
            self._logger.error('%s: error executing operation "%s": %s', operation.__class__.__name__,
                               operation.handle, traceback.format_exc())
            self._set_service.notify_operation(
                operation, transaction_id, InvocationState.FAILED, self._mdib.mdib_version_group,
                error=self._mdib.data_model.msg_types.InvocationError.OTHER, error_message=repr(ex))
            return InvocationState.FAILED

    def start_worker(self):
        """Start worker thread."""
        if self._worker is not None:
            raise ApiUsageError('SCO worker is already running')
        self._worker = _OperationsWorker(self, self._set_service, self._mdib, self._log_prefix)
        self._worker.start()

    def stop_worker(self):
        """Stop worker thread."""
        if self._worker is not None:
            self._worker.stop()
            self._worker = None
