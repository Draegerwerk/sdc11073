"""Sco Module implements Service Controller Operations(sco) functionality.

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
        self._logger = loghelper.get_logger_adapter('sdc.device.op_worker', log_prefix)

    def enqueue_operation(self, operation, request, operation_request, transaction_id: int):
        """Enqueue operation "operation".

        :param operation: a callable with signature operation(request, mdib)
        :param request: the soapEnvelope of the request
        :param operation_request: parsed argument for the operation handler
        :param transaction_id: int
        """
        self._operations_queue.put((transaction_id, operation, request, operation_request), timeout=1)

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

    def enqueue_operation(self, operation: OperationDefinition, request, operation_request, transaction_id: int):
        """Enqueue operation "operation".

        :param operation: a callable with signature operation(request, mdib)
        :param request: the soapEnvelope of the request
        :param operation_request: the argument for the operation
        :param transaction_id:
        """
        self._worker.enqueue_operation(operation, request, operation_request, transaction_id)

    def start_worker(self):
        if self._worker is not None:
            raise ApiUsageError('SCO worker is already running')
        self._worker = _OperationsWorker(self, self._set_service, self._mdib, self._log_prefix)
        self._worker.start()

    def stop_worker(self):
        if self._worker is not None:
            self._worker.stop()
            self._worker = None
