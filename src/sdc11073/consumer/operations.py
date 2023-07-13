from __future__ import annotations

import weakref
from concurrent.futures import Future
from threading import Lock
from typing import TYPE_CHECKING, Protocol

from sdc11073 import loghelper

if TYPE_CHECKING:
    from sdc11073.consumer.manipulator import RequestManipulatorProtocol
    from sdc11073.consumer.serviceclients.serviceclientbase import HostedServiceClient
    from sdc11073.pysoap.msgreader import MessageReader, ReceivedMessage
    from sdc11073.pysoap.msgfactory import CreatedMessage

class OperationsManagerProtocol(Protocol):
    """OperationsManager calls an operation.

     It returns a Future object that will contain the result at some point in time.
     """

    def __init__(self, msg_reader: MessageReader, log_prefix: str):
        """Construct the OperationsManager."""
        ...

    def call_operation(self, hosted_service_client: HostedServiceClient,
                       message: CreatedMessage,
                       request_manipulator: RequestManipulatorProtocol | None = None) -> Future:
        """Call an operation.

        An operation call does not return the result of the operation directly. You get a transaction id,
        and will receive the status of this transaction as notification ("OperationInvokedReport").
        This method returns a "Future" object.
        The Future object has a result as soon as a final transaction state is received.
        """

    def on_operation_invoked_report(self, message_data: ReceivedMessage):
        """Check operation state and set future result if it is a final state."""


class OperationsManager(OperationsManagerProtocol):  # inheriting from protocol to help typing
    """OperationsManager handles the multiple messages that are related to an operation.

    The complex mechanic is hidden from user, he receives the final result.
    """

    def __init__(self, msg_reader: MessageReader, log_prefix: str):
        self._msg_reader = msg_reader
        self.log_prefix = log_prefix
        self._logger = loghelper.get_logger_adapter('sdc.client.op_mgr', log_prefix)
        self._transactions = {}
        self._transactions_lock = Lock()
        msg_types = msg_reader.msg_types
        self.nonFinalOperationStates = (msg_types.InvocationState.WAIT, msg_types.InvocationState.START)

    def call_operation(self, hosted_service_client: HostedServiceClient,
                       message: CreatedMessage,
                       request_manipulator: RequestManipulatorProtocol | None = None) -> Future:
        """Call an operation."""
        ret = Future()
        with self._transactions_lock:
            message_data = hosted_service_client.post_message(message,
                                                              msg='call Operation',
                                                              request_manipulator=request_manipulator)
            msg_types = self._msg_reader.msg_types
            abstract_set_response = msg_types.AbstractSetResponse.from_node(message_data.p_msg.msg_node)
            invocation_info = abstract_set_response.InvocationInfo
            if invocation_info.InvocationState in self.nonFinalOperationStates:
                self._transactions[invocation_info.TransactionId] = weakref.ref(ret)
                self._logger.info('call_operation: transaction_id {} registered, state={}',  # noqa: PLE1205
                                  invocation_info.TransactionId, invocation_info.InvocationState)
            else:
                self._logger.debug('Result of Operation: {}', invocation_info)  # noqa: PLE1205
                ret.set_result(abstract_set_response)
        return ret

    def on_operation_invoked_report(self, message_data: ReceivedMessage):
        """Check operation state and set future result if it is a final state."""
        msg_types = self._msg_reader.msg_types
        operation_invoked_report = msg_types.OperationInvokedReport.from_node(message_data.p_msg.msg_node)

        for report_part in operation_invoked_report.ReportPart:
            invocation_state = report_part.InvocationInfo.InvocationState
            transaction_id = report_part.InvocationInfo.TransactionId
            self._logger.debug(  # noqa: PLE1205
                '{}on_operation_invoked_report: got transaction_id {} state {}',
                self.log_prefix, transaction_id, invocation_state)
            if invocation_state in self.nonFinalOperationStates:
                self._logger.debug('nonFinal state detected, ignoring message...')
                continue
            with self._transactions_lock:
                future_ref = self._transactions.pop(transaction_id, None)
            if future_ref is None:
                # this was not my transaction
                self._logger.debug('transaction_id {} is not registered!', transaction_id)  # noqa: PLE1205
                continue
            future_obj = future_ref()
            if future_obj is None:
                # client gave up.
                self._logger.debug('transaction_id {} given up', transaction_id)  # noqa: PLE1205
                continue
            if invocation_state == msg_types.InvocationState.FAILED:
                error_text = ', '.join([err.text for err in report_part.InvocationInfo.InvocationErrorMessage])
                self._logger.warning(  # noqa: PLE1205
                    'transaction Id {} finished with error: error={}, error-message={}',
                                  transaction_id, report_part.InvocationInfo.InvocationError, error_text)
            else:
                self._logger.info('transaction Id {} ok', transaction_id)  # noqa: PLE1205
            future_obj.set_result(report_part)
