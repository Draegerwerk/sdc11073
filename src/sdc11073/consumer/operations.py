from __future__ import annotations

import weakref
from collections import deque
from concurrent.futures import Future
from dataclasses import dataclass
from threading import Lock
from typing import TYPE_CHECKING, Protocol

from sdc11073 import loghelper

if TYPE_CHECKING:
    from sdc11073.consumer.manipulator import RequestManipulatorProtocol
    from sdc11073.consumer.serviceclients.serviceclientbase import HostedServiceClient
    from sdc11073.pysoap.msgfactory import CreatedMessage
    from sdc11073.pysoap.msgreader import MessageReader, ReceivedMessage
    from sdc11073.xml_types import msg_types, pm_types

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


@dataclass
class OperationResult:
    """OperationResult is the result of a Set operation.

    Usually only the result is relevant, but for testing all intermediate data is also available.
    """

    InvocationInfo: msg_types.InvocationInfo
    InvocationSource: pm_types.InstanceIdentifier | None
    OperationHandleRef: str | None
    OperationTarget: str | None

    set_response: msg_types.AbstractSetResponse
    report_parts: list[
        msg_types.OperationInvokedReportPart]  # contains data of all OperationInvokedReportPart for operation


@dataclass
class OperationData:
    """collect all progress data of a transaction."""

    future_ref: weakref.ref[Future]
    set_response: msg_types.AbstractSetResponse
    report_parts: list[
        msg_types.OperationInvokedReportPart]  # contains data of all OperationInvokedReportPart for operation


class OperationsManager(OperationsManagerProtocol):  # inheriting from protocol to help typing
    """OperationsManager handles the multiple messages that are related to an operation.

    The complex mechanic is hidden from user, he receives the final result.
    """

    def __init__(self, msg_reader: MessageReader, log_prefix: str):
        super().__init__(msg_reader, log_prefix)
        self._msg_reader = msg_reader
        self.log_prefix = log_prefix
        self._logger = loghelper.get_logger_adapter('sdc.client.op_mgr', log_prefix)
        self._transactions: dict[int, OperationData] = {}
        self._transactions_lock = Lock()
        # An OperationInvokedReport can be received even before the response of the set operation is received.
        # This means we must always store the last n OperationInvokedReportParts, one of them might already be the
        # needed one.
        self._last_operation_invoked_reports: deque[msg_types.OperationInvokedReportPart] = deque(maxlen=50)
        msg_types = msg_reader.msg_types
        self.nonFinalOperationStates = (msg_types.InvocationState.WAIT, msg_types.InvocationState.START)

    def call_operation(self,
                       hosted_service_client: HostedServiceClient,
                       message: CreatedMessage,
                       request_manipulator: RequestManipulatorProtocol | None = None) -> Future:
        """Call an operation."""
        future_object = Future()
        with self._transactions_lock:
            message_data = hosted_service_client.post_message(message,
                                                              msg='call Operation',
                                                              request_manipulator=request_manipulator)
            msg_types = self._msg_reader.msg_types
            abstract_set_response = msg_types.AbstractSetResponse.from_node(message_data.p_msg.msg_node)
            invocation_info = abstract_set_response.InvocationInfo
            if invocation_info.InvocationState in (msg_types.InvocationState.FAILED,
                                                   msg_types.InvocationState.CANCELLED,
                                                   msg_types.InvocationState.CANCELLED_MANUALLY):
                # do not wait for an OperationInvokedReport
                operation_result = OperationResult(abstract_set_response.InvocationInfo,
                                                   None,
                                                   None,
                                                   None,
                                                   abstract_set_response,
                                                   [])
                future_object.set_result(operation_result)
                return future_object
            transaction_id = invocation_info.TransactionId
            # now look for all related report parts and add them to result
            parts = [part for part in self._last_operation_invoked_reports if
                     part.InvocationInfo.TransactionId == transaction_id]
            # now look for a final report part
            final_parts = [part for part in parts if
                           part.InvocationInfo.InvocationState not in self.nonFinalOperationStates]
            if final_parts:
                report_part = final_parts[0]  # assuming there is only one
                future_object.set_result(self._mk_operation_result(report_part, abstract_set_response, parts))
            else:
                self._logger.info('call_operation: transaction_id {} registered, state={}',  # noqa: PLE1205
                                  invocation_info.TransactionId, invocation_info.InvocationState)
                self._transactions[transaction_id] = OperationData(weakref.ref(future_object),
                                                                   abstract_set_response,
                                                                   parts)
        return future_object

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
            if transaction_id in self._transactions:
                self._transactions[transaction_id].report_parts.append(report_part)
                if invocation_state in self.nonFinalOperationStates:
                    pass
                else:
                    with self._transactions_lock:
                        operation_data = self._transactions.pop(transaction_id, None)
                    future_object = operation_data.future_ref()
                    if future_object is None:
                        # client gave up.
                        self._logger.debug('transaction_id {} given up', transaction_id)  # noqa: PLE1205
                    else:
                        future_object.set_result(self._mk_operation_result(report_part,
                                                                           operation_data.set_response,
                                                                           operation_data.report_parts))
            else:
                self._last_operation_invoked_reports.append(report_part)

    def _mk_operation_result(self,
                             current_report_part: msg_types.OperationInvokedReportPart,
                             set_response: msg_types.AbstractSetResponse,
                             all_report_parts: list[msg_types.OperationInvokedReportPart]) -> OperationResult:
        return OperationResult(current_report_part.InvocationInfo,
                               current_report_part.InvocationSource,
                               current_report_part.OperationHandleRef,
                               current_report_part.OperationTarget,
                               set_response,
                               all_report_parts)
