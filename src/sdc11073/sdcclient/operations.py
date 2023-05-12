from __future__ import annotations
import weakref
from concurrent.futures import Future
from threading import Lock
from typing import TYPE_CHECKING, Optional
from .. import loghelper

if TYPE_CHECKING:
    from ..sdcclient.manipulator import RequestManipulatorProtocol
class OperationsManager:

    def __init__(self, msg_reader, log_prefix):
        self._msg_reader = msg_reader
        self.log_prefix = log_prefix
        self._logger = loghelper.get_logger_adapter('sdc.client.op_mgr', log_prefix)
        self._transactions = {}
        self._transactions_lock = Lock()
        msg_types = msg_reader.msg_types
        self.nonFinalOperationStates = (msg_types.InvocationState.WAIT, msg_types.InvocationState.START)

    def call_operation(self, hosted_service_client,
                       message,
                       request_manipulator: Optional[RequestManipulatorProtocol] = None):
        """ An operation call does not return the result of the operation directly. You get a transaction id,
        and will receive the status of this transaction as notification ("OperationInvokedReport").
        This method returns a "future" object.
        The future object has a result as soon as a final transaction state is received.
        :param hosted_service_client:
        :param message: the CreatedMessage to be sent
        :param request_manipulator: see documentation of RequestManipulatorProtocol
        :return: a concurrent.futures.Future object
        """
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
                self._logger.info('call_operation: transaction_id {} registered, state={}',
                                  invocation_info.TransactionId, invocation_info.InvocationState)
            else:
                self._logger.debug('Result of Operation: {}', invocation_info)
                ret.set_result(abstract_set_response)
        return ret

    def on_operation_invoked_report(self, message_data):
        msg_types = self._msg_reader.msg_types
        operation_invoked_report = msg_types.OperationInvokedReport.from_node(message_data.p_msg.msg_node)

        for report_part in operation_invoked_report.ReportPart:
            invocation_state = report_part.InvocationInfo.InvocationState
            transaction_id = report_part.InvocationInfo.TransactionId
            self._logger.debug('{}on_operation_invoked_report: got transaction_id {} state {}', self.log_prefix,
                               transaction_id, invocation_state)
            if invocation_state in self.nonFinalOperationStates:
                self._logger.debug('nonFinal state detected, ignoring message...')
                continue
            with self._transactions_lock:
                future_ref = self._transactions.pop(transaction_id, None)
            if future_ref is None:
                # this was not my transaction
                self._logger.debug('transaction_id {} is not registered!', transaction_id)
                continue
            future_obj = future_ref()
            if future_obj is None:
                # client gave up.
                self._logger.debug('transaction_id {} given up', transaction_id)
                continue
            if invocation_state == msg_types.InvocationState.FAILED:
                error_text = ', '.join([l.text for l in report_part.InvocationInfo.InvocationErrorMessage])
                self._logger.warn('transaction Id {} finished with error: error={}, error-message={}',
                                  transaction_id, report_part.InvocationInfo.InvocationError, error_text)
            else:
                self._logger.info('transaction Id {} ok', transaction_id)
            future_obj.set_result(report_part)
