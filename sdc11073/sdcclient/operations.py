import weakref
from concurrent.futures import Future
from threading import Lock

from .. import loghelper
from ..pmtypes import InvocationState


class OperationsManager:
    nonFinalOperationStates = (InvocationState.WAIT, InvocationState.START)

    def __init__(self, msg_reader, log_prefix):
        self._msg_reader = msg_reader
        self.log_prefix = log_prefix
        self._logger = loghelper.get_logger_adapter('sdc.client.op_mgr', log_prefix)
        self._transactions = {}
        self._transactions_lock = Lock()

    def call_operation(self, hosted_service_client, envelope, request_manipulator=None):
        ''' an operation call does not return the result of the operation directly. Instead you get an transaction id,
        and will receive the status of this transaction as notification ("OperationInvokedReport").
        This method returns a "future" object. The future object has a result as soon as a final transaction state is received.
        :param hosted_service_client:
        :param envelope: the envelope to be sent
        @return: a concurrent.futures.Future object
        '''
        ret = Future()
        with self._transactions_lock:
            result_envelope = hosted_service_client.post_soap_envelope(envelope,
                                                                       msg='call Operation',
                                                                       request_manipulator=request_manipulator)
            operation_invoked_report = self._msg_reader._read_operation_response(result_envelope)

            if operation_invoked_report.invocation_state in self.nonFinalOperationStates:
                self._transactions[operation_invoked_report.transaction_id] = weakref.ref(ret)
                self._logger.info('call_operation: transaction_id {} registered, state={}',
                                  operation_invoked_report.transaction_id, operation_invoked_report.invocation_state)
            else:
                self._logger.debug('Result of Operation: {}', operation_invoked_report)
                ret.set_result(operation_invoked_report)
        return ret

    def on_operation_invoked_report(self, message_data):
        operation_invoked_report = self._msg_reader.read_operation_invoked_report(message_data)
        self._logger.debug('{}on_operation_invoked_report: got transaction_id {} state {}', self.log_prefix,
                           operation_invoked_report.transaction_id,
                           operation_invoked_report.invocation_state)
        if operation_invoked_report.invocation_state in self.nonFinalOperationStates:
            self._logger.debug('nonFinal state detected, ignoring message...')
            return
        with self._transactions_lock:
            future_ref = self._transactions.pop(operation_invoked_report.transaction_id, None)
        if future_ref is None:
            # this was not my transaction
            self._logger.debug('transaction_id {} is not registered!', operation_invoked_report.transaction_id)
            return
        future_obj = future_ref()
        if future_obj is None:
            # client gave up.
            self._logger.debug('transaction_id {} given up', operation_invoked_report.transaction_id)
            return
        if operation_invoked_report.invocation_state == InvocationState.FAILED:
            self._logger.warn('transaction Id {} finished with error: error={}, error-message={}',
                              operation_invoked_report.transaction_id, operation_invoked_report.error,
                              operation_invoked_report.errorMsg)
        else:
            self._logger.info('transaction Id {} ok', operation_invoked_report.transaction_id)
        future_obj.set_result(operation_invoked_report)
