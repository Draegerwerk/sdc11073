import weakref
from collections import namedtuple
from concurrent.futures import Future
from threading import Lock

from .. import loghelper
from ..namespaces import nsmap
from ..pmtypes import InvocationState

OperationResult = namedtuple('OperationResult', 'state error errorMsg soapEnvelope')


class OperationsManager:
    nonFinalOperationStates = (InvocationState.WAIT, InvocationState.START)

    def __init__(self, log_prefix):
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
            transaction_id = \
                result_envelope.msg_node.xpath('msg:InvocationInfo/msg:TransactionId/text()', namespaces=nsmap)[0]
            invocation_state = \
                result_envelope.msg_node.xpath('msg:InvocationInfo/msg:InvocationState/text()', namespaces=nsmap)[0]

            if invocation_state in self.nonFinalOperationStates:
                self._transactions[int(transaction_id)] = weakref.ref(ret)
                self._logger.info('call_operation: transaction_id {} registered, state={}', transaction_id,
                                  invocation_state)
            else:
                errors = result_envelope.msg_node.xpath('msg:InvocationInfo/msg:InvocationError/text()',
                                                        namespaces=nsmap)
                error = '' if len(errors) == 0 else str(errors[0])
                error_msgs = result_envelope.msg_node.xpath('msg:InvocationInfo/msg:InvocationErrorMessage/text()',
                                                            namespaces=nsmap)
                error_msg = '' if len(error_msgs) == 0 else str(error_msgs[0])

                result = OperationResult(invocation_state, error, error_msg, result_envelope)
                self._logger.debug('Result of Operation: {}', result)
                ret.set_result(result)
        return ret

    def on_operation_invoked_report(self, envelope):
        self._logger.debug('on_operation_invoked_report: response= {}', lambda: envelope.as_xml(pretty=True))
        transaction_id = \
            envelope.msg_node.xpath('msg:ReportPart/msg:InvocationInfo/msg:TransactionId/text()', namespaces=nsmap)[
                0]
        operation_state = \
            envelope.msg_node.xpath('msg:ReportPart/msg:InvocationInfo/msg:InvocationState/text()',
                                    namespaces=nsmap)[0]
        self._logger.debug('{}on_operation_invoked_report: got transaction_id {} state {}', self.log_prefix,
                           transaction_id,
                           operation_state)
        if operation_state in self.nonFinalOperationStates:
            self._logger.debug('nonFinal state detected, ignoring message...')
            return
        with self._transactions_lock:
            future_ref = self._transactions.pop(int(transaction_id), None)
        if future_ref is None:
            # this was not my transaction
            self._logger.debug('transaction_id {} is not registered!', transaction_id)
            return
        future_obj = future_ref()
        if future_obj is None:
            # client gave up.
            self._logger.debug('transaction_id {} given up', transaction_id)
            return
        errors = envelope.msg_node.xpath('msg:ReportPart/msg:InvocationInfo/msg:InvocationError/text()',
                                         namespaces=nsmap)
        error_msgs = envelope.msg_node.xpath(
            'msg:ReportPart/msg:InvocationInfo/msg:InvocationErrorMessage/text()', namespaces=nsmap)
        result = OperationResult(operation_state, ''.join(errors), ''.join(error_msgs), envelope)
        if operation_state == InvocationState.FAILED:
            self._logger.warn('transaction Id {} finished with error: error={}, error-message={}',
                              transaction_id, result.error, result.errorMsg)
        else:
            self._logger.info('transaction Id {} ok', transaction_id)
        future_obj.set_result(result)
