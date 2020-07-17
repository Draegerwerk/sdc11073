import weakref
from threading import Lock
from collections import namedtuple
from ..pmtypes import InvocationState
from .. import loghelper
from ..namespaces import nsmap
from concurrent.futures import Future


OperationResult = namedtuple('OperationResult', 'state error errorMsg soapEnvelope')


class OperationsManager(object):
    nonFinalOperationStates = (InvocationState.WAIT, InvocationState.START)
    def __init__(self, log_prefix):
        self.log_prefix = log_prefix
        self._logger = loghelper.getLoggerAdapter('sdc.client.op_mgr', log_prefix)
        self._transactions = {}
        self._transactionsLock = Lock()


    def callOperation(self, hostedServiceClient, soapEnvelope, request_manipulator=None):
        ''' an operation call does not return the result of the operation directly. Instead you get an transaction id,
        and will receive the status of this transaction as notification ("OperationInvokedReport").
        This method returns a "future" object. The future object has a result as soon as a final transaction state is received.
        @param soapenvelope: the envelope to be sent
        @return: a concurrent.futures.Future object
        '''
        ret = Future()
        with self._transactionsLock:
            resultSoapEnvelope = hostedServiceClient.postSoapEnvelope(soapEnvelope,
                                                                      msg='call Operation',
                                                                      request_manipulator=request_manipulator)
            transactionId = resultSoapEnvelope.msgNode.xpath('msg:InvocationInfo/msg:TransactionId/text()', namespaces=nsmap)[0]
            invocationState = resultSoapEnvelope.msgNode.xpath('msg:InvocationInfo/msg:InvocationState/text()', namespaces=nsmap)[0]

            if invocationState in self.nonFinalOperationStates:
                self._transactions[int(transactionId)] = weakref.ref(ret)
                self._logger.info('callOperation: transactionId {} registered, state={}', transactionId, invocationState)
            else:
                errors = resultSoapEnvelope.msgNode.xpath('msg:InvocationInfo/msg:InvocationError/text()', namespaces=nsmap)
                error = '' if len(errors) == 0 else str(errors[0])
                errorMsgs = resultSoapEnvelope.msgNode.xpath('msg:InvocationInfo/msg:InvocationErrorMessage/text()', namespaces=nsmap)
                errorMsg = '' if len(errorMsgs) == 0 else str(errorMsgs[0])

                result = OperationResult(invocationState, error, errorMsg, resultSoapEnvelope)
                self._logger.debug('Result of Operation: {}',  result)
                ret.set_result(result)
        return ret


    def onOperationInvokedReport(self, soapEnvelope):
        self._logger.debug('onOperationInvokedReport: response= {}', lambda: soapEnvelope.as_xml(pretty=True))
        transactionId = soapEnvelope.msgNode.xpath('msg:ReportPart/msg:InvocationInfo/msg:TransactionId/text()', namespaces=nsmap)[0]
        operationState = soapEnvelope.msgNode.xpath('msg:ReportPart/msg:InvocationInfo/msg:InvocationState/text()', namespaces=nsmap)[0]
        self._logger.debug('{}onOperationInvokedReport: got transactionId {} state {}', self.log_prefix, transactionId, operationState)
        if operationState in self.nonFinalOperationStates:
            self._logger.debug('nonFinal state detected, ignoring message...')
            return
        with self._transactionsLock:
            futureRef = self._transactions.pop(int(transactionId), None)
        if futureRef is None:
            # this was not my transaction
            self._logger.debug('transactionId {} is not registered!', transactionId)
            return
        futureObj = futureRef()
        if futureObj is None:
            # client gave up.
            self._logger.debug('transactionId {} given up', transactionId)
            return
        else:
            errors = soapEnvelope.msgNode.xpath('msg:ReportPart/msg:InvocationInfo/msg:InvocationError/text()', namespaces=nsmap)
            errorMsgs = soapEnvelope.msgNode.xpath('msg:ReportPart/msg:InvocationInfo/msg:InvocationErrorMessage/text()', namespaces=nsmap)
            result = OperationResult(operationState, ''.join(errors), ''.join(errorMsgs), soapEnvelope)
            if operationState == InvocationState.FAILED:
                self._logger.warn('transaction Id {} finished with error: error={}, error-message={}',
                                  transactionId, result.error, result.errorMsg)
            else:
                self._logger.info('transaction Id {} ok', transactionId)
            futureObj.set_result(result)

