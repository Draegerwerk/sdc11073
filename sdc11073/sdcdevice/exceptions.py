from ..pysoap.soapenvelope import SoapFault, SoapFaultCode, AdressingFault
class HTTPRequestHandlingError(Exception):
    ''' This class is used to communicate errors from http request handlers back to http server.'''
    def __init__(self, status, reason, soapfault):
        '''
        @param status: integer, e.g. 404
        param reason: the provided human readable text
        '''
        super(HTTPRequestHandlingError, self).__init__()
        self.status = status
        self.reason = reason
        self.soapfault = soapfault

    def __repr__(self):
        if self.soapfault:
            return '{}(status={}, reason={}'.format(self.__class__.__name__, self.status, self.soapfault)
        else:
            return '{}(status={}, reason={}'.format(self.__class__.__name__, self.status, self.reason)


class FunctionNotImplementedError(HTTPRequestHandlingError):
    def __init__(self, request):
        fault = SoapFault(request, code=SoapFaultCode.RECEIVER, reason='not implemented', details='sorry!')
        super().__init__(500, 'not implemented', fault.as_xml())


class InvalidActionError(HTTPRequestHandlingError):
    def __init__(self, request):
        fault = AdressingFault(request,
                               code=SoapFaultCode.SENDER,
                               reason='invalid action {}'.format(request.address.action))
        super().__init__(400, 'Bad Request', fault.as_xml())


class InvalidPathError(HTTPRequestHandlingError):
    def __init__(self, request, path):
        fault = AdressingFault(request,
                               code=SoapFaultCode.SENDER,
                               reason='invalid path {}'.format(path))
        super().__init__(400, 'Bad Request', fault.as_xml())
