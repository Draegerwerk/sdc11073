from ..pysoap.soapenvelope import AdressingFault
from ..pysoap.soapenvelope import MODE_PUSH
from ..pysoap.soapenvelope import SoapFault
from ..pysoap.soapenvelope import SoapFaultCode


class HTTPRequestHandlingError(Exception):
    """ This class is used to communicate errors from http request handlers back to http server."""

    def __init__(self, status, reason, soapfault):
        """
        :param status: integer, e.g. 404
        param reason: the provided human readable text
        """
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


class InvalidMessageError(HTTPRequestHandlingError):
    def __init__(self, request, detail):
        fault = AdressingFault(request,
                               code=SoapFaultCode.SENDER,
                               reason='The message is not valid and cannot be processed.',
                               details='Detail: {} - The invalid message: {}'.format(detail,
                                                                                     request.rawdata.decode("utf-8")))
        super().__init__(400, 'Bad Request', fault.as_xml())


class DeliveryModeRequestedUnavailableError(HTTPRequestHandlingError):
    def __init__(self, request, detail=None):
        if detail is None:
            detail = f"The only supported mode: {MODE_PUSH}"
        fault = AdressingFault(request,
                               code=SoapFaultCode.SENDER,
                               reason='The requested delivery mode is not supported.',
                               details=detail)
        super().__init__(400, 'Bad Request', fault.as_xml())
