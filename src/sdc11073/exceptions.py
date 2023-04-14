
class ApiUsageError(Exception):
    """This Exception is thrown when a call is made when it should not be called, e.g. call initialize() twice."""


class HTTPRequestHandlingError(Exception):
    """ This class is used to communicate errors from http request handlers back to http server."""

    def __init__(self, status, reason, soap_fault):
        """
        :param status: integer, e.g. 404
        :param reason: the provided human readable text
        :param soap_fault: soapenvelope.SoapFault instance
        """
        super().__init__()
        self.status = status
        self.reason = reason
        self.soap_fault = soap_fault

    def __repr__(self):
        if self.soap_fault:
            return f'{self.__class__.__name__}(status={self.status}, reason={self.soap_fault})'
        return f'{self.__class__.__name__}(status={self.status}, reason={self.reason})'


class FunctionNotImplementedError(HTTPRequestHandlingError):
    def __init__(self, soap_fault):
        """
        :param soap_fault: soapenvelope.SoapFault instance
        """
        super().__init__(500, 'not implemented', soap_fault)


class InvalidActionError(HTTPRequestHandlingError):
    def __init__(self, soap_fault):
        """
        :param soap_fault: soapenvelope.SoapFault instance
        """
        super().__init__(400, 'Bad Request', soap_fault)


class InvalidPathError(HTTPRequestHandlingError):
    def __init__(self, reason, soap_fault):
        """
        :param soap_fault: soapenvelope.SoapFault instance
        """
        super().__init__(404, reason, soap_fault)

class ValidationError(HTTPRequestHandlingError):
    def __init__(self, reason, soap_fault):
        """
        :param soap_fault: soapenvelope.SoapFault instance
        """
        super().__init__(400, reason, soap_fault)
