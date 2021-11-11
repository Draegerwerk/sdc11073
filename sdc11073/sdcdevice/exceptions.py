class HTTPRequestHandlingError(Exception):
    ''' This class is used to communicate errors from http request handlers back to http server.'''

    def __init__(self, status, reason, soap_fault):
        '''
        :param status: integer, e.g. 404
        param reason: the provided human readable text
        '''
        super().__init__()
        self.status = status
        self.reason = reason
        self.soap_fault = soap_fault

    def __repr__(self):
        if self.soap_fault:
            return f'{self.__class__.__name__}(status={self.status}, reason={self.soap_fault})'
        return f'{self.__class__.__name__}(status={self.status}, reason={self.reason})'


class FunctionNotImplementedError(HTTPRequestHandlingError):
    def __init__(self, request, fault_xml):
        super().__init__(500, 'not implemented', fault_xml)


class InvalidActionError(HTTPRequestHandlingError):
    def __init__(self, request, fault_xml):
        super().__init__(400, 'Bad Request', fault_xml)


class InvalidPathError(HTTPRequestHandlingError):
    def __init__(self, request, path, fault_xml):
        super().__init__(400, 'Bad Request', fault_xml)
