"""Exceptions used throughout the sdc11073 library."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sdc11073.pysoap.soapenvelope import Fault


class ApiUsageError(Exception):
    """Raised when a call is made when it should not be called, e.g. call initialize() twice."""


class HTTPRequestHandlingError(Exception):
    """Communicates errors from http request handlers back to the http server."""

    def __init__(self, status: int, reason: str, soap_fault: Fault):
        """Construct an HTTPRequestHandlingError.

        :param status: integer, e.g. 404
        :param reason: the provided human readable text
        :param soap_fault: soapenvelope.Fault instance
        """
        super().__init__()
        self.status = status
        self.reason = reason
        self.soap_fault = soap_fault

    def __repr__(self) -> str:
        if self.soap_fault:
            return f'{self.__class__.__name__}(status={self.status}, reason={self.soap_fault})'
        return f'{self.__class__.__name__}(status={self.status}, reason={self.reason})'


class FunctionNotImplementedError(HTTPRequestHandlingError):
    """Raised when a requested function is not implemented."""

    def __init__(self, soap_fault: Fault):
        """Construct a FunctionNotImplementedError.

        :param soap_fault: soapenvelope.Fault instance
        """
        super().__init__(500, 'not implemented', soap_fault)


class InvalidActionError(HTTPRequestHandlingError):
    """Raised when the requested action is invalid."""

    def __init__(self, soap_fault: Fault):
        """Construct an InvalidActionError.

        :param soap_fault: soapenvelope.Fault instance
        """
        super().__init__(400, 'Bad Request', soap_fault)


class InvalidPathError(HTTPRequestHandlingError):
    """Raised when the requested path cannot be found."""

    def __init__(self, reason: str, soap_fault: Fault):
        """Construct an InvalidPathError.

        :param reason: the provided human readable text
        :param soap_fault: soapenvelope.Fault instance
        """
        super().__init__(404, reason, soap_fault)


class ValidationError(HTTPRequestHandlingError):
    """Raised when validation of a request fails."""

    def __init__(self, reason: str, soap_fault: Fault):
        """Construct a ValidationError.

        :param reason: the provided human readable text
        :param soap_fault: soapenvelope.Fault instance
        """
        super().__init__(400, reason, soap_fault)
