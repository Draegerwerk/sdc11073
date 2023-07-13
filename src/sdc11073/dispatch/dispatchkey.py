from __future__ import annotations

import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, Protocol

from sdc11073 import loghelper
from sdc11073.dispatch.request import RequestData
from sdc11073.exceptions import InvalidActionError
from sdc11073.pysoap.msgfactory import CreatedMessage
from sdc11073.pysoap.soapenvelope import Fault, faultcodeEnum

if TYPE_CHECKING:
    from lxml.etree import QName


@dataclass(frozen=True)
class DispatchKey:
    """Describes a message + action combination.

    The class is used to associate a handler to a soap message by action - message combination.
    """

    action: str
    message_tag: QName | None

    def __repr__(self) -> str:
        return f'{self.__class__.__name__} action={self.action} msg={self.message_tag}'


OnPostHandler = Callable[[RequestData], CreatedMessage]

OnGetHandler = Callable[[RequestData], bytes]


class RequestHandlerProtocol(Protocol):
    """A request handler handles POST and GET requests."""

    def on_post(self, request_data: RequestData) -> CreatedMessage:
        """Dispatch POST requests."""

    def on_get(self, request_data: RequestData) -> str:
        """Dispatch GET requests."""


class RequestDispatcherProtocol(RequestHandlerProtocol):
    """The Protocol of a RequestDispatcher.

    A RequestDispatcher forwards messages to registered handlers. It implements the RequestHandlerProtocol.
    """

    def __init__(self, log_prefix: str):
        """Construct an instance."""

    def register_post_handler(self, dispatch_key: DispatchKey, on_post_handler: OnPostHandler):
        """Register a POST handler for a DispatchKey.

        The DispatchKey is a specific Soap message body and action string combination.
        An incoming message with this message body and action is forwarded to th on_post_handler.
        """

    def register_get_handler(self, dispatch_key: str, on_get_handler: OnGetHandler):
        """Register a GET handler for a DispatchKey.

        The DispatchKey is a specific Soap message body and action string combination.
        An incoming message with this message body and action is forwarded to th on_get_handler.
        """


class RequestDispatcher(RequestDispatcherProtocol):  # derive from protocol to help typing identify it.
    """The RequestDispatcher forwards messages to registered handlers.

    it implements the RequestDispatcherProtocol.
    """

    def __init__(self, log_prefix: str | None = None):
        self._post_handlers = {}
        self._get_handlers = {}
        self._logger = loghelper.get_logger_adapter(f'sdc.device.{self.__class__.__name__}', log_prefix)

    def register_post_handler(self, dispatch_key: DispatchKey, on_post_handler: OnPostHandler):
        """See documentation in RequestDispatcherProtocol."""
        self._post_handlers[dispatch_key] = on_post_handler

    def register_get_handler(self, dispatch_key: str, on_get_handler: OnGetHandler):
        """See documentation in RequestDispatcherProtocol."""
        self._get_handlers[dispatch_key] = on_get_handler

    def on_post(self, request_data: RequestData) -> CreatedMessage:
        """See documentation in RequestHandlerProtocol."""
        begin = time.monotonic()
        action = request_data.message_data.action
        func = self._get_post_handler(request_data)
        if func is None:
            fault = Fault()
            fault.Code.Value = faultcodeEnum.SENDER
            fault.add_reason_text(f'invalid action {action}')

            raise InvalidActionError(fault)
        returned_message = func(request_data)
        duration = time.monotonic() - begin
        self._logger.debug(  # noqa: PLE1205
            'incoming soap action "{}" to {}: duration={:.3f}sec.', action, request_data.path_elements, duration)
        return returned_message

    def on_get(self, request_data: RequestData) -> str:
        """See documentation in RequestHandlerProtocol."""
        begin = time.monotonic()
        key = request_data.current_path_element  # the current path element is the dispatch key
        func = self._get_handlers.get(key)
        if func is not None:
            self._logger.debug('on_get:path="{}" ,function="{}"', key, func.__name__)  # noqa: PLE1205
            result = func()
            duration = time.monotonic() - begin
            self._logger.debug('on_get:duration="{:.4f}"', duration)  # noqa: PLE1205
            return result
        error_text = f'on_get:path="{key}", no handler found!'
        self._logger.error(error_text)
        raise KeyError(error_text)

    def _get_post_handler(self, request_data: RequestData) -> OnPostHandler:
        key = DispatchKey(request_data.message_data.action, request_data.message_data.q_name)
        handler = self._post_handlers.get(key)
        if handler is None:
            self._logger.info('no handler for key={}', key)  # noqa: PLE1205
        return self._post_handlers.get(key)
