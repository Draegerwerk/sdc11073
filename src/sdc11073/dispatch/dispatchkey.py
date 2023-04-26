from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Callable, Union, Protocol

from lxml import etree as etree_

from sdc11073.dispatch.request import RequestData
from .. import loghelper
from ..exceptions import InvalidActionError
from ..pysoap.msgfactory import CreatedMessage
from ..pysoap.soapenvelope import Fault, faultcodeEnum


@dataclass(frozen=True)
class DispatchKey:
    """"Used to associate a handler to a soap message by action - message combination"""
    action: str
    message_tag: Union[etree_.QName, None]

    def __repr__(self):
        """This shows namespace and localname of the QName."""
        if isinstance(self.message_tag, etree_.QName):
            return f'{self.__class__.__name__} action={self.action} ' \
                   f'msg={self.message_tag.namespace}::{self.message_tag.localname}'
        return f'{self.__class__.__name__} action={self.action} msg={self.message_tag}'


OnPostHandler = Callable[[RequestData], CreatedMessage]

OnGetHandler = Callable[[RequestData], bytes]


class RequestHandlerProtocol(Protocol):
    def on_post(self, request_data: RequestData) -> CreatedMessage:
        ...

    def on_get(self, request_data: RequestData) -> str:
        ...


class DispatchKeyRegistry:
    """This class handles messages.
    It allows to register handlers for requests. If a message is passed via on_post, it determines the key,
    gets the registered callback for the key and calls it.
    The key of a message is determined by the provided get_key_method in constructor. Usually it is the
    tag of the message in the body or the action in the SOAP header.
    """

    def __init__(self, log_prefix=None):
        self._post_handlers = {}
        self._get_handlers = {}
        self._logger = loghelper.get_logger_adapter(f'sdc.device.{self.__class__.__name__}', log_prefix)

    def register_post_handler(self, dispatch_key: DispatchKey, on_post_handler: OnPostHandler):
        self._post_handlers[dispatch_key] = on_post_handler

    def register_get_handler(self, dispatch_key: str, on_get_handler: OnGetHandler):
        self._get_handlers[dispatch_key] = on_get_handler

    def on_post(self, request_data: RequestData) -> CreatedMessage:
        begin = time.monotonic()
        action = request_data.message_data.action
        func = self.get_post_handler(request_data)
        if func is None:
            fault = Fault()
            fault.Code.Value = faultcodeEnum.SENDER
            fault.add_reason_text(f'invalid action {action}')

            raise InvalidActionError(fault)
        returned_message = func(request_data)
        duration = time.monotonic() - begin
        self._logger.debug('incoming soap action "{}" to {}: duration={:.3f}sec.', action, request_data.path_elements,
                           duration)
        return returned_message

    def on_get(self, request_data: RequestData) -> str:
        begin = time.monotonic()
        key = request_data.current
        func = self._get_handlers.get(key)
        if func is not None:
            self._logger.debug('on_get:path="{}" ,function="{}"', key, func.__name__)
            result = func()
            duration = time.monotonic() - begin
            self._logger.debug('on_get:duration="{:.4f}"', duration)
            return result
        error_text = f'on_get:path="{key}", no handler found!'
        self._logger.error(error_text)
        raise KeyError(error_text)

    def get_post_handler(self, request_data):
        key = DispatchKey(request_data.message_data.action, request_data.message_data.q_name)
        handler = self._post_handlers.get(key)
        if handler is None:
            self._logger.info('no handler for key={}', key)
        return self._post_handlers.get(key)

    def get_keys(self):
        """ returns a list of action strings that can be handled."""
        return list(self._post_handlers.keys())
