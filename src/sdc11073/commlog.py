"""Communication logger."""
from __future__ import annotations

import functools
import logging
import pathlib
import time
from threading import Lock
from typing import Callable

D_IN = 'in'
D_OUT = 'out'

T_UDP = 'udp'
T_UDP_MULTICAST = 'udpB'
T_HTTP = 'http'
T_WSDL = 'wsdl'

T_HTTP_REQ = 'http_req'
T_HTTP_RESP = 'http_resp'

MULTICAST_OUT = 'sdc.com.multicast.out'
DISCOVERY_IN = 'sdc.com.discovery.in'
DISCOVERY_OUT = 'sdc.com.discovery.out'
SOAP_REQUEST_IN = 'sdc.com.soap.request.in'
SOAP_REQUEST_OUT = 'sdc.com.soap.request.out'
SOAP_RESPONSE_IN = 'sdc.com.soap.response.in'
SOAP_RESPONSE_OUT = 'sdc.com.soap.response.out'
SOAP_SUBSCRIPTION_IN = 'sdc.com.soap.subscription.in'
WSDL = 'sdc.com.wsdl'

LOGGER_NAMES = (MULTICAST_OUT, DISCOVERY_IN, DISCOVERY_OUT, SOAP_REQUEST_IN, SOAP_REQUEST_OUT,
                SOAP_RESPONSE_IN, SOAP_RESPONSE_OUT, SOAP_SUBSCRIPTION_IN, WSDL)


class IpFilter(logging.Filter):
    """Filter all messages for the specific ip address."""

    def __init__(self, ip: str):
        super().__init__()
        self.ip = ip

    def filter(self, record: logging.LogRecord) -> bool:  # noqa: D102
        return self.ip == getattr(record, 'ip_address', None)


class CommLogger:
    """Base class to make configuring comm logger easy."""

    def __init__(self):
        self.handlers: dict[str, logging.Handler] = {}

    def start(self) -> None:
        """Start logger."""
        for name, handler in self.handlers.items():
            logger = logging.getLogger(name)
            logger.addHandler(handler)
            logger.setLevel(logging.DEBUG)

    def stop(self) -> None:
        """Stop logger."""
        for name, handler in self.handlers.items():
            logger = logging.getLogger(name)
            logger.removeHandler(handler)

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *args, **kwargs):  # noqa: ANN002, ANN003
        self.stop()


class DirectoryLogger(CommLogger):
    """Logger writing communication logs into a directory. Each message will be contained in a single file."""

    def __init__(self, log_folder: str | pathlib.Path, log_out: bool = False, log_in: bool = False,
                 broadcast_ip_filter: str | None = None):
        super().__init__()
        self._log_folder = pathlib.Path(log_folder)
        self._log_out = log_out
        self._log_in = log_in
        self._broadcast_ip_filter = broadcast_ip_filter
        self._counter = 1
        self._io_lock = Lock()

        self._log_folder.mkdir(parents=True, exist_ok=True)

        if log_in:
            self.handlers.update({
                DISCOVERY_IN: self._GenericHandler(functools.partial(self._write_log, T_UDP, D_IN, info=None)),
                SOAP_REQUEST_IN: self._GenericHandler(functools.partial(self._write_log, T_HTTP_REQ, D_IN)),
                SOAP_RESPONSE_IN: self._GenericHandler(functools.partial(self._write_log, T_HTTP_RESP, D_IN)),
                SOAP_SUBSCRIPTION_IN: self._GenericHandler(
                    functools.partial(self._write_log, T_HTTP, D_IN, info='subscr')),
                WSDL: self._GenericHandler(functools.partial(self._write_log, T_WSDL, D_IN, info=None)),
            })
        if log_out:
            self.handlers.update({
                MULTICAST_OUT: self._GenericHandler(
                    functools.partial(self._write_log, T_UDP_MULTICAST, D_OUT, info=None)),
                DISCOVERY_OUT: self._GenericHandler(functools.partial(self._write_log, T_UDP, D_OUT, info=None)),
                SOAP_REQUEST_OUT: self._GenericHandler(functools.partial(self._write_log, T_HTTP_REQ, D_OUT)),
                SOAP_RESPONSE_OUT: self._GenericHandler(functools.partial(self._write_log, T_HTTP_RESP, D_OUT)),
            })
        if broadcast_ip_filter:
            broadcast_filter = IpFilter(broadcast_ip_filter)
            if DISCOVERY_IN in self.handlers:
                self.handlers[DISCOVERY_IN].addFilter(broadcast_filter)
            if DISCOVERY_OUT in self.handlers:
                self.handlers[DISCOVERY_OUT].addFilter(broadcast_filter)

    def _mk_filename(self, ip_type: str, direction: str, info: str | None) -> str:
        """Create file name.

        :param ip_type: "tcp" or "udp"
        :param direction: "in" or "out"
        :param info: becomes part of filename
        :return:
        """
        assert ip_type in (T_UDP, T_UDP_MULTICAST, T_HTTP, T_HTTP_REQ, T_HTTP_RESP, T_WSDL)
        assert direction in (D_IN, D_OUT)
        extension = 'wsdl' if ip_type == T_WSDL else 'xml'
        time_string = f'{time.time():06.3f}'[-8:]
        self._counter += 1
        info_text = f'-{info}' if info else ''
        return f'{time_string}-{direction}-{ip_type}{info_text}.{extension}'

    def _write_log(self, ttype: str, direction: str, xml: str | bytes, info: str | None) -> None:
        path = self._log_folder.joinpath(self._mk_filename(ttype, direction, info))
        if not isinstance(xml, bytes):
            xml = xml.encode('utf-8')
        with self._io_lock:
            path.write_bytes(xml)

    class _GenericHandler(logging.Handler):
        def __init__(self, emit: Callable):
            super().__init__()
            self._emit = emit

        def emit(self, record: logging.LogRecord) -> None:
            try:
                msg = self.format(record).encode()  # defaults to utf-8
                if ip := getattr(record, 'ip_address', None):
                    self._emit(msg, ip)
                elif http_method := getattr(record, 'http_method', None):
                    self._emit(msg, http_method)
                else:
                    self._emit(msg)
            except Exception:  # noqa: BLE001
                self.handleError(record)


class StreamLogger(CommLogger):
    """Set a stream handler for each comm logger."""

    def __init__(self):
        super().__init__()
        for name in LOGGER_NAMES:
            self.handlers[name] = self._get_handler()

    @staticmethod
    def _get_handler() -> logging.StreamHandler:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(message)s'))
        return handler
