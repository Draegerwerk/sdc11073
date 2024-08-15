"""Communication logger."""
from __future__ import annotations

import functools
import logging
import pathlib
import threading
import time
from typing import Any, Callable

DISCOVERY_IN = 'sdc_comm.discovery.in'
DISCOVERY_OUT = 'sdc_comm.discovery.out'
SOAP_REQUEST_IN = 'sdc_comm.soap.request.in'
SOAP_REQUEST_OUT = 'sdc_comm.soap.request.out'
SOAP_RESPONSE_IN = 'sdc_comm.soap.response.in'
SOAP_RESPONSE_OUT = 'sdc_comm.soap.response.out'
SOAP_SUBSCRIPTION_IN = 'sdc_comm.soap.subscription.in'
WSDL = 'sdc_comm.wsdl'

LOGGER_NAMES = (DISCOVERY_IN, DISCOVERY_OUT, SOAP_REQUEST_IN, SOAP_REQUEST_OUT,
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

    D_IN = 'in'
    D_OUT = 'out'

    T_UDP = 'udp'
    T_UDP_MULTICAST = 'udpB'
    T_HTTP = 'http_subscr'
    T_WSDL = 'wsdl'

    T_HTTP_REQ = 'http_req'
    T_HTTP_RESP = 'http_resp'

    def __init__(self, log_folder: str | pathlib.Path, log_out: bool = False, log_in: bool = False,
                 broadcast_ip_filter: str | None = None):
        super().__init__()
        self._log_folder = pathlib.Path(log_folder)
        self._counter = 1
        self._io_lock = threading.Lock()

        if log_in:
            self.handlers.update({
                DISCOVERY_IN: self._GenericHandler(functools.partial(self._write_log, self.T_UDP, self.D_IN)),
                SOAP_REQUEST_IN: self._GenericHandler(functools.partial(self._write_log, self.T_HTTP_REQ, self.D_IN)),
                SOAP_RESPONSE_IN: self._GenericHandler(functools.partial(self._write_log, self.T_HTTP_RESP, self.D_IN)),
                SOAP_SUBSCRIPTION_IN: self._GenericHandler(functools.partial(self._write_log, self.T_HTTP, self.D_IN)),
                WSDL: self._GenericHandler(functools.partial(self._write_log, self.T_WSDL, self.D_IN)),
            })
        if log_out:
            self.handlers.update({
                DISCOVERY_OUT: self._GenericHandler(functools.partial(self._write_log, self.T_UDP, self.D_OUT)),
                SOAP_REQUEST_OUT: self._GenericHandler(functools.partial(self._write_log, self.T_HTTP_REQ, self.D_OUT)),
                SOAP_RESPONSE_OUT: self._GenericHandler(
                    functools.partial(self._write_log, self.T_HTTP_RESP, self.D_OUT)),
            })
        if broadcast_ip_filter:
            broadcast_filter = IpFilter(broadcast_ip_filter)
            if DISCOVERY_IN in self.handlers:
                self.handlers[DISCOVERY_IN].addFilter(broadcast_filter)
            if DISCOVERY_OUT in self.handlers:
                self.handlers[DISCOVERY_OUT].addFilter(broadcast_filter)

    def start(self) -> None:
        self._log_folder.mkdir(parents=True, exist_ok=True)
        super().start()

    def _mk_filename(self, ip_type: str, direction: str, *infos: str) -> str:
        """Create file name.

        :param ip_type: "tcp" or "udp"
        :param direction: "in" or "out"
        :param info: becomes part of filename
        :return:
        """
        assert ip_type in (self.T_UDP, self.T_UDP_MULTICAST, self.T_HTTP,
                           self.T_HTTP_REQ, self.T_HTTP_RESP, self.T_WSDL)
        assert direction in (self.D_IN, self.D_OUT)
        extension = 'wsdl' if ip_type == self.T_WSDL else 'xml'
        time_string = f'{time.time():06.3f}'[-8:]
        self._counter += 1
        info_text = f'-{"-".join(infos)}' if infos else ''
        return f'{time_string}-{direction}-{ip_type}{info_text}.{extension}'

    def _write_log(self, ttype: str, direction: str, msg: bytes, *infos: str) -> None:
        path = self._log_folder.joinpath(self._mk_filename(ttype, direction, *infos))
        with self._io_lock:
            path.write_bytes(msg)

    class _GenericHandler(logging.Handler):
        def __init__(self, emit: Callable):
            super().__init__()
            self._emit = emit

        def emit(self, record: logging.LogRecord) -> None:
            try:
                msg = self.format(record).encode()  # defaults to utf-8
                args = []
                if ip := getattr(record, 'ip_address', None):
                    args.append(ip)
                if http_method := getattr(record, 'http_method', None):
                    args.append(http_method)
                self._emit(msg, *args)
            except Exception:  # noqa: BLE001
                self.handleError(record)


class StreamLogger(CommLogger):
    """Set a stream handler for each comm logger."""

    def __init__(self, stream: Any | None = None, broadcast_ip_filter: str | None = None):
        super().__init__()
        for name in LOGGER_NAMES:
            self.handlers[name] = self._get_handler(stream)

        if broadcast_ip_filter:
            broadcast_filter = IpFilter(broadcast_ip_filter)
            if DISCOVERY_IN in self.handlers:
                self.handlers[DISCOVERY_IN].addFilter(broadcast_filter)
            if DISCOVERY_OUT in self.handlers:
                self.handlers[DISCOVERY_OUT].addFilter(broadcast_filter)

    @staticmethod
    def _get_handler(stream: Any | None) -> logging.StreamHandler:
        handler = logging.StreamHandler(stream=stream)
        handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(message)s'))
        return handler
