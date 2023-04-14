import os
import time
from threading import Lock

D_IN = 'in'
D_OUT = 'out'

T_UDP = 'udp'
T_UDP_MULTICAST = 'udpB'
T_HTTP = 'http'
T_WSDL = 'wsdl'

T_HTTP_REQ = 'http_req'
T_HTTP_RESP = 'http_resp'


class NullLogger:
    """This is a dummy logger that does nothing."""

    def __getattr__(self, name):
        return self.do_nothing

    def do_nothing(self, *args, **kwargs):
        pass


class CommLogger:
    """This is the logger that writes communication logs."""

    def __init__(self, log_folder, log_out=False, log_in=False, broadcast_ip_filter=None):
        self._log_folder = log_folder
        self._log_out = log_out
        self._log_in = log_in
        self._broadcast_ip_filter = broadcast_ip_filter
        self._counter = 1
        self._io_lock = Lock()

        self._mk_log_folder(log_folder)

    def set_broadcast_ip_filter(self, broadcast_ip_filter):
        self._broadcast_ip_filter = broadcast_ip_filter

    @staticmethod
    def _mk_log_folder(path):
        if not os.path.exists(path):
            os.makedirs(path)

    def _mk_filename(self, ip_type, direction, info):
        """
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

    def _write_log(self, ttype, direction, xml, info):
        path = os.path.join(self._log_folder, self._mk_filename(ttype, direction, info))
        with self._io_lock:
            with open(path, 'wb') as my_file:
                if isinstance(xml, bytes):
                    my_file.write(xml)
                else:
                    my_file.write(xml.encode('utf-8'))

    def log_multicast_msg_out(self, xml, info=None):
        if self._log_out:
            self._write_log(T_UDP_MULTICAST, D_OUT, xml, info)

    def log_discovery_msg_out(self, ipaddr, xml, info=None):
        if self._log_out and (not self._broadcast_ip_filter or self._broadcast_ip_filter == ipaddr):
            self._write_log(T_UDP, D_OUT, xml, info)

    def log_discovery_msg_in(self, ipaddr, xml):
        if self._log_in and (not self._broadcast_ip_filter or self._broadcast_ip_filter == ipaddr):
            self._write_log(T_UDP, D_IN, xml, None)

    def log_soap_request_in(self, xml, info=None):
        if self._log_in:
            self._write_log(T_HTTP_REQ, D_IN, xml, info)

    def log_soap_request_out(self, xml, info=None):
        if self._log_out:
            self._write_log(T_HTTP_REQ, D_OUT, xml, info)

    def log_soap_response_in(self, xml, info=None):
        if self._log_in:
            self._write_log(T_HTTP_RESP, D_IN, xml, info)

    def log_soap_response_out(self, xml, info=None):
        if self._log_out:
            self._write_log(T_HTTP_RESP, D_OUT, xml, info)

    def log_soap_subscription_msg_in(self, xml):
        if self._log_in:
            self._write_log(T_HTTP, D_IN, xml, 'subscr')

    def log_wsdl(self, wsdl):
        if self._log_in:
            self._write_log(T_WSDL, D_IN, wsdl, None)


class _LogManager:
    def __init__(self):
        self.comm_logger = NullLogger()

    def set_logger(self, comm_logger):
        self.comm_logger = comm_logger

_MGR = _LogManager()


def get_communication_logger():
    return _MGR.comm_logger


def set_communication_logger(comm_logger):
    _MGR.set_logger(comm_logger)
