from __future__ import annotations

import logging
import platform
import queue
import random
import re
import selectors
import socket
import struct
import threading
import time
import traceback
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any
from typing import List, TYPE_CHECKING, Optional, Union
from urllib.parse import urlsplit, unquote

# pylint: disable=no-name-in-module
from lxml.etree import QName, XMLSyntaxError

from .commlog import get_communication_logger
from .definitions_sdc import SDC_v1_Definitions
from .exceptions import ApiUsageError
from .exceptions import ValidationError
from .namespaces import default_ns_helper as nsh
from .netconn import get_ipv4_addresses, get_ip_for_adapter, get_ipv4_ips
from .pysoap.msgfactory import MessageFactory
from .pysoap.msgreader import MessageReader
from .pysoap.soapenvelope import Soap12Envelope
from .xml_types import wsd_types
from .xml_types.addressing_types import HeaderInformationBlock

# pylint: enable=no-name-in-module


if TYPE_CHECKING:
    from .pysoap.msgreader import ReceivedMessage

message_factory = MessageFactory(SDC_v1_Definitions, None, logger=logging.getLogger('sdc.discover.msg'))
message_reader = MessageReader(SDC_v1_Definitions, None, logger=logging.getLogger('sdc.discover.msg'))

BUFFER_SIZE = 0xffff
APP_MAX_DELAY = 500  # miliseconds
DP_MAX_TIMEOUT = 5000  # 5 seconds

_NETWORK_ADDRESSES_CHECK_TIMEOUT = 5

MULTICAST_PORT = 3702
MULTICAST_IPV4_ADDRESS = "239.255.255.250"
MULTICAST_OUT_TTL = 15  # Time To Live for multicast_out

UNICAST_UDP_REPEAT = 2
UNICAST_UDP_MIN_DELAY = 50
UNICAST_UDP_MAX_DELAY = 250
UNICAST_UDP_UPPER_DELAY = 500

MULTICAST_UDP_REPEAT = 4
MULTICAST_UDP_MIN_DELAY = 50
MULTICAST_UDP_MAX_DELAY = 250
MULTICAST_UDP_UPPER_DELAY = 500

# pylint: disable=protected-access, redefined-outer-name, len-as-condition, attribute-defined-outside-init

NS_D = nsh.WSD.namespace

ADDRESS_ALL = "urn:docs-oasis-open-org:ws-dd:ns:discovery:2009:01"  # format acc to RFC 2141
WSA_ANONYMOUS = nsh.WSA.namespace + '/anonymous'
MATCH_BY_LDAP = NS_D + '/ldap'  # "http://docs.oasis-open.org/ws-dd/ns/discovery/2009/01/ldap"
MATCH_BY_URI = NS_D + '/rfc3986'  # "http://docs.oasis-open.org/ws-dd/ns/discovery/2009/01/rfc3986"
MATCH_BY_UUID = NS_D + '/uuid'  # "http://docs.oasis-open.org/ws-dd/ns/discovery/2009/01/uuid"
MATCH_BY_STRCMP = NS_D + '/strcmp0'  # "http://docs.oasis-open.org/ws-dd/ns/discovery/2009/01/strcmp0"

# these time constants control the send-loop
SEND_LOOP_IDLE_SLEEP = 0.1
SEND_LOOP_BUSY_SLEEP = 0.01


def types_info(types):
    # helper for logging
    return [str(t) for t in types] if types else types


def generate_instance_id():
    return str(random.randint(1, 0xFFFFFFFF))


Scopes = wsd_types.ScopesType


class _MessageType(Enum):
    MULTICAST = 1
    UNICAST = 2


@dataclass(frozen=True)
class Message:
    env: Soap12Envelope
    addr: str
    port: int
    msg_type: _MessageType


def _mk_wsd_soap_message(header_info, payload):
    # use discovery specific namespaces
    return message_factory.mk_soap_message(header_info, payload,
                                           ns_list=[nsh.S12, nsh.WSA, nsh.WSD], use_defaults=False)


class Service:
    def __init__(self, types: Optional[List[QName]], scopes: Optional[wsd_types.ScopesType], x_addrs, epr, instance_id,
                 metadata_version=1):
        self.types = types
        if scopes is not None:
            assert isinstance(scopes, wsd_types.ScopesType)
        self.scopes = scopes
        self._x_addrs = x_addrs or []
        self.epr = epr
        self.instance_id = instance_id
        self.message_number = 0
        self.metadata_version = metadata_version

    def get_x_addrs(self):
        ret = []
        ip_addrs = None
        for x_addr in self._x_addrs:
            if '{ip}' in x_addr:
                if ip_addrs is None:
                    ip_addrs = get_ipv4_addresses()
                for ip_addr in ip_addrs:
                    ret.append(x_addr.format(ip=ip_addr))
            else:
                ret.append(x_addr)
        return ret

    def set_x_addrs(self, x_addrs):
        self._x_addrs = x_addrs

    def increment_message_number(self):
        self.message_number = self.message_number + 1

    def is_located_on(self, *ip_addresses):
        """
        :param ip_addresses: ip addresses, lists of strings or strings
        """
        my_addresses = []
        for ip_address in ip_addresses:
            if isinstance(ip_address, str):
                my_addresses.append(ip_address)
            else:
                my_addresses.extend(ip_address)
        for addr in self.get_x_addrs():
            parsed = urlsplit(addr)
            ip_addr = parsed.netloc.split(':')[0]
            if ip_addr in my_addresses:
                return True
        return False

    def __repr__(self):
        scopes_str = 'None' if self.scopes is None else ', '.join([str(x) for x in self.scopes.text])
        types_str = 'None' if self.types is None else ', '.join([str(x) for x in self.types])
        return f'Service epr={self.epr}, instanceId={self.instance_id} Xaddr={self._x_addrs} ' \
               f'scopes={scopes_str} types={types_str}'

    def __str__(self):
        scopes_str = 'None' if self.scopes is None else ', '.join([str(x) for x in self.scopes.text])
        types_str = 'None' if self.types is None else ', '.join([str(x) for x in self.types])
        return f'Service epr={self.epr}, instanceId={self.instance_id}\n' \
               f'   Xaddr={self._x_addrs}\n' \
               f'   scopes={scopes_str}\n' \
               f'   types={types_str}'


def match_scope(my_scope: str, other_scope: str, match_by: str):
    """ This implementation correctly handles "%2F" (== '/') encoded values"""
    if match_by == "" or match_by is None or match_by == MATCH_BY_LDAP or match_by == MATCH_BY_URI or match_by == MATCH_BY_UUID:
        my_scope = urlsplit(my_scope)
        other_scope = urlsplit(other_scope)
        if my_scope.scheme.lower() != other_scope.scheme.lower():
            return False
        if my_scope.netloc.lower() != other_scope.netloc.lower():
            return False
        if my_scope.path == other_scope.path:
            return True
        src_path_elements = my_scope.path.split('/')
        target_path_elements = other_scope.path.split('/')
        src_path_elements = [unquote(elem) for elem in src_path_elements]
        target_path_elements = [unquote(elem) for elem in target_path_elements]
        if len(src_path_elements) > len(target_path_elements):
            return False
        for i, elem in enumerate(src_path_elements):
            if target_path_elements[i] != elem:
                return False
        return True
    if match_by == MATCH_BY_STRCMP:
        return my_scope == other_scope
    return False


def match_type(type1, type2):
    return type1.namespace == type2.namespace and type1.localname == type2.localname


class _AddressMonitorThread(threading.Thread):
    """ This thread frequently checks the available Network adapters.
    Any change is reported vis wsd._network_address_removed or wsd._network_address_added
    """

    def __init__(self, wsd):
        self._addresses = set()
        self._wsd = wsd
        self._logger = logging.getLogger('sdc.discover.monitor')
        self._quit_event = threading.Event()
        super().__init__(name='AddressMonitorThread')
        self.daemon = True
        self._update_addresses()

    def _update_addresses(self):
        addresses = set(get_ipv4_addresses())

        disappeared = self._addresses.difference(addresses)
        new = addresses.difference(self._addresses)

        for address in disappeared:
            self._wsd._network_address_removed(address)

        for address in new:
            self._wsd._network_address_added(address)
        self._addresses = addresses

    def run(self):
        try:
            while not self._quit_event.wait(_NETWORK_ADDRESSES_CHECK_TIMEOUT):
                self._update_addresses()
        except Exception:
            self._logger.error('Unhandled Exception at thread runtime. Thread will abort! %s',
                               traceback.format_exc())
            raise

    def schedule_stop(self):
        """Schedule stopping the thread.
        Use join() to wait, until thread really has been stopped
        """
        self._quit_event.set()


@dataclass(frozen=True)
class _Sockets:
    multi_in: socket.socket
    multi_out_uni_in: socket.socket
    uni_in: Union[socket.socket, None]


class _NetworkingThreadBase:
    """ Has one thread for sending and one for receiving"""

    @dataclass(order=True)
    class _EnqueuedMessage:
        send_time: float
        msg: Any = field(compare=False)
        repeat: int

    def __init__(self, observer, logger, multicast_port):
        self._observer = observer
        self._logger = logger
        self.multicast_port = multicast_port
        self._recv_thread = None
        self._qread_thread = None
        self._send_thread = None
        self._quit_recv_event = threading.Event()
        self._quit_send_event = threading.Event()
        self._send_queue = queue.PriorityQueue(10000)
        self._read_queue = queue.Queue(10000)
        self._known_message_ids = deque(maxlen=50)
        self._select_in = []
        self._full_selector = selectors.DefaultSelector()
        self._sockets_by_address = {}
        self._sockets_by_address_lock = threading.RLock()
        self._uni_out_socket = None

    def _register(self, sock):
        self._select_in.append(sock)
        self._full_selector.register(sock, selectors.EVENT_READ)

    def _unregister(self, sock):
        self._select_in.remove(sock)
        self._full_selector.unregister(sock)

    @staticmethod
    def _make_mreq(addr):
        return struct.pack("4s4s", socket.inet_aton(MULTICAST_IPV4_ADDRESS), socket.inet_aton(addr))

    @staticmethod
    def _create_multicast_out_socket(addr):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, MULTICAST_OUT_TTL)
        if addr is None:
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, socket.INADDR_ANY)
        else:
            _addr = socket.inet_aton(addr)
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, _addr)
        return sock

    def remove_source_addr(self, addr):
        sockets = self._sockets_by_address.get(addr)
        if sockets:
            with self._sockets_by_address_lock:
                for sock in (sockets.multi_in, sockets.uni_in, sockets.multi_out_uni_in):
                    if sock is not None:
                        self._unregister(sock)
                        sock.close()
                del self._sockets_by_address[addr]

    def add_unicast_message(self, env, addr, port, initial_delay=0):
        msg = Message(env, addr, port, _MessageType.UNICAST)
        self._logger.debug('add_unicast_message: adding message Id %s. initial delay=%.2f msec',
                           env.p_msg.header_info_block.MessageID, initial_delay)
        self._repeated_enqueue_msg(msg, initial_delay, UNICAST_UDP_REPEAT, UNICAST_UDP_MIN_DELAY,
                                   UNICAST_UDP_MAX_DELAY, UNICAST_UDP_UPPER_DELAY)

    def add_multicast_message(self, env, addr, port, initial_delay=0):
        msg = Message(env, addr, port, _MessageType.MULTICAST)
        self._logger.debug('add_multicast_message: adding message Id %s. initial delay=%.2f msec',
                           env.p_msg.header_info_block.MessageID, initial_delay)
        self._repeated_enqueue_msg(msg, initial_delay, MULTICAST_UDP_REPEAT, MULTICAST_UDP_MIN_DELAY,
                                   MULTICAST_UDP_MAX_DELAY, MULTICAST_UDP_UPPER_DELAY)

    def _repeated_enqueue_msg(self, msg, initial_delay_ms, repeat, min_delay_ms, max_delay_ms, upper_delay_ms):
        next_send = time.time() + initial_delay_ms / 1000.0
        delta_t = random.randrange(min_delay_ms, max_delay_ms) / 1000.0  # millisec -> seconds
        self._send_queue.put(self._EnqueuedMessage(next_send, msg, 1))
        for i in range(repeat):
            next_send += delta_t
            self._send_queue.put(self._EnqueuedMessage(next_send, msg, i + 2))
            delta_t = min(delta_t * 2, upper_delay_ms)

    def _run_send(self):
        """send-loop"""
        while not self._quit_send_event.is_set():
            if self._send_queue.empty():
                time.sleep(SEND_LOOP_IDLE_SLEEP)  # nothing to do currently
            else:
                if self._send_queue.queue[0].send_time <= time.time():
                    enqueued_msg = self._send_queue.get()
                    self._send_msg(enqueued_msg)
                else:
                    time.sleep(SEND_LOOP_BUSY_SLEEP)  # this creates a 10ms raster for sending, but that is good enough

    def _run_recv(self):
        """ run by thread"""
        while not self._quit_recv_event.is_set():
            if len(self._sockets_by_address) == 0:
                # avoid errors while no sockets are registered
                time.sleep(0.1)
                continue
            try:
                self._recv_messages()
            except:  # pylint: disable=bare-except
                if not self._quit_recv_event.is_set():  # only log error if it does not happen during stop
                    self._logger.error('_run_recv:%s', traceback.format_exc())

    def is_from_my_socket(self, addr):
        with self._sockets_by_address_lock:
            for ip_addr, sock_pair in self._sockets_by_address.items():
                if addr[0] == ip_addr:
                    try:
                        sock_name = sock_pair.multi_out_uni_in.getsockname()
                        if addr[1] == sock_name[1]:  # compare ports
                            return True
                    except OSError:  # port is not opened
                        continue
        return False

    def _recv_messages(self):
        """For performance reasons this thread only writes to a queue, no parsing etc."""
        for key, _ in self._full_selector.select(timeout=0.1):
            sock = key.fileobj
            try:
                data, addr = sock.recvfrom(BUFFER_SIZE)
            except socket.error as exc:
                self._logger.warning('socket read error %s', exc)
                time.sleep(0.01)
                continue
            if self.is_from_my_socket(addr):
                continue
            self._add_to_recv_queue(addr, data)

    def _add_to_recv_queue(self, addr, data: bytes):
        # method is needed for testing
        self._read_queue.put((addr, data))

    def _run_q_read(self):
        """Read from internal queue and process message"""
        while not self._quit_recv_event.is_set():
            try:
                incoming = self._read_queue.get(timeout=0.1)
            except queue.Empty:
                pass
            else:
                addr, data = incoming
                get_communication_logger().log_discovery_msg_in(addr[0], data)
                try:
                    try:
                        received_message = message_reader.read_received_message(data, validate=True)
                    except (XMLSyntaxError, ValidationError) as ex:
                        self._logger.info('_run_q_read: received invalid message from %r, ignoring it (error=%s)', addr,
                                          ex)
                    else:
                        mid = received_message.p_msg.header_info_block.MessageID
                        if mid in self._known_message_ids:
                            self._logger.debug('incoming message already known :%s (from %r, Id %s).',
                                               received_message.action, addr, mid)
                            continue
                        self._known_message_ids.appendleft(mid)
                        self._observer.handle_received_message(received_message, addr)
                except Exception as ex:
                    self._logger.error('_run_q_read: %s', traceback.format_exc())

    def _send_msg(self, q_msg: _EnqueuedMessage):
        msg = q_msg.msg
        data = msg.env.serialize()
        if msg.msg_type == _MessageType.UNICAST:
            self._logger.debug('send unicast %d bytes (%d) action=%s: to=%s:%r id=%s',
                               len(data),
                               q_msg.repeat,
                               msg.env.p_msg.header_info_block.Action.text,
                               msg.addr, msg.port,
                               msg.env.p_msg.header_info_block.MessageID)
            get_communication_logger().log_discovery_msg_out(msg.addr, data)
            self._uni_out_socket.sendto(data, (msg.addr, msg.port))
        else:
            get_communication_logger().log_multicast_msg_out(data)
            with self._sockets_by_address_lock:
                for sock_pair in self._sockets_by_address.values():
                    self._logger.debug('send multicast %d bytes, msg (%d) action=%s: to=%s:%r id=%s',
                                       len(data),
                                       q_msg.repeat,
                                       msg.env.p_msg.header_info_block.Action.text,
                                       msg.addr, msg.port,
                                       msg.env.p_msg.header_info_block.MessageID)
                    sock_pair.multi_out_uni_in.sendto(data, (msg.addr, msg.port))

    def start(self):
        self._logger.debug('%s: starting ', self.__class__.__name__)
        self._uni_out_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._recv_thread = threading.Thread(target=self._run_recv, name='wsd.recvThread')
        self._qread_thread = threading.Thread(target=self._run_q_read, name='wsd.qreadThread')
        self._send_thread = threading.Thread(target=self._run_send, name='wsd.sendThread')
        self._recv_thread.daemon = True
        self._qread_thread.daemon = True
        self._send_thread.daemon = True
        self._recv_thread.start()
        self._qread_thread.start()
        self._send_thread.start()

    def schedule_stop(self):
        """Schedule stopping the thread.
        Use join() to wait, until thread really has been stopped
        """
        self._logger.debug('%s: schedule_stop ', self.__class__.__name__)
        self._quit_recv_event.set()
        self._quit_send_event.set()

    def join(self):
        self._logger.debug('%s: join... ', self.__class__.__name__)
        self._recv_thread.join(1)
        self._send_thread.join(1)
        self._qread_thread.join(1)
        self._recv_thread = None
        self._send_thread = None
        self._qread_thread = None
        for sock in self._select_in:
            sock.close()
        self._uni_out_socket.close()
        self._full_selector.close()
        self._logger.debug('%s: ... join done', self.__class__.__name__)

    def get_active_addresses(self):
        with self._sockets_by_address_lock:
            return list(self._sockets_by_address.keys())


class _NetworkingThreadWindows(_NetworkingThreadBase):
    def _create_multicast_in_socket(self, addr, port):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((addr, port))
        sock.setblocking(False)
        try:
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, self._make_mreq(addr))
        except socket.error as ex:  # if 1 interface has more than 1 address, exception is raised for the second
            print(traceback.format_exc())
            self._logger.error('could not join MULTICAST group on %s:%d error = %r', addr, port, ex)
            return None

        self._logger.info('UDP socket listens on %s:%d', addr, port)
        return sock

    def add_source_addr(self, addr):
        """None means 'system default'"""
        multicast_in_sock = self._create_multicast_in_socket(addr, self.multicast_port)
        if multicast_in_sock is None:
            return
        multicast_out_sock = self._create_multicast_out_socket(addr)
        with self._sockets_by_address_lock:
            self._register(multicast_out_sock)
            self._register(multicast_in_sock)
            self._sockets_by_address[addr] = _Sockets(multicast_in_sock, multicast_out_sock, None)


class _NetworkingThreadPosix(_NetworkingThreadBase):
    def _create_multicast_in_socket(self, addr, port):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((MULTICAST_IPV4_ADDRESS, port))
        sock.setblocking(False)
        try:
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, self._make_mreq(addr))
        except socket.error as ex:  # if 1 interface has more than 1 address, exception is raised for the second
            print(traceback.format_exc())
            self._logger.error('could not join MULTICAST group on %s:%d error = %r', addr, port, ex)
            return None
        self._logger.info('UDP socket listens on %s:%d', addr, port)
        return sock

    def _create_unicast_in_socket(self, addr, port):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((addr, port))
        sock.setblocking(False)
        return sock

    def add_source_addr(self, addr):
        """None means 'system default'"""
        multicast_in_sock = self._create_multicast_in_socket(addr, self.multicast_port)
        if multicast_in_sock is None:
            return

        # unicast_in_sock is needed for handling of unicast messages on multicast port
        unicast_in_sock = self._create_unicast_in_socket(addr, self.multicast_port)
        multicast_out_sock = self._create_multicast_out_socket(addr)
        with self._sockets_by_address_lock:
            self._register(multicast_out_sock)
            self._register(unicast_in_sock)
            self._register(multicast_in_sock)
            self._sockets_by_address[addr] = _Sockets(multicast_in_sock, multicast_out_sock, unicast_in_sock)


def _is_type_in_list(ttype, types):
    for entry in types:
        if match_type(ttype, entry):
            return True
    return False


def _is_scope_in_list(uri: str, match_by: str, srv_sc: wsd_types.ScopesType):
    # returns True if every entry in scope.text is also found in srv_sc.text
    # all entries are URIs
    if srv_sc is None:
        return False
    for entry in srv_sc.text:
        if match_scope(uri, entry, match_by):
            return True
    return False


def _matches_filter(service, types, scopes: Optional[wsd_types.ScopesType], logger=None):
    if types is not None:
        srv_ty = service.types
        for ttype in types:
            if not _is_type_in_list(ttype, srv_ty):
                if logger:
                    logger.debug(f'types not matching: {ttype} is not in types list {srv_ty}')
                return False
        if logger:
            logger.debug('matching types')
    if scopes is not None:
        srv_sc = service.scopes
        for uri in scopes.text:
            if not _is_scope_in_list(uri, scopes.MatchBy, srv_sc):
                if logger:
                    logger.debug(f'scope not matching: {uri} is not in scopes list {srv_sc}')
                return False
        if logger:
            logger.debug('matching scopes')
    return True


def filter_services(services, types, scopes, logger=None):
    return [service for service in services if _matches_filter(service, types, scopes, logger)]


class WSDiscoveryBase:
    # UDP based discovery.
    # these flags control which data is included in ProbeResponse messages.
    PROBEMATCH_EPR = True
    PROBEMATCH_TYPES = True
    PROBEMATCH_SCOPES = True
    PROBEMATCH_XADDRS = True

    def __init__(self, logger=None, multicast_port=None):
        """
        :param logger: use this logger. if None a logger 'sdc.discover' is created.
        """
        self._networking_thread = None
        self._addrs_monitor_thread = None
        self._server_started = False
        self._remote_services = {}
        self._local_services = {}

        self._disco_proxy_active = False  # True if discovery proxy detected (is not relevant in sdc context)
        self.__disco_proxy_address = None
        self._disco_proxy_epr = None

        self._remote_service_hello_callback = None
        self._remote_service_hello_callback_types_filter = None
        self._remote_service_hello_callback_scopes_filter = None
        self._remote_service_bye_callback = None
        self._remote_service_resolve_match_callback = None  # B.D.
        self._on_probe_callback = None

        self._logger = logger or logging.getLogger('sdc.discover')
        self.multicast_port = multicast_port or MULTICAST_PORT
        random.seed(int(time.time() * 1000000))

    def start(self):
        """start the discovery server - should be called before using other functions"""
        if not self._server_started:
            self._start_threads()
            self._server_started = True

    def stop(self):
        """cleans up and stops the discovery server"""
        if self._server_started:
            self.clear_remote_services()
            self.clear_local_services()

            self._stop_threads()
            self._server_started = False

    def search_services(self,
                        types: Optional[List[QName]] = None,
                        scopes: Optional[wsd_types.ScopesType] = None,
                        timeout: Optional[Union[int, float]] = 5,
                        repeat_probe_interval: Optional[int] = 3):
        """
        search for services that match given types and scopes
        :param types:
        :param scopes:
        :param timeout: total duration of search
        :param repeat_probe_interval: send another probe message after x seconds
        :return:
        """
        if not self._server_started:
            raise RuntimeError("Server not started")

        start = time.monotonic()
        end = start + timeout
        now = time.monotonic()
        while now < end:
            self._send_probe(types, scopes)
            if now + repeat_probe_interval <= end:
                time.sleep(repeat_probe_interval)
            elif now < end:
                time.sleep(end - now)
            now = time.monotonic()
        return filter_services(self._remote_services.values(), types, scopes)

    def search_sdc_services(self,
                            scopes: Optional[wsd_types.ScopesType] = None,
                            timeout: Optional[Union[int, float]] = 5,
                            repeat_probe_interval: Optional[int] = 3):
        """
        search for sdc services that match given scopes
        :param scopes:
        :param timeout: total duration of search
        :param repeat_probe_interval: send another probe message after x seconds
        :return:
        """
        return self.search_services(SDC_v1_Definitions.MedicalDeviceTypesFilter, scopes, timeout, repeat_probe_interval)

    def search_multiple_types(self,
                              types_list: List[List[QName]],
                              scopes: Optional[wsd_types.ScopesType] = None,
                              timeout: Optional[Union[int, float]] = 10,
                              repeat_probe_interval: Optional[int] = 3):
        """search for services given the list of TYPES and SCOPES in a given timeout.
        It returns services that match at least one of the types (OR condition).
        Can be used to search for devices that support Biceps Draft6 and Final with one search.
        :param types_list:
        :param scopes:
        :param timeout: total duration of search
        :param repeat_probe_interval: send another probe message after x seconds"""
        if not self._server_started:
            raise ApiUsageError("Server not started")

        start = time.monotonic()
        end = start + timeout
        now = time.monotonic()
        while now < end:
            for _type in types_list:
                self._send_probe(_type, scopes)
            now = time.monotonic()
            if now + repeat_probe_interval <= end:
                time.sleep(repeat_probe_interval)
            elif now < end:
                time.sleep(end - now)
        # prevent possible duplicates by adding them to a dictionary by epr
        result = {}
        for _type in types_list:
            tmp = filter_services(self._remote_services.values(), _type, scopes)
            for srv in tmp:
                result[srv.epr] = srv
        return list(result.values())

    def search_sdc_device_services_in_location(self, sdc_location, timeout=3):
        services = self.search_sdc_services(timeout=timeout)
        return sdc_location.matching_services(services)

    def publish_service(self, epr: str,
                        types: List[QName],
                        scopes: wsd_types.ScopesType,
                        x_addrs: List[str]):
        """Publish a service with the given TYPES, SCOPES and XAddrs (service addresses)

        if x_addrs contains item, which includes {ip} pattern, one item per IP address will be sent
        """
        if not self._server_started:
            raise ApiUsageError("Server not started")

        metadata_version = self._local_services[epr].metadata_version + 1 if epr in self._local_services else 1
        service = Service(types, scopes, x_addrs, epr, generate_instance_id(), metadata_version=metadata_version)
        self._logger.info('publishing %r', service)
        self._local_services[epr] = service
        self._send_hello(service)

    def clear_remote_services(self):
        """clears remotely discovered services"""
        self._remote_services.clear()

    def clear_local_services(self):
        """send Bye messages for the services and remove them"""
        for service in self._local_services.values():
            self._send_bye(service)
        self._local_services.clear()

    def clear_service(self, epr):
        service = self._local_services[epr]
        self._send_bye(service)
        del self._local_services[epr]

    def get_active_addresses(self):
        return self._networking_thread.get_active_addresses()

    def set_remote_service_hello_callback(self, callback, types=None, scopes=None):
        """Set callback, which will be called when new service appeared online
        and sent Hi message

        typesFilter and scopesFilter might be list of types and scopes.
        If filter is set, callback is called only for Hello messages,
        which match filter

        Set None to disable callback
        """
        self._remote_service_hello_callback = callback
        self._remote_service_hello_callback_types_filter = types
        self._remote_service_hello_callback_scopes_filter = scopes

    def set_remote_service_bye_callback(self, callback):
        """Set callback, which will be called when new service appeared online
        and sent Hi message
        Service is passed as a parameter to the callback
        Set None to disable callback
        """
        self._remote_service_bye_callback = callback

    def set_remote_service_resolve_match_callback(self, callback):  # B.D.
        self._remote_service_resolve_match_callback = callback

    def set_on_probe_callback(self, callback):
        self._on_probe_callback = callback

    def _add_remote_service(self, service):
        epr = service.epr
        if not epr:
            self._logger.info('service without epr, ignoring it! %r', service)
            return
        already_known_service = self._remote_services.get(service.epr)
        if not already_known_service:
            self._remote_services[service.epr] = service
            self._logger.info('new remote %r', service)
        else:
            if service.metadata_version == already_known_service.metadata_version:
                self._logger.debug('update remote service: remote Service %s; MetadataVersion: %d',
                                   service.epr, service.metadata_version)
                if len(service.get_x_addrs()) > len(already_known_service.get_x_addrs()):
                    already_known_service.set_x_addrs(service.get_x_addrs())
                if service.scopes is not None:
                    already_known_service.scopes = service.scopes
                if service.types is not None:
                    already_known_service.types = service.types
            elif service.metadata_version > already_known_service.metadata_version:
                self._logger.info('remote Service %s:\n    updated MetadataVersion\n      '
                                  'updated: %d\n      existing: %d',
                                  service.epr, service.metadata_version, already_known_service.metadata_version)
                self._remote_services[service.epr] = service
            else:
                self._logger.debug('_add_remote_service: remote Service %s:\n    outdated MetadataVersion\n      '
                                   'outdated: %d\n      existing: %d',
                                   service.epr, service.metadata_version, already_known_service.metadata_version)

    def _remove_remote_service(self, epr):
        if epr in self._remote_services:
            del self._remote_services[epr]

    def handle_received_message(self, received_message: ReceivedMessage, addr: str):
        act = received_message.action
        self._logger.debug('handle_received_message: received %s from %s', act.split('/')[-1], addr)

        app_sequence_node = received_message.p_msg.header_node.find(nsh.WSD.tag('AppSequence'))

        if act == wsd_types.ProbeMatchesType.action:
            app_sequence = wsd_types.AppSequenceType.from_node(app_sequence_node)
            probe_matches = wsd_types.ProbeMatchesType.from_node(received_message.p_msg.msg_node)
            self._logger.debug('handle_received_message: len(ProbeMatch) = %d', len(probe_matches.ProbeMatch))
            for match in probe_matches.ProbeMatch:
                epr = match.EndpointReference.Address
                scopes = match.Scopes
                service = Service(match.Types, scopes, match.XAddrs, epr,
                                  app_sequence.InstanceId, metadata_version=match.MetadataVersion)
                self._add_remote_service(service)
                if match.XAddrs is None or len(match.XAddrs) == 0:
                    self._logger.info('%s(%s) has no Xaddr, sending resolve message', epr, addr)
                    self._send_resolve(epr)
                elif not match.Types:
                    self._logger.info('%s(%s) has no Types, sending resolve message', epr, addr)
                    self._send_resolve(epr)
                elif not match.Scopes:
                    self._logger.info('%s(%s) has no Scopes, sending resolve message', epr, addr)
                    self._send_resolve(epr)

        elif act == wsd_types.ResolveMatchesType.action:
            app_sequence = wsd_types.AppSequenceType.from_node(app_sequence_node)
            resolve_matches = wsd_types.ResolveMatchesType.from_node(received_message.p_msg.msg_node)
            match = resolve_matches.ResolveMatch
            epr = match.EndpointReference.Address
            scopes = match.Scopes
            service = Service(match.Types, scopes, match.XAddrs, epr,
                              app_sequence.InstanceId, metadata_version=match.MetadataVersion)
            self._add_remote_service(service)
            if self._remote_service_resolve_match_callback is not None:
                self._remote_service_resolve_match_callback(service)

        elif act == wsd_types.ProbeType.action:
            probe = wsd_types.ProbeType.from_node(received_message.p_msg.msg_node)
            scopes = probe.Scopes
            services = filter_services(self._local_services.values(), probe.Types, scopes)
            if services:
                self._send_probe_match(services, received_message.p_msg.header_info_block.MessageID, addr)
            if self._on_probe_callback is not None:
                self._on_probe_callback(addr, probe)

        elif act == wsd_types.ResolveType.action:
            resolve = wsd_types.ResolveType.from_node(received_message.p_msg.msg_node)
            epr = resolve.EndpointReference.Address
            if epr in self._local_services:
                service = self._local_services[epr]
                self._send_resolve_match(service, received_message.p_msg.header_info_block.MessageID, addr)

        elif act == wsd_types.HelloType.action:
            app_sequence = wsd_types.AppSequenceType.from_node(app_sequence_node)
            hello = wsd_types.HelloType.from_node(received_message.p_msg.msg_node)
            epr = hello.EndpointReference.Address
            # check if it is from a discovery proxy
            relates_to = received_message.p_msg.header_info_block.RelatesTo
            if relates_to is not None and relates_to.RelationshipType == nsh.WSD.tag('Suppression'):
                x_addr = hello.XAddrs[0]
                if x_addr.startswith("soap.udp:"):
                    self._disco_proxy_active = True
                    tmp = urlsplit(hello.XAddrs[0])
                    self.__disco_proxy_address = (tmp.hostname, tmp.port)
                    self._disco_proxy_epr = epr
            scopes = hello.Scopes
            service = Service(hello.Types, scopes, hello.XAddrs, epr,
                              app_sequence.InstanceId, metadata_version=hello.MetadataVersion)
            self._add_remote_service(service)
            if not hello.XAddrs:  # B.D.
                self._logger.debug('%s(%s) has no Xaddr, sending resolve message', epr, addr)
                self._send_resolve(epr)
            if self._remote_service_hello_callback is not None:
                if _matches_filter(service,
                                   self._remote_service_hello_callback_types_filter,
                                   self._remote_service_hello_callback_scopes_filter):
                    self._remote_service_hello_callback(addr, service)

        elif act == wsd_types.ByeType.action:  # ACTION_BYE:
            bye = wsd_types.ByeType.from_node(received_message.p_msg.msg_node)
            epr = bye.EndpointReference.Address
            # if the bye is from discovery proxy... revert back to multicasting
            if self._disco_proxy_active and self._disco_proxy_epr == epr:
                self._disco_proxy_active = False
                self.__disco_proxy_address = None
                self._disco_proxy_epr = None

            self._remove_remote_service(epr)
            if self._remote_service_bye_callback is not None:
                self._remote_service_bye_callback(addr, epr)
        else:
            self._logger.info('unknown action %s', act)

    def _send_resolve_match(self, service: Service, relates_to, addr):
        self._logger.info('sending resolve match to %s', addr)
        service.increment_message_number()
        payload = wsd_types.ResolveMatchesType()
        payload.ResolveMatch = wsd_types.ResolveMatchType()
        payload.ResolveMatch.EndpointReference.Address = service.epr
        payload.ResolveMatch.MetadataVersion = service.metadata_version
        payload.ResolveMatch.Types = service.types
        payload.ResolveMatch.Scopes = service.scopes
        payload.ResolveMatch.XAddrs.extend(service.get_x_addrs())
        inf = HeaderInformationBlock(action=payload.action,
                                     addr_to=WSA_ANONYMOUS,
                                     relates_to=relates_to)
        app_sequence = wsd_types.AppSequenceType()
        app_sequence.InstanceId = int(service.instance_id)
        app_sequence.MessageNumber = service.message_number

        created_message = _mk_wsd_soap_message(inf, payload)
        created_message.p_msg.add_header_element(app_sequence.as_etree_node(nsh.WSD.tag('AppSequence'),
                                                                            ns_map=nsh.partial_map(nsh.WSD)))
        self._networking_thread.add_unicast_message(created_message, addr[0], addr[1], random.randint(0, APP_MAX_DELAY))

    def _send_probe_match(self, services, relates_to, addr):
        self._logger.info('sending probe match to %s for %d services', addr, len(services))
        msg_number = 1
        # send one match response for every service, dpws explorer can't handle telegram otherwise if too many devices reported
        for service in services:
            payload = wsd_types.ProbeMatchesType()

            # add values to ProbeResponse acc. to flags
            epr = service.epr if self.PROBEMATCH_EPR else ''
            types = service.types if self.PROBEMATCH_TYPES else []
            scopes = service.scopes if self.PROBEMATCH_SCOPES else None
            xaddrs = service.get_x_addrs() if self.PROBEMATCH_XADDRS else []

            probe_match = wsd_types.ProbeMatchType()
            probe_match.EndpointReference.Address = epr
            probe_match.MetadataVersion = service.metadata_version
            probe_match.Types = types
            probe_match.Scopes = scopes
            probe_match.XAddrs.extend(xaddrs)
            payload.ProbeMatch.append(probe_match)
            inf = HeaderInformationBlock(action=payload.action,
                                         addr_to=WSA_ANONYMOUS,
                                         relates_to=relates_to)
            app_sequence = wsd_types.AppSequenceType()
            app_sequence.InstanceId = int(service.instance_id)
            app_sequence.MessageNumber = msg_number

            created_message = _mk_wsd_soap_message(inf, payload)
            created_message.p_msg.add_header_element(app_sequence.as_etree_node(nsh.WSD.tag('AppSequence'),
                                                                                ns_map=nsh.partial_map(nsh.WSD)))
            self._networking_thread.add_unicast_message(created_message, addr[0], addr[1],
                                                        random.randint(0, APP_MAX_DELAY))

    def _send_probe(self, types=None, scopes: Optional[wsd_types.ScopesType] = None):
        self._logger.debug('sending probe types=%r scopes=%r', types_info(types), scopes)
        payload = wsd_types.ProbeType()
        payload.Types = types
        if scopes is not None:
            payload.Scopes = scopes

        inf = HeaderInformationBlock(action=payload.action, addr_to=ADDRESS_ALL)
        created_message = _mk_wsd_soap_message(inf, payload)

        if self._disco_proxy_active:
            self._networking_thread.add_unicast_message(created_message, self.__disco_proxy_address[0],
                                                        self.__disco_proxy_address[1])
        else:
            self._networking_thread.add_multicast_message(created_message, MULTICAST_IPV4_ADDRESS, self.multicast_port)

    def _send_resolve(self, epr):
        self._logger.debug('sending resolve on %s', epr)
        payload = wsd_types.ResolveType()
        payload.EndpointReference.Address = epr

        inf = HeaderInformationBlock(action=payload.action, addr_to=ADDRESS_ALL)
        created_message = _mk_wsd_soap_message(inf, payload)

        if self._disco_proxy_active:
            self._networking_thread.add_unicast_message(created_message,
                                                        self.__disco_proxy_address[0],
                                                        self.__disco_proxy_address[1])
        else:
            self._networking_thread.add_multicast_message(created_message, MULTICAST_IPV4_ADDRESS, self.multicast_port)

    def _send_hello(self, service):
        self._logger.info('sending hello on %s', service)
        service.increment_message_number()
        app_sequence = wsd_types.AppSequenceType()
        app_sequence.InstanceId = int(service.instance_id)
        app_sequence.MessageNumber = service.message_number

        payload = wsd_types.HelloType()
        payload.Types = service.types
        payload.Scopes = service.scopes
        payload.XAddrs = service.get_x_addrs()
        payload.EndpointReference.Address = service.epr

        inf = HeaderInformationBlock(action=payload.action, addr_to=ADDRESS_ALL)

        created_message = _mk_wsd_soap_message(inf, payload)
        created_message.p_msg.add_header_element(app_sequence.as_etree_node(nsh.WSD.tag('AppSequence'),
                                                                            ns_map=nsh.partial_map(nsh.WSD)))
        self._networking_thread.add_multicast_message(created_message, MULTICAST_IPV4_ADDRESS, self.multicast_port,
                                                      random.randint(0, APP_MAX_DELAY))

    def _send_bye(self, service):
        self._logger.debug('sending bye on %s', service)

        bye = wsd_types.ByeType()
        bye.EndpointReference.Address = service.epr

        inf = HeaderInformationBlock(action=bye.action, addr_to=ADDRESS_ALL)

        app_sequence = wsd_types.AppSequenceType()
        app_sequence.InstanceId = int(service.instance_id)
        app_sequence.MessageNumber = service.message_number

        created_message = _mk_wsd_soap_message(inf, bye)
        created_message.p_msg.add_header_element(app_sequence.as_etree_node(nsh.WSD.tag('AppSequence'),
                                                                            ns_map=nsh.partial_map(nsh.WSD)))
        self._networking_thread.add_multicast_message(created_message, MULTICAST_IPV4_ADDRESS, self.multicast_port)

    def _is_accepted_address(self, address):  # pylint: disable=unused-argument, no-self-use
        """ accept any interface. Overwritten in derived classes."""
        return True

    def _network_address_added(self, address):
        if not self._is_accepted_address(address):
            self._logger.debug('network Address ignored: %s', address)
            return

        self._logger.debug('network Address Add: %s', address)
        try:
            self._networking_thread.add_source_addr(address)
            for service in self._local_services.values():
                self._send_hello(service)
        except:  # pylint: disable=bare-except
            self._logger.warning('error in network Address "%s" Added: %s', address, traceback.format_exc())

    def _network_address_removed(self, addr):
        self._logger.debug('network Address removed %s', addr)
        self._networking_thread.remove_source_addr(addr)

    def _start_threads(self):
        if self._networking_thread is not None:
            return
        if platform.system() != 'Windows':
            self._networking_thread = _NetworkingThreadPosix(self, self._logger, self.multicast_port)
        else:
            self._networking_thread = _NetworkingThreadWindows(self, self._logger, self.multicast_port)
        self._networking_thread.start()

        self._addrs_monitor_thread = _AddressMonitorThread(self)
        self._addrs_monitor_thread.start()

    def _stop_threads(self):
        if self._networking_thread is None:
            return

        self._networking_thread.schedule_stop()
        self._addrs_monitor_thread.schedule_stop()

        self._networking_thread.join()
        self._addrs_monitor_thread.join()

        self._networking_thread = None
        self._addrs_monitor_thread = None


class WSDiscoveryBlacklist(WSDiscoveryBase):
    """ Binds to all IP addresses except the black listed ones. """

    def __init__(self, ignored_adaptor_addresses=None, logger=None, multicast_port=None):
        """
        :param ignored_adaptor_addresses: an optional list of (own) ip addresses that shall not be used for discovery.
                                          IP addresses are handled as regular expressions.
        """
        super().__init__(logger, multicast_port)
        tmp = [] if ignored_adaptor_addresses is None else ignored_adaptor_addresses
        self._ignored_adaptor_addresses = [re.compile(x) for x in tmp]

    def _is_accepted_address(self, address):
        """ check if any of the regular expressions matches the argument"""
        for ign_address in self._ignored_adaptor_addresses:
            if ign_address.match(address) is not None:
                return False
        return True


WSDiscovery = WSDiscoveryBlacklist  # deprecated name, for backward compatibility


class WSDiscoveryWhitelist(WSDiscoveryBase):
    """ Binds to all IP listed IP addresses. """

    def __init__(self, accepted_adapter_addresses, logger=None, multicast_port=None):
        """
        :param accepted_adapter_addresses: an optional list of (own) ip addresses that shall not be used for discovery.
        """
        super().__init__(logger, multicast_port)
        tmp = [] if accepted_adapter_addresses is None else accepted_adapter_addresses
        self.accepted_adapter_addresses = [re.compile(x) for x in tmp]

    def _is_accepted_address(self, address):
        """ check if any of the regular expressions matches the argument"""
        for acc_address in self.accepted_adapter_addresses:
            if acc_address.match(address) is not None:
                return True
        return False


class WSDiscoverySingleAdapter(WSDiscoveryBase):
    """ Bind to a single adapter, identified by name.
    """

    def __init__(self, adapter_name, logger=None, force_adapter_name=False, multicast_port=None):
        """
        :param adapter_name: a string,  e.g. 'local area connection'.
                            parameter is only relevant if host has more than one adapter or forceName is True
                            If host has more than one adapter, the adapter with this friendly name is used, but if it does not exist, a RuntimeError is thrown.
        :param logger: use this logger. If none, 'sdc.discover' is used.
        :param force_adapter_name: if True, only this named adapter will be used.
                                 If False, and only one Adapter exists, the one existing adapter is used. (localhost is ignored in this case).
        """
        super().__init__(logger, multicast_port)
        self._my_ip_address = get_ip_for_adapter(adapter_name)

        if self._my_ip_address is None:
            all_adapters = get_ipv4_ips()
            all_adapter_names = [ip.nice_name for ip in all_adapters]
            if force_adapter_name:
                raise RuntimeError(f'No adapter "{adapter_name}" found. Having {all_adapter_names}')

            # see if there is only one physical adapter. if yes, use it
            adapters_not_localhost = [a for a in all_adapters if not a.ip.startswith('127.')]
            if len(adapters_not_localhost) == 1:
                self._my_ip_address = (adapters_not_localhost[0].ip,)  # a tuple
            else:
                raise RuntimeError(f'No adapter "{adapter_name}" found. Having {all_adapter_names}')

    def _is_accepted_address(self, address):
        """ check if any of the regular expressions matches the argument"""
        return address in self._my_ip_address
