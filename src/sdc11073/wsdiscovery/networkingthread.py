"""wsdiscovery networking input output."""

from __future__ import annotations

import logging
import queue
import random
import selectors
import socket
import struct
import threading
import time
import traceback
from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass, field
from enum import IntEnum
from typing import TYPE_CHECKING

from lxml.etree import XMLSyntaxError

from sdc11073 import commlog
from sdc11073.exceptions import ValidationError
from sdc11073.wsdiscovery.common import MULTICAST_IPV4_ADDRESS, MULTICAST_OUT_TTL, message_reader

if TYPE_CHECKING:
    from logging import Logger

    from sdc11073.pysoap.msgfactory import CreatedMessage
    from sdc11073.wsdiscovery.wsdimpl import WSDiscovery

BUFFER_SIZE = 0xffff
DP_MAX_TIMEOUT = 5000  # 5 seconds


@dataclass
class _UdpRepeatParams:
    """Udp messages are send multiple times with random gaps. These parameters define the limits of the randomness."""

    max_initial_delay_ms: int  # max. delay before sending the fist datagram
    repeat: int  # number of repeated sends
    min_delay_ms: int  # minimum delay for first repetition in ms
    max_delay_ms: int  # maximal delay for first repetition in ms
    upper_delay_ms: int  # max. delay between repetitions in ms
    # (gap is doubled for each further repetition, but not more than this value)


unicast_repeat_params = _UdpRepeatParams(500, 2, 50, 250, 500)
multicast_repeat_params = _UdpRepeatParams(500, 4, 50, 250, 500)

# these time constants control the send-loop
SEND_LOOP_IDLE_SLEEP = 0.1
SEND_LOOP_BUSY_SLEEP = 0.01


class _MessageType(IntEnum):
    MULTICAST = 1
    UNICAST = 2


@dataclass(frozen=True)
class OutgoingMessage:
    """OutgoingMessage instances contain a soap envelope, destination address and multicast / unicast information."""

    created_message: CreatedMessage
    addr: str
    port: int
    msg_type: _MessageType

    def __repr__(self):
        return (f"{self.__class__.__name__}(addr={self.addr}, port={self.port}, "
                f"msg_type={self.msg_type}, created_message={self.created_message.serialize()})")


@dataclass(frozen=True)
class _SocketsCollection:
    multi_in: socket.socket
    multi_out_uni_in: socket.socket
    uni_out_socket: socket.socket
    uni_in: socket.socket | None


class _NetworkingThreadBase(ABC):
    """Has one thread for sending and one for receiving."""

    @dataclass(order=True)
    class _EnqueuedMessage:
        send_time: float
        msg: OutgoingMessage = field(compare=False)
        repeat: int

    def __init__(self,
                 my_ip_address: str,
                 wsd: WSDiscovery,
                 logger: Logger,
                 multicast_port: int):
        self._my_ip_address = my_ip_address
        self._wsd = wsd
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
        self._sockets: list[socket.socket] = []
        # ports from which messages will be ignored, because they origin from sockets from this instance
        self._ports_ignored_for_incoming_messages: list[str] = []
        self._inbound_selector = selectors.DefaultSelector()
        self._outbound_selector = selectors.DefaultSelector()
        self.sockets_collection = self._mk_sockets(my_ip_address)

    @abstractmethod
    def _mk_sockets(self, addr: str):
        ...

    def _register_inbound_socket(self, sock: socket.SocketType):
        self._sockets.append(sock)
        self._inbound_selector.register(sock, selectors.EVENT_READ)

    def _register_outbound_socket(self, sock: socket.SocketType):
        self._sockets.append(sock)
        self._outbound_selector.register(sock, selectors.EVENT_WRITE)

    @staticmethod
    def _make_mreq(addr: str) -> bytes:
        return struct.pack("4s4s", socket.inet_aton(MULTICAST_IPV4_ADDRESS), socket.inet_aton(addr))

    @staticmethod
    def _create_multicast_out_socket(addr: str) -> socket.SocketType:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, MULTICAST_OUT_TTL)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, socket.inet_aton(addr))

        sock.bind((addr, 0))
        return sock

    @staticmethod
    def _create_unicast_out_socket(addr: str) -> socket.SocketType:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # set port explicitly when creating (it would otherwise be set after sending first message via this socket)
        sock.bind((addr, 0))
        return sock

    def add_unicast_message(self,
                            created_message: CreatedMessage,
                            addr: str,
                            port: int):
        msg = OutgoingMessage(created_message, addr, port, _MessageType.UNICAST)
        self._logger.debug('add_unicast_message: adding message Id %s',
                           created_message.p_msg.header_info_block.MessageID)
        self._repeated_enqueue_msg(msg, unicast_repeat_params)

    def add_multicast_message(self,
                              created_message: CreatedMessage,
                              addr: str,
                              port: int):
        msg = OutgoingMessage(created_message, addr, port, _MessageType.MULTICAST)
        self._logger.debug('add_multicast_message: adding message Id %s',
                           created_message.p_msg.header_info_block.MessageID)
        self._repeated_enqueue_msg(msg, multicast_repeat_params)

    def _repeated_enqueue_msg(self,
                              msg: OutgoingMessage,
                              delay_params: _UdpRepeatParams):
        if self._quit_send_event.is_set():
            self._logger.warning('_repeated_enqueue_msg: sending thread not running - message will be dropped - %s',
                                 msg)
            return
        initial_delay_ms = random.randint(0, delay_params.max_initial_delay_ms)
        next_send = time.time() + initial_delay_ms / 1000.0
        delta_t = random.randrange(delay_params.min_delay_ms, delay_params.max_delay_ms) / 1000.0  # millisec -> seconds
        self._send_queue.put(self._EnqueuedMessage(next_send, msg, 1))
        for i in range(delay_params.repeat):
            next_send += delta_t
            self._send_queue.put(self._EnqueuedMessage(next_send, msg, i + 2))
            delta_t = min(delta_t * 2, delay_params.upper_delay_ms)

    def _run_send(self):
        """send-loop."""
        while not self._quit_send_event.is_set() or not self._send_queue.empty():
            if self._send_queue.empty():
                time.sleep(SEND_LOOP_IDLE_SLEEP)  # nothing to do currently
                continue
            if self._send_queue.queue[0].send_time <= time.time():
                enqueued_msg = self._send_queue.get()
                msg_sent = False
                while not msg_sent:
                    for key, _ in self._outbound_selector.select(timeout=0.1):
                        sock: socket.SocketType = key.fileobj
                        if (enqueued_msg.msg.msg_type == _MessageType.UNICAST
                                and sock == self.sockets_collection.uni_out_socket):
                            msg_sent = True
                            self._send_msg(enqueued_msg, sock)

                        if (enqueued_msg.msg.msg_type == _MessageType.MULTICAST
                                and sock == self.sockets_collection.multi_out_uni_in):
                            msg_sent = True
                            self._send_msg(enqueued_msg, sock)
            else:
                time.sleep(SEND_LOOP_BUSY_SLEEP)  # this creates a 10ms raster for sending, but that is good enough

    def _run_recv(self):
        """Run by thread."""
        while not self._quit_recv_event.is_set():
            try:
                self._recv_messages()
            except:  # noqa: E722. use bare except here, this is a catch-all that keeps thread running.
                self._logger.exception('exception during receiving')

    def is_from_my_socket(self, addr: str) -> bool:
        if addr[0] != self._my_ip_address:
            # if the address is different then the incoming message is not from our sockets
            return False

        # True if the message has been sent from a port contained in the blacklisted port
        return any(addr[1] == port for port in self._ports_ignored_for_incoming_messages)

    def _recv_messages(self):
        """For performance reasons this thread only writes to a queue, no parsing etc."""
        for key, _ in self._inbound_selector.select(timeout=0.1):
            sock: socket.SocketType = key.fileobj
            try:
                data, addr = sock.recvfrom(BUFFER_SIZE)
            except OSError as exc:
                self._logger.warning('socket read error %s', exc)
                time.sleep(0.01)
                continue
            if self.is_from_my_socket(addr):
                continue
            self._logger.debug('received data on my socket %s', sock.getsockname())
            self._add_to_recv_queue(addr, data)

    def _add_to_recv_queue(self, addr: str, data: bytes):
        # method is needed for testing
        self._read_queue.put((addr, data))

    def _run_q_read(self):
        """Read from internal queue and process message."""
        while not self._quit_recv_event.is_set():
            try:
                incoming = self._read_queue.get(timeout=0.1)
            except queue.Empty:
                pass
            else:
                addr, data = incoming
                if b"http://schemas.xmlsoap.org/ws/2005/04/discovery" in data:
                    continue  # older version of discovery standard, ignore completely.
                logging.getLogger(commlog.DISCOVERY_IN).debug(data, extra={'ip_address': addr[0]})
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
                        self._wsd.handle_received_message(received_message, addr)
                except Exception:  # noqa: BLE001
                    self._logger.error('_run_q_read: %s', traceback.format_exc())

    def _send_msg(self, q_msg: _EnqueuedMessage, s: socket.socket):
        msg = q_msg.msg
        data = msg.created_message.serialize()
        is_unicast = msg.msg_type == _MessageType.UNICAST
        self._logger.debug(f'send {"unicast" if is_unicast else "multicast"} '  # noqa: G004
                           f'%d bytes (%d) action=%s: to=%s:%r id=%s',
                           len(data),
                           q_msg.repeat,
                           msg.created_message.p_msg.header_info_block.Action,
                           msg.addr, msg.port,
                           msg.created_message.p_msg.header_info_block.MessageID)
        try:
            s.sendto(data, (msg.addr, msg.port))
        except:  # noqa: E722. use bare except here, this is a catch-all that keeps thread running.
            self._logger.exception('exception during sending')
        else:
            # log this if there was no exception during send
            if is_unicast:
                logging.getLogger(commlog.DISCOVERY_OUT).debug(data, extra={'ip_address': msg.addr})
            else:
                logging.getLogger(commlog.MULTICAST_OUT).debug(data)

    def start(self):
        self._logger.debug('%s: starting ', self.__class__.__name__)
        for sock in self._sockets:
            self._ports_ignored_for_incoming_messages.append(sock.getsockname()[1])
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

        Use join() to wait, until thread really has been stopped.
        """
        self._logger.debug('%s: schedule_stop ', self.__class__.__name__)
        self._quit_recv_event.set()
        self._quit_send_event.set()

    def join(self):
        self._logger.debug('%s: join... ', self.__class__.__name__)
        self._recv_thread.join()
        self._send_thread.join()
        self._qread_thread.join()
        self._recv_thread = None
        self._send_thread = None
        self._qread_thread = None
        for sock in self._sockets:
            sock.close()
        self._inbound_selector.close()
        self._outbound_selector.close()
        self._logger.debug('%s: ... join done', self.__class__.__name__)


class NetworkingThreadWindows(_NetworkingThreadBase):
    """Implementation for Windows. Socket creation is OS specific."""

    def _create_multicast_in_socket(self, addr: str, port: int) -> socket.SocketType:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((addr, port))
        sock.setblocking(False)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, self._make_mreq(addr))
        self._logger.info('UDP socket listens on %s:%d', addr, port)
        return sock

    def _mk_sockets(self, addr: str) -> _SocketsCollection:
        multicast_in_sock = self._create_multicast_in_socket(addr, self.multicast_port)
        multicast_out_sock = self._create_multicast_out_socket(addr)

        uni_out_socket = self._create_unicast_out_socket(addr)

        self._register_outbound_socket(multicast_out_sock)
        self._register_outbound_socket(uni_out_socket)
        self._register_inbound_socket(multicast_in_sock)
        return _SocketsCollection(multicast_in_sock,
                                  multicast_out_sock,
                                  uni_out_socket,
                                  None)


class NetworkingThreadPosix(_NetworkingThreadBase):
    """Implementation for posix. Socket creation is OS specific."""

    def _create_multicast_in_socket(self, addr: str, port: int) -> socket.SocketType:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((MULTICAST_IPV4_ADDRESS, port))
        sock.setblocking(False)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, self._make_mreq(addr))
        self._logger.info('UDP socket listens on %s:%d', addr, port)
        return sock

    @staticmethod
    def _create_unicast_in_socket(addr: str, port: int) -> socket.SocketType:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((addr, port))
        sock.setblocking(False)
        return sock

    def _mk_sockets(self, addr: str) -> _SocketsCollection:
        multicast_in_sock = self._create_multicast_in_socket(addr, self.multicast_port)
        multicast_out_sock = self._create_multicast_out_socket(addr)

        # The unicast_in_sock is needed for handling of unicast messages on multicast port
        unicast_in_sock = self._create_unicast_in_socket(addr, self.multicast_port)
        uni_out_socket = self._create_unicast_out_socket(addr)

        self._register_outbound_socket(multicast_out_sock)
        self._register_outbound_socket(uni_out_socket)
        self._register_inbound_socket(unicast_in_sock)
        self._register_inbound_socket(multicast_in_sock)
        return _SocketsCollection(multicast_in_sock,
                                  multicast_out_sock,
                                  uni_out_socket,
                                  unicast_in_sock)
