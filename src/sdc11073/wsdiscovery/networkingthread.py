from __future__ import annotations

import queue
import random
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
from typing import Union

from lxml.etree import XMLSyntaxError

from .common import MULTICAST_IPV4_ADDRESS, MULTICAST_OUT_TTL
from .common import message_reader
from ..commlog import get_communication_logger
from ..exceptions import ValidationError
from ..pysoap.soapenvelope import Soap12Envelope

BUFFER_SIZE = 0xffff
APP_MAX_DELAY = 500  # miliseconds
DP_MAX_TIMEOUT = 5000  # 5 seconds

UNICAST_UDP_REPEAT = 2
UNICAST_UDP_MIN_DELAY = 50
UNICAST_UDP_MAX_DELAY = 250
UNICAST_UDP_UPPER_DELAY = 500

MULTICAST_UDP_REPEAT = 4
MULTICAST_UDP_MIN_DELAY = 50
MULTICAST_UDP_MAX_DELAY = 250
MULTICAST_UDP_UPPER_DELAY = 500

# these time constants control the send-loop
SEND_LOOP_IDLE_SLEEP = 0.1
SEND_LOOP_BUSY_SLEEP = 0.01


class _MessageType(Enum):
    MULTICAST = 1
    UNICAST = 2


@dataclass(frozen=True)
class Message:
    env: Soap12Envelope
    addr: str
    port: int
    msg_type: _MessageType


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
            self._logger.debug('received data on my socket %s', sock.getsockname())
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


class NetworkingThreadWindows(_NetworkingThreadBase):
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


class NetworkingThreadPosix(_NetworkingThreadBase):
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
