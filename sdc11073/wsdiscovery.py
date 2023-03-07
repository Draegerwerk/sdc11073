#!/usr/bin/env python
import logging
import queue
import random
import re
import selectors
import socket
import struct
import threading
import time
import traceback
import urllib
import uuid
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from http.client import HTTPConnection, HTTPSConnection, RemoteDisconnected
from typing import Any
from typing import Iterable

# pylint: disable=no-name-in-module
from lxml.etree import ETCompatXMLParser, QName, Element, SubElement, tostring, fromstring

from .commlog import get_communication_logger
from .netconn import get_ipv4_addresses, get_ip_for_adapter
from .exceptions import ApiUsageError
# pylint: enable=no-name-in-module


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

# values  acc to
#  http://docs.oasis-open.org/ws-dd/discovery/1.1/wsdd-discovery-1.1-spec.html
NS_A = "http://www.w3.org/2005/08/addressing"  # ws-addressing
NS_D = 'http://docs.oasis-open.org/ws-dd/ns/discovery/2009/01'  # ws-discovery
NS_S = "http://www.w3.org/2003/05/soap-envelope"  # "http://www.w3.org/2003/05/soap-envelope"
NS_DPWS = 'http://docs.oasis-open.org/ws-dd/ns/dpws/2009/01'
ACTION_HELLO = NS_D + '/Hello'  # "http://docs.oasis-open.org/ws-dd/ns/discovery/2009/01/Hello"
ACTION_BYE = NS_D + '/Bye'  # "http://docs.oasis-open.org/ws-dd/ns/discovery/2009/01/Bye"
ACTION_PROBE = NS_D + '/Probe'  # "http://docs.oasis-open.org/ws-dd/ns/discovery/2009/01/Probe"
ACTION_PROBE_MATCH = NS_D + '/ProbeMatches'  # "http://docs.oasis-open.org/ws-dd/ns/discovery/2009/01/ProbeMatches"
ACTION_RESOLVE = NS_D + '/Resolve'  # "http://docs.oasis-open.org/ws-dd/ns/discovery/2009/01/Resolve"
ACTION_RESOLVE_MATCH = NS_D + '/ResolveMatches'  # "http://docs.oasis-open.org/ws-dd/ns/discovery/2009/01/ResolveMatches"

ADDRESS_ALL = "urn:docs-oasis-open-org:ws-dd:ns:discovery:2009:01"  # format acc to RFC 2141
WSA_ANONYMOUS = NS_A + '/anonymous'
MATCH_BY_LDAP = NS_D + '/ldap'  # "http://docs.oasis-open.org/ws-dd/ns/discovery/2009/01/ldap"
MATCH_BY_URI = NS_D + '/rfc3986'  # "http://docs.oasis-open.org/ws-dd/ns/discovery/2009/01/rfc3986"
MATCH_BY_UUID = NS_D + '/uuid'  # "http://docs.oasis-open.org/ws-dd/ns/discovery/2009/01/uuid"
MATCH_BY_STRCMP = NS_D + '/strcmp0'  # "http://docs.oasis-open.org/ws-dd/ns/discovery/2009/01/strcmp0"

# these time constants control the send loop
SEND_LOOP_IDLE_SLEEP = 0.1
SEND_LOOP_BUSY_SLEEP = 0.01


class WsaTag(QName):
    def __init__(self, localname):
        super().__init__(NS_A, localname)


class WsdTag(QName):
    def __init__(self, localname):
        super().__init__(NS_D, localname)


class S12Tag(QName):
    def __init__(self, localname):
        super().__init__(NS_S, localname)


_namespaces_map = {'wsd': NS_D, 'wsa': NS_A, 's12': NS_S, 'dpws': NS_DPWS}


def _types_info(types):
    # helper for logging
    return [str(t) for t in types] if types else types


def _get_prefix(nsmap, namespace):
    for prefix, _namespace in nsmap.items():
        if _namespace == namespace:
            return prefix
    return None


def _generate_instance_id():
    return str(random.randint(1, 0xFFFFFFFF))


class URI:

    def __init__(self, uri):
        i_1 = uri.find(":")
        i_2 = uri.find("@")
        self._scheme = uri[:i_1]
        if i_2 != -1:
            self._authority = uri[i_1 + 1: i_2]
            self._path = uri[i_2 + 1:]
        else:
            self._authority = ""
            self._path = uri[i_1 + 1:]

    @property
    def scheme(self):
        return self._scheme

    @property
    def authority(self):
        return self._authority

    @property
    def path(self):
        return self._path

    def get_pathex_query_fragment(self):
        i = self._path.find("?")
        path = self._path
        if i != -1:
            return path[:self._path.find("?")]
        return path


@dataclass(frozen=True)
class Scope:
    value: str
    match_by: [str, None] = None

    @property
    def quoted_value(self):
        return self.value.replace(' ', '%20')

    def __repr__(self):
        if self.match_by is None or len(self.match_by) == 0:
            return self.value
        return self.match_by + ":" + self.value


@dataclass(frozen=True)
class ProbeResolveMatch:
    epr: str
    types: Iterable[QName]
    scopes: Iterable[Scope]
    x_addrs: Iterable[str]
    metadata_version: str

    def __repr__(self):
        return f"ProbeResolveMatch(EPR:{self.epr} Types:{_types_info(self.types)} " \
               f"Scopes:{[str(s) for s in self.scopes]} XAddrs:{self.x_addrs} " \
               f"Metadata Version:{self.metadata_version})"


@dataclass()
class SoapEnvelope:
    message_id: str = ''
    types: list = field(default_factory=list)
    scopes: list = field(default_factory=list)
    x_addrs: list = field(default_factory=list)
    probe_resolve_matches: list = field(default_factory=list)
    action: str = ''
    relates_to: str = ''
    relationship_type: [str, None] = None
    addr_to: str = ''
    addr_reply_to: str = ''
    instance_id: str = ''
    sequence_id: str = ''
    message_number: str = ''
    epr: str = ''
    metadata_version: str = '1'

    def __post_init__(self):
        if not self.message_id:
            self.message_id = uuid.uuid4().urn


class _MessageType(Enum):
    MULTICAST = 1
    UNICAST = 2


@dataclass(frozen=True)
class Message:
    env: SoapEnvelope
    addr: str
    port: int
    msg_type: _MessageType


class Service:
    def __init__(self, types, scopes, x_addrs, epr, instance_id, metadata_version=1):
        self.types = types
        self.scopes = scopes
        self._x_addrs = x_addrs
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
        :param ipaddresses: ip addresses, lists of strings or strings
        """
        my_addresses = []
        for ip_address in ip_addresses:
            if isinstance(ip_address, str):
                my_addresses.append(ip_address)
            else:
                my_addresses.extend(ip_address)
        for addr in self.get_x_addrs():
            parsed = urllib.parse.urlsplit(addr)
            ip_addr = parsed.netloc.split(':')[0]
            if ip_addr in my_addresses:
                return True
        return False

    def __repr__(self):
        scopes_str = ', '.join([str(x) for x in self.scopes])
        types_str = ', '.join([str(x) for x in self.types])
        return f'Service epr={self.epr}, instanceId={self.instance_id} Xaddr={self._x_addrs} ' \
               f'scopes={scopes_str} types={types_str}'

    def __str__(self):
        scopes_str = ', '.join([str(x) for x in self.scopes])
        types_str = ', '.join([str(x) for x in self.types])
        return f'Service epr={self.epr}, instanceId={self.instance_id}\n' \
               f'   Xaddr={self._x_addrs}\n' \
               f'   scopes={scopes_str}\n' \
               f'   types={types_str}'


def match_scope(src, target, match_by):
    """ This implementation correctly handles "%2F" (== '/') encoded values"""
    if match_by == "" or match_by is None or match_by == MATCH_BY_LDAP or match_by == MATCH_BY_URI or match_by == MATCH_BY_UUID:
        src = urllib.parse.urlsplit(src)
        target = urllib.parse.urlsplit(target)
        if src.scheme.lower() != target.scheme.lower():
            return False
        if src.netloc.lower() != target.netloc.lower():
            return False
        if src.path == target.path:
            return True
        src_path_elements = src.path.split('/')
        target_path_elements = target.path.split('/')
        src_path_elements = [urllib.parse.unquote(elem) for elem in src_path_elements]
        target_path_elements = [urllib.parse.unquote(elem) for elem in target_path_elements]
        if len(src_path_elements) > len(target_path_elements):
            return False
        for i, elem in enumerate(src_path_elements):
            if target_path_elements[i] != elem:
                return False
        return True
    if match_by == MATCH_BY_STRCMP:
        return src == target
    return False


def match_type(type1, type2):
    return type1.namespace == type2.namespace and type1.localname == type2.localname


def _create_skel_soap_message(soap_action, message_id, relates_to=None, addr_to=None, reply_to=None):
    doc = Element(S12Tag('Envelope'), nsmap=_namespaces_map)
    header = SubElement(doc, S12Tag('Header'))
    action = SubElement(header, WsaTag('Action'))
    action.text = soap_action
    body = SubElement(doc, S12Tag('Body'))

    _mk_subelement_with_text(header, WsaTag('MessageID'), message_id)
    if relates_to:
        _mk_subelement_with_text(header, WsaTag('RelatesTo'), relates_to)
    if addr_to:
        _mk_subelement_with_text(header, WsaTag('To'), addr_to)
    if reply_to:
        _mk_subelement_with_text(header, WsaTag('ReplyTo'), reply_to)

    return doc, header, body


def _mk_subelement_with_text(parent_node, qname, text, attrib=None, nsmap=None):
    elem = SubElement(parent_node, qname, attrib=attrib or {}, nsmap=nsmap or {})
    elem.text = text
    return elem


_ASCII_LETTERS = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'


def _gt_random_str():
    return "".join([random.choice(_ASCII_LETTERS) for x in range(10)])


def _create_type_nodes(parent_node, types):
    if types is not None and len(types) > 0:
        ns_map = {}
        type_list = []

        for i, _type in enumerate(types):
            namespace, localname = _type.namespace, _type.localname
            prefix = _get_prefix(parent_node, namespace)
            if prefix is None:
                prefix = _gt_random_str()
                ns_map[prefix] = namespace
            if i == 0:
                # make namespace of first type the default namespace (so that we can test handling of this case)
                ns_map[None] = namespace
                type_list.append(localname)
            else:
                type_list.append(prefix + ":" + localname)

        types_string = " ".join(type_list)
        _mk_subelement_with_text(parent_node, WsdTag('Types'), types_string, nsmap=ns_map)


def _create_scope_node(parent_node, scopes):
    if scopes is not None and len(scopes) > 0:
        scopes_string = " ".join([x.quoted_value for x in scopes])
        _mk_subelement_with_text(parent_node, WsdTag('Scopes'), scopes_string)


def _create_xaddr_node(parent_node, x_addrs):
    if x_addrs is not len(x_addrs) > 0:
        addr_string = " ".join(x_addrs)
        _mk_subelement_with_text(parent_node, WsdTag('XAddrs'), addr_string)


def _create_epr_node(parent_node, epr):
    epr_element = SubElement(parent_node, WsaTag("EndpointReference"))
    _mk_subelement_with_text(epr_element, WsaTag('Address'), epr)


def _parse_types(parent_node):
    types = []
    types_node = parent_node.find('wsd:Types', _namespaces_map)
    if types_node is not None:
        _types = [] if not types_node.text else types_node.text.split()
        for _type in _types:
            elements = _type.split(':')
            prefix = None if len(elements) == 1 else elements[0]  # None means default namespace
            local_name = elements[-1]
            types.append(QName(types_node.nsmap[prefix], local_name))
    return types


def _parse_scopes(parent_node):
    scopes_node = parent_node.find('wsd:Scopes', _namespaces_map)
    if scopes_node is not None:
        match_by = scopes_node.attrib.get("MatchBy")
        scopes = [] if not scopes_node.text else scopes_node.text.split()
        return [Scope(item, match_by) for item in scopes]
    return []


def _parse_xaddrs(parent_node):
    x_addr_node = parent_node.find('wsd:XAddrs', _namespaces_map)
    if x_addr_node is not None:
        return [] if not x_addr_node.text else x_addr_node.text.split()
    return []


def _parse_epr(parent_node):
    """

    :param parent_node: direct parent of wsa:EndpointReference node
    :return: epr address
    """
    epr_node = parent_node.find('wsa:EndpointReference', _namespaces_map)
    if epr_node is not None:
        address_node = epr_node.find('wsa:Address', _namespaces_map)
        return address_node.text
    return ''


def _parse_metadata_version(parent_node):
    mdv_node = parent_node.find('wsd:MetadataVersion', _namespaces_map)
    if mdv_node is not None:
        return mdv_node.text
    return ''


def _parse_app_sequence(header_node, env):
    app_seq_node = header_node.find('wsd:AppSequence', _namespaces_map)
    if app_seq_node is not None:
        env.instance_id = app_seq_node.attrib.get("InstanceId")
        env.sequence_id = app_seq_node.attrib.get("SequenceId")
        env.message_number = app_seq_node.attrib.get("MessageNumber")


def _parse_relates_to(header_node, env):
    relates_to = header_node.find('wsa:RelatesTo', _namespaces_map)
    if relates_to is not None:
        env.relates_to = relates_to.text
        rel_type = relates_to.attrib.get('RelationshipType')
        if rel_type:
            env.relationship_type = rel_type


def _parse_reply_to(header_node, env):
    reply_to = header_node.find('wsa:ReplyTo', _namespaces_map)
    if reply_to is not None:
        env.addr_reply_to = reply_to.text


def _parse_envelope(data, ip_addr, logger):
    parser = ETCompatXMLParser(resolve_entities=False)
    try:
        dom = fromstring(data, parser=parser)
    except Exception as ex:
        logger.error('load error "%s" in "%s"', ex, data)
        return None

    header = dom.find('s12:Header', _namespaces_map)
    body = dom.find('s12:Body', _namespaces_map)
    if header is None or body is None:
        logger.error('received message from {} is not a soap message: {}', ip_addr, data)
        return None

    msg_node = body[0]

    msg_id = header.find('wsa:MessageID', _namespaces_map)
    msg_id = None if msg_id is None else msg_id.text
    soap_action = header.find('wsa:Action', _namespaces_map)
    if soap_action is None:
        # this is something else, ignore it
        return None

    soap_action = soap_action.text

    env = SoapEnvelope(msg_id)
    env.action = soap_action

    addr_to = header.find('wsa:To', _namespaces_map)
    if addr_to is not None:
        env.addr_to = addr_to.text

    # parse action specific data
    try:
        if soap_action == ACTION_PROBE:
            _parse_reply_to(header, env)
            env.types.extend(_parse_types(msg_node))
            env.scopes.extend(_parse_scopes(msg_node))
            return env
        if soap_action == ACTION_PROBE_MATCH:
            _parse_relates_to(header, env)
            _parse_app_sequence(header, env)
            pm_nodes = msg_node.findall('wsd:ProbeMatch', _namespaces_map)
            for node in pm_nodes:
                epr = _parse_epr(node)
                types = _parse_types(node)
                scopes = _parse_scopes(node)
                x_addrs = _parse_xaddrs(node)
                mdv = _parse_metadata_version(node)
                env.probe_resolve_matches.append(ProbeResolveMatch(epr, types, scopes, x_addrs, mdv))
            return env
        if soap_action == ACTION_RESOLVE:
            _parse_reply_to(header, env)
            env.epr = _parse_epr(msg_node)
            return env
        if soap_action == ACTION_RESOLVE_MATCH:
            _parse_relates_to(header, env)
            _parse_app_sequence(header, env)
            resolve_match_node = msg_node.find('wsd:ResolveMatch', _namespaces_map)
            if resolve_match_node is not None:
                epr = _parse_epr(resolve_match_node)
                types = _parse_types(resolve_match_node)
                scopes = _parse_scopes(resolve_match_node)
                x_addrs = _parse_xaddrs(resolve_match_node)
                mdv = _parse_metadata_version(resolve_match_node)
                env.probe_resolve_matches.append(ProbeResolveMatch(epr, types, scopes, x_addrs, mdv))
            return env
        if soap_action == ACTION_BYE:
            _parse_app_sequence(header, env)
            env.epr = _parse_epr(msg_node)
            return env
        if soap_action == ACTION_HELLO:
            _parse_app_sequence(header, env)
            env.epr = _parse_epr(msg_node)
            env.types.extend(_parse_types(msg_node))
            env.scopes.extend(_parse_scopes(msg_node))
            env.x_addrs = _parse_xaddrs(msg_node)
            env.metadata_version = _parse_metadata_version(msg_node)
            return env
    except:  # pylint: disable=bare-except
        logger.error('Parse Error %s:', traceback.format_exc())
        logger.error('parsed data is from %r, data: %r:', ip_addr, data)
    return None


def _create_message(env):
    if env.action == ACTION_PROBE:
        return _create_probe_message(env)
    if env.action == ACTION_PROBE_MATCH:
        return _create_probe_match_message(env)
    if env.action == ACTION_RESOLVE:
        return _create_resolve_message(env)
    if env.action == ACTION_RESOLVE_MATCH:
        return _create_resolve_match_message(env)
    if env.action == ACTION_HELLO:
        return _create_hello_message(env)
    if env.action == ACTION_BYE:
        return _create_bye_message(env)
    raise ValueError(f'do not know how to handle action {env.action}')


def _create_probe_message(env):
    doc, _, body = _create_skel_soap_message(ACTION_PROBE, env.message_id, addr_to=env.addr_to,
                                             reply_to=env.addr_reply_to)
    probe_element = SubElement(body, WsdTag('Probe'))
    _create_type_nodes(probe_element, env.types)
    _create_scope_node(probe_element, env.scopes)
    return tostring(doc)


def _create_probe_match_message(env):
    doc, header, body = _create_skel_soap_message(ACTION_PROBE_MATCH, env.message_id,
                                                  relates_to=env.relates_to, addr_to=env.addr_to,
                                                  reply_to=env.addr_reply_to)
    SubElement(header, WsdTag('AppSequence'),
               attrib={"InstanceId": env.instance_id,
                       "MessageNumber": env.message_number})

    probe_matches_element = SubElement(body, WsdTag('ProbeMatches'))

    probe_matches = env.probe_resolve_matches
    for probe_match in probe_matches:
        probe_match_element = SubElement(probe_matches_element, WsdTag('ProbeMatch'))
        _create_epr_node(probe_match_element, probe_match.epr)
        _create_type_nodes(probe_match_element, probe_match.types)
        _create_scope_node(probe_match_element, probe_match.scopes)
        _create_xaddr_node(probe_match_element, probe_match.x_addrs)
        _mk_subelement_with_text(probe_match_element, WsdTag('MetadataVersion'), probe_match.metadata_version)
    return tostring(doc)


def _create_resolve_message(env):
    doc, _, body = _create_skel_soap_message(ACTION_RESOLVE, env.message_id,
                                             addr_to=env.addr_to, reply_to=env.addr_reply_to)
    resolve_element = SubElement(body, WsdTag('Resolve'))
    _create_epr_node(resolve_element, env.epr)
    return tostring(doc)


def _create_resolve_match_message(env):
    doc, header, body = _create_skel_soap_message(ACTION_RESOLVE_MATCH, env.message_id,
                                                  relates_to=env.relates_to, addr_to=env.addr_to)
    header.append(Element(WsdTag('AppSequence'),
                          attrib={"InstanceId": env.instance_id,
                                  "MessageNumber": env.message_number}))
    resolve_matches_element = SubElement(body, WsdTag('ResolveMatches'))
    if len(env.probe_resolve_matches) > 0:
        resolve_match = env.probe_resolve_matches[0]
        resolve_match_element = SubElement(resolve_matches_element, WsdTag('ResolveMatch'))
        _create_epr_node(resolve_match_element, resolve_match.epr)
        _create_type_nodes(resolve_match_element, resolve_match.types)
        _create_scope_node(resolve_match_element, resolve_match.scopes)
        _create_xaddr_node(resolve_match_element, resolve_match.x_addrs)
        _mk_subelement_with_text(resolve_match_element, WsdTag('MetadataVersion'), resolve_match.metadata_version)
    return tostring(doc)


def _create_hello_message(env):
    doc, header, body = _create_skel_soap_message(ACTION_HELLO, env.message_id)
    if len(env.relates_to) > 0:
        _mk_subelement_with_text(header, WsaTag('RelatesTo'), env.relates_to,
                                 attrib={"RelationshipType": "d:Suppression"})
    _mk_subelement_with_text(header, WsaTag('To'), env.addr_to)
    header.append(Element(WsdTag('AppSequence'),
                          attrib={"InstanceId": env.instance_id,
                                  "MessageNumber": env.message_number}))
    hello_element = SubElement(body, WsdTag('Hello'))
    _create_epr_node(hello_element, env.epr)
    _create_type_nodes(hello_element, env.types)
    _create_scope_node(hello_element, env.scopes)
    _create_xaddr_node(hello_element, env.x_addrs)
    _mk_subelement_with_text(hello_element, WsdTag('MetadataVersion'), env.metadata_version)
    return tostring(doc)


def _create_bye_message(env):
    doc, header, body = _create_skel_soap_message(ACTION_BYE, env.message_id,
                                                  addr_to=env.addr_to)
    SubElement(header, WsdTag('AppSequence'),
               attrib={"InstanceId": env.instance_id,
                       "MessageNumber": env.message_number})

    bye_element = SubElement(body, WsdTag('Bye'))
    _create_epr_node(bye_element, env.epr)
    return tostring(doc)


def _extract_soap_udp_address_from_uri(uri):
    val = uri.get_pathex_query_fragment().split(":")
    part1 = val[0][2:]
    part2 = None
    if val[1].count('/') > 0:
        part2 = int(val[1][:val[1].index('/')])
    else:
        part2 = int(val[1])
    return [part1, part2]


class _StopableDaemonThread(threading.Thread):
    """Stopable daemon thread.

    run() method shall exit, when self._quit_event.wait() returned True
    """

    def __init__(self, name):
        self._quit_event = threading.Event()
        super().__init__(name=name)
        self.daemon = True

    def schedule_stop(self):
        """Schedule stopping the thread.
        Use join() to wait, until thread really has been stopped
        """
        self._quit_event.set()


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
class _SocketPair:
    multi_in: socket.socket
    multi_out_uni_in: socket.socket


class _NetworkingThread:
    """ Has one thread for sending and one for receiving"""

    @dataclass(order=True)
    class _EnqueuedMessage:
        send_time: float
        msg: Any = field(compare=False)

    def __init__(self, observer, logger):
        self._recv_thread = None
        self._qread_thread = None
        self._send_thread = None
        self._quit_recv_event = threading.Event()
        self._quit_send_event = threading.Event()
        self._send_queue = queue.PriorityQueue(10000)
        self._read_queue = queue.Queue(10000)
        self._known_message_ids = deque(maxlen=50)
        self._observer = observer
        self._logger = logger

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

    def _create_multicast_in_socket(self, ip_address):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((ip_address, MULTICAST_PORT))
        sock.setblocking(False)
        self._logger.info('UDP socket listens on %s:%d', ip_address, MULTICAST_PORT)
        return sock

    def add_source_addr(self, addr):
        """None means 'system default'"""
        multicast_in_sock = self._create_multicast_in_socket(addr)
        try:
            multicast_in_sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, self._make_mreq(addr))
        except socket.error:  # if 1 interface has more than 1 address, exception is raised for the second
            print(traceback.format_exc())
            pass
        multicast_out_sock = self._create_multicast_out_socket(addr)
        with self._sockets_by_address_lock:
            self._register(multicast_out_sock)
            self._register(multicast_in_sock)
            self._sockets_by_address[addr] = _SocketPair(multicast_in_sock, multicast_out_sock)

    def remove_source_addr(self, addr):
        sock_pair = self._sockets_by_address.get(addr)
        if sock_pair:
            with self._sockets_by_address_lock:
                for sock in (sock_pair.multi_in, sock_pair.multi_out_uni_in):
                    self._unregister(sock)
                    sock.close()
                del self._sockets_by_address[addr]

    def add_unicast_message(self, env, addr, port, initial_delay=0):
        msg = Message(env, addr, port, _MessageType.UNICAST)
        self._logger.debug(
            'add_unicast_message: adding message Id %s. delay=%.2f', env.message_id, initial_delay)
        self._repeated_enqueue_msg(msg, initial_delay, UNICAST_UDP_REPEAT, UNICAST_UDP_MIN_DELAY,
                                   UNICAST_UDP_MAX_DELAY, UNICAST_UDP_UPPER_DELAY)

    def add_multicast_message(self, env, addr, port, initial_delay=0):
        msg = Message(env, addr, port, _MessageType.MULTICAST)
        self._logger.debug(
            'add_multicast_message: adding message Id %s. delay=%.2f', env.message_id, initial_delay)
        self._repeated_enqueue_msg(msg, initial_delay, MULTICAST_UDP_REPEAT, MULTICAST_UDP_MIN_DELAY,
                                   MULTICAST_UDP_MAX_DELAY, MULTICAST_UDP_UPPER_DELAY)

    # def _repeated_enqueue_msg(self, msg, initial_delay_ms, repeat, min_delay_ms, max_delay_ms, upper_delay_ms):
    #     next_send = time.time() + initial_delay_ms / 1000.0
    #     delta_t = random.randrange(min_delay_ms, max_delay_ms) / 1000.0  # millisec -> seconds
    #     self._send_queue.put(self._EnqueuedMessage(next_send, msg))
    #     for _ in range(repeat):
    #         next_send += delta_t
    #         self._send_queue.put(self._EnqueuedMessage(next_send, msg))
    #         delta_t = min(delta_t * 2, upper_delay_ms)
    def _repeated_enqueue_msg(self, msg, initial_delay_ms, repeat, min_delay_ms, max_delay_ms, upper_delay_ms):
        if not self._quit_send_event.is_set():
            next_send = time.time() + initial_delay_ms/1000.0
            dt = random.randrange(min_delay_ms, max_delay_ms) /1000.0 # millisec -> seconds
            self._send_queue.put(self._EnqueuedMessage(next_send, msg))
            for _ in range(repeat):
                next_send += dt
                self._send_queue.put(self._EnqueuedMessage(next_send, msg))
                dt = min(dt*2, upper_delay_ms)

    def _run_send(self):
        """send-loop"""
        while not self._quit_send_event.is_set() or not self._send_queue.empty():
            if self._send_queue.empty():
                time.sleep(SEND_LOOP_IDLE_SLEEP)  # nothing to do currently
            else:
                if self._send_queue.queue[0].send_time <= time.time():
                    enqueued_msg = self._send_queue.get()
                    self._send_msg(enqueued_msg.msg)
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
            #self._read_queue.put((addr, data))

    def _add_to_recv_queue(self, addr, data):
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

                env = _parse_envelope(data, addr[0], self._logger)
                if env is None:  # fault or failed to parse
                    continue

                mid = env.message_id
                if mid in self._known_message_ids:
                    self._logger.debug('message Id %s already known. This is a duplicate receive, ignoring.', mid)
                    continue
                self._known_message_ids.appendleft(mid)
                self._observer.env_received(env, addr)

    def _send_msg(self, msg):
        action = msg.env.action.split('/')[-1]  # only last part
        if action in ('ResolveMatches', 'ProbeMatches'):
            self._logger.debug('_send_msg: sending %s %s to %s ProbeResolveMatches=%r, epr=%s, msgNo=%r',
                               action,
                               msg.msg_type,
                               msg.addr,
                               msg.env.probe_resolve_matches,
                               msg.env.epr,
                               msg.env.message_number
                               )
        elif action == 'Probe':
            self._logger.debug('_send_msg: sending %s %s to %s types=%s scopes=%r',
                               action,
                               msg.msg_type,
                               msg.addr,
                               _types_info(msg.env.types),
                               msg.env.scopes,
                               )
        else:
            self._logger.debug('_send_msg: sending %s %s to %s xaddr=%r, epr=%s, msgNo=%r',
                               action,
                               msg.msg_type,
                               msg.addr,
                               msg.env.x_addrs,
                               msg.env.epr,
                               msg.env.message_number
                               )

        data = _create_message(msg.env)

        if msg.msg_type == _MessageType.UNICAST:
            get_communication_logger().log_discovery_msg_out(msg.addr, data)
            self._uni_out_socket.sendto(data, (msg.addr, msg.port))
        else:
            get_communication_logger().log_multicast_msg_out(data)
            with self._sockets_by_address_lock:
                for sock_pair  in self._sockets_by_address.values():
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
        self._qread_thread.join(1)
        self._send_thread.join(10)
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


def _is_type_in_list(ttype, types):
    for entry in types:
        if match_type(ttype, entry):
            return True
    return False


def _is_scope_in_list(scope, scopes):
    for entry in scopes:
        if match_scope(scope.value, entry.value, scope.match_by):
            return True
    return False


def _matches_filter(service, types, scopes, logger=None):
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
        for scope in scopes:
            if not _is_scope_in_list(scope, srv_sc):
                if logger:
                    logger.debug(f'scope not matching: {scope} is not in scopes list {srv_sc}')
                return False
        if logger:
            logger.debug('matching scopes')
    return True


def _filter_services(services, types, scopes, logger=None):
    return [service for service in services if _matches_filter(service, types, scopes, logger)]


class WSDiscoveryWithHTTPProxy:
    """
    This uses an http proxy for discovery
    """

    def __init__(self, proxy_url, logger=None, ssl_context=None):
        self.__disco_proxy_address = urllib.parse.urlsplit(proxy_url)
        self._logger = logger or logging.getLogger('sdc.discover')
        self._ssl_context = ssl_context
        self._local_services = {}
        self.resolve_services = False

    def start(self):
        """start the discovery server - should be called before using other functions"""

    def stop(self):
        """cleans up and stops the discovery server"""

    def search_services(self, types=None, scopes=None, timeout=5):
        """search for services given the TYPES and SCOPES in a given timeout
        """
        remote_services = self._send_probe(types, scopes, timeout)
        filtered_services = _filter_services(remote_services.values(), types, scopes, self._logger)
        if not self.resolve_services:
            return filtered_services

        resolved_services = []
        for service in filtered_services:
            resolved_services.append(self._send_resolve(service.epr))
        return resolved_services

    searchServices = search_services  # backwards compatibility

    def search_multiple_types(self, types_list, scopes=None, timeout=5):
        # repeat_probe_interval is not needed, but kept in order to have identical signature
        result = {}  # avoid double entries by adding to dictionary with epr as key
        for _type in types_list:
            services = self.search_services(_type, scopes, timeout)
            for service in services:
                result[service.epr] = service
        return result.values()

    searchMultipleTypes = search_multiple_types  # backwards compatibility

    def get_active_addresses(self):
        sock = socket.socket()
        try:
            sock.connect((self.__disco_proxy_address.hostname, self.__disco_proxy_address.port))
            return [sock.getsockname()[0]]
        finally:
            sock.close()

    def clear_remote_services(self):
        # do nothing, this implementation has no internal list
        pass

    def clear_local_services(self):
        # do nothing, this implementation has no internal list
        pass

    def publish_service(self, epr, types, scopes, x_addrs):
        """Publish a service with the given TYPES, SCOPES and XAddrs (service addresses)

        if x_addrs contains item, which includes {ip} pattern, one item per IP addres will be sent
        """
        instance_id = _generate_instance_id()
        metadata_version = self._local_services[epr].metadata_version + 1 if epr in self._local_services else 1
        service = Service(types, scopes, x_addrs, epr, instance_id, metadata_version)
        self._logger.info('publishing %r', service)
        self._local_services[epr] = service
        self._send_hello(service)

    publishService = publish_service  # backwards compatibility

    def clear_service(self, epr):
        service = self._local_services[epr]
        self._send_bye(service)
        del self._local_services[epr]

    clearService = clear_service  # backwards compatibility

    def _post_http(self, data, timeout=5):
        if self.__disco_proxy_address.scheme == 'https':
            conn = HTTPSConnection(self.__disco_proxy_address.netloc, timeout=timeout, context=self._ssl_context)
        else:
            conn = HTTPConnection(self.__disco_proxy_address.netloc, timeout=timeout)
        conn.request('POST', self.__disco_proxy_address.path, data)
        resp = conn.getresponse()
        resp_data = resp.read()
        conn.close()
        return resp_data

    def _send_probe(self, types, scopes, timeout):
        self._logger.debug('sending probe types=%r scopes=%r', _types_info(types), scopes)
        env = SoapEnvelope()
        env.action = ACTION_PROBE
        env.addr_to = ADDRESS_ALL
        env.types = types
        env.scopes = scopes
        data = _create_probe_message(env)
        resp_data = self._post_http(data, timeout)
        get_communication_logger().log_discovery_msg_in(self.__disco_proxy_address.netloc, resp_data)
        resp_env = _parse_envelope(resp_data, self.__disco_proxy_address.netloc, self._logger)
        services = {}
        for match in resp_env.probe_resolve_matches:
            services[match.epr] = (Service(match.types, match.scopes, match.get_x_addrs(), match.epr,
                                           resp_env.instance_id, metadata_version=int(match.metadata_version)))
        return services

    def _send_resolve(self, epr):
        self._logger.debug('sending resolve on %s', epr)
        env = SoapEnvelope()
        env.action = ACTION_RESOLVE
        env.addr_to = ADDRESS_ALL
        env.epr = epr
        data = _create_resolve_message(env)
        resp_data = self._post_http(data)
        get_communication_logger().log_discovery_msg_in(self.__disco_proxy_address.netloc, resp_data)
        resp_env = _parse_envelope(resp_data, self.__disco_proxy_address.netloc, self._logger)
        services = {}
        for match in resp_env.probe_resolve_matches:
            services[match.epr] = (Service(match.types, match.scopes, match.get_x_addrs(), match.epr,
                                           resp_env.instance_id, metadata_version=int(match.metadata_version)))
        return services

    def _send_hello(self, service):
        self._logger.info('sending hello on %r', service)
        service.increment_message_number()
        env = SoapEnvelope()
        env.action = ACTION_HELLO
        env.addr_to = ADDRESS_ALL
        env.instance_id = str(service.instance_id)
        env.message_number = str(service.message_number)
        env.types = service.types
        env.scopes = service.scopes
        env.x_addrs = service.get_x_addrs()
        env.metadata_version = service.metadata_version
        env.epr = service.epr
        data = _create_hello_message(env)
        try:
            self._post_http(data)
        except RemoteDisconnected:
            pass

    def _send_bye(self, service):
        self._logger.debug('sending bye on %r', service)
        env = SoapEnvelope()
        env.action = ACTION_BYE
        env.addr_to = ADDRESS_ALL
        env.instance_id = str(service.instance_id)
        env.message_number = str(service.message_number)
        env.epr = service.epr
        service.increment_message_number()
        data = _create_bye_message(env)
        try:
            self._post_http(data)
        except RemoteDisconnected:
            pass


class WsDiscoveryProxyAndUdp:
    """Use proxy and local discovery at the same time.
    A device is published and cleared over both mechanisms.
    The search methods allow to select where to search."""

    def __init__(self, wsd_proxy_instance, wsd_over_udp_instance):
        self._wsd_proxy = wsd_proxy_instance
        self._wsd_udp = wsd_over_udp_instance

    @classmethod
    def with_single_adapter(cls, proxy_url, adapter_name, logger=None, force_adapter_name=False, ssl_context=None):
        """Alternative constructor that instantiates WSDiscoveryWithHTTPProxy and WSDiscoverySingleAdapter"""
        proxy = WSDiscoveryWithHTTPProxy(proxy_url, logger, ssl_context)
        direct = WSDiscoverySingleAdapter(adapter_name, logger, force_adapter_name)
        return cls(proxy, direct)

    @classmethod
    def with_whitelist_adapter(cls, proxy_url, accepted_adapter_addresses, logger=None, ssl_context=None):
        """Alternative constructor that instantiates WSDiscoveryWithHTTPProxy and WSDiscoveryWhitelist"""
        proxy = WSDiscoveryWithHTTPProxy(proxy_url, logger, ssl_context)
        direct = WSDiscoveryWhitelist(accepted_adapter_addresses, logger)
        return cls(proxy, direct)

    @classmethod
    def with_blacklist_adapter(cls, proxy_url, ignored_adaptor_addresses, logger=None, ssl_context=None):
        """Alternative constructor that instantiates WSDiscoveryWithHTTPProxy and WSDiscoveryBlacklist"""
        proxy = WSDiscoveryWithHTTPProxy(proxy_url, logger, ssl_context)
        direct = WSDiscoveryBlacklist(ignored_adaptor_addresses, logger)
        return cls(proxy, direct)

    def start(self):
        self._wsd_proxy.start()
        self._wsd_udp.start()

    def stop(self):
        self._wsd_proxy.stop()
        self._wsd_udp.stop()

    def search_services(self, types=None, scopes=None, timeout=5,
                        searchproxy=True, searchdirekt=False):
        results = {}
        if searchproxy:
            services = self._wsd_proxy.search_services(types, scopes, timeout)
            for service in services:
                results[service.epr] = service
        if searchdirekt:
            services = self._wsd_udp.search_services(types, scopes, timeout)
            for service in services:
                results[service.epr] = service
        return results.values()

    def search_multiple_types(self, types_list, scopes=None, timeout=5, repeat_probe_interval=3,
                              search_proxy=True, search_direct=False):
        results = {}
        if search_proxy:
            services = self._wsd_proxy.search_multiple_types(types_list, scopes, timeout)
            for service in services:
                results[service.epr] = service
        if search_direct:
            services = self._wsd_udp.search_multiple_types(types_list, scopes, timeout, repeat_probe_interval)
            for service in services:
                results[service.epr] = service
        return results.values()

    def get_active_addresses(self):
        addresses = set(self._wsd_proxy.get_active_addresses())
        addresses.update(self._wsd_udp.get_active_addresses())
        return list(addresses)

    def clear_remote_services(self):
        self._wsd_proxy.clear_remote_services()
        self._wsd_udp.clear_remote_services()

    def clear_local_services(self):
        self._wsd_proxy.clear_local_services()
        self._wsd_udp.clear_local_services()

    def publish_service(self, epr, types, scopes, x_addrs):
        self._wsd_proxy.publish_service(epr, types, scopes, x_addrs)
        self._wsd_udp.publish_service(epr, types, scopes, x_addrs)

    def clear_service(self, epr):
        self._wsd_proxy.clear_service(epr)
        self._wsd_udp.clear_service(epr)


class WSDiscoveryBase:
    # UDP based discovery.
    # these flags control which data is included in ProbeResponse messages.
    PROBEMATCH_EPR = True
    PROBEMATCH_TYPES = True
    PROBEMATCH_SCOPES = True
    PROBEMATCH_XADDRS = True

    def __init__(self, logger=None):
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
        random.seed(int(time.time() * 1000000))

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

    # def set_remote_service_disappeared_callback(self, cb):
    #     """Set callback, which will be called when new service disappears
    #     Service uuid is passed as a parameter to the callback
    #     Set None to disable callback
    #     """
    #     self._remoteServiceDisppearedCallback = cb

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
                self._logger.debug('_add_remote_service: remote Service %s:\n    MetadataVersion: %d',
                                   service.epr, service.metadata_version)
                # use the longest elements for merged service
                merged = []
                if len(service.get_x_addrs()) > len(already_known_service.get_x_addrs()):
                    already_known_service.set_x_addrs(service.get_x_addrs())
                    merged.append(f'XAddr={service.get_x_addrs()}')
                if len(service.scopes) > len(already_known_service.scopes):
                    already_known_service.scopes = service.scopes
                    merged.append(f'Scopes={service.scopes}')
                if len(service.types) > len(already_known_service.types):
                    already_known_service.types = service.types
                    merged.append(f'Types={service.types}')
                if merged:
                    tmp = '\n      '.join(merged)
                    self._logger.info(f'merge from remote Service {service.epr}:\n      {tmp}')
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

    def _handle_env(self, env, addr):
        act = env.action
        self._logger.debug('_handle_env: received %s from %s', act.split('/')[-1], addr)
        if act == ACTION_PROBE_MATCH:
            for match in env.probe_resolve_matches:
                service = Service(match.types, match.scopes, match.x_addrs, match.epr,
                                  env.instance_id, metadata_version=int(match.metadata_version))
                self._add_remote_service(service)
                if match.x_addrs is None or len(match.x_addrs) == 0:
                    self._logger.info('%s(%s) has no Xaddr, sending resolve message', match.epr, addr)
                    self._send_resolve(match.epr)
                elif not match.types:
                    self._logger.info('%s(%s) has no Types, sending resolve message', match.epr, addr)
                    self._send_resolve(match.epr)
                elif not match.scopes:
                    self._logger.info('%s(%s) has no Scopes, sending resolve message', match.epr, addr)
                    self._send_resolve(match.epr)

        elif act == ACTION_RESOLVE_MATCH:
            for match in env.probe_resolve_matches:
                service = Service(match.types, match.scopes, match.x_addrs, match.epr,
                                  env.instance_id, metadata_version=int(match.metadata_version))
                self._add_remote_service(service)
                if self._remote_service_resolve_match_callback is not None:
                    self._remote_service_resolve_match_callback(service)

        elif act == ACTION_PROBE:
            services = _filter_services(self._local_services.values(), env.types, env.scopes)
            if services:
                self._send_probe_match(services, env.message_id, addr)
            if self._on_probe_callback is not None:
                self._on_probe_callback(addr, env)

        elif act == ACTION_RESOLVE:
            if env.epr in self._local_services:
                service = self._local_services[env.epr]
                self._send_resolve_match(service, env.message_id, addr)

        elif act == ACTION_HELLO:
            # check if it is from a discovery proxy
            rel_type = env.relationship_type
            if rel_type is not None and rel_type.localname == "Suppression" and rel_type.namespace == NS_D:
                x_addr = env.x_addrs[0]
                # only support 'soap.udp'
                if x_addr.startswith("soap.udp:"):
                    self._disco_proxy_active = True
                    self.__disco_proxy_address = _extract_soap_udp_address_from_uri(URI(x_addr))
                    self._disco_proxy_epr = env.epr

            service = Service(env.types, env.scopes, env.x_addrs, env.epr, env.instance_id,
                              metadata_version=int(env.metadata_version))
            self._add_remote_service(service)
            if not env.x_addrs:  # B.D.
                self._logger.debug('%s(%s) has no Xaddr, sending resolve message', env.epr, addr)
                self._send_resolve(env.epr)
            if self._remote_service_hello_callback is not None:
                if _matches_filter(service,
                                   self._remote_service_hello_callback_types_filter,
                                   self._remote_service_hello_callback_scopes_filter):
                    self._remote_service_hello_callback(addr, service)

        elif act == ACTION_BYE:
            # if the bye is from discovery proxy... revert back to multicasting
            if self._disco_proxy_active and self._disco_proxy_epr == env.epr:
                self._disco_proxy_active = False
                self.__disco_proxy_address = None
                self._disco_proxy_epr = None

            self._remove_remote_service(env.epr)
            if self._remote_service_bye_callback is not None:
                self._remote_service_bye_callback(addr, env.epr)
        else:
            self._logger.info('unknown action %s', act)

    def env_received(self, env, addr):
        self._handle_env(env, addr)

    def _send_resolve_match(self, service, relates_to, addr):
        self._logger.info('sending resolve match to %s', addr)
        service.increment_message_number()

        env = SoapEnvelope()
        env.action = ACTION_RESOLVE_MATCH
        env.addr_to = WSA_ANONYMOUS
        env.instance_id = str(service.instance_id)
        env.message_number = str(service.message_number)
        env.relates_to = relates_to

        env.probe_resolve_matches.append(ProbeResolveMatch(service.epr,
                                                           service.types, service.scopes,
                                                           service.get_x_addrs(), str(service.metadata_version)))
        self._networking_thread.add_unicast_message(env, addr[0], addr[1])

    def _send_probe_match(self, services, relates_to, addr):
        self._logger.info('sending probe match to %s for %d services', addr, len(services))
        msg_number = 1
        # send one match response for every service, dpws explorer can't handle telegram otherwise if too many devices reported
        for service in services:
            env = SoapEnvelope()
            env.action = ACTION_PROBE_MATCH
            env.addr_to = WSA_ANONYMOUS
            env.instance_id = _generate_instance_id()
            env.message_number = str(msg_number)
            env.relates_to = relates_to

            # add values to ProbeResponse acc. to flags
            epr = service.epr if self.PROBEMATCH_EPR else ''
            types = service.types if self.PROBEMATCH_TYPES else []
            scopes = service.scopes if self.PROBEMATCH_SCOPES else []
            xaddrs = service.get_x_addrs() if self.PROBEMATCH_XADDRS else []
            env.probe_resolve_matches.append(ProbeResolveMatch(epr, types, scopes, xaddrs,
                                                               str(service.metadata_version)))

            self._networking_thread.add_unicast_message(env, addr[0], addr[1], random.randint(0, APP_MAX_DELAY))

    def _send_probe(self, types=None, scopes=None):
        self._logger.debug('sending probe types=%r scopes=%r', _types_info(types), scopes)
        env = SoapEnvelope()
        env.action = ACTION_PROBE
        env.addr_to = ADDRESS_ALL
        env.types = types
        env.scopes = scopes

        if self._disco_proxy_active:
            self._networking_thread.add_unicast_message(env, self.__disco_proxy_address[0],
                                                        self.__disco_proxy_address[1])
        else:
            self._networking_thread.add_multicast_message(env, MULTICAST_IPV4_ADDRESS, MULTICAST_PORT)

    def _send_resolve(self, epr):
        self._logger.debug('sending resolve on %s', epr)
        env = SoapEnvelope()
        env.action = ACTION_RESOLVE
        env.addr_to = ADDRESS_ALL
        env.epr = epr

        if self._disco_proxy_active:
            self._networking_thread.add_unicast_message(env, self.__disco_proxy_address[0],
                                                        self.__disco_proxy_address[1])
        else:
            self._networking_thread.add_multicast_message(env, MULTICAST_IPV4_ADDRESS, MULTICAST_PORT)

    def _send_hello(self, service):
        self._logger.info('sending hello on %s', service)
        service.increment_message_number()
        env = SoapEnvelope()
        env.action = ACTION_HELLO
        env.addr_to = ADDRESS_ALL
        env.instance_id = str(service.instance_id)
        env.message_number = str(service.message_number)
        env.types = service.types
        env.scopes = service.scopes
        env.x_addrs = service.get_x_addrs()
        env.epr = service.epr
        self._networking_thread.add_multicast_message(env, MULTICAST_IPV4_ADDRESS, MULTICAST_PORT,
                                                      random.randint(0, APP_MAX_DELAY))

    def _send_bye(self, service):
        self._logger.debug('sending bye on %s', service)
        env = SoapEnvelope()
        env.action = ACTION_BYE
        env.addr_to = ADDRESS_ALL
        env.instance_id = str(service.instance_id)
        env.message_number = str(service.message_number)
        env.epr = service.epr
        service.increment_message_number()
        self._networking_thread.add_multicast_message(env, MULTICAST_IPV4_ADDRESS, MULTICAST_PORT)

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

        self._networking_thread = _NetworkingThread(self, self._logger)
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

    def clear_remote_services(self):
        """clears remotely discovered services"""
        self._remote_services.clear()

    def clear_local_services(self):
        """send Bye messages for the services and remove them"""
        for service in self._local_services.values():
            self._send_bye(service)
        self._local_services.clear()

    def search_services(self, types=None, scopes=None, timeout=5, repeat_probe_interval=3):
        """search for services given the TYPES and SCOPES in a given timeout
        :param repeat_probe_interval: send another probe message after x seconds"""
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
        return _filter_services(self._remote_services.values(), types, scopes)

    def search_multiple_types(self, types_list, scopes=None, timeout=10, repeat_probe_interval=3):
        """search for services given the list of TYPES and SCOPES in a given timeout.
        It returns services that match at least one of the types (OR condition).
        Can be used to search for devices that support Biceps Draft6 and Final with one search.
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
        result = []
        for _type in types_list:
            result.extend(_filter_services(self._remote_services.values(), _type, scopes))
        return result

    def search_medical_device_services_in_location(self, sdc_location, timeout=3):
        services = self.search_services(types=_MedicalDeviceTypesFilter, timeout=timeout)
        return sdc_location.matching_services(services)

    def publish_service(self, epr, types, scopes, x_addrs):
        """Publish a service with the given TYPES, SCOPES and XAddrs (service addresses)

        if x_addrs contains item, which includes {ip} pattern, one item per IP addres will be sent
        """
        if not self._server_started:
            raise ApiUsageError("Server not started")

        metadata_version = self._local_services[epr].metadata_version + 1 if epr in self._local_services else 1
        service = Service(types, scopes, x_addrs, epr, _generate_instance_id(), metadata_version=metadata_version)
        self._logger.info('publishing %r', service)
        self._local_services[epr] = service
        self._send_hello(service)

    def clear_service(self, epr):
        service = self._local_services[epr]
        self._send_bye(service)
        del self._local_services[epr]

    def get_active_addresses(self):
        return self._networking_thread.get_active_addresses()


class WSDiscoveryBlacklist(WSDiscoveryBase):
    """ Binds to all IP addresses except the black listed ones. """

    def __init__(self, ignored_adaptor_addresses=None, logger=None):
        """
        :param ignoredAdaptorIPAddresses: an optional list of (own) ip addresses that shall not be used for discovery.
                                          IP addresses are handled as regular expressions.
        """
        super().__init__(logger)
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

    def __init__(self, accepted_adapter_addresses, logger=None):
        """
        :param acceptedAdaptorIPAddresses: an optional list of (own) ip addresses that shall not be used for discovery.
        """
        super().__init__(logger)
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

    def __init__(self, adapter_name, logger=None, force_adapter_name=False):
        """
        :param adapter_name: a string,  e.g. 'local area connection'.
                            parameter is only relevant if host has more than one adapter or forceName is True
                            If host has more than one adapter, the adapter with this friendly name is used, but if it does not exist, a RuntimeError is thrown.
        :param logger: use this logger. If none, 'sdc.discover' is used.
        :param force_adapter_name: if True, only this named adapter will be used.
                                 If False, and only one Adapter exists, the one existing adapter is used. (localhost is ignored in this case).
        """
        super().__init__(logger)
        self._my_ip_address = get_ip_for_adapter(adapter_name)

        # all_adapters = get_network_adapter_configs()
        # # try to match name. if it matches, we are already ready.
        # filtered_adapters = [a for a in all_adapters if a.friendly_name == adapter_name]
        # if len(filtered_adapters) == 1:
        #     self._my_ip_address = (filtered_adapters[0].ip,)  # a tuple
        #     return
        if self._my_ip_address is None:
            all_adapters = get_ipv4_addresses()
            all_adapter_names = [ip.nice_name for ip in all_adapters]
            if force_adapter_name:
                raise RuntimeError(f'No adapter "{adapter_name}" found. Having {all_adapter_names}')

            # see if there is only one physical adapter. if yes, use it
            #adapters_not_localhost = [a for a in all_adapters if not a.ip.startswith('127.')]
            adapters_not_localhost = [a for a in all_adapters if not a.ip.startswith('127.')]
            if len(adapters_not_localhost) == 1:
                self._my_ip_address = (adapters_not_localhost[0].ip,)  # a tuple
            else:
                raise RuntimeError(f'No adapter "{adapter_name}" found. Having {all_adapter_names}')

    def _is_accepted_address(self, address):
        """ check if any of the regular expressions matches the argument"""
        return address in self._my_ip_address


_MedicalDeviceTypesFilter = [QName(NS_DPWS, 'Device'),
                             QName('http://standards.ieee.org/downloads/11073/11073-20702-2016',
                                   'MedicalDevice')]
