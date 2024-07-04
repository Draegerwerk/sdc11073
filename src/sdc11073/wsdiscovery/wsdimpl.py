"""implementation of ws discovery."""

from __future__ import annotations

import logging
import random
import time
from enum import Enum
from typing import TYPE_CHECKING, Callable
from urllib.parse import unquote, urlsplit

from sdc11073 import network
from sdc11073.definitions_sdc import SdcV1Definitions
from sdc11073.exceptions import ApiUsageError
from sdc11073.namespaces import default_ns_helper as nsh
from sdc11073.xml_types import wsd_types
from sdc11073.xml_types.addressing_types import HeaderInformationBlock

from .common import MULTICAST_IPV4_ADDRESS, MULTICAST_PORT, message_factory
from sdc11073.wsdiscovery import networkingthread
from .service import Service

if TYPE_CHECKING:
    import ipaddress
    from collections.abc import Iterable
    from logging import Logger

    from lxml.etree import QName

    from sdc11073.location import SdcLocation
    from sdc11073.pysoap.msgfactory import CreatedMessage
    from sdc11073.pysoap.msgreader import ReceivedMessage
    from sdc11073.xml_types.msg_types import MessageType

WSA_ANONYMOUS = nsh.WSA.namespace + '/anonymous'
ADDRESS_ALL = "urn:docs-oasis-open-org:ws-dd:ns:discovery:2009:01"  # format acc to RFC 2141

NS_D = nsh.WSD.namespace

# AppSequence node is mandatory acc. to text is wsdicovery spec, but in reality this is sometimes missing.
# An application can decide to modify this value if it seems appropriate.
# If allow_missing_app_sequence is True, the message is accepted and an InstanceId of 0 is used.
# If allow_missing_app_sequence is False and the element is missing, the whole message will be ignored.
allow_missing_app_sequence = False


class MatchBy(str, Enum):
    """Different Match Options."""

    ldap = NS_D + '/ldap'  # "http://docs.oasis-open.org/ws-dd/ns/discovery/2009/01/ldap"
    uri = NS_D + '/rfc3986'  # "http://docs.oasis-open.org/ws-dd/ns/discovery/2009/01/rfc3986"
    uuid = NS_D + '/uuid'  # "http://docs.oasis-open.org/ws-dd/ns/discovery/2009/01/uuid"
    strcmp = NS_D + '/strcmp0'  # "http://docs.oasis-open.org/ws-dd/ns/discovery/2009/01/strcmp0"


def types_info(types: Iterable[QName] | None) -> list[str] | None:
    """Make printable strings from list of QNames (helper method for logging)."""
    return [str(t) for t in types] if types else types


def match_scope(my_scope: str, other_scope: str, match_by: MatchBy | str | None) -> bool:
    """match_scope checks if my_scope matches other_scope by applying the algorithm defined by match_by.

    match_scope correctly handles "%2F" (== '/') encoded values.
    """
    if match_by in (MatchBy.ldap, MatchBy.uri, MatchBy.uuid, '', None):
        my_scope = urlsplit(my_scope)
        other_scope = urlsplit(other_scope)
        if my_scope.scheme.lower() != other_scope.scheme.lower() \
                or my_scope.netloc.lower() != other_scope.netloc.lower():
            return False
        if my_scope.path == other_scope.path:
            return True
        src_path_elements = my_scope.path.split('/')
        target_path_elements = other_scope.path.split('/')
        src_path_elements = [unquote(elem) for elem in src_path_elements]
        target_path_elements = [unquote(elem) for elem in target_path_elements]
        if len(src_path_elements) > len(target_path_elements):
            return False
        return all(target_path_elements[i] == elem for i, elem in enumerate(src_path_elements))
    if match_by == MatchBy.strcmp:
        return my_scope == other_scope
    return False


def match_type(type1: QName, type2: QName) -> bool:
    """Check if namespace and localname are identical."""
    return type1.namespace == type2.namespace and type1.localname == type2.localname


def _is_type_in_list(ttype: QName, types: list[QName]) -> bool:
    return any(match_type(ttype, entry) for entry in types)


def _is_scope_in_list(uri: str, match_by: str, srv_sc: wsd_types.ScopesType) -> bool:
    # returns True if every entry in scope.text is also found in srv_sc.text
    # all entries are URIs
    if srv_sc is None:
        return False
    return any(match_scope(uri, entry, match_by) for entry in srv_sc.text)


def matches_filter(service: Service,
                   types: Iterable[QName] | None,
                   scopes: wsd_types.ScopesType | None) -> bool:
    """Check if service matches the types and scopes."""
    if types is not None:
        for ttype in types:
            if not _is_type_in_list(ttype, service.types):
                return False
    if scopes is not None:
        for uri in scopes.text:
            if not _is_scope_in_list(uri, scopes.MatchBy, service.scopes):
                return False
    return True


def filter_services(services: Iterable[Service],
                    types: Iterable[QName] | None,
                    scopes: wsd_types.ScopesType | None) -> list[Service]:
    """Filter services that match types and scopes."""
    return [service for service in services if matches_filter(service, types, scopes)]


def _mk_wsd_soap_message(header_info: HeaderInformationBlock,
                         payload: MessageType) -> CreatedMessage:
    # use discovery specific namespaces
    return message_factory.mk_soap_message(header_info, payload,
                                           ns_list=[nsh.S12, nsh.WSA, nsh.WSD], use_defaults=False)


class WSDiscovery:
    """UDP based discovery."""

    # these flags control which data is included in ProbeResponse messages.
    PROBEMATCH_EPR = True
    PROBEMATCH_TYPES = True
    PROBEMATCH_SCOPES = True
    PROBEMATCH_XADDRS = True

    def __init__(self,
                 ip_address: str | ipaddress.IPv4Address,
                 logger: Logger | None = None,
                 multicast_port: int = MULTICAST_PORT):
        """Create a WsDiscovery instance.

        :param ip_address: network adapter to bind to
        :param logger: use this logger. if None a logger 'sdc.discover' is created.
        :param multicast_port: defaults to MULTICAST_PORT.
               If port is changed, instance will not be able to communicate with implementations
               that use the correct port (which is the default MULTICAST_PORT)!
        """
        self._adapter = network.get_adapter_containing_ip(ip_address)
        self._networking_thread = None
        self._addrs_monitor_thread = None
        self._server_started = False
        self._remote_services = {}
        self._local_services = {}
        self._remote_service_hello_callback = None
        self._remote_service_hello_callback_types_filter = None
        self._remote_service_hello_callback_scopes_filter = None
        self._remote_service_bye_callback = None
        self._remote_service_resolve_match_callback = None  # B.D.
        self._on_probe_callback = None

        self._logger = logger or logging.getLogger('sdc.discover')
        self.multicast_port = multicast_port

    def start(self):
        """Start the discovery server - should be called before using other functions."""
        if not self._server_started:
            self._start_threads()
            self._server_started = True

    def stop(self):
        """Clean up and stop the discovery server."""
        if self._server_started:
            self.clear_remote_services()
            self.clear_local_services()

            self._stop_threads()
            self._server_started = False

    def search_services(self,
                        types: Iterable[QName] | None = None,
                        scopes: wsd_types.ScopesType | None = None,
                        timeout: int | float | None = 5,
                        repeat_probe_interval: int | None = 3) -> list[Service]:
        """Search for services that match given types and scopes.

        :param types: list of types that a service must have (all of them), no filtering if value is None
        :param scopes:scopes to search for, no filtering if value is None
        :param timeout: total duration of search
        :param repeat_probe_interval: send another probe message after x seconds
        :return:
        """
        if not self._server_started:
            raise RuntimeError("Server not started")

        types = list(types) if types is not None else None
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
        return filter_services(list(self._remote_services.values()), types, scopes)

    def search_sdc_services(self,
                            scopes: wsd_types.ScopesType | None = None,
                            timeout: int | float | None = 5,
                            repeat_probe_interval: int | None = 3) -> list[Service]:
        """Search for sdc services that match given scopes.

        :param scopes: scopes to search for, no scopes filtering if value is None
        :param timeout: total duration of search
        :param repeat_probe_interval: send another probe message after x seconds
        :return:
        """
        return self.search_services(SdcV1Definitions.MedicalDeviceTypesFilter, scopes, timeout, repeat_probe_interval)

    def search_multiple_types(self,
                              types_list: list[list[QName]],
                              scopes: wsd_types.ScopesType | None = None,
                              timeout: int | float | None = 10,
                              repeat_probe_interval: int | None = 3) -> list[Service]:
        """Search for services given the list of TYPES and SCOPES in a given timeout.

        It returns services that match at least one of the types (OR condition).
        Can be used to search for devices that support Biceps Draft6 and Final with one search.
        :param types_list:
        :param scopes:
        :param timeout: total duration of search
        :param repeat_probe_interval: send another probe message after x seconds.
        """
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
            tmp = filter_services(list(self._remote_services.values()), _type, scopes)
            for srv in tmp:
                result[srv.epr] = srv
        return list(result.values())

    def search_sdc_device_services_in_location(self, sdc_location: SdcLocation, timeout: int = 3) -> list[Service]:
        """Search for all sdc devices (no scopes filter applied), then filter locally for location."""
        services = self.search_sdc_services(timeout=timeout)
        return sdc_location.filter_services_inside(services)

    def publish_service(self, epr: str,
                        types: list[QName],
                        scopes: wsd_types.ScopesType,
                        x_addrs: list[str]):
        """Publish a service with the given TYPES, SCOPES and XAddrs (service addresses).

        if x_addrs contains item, which includes {ip} pattern, one item per IP address will be sent
        """
        if not self._server_started:
            raise ApiUsageError("Server not started")

        metadata_version = self._local_services[epr].metadata_version + 1 if epr in self._local_services else 1
        instance_id = str(random.randint(1, 0xFFFFFFFF))  # noqa: S311
        service = Service(types, scopes, x_addrs, epr, instance_id, metadata_version=metadata_version)
        self._logger.info('publishing %r', service)
        self._local_services[epr] = service
        self._send_hello(service)

    def clear_remote_services(self):
        """Clear remotely discovered services."""
        self._remote_services.clear()

    def clear_local_services(self):
        """Send Bye messages for the services and remove them."""
        for service in self._local_services.values():
            self._send_bye(service)
        self._local_services.clear()

    def clear_service(self, epr: str):
        """Clear local service with given epr."""
        service = self._local_services[epr]
        self._send_bye(service)
        del self._local_services[epr]

    def get_active_addresses(self) -> list[str]:
        """Get active addresses."""
        # TODO: do not return list
        return [str(self._adapter.ip)]

    def set_remote_service_hello_callback(self,
                                          callback: Callable,
                                          types: list[QName] | None = None,
                                          scopes: wsd_types.ScopesType | None = None):
        """Set callback, which will be called when new service appeared online and sent Hello message.

        typesFilter and scopesFilter might be list of types and scopes.
        If filter is set, callback is called only for Hello messages,
        which match filter

        Set None to disable callback
        """
        self._remote_service_hello_callback = callback
        self._remote_service_hello_callback_types_filter = types
        self._remote_service_hello_callback_scopes_filter = scopes

    def set_remote_service_bye_callback(self, callback: Callable[[str, str], None] | None):
        """Set callback, which will be called when a bye message is received.

        Set to None to disable callback.
        """
        self._remote_service_bye_callback = callback

    def set_remote_service_resolve_match_callback(self, callback: Callable[[Service], None] | None):
        """Set callback, which will be called when a resolve match message is received.

        Set to None to disable callback.
        """
        self._remote_service_resolve_match_callback = callback

    def set_on_probe_callback(self, callback: Callable[[str, wsd_types.ProbeType], None] | None):
        """Set callback, which will be called when a probe message is received.

        Set to None to disable callback.
        """
        self._on_probe_callback = callback

    def _add_remote_service(self, service: Service):
        if not service.epr:
            self._logger.info('service without epr, ignoring it! %r', service)
            return
        already_known_service = self._remote_services.get(service.epr)
        if not already_known_service:
            self._remote_services[service.epr] = service
            self._logger.info('new remote %r', service)
            return

        if service.metadata_version == already_known_service.metadata_version:
            self._logger.debug('update remote service: remote Service %s; MetadataVersion: %d',
                               service.epr, service.metadata_version)
            if len(service.x_addrs) > len(already_known_service.x_addrs):
                already_known_service.x_addrs = service.x_addrs
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

    def _remove_remote_service(self, epr: str):
        if epr in self._remote_services:
            del self._remote_services[epr]

    def _handle_received_hello(self, received_message: ReceivedMessage, addr_from: str):
        app_sequence_node = received_message.p_msg.header_node.find(nsh.WSD.tag('AppSequence'))
        if app_sequence_node is None:
            if allow_missing_app_sequence:
                instance_id = 0
            else:
                self._logger.debug('received Hello from %r without AppSequence, ignoring it', addr_from)
                return
        else:
            app_sequence = wsd_types.AppSequenceType.from_node(app_sequence_node)
            instance_id = app_sequence.InstanceId
        hello = wsd_types.HelloType.from_node(received_message.p_msg.msg_node)
        epr = hello.EndpointReference.Address
        scopes = hello.Scopes
        service = Service(hello.Types, scopes, hello.XAddrs, epr,
                          instance_id, metadata_version=hello.MetadataVersion)
        self._add_remote_service(service)
        if not hello.XAddrs:  # B.D.
            self._logger.debug('%s(%s) has no Xaddr, sending resolve message', epr, addr_from)
            self._send_resolve(epr)
        if self._remote_service_hello_callback is not None:
            if matches_filter(service,
                              self._remote_service_hello_callback_types_filter,
                              self._remote_service_hello_callback_scopes_filter):
                self._remote_service_hello_callback(addr_from, service)

    def _handle_received_probe(self, received_message: ReceivedMessage, addr_from: str):
        probe = wsd_types.ProbeType.from_node(received_message.p_msg.msg_node)
        scopes = probe.Scopes
        services = filter_services(self._local_services.values(), probe.Types, scopes)
        if services:
            self._send_probe_match(services, received_message.p_msg.header_info_block.MessageID, addr_from)
        if self._on_probe_callback is not None:
            self._on_probe_callback(addr_from, probe)

    def _handle_received_probe_matches(self, received_message: ReceivedMessage, addr_from: str):
        app_sequence_node = received_message.p_msg.header_node.find(nsh.WSD.tag('AppSequence'))
        if app_sequence_node is None:
            if allow_missing_app_sequence:
                instance_id = 0
            else:
                self._logger.debug('received ProbeMatches from %r without AppSequence, ignoring it', addr_from)
                return
        else:
            app_sequence = wsd_types.AppSequenceType.from_node(app_sequence_node)
            instance_id = app_sequence.InstanceId
        probe_matches = wsd_types.ProbeMatchesType.from_node(received_message.p_msg.msg_node)
        self._logger.debug('handle_received_message: len(ProbeMatch) = %d', len(probe_matches.ProbeMatch))
        for match in probe_matches.ProbeMatch:
            epr = match.EndpointReference.Address
            scopes = match.Scopes
            service = Service(match.Types, scopes, match.XAddrs, epr,
                              instance_id, metadata_version=match.MetadataVersion)
            self._add_remote_service(service)
            if match.XAddrs is None or len(match.XAddrs) == 0:
                self._logger.info('%s(%s) has no Xaddr, sending resolve message', epr, addr_from)
                self._send_resolve(epr)
            elif not match.Types:
                self._logger.info('%s(%s) has no Types, sending resolve message', epr, addr_from)
                self._send_resolve(epr)
            elif not match.Scopes:
                self._logger.info('%s(%s) has no Scopes, sending resolve message', epr, addr_from)
                self._send_resolve(epr)

    def _handle_received_resolve(self, received_message: ReceivedMessage, addr_from: str):
        resolve = wsd_types.ResolveType.from_node(received_message.p_msg.msg_node)
        epr = resolve.EndpointReference.Address
        if epr in self._local_services:
            service = self._local_services[epr]
            self._send_resolve_match(service, received_message.p_msg.header_info_block.MessageID, addr_from)

    def _handle_received_resolve_matches(self, received_message: ReceivedMessage, addr_from: str):  # ARG002
        app_sequence_node = received_message.p_msg.header_node.find(nsh.WSD.tag('AppSequence'))
        if app_sequence_node is None:
            if allow_missing_app_sequence:
                instance_id = 0
            else:
                self._logger.debug('received ResolveMatches from %r without AppSequence, ignoring it', addr_from)
                return
        else:
            app_sequence = wsd_types.AppSequenceType.from_node(app_sequence_node)
            instance_id = app_sequence.InstanceId
        resolve_matches = wsd_types.ResolveMatchesType.from_node(received_message.p_msg.msg_node)
        match = resolve_matches.ResolveMatch
        epr = match.EndpointReference.Address
        scopes = match.Scopes
        service = Service(match.Types, scopes, match.XAddrs, epr,
                          instance_id, metadata_version=match.MetadataVersion)
        self._add_remote_service(service)
        if self._remote_service_resolve_match_callback is not None:
            self._remote_service_resolve_match_callback(service)

    def _handle_received_bye(self, received_message: ReceivedMessage, addr_from: str):
        bye = wsd_types.ByeType.from_node(received_message.p_msg.msg_node)
        epr = bye.EndpointReference.Address
        self._remove_remote_service(epr)
        if self._remote_service_bye_callback is not None:
            self._remote_service_bye_callback(addr_from, epr)

    def handle_received_message(self, received_message: ReceivedMessage, addr_from: str):
        """Forward received message to specific handler (dispatch by action)."""
        action = received_message.action
        self._logger.debug('handle_received_message: received %s from %s', action.split('/')[-1], addr_from)
        lookup = {wsd_types.HelloType.action: self._handle_received_hello,
                  wsd_types.ProbeType.action: self._handle_received_probe,
                  wsd_types.ProbeMatchesType.action: self._handle_received_probe_matches,
                  wsd_types.ResolveType.action: self._handle_received_resolve,
                  wsd_types.ResolveMatchesType.action: self._handle_received_resolve_matches,
                  wsd_types.ByeType.action: self._handle_received_bye,
                  }
        try:
            func: Callable[[ReceivedMessage, str], None] = lookup[action]
        except KeyError:
            self._logger.error('unknown action %s', action)
        else:
            func(received_message, addr_from)

    def _send_resolve_match(self, service: Service, relates_to: str, addr: str):
        self._logger.info('sending resolve match to %s', addr)
        service.increment_message_number()
        payload = wsd_types.ResolveMatchesType()
        payload.ResolveMatch = wsd_types.ResolveMatchType()
        payload.ResolveMatch.EndpointReference.Address = service.epr
        payload.ResolveMatch.MetadataVersion = service.metadata_version
        payload.ResolveMatch.Types = service.types
        payload.ResolveMatch.Scopes = service.scopes
        payload.ResolveMatch.XAddrs.extend(service.x_addrs)
        inf = HeaderInformationBlock(action=payload.action,
                                     addr_to=WSA_ANONYMOUS,
                                     relates_to=relates_to)
        app_sequence = wsd_types.AppSequenceType()
        app_sequence.InstanceId = int(service.instance_id)
        app_sequence.MessageNumber = service.message_number

        created_message = _mk_wsd_soap_message(inf, payload)
        created_message.p_msg.add_header_element(app_sequence.as_etree_node(nsh.WSD.tag('AppSequence'),
                                                                            ns_map=nsh.partial_map(nsh.WSD)))
        self._networking_thread.add_outbound_message(created_message, addr[0], addr[1],
                                                     networkingthread.UNICAST_REPEAT_PARAMS)

    def _send_probe_match(self, services: list[Service], relates_to: str, addr: str):
        self._logger.info('sending probe match to %s for %d services', addr, len(services))
        msg_number = 1
        # send one match response for every service.
        # dpws explorer can't handle telegram otherwise if too many devices reported
        for service in services:
            payload = wsd_types.ProbeMatchesType()

            # add values to ProbeResponse acc. to flags
            epr = service.epr if self.PROBEMATCH_EPR else ''
            types = service.types if self.PROBEMATCH_TYPES else []
            scopes = service.scopes if self.PROBEMATCH_SCOPES else None
            xaddrs = service.x_addrs if self.PROBEMATCH_XADDRS else []

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
            self._networking_thread.add_outbound_message(created_message, addr[0], addr[1],
                                                         networkingthread.UNICAST_REPEAT_PARAMS)

    def _send_probe(self, types: Iterable[QName] | None = None, scopes: wsd_types.ScopesType | None = None):
        types = list(types) if types is not None else None  # enforce iteration
        self._logger.debug('sending probe types=%r scopes=%r', types_info(types), scopes)
        payload = wsd_types.ProbeType()
        payload.Types = types
        if scopes is not None:
            payload.Scopes = scopes

        inf = HeaderInformationBlock(action=payload.action, addr_to=ADDRESS_ALL)
        created_message = _mk_wsd_soap_message(inf, payload)
        self._networking_thread.add_outbound_message(created_message, MULTICAST_IPV4_ADDRESS, self.multicast_port,
                                                     networkingthread.MULTICAST_REPEAT_PARAMS)

    def _send_resolve(self, epr: str):
        self._logger.debug('sending resolve on %s', epr)
        payload = wsd_types.ResolveType()
        payload.EndpointReference.Address = epr

        inf = HeaderInformationBlock(action=payload.action, addr_to=ADDRESS_ALL)
        created_message = _mk_wsd_soap_message(inf, payload)
        self._networking_thread.add_outbound_message(created_message, MULTICAST_IPV4_ADDRESS, self.multicast_port,
                                                     networkingthread.MULTICAST_REPEAT_PARAMS)

    def _send_hello(self, service: Service):
        self._logger.info('sending hello on %s', service)
        service.increment_message_number()
        app_sequence = wsd_types.AppSequenceType()
        app_sequence.InstanceId = int(service.instance_id)
        app_sequence.MessageNumber = service.message_number

        payload = wsd_types.HelloType()
        payload.Types = service.types
        payload.Scopes = service.scopes
        payload.XAddrs = service.x_addrs
        payload.EndpointReference.Address = service.epr

        inf = HeaderInformationBlock(action=payload.action, addr_to=ADDRESS_ALL)

        created_message = _mk_wsd_soap_message(inf, payload)
        created_message.p_msg.add_header_element(app_sequence.as_etree_node(nsh.WSD.tag('AppSequence'),
                                                                            ns_map=nsh.partial_map(nsh.WSD)))
        self._networking_thread.add_outbound_message(created_message, MULTICAST_IPV4_ADDRESS, self.multicast_port,
                                                     networkingthread.MULTICAST_REPEAT_PARAMS)

    def _send_bye(self, service: Service):
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
        self._networking_thread.add_outbound_message(created_message, MULTICAST_IPV4_ADDRESS, self.multicast_port,
                                                     networkingthread.MULTICAST_REPEAT_PARAMS)

    def _start_threads(self):
        if self._networking_thread is not None:
            return
        self._networking_thread = networkingthread.NetworkingThread(str(self._adapter.ip), self, self._logger,
                                                                    self.multicast_port)
        self._networking_thread.start()

    def _stop_threads(self):
        if self._networking_thread is None:
            return

        self._networking_thread.schedule_stop()

        self._networking_thread.join()

        self._networking_thread = None


class WSDiscoverySingleAdapter(WSDiscovery):
    """Bind to a single adapter, identified by name."""

    def __init__(self, adapter_name: str, logger: Logger | None = None, multicast_port: int = MULTICAST_PORT):
        """WSDiscoverySingleAdapter uses an adapter name to determine the ip address.

        :param adapter_name: name of the network adapter on which the discovery is to be performed
        :param logger: use this logger. If None, 'sdc.discover' is used.
        :param multicast_port: multicast port
        """
        adapters = [adapter for adapter in network.get_adapters() if adapter.name == adapter_name]
        if not adapters:
            names = [adapter.name for adapter in network.get_adapters()]
            raise RuntimeError(f'No adapter named "{adapter_name}", have {names}')
        if len(adapters) > 1:
            raise RuntimeError(f'Found multiple possible ip addresses on adapter "{adapter_name}"')
        super().__init__(adapters[0].ip, logger, multicast_port)
