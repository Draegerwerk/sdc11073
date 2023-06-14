from __future__ import annotations

import logging
import platform
import random
import time
import traceback
from typing import List, TYPE_CHECKING, Optional, Union
from urllib.parse import urlsplit, unquote

from lxml.etree import QName

from .addressmonitorthread import AddressMonitorThread
from .service import Service
from .common import MULTICAST_IPV4_ADDRESS, MULTICAST_PORT
from .common import message_factory
from .networkingthread import NetworkingThreadWindows, NetworkingThreadPosix
from ..definitions_sdc import SDC_v1_Definitions
from ..exceptions import ApiUsageError
from ..namespaces import default_ns_helper as nsh
from ..xml_types import wsd_types
from ..xml_types.addressing_types import HeaderInformationBlock

if TYPE_CHECKING:
    from ..pysoap.msgreader import ReceivedMessage

APP_MAX_DELAY = 500  # miliseconds

WSA_ANONYMOUS = nsh.WSA.namespace + '/anonymous'
ADDRESS_ALL = "urn:docs-oasis-open-org:ws-dd:ns:discovery:2009:01"  # format acc to RFC 2141

NS_D = nsh.WSD.namespace
MATCH_BY_LDAP = NS_D + '/ldap'  # "http://docs.oasis-open.org/ws-dd/ns/discovery/2009/01/ldap"
MATCH_BY_URI = NS_D + '/rfc3986'  # "http://docs.oasis-open.org/ws-dd/ns/discovery/2009/01/rfc3986"
MATCH_BY_UUID = NS_D + '/uuid'  # "http://docs.oasis-open.org/ws-dd/ns/discovery/2009/01/uuid"
MATCH_BY_STRCMP = NS_D + '/strcmp0'  # "http://docs.oasis-open.org/ws-dd/ns/discovery/2009/01/strcmp0"


def types_info(types):
    # helper for logging
    return [str(t) for t in types] if types else types


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


def matches_filter(service, types, scopes: Optional[wsd_types.ScopesType], logger=None):
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
    return [service for service in services if matches_filter(service, types, scopes, logger)]


def generate_instance_id():
    return str(random.randint(1, 0xFFFFFFFF))


def _mk_wsd_soap_message(header_info, payload):
    # use discovery specific namespaces
    return message_factory.mk_soap_message(header_info, payload,
                                           ns_list=[nsh.S12, nsh.WSA, nsh.WSD], use_defaults=False)


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
                if matches_filter(service,
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
            self._networking_thread = NetworkingThreadPosix(self, self._logger, self.multicast_port)
        else:
            self._networking_thread = NetworkingThreadWindows(self, self._logger, self.multicast_port)
        self._networking_thread.start()

        self._addrs_monitor_thread = AddressMonitorThread(self)
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
