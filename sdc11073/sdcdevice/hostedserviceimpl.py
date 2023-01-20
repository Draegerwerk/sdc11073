from __future__ import annotations

import time
from dataclasses import dataclass
from io import BytesIO
from typing import Iterable, Type, Callable

from lxml import etree as etree_

from .services.servicesbase import DPWSPortTypeImpl
from .. import loghelper
from ..addressing import EndpointReferenceType
from ..dpws import HostedServiceType
from ..httprequesthandler import InvalidActionError
from ..httprequesthandler import RequestData
from ..namespaces import EventingActions
from ..namespaces import default_ns_helper as ns_hlp
from ..pysoap.msgfactory import CreatedMessage
from ..pysoap.soapenvelope import SoapFault, FaultCodeEnum

_wsdl_ns = ns_hlp.WSDL.namespace

WSP_NS = ns_hlp.WSP.namespace
_WSP_PREFIX = ns_hlp.WSP.prefix

# DiscoveryType, only used in SDC
_DISCOVERY_TYPE_NS = "http://standards.ieee.org/downloads/11073/11073-10207-2017"

WSDL_S12 = ns_hlp.WSDL12.namespace  # old soap 12 namespace, used in wsdl 1.1. used only for wsdl


def etree_from_file(path):
    parser = etree_.ETCompatXMLParser(resolve_entities=False)
    with open(path, 'rb') as opened_file:
        xml_text = opened_file.read()
    return etree_.fromstring(xml_text, parser=parser, base_url=path)


@dataclass(frozen=True)
class DispatchKey:
    """"Used to associate a handler to a soap message by action - message combination"""
    action: str
    message_tag: etree_.QName

    def __repr__(self):
        """This shows namespace and localname of the QName."""
        if isinstance(self.message_tag, etree_.QName):
            return f'{self.__class__.__name__} action={self.action} ' \
                   f'msg={self.message_tag.namespace}::{self.message_tag.localname}'
        return f'{self.__class__.__name__} action={self.action} msg={self.message_tag}'


OnPostHandler = Callable[[RequestData], CreatedMessage]

OnGetHandler = Callable[[RequestData], str]


class SoapMessageHandler:
    """This class handles SOAP messages.
    It allows to register handlers for requests. If a message is passed via on_post, it determines the key,
    gets the registered callback for the key and calls it.
    The key of a message is determined by the provided get_key_method in constructor. Usually it is the
    tag of the message in the body or the action in the SOAP header.
    """

    def __init__(self, path_element, msg_factory, log_prefix=None):
        self.path_element = path_element
        self._post_handlers = {}
        self._get_handlers = {}
        self._msg_factory = msg_factory
        self._logger = loghelper.get_logger_adapter(f'sdc.device.{self.__class__.__name__}', log_prefix)

    def register_post_handler(self, dispatch_key: DispatchKey, on_post_handler: OnPostHandler):
        self._post_handlers[dispatch_key] = on_post_handler

    def register_get_handler(self, dispatch_key: str, on_get_handler: OnGetHandler):
        self._get_handlers[dispatch_key] = on_get_handler

    def on_post(self, request_data):
        begin = time.monotonic()
        action = request_data.message_data.action
        func = self.get_post_handler(request_data)
        if func is None:
            fault = SoapFault(code=FaultCodeEnum.SENDER, reason=f'invalid action {action}')
            raise InvalidActionError(fault)
        returned_envelope = func(request_data)
        duration = time.monotonic() - begin
        self._logger.debug('incoming soap action "{}" to {}: duration={:.3f}sec.', action, request_data.path_elements,
                           duration)
        return returned_envelope

    def on_get(self, request_data):
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


class _EventService(SoapMessageHandler):
    """ A service that offers subscriptions"""

    def __init__(self, sdc_device, subscriptions_manager, path_element, offered_subscriptions):
        super().__init__(path_element, sdc_device.msg_factory)
        self._msg_reader = sdc_device.msg_reader
        self._subscriptions_manager = subscriptions_manager
        self._offered_subscriptions = offered_subscriptions
        self.register_post_handler(DispatchKey(EventingActions.Subscribe, ns_hlp.wseTag('Subscribe')),
                                   self._on_subscribe)
        self.register_post_handler(DispatchKey(EventingActions.Unsubscribe, ns_hlp.wseTag('Unsubscribe')),
                                   self._on_unsubscribe)
        self.register_post_handler(DispatchKey(EventingActions.GetStatus, ns_hlp.wseTag('GetStatus')),
                                   self._on_get_status)
        self.register_post_handler(DispatchKey(EventingActions.Renew, ns_hlp.wseTag('Renew')),
                                   self._on_renew_status)

    @property
    def subscriptions_manager(self):
        return self._subscriptions_manager

    def _on_subscribe(self, request_data):
        subscribe_request = self._msg_reader.read_subscribe_request(request_data)
        for subscription_filter in subscribe_request.subscription_filters:
            if subscription_filter not in self._offered_subscriptions:
                raise Exception(f'{self.__class__.__name__}::{self.path_element}: "{subscription_filter}" '
                                f'is not in offered subscriptions: {self._offered_subscriptions}')

        returned_envelope = self._subscriptions_manager.on_subscribe_request(request_data, subscribe_request)
        return returned_envelope

    def _on_unsubscribe(self, request_data):
        returned_envelope = self._subscriptions_manager.on_unsubscribe_request(request_data)
        return returned_envelope

    def _on_get_status(self, request_data):
        returned_envelope = self._subscriptions_manager.on_get_status_request(request_data)
        return returned_envelope

    def _on_renew_status(self, request_data):
        returned_envelope = self._subscriptions_manager.on_renew_request(request_data)
        return returned_envelope


class DPWSHostedService(_EventService):
    """ Container for DPWSPortTypeImpl instances"""

    def __init__(self, sdc_device, subscriptions_manager, path_element, port_type_impls):
        """

        :param sdc_device:
        :param path_element:
        :param port_type_impls: list of DPWSPortTypeImpl
        """
        offered_subscriptions = []
        for p in port_type_impls:
            offered_subscriptions.extend(p.offered_subscriptions)
        super().__init__(sdc_device, subscriptions_manager, path_element, offered_subscriptions)

        self._sdc_device = sdc_device
        self._mdib = sdc_device.mdib
        self._port_type_impls = port_type_impls
        self._my_port_types = [p.port_type_string for p in port_type_impls]
        self._wsdl_string = self._mk_wsdl_string()
        self.register_post_handler(DispatchKey(f'{ns_hlp.WSX.namespace}/GetMetadata/Request',
                                               ns_hlp.wsxTag('GetMetadata')),
                                   self._on_get_metadata)
        self.register_get_handler('?wsdl', self._on_get_wsdl)
        for port_type_impl in port_type_impls:
            port_type_impl.register_handlers(self)

    def mk_dpws_hosted_instance(self) -> HostedServiceType:
        endpoint_references_list = []
        for addr in self._sdc_device.base_urls:
            endpoint_references_list.append(
                EndpointReferenceType(f'{addr.geturl()}/{self.path_element}'))
        port_type_ns = self._mdib.sdc_definitions.PortTypeNamespace
        dpws_hosted = HostedServiceType(
            endpoint_references_list=endpoint_references_list,
            types_list=[etree_.QName(port_type_ns, p) for p in self._my_port_types],
            service_id=self._my_port_types[0])
        return dpws_hosted

    def _on_get_wsdl(self) -> str:
        """ return wsdl"""
        self._logger.debug('_onGetWsdl returns {}', self._wsdl_string)
        return self._wsdl_string

    def _mk_wsdl_string(self):
        sdc_definitions = self._sdc_device.mdib.sdc_definitions
        my_nsmap = ns_hlp.partial_map(
            ns_hlp.MSG, ns_hlp.PM, ns_hlp.WSA, ns_hlp.WSE, ns_hlp.DPWS, ns_hlp.MDPWS)
        my_nsmap['tns'] = ns_hlp.SDC.namespace
        my_nsmap['dt'] = _DISCOVERY_TYPE_NS
        porttype_prefix = 'tns'
        my_nsmap['wsdl'] = _wsdl_ns
        my_nsmap['s12'] = WSDL_S12
        my_nsmap[_WSP_PREFIX] = WSP_NS
        wsdl_definitions = etree_.Element(etree_.QName(_wsdl_ns, 'definitions'),
                                          nsmap=my_nsmap,
                                          attrib={'targetNamespace': sdc_definitions.PortTypeNamespace})

        types = etree_.SubElement(wsdl_definitions, etree_.QName(_wsdl_ns, 'types'))
        # remove annotations from schemas, this reduces wsdl size from 280kb to 100kb!
        ext_schema_ = etree_from_file(sdc_definitions.SchemaFilePaths.ExtensionPointSchemaFile)
        ext_schema = self._remove_annotations(ext_schema_)
        pm_schema_ = etree_from_file(sdc_definitions.SchemaFilePaths.ParticipantModelSchemaFile)
        participant_schema = self._remove_annotations(pm_schema_)
        bmm_schema_ = etree_from_file(sdc_definitions.SchemaFilePaths.MessageModelSchemaFile)
        message_schema = self._remove_annotations(bmm_schema_)
        types.append(ext_schema)
        types.append(participant_schema)
        types.append(message_schema)
        # append all message nodes
        for _port_type_impl in self._port_type_impls:
            _port_type_impl.add_wsdl_messages(wsdl_definitions)
        for _port_type_impl in self._port_type_impls:
            _port_type_impl.add_wsdl_port_type(wsdl_definitions)
        for _port_type_impl in self._port_type_impls:
            _port_type_impl.add_wsdl_binding(wsdl_definitions, porttype_prefix)
        return etree_.tostring(wsdl_definitions)

    @staticmethod
    def _remove_annotations(root_node):
        remove_annotations_string = b'''<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                                      xmlns:xs="http://www.w3.org/2001/XMLSchema">
          <xsl:output method="xml" indent="yes"/>

          <xsl:template match="@* | node()">
            <xsl:copy>
              <xsl:apply-templates select="@* | node()"/>
            </xsl:copy>
          </xsl:template>

          <xsl:template match="xs:annotation" />
        </xsl:stylesheet>'''
        remove_annotations_doc = etree_.parse(BytesIO(remove_annotations_string))
        remove_annotations_xslt = etree_.XSLT(remove_annotations_doc)
        return remove_annotations_xslt(root_node).getroot()

    def _on_get_metadata(self, request_data):
        msg_factory = self._sdc_device.msg_factory
        consumed_path_elements = request_data.consumed_path_elements
        http_header = request_data.http_header

        # determine the correct location of wsdl, depending on call
        host = http_header['Host']  # this is the address that was called.
        all_base_urls = self._sdc_device.base_urls
        my_base_urls = [u for u in all_base_urls if u.netloc == host]
        my_base_url = my_base_urls[0] if len(my_base_urls) > 0 else all_base_urls[0]
        tmp = '/'.join(consumed_path_elements)
        location_text = f'{my_base_url.scheme}://{my_base_url.netloc}/{tmp}/?wsdl'
        response = msg_factory.mk_hosted_get_metadata_response_message(request_data.message_data,
                                                                       self._sdc_device.dpws_host,
                                                                       self.mk_dpws_hosted_instance(),
                                                                       location_text)
        return response

    def __repr__(self):
        return f'{self.__class__.__name__} path={self.path_element} ' \
               f'port types={[dp.port_type_string for dp in self._port_type_impls]}'


@dataclass(frozen=True)
class HostedServices:
    dpws_hosted_services: Iterable[DPWSHostedService]
    get_service: Type[DPWSPortTypeImpl]
    set_service: Type[DPWSPortTypeImpl] = None
    context_service: Type[DPWSPortTypeImpl] = None
    description_event_service: Type[DPWSPortTypeImpl] = None
    state_event_service: Type[DPWSPortTypeImpl] = None
    waveform_service: Type[DPWSPortTypeImpl] = None
    containment_tree_service: Type[DPWSPortTypeImpl] = None
    localization_service: Type[DPWSPortTypeImpl] = None


def mk_dpws_hosts(sdc_device, components, dpws_hosted_service_cls, subscription_managers: dict):
    dpws_services = []
    services_by_name = {}
    for host_name, service_cls_dict in components.hosted_services.items():
        services = []
        for service_name, service_cls in service_cls_dict.items():
            service = service_cls(service_name, sdc_device)
            services.append(service)
            services_by_name[service_name] = service
        subscription_manager = subscription_managers.get(host_name)
        hosted = dpws_hosted_service_cls(sdc_device, subscription_manager, host_name, services)
        dpws_services.append(hosted)
    return dpws_services, services_by_name


def mk_all_services(sdc_device, components, subscription_managers) -> HostedServices:
    # register all services with their endpoint references acc. to structure in components
    dpws_services, services_by_name = mk_dpws_hosts(sdc_device, components, DPWSHostedService, subscription_managers)
    hosted_services = HostedServices(dpws_services,
                                     services_by_name['GetService'],
                                     set_service=services_by_name.get('SetService'),
                                     context_service=services_by_name.get('ContextService'),
                                     description_event_service=services_by_name.get('DescriptionEventService'),
                                     state_event_service=services_by_name.get('StateEventService'),
                                     waveform_service=services_by_name.get('WaveformService'),
                                     containment_tree_service=services_by_name.get('ContainmentTreeService'),
                                     localization_service=services_by_name.get('LocalizationService')
                                     )
    return hosted_services
