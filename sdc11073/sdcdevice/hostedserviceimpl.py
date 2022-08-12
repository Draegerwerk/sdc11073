import time
from io import BytesIO

from lxml import etree as etree_

from .. import loghelper
from ..addressing import EndpointReferenceType
from ..dpws import HostedServiceType
from ..httprequesthandler import InvalidActionError
from ..namespaces import Prefixes
from ..pysoap.soapenvelope import SoapFault, SoapFaultCode

_wsdl_ns = Prefixes.WSDL.namespace

WSP_NS = 'http://www.w3.org/ns/ws-policy'
_WSP_PREFIX = 'wsp'

# DiscoveryType, only used in SDC
_DISCOVERY_TYPE_NS = "http://standards.ieee.org/downloads/11073/11073-10207-2017"

WSDL_S12 = "http://schemas.xmlsoap.org/wsdl/soap12/"  # old soap 12 namespace, used in wsdl 1.1. used only for wsdl


def by_action(request_data):
    """returns the action string of the request"""
    return request_data.message_data.action


def by_msg_tag(request_data):
    """returns Qname of the message if the soap body is not empty, otherwise the action string"""
    ret = request_data.message_data.msg_name
    if ret is None:
        ret = by_action(request_data)
    return ret


def etree_from_file(path):
    parser = etree_.ETCompatXMLParser(resolve_entities=False)
    with open(path, 'rb') as opened_file:
        xml_text = opened_file.read()
    return etree_.fromstring(xml_text, parser=parser, base_url=path)


class SoapMessageHandler:
    """This class handles SOAP messages.
    It allows to register handlers for requests. If a message is passed via on_post, it determines the key,
    gets the registered callback for the key and calls it.
    The key of a message is determined by the provided get_key_method in constructor. It usually is the
    tag of the message in the body or the action in the SOAP header.
    """

    def __init__(self, path_element, get_key_method, msg_factory, log_prefix=None):
        self.path_element = path_element
        self._post_handlers = {}
        self._get_handlers = {}
        self._get_key_method = get_key_method
        self._msg_factory = msg_factory
        self._logger = loghelper.get_logger_adapter(f'sdc.device.{self.__class__.__name__}', log_prefix)

    def register_post_handler(self, key, func):
        self._post_handlers[key] = func

    def register_get_handler(self, key, func):
        self._get_handlers[key] = func

    def on_post(self, request_data):
        begin = time.monotonic()
        action = request_data.message_data.action
        func = self.get_post_handler(request_data)
        if func is None:
            fault = SoapFault(code=SoapFaultCode.SENDER, reason=f'invalid action {action}')
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
        key = self._get_key_method(request_data)
        handler = self._post_handlers.get(key)
        if handler is None:
            self._logger.info('no handler for key={}', key)
        return self._post_handlers.get(key)

    def get_keys(self):
        """ returns a list of action strings that can be handled."""
        return list(self._post_handlers.keys())


class EventService(SoapMessageHandler):
    """ A service that offers subscriptions"""

    def __init__(self, sdc_device, path_element, get_key_method, offered_subscriptions):
        super().__init__(path_element, get_key_method, sdc_device.msg_factory)
        self._sdc_device = sdc_device
        self._subscriptions_manager = sdc_device.subscriptions_manager
        self._offered_subscriptions = offered_subscriptions
        self.register_post_handler(f'{Prefixes.WSE.namespace}/Subscribe', self._on_subscribe)
        self.register_post_handler(f'{Prefixes.WSE.namespace}/Unsubscribe', self._on_unsubscribe)
        self.register_post_handler(f'{Prefixes.WSE.namespace}/GetStatus', self._on_get_status)
        self.register_post_handler(f'{Prefixes.WSE.namespace}/Renew', self._on_renew_status)
        self.register_post_handler('Subscribe', self._on_subscribe)
        self.register_post_handler('Unsubscribe', self._on_unsubscribe)
        self.register_post_handler('GetStatus', self._on_get_status)
        self.register_post_handler('Renew', self._on_renew_status)

    def _on_subscribe(self, request_data):
        subscribe_request = self._sdc_device.msg_reader.read_subscribe_request(request_data)
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


class DPWSHostedService(EventService):
    """ Container for DPWSPortTypeImpl instances"""

    def __init__(self, sdc_device, path_element, get_key_method, port_type_impls, offered_subscriptions):
        """

        :param sdc_device:
        :param path_element:
        :param port_type_impls: list of DPWSPortTypeImpl
        :param offered_subscriptions: list of action strings
        """
        super().__init__(sdc_device, path_element, get_key_method, offered_subscriptions)
        self._sdc_device = sdc_device
        self._mdib = sdc_device.mdib
        self._port_type_impls = port_type_impls
        self._my_port_types = [p.port_type_string for p in port_type_impls]
        self._wsdl_string = self._mk_wsdl_string()
        self.register_post_handler(f'{Prefixes.WSX.namespace}/GetMetadata/Request', self._on_get_metadata)
        self.register_post_handler('GetMetadata', self._on_get_metadata)
        self.register_get_handler('?wsdl', func=self._on_get_wsdl)
        for port_type_impl in port_type_impls:
            port_type_impl.register_handlers(self)

    def mk_dpws_hosted_instance(self) -> HostedServiceType:
        endpoint_references_list = []
        for addr in self._sdc_device.base_urls:
            endpoint_references_list.append(
                EndpointReferenceType(f'{addr.geturl()}/{self.path_element}'))
        porttype_ns = self._mdib.sdc_definitions.PortTypeNamespace
        # little bit ugly: normalize_xml_text needs bytes, not string. and it looks for namespace in "".
        _normalized = self._mdib.sdc_definitions.normalize_xml_text(b'"' + porttype_ns.encode('utf-8') + b'"')
        porttype_ns = _normalized[1:-1].decode('utf-8')
        dpws_hosted = HostedServiceType(
            endpoint_references_list=endpoint_references_list,
            types_list=[etree_.QName(porttype_ns, p) for p in self._my_port_types],
            service_id=self._my_port_types[0])
        return dpws_hosted

    def _on_get_wsdl(self) -> str:
        """ return wsdl"""
        self._logger.debug('_onGetWsdl returns {}', self._wsdl_string)
        return self._wsdl_string

    def _mk_wsdl_string(self):
        sdc_definitions = self._sdc_device.mdib.sdc_definitions
        my_nsmap = Prefixes.partial_map(
            Prefixes.MSG, Prefixes.PM, Prefixes.WSA, Prefixes.WSE, Prefixes.DPWS, Prefixes.MDPWS)
        my_nsmap['tns'] = Prefixes.SDC.namespace
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
        return sdc_definitions.denormalize_xml_text(etree_.tostring(wsdl_definitions))

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
               f'Porttypes={[dp.port_type_string for dp in self._port_type_impls]}'
