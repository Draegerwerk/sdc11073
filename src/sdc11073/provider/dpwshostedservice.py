from __future__ import annotations

import typing
from io import BytesIO

from lxml import etree as etree_

from sdc11073.dispatch import DispatchKey, RequestDispatcher
from sdc11073.namespaces import EventingActions
from sdc11073.namespaces import default_ns_helper as ns_hlp
from sdc11073.xml_types import mex_types
from sdc11073.xml_types.addressing_types import EndpointReferenceType
from sdc11073.xml_types.dpws_types import HostedServiceType

if typing.TYPE_CHECKING:
    import pathlib
    from sdc11073 import xml_utils

_wsdl_ns = ns_hlp.WSDL.namespace

WSP_NS = ns_hlp.WSP.namespace
_WSP_PREFIX = ns_hlp.WSP.prefix

# DiscoveryType, only used in SDC
_DISCOVERY_TYPE_NS = "http://standards.ieee.org/downloads/11073/11073-10207-2017"

WSDL_S12 = ns_hlp.WSDL12.namespace  # old soap 12 namespace, used in wsdl 1.1. used only for wsdl


def etree_from_file(path: str | pathlib.Path) -> xml_utils.LxmlElement:
    parser = etree_.ETCompatXMLParser(resolve_entities=False)
    doc = etree_.parse(str(path), parser=parser)
    return doc.getroot()


class _EventService(RequestDispatcher):
    """A service that offers subscriptions."""

    def __init__(self, sdc_device, subscriptions_manager, offered_subscriptions):
        super().__init__()
        self._msg_reader = sdc_device.msg_reader
        self._subscriptions_manager = subscriptions_manager
        self._offered_subscriptions = offered_subscriptions
        self.register_post_handler(DispatchKey(EventingActions.Subscribe, ns_hlp.WSE.tag('Subscribe')),
                                   self._on_subscribe)
        self.register_post_handler(DispatchKey(EventingActions.Unsubscribe, ns_hlp.WSE.tag('Unsubscribe')),
                                   self._on_unsubscribe)
        self.register_post_handler(DispatchKey(EventingActions.GetStatus, ns_hlp.WSE.tag('GetStatus')),
                                   self._on_get_status)
        self.register_post_handler(DispatchKey(EventingActions.Renew, ns_hlp.WSE.tag('Renew')),
                                   self._on_renew_status)

    @property
    def subscriptions_manager(self):
        return self._subscriptions_manager

    def _on_subscribe(self, request_data):
        returned_envelope = self._subscriptions_manager.on_subscribe_request(request_data)
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
    """Container for DPWSPortTypeBase instances."""

    def __init__(self, sdc_device, subscriptions_manager, path_element, port_type_impls):
        """:param sdc_device:
        :param path_element:
        :param port_type_impls: list of DPWSPortTypeBase
        """
        offered_subscriptions = []
        for p in port_type_impls:
            offered_subscriptions.extend(p.offered_subscriptions)
        super().__init__(sdc_device, subscriptions_manager, offered_subscriptions)
        self.path_element = path_element

        self._sdc_device = sdc_device
        self._mdib = sdc_device.mdib
        self.port_type_impls = port_type_impls
        self._wsdl_string = self._mk_wsdl_string()
        self.register_post_handler(DispatchKey(f'{ns_hlp.WSX.namespace}/GetMetadata/Request',
                                               ns_hlp.WSX.tag('GetMetadata')),
                                   self._on_get_metadata)
        self.register_get_handler('?wsdl', self._on_get_wsdl)
        for port_type_impl in port_type_impls:
            port_type_impl.register_hosting_service(self)

    def mk_dpws_hosted_instance(self) -> HostedServiceType:
        endpoint_references_list = []
        for addr in self._sdc_device.base_urls:
            epr_type = EndpointReferenceType()
            epr_type.Address = f'{addr.geturl()}/{self.path_element}'
            endpoint_references_list.append(epr_type)
        dpws_hosted = HostedServiceType()
        dpws_hosted.EndpointReference.extend(endpoint_references_list)
        dpws_hosted.Types = [p.port_type_name for p in self.port_type_impls]
        dpws_hosted.ServiceId = self.path_element  # value seems to be not important as long as it is unique
        return dpws_hosted

    def _on_get_wsdl(self) -> bytes:
        """Return wsdl."""
        self._logger.debug('_onGetWsdl returns {}', self._wsdl_string)
        return self._wsdl_string

    def _mk_wsdl_string(self) -> bytes:
        sdc_definitions = self._sdc_device.mdib.sdc_definitions
        my_nsmap = ns_hlp.partial_map(
            ns_hlp.MSG, ns_hlp.PM, ns_hlp.WSA, ns_hlp.WSE, ns_hlp.DPWS, ns_hlp.MDPWS)
        my_nsmap['tns'] = ns_hlp.SDC.namespace
        my_nsmap['dt'] = _DISCOVERY_TYPE_NS
        porttype_prefix = 'tns'
        my_nsmap['wsdl'] = _wsdl_ns
        my_nsmap['s12'] = WSDL_S12
        my_nsmap[_WSP_PREFIX] = WSP_NS
        for port_type_impl in self.port_type_impls:
            for entry in port_type_impl.additional_namespaces:
                my_nsmap[entry.prefix] = entry.namespace
        wsdl_definitions = etree_.Element(etree_.QName(_wsdl_ns, 'definitions'),
                                          nsmap=my_nsmap,
                                          attrib={'targetNamespace': sdc_definitions.PortTypeNamespace})

        types = etree_.SubElement(wsdl_definitions, etree_.QName(_wsdl_ns, 'types'))
        # remove annotations from schemas, this reduces wsdl size from 280kb to 100kb!
        ext_schema_ = etree_from_file(ns_hlp.EXT.local_schema_file)
        ext_schema = self._remove_annotations(ext_schema_)
        pm_schema_ = etree_from_file(ns_hlp.PM.local_schema_file)
        participant_schema = self._remove_annotations(pm_schema_)
        mm_schema_ = etree_from_file(ns_hlp.MSG.local_schema_file)
        message_schema = self._remove_annotations(mm_schema_)
        types.append(ext_schema)
        types.append(participant_schema)
        types.append(message_schema)
        # append all message nodes
        for _port_type_impl in self.port_type_impls:
            _port_type_impl.add_wsdl_messages(wsdl_definitions)
        for _port_type_impl in self.port_type_impls:
            _port_type_impl.add_wsdl_port_type(wsdl_definitions)
        for _port_type_impl in self.port_type_impls:
            _port_type_impl.add_wsdl_binding(wsdl_definitions, porttype_prefix)
        return etree_.tostring(wsdl_definitions, encoding='UTF-8', xml_declaration=True)

    @staticmethod
    def _remove_annotations(root_node):
        remove_annotations_string = b"""<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                                      xmlns:xs="http://www.w3.org/2001/XMLSchema">
          <xsl:output method="xml" indent="yes"/>

          <xsl:template match="@* | node()">
            <xsl:copy>
              <xsl:apply-templates select="@* | node()"/>
            </xsl:copy>
          </xsl:template>

          <xsl:template match="xs:annotation" />
        </xsl:stylesheet>"""
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

        metadata = mex_types.Metadata()
        section = mex_types.RelationshipMetadataSection()
        section.MetadataReference.Host = self._sdc_device.dpws_host
        hosted = self.mk_dpws_hosted_instance()
        section.MetadataReference.Hosted.append(hosted)
        metadata.MetadataSection.append(section)

        section = mex_types.LocationMetadataSection()
        section.Location = location_text
        metadata.MetadataSection.append(section)

        # find namespaces that are used in Types of Host and Hosted
        _nsm = self._mdib.nsmapper
        needed_namespaces = [_nsm.DPWS, _nsm.WSX]
        q_names = self._sdc_device.dpws_host.Types[:]
        q_names.extend(hosted.Types)
        for q_name in q_names:
            for e in _nsm.prefix_enum:
                if e.namespace == q_name.namespace and e not in needed_namespaces:
                    needed_namespaces.append(e)
        response = msg_factory.mk_reply_soap_message(request_data, metadata, needed_namespaces)
        return response

    def __repr__(self):
        return f'{self.__class__.__name__} path={self.path_element} ' \
               f'port types={[dp.port_type_string for dp in self.port_type_impls]}'
