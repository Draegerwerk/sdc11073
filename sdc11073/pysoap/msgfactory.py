from __future__ import annotations

import weakref
from io import BytesIO
from typing import Optional, TYPE_CHECKING

from lxml import etree as etree_

from .msgreader import validate_node
from .soapenvelope import Soap12Envelope
from sdc11073.xml_types.addressing import HeaderInformationBlock
from ..exceptions import ApiUsageError
from ..namespaces import WSA_ANONYMOUS
from ..schema_resolver import SchemaResolver
from ..schema_resolver import mk_schema_validator

if TYPE_CHECKING:
    from sdc11073.xml_types.msg_types import MessageType

_LANGUAGE_ATTR = '{http://www.w3.org/XML/1998/namespace}lang'


class CreatedMessage:
    def __init__(self, message, msg_factory):
        self.p_msg = message
        self.msg_factory = msg_factory

    def serialize_message(self, pretty=False, request_manipulator=None, validate=True):
        return self.msg_factory.serialize_message(self, pretty, request_manipulator, validate)


# pylint: disable=no-self-use


class MessageFactory:
    """This class creates soap messages. It is used in two phases:
     1) call one of the mk_xxx methods. All return a CreatedMessage instance that contains the data provided in the call
     2) call the serialize_message method of the CreatedMessage instance to get the xml representation
     """

    def __init__(self, sdc_definitions, logger, validate=True):
        self._logger = logger
        self._sdc_definitions = sdc_definitions

        self._mdib_wref = None
        self._validate = validate
        self._xml_schema = mk_schema_validator(SchemaResolver(sdc_definitions))

    @property
    def _pm_names(self):
        return self._sdc_definitions.data_model.pm_names

    @property
    def _msg_names(self):
        return self._sdc_definitions.data_model.msg_names

    @property
    def _msg_types(self):
        return self._sdc_definitions.data_model.msg_types

    @property
    def _ns_hlp(self):
        return self._sdc_definitions.data_model.ns_helper

    def register_mdib(self, mdib):
        """Factory sometimes must know the mdib data (e.g. Set service, activate method).
        :param mdib: the current mdib
        """
        if mdib is not None and self._mdib_wref is not None:
            raise ApiUsageError('MessageFactory has already an registered mdib')
        self._mdib_wref = None if mdib is None else weakref.ref(mdib)

    def serialize_message(self, message: CreatedMessage, pretty=False,
                          request_manipulator=None, validate=True) -> bytes:
        """

        :param message: a soap envelope
        :param pretty:
        :param request_manipulator: can modify data before sending
        :param validate: if False, no validation is performed, independent of constructor setting
        :return: bytes
        """
        p_msg = message.p_msg
        tmp = BytesIO()
        root = etree_.Element(self._ns_hlp.s12Tag('Envelope'), nsmap=p_msg.nsmap)

        header_node = etree_.SubElement(root, self._ns_hlp.s12Tag('Header'))
        if p_msg.header_info_block:
            info_node = p_msg.header_info_block.as_etree_node('tmp', {})
            header_node.extend(info_node[:])
        header_node.extend(p_msg.header_nodes)
        body_node = etree_.SubElement(root, self._ns_hlp.s12Tag('Body'), nsmap=p_msg.nsmap)
        if p_msg.payload_element is not None:
            body_node.append(p_msg.payload_element)
        if validate:
            self._validate_node(root)

        doc = etree_.ElementTree(element=root)
        if hasattr(request_manipulator, 'manipulate_domtree'):
            _doc = request_manipulator.manipulate_domtree(doc)
            if _doc:
                doc = _doc
        doc.write(tmp, encoding='UTF-8', xml_declaration=True, pretty_print=pretty)
        return tmp.getvalue()

    def mk_soap_message(self,
                        header_info: HeaderInformationBlock,
                        payload: MessageType,
                        ns_map: Optional[list] = None):
        ns_set = {self._ns_hlp.S12, self._ns_hlp.WSA, self._ns_hlp.MSG, self._ns_hlp.PM}  # default
        ns_set.update(payload.additional_namespaces)
        if ns_map:
            ns_set.update(ns_map)
        my_ns_map = self._ns_hlp.partial_map(*ns_set)
        soap_envelope = Soap12Envelope(my_ns_map)
        soap_envelope.set_header_info_block(header_info)
        soap_envelope.payload_element = payload.as_etree_node(payload.NODETYPE, my_ns_map)
        return CreatedMessage(soap_envelope, self)

    def mk_soap_message_etree_payload(self,
                                      header_info: HeaderInformationBlock,
                                      payload_element: Optional[etree_.Element] = None):
        my_ns_map = self._ns_hlp.partial_map(self._ns_hlp.S12, self._ns_hlp.WSE, self._ns_hlp.WSA)
        soap_envelope = Soap12Envelope(my_ns_map)
        soap_envelope.set_header_info_block(header_info)
        soap_envelope.payload_element = payload_element
        return CreatedMessage(soap_envelope, self)

    def mk_reply_soap_message(self,
                              request,
                              response_payload: MessageType,
                              ns_map: Optional[list] = None):
        ns_set = {self._ns_hlp.S12, self._ns_hlp.WSA, self._ns_hlp.MSG, self._ns_hlp.PM}  # default
        ns_set.update(response_payload.additional_namespaces)
        if ns_map:
            ns_set.update(ns_map)
        my_ns_map = self._ns_hlp.partial_map(*ns_set)
        soap_envelope = Soap12Envelope(my_ns_map)
        reply_address = request.message_data.p_msg.header_info_block.mk_reply_header_block(
            action=response_payload.action)
        soap_envelope.set_header_info_block(reply_address)
        soap_envelope.payload_element = response_payload.as_etree_node(response_payload.NODETYPE, my_ns_map)
        return CreatedMessage(soap_envelope, self)

    def _validate_node(self, node):
        if self._validate:
            validate_node(node, self._xml_schema, self._logger)


class MessageFactoryDevice(MessageFactory):
    """This class creates all messages that a device needs to create"""

    def mk_probe_matches_response_message(self, message_data, addresses) -> CreatedMessage:
        nsh = self._ns_hlp
        response = Soap12Envelope()
        reply_info_block = message_data.p_msg.header_info_block.mk_reply_header_block(
            action='{ns_hlp.WSD.namespace}/ProbeMatches', addr_to=WSA_ANONYMOUS)
        response.set_header_info_block(reply_info_block)
        probe_match_node = etree_.Element(nsh.wsdTag('Probematch'),
                                          nsmap=nsh.partial_map(nsh.WSD, nsh.DPWS, nsh.MDPWS))
        types = etree_.SubElement(probe_match_node, nsh.wsdTag('Types'))
        types.text = f'{nsh.DPWS.prefix}:Device {nsh.MDPWS.prefix}:MedicalDevice'
        scopes = etree_.SubElement(probe_match_node, nsh.wsdTag('Scopes'))
        scopes.text = ''
        xaddrs = etree_.SubElement(probe_match_node, nsh.wsdTag('XAddrs'))
        xaddrs.text = ' '.join(addresses)
        response.payload_element = probe_match_node
        return CreatedMessage(response, self)

    def mk_get_mddescription_response_message(self, message_data, mdib, requested_handles) -> CreatedMessage:
        """For simplification reason this implementation returns either all descriptors or none."""
        nsh = self._ns_hlp
        request = message_data.p_msg
        return_all = len(requested_handles) == 0  # if we have handles, we need to check them
        my_namespaces = nsh.partial_map(nsh.S12, nsh.WSA, nsh.MSG, nsh.PM)
        response_envelope = Soap12Envelope(my_namespaces)
        reply_block = request.header_info_block.mk_reply_header_block(
            action=mdib.sdc_definitions.Actions.GetMdDescriptionResponse)
        response_envelope.set_header_info_block(reply_block)

        response_node = etree_.Element(self._msg_names.GetMdDescriptionResponse, nsmap=self._ns_hlp.ns_map)

        for handle in requested_handles:
            # if at least one requested handle is valid, return all.
            if mdib.descriptions.handle.get_one(handle, allow_none=True) is not None:
                return_all = True
                break
        if return_all:
            md_description_node, mdib_version_group = mdib.reconstruct_md_description()
            md_description_node.tag = self._msg_names.MdDescription  # rename according to message
            self._set_mdib_version_group(response_node, mdib_version_group)
        else:
            md_description_node = etree_.Element(self._msg_names.MdDescription)
            self._set_mdib_version_group(response_node, mdib.mdib_version_group)
        response_node.append(md_description_node)
        response_envelope.payload_element = response_node
        return CreatedMessage(response_envelope, self)

    def mk_description_modification_report_body(self, mdib_version_group, updated, created, deleted,
                                                updated_states) -> etree_.Element:
        # This method creates one ReportPart for every descriptor.
        # An optimization is possible by grouping all descriptors with the same parent handle into one ReportPart.
        # This is not implemented, and I think it is not needed.

        report = self._msg_types.DescriptionModificationReport()
        report.set_mdib_version_group(mdib_version_group)
        DescriptionModificationType = self._msg_types.DescriptionModificationType

        for descriptors, modification_type in ((updated, DescriptionModificationType.UPDATE),
                                               (created, DescriptionModificationType.CREATE),
                                               (deleted, DescriptionModificationType.DELETE)):
            for descriptor in descriptors:
                # one report part for every descriptor,
                report_part = report.add_report_part()
                report_part.ModificationType = modification_type
                report_part.ParentDescriptor = descriptor.parent_handle
                report_part.SourceMds = descriptor.source_mds
                report_part.Descriptor.append(descriptor)
                states = [s for s in updated_states if s.DescriptorHandle == descriptor.Handle]
                report_part.State.extend(states)

        nsh = self._ns_hlp
        ns_map = nsh.partial_map(nsh.MSG, nsh.PM)
        return report.as_etree_node(self._msg_names.DescriptionModificationReport, ns_map)

    @staticmethod
    def _set_mdib_version_group(node, mdib_version_group):
        if mdib_version_group.mdib_version is not None:
            node.set('MdibVersion', str(mdib_version_group.mdib_version))
        node.set('SequenceId', str(mdib_version_group.sequence_id))
        if mdib_version_group.instance_id is not None:
            node.set('InstanceId', str(mdib_version_group.instance_id))
