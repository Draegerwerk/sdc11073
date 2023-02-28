from __future__ import annotations

import copy
import uuid
import weakref
from io import BytesIO
from typing import List, Optional, TYPE_CHECKING

from lxml import etree as etree_

from .msgreader import validate_node
from .soapenvelope import Soap12Envelope
from sdc11073.xml_types.addressing import Address
from ..exceptions import ApiUsageError
from ..namespaces import WSA_ANONYMOUS
from ..schema_resolver import SchemaResolver
from ..schema_resolver import mk_schema_validator

if TYPE_CHECKING:
    from sdc11073.xml_types.msgtypes import MessageType

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
        if p_msg.address:
            self._mk_header_address(p_msg.address, header_node)
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

    def mk_soap_message(self, addr_to: str,
                        payload: MessageType,
                        ns_map: Optional[dict] = None,
                        reference_param: Optional[list] = None):
        my_ns_map = self._ns_hlp.partial_map(self._ns_hlp.S12, self._ns_hlp.MSG, self._ns_hlp.PM)

        if ns_map is not None:
            my_ns_map.update(ns_map)
        soap_envelope = Soap12Envelope(my_ns_map)
        soap_envelope.set_address(Address(action=payload.action, addr_to=addr_to))
        soap_envelope.payload_element = payload.as_etree_node(payload.NODETYPE, my_ns_map)
        if reference_param is not None:
            self._add_reference_params_to_header(soap_envelope, reference_param)
        return CreatedMessage(soap_envelope, self)

    def mk_soap_message_etree_payload(self, addr_to: str,
                                      action: str,
                                      payload_element: Optional[etree_.Element] = None,
                                      ns_map: Optional[dict] = None,
                                      reference_param: Optional[list] = None):
        my_ns_map = self._ns_hlp.partial_map(self._ns_hlp.S12, self._ns_hlp.MSG, self._ns_hlp.PM)

        if ns_map is not None:
            my_ns_map.update(ns_map)
        soap_envelope = Soap12Envelope(my_ns_map)
        soap_envelope.set_address(Address(action=action, addr_to=addr_to))
        soap_envelope.payload_element = payload_element
        if reference_param is not None:
            self._add_reference_params_to_header(soap_envelope, reference_param)
        return CreatedMessage(soap_envelope, self)

    def mk_reply_soap_message_etree_payload(self, addr_to: Address, payload: etree_.Element):
        soap_envelope = Soap12Envelope(self._ns_hlp.partial_map(self._ns_hlp.MSG))
        soap_envelope.set_address(addr_to)
        soap_envelope.payload_element = payload
        return CreatedMessage(soap_envelope, self)

    def mk_reply_soap_message(self, request,
                              response_payload: MessageType,
                              ns_map=None):
        my_ns_map = self._ns_hlp.partial_map(self._ns_hlp.S12, self._ns_hlp.MSG, self._ns_hlp.PM)
        if ns_map is not None:
            my_ns_map.update(ns_map)
        soap_envelope = Soap12Envelope(my_ns_map)
        reply_address = request.message_data.p_msg.address.mk_reply_address(action=response_payload.action)
        soap_envelope.set_address(reply_address)
        soap_envelope.payload_element = response_payload.as_etree_node(response_payload.NODETYPE, my_ns_map)
        return CreatedMessage(soap_envelope, self)

    def mk_fault_message(self, message_data, soap_fault, action_string=None) -> CreatedMessage:
        ns_hlp = self._ns_hlp
        if action_string is None:
            action_string = f'{ns_hlp.WSA.namespace}/fault'
        soap_envelope = Soap12Envelope(ns_hlp.partial_map(ns_hlp.S12, ns_hlp.WSA, ns_hlp.WSE))
        reply_address = message_data.p_msg.address.mk_reply_address(action_string)
        soap_envelope.set_address(reply_address)
        fault_node = etree_.Element(ns_hlp.s12Tag('Fault'))
        code_node = etree_.SubElement(fault_node, ns_hlp.s12Tag('Code'))
        value_node = etree_.SubElement(code_node, ns_hlp.s12Tag('Value'))
        value_node.text = f's12:{soap_fault.code}'
        if soap_fault.sub_code is not None:
            subcode_node = etree_.SubElement(code_node, ns_hlp.s12Tag('Subcode'))
            sub_value_node = etree_.SubElement(subcode_node, ns_hlp.s12Tag('Value'))
            sub_value_node.text = ns_hlp.doc_name_from_qname(soap_fault.sub_code)
        reason_node = etree_.SubElement(fault_node, ns_hlp.s12Tag('Reason'))
        reason_text_node = etree_.SubElement(reason_node, ns_hlp.s12Tag('Text'))
        reason_text_node.set(ns_hlp.xmlTag('lang'), 'en-US')
        reason_text_node.text = soap_fault.reason
        if soap_fault.details is not None:
            detail_node = etree_.SubElement(fault_node, ns_hlp.s12Tag('Detail'))
            detail_node.set(ns_hlp.xmlTag('lang'), 'en-US')
            det_data_node = etree_.SubElement(detail_node, 'data')
            det_data_node.text = soap_fault.details
        soap_envelope.payload_element = fault_node
        return CreatedMessage(soap_envelope, self)

    def _mk_header_address(self, address, header_node):
        # To (OPTIONAL), defaults to anonymous
        node = etree_.SubElement(header_node, self._ns_hlp.wsaTag('To'),
                                 attrib={self._ns_hlp.s12Tag('mustUnderstand'): 'true'})
        node.text = address.addr_to or WSA_ANONYMOUS
        # From
        if address.addr_from:
            address.addr_from.as_etree_subnode(header_node)
        # ReplyTo (OPTIONAL), defaults to anonymous
        if address.reply_to:
            address.reply_to.as_etree_subnode(header_node)
        # FaultTo (OPTIONAL)
        if address.fault_to:
            address.fault_to.as_etree_subnode(header_node)
        # Action (REQUIRED)
        node = etree_.SubElement(header_node, self._ns_hlp.wsaTag('Action'),
                                 attrib={self._ns_hlp.s12Tag('mustUnderstand'): 'true'})
        node.text = address.action
        # MessageID (OPTIONAL)
        if address.message_id:
            node = etree_.SubElement(header_node, self._ns_hlp.wsaTag('MessageID'))
            node.text = address.message_id
        # RelatesTo (OPTIONAL)
        if address.relates_to:
            node = etree_.SubElement(header_node, self._ns_hlp.wsaTag('RelatesTo'))
            node.text = address.relates_to
            if address.relationship_type is not None:
                node.set('RelationshipType', address.relationship_type)
        for parameter in address.reference_parameters:
            header_node.append(copy.deepcopy(parameter))

    @staticmethod
    def _add_reference_params_to_header(soap_envelope, reference_parameters: list):
        """ add references for requests to device (renew, getstatus, unsubscribe)"""
        for element in reference_parameters:
            tmp = copy.deepcopy(element)
            # mandatory attribute acc. to ws_addressing SOAP Binding (https://www.w3.org/TR/2006/REC-ws-addr-soap-20060509/)
            tmp.set('IsReferenceParameter', 'true')
            soap_envelope.add_header_element(tmp)

    def _validate_node(self, node):
        if self._validate:
            validate_node(node, self._xml_schema, self._logger)


class MessageFactoryDevice(MessageFactory):
    """This class creates all messages that a device needs to create"""

    def mk_probe_matches_response_message(self, message_data, addresses) -> CreatedMessage:
        nsh = self._ns_hlp
        response = Soap12Envelope()
        reply_address = message_data.p_msg.address.mk_reply_address('{ns_hlp.WSD.namespace}/ProbeMatches')
        reply_address.addr_to = WSA_ANONYMOUS
        reply_address.message_id = uuid.uuid4().urn
        response.set_address(reply_address)
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
        reply_address = request.address.mk_reply_address(
            action=mdib.sdc_definitions.Actions.GetMdDescriptionResponse)
        response_envelope.set_address(reply_address)

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

    def mk_operation_response_message(self, message_data, action, response_name, mdib_version_group,
                                      transaction_id, invocation_state, invocation_error, error_text
                                      ) -> CreatedMessage:
        nsh = self._ns_hlp
        request = message_data.p_msg
        response = Soap12Envelope(nsh.partial_map(nsh.S12, nsh.MSG, nsh.WSA))
        reply_address = request.address.mk_reply_address(action=action)
        response.set_address(reply_address)
        ns_map = nsh.partial_map(nsh.PM, nsh.MSG, nsh.XSI, nsh.EXT, nsh.XML)
        reply_body_node = etree_.Element(nsh.msgTag(response_name), nsmap=ns_map)
        self._set_mdib_version_group(reply_body_node, mdib_version_group)
        invocation_info_node = etree_.SubElement(reply_body_node, self._msg_names.InvocationInfo)

        transaction_id_node = etree_.SubElement(invocation_info_node, self._msg_names.TransactionId)
        invocation_state_node = etree_.SubElement(invocation_info_node, self._msg_names.InvocationState)

        invocation_state_node.text = invocation_state
        transaction_id_node.text = str(transaction_id)

        if invocation_error is not None:
            invocation_error_node = etree_.SubElement(invocation_info_node, self._msg_names.InvocationError)
            invocation_error_node.text = invocation_error
        if error_text is not None:
            invocation_error_msg_node = etree_.SubElement(invocation_info_node,
                                                          self._msg_names.InvocationErrorMessage)
            invocation_error_msg_node.text = error_text
        response.payload_element = reply_body_node
        return CreatedMessage(response, self)

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

    def mk_notification_message(self, ws_addr, message_node, reference_params: List[etree_._Element],
                                doc_nsmap) -> CreatedMessage:
        envelope = Soap12Envelope(doc_nsmap)
        envelope.payload_element = message_node
        envelope.set_address(ws_addr)
        for node in reference_params:
            envelope.add_header_element(node)
        return CreatedMessage(envelope, self)
