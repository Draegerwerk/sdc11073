"""The msgfactory module contains the MessageFactory that creates soap messages."""

from __future__ import annotations

from io import BytesIO
from typing import TYPE_CHECKING

from lxml import etree

from sdc11073.pysoap.msgreader import validate_node
from sdc11073.pysoap.soapenvelope import Soap12Envelope
from sdc11073.schema_resolver import mk_schema_validator

if TYPE_CHECKING:
    from collections.abc import Iterable

    from sdc11073 import xml_utils
    from sdc11073.consumer.manipulator import RequestManipulatorProtocol
    from sdc11073.definitions_base import BaseDefinitions
    from sdc11073.dispatch.request import RequestData
    from sdc11073.loghelper import LoggerAdapter
    from sdc11073.namespaces import PrefixNamespace
    from sdc11073.xml_types.addressing_types import HeaderInformationBlock
    from sdc11073.xml_types.msg_types import MessageType


class CreatedMessage:
    """Represents a soap message created by a MessageFactory together with the factory that created it."""

    def __init__(self, message: Soap12Envelope, msg_factory: MessageFactory):
        self.p_msg = message
        self.msg_factory = msg_factory

    def serialize(
        self,
        pretty: bool = False,
        request_manipulator: RequestManipulatorProtocol | None = None,
        validate: bool = True,
    ) -> bytes:
        """Serialize the message to its raw XML representation."""
        return self.msg_factory.serialize_message(self, pretty, request_manipulator, validate)


class MessageFactory:
    """Create soap messages.

    It is used in two phases:
    1) call one of the mk_xxx methods. All return a CreatedMessage instance that contains the data provided in the call
    2) call the serialize method of the CreatedMessage instance to get the xml representation
    """

    def __init__(
        self,
        sdc_definitions: type[BaseDefinitions],
        additional_schema_specs: Iterable[PrefixNamespace] | None,
        logger: LoggerAdapter,
        validate: bool = True,
    ):
        self.schema_specs = [entry.value for entry in sdc_definitions.data_model.ns_helper.prefix_enum]
        if additional_schema_specs is not None:
            self.schema_specs.extend(additional_schema_specs)
        self._logger = logger
        self.ns_hlp = sdc_definitions.data_model.ns_helper
        self._validate = validate
        self._xml_schema: etree.XMLSchema = mk_schema_validator(self.schema_specs, self.ns_hlp)

    def serialize_message(
        self,
        message: CreatedMessage,
        pretty: bool = False,
        request_manipulator: RequestManipulatorProtocol | None = None,
        validate: bool = True,
    ) -> bytes:
        """Serialize a CreatedMessage instance to bytes.

        :param message: a CreatedMessage instance
        :param pretty:
        :param request_manipulator: can modify data before sending
        :param validate: if False, no validation is performed, independent of constructor setting
        :return: bytes
        """
        p_msg = message.p_msg
        nsh = self.ns_hlp
        tmp = BytesIO()
        root = etree.Element(nsh.S12.tag('Envelope'), nsmap=p_msg.nsmap)

        header_node = etree.SubElement(root, nsh.S12.tag('Header'))
        if p_msg.header_info_block:
            info_node = p_msg.header_info_block.as_etree_node('tmp', {})
            header_node.extend(info_node[:])
        header_node.extend(p_msg.header_nodes)
        body_node = etree.SubElement(root, nsh.S12.tag('Body'), nsmap=p_msg.nsmap)
        if validate:
            self._validate_node(root)
        if p_msg.payload_element is not None:
            if validate:
                self._validate_node(p_msg.payload_element)
            body_node.append(p_msg.payload_element)

        doc = etree.ElementTree(element=root)
        if hasattr(request_manipulator, 'manipulate_domtree'):
            _doc = request_manipulator.manipulate_domtree(doc)
            if _doc:
                doc = _doc
        doc.write(tmp, encoding='UTF-8', xml_declaration=True, pretty_print=pretty)
        return tmp.getvalue()

    def mk_soap_message(
        self,
        header_info: HeaderInformationBlock,
        payload: MessageType,
        ns_list: Iterable[PrefixNamespace] | None = None,
        use_defaults: bool = True,
    ) -> CreatedMessage:
        """Create a soap message for the given payload."""
        nsh = self.ns_hlp
        ns_set = {nsh.S12, nsh.WSA, nsh.MSG, nsh.PM} if use_defaults else set()  # default
        ns_set.update(payload.additional_namespaces)
        if ns_list:
            ns_set.update(ns_list)
        my_ns_map = nsh.partial_map(*ns_set)
        soap_envelope = Soap12Envelope(my_ns_map)
        soap_envelope.set_header_info_block(header_info)
        soap_envelope.payload_element = payload.as_etree_node(payload.NODETYPE, my_ns_map)
        return CreatedMessage(soap_envelope, self)

    def mk_soap_message_etree_payload(
        self, header_info: HeaderInformationBlock, payload_element: xml_utils.LxmlElement | None = None
    ) -> CreatedMessage:
        """Create a soap message with a raw etree payload element."""
        nsh = self.ns_hlp
        my_ns_map = nsh.partial_map(nsh.S12, nsh.WSE, nsh.WSA)
        soap_envelope = Soap12Envelope(my_ns_map)
        soap_envelope.set_header_info_block(header_info)
        soap_envelope.payload_element = payload_element
        return CreatedMessage(soap_envelope, self)

    def mk_reply_soap_message(
        self, request: RequestData, response_payload: MessageType, ns_map: Iterable[PrefixNamespace] | None = None
    ) -> CreatedMessage:
        """Create a soap reply message for the given request."""
        nsh = self.ns_hlp
        ns_set = {nsh.S12, nsh.WSA, nsh.MSG, nsh.PM}  # default
        ns_set.update(response_payload.additional_namespaces)
        if ns_map:
            ns_set.update(ns_map)
        my_ns_map = nsh.partial_map(*ns_set)
        soap_envelope = Soap12Envelope(my_ns_map)
        reply_address = request.message_data.p_msg.header_info_block.mk_reply_header_block(
            action=response_payload.action
        )
        soap_envelope.set_header_info_block(reply_address)
        soap_envelope.payload_element = response_payload.as_etree_node(response_payload.NODETYPE, my_ns_map)
        return CreatedMessage(soap_envelope, self)

    def _validate_node(self, node: xml_utils.LxmlElement):
        if self._validate:
            validate_node(node, self._xml_schema, self._logger)
