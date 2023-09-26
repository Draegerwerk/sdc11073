from __future__ import annotations

import copy
import traceback
from collections import namedtuple
from dataclasses import dataclass
from io import BytesIO
from typing import TYPE_CHECKING

from lxml import etree as etree_

from sdc11073.exceptions import ValidationError
from sdc11073.namespaces import QN_TYPE, default_ns_helper, text_to_qname
from sdc11073.schema_resolver import mk_schema_validator
from sdc11073.xml_types.addressing_types import HeaderInformationBlock

from .soapenvelope import Fault, ReceivedSoapMessage, faultcodeEnum

if TYPE_CHECKING:
    from types import ModuleType

    from sdc11073.definitions_base import BaseDefinitions
    from sdc11073.loghelper import LoggerAdapter
    from sdc11073.mdib.descriptorcontainers import AbstractDescriptorProtocol
    from sdc11073.mdib.statecontainers import AbstractStateProtocol
    from sdc11073.namespaces import PrefixNamespace
    from sdc11073 import xml_utils


def validate_node(node: xml_utils.LxmlElement, xml_schema: etree_.XMLSchema, logger: LoggerAdapter):
    """Let xml_schema instance validate the node."""
    try:
        xml_schema.assertValid(node)
    except etree_.DocumentInvalid as ex:
        logger.warning(traceback.format_exc())
        logger.warning(etree_.tostring(node, pretty_print=True).decode('utf-8'))
        fault = Fault()
        fault.Code.Value = faultcodeEnum.SENDER
        fault.set_sub_code(default_ns_helper.WSE.tag('InvalidMessage'))
        fault.add_reason_text(f'validation error: {ex}')

        raise ValidationError(reason='document invalid', soap_fault=fault) from ex


def _get_text(node: xml_utils.LxmlElement, q_name: etree_.QName) -> str | None:
    if node is None:
        return None
    tmp = node.find(q_name)
    if tmp is None:
        return None
    return tmp.text


OperationRequest = namedtuple('OperationRequest', 'operation_handle argument')

SubscriptionEndResult = namedtuple('SubscriptionEndResult', 'status_list reason_list reference_parameter_list')


@dataclass
class MdibVersionGroupReader:
    """Groups the attributes that identify a mdib version."""

    mdib_version: int
    sequence_id: str
    instance_id: int | None

    @classmethod
    def from_node(cls, node: xml_utils.LxmlElement) -> MdibVersionGroupReader:
        """Construct from a node with version attributes."""
        mdib_version = int(node.get('MdibVersion', '0'))
        sequence_id = node.get('SequenceId')
        instance_id = node.get('InstanceId')
        if instance_id is not None:
            instance_id = int(instance_id)
        if sequence_id is None:
            raise ValueError('missing mandatory SequenceId attribute')
        return cls(mdib_version, sequence_id, instance_id)


@dataclass(frozen=True)
class ReceivedMessage:
    """ReceivedMessage contains all data of a received Message."""

    msg_reader: MessageReader
    p_msg: ReceivedSoapMessage
    action: str | None
    q_name: etree_.QName
    mdib_version_group: MdibVersionGroupReader



class MessageReader:
    """MessageReader does all the conversions from DOM trees (body of SOAP messages) to MDIB objects."""

    def __init__(self, sdc_definitions: type[BaseDefinitions],
                 additional_schema_specs: list[PrefixNamespace] | None,
                 logger: LoggerAdapter,
                 validate: bool = True):
        self.schema_specs = [entry.value for entry in sdc_definitions.data_model.ns_helper.prefix_enum]
        if additional_schema_specs is not None:
            self.schema_specs.extend(additional_schema_specs)
        self._logger = logger
        self._data_model = sdc_definitions.data_model
        self.ns_hlp = sdc_definitions.data_model.ns_helper
        self._validate = validate
        self._xml_schema: etree_.XMLSchema = mk_schema_validator(self.schema_specs, self.ns_hlp)

    @property
    def msg_names(self) -> ModuleType:
        """Return a module with all qualified names of the BICEPS message model."""
        return self._data_model.msg_names

    @property
    def pm_names(self) -> ModuleType:
        """Return a module with all qualified names of the BICEPS participant model."""
        return self._data_model.pm_names

    @property
    def pm_types(self) -> ModuleType:
        """Return a module with participant model types."""
        return self._data_model.pm_types

    @property
    def msg_types(self) -> ModuleType:
        """Return a module with message model types."""
        return self._data_model.msg_types

    def get_descriptor_container_class(self, qname: etree_.QName) -> type[AbstractDescriptorProtocol]:
        """Get the class that represents a BICEPS descriptor entity with given QName."""
        return self._data_model.get_descriptor_container_class(qname)

    def get_state_container_class(self, qname: etree_.QName) -> type[AbstractStateProtocol]:
        """Return the class that represents a BICEPS state entity with given QName."""
        return self._data_model.get_state_container_class(qname)

    def read_received_message(self, xml_text: bytes, validate: bool = True) -> ReceivedMessage:
        """Read complete message with addressing, message_id, payload,..."""
        parser = etree_.ETCompatXMLParser(resolve_entities=False)
        try:
            doc_root = etree_.fromstring(xml_text, parser=parser)
        except etree_.XMLSyntaxError as ex:
            self._logger.warning('Error reading response ex=%r xml=%s', ex, xml_text.decode('utf-8'))
            raise
        if validate:
            self._validate_node(doc_root)

        message = ReceivedSoapMessage(xml_text, doc_root)
        if message.msg_node is not None and validate:
            self._validate_node(message.msg_node)
        message.header_info_block = HeaderInformationBlock.from_node(message.header_node)

        mdib_version_group = None
        if message.msg_node is not None:
            try:
                mdib_version_group = MdibVersionGroupReader.from_node(message.msg_node)
            except ValueError:
                mdib_version_group = None
        return ReceivedMessage(self, message, message.header_info_block.Action,
                               message.msg_name, mdib_version_group)

    def read_get_mdib_response(self, received_message_data: ReceivedMessage) -> tuple[
    list[AbstractDescriptorProtocol], list[AbstractStateProtocol]]:
        """Return list of all descriptors and states in mdib of received message."""
        mdib_node = received_message_data.p_msg.msg_node[0]
        return self.read_get_mdib_payload(mdib_node)

    def read_get_mdib_payload(self, mdib_node: xml_utils.LxmlElement) -> tuple[
    list[AbstractDescriptorProtocol], list[AbstractStateProtocol]]:
        """Return list of all descriptors and states in mdib."""
        descriptors = []
        states = []
        md_descr_node = mdib_node.find(self.pm_names.MdDescription)
        md_state_node = mdib_node.find(self.pm_names.MdState)
        if md_descr_node is not None:
            descriptors = self._read_md_description_node(md_descr_node)
        if md_state_node is not None:
            states = self._read_md_state_node(md_state_node)
        return descriptors, states

    def read_mdib_xml(self, xml_text: bytes) -> tuple[list[AbstractDescriptorProtocol], list[AbstractStateProtocol]]:
        """Return list of all descriptors and states in mdib."""
        payload = self.read_xml_text(xml_text)
        q_name = etree_.QName(payload.tag)
        if q_name == self.msg_names.GetMdibResponse:
            return self.read_get_mdib_payload(payload[0])
        return self.read_get_mdib_payload(payload)

    def read_xml_text(self, xml_text: bytes) -> xml_utils.LxmlElement:
        """Parse imput, return a node."""
        parser = etree_.ETCompatXMLParser(resolve_entities=False)
        try:
            node = etree_.fromstring(xml_text, parser=parser)
        except Exception as ex:
            print(f'load error "{ex}" in "{xml_text}"')
            raise
        self._validate_node(node)
        return node

    def _read_md_description_node(self, md_description_node: xml_utils.LxmlElement) -> list[AbstractDescriptorProtocol]:
        descriptions = []

        def add_children(parent_node: xml_utils.LxmlElement):
            p_handle = parent_node.get('Handle')
            for child_node in parent_node:
                if child_node.get('Handle') is not None:
                    container = self._mk_descriptor_container_from_node(child_node, p_handle)
                    descriptions.append(container)
                    add_children(child_node)

        # iterate over tree, collect all handles of vmds, channels and metric descriptors
        all_mds = md_description_node.findall(self.pm_names.Mds)
        for mds_node in all_mds:
            mds = self._mk_descriptor_container_from_node(mds_node, None)
            descriptions.append(mds)
            add_children(mds_node)
        return descriptions

    def _read_md_state_node(self, md_state_node: xml_utils.LxmlElement) -> list[AbstractStateProtocol]:
        """Parse a GetMdStateResponse or the MdState part of GetMdibResponse."""
        state_containers = []
        all_state_nodes = md_state_node.findall(self.pm_names.State)
        for state_node in all_state_nodes:
            state_containers.append(self._mk_state_container_from_node(state_node))
        return state_containers

    def _mk_descriptor_container_from_node(self, node: xml_utils.LxmlElement,
                                           parent_handle: str | None) -> AbstractDescriptorProtocol:
        node_type = node.get(QN_TYPE)
        node_type = text_to_qname(node_type, node.nsmap) if node_type is not None else etree_.QName(node.tag)
        descr_cls = self.get_descriptor_container_class(node_type)
        return descr_cls.from_node(node, parent_handle)

    def _mk_state_container_from_node(self, node: xml_utils.LxmlElement,
                                      forced_type: etree_.QName | None = None) -> AbstractStateProtocol:
        """Create a state container from a node.

        :param node: an etree node
        :param forced_type: if given, the QName that shall be used for class instantiation instead of the data in node
        """
        if forced_type is not None:
            node_type = forced_type
        else:
            node_type = node.get(QN_TYPE)
            if node_type is not None:
                node_type = text_to_qname(node_type, node.nsmap)

        descriptor_container = None
        st_cls = self.get_state_container_class(node_type)
        if st_cls is None:
            raise ValueError(f'body type {node_type} is not known')

        if node.tag != self.pm_names.State:
            node = copy.copy(node)  # make a copy, do not modify the original report
            node.tag = self.pm_names.State
        state = st_cls(descriptor_container)
        state.update_from_node(node)
        state.node = node
        return state

    def _validate_node(self, node: xml_utils.LxmlElement):
        if self._validate:
            validate_node(node, self._xml_schema, self._logger)

    @staticmethod
    def read_wsdl(wsdl_text: bytes) -> etree_.ElementTree:
        """Make am ElementTree instance."""
        return etree_.parse(BytesIO(wsdl_text), parser=etree_.ETCompatXMLParser(resolve_entities=False))
