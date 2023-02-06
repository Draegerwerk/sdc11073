from __future__ import annotations

import copy
import traceback
from collections import namedtuple
from dataclasses import dataclass, field
from decimal import Decimal
from typing import List, Dict, Union, Optional
from urllib.parse import urlparse, ParseResult

from lxml import etree as etree_

from sdc11073.namespaces import QN_TYPE, text_to_qname
from .soapenvelope import SoapFault, FaultCodeEnum, ReceivedSoapMessage
from .. import isoduration
from ..addressing import EndpointReferenceType, Address, ReferenceParameters
from ..httpserver.compression import CompressionHandler
from ..dpws import DeviceMetadataDialectURI, DeviceRelationshipTypeURI
from ..dpws import LocalizedStringTypeDict
from ..dpws import ThisDeviceType, ThisModelType, HostServiceType, HostedServiceType, Relationship
from ..exceptions import HTTPRequestHandlingError
from ..metadata import MetaData
from ..schema_resolver import SchemaResolver
from ..schema_resolver import mk_schema_validator

# pylint: disable=no-self-use

_LANGUAGE_ATTR = '{http://www.w3.org/XML/1998/namespace}lang'


@dataclass
class DescriptionModification:
    descriptors: list = field(default_factory=list)
    states: list = field(default_factory=list)


@dataclass
class DescriptionModifications:
    create: DescriptionModification = field(default_factory=DescriptionModification)
    update: DescriptionModification = field(default_factory=DescriptionModification)
    delete: DescriptionModification = field(default_factory=DescriptionModification)


def validate_node(node, xml_schema, logger):
    try:
        xml_schema.assertValid(node)
    except etree_.DocumentInvalid as ex:
        logger.error(traceback.format_exc())
        logger.error(etree_.tostring(node, pretty_print=True).decode('utf-8'))
        soap_fault = SoapFault(code=FaultCodeEnum.SENDER, reason=f'{ex}')
        raise HTTPRequestHandlingError(status=400,
                                       reason='document invalid',
                                       soap_fault=soap_fault) from ex


def _get_text(node, q_name):
    if node is None:
        return None
    tmp = node.find(q_name)
    if tmp is None:
        return None
    return tmp.text


class MdibStructureError(Exception):
    pass


OperationRequest = namedtuple('OperationRequest', 'operation_handle argument')
OperationResult = namedtuple('OperationResult', 'result soapEnvelope')

OperationReportResult = namedtuple('OperationReportResult', 'operation_report_parts soapEnvelope')

SubscriptionEndResult = namedtuple('SubscriptionEndResult', 'status_list reason_list reference_parameter_list')
LocalizedTextsRequest = namedtuple('LocalizedTextsRequest',
                                   'requested_handles requested_versions requested_langs text_widths number_of_lines')


@dataclass(frozen=True)
class SubscribeRequest:
    accepted_encodings: List[str]
    subscription_filters: List[str]
    notify_to_address: str
    notify_ref_params: ReferenceParameters
    end_to_address: Union[str, None]
    end_to_ref_params: Union[ReferenceParameters, None]
    mode: str
    expires: float
    any_nodes:Optional[List[etree_.Element]] = None
    any_attributes:Optional[Dict[str,str]] = None


@dataclass(frozen=True)
class SubscribeResult:
    subscription_manager_address: ParseResult
    reference_param: ReferenceParameters
    expire_seconds: float


@dataclass
class MdibVersionGroupReader:
    mdib_version: int
    sequence_id: str
    instance_id: Union[int, None]

    @classmethod
    def from_node(cls, node):
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
    """This class contains all data of a received Message"""
    msg_reader: MessageReader
    p_msg: Union[ReceivedSoapMessage, PayloadData]
    action: Union[str, None]
    q_name: etree_.QName
    mdib_version_group: MdibVersionGroupReader
    # action: Optional[str] = None
    # q_name: Optional[etree_.QName] = None
    # mdib_version_group: Optional[MdibVersionGroupReader] = None


class PayloadData:
    """Similar to ReceivedMessage, but it is only works with the body of the soap envelope, no addressing, action etc."""

    def __init__(self, xml_string):
        parser = etree_.ETCompatXMLParser(resolve_entities=False)
        try:
            self._doc_root = etree_.fromstring(xml_string, parser=parser)
        except Exception as ex:
            print(f'load error "{ex}" in "{xml_string}"')
            raise
        self.raw_data = xml_string
        self.msg_node = self._doc_root
        self.msg_name = etree_.QName(self.msg_node.tag)


class MessageReader:
    """ This class does all the conversions from DOM trees (body of SOAP messages) to MDIB objects."""

    def __init__(self, sdc_definitions, logger, log_prefix='', validate=True):
        self.sdc_definitions = sdc_definitions
        self.ns_hlp = sdc_definitions.data_model.ns_helper  # shortcut for easier access
        self.ns_map = self.ns_hlp.ns_map
        self._logger = logger
        self._log_prefix = log_prefix
        self._validate = validate
        self._xml_schema = mk_schema_validator(SchemaResolver(sdc_definitions))

    @property
    def _msg_names(self):
        return self.sdc_definitions.data_model.msg_names

    @property
    def _pm_names(self):
        return self.sdc_definitions.data_model.pm_names

    @property
    def _pm_types(self):
        return self.sdc_definitions.data_model.pm_types

    @property
    def _msg_types(self):
        return self.sdc_definitions.data_model.msg_types

    def get_descriptor_container_class(self, qname):
        return self.sdc_definitions.data_model.get_descriptor_container_class(qname)

    def get_state_container_class(self, qname):
        return self.sdc_definitions.data_model.get_state_container_class(qname)

    def read_received_message(self, xml_text: bytes, validate: bool = True) -> ReceivedMessage:
        """Reads complete message with addressing, message_id, payload,..."""
        parser = etree_.ETCompatXMLParser(resolve_entities=False)
        try:
            doc_root = etree_.fromstring(xml_text, parser=parser)
        except etree_.XMLSyntaxError as ex:
            self._logger.error('Error reading response ex={} xml={}', ex, xml_text.decode('utf-8'))
            raise
        if validate:
            self._validate_node(doc_root)

        message = ReceivedSoapMessage(xml_text, doc_root)
        if message.msg_node is not None and validate:
            self._validate_node(message.msg_node)
        message.address = self._mk_address_from_header(message.header_node)
        mdib_version_group = None
        if message.msg_node is not None:
            try:
                mdib_version_group = MdibVersionGroupReader.from_node(message.msg_node)
            except ValueError:
                mdib_version_group = None
        data = ReceivedMessage(self, message, message.address.action, message.msg_name, mdib_version_group)
        return data

    def read_payload_data(self, xml_text: bytes) -> ReceivedMessage:
        """ Read only payload part of a message"""
        payload = PayloadData(xml_text)
        action = None
        #q_name = payload.msg_name
        #data.msg_name = q_name.localname if q_name else None
        mdib_version_group = None
        if payload.msg_node is not None:
            mdib_version_group = MdibVersionGroupReader.from_node(payload.msg_node)
            self._validate_node(payload.msg_node)
        data = ReceivedMessage(self, payload, action, payload.msg_name, mdib_version_group)
        return data

    def read_get_mdib_response(self, received_message_data):
        descriptors = []
        states = []
        mdib_node = received_message_data.p_msg.msg_node[0]
        md_descr_node = mdib_node.find(self._pm_names.MdDescription)
        md_state_node = mdib_node.find(self._pm_names.MdState)
        if md_descr_node is not None:
            descriptors = self._read_md_description_node(md_descr_node)
        if md_state_node is not None:
            states = self._read_md_state_node(md_state_node)
        return descriptors, states

    def _read_md_description_node(self, md_description_node):
        descriptions = []

        def add_children(parent_node):
            p_handle = parent_node.get('Handle')
            for child_node in parent_node:
                if child_node.get('Handle') is not None:
                    container = self._mk_descriptor_container_from_node(child_node, p_handle)
                    descriptions.append(container)
                    add_children(child_node)

        # iterate over tree, collect all handles of vmds, channels and metric descriptors
        all_mds = md_description_node.findall(self._pm_names.Mds)
        for mds_node in all_mds:
            mds = self._mk_descriptor_container_from_node(mds_node, None)
            descriptions.append(mds)
            add_children(mds_node)
        return descriptions

    def _read_md_state_node(self, md_state_node):
        """
        Parses a GetMdStateResponse or the MdState part of GetMdibResponse
        :param md_state_node: A MdState node
        :return: a list of state containers
        """
        state_containers = []
        all_state_nodes = md_state_node.findall(self._pm_names.State)
        for state_node in all_state_nodes:
            try:
                state_containers.append(self._mk_state_container_from_node(state_node))
            except MdibStructureError as ex:
                self._logger.error('{}_read_md_state_node: cannot create: {}', self._log_prefix, ex)
        return state_containers

    def _mk_endpoint_reference(self, root_node):
        if root_node is None:
            return None
        ns_hlp = self.ns_hlp
        address_node = root_node.find(ns_hlp.wsaTag('Address'))
        address = address_node.text
        reference_parameters_node = root_node.find(ns_hlp.wsaTag('ReferenceParameters'))
        return EndpointReferenceType(address, reference_parameters_node)

    def _mk_address_from_header(self, root_node):
        ns_hlp = self.ns_hlp
        message_id = _get_text(root_node, ns_hlp.wsaTag('MessageID'))
        addr_to = _get_text(root_node, ns_hlp.wsaTag('To'))
        action = _get_text(root_node, ns_hlp.wsaTag('Action'))
        relates_to = _get_text(root_node, ns_hlp.wsaTag('RelatesTo'))

        relationship_type = None
        relates_to_node = root_node.find(ns_hlp.wsaTag('RelatesTo'))
        if relates_to_node is not None:
            relates_to = relates_to_node.text
            relationshiptype_text = relates_to_node.attrib.get('RelationshipType')
            if relationshiptype_text:
                # split into namespace, localname
                namespace, localname = relationshiptype_text.rsplit('/', 1)
                relationship_type = etree_.QName(namespace, localname)

        addr_from = self._mk_endpoint_reference(root_node.find(ns_hlp.wsaTag('From')))
        reply_to = self._mk_endpoint_reference(root_node.find(ns_hlp.wsaTag('ReplyTo')))
        fault_to = self._mk_endpoint_reference(root_node.find(ns_hlp.wsaTag('FaultTo')))

        reference_parameters_node = root_node.find(ns_hlp.wsaTag('ReferenceParameters'))
        if reference_parameters_node is None:
            reference_parameters = None
        else:
            reference_parameters = ReferenceParameters(reference_parameters_node[:])

        return Address(message_id=message_id,
                       addr_to=addr_to,
                       action=action,
                       relates_to=relates_to,
                       addr_from=addr_from,
                       reply_to=reply_to,
                       fault_to=fault_to,
                       reference_parameters=reference_parameters,
                       relationship_type=relationship_type)

    def _mk_descriptor_container_from_node(self, node, parent_handle):
        """
        :param node: a descriptor node
        :param parent_handle: the handle of the parent
        :return: a DescriptorContainer object representing the content of node
        """
        node_type = node.get(QN_TYPE)
        if node_type is not None:
            node_type = text_to_qname(node_type, node.nsmap)
        else:
            node_type = etree_.QName(node.tag)
        descr_cls = self.get_descriptor_container_class(node_type)
        return descr_cls.from_node(node, parent_handle)

    def _mk_state_container_from_node(self, node, forced_type=None):
        """
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
            raise ValueError(f'nody type {node_type} is not known')

        if node.tag != self._pm_names.State:
            node = copy.copy(node)  # make a copy, do not modify the original report
            node.tag = self._pm_names.State
        state = st_cls(descriptor_container)
        state.update_from_node(node)
        state.node = node
        return state

    def _mk_realtime_sample_array_states(self, node):
        return self._mk_state_container_from_node(node, self._pm_names.RealTimeSampleArrayMetricState)

    def _mk_statecontainers_from_reportpart(self, reportpart_node, state_qname):
        containers = []
        state_nodes = reportpart_node.findall(state_qname)
        for state_node in state_nodes:
            desc_h = state_node.get('DescriptorHandle')
            if desc_h is None:
                self._logger.error('{}_mk_statecontainers_from_reportpart: missing descriptor handle in {}!',
                                   self._log_prefix,
                                   lambda: etree_.tostring(state_node))  # pylint: disable=cell-var-from-loop
            else:
                containers.append(self._mk_state_container_from_node(state_node))
        return containers

    def _validate_node(self, node):
        if self._validate:
            validate_node(node, self._xml_schema, self._logger)


class MessageReaderClient(MessageReader):

    def read_get_mddescription_response(self, received_message_data):
        msg_node = received_message_data.p_msg.msg_node
        md_description_node = msg_node[0]
        descriptors = self._read_md_description_node(md_description_node)
        return descriptors

    def read_get_mdstate_response(self, message_data: ReceivedMessage):
        md_state_node = message_data.p_msg.msg_node[0]
        return self._read_md_state_node(md_state_node)

    def read_context_states(self, message_data: ReceivedMessage):
        """ Creates Context State Containers from message .
        :return: a list of state containers
        """
        states = []
        context_state_nodes = message_data.p_msg.msg_node[:]
        for context_state_node in context_state_nodes:
            # hard rename to dom:State
            context_state_node.tag = self._pm_names.State
            try:
                state_container = self._mk_state_container_from_node(context_state_node)
                states.append(state_container)
            except MdibStructureError as ex:
                self._logger.error('{}read_context_states: cannot create: {}', self._log_prefix, ex)
        return states

    def read_get_localized_text_response(self, message_data: ReceivedMessage) -> list:
        result = []
        response_node = message_data.p_msg.msg_node
        if response_node is not None:
            for element in response_node:
                l_text = self._pm_types.LocalizedText.from_node(element)
                result.append(l_text)
        return result

    def read_get_supported_languages_response(self, message_data) -> List[str]:
        result = []
        response_node = message_data.p_msg.msg_node
        if response_node is not None:
            for element in response_node:
                result.append(str(element.text))
        return result

    def read_waveform_report(self, message_data):
        states = []
        report_node = message_data.p_msg.msg_node
        all_sample_arrays = list(report_node)
        for sample_array in all_sample_arrays:
            if sample_array.tag.endswith('State'):  # ignore everything else, e.g. Extension
                states.append(self._mk_realtime_sample_array_states(sample_array))
        return states

    def read_periodic_metric_report(self, message_data):
        return self._read_metric_report(message_data.p_msg.msg_node)

    def read_episodic_metric_report(self, message_data):
        return self._read_metric_report(message_data.p_msg.msg_node)

    def _read_metric_report(self, report_node):
        """
        Parses an episodic or periodic metric report
        :param report_node:  An episodic metric report etree
        :return: a list of StateContainer objects
        """
        states = []
        for reportpart_node in report_node:
            states.extend(self._mk_statecontainers_from_reportpart(reportpart_node, self._msg_names.MetricState))
        return states

    def read_episodic_alert_report(self, message_data):
        return self._read_alert_report_node(message_data.p_msg.msg_node)

    def read_periodic_alert_report(self, message_data):
        return self._read_alert_report_node(message_data.p_msg.msg_node)

    def _read_alert_report_node(self, report_node):
        """
        Parses an episodic alert report
        :param report_node:  An episodic alert report etree
        :return: a list of StateContainer objects
        """
        states = []
        for reportpart_node in report_node:
            states.extend(self._mk_statecontainers_from_reportpart(reportpart_node, self._msg_names.AlertState))
        return states

    def read_operational_state_report(self, message_data):
        """
        Parses an operational state report
        :param message_data:
        :return: a list of StateContainer objects
        """
        states = []
        for report_part_node in message_data.p_msg.msg_node:
            states.extend(self._mk_statecontainers_from_reportpart(report_part_node, self._msg_names.OperationState))

        return states

    def read_episodic_context_report(self, message_data):
        """
        Parses an episodic context report
        :param message_data:
        :return: a list of StateContainer objects
        """
        states = []
        for report_part_node in message_data.p_msg.msg_node:  # reportpart_nodes:
            states.extend(self._mk_statecontainers_from_reportpart(report_part_node, self._msg_names.ContextState))
        return states

    def read_periodic_component_report(self, message_data):
        return self._read_component_report(message_data.p_msg.msg_node)

    def read_episodic_component_report(self, message_data):
        return self._read_component_report(message_data.p_msg.msg_node)

    def _read_component_report(self, report_node):
        """
        Parses an episodic component report
        :param report_node:  An episodic component report etree
        :return: a list of StateContainer objects
        """
        states = []
        for report_part_node in report_node:
            states.extend(self._mk_statecontainers_from_reportpart(report_part_node, self._msg_names.ComponentState))
        return states

    def read_description_modification_report(self, message_data: ReceivedMessage) -> DescriptionModifications:
        """
        Parses a description modification report
        :param message_data:  MessageData instance
        :return: a list of DescriptorContainer objects
        """
        DescriptionModificationType = self._msg_types.DescriptionModificationType
        descriptors_list = []
        report_parts = list(message_data.p_msg.msg_node)  # list of msg:ReportPart nodes
        descriptors = DescriptionModifications()
        for report_part in report_parts:
            descriptors_list.append(descriptors)
            parent_descriptor = report_part.get('ParentDescriptor')
            modification_type = report_part.get('ModificationType',
                                                DescriptionModificationType.UPDATE)  # implied Value is 'Upt'
            if modification_type == DescriptionModificationType.CREATE:
                description_modification = descriptors.create
            elif modification_type == DescriptionModificationType.UPDATE:
                description_modification = descriptors.update
            elif modification_type == DescriptionModificationType.DELETE:
                description_modification = descriptors.delete
            else:
                raise ValueError(f'unknown modification type {modification_type} in description modification report')
            descriptor_nodes = report_part.findall(self._msg_names.Descriptor)
            for descriptor_node in descriptor_nodes:
                descr_container = self._mk_descriptor_container_from_node(descriptor_node, parent_descriptor)
                description_modification.descriptors.append(descr_container)
            state_nodes = report_part.findall(self._msg_names.State)
            for state_node in state_nodes:
                state_container = self._mk_state_container_from_node(state_node)
                description_modification.states.append(state_container)
        return descriptors

    def read_operation_response(self, message_data: ReceivedMessage) -> OperationResult:
        msg_node = message_data.p_msg.msg_node
        abstract_set_response = self._msg_types.AbstractSetResponse.from_node(msg_node)
        return OperationResult(abstract_set_response, message_data.p_msg)

    def _read_invocation_info(self, invocation_info_node: etree_._Element):
        return self._msg_types.InvocationInfo.from_node(invocation_info_node)

    def read_operation_invoked_report(self, message_data: ReceivedMessage) -> OperationReportResult:
        msg_node = message_data.p_msg.msg_node
        report_part_nodes = msg_node.findall(self._msg_names.ReportPart)
        report_parts = []
        for node in report_part_nodes:
            report_parts.append(self._msg_types.OperationInvokedReportPart.from_node(node))
        return OperationReportResult(report_parts, message_data.p_msg)

    def read_subscribe_response(self, message_data: ReceivedMessage) -> SubscribeResult:
        msg_node = message_data.p_msg.msg_node
        ns = {'wse': self.ns_hlp.WSE.namespace, 'wsa': self.ns_hlp.WSA.namespace}
        address = msg_node.xpath('wse:SubscriptionManager/wsa:Address/text()', namespaces=ns)
        reference_params = msg_node.xpath('wse:SubscriptionManager/wsa:ReferenceParameters',
                                          namespaces=ns)
        reference_param = None if len(reference_params) == 0 else reference_params[0]
        expires = msg_node.xpath('wse:Expires/text()', namespaces=ns)

        subscription_manager_address = urlparse(address[0])
        expire_seconds = isoduration.parse_duration(expires[0])
        return SubscribeResult(subscription_manager_address, ReferenceParameters(reference_param), expire_seconds)

    def read_renew_response(self, message_data: ReceivedMessage) -> [float, None]:
        ns = {'wse': self.ns_hlp.WSE.namespace, 'wsa': self.ns_hlp.WSA.namespace}
        expires = message_data.p_msg.body_node.xpath('wse:RenewResponse/wse:Expires/text()',
                                                     namespaces=ns)
        if len(expires) == 0:
            return None
        expire_seconds = isoduration.parse_duration(expires[0])
        return expire_seconds

    def read_get_status_response(self, message_data: ReceivedMessage) -> [float, None]:
        ns = {'wse': self.ns_hlp.WSE.namespace, 'wsa': self.ns_hlp.WSA.namespace}
        expires = message_data.p_msg.body_node.xpath('wse:GetStatusResponse/wse:Expires/text()',
                                                     namespaces=ns)
        if len(expires) == 0:
            return None
        expire_seconds = isoduration.parse_duration(expires[0])
        return expire_seconds

    def read_subscription_end_message(self, message_data: ReceivedMessage) -> SubscriptionEndResult:
        ns = {'wse': self.ns_hlp.WSE.namespace, 'wsa': self.ns_hlp.WSA.namespace}
        body_node = message_data.p_msg.body_node
        status_list = body_node.xpath('wse:SubscriptionEnd/wse:Status/text()', namespaces=ns)
        reason_list = body_node.xpath('wse:SubscriptionEnd/wse:Reason/text()', namespaces=ns)
        reference_parameters = message_data.p_msg.address.reference_parameters
        return SubscriptionEndResult(status_list, reason_list, reference_parameters)

    @staticmethod
    def read_wsdl(wsdl_string: str) -> etree_.ElementTree:
        """ make am ElementTree instance"""
        return etree_.fromstring(wsdl_string, parser=etree_.ETCompatXMLParser(resolve_entities=False))

    def read_get_metadata_response(self, message_data: ReceivedMessage) -> MetaData:
        meta_data = MetaData()
        body_node = message_data.p_msg.body_node
        metadata_node = body_node.find(self.ns_hlp.wsxTag('Metadata'))
        if metadata_node is not None:
            section_nodes = metadata_node.findall(self.ns_hlp.wsxTag('MetadataSection'))
            for metadata_section_node in section_nodes:
                dialect = metadata_section_node.attrib['Dialect']
                if dialect[-1] == '/':
                    dialect = dialect[:-1]
                if dialect == "http://schemas.xmlsoap.org/wsdl":
                    location_node = metadata_section_node.find(self.ns_hlp.wsxTag('Location'))
                    meta_data.wsdl_location = location_node.text
                elif dialect == DeviceMetadataDialectURI.THIS_MODEL:
                    this_model_node = metadata_section_node.find(self.ns_hlp.dpwsTag('ThisModel'))
                    meta_data.this_model = self._mk_this_model(this_model_node)
                elif dialect == DeviceMetadataDialectURI.THIS_DEVICE:
                    this_device_node = metadata_section_node.find(self.ns_hlp.dpwsTag('ThisDevice'))
                    meta_data.this_device = self._mk_this_device(this_device_node)
                elif dialect == DeviceMetadataDialectURI.RELATIONSHIP:
                    relationship_node = metadata_section_node.find(self.ns_hlp.dpwsTag('Relationship'))
                    if relationship_node.get('Type') == DeviceRelationshipTypeURI.HOST:
                        meta_data.relationship = Relationship()
                        host_node = relationship_node.find(self.ns_hlp.dpwsTag('Host'))
                        meta_data.relationship.host = self._mk_host(host_node)
                        hosted_nodes = relationship_node.findall(self.ns_hlp.dpwsTag('Hosted'))
                        for hosted_node in hosted_nodes:
                            hosted = self._mk_hosted(hosted_node)
                            meta_data.relationship.hosted[hosted.service_id] = hosted
        return meta_data

    def read_fault_message(self, message_data: ReceivedMessage) -> SoapFault:
        body_node = message_data.p_msg.body_node
        ns = {'s12': self.ns_hlp.S12.namespace}
        code = ', '.join(body_node.xpath('s12:Fault/s12:Code/s12:Value/text()', namespaces=ns))
        sub_code = ', '.join(body_node.xpath('s12:Fault/s12:Code/s12:Subcode/s12:Value/text()',
                                             namespaces=ns))
        reason = ', '.join(body_node.xpath('s12:Fault/s12:Reason/s12:Text/text()',
                                           namespaces=ns))
        detail = ', '.join(body_node.xpath('s12:Fault/s12:Detail/text()', namespaces=ns))

        return SoapFault(code, reason, sub_code, detail)

    def _mk_this_device(self, root_node) -> ThisDeviceType:
        friendly_name = LocalizedStringTypeDict()
        fname_nodes = root_node.findall(self.ns_hlp.dpwsTag('FriendlyName'))
        for f_name in fname_nodes:
            friendly_name.add_localized_string(f_name.text, f_name.get(_LANGUAGE_ATTR))
        firmware_version = _get_text(root_node, self.ns_hlp.dpwsTag('FirmwareVersion'))
        serial_number = _get_text(root_node, self.ns_hlp.dpwsTag('SerialNumber'))
        return ThisDeviceType(friendly_name, firmware_version, serial_number)

    def _mk_this_model(self, root_node) -> ThisModelType:
        manufacturer = LocalizedStringTypeDict()
        manufact_nodes = root_node.findall(self.ns_hlp.dpwsTag('Manufacturer'))
        for manufact_node in manufact_nodes:
            manufacturer.add_localized_string(manufact_node.text, manufact_node.get(_LANGUAGE_ATTR))
        manufacturer_url = _get_text(root_node, self.ns_hlp.dpwsTag('ManufacturerUrl'))
        model_name = LocalizedStringTypeDict()
        model_name_nodes = root_node.findall(self.ns_hlp.dpwsTag('ModelName'))
        for model_name_node in model_name_nodes:
            model_name.add_localized_string(model_name_node.text, model_name_node.get(_LANGUAGE_ATTR))
        model_number = _get_text(root_node, self.ns_hlp.dpwsTag('ModelNumber'))
        model_url = _get_text(root_node, self.ns_hlp.dpwsTag('ModelUrl'))
        presentation_url = _get_text(root_node, self.ns_hlp.dpwsTag('PresentationUrl'))
        return ThisModelType(manufacturer, manufacturer_url, model_name, model_number, model_url, presentation_url)

    def _mk_host(self, root_node) -> HostServiceType:
        endpoint_reference = root_node.find(self.ns_hlp.wsaTag('EndpointReference'))
        types = _get_text(root_node, self.ns_hlp.dpwsTag('Types'))
        if types:
            types = types.split()
        return HostServiceType(endpoint_reference, types)

    def _mk_hosted(self, root_node) -> HostedServiceType:
        endpoint_references = []
        epr_nodes = root_node.findall(self.ns_hlp.wsaTag('EndpointReference'))
        for epr_node in epr_nodes:
            endpoint_references.append(self._mk_endpoint_reference(epr_node))
        types = _get_text(root_node, self.ns_hlp.dpwsTag('Types'))
        if types:
            types = types.split()
        service_id = _get_text(root_node, self.ns_hlp.dpwsTag('ServiceId'), )
        return HostedServiceType(endpoint_references, types, service_id)


class MessageReaderDevice(MessageReader):
    """Contains methods that are only used by device"""

    def read_subscribe_request(self, request_data) -> SubscribeRequest:
        envelope = request_data.message_data.p_msg
        accepted_encodings = CompressionHandler.parse_header(request_data.http_header.get('Accept-Encoding'))
        ns = {'wse': self.ns_hlp.WSE.namespace, 'wsa': self.ns_hlp.WSA.namespace}

        subscribe_node = envelope.body_node.find(self.ns_hlp.wseTag('Subscribe'))
        subscribe_node_children = subscribe_node[:]
        index = 0
        # read optional EndTo element
        end_to_ref = None
        end_to_address = None
        if subscribe_node_children[index].tag == self.ns_hlp.wseTag('EndTo'):
            end_to_node = subscribe_node_children[index]
            end_to_address = end_to_node.find(self.ns_hlp.wsaTag('Address')).text
            end_to_ref_node = end_to_node.find(self.ns_hlp.wsaTag('ReferenceParameters'))
            if end_to_ref_node is not None:
                end_to_ref = ReferenceParameters(end_to_ref_node[:])
            else:
                end_to_ref = ReferenceParameters(None)
            index +=1

        # read Delivery element
        delivery_node = subscribe_node_children[index]
        if delivery_node.tag != self.ns_hlp.wseTag('Delivery'):
            raise Exception
        notify_to_node = delivery_node.find(self.ns_hlp.wseTag('NotifyTo'))
        notify_to_address = notify_to_node.xpath('wsa:Address/text()', namespaces=ns)[0]
        notify_ref_node = notify_to_node.find(self.ns_hlp.wsaTag('ReferenceParameters'))
        if notify_ref_node is not None:
            notify_ref = ReferenceParameters(notify_ref_node[:])
        else:
            notify_ref = ReferenceParameters(None)
        mode = delivery_node.get('Mode')  # mandatory attribute
        index += 1

        # read optional Expires element
        expires = None
        if subscribe_node_children[index].tag == self.ns_hlp.wseTag('Expires'):
            expires_node = subscribe_node_children[index]
            expires = isoduration.parse_duration(str(expires_node.text))
            index += 1

        # read optional Filter element
        subscription_filters = None
        if subscribe_node_children[index].tag == self.ns_hlp.wseTag('Filter'):
            subscription_filter_node = subscribe_node_children[index]
            dialect = subscription_filter_node.get('Dialect')
            if dialect == f'{self.ns_hlp.DPWS.namespace}/Action':
                subscription_filters = subscription_filter_node.text.split()
            index += 1

        # remaining "any" nodes
        any_nodes = subscribe_node_children[index:]
        any_attributes = subscribe_node.attrib
        return SubscribeRequest(accepted_encodings, subscription_filters, str(notify_to_address), notify_ref,
                                str(end_to_address), end_to_ref, mode, expires, any_nodes, any_attributes)

    def read_renew_request(self, message_data):
        ns = {'wse': self.ns_hlp.WSE.namespace}
        expires = message_data.p_msg.body_node.xpath('wse:Renew/wse:Expires/text()', namespaces=ns)
        if len(expires) == 0:
            return None
        return isoduration.parse_duration(str(expires[0]))

    @staticmethod
    def read_header_reference_parameters(message_data: ReceivedMessage) -> ReferenceParameters:
        reference_parameter_nodes = []
        for header_element in message_data.p_msg.header_node:
            is_reference_parameter = header_element.attrib.get('IsReferenceParameter', 'false')
            if is_reference_parameter.lower() == 'true':
                reference_parameter_nodes.append(header_element)
        return ReferenceParameters(reference_parameter_nodes)

    def read_getmddescription_request(self, message_data: ReceivedMessage) -> List[str]:
        ns = {'msg': self.ns_hlp.MSG.namespace}
        return message_data.p_msg.body_node.xpath('*/msg:HandleRef/text()', namespaces=ns)

    def read_getmdstate_request(self, message_data: ReceivedMessage) -> List[str]:
        ns = {'msg': self.ns_hlp.MSG.namespace}
        return message_data.p_msg.body_node.xpath('*/msg:HandleRef/text()', namespaces=ns)

    def _operation_handle(self, message_data):
        ns = {'msg': self.ns_hlp.MSG.namespace}
        operation_handle_refs = message_data.p_msg.body_node.xpath('*/msg:OperationHandleRef/text()',
                                                                   namespaces=ns)
        return operation_handle_refs[0]

    def read_activate_request(self, message_data: ReceivedMessage) -> OperationRequest:
        ns = {'msg': self.ns_hlp.MSG.namespace}
        argument_strings = message_data.p_msg.body_node.xpath('*/msg:Argument/msg:ArgValue/text()',
                                                              namespaces=ns)
        return OperationRequest(self._operation_handle(message_data), argument_strings)

    def convert_activate_arguments(self, operation_descriptor, operation_request):
        # ToDo: check type of each argument an convert string to corresponding python type
        return operation_request

    def read_set_value_request(self, message_data: ReceivedMessage) -> OperationRequest:
        ns = {'msg': self.ns_hlp.MSG.namespace}
        value_nodes = message_data.p_msg.body_node.xpath('*/msg:RequestedNumericValue',
                                                         namespaces=ns)
        if value_nodes:
            argument = Decimal(value_nodes[0].text)
        else:
            argument = None
        return OperationRequest(self._operation_handle(message_data), argument)

    def read_set_string_request(self, message_data: ReceivedMessage) -> OperationRequest:
        ns = {'msg': self.ns_hlp.MSG.namespace}
        string_node = message_data.p_msg.body_node.xpath('*/msg:RequestedStringValue',
                                                         namespaces=ns)
        if string_node:
            argument = str(string_node[0].text)
        else:
            argument = None
        return OperationRequest(self._operation_handle(message_data), argument)

    def read_set_metric_state_request(self, message_data: ReceivedMessage) -> OperationRequest:
        ns = {'msg': self.ns_hlp.MSG.namespace}
        proposed_state_nodes = message_data.p_msg.body_node.xpath('*/msg:ProposedMetricState',
                                                                  namespaces=ns)
        proposed_states = [self._mk_state_container_from_node(m) for m in proposed_state_nodes]
        return OperationRequest(self._operation_handle(message_data), proposed_states)

    def read_set_alert_state_request(self, message_data: ReceivedMessage) -> OperationRequest:
        ns = {'msg': self.ns_hlp.MSG.namespace}
        proposed_state_nodes = message_data.p_msg.body_node.xpath('*/msg:ProposedAlertState',
                                                                  namespaces=ns)
        if len(proposed_state_nodes) > 1:  # schema allows exactly one ProposedAlertState:
            raise ValueError(
                f'only one ProposedAlertState argument allowed, found {len(proposed_state_nodes)}')
        if len(proposed_state_nodes) == 0:
            raise ValueError('no ProposedAlertState argument found')
        proposed_states = [self._mk_state_container_from_node(m) for m in proposed_state_nodes]
        return OperationRequest(self._operation_handle(message_data), proposed_states)

    def read_set_component_state_request(self, message_data: ReceivedMessage) -> OperationRequest:
        ns = {'msg': self.ns_hlp.MSG.namespace}
        proposed_state_nodes = message_data.p_msg.body_node.xpath('*/msg:ProposedComponentState',
                                                                  namespaces=ns)
        proposed_states = [self._mk_state_container_from_node(m) for m in proposed_state_nodes]
        return OperationRequest(self._operation_handle(message_data), proposed_states)

    def read_get_context_states_request(self, message_data: ReceivedMessage) -> List[str]:
        ns = {'msg': self.ns_hlp.MSG.namespace}
        requested_handles = message_data.p_msg.body_node.xpath('*/msg:HandleRef/text()', namespaces=ns)
        return requested_handles

    def read_set_context_state_request(self, message_data: ReceivedMessage) -> OperationRequest:
        ns = {'msg': self.ns_hlp.MSG.namespace}
        proposed_state_nodes = message_data.p_msg.body_node.xpath('*/msg:ProposedContextState',
                                                                  namespaces=ns)
        proposed_states = [self._mk_state_container_from_node(m) for m in proposed_state_nodes]
        return OperationRequest(self._operation_handle(message_data), proposed_states)

    def read_get_localized_text_request(self, message_data: ReceivedMessage) -> LocalizedTextsRequest:
        ns = {'msg': self.ns_hlp.MSG.namespace}
        body_node = message_data.p_msg.body_node
        requested_handles = body_node.xpath('*/msg:Ref/text()',
                                            namespaces=ns)  # handle strings 0...n
        requested_versions = body_node.xpath('*/msg:Version/text()',
                                             namespaces=ns)  # unsigned long int 0..1
        requested_langs = body_node.xpath('*/msg:Lang/text()',
                                          namespaces=ns)  # unsigned long int 0..n
        text_widths = body_node.xpath('*/msg:TextWidth/text()',
                                      namespaces=ns)  # strings 0..n
        number_of_lines = body_node.xpath('*/msg:NumberOfLines/text()',
                                          namespaces=ns)  # int 0..n
        return LocalizedTextsRequest(requested_handles, requested_versions, requested_langs, text_widths,
                                     number_of_lines)
