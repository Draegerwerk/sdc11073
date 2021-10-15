import copy
import urllib
from abc import abstractmethod, ABC
from collections import namedtuple
from typing import List

from lxml import etree as etree_

from sdc11073 import namespaces
from sdc11073 import pmtypes
from .soapenvelope import ReceivedSoap12Envelope
from .. import isoduration
from ..compression import CompressionHandler


class MdibStructureError(Exception):
    pass


OperationRequest = namedtuple('OperationRequest', 'operation_handle argument')
OperationResult = namedtuple('OperationResult', 'transaction_id invocation_state error errorMsg soapEnvelope')
SubscriptionEndResult = namedtuple('SubscriptionEndResult', 'status_list reason_list reference_parameter_list')
LocalizedTextsRequest = namedtuple('LocalizedTextsRequest',
                                   'requested_handles requested_versions requested_langs text_widths number_of_lines')
SubscribeRequest = namedtuple('SubscribeRequest',
                              'accepted_encodings subscription_filters notify_to_address notify_ref_node end_to_address end_to_ref_node mode expires')


class ReceivedMessageData:
    """This class contains all data of a received Message"""

    def __init__(self, reader_instance, parsed_message):
        self.msg_reader = reader_instance  #
        self.p_msg = parsed_message  # parsed message, e g. a Soap12Envelope
        self.instance_id = None  # a number
        self.sequence_id = None  # a string
        self.mdib_version = None # a number
        self.action = None
        self.msg_name = None


class AbstractMessageReader(ABC):

    def __init__(self, sdc_definitions, logger, log_prefix=''):
        self.sdc_definitions = sdc_definitions
        self._logger = logger
        self._log_prefix = log_prefix

    @abstractmethod
    def read_received_message(self, xml_text: str, parser_cls=None) -> ReceivedMessageData:
        """

        :param xml_text:
        :param parser_cls:
        :return:
        """


class MessageReader(AbstractMessageReader):
    """ This class does all the conversions from DOM trees (body of SOAP messages) to MDIB objects."""

    def get_descriptor_container_class(self, qname):
        return self.sdc_definitions.get_descriptor_container_class(qname)

    def get_state_container_class(self, qname):
        return self.sdc_definitions.get_state_container_class(qname)

    def read_received_message(self, xml_text: str, parser_cls=None) -> ReceivedMessageData:
        normalized_xml = self.sdc_definitions.normalize_xml_text(xml_text)
        if parser_cls is not None:
            envelope = parser_cls(normalized_xml)
        else:
            envelope = ReceivedSoap12Envelope(normalized_xml)
        return self._mk_received_message_data(envelope)

    def _mk_received_message_data(self, envelope):
        data = ReceivedMessageData(self, envelope)
        data.action = envelope.address.action
        q_name = envelope.msg_name
        data.msg_name = q_name.localname if q_name else None
        if envelope.msg_node is not None:
            data.mdib_version = int(envelope.msg_node.get('MdibVersion', '0'))
            data.sequence_id = envelope.msg_node.get('SequenceId')
        return data

    @staticmethod
    def get_mdib_root_node(sdc_definitions, xml_text):
        """
        Creates a normalized and validated elementtree from xml_text.
        normalizing means that draft6 or final BICEPS namespaces are replaced by an standardized internal namespace.
        :param sdc_definitions:
        :param xml_text: xml document
        :return: elementtree node of the root element
        """
        xml_text = sdc_definitions.normalize_xml_text(xml_text)
        parser = etree_.ETCompatXMLParser(remove_comments=True, remove_blank_text=True, resolve_entities=False)
        root = etree_.fromstring(xml_text, parser=parser, base_url=None)
        if root.tag != namespaces.msgTag('GetMdibResponse'):
            found_nodes = root.xpath('//msg:GetMdibResponse', namespaces=namespaces.nsmap)
            if len(found_nodes) != 1:
                raise ValueError('provided xml does not contain a msg:GetMdibResponse node!')
            root = found_nodes[0]
        return root

    def read_mddescription(self, node):
        """
        Parses a GetMdDescriptionResponse or the MdDescription part of GetMdibResponse
        :param node: An etree node
        :return: a list of DescriptorContainer objects, sorted depth last
        """
        descriptions = []
        found_nodes = node.xpath('//dom:MdDescription', namespaces=namespaces.nsmap)
        if not found_nodes:
            raise ValueError('no MdDescription node found in tree')
        mddescription_node = found_nodes[0]

        def add_children(parent_node):
            p_handle = parent_node.get('Handle')
            for child_node in parent_node:
                if child_node.get('Handle') is not None:
                    container = self._mk_descriptorcontainer_from_node(child_node, p_handle)
                    descriptions.append(container)
                    add_children(child_node)

        # iterate over tree, collect all handles of vmds, channels and metric descriptors
        all_mds = mddescription_node.findall(namespaces.domTag('Mds'))
        for mds_node in all_mds:
            mds = self._mk_descriptorcontainer_from_node(mds_node, None)
            descriptions.append(mds)
            add_children(mds_node)
        return descriptions

    def read_mdstate(self, node):
        """
        Parses a GetMdStateResponse or the MdState part of GetMdibResponse
        :param node: A node that contains MdState nodes
        :return: a list of state containers
        """
        state_containers = []
        mdstate_nodes = node.xpath('//dom:MdState', namespaces=namespaces.nsmap)
        if mdstate_nodes:
            all_state_nodes = mdstate_nodes[0].findall(namespaces.domTag('State'))
            for state_node in all_state_nodes:
                try:
                    state_containers.append(self.mk_statecontainer_from_node(state_node))
                except MdibStructureError as ex:
                    self._logger.error('{}read_mdstate: cannot create: {}', self._log_prefix, ex)
        return state_containers

    def _mk_descriptorcontainer_from_node(self, node, parent_handle):
        """

        :param node: a descriptor node
        :param parent_handle: the handle of the parent
        :return: a DescriptorContainer object representing the content of node
        """
        node_type = node.get(namespaces.QN_TYPE)
        if node_type is not None:
            node_type = namespaces.text_to_qname(node_type, node.nsmap)
        else:
            node_type = etree_.QName(node.tag)
        descr_cls = self.get_descriptor_container_class(node_type)
        return descr_cls.from_node(node, parent_handle)

    def mk_statecontainer_from_node(self, node, forced_type=None):
        """
        :param node: a etree node
        :param forced_type: if given, the QName that shall be used for class instantiation instead of the data in node
        """
        if forced_type is not None:
            node_type = forced_type
        else:
            node_type = node.get(namespaces.QN_TYPE)
            if node_type is not None:
                node_type = namespaces.text_to_qname(node_type, node.nsmap)

        descriptor_container = None
        st_cls = self.get_state_container_class(node_type)
        if st_cls is None:
            raise ValueError(f'nody type {node_type} is not known')

        if node.tag != namespaces.domTag('State'):
            node = copy.copy(node)  # make a copy, do not modify the original report
            node.tag = namespaces.domTag('State')
        state = st_cls(descriptor_container)
        state.update_from_node(node)
        state.node = node
        return state

    def _mk_realtime_sample_array_states(self, node):
        return self.mk_statecontainer_from_node(node, namespaces.domTag('RealTimeSampleArrayMetricState'))

    def _mk_statecontainers_from_reportpart2(self, reportpart_node):
        containers = []
        for child_node in reportpart_node:
            desc_h = child_node.get('DescriptorHandle')
            if desc_h is None:
                self._logger.error('{}_on_episodic_component_report: missing descriptor handle in {}!',
                                   self._log_prefix,
                                   lambda: etree_.tostring(child_node))  # pylint: disable=cell-var-from-loop
            else:
                containers.append(self.mk_statecontainer_from_node(child_node))
        return containers


class MessageReaderClient(MessageReader):

    def read_get_mdstate_response(self, message_data: ReceivedMessageData):
        state_containers = []
        mdstate_nodes = message_data.p_msg.msg_node.xpath('//msg:MdState', namespaces=namespaces.nsmap)
        if mdstate_nodes:
            mdstate_node = mdstate_nodes[0]
            for state_node in mdstate_node:
                state_containers.append(self.mk_statecontainer_from_node(state_node))
        return state_containers

    def read_context_states(self, message_data):
        """ Creates Context State Containers from dom tree.
        :param getcontextstates_response_node: node "getContextStatesResponse" of getContextStates.
        @return: a list of state containers
        """
        states = []
        context_state_nodes = message_data.p_msg.msg_node[:]  # list of msg:ContextStatenodes
        for context_state_node in context_state_nodes:
            # hard remame to dom:State
            context_state_node.tag = namespaces.domTag('State')
            try:
                state_container = self.mk_statecontainer_from_node(context_state_node)
                states.append(state_container)
            except MdibStructureError as ex:
                self._logger.error('{}read_contextstates: cannot create: {}', self._log_prefix, ex)
        return states

    def read_get_localized_text_response(self, message_data) -> List[pmtypes.LocalizedText]:
        result = []
        response_node = message_data.p_msg.msg_node
        if response_node is not None:
            for element in response_node:
                l_text = pmtypes.LocalizedText.from_node(element)
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
        return self._read_waveform_report(message_data.p_msg.msg_node)

    def _read_waveform_report(self, report_node):
        """
        Parses a waveform report
        :param report_node: A waveform report etree
        :return: a list of StateContainer objects
        """
        states = []
        all_samplearrays = list(report_node)
        for samplearray in all_samplearrays:
            if samplearray.tag.endswith('State'):  # ignore everything else, e.g. Extension
                states.append(self._mk_realtime_sample_array_states(samplearray))
        return states

    def read_periodicmetric_report(self, message_data):
        return self._read_metric_report(message_data.p_msg.msg_node)

    def read_episodicmetric_report(self, message_data):
        return self._read_metric_report(message_data.p_msg.msg_node)

    def _read_metric_report(self, report_node):
        """
        Parses an episodic or periodic metric report
        :param report_node:  An episodic metric report etree
        :return: a list of StateContainer objects
        """
        states = []
        reportpart_nodes = report_node.xpath('msg:ReportPart', namespaces=namespaces.nsmap)
        for reportpart_node in reportpart_nodes:
            states.extend(self._mk_statecontainers_from_reportpart2(reportpart_node))
        return states

    def read_episodicalert_report(self, message_data):
        return self._read_alert_report_node(message_data.p_msg.msg_node)

    def read_periodicalert_report(self, message_data):
        return self._read_alert_report_node(message_data.p_msg.msg_node)

    def _read_alert_report_node(self, report_node):
        """
        Parses an episodic alert report
        :param report_node:  An episodic alert report etree
        :return: a list of StateContainer objects
        """
        states = []
        all_alerts = report_node.xpath('msg:ReportPart/msg:AlertState', namespaces=namespaces.nsmap)
        for alert in all_alerts:
            states.append(self.mk_statecontainer_from_node(alert))
        return states

    def read_operational_state_report(self, message_data):
        """
        Parses an operational state report
        :param message_data:
        :return: a list of StateContainer objects
        """
        states = []
        found_nodes = message_data.p_msg.msg_node.xpath('msg:ReportPart/msg:OperationState',
                                                           namespaces=namespaces.nsmap)
        for found_node in found_nodes:
            states.append(self.mk_statecontainer_from_node(found_node))
        return states

    def read_episodic_context_report(self, message_data):
        """
        Parses an episodic context report
        :param message_data:
        :return: a list of StateContainer objects
        """
        states = []
        found_nodes = message_data.p_msg.msg_node.xpath('msg:ReportPart', namespaces=namespaces.nsmap)
        for found_node in found_nodes:
            states.extend(self._mk_statecontainers_from_reportpart2(found_node))
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
        found_nodes = report_node.xpath('msg:ReportPart/msg:ComponentState', namespaces=namespaces.nsmap)
        for found_node in found_nodes:
            states.append(self.mk_statecontainer_from_node(found_node))
        return states

    def read_description_modification_report(self, message_data):
        """
        Parses a description modification report
        :param report_node:  A description modification report etree
        :return: a list of DescriptorContainer objects
        """
        descriptors_list = []
        report_parts = list(message_data.p_msg.msg_node)  # list of msg:ReportPart nodes
        for report_part in report_parts:
            descriptors = {pmtypes.DescriptionModificationTypes.UPDATE: ([], []),
                           pmtypes.DescriptionModificationTypes.CREATE: ([], []),
                           pmtypes.DescriptionModificationTypes.DELETE: ([], []),
                           }
            descriptors_list.append(descriptors)
            parent_descriptor = report_part.get('ParentDescriptor')
            modification_type = report_part.get('ModificationType', 'Upt')  # implied Value is 'Upt'
            descriptor_nodes = report_part.findall(namespaces.msgTag('Descriptor'))
            for descriptor_node in descriptor_nodes:
                descr_container = self._mk_descriptorcontainer_from_node(descriptor_node, parent_descriptor)
                descriptors[modification_type][0].append(descr_container)
            state_nodes = report_part.findall(namespaces.msgTag('State'))
            for state_node in state_nodes:
                state_container = self.mk_statecontainer_from_node(state_node)
                # set descriptor_container member
                corresponding_descriptors = [d for d in descriptors[modification_type][0] if
                                             d.handle == state_container.DescriptorHandle]
                if len(corresponding_descriptors) == 0:
                    raise MdibStructureError(
                        'new state {}: descriptor with handle "{}" does not exist!'.format(
                            state_container.NODETYPE.localname, state_container.DescriptorHandle))
                descriptor_container = corresponding_descriptors[0]
                state_container.set_descriptor_container(descriptor_container)
                descriptors[modification_type][1].append(state_container)
        return descriptors_list

    @staticmethod
    def read_operation_response(message_data: ReceivedMessageData) -> OperationResult:
        msg_node = message_data.p_msg.msg_node
        transaction_id = msg_node.xpath('msg:InvocationInfo/msg:TransactionId/text()',
                                        namespaces=namespaces.nsmap)[0]
        invocation_state = msg_node.xpath('msg:InvocationInfo/msg:InvocationState/text()',
                                          namespaces=namespaces.nsmap)[0]
        errors = msg_node.xpath('msg:InvocationInfo/msg:InvocationError/text()',
                                namespaces=namespaces.nsmap)
        error_msgs = msg_node.xpath('msg:InvocationInfo/msg:InvocationErrorMessage/text()',
                                    namespaces=namespaces.nsmap)
        return OperationResult(int(transaction_id), invocation_state, ''.join(errors), ''.join(error_msgs),
                               message_data.p_msg)

    @staticmethod
    def read_operation_invoked_report(message_data: ReceivedMessageData) -> OperationResult:
        msg_node = message_data.p_msg.msg_node
        transaction_id = msg_node.xpath('msg:ReportPart/msg:InvocationInfo/msg:TransactionId/text()',
                                        namespaces=namespaces.nsmap)[0]
        invocation_state = msg_node.xpath('msg:ReportPart/msg:InvocationInfo/msg:InvocationState/text()',
                                          namespaces=namespaces.nsmap)[0]
        errors = msg_node.xpath('msg:ReportPart/msg:InvocationInfo/msg:InvocationError/text()',
                                namespaces=namespaces.nsmap)
        error_msgs = msg_node.xpath('msg:ReportPart/msg:InvocationInfo/msg:InvocationErrorMessage/text()',
                                    namespaces=namespaces.nsmap)
        return OperationResult(int(transaction_id), invocation_state, ''.join(errors), ''.join(error_msgs),
                               message_data.p_msg)

    @staticmethod
    def read_subscribe_response(message_data: ReceivedMessageData):
        msg_node = message_data.p_msg.msg_node
        if msg_node.tag == namespaces.wseTag('SubscribeResponse'):
            address = msg_node.xpath('wse:SubscriptionManager/wsa:Address/text()', namespaces=namespaces.nsmap)
            reference_params = msg_node.xpath('wse:SubscriptionManager/wsa:ReferenceParameters',
                                              namespaces=namespaces.nsmap)
            expires = msg_node.xpath('wse:Expires/text()', namespaces=namespaces.nsmap)

            subscription_manager_address = urllib.parse.urlparse(address[0])
            expire_seconds = isoduration.parse_duration(expires[0])
            return subscription_manager_address, reference_params, expire_seconds
        return None

    @staticmethod
    def read_renew_response(message_data: ReceivedMessageData) -> float:
        expires = message_data.p_msg.body_node.xpath('wse:RenewResponse/wse:Expires/text()',
                                                        namespaces=namespaces.nsmap)
        if len(expires) == 0:
            return None
        expire_seconds = isoduration.parse_duration(expires[0])
        return expire_seconds

    @staticmethod
    def read_get_status_response(message_data: ReceivedMessageData) -> float:
        expires = message_data.p_msg.body_node.xpath('wse:GetStatusResponse/wse:Expires/text()',
                                                        namespaces=namespaces.nsmap)
        if len(expires) == 0:
            return None
        expire_seconds = isoduration.parse_duration(expires[0])
        return expire_seconds

    @staticmethod
    def read_subscription_end_message(message_data: ReceivedMessageData) -> SubscriptionEndResult:
        body_node = message_data.p_msg.body_node
        status_list = body_node.xpath('wse:SubscriptionEnd/wse:Status/text()', namespaces=namespaces.nsmap)
        reason_list = body_node.xpath('wse:SubscriptionEnd/wse:Reason/text()', namespaces=namespaces.nsmap)
        reference_params_node = message_data.p_msg.address.reference_parameters_node
        if reference_params_node is None:
            reference_parameters = None
        else:
            reference_parameters = reference_params_node[:]
        # subscr_ident_list = envelope.header_node.findall(_ClSubscription.IDENT_TAG, namespaces=namespaces.nsmap)
        return SubscriptionEndResult(status_list, reason_list, reference_parameters)

    @staticmethod
    def read_wsdl(wsdl_string):
        """ make am ElementTree instance"""
        return etree_.fromstring(wsdl_string, parser=etree_.ETCompatXMLParser(resolve_entities=False))


class MessageReaderDevice(MessageReader):
    """Contains methods that are only used by device"""

    def read_subscribe_request(self, request_data):
        envelope = request_data.message_data.p_msg
        accepted_encodings = CompressionHandler.parse_header(request_data.http_header.get('Accept-Encoding'))

        subscription_filter_nodes = envelope.body_node.xpath(
            "//wse:Filter[@Dialect='{}/Action']".format(namespaces.Prefixes.DPWS.namespace),
            namespaces=namespaces.nsmap)
        if len(subscription_filter_nodes) != 1:
            raise Exception
        subscription_filters = subscription_filter_nodes[0].text.split()
        end_to_addresses = envelope.body_node.xpath('wse:Subscribe/wse:EndTo', namespaces=namespaces.nsmap)
        end_to_address = None
        end_to_ref_node = None
        if len(end_to_addresses) == 1:
            end_to_node = end_to_addresses[0]
            end_to_address = end_to_node.xpath('wsa:Address/text()', namespaces=namespaces.nsmap)[0]
            end_to_ref_node = end_to_node.find('wsa:ReferenceParameters', namespaces=namespaces.nsmap)

        # determine (mandatory) notification address
        delivery_node = envelope.body_node.xpath('wse:Subscribe/wse:Delivery', namespaces=namespaces.nsmap)[0]
        notify_to_node = delivery_node.find('wse:NotifyTo', namespaces=namespaces.nsmap)
        notify_to_address = notify_to_node.xpath('wsa:Address/text()', namespaces=namespaces.nsmap)[0]
        notify_ref_node = notify_to_node.find('wsa:ReferenceParameters', namespaces=namespaces.nsmap)

        mode = delivery_node.get('Mode')  # mandatory attribute

        expires_nodes = envelope.body_node.xpath('wse:Subscribe/wse:Expires/text()', namespaces=namespaces.nsmap)
        if len(expires_nodes) == 0:
            expires = None
        else:
            expires = isoduration.parse_duration(str(expires_nodes[0]))

        # filter_ = envelope.body_node.xpath('wse:Subscribe/wse:Filter/text()', namespaces=namespaces.nsmap)[0]
        return SubscribeRequest(accepted_encodings, subscription_filters, notify_to_address, notify_ref_node,
                                end_to_address, end_to_ref_node, mode, expires)

    @staticmethod
    def read_renew_request(message_data):
        expires = message_data.p_msg.body_node.xpath('wse:Renew/wse:Expires/text()', namespaces=namespaces.nsmap)
        if len(expires) == 0:
            return None
        else:
            return isoduration.parse_duration(str(expires[0]))

    @staticmethod
    def read_identifier(message_data):
        reference_parameters_node = message_data.p_msg.header_node.find(
            namespaces.wsaTag('ReferenceParameters'),
            namespaces=namespaces.nsmap)
        if reference_parameters_node is None:
            identifier_node = None
        else:
            identifier_node = reference_parameters_node[0]
        return identifier_node

    @staticmethod
    def read_getmddescription_request(message_data: ReceivedMessageData) -> List[str]:
        """
        :param request: a soap envelope
        :return : a list of requested Handles
        """
        return message_data.p_msg.body_node.xpath('*/msg:HandleRef/text()', namespaces=namespaces.nsmap)

    @staticmethod
    def read_getmdstate_request(message_data) -> List[str]:
        """
        :param request: a soap envelope
        :return : a list of requested Handles
        """
        return message_data.p_msg.body_node.xpath('*/msg:HandleRef/text()', namespaces=namespaces.nsmap)

    def _operation_handle(self, message_data):
        operation_handle_refs = message_data.p_msg.body_node.xpath('*/msg:OperationHandleRef/text()',
                                                                      namespaces=namespaces.nsmap)

        return operation_handle_refs[0]

    def read_activate_request(self, message_data: ReceivedMessageData) -> OperationRequest:
        argument_strings = message_data.p_msg.body_node.xpath('*/msg:Argument/msg:ArgValue/text()',
                                                                 namespaces=namespaces.nsmap)
        return OperationRequest(self._operation_handle(message_data), argument_strings)

    def convert_activate_arguments(self, operation_descriptor, operation_request):
        # ToDo: check type of each argument an convert string to corresponding python type
        return operation_request

    def read_set_value_request(self, message_data: ReceivedMessageData) -> OperationRequest:
        value_nodes = message_data.p_msg.body_node.xpath('*/msg:RequestedNumericValue',
                                                            namespaces=namespaces.nsmap)
        if value_nodes:
            argument = float(value_nodes[0].text)
        else:
            argument = None
        return OperationRequest(self._operation_handle(message_data), argument)

    def read_set_string_request(self, message_data: ReceivedMessageData) -> OperationRequest:
        string_node = message_data.p_msg.body_node.xpath('*/msg:RequestedStringValue',
                                                            namespaces=namespaces.nsmap)
        if string_node:
            argument = str(string_node[0].text)
        else:
            argument = None
        return OperationRequest(self._operation_handle(message_data), argument)

    def read_set_metric_state_request(self, message_data: ReceivedMessageData) -> OperationRequest:
        proposed_state_nodes = message_data.p_msg.body_node.xpath('*/msg:ProposedMetricState',
                                                                     namespaces=namespaces.nsmap)
        proposed_states = [self.mk_statecontainer_from_node(m) for m in proposed_state_nodes]
        return OperationRequest(self._operation_handle(message_data), proposed_states)

    def read_set_alert_state_request(self, message_data: ReceivedMessageData) -> OperationRequest:
        proposed_state_nodes = message_data.p_msg.body_node.xpath('*/msg:ProposedAlertState',
                                                                     namespaces=namespaces.nsmap)
        if len(proposed_state_nodes) > 1:  # schema allows exactly one ProposedAlertState:
            raise ValueError(
                'only one ProposedAlertState argument allowed, found {}'.format(len(proposed_state_nodes)))
        if len(proposed_state_nodes) == 0:
            raise ValueError('no ProposedAlertState argument found')
        proposed_states = [self.mk_statecontainer_from_node(m) for m in proposed_state_nodes]
        return OperationRequest(self._operation_handle(message_data), proposed_states)

    def read_set_component_state_request(self, message_data: ReceivedMessageData) -> OperationRequest:
        proposed_state_nodes = message_data.p_msg.body_node.xpath('*/msg:ProposedComponentState',
                                                                     namespaces=namespaces.nsmap)
        proposed_states = [self.mk_statecontainer_from_node(m) for m in proposed_state_nodes]
        return OperationRequest(self._operation_handle(message_data), proposed_states)

    def read_get_context_states_request(self, message_data: ReceivedMessageData) -> List[str]:
        requested_handles = message_data.p_msg.body_node.xpath(
            '*/msg:HandleRef/text()', namespaces=namespaces.nsmap)
        return requested_handles

    def read_set_context_state_request(self, message_data: ReceivedMessageData) -> OperationRequest:
        proposed_state_nodes = message_data.p_msg.body_node.xpath('*/msg:ProposedContextState',
                                                                     namespaces=namespaces.nsmap)
        proposed_states = [self.mk_statecontainer_from_node(m) for m in proposed_state_nodes]
        return OperationRequest(self._operation_handle(message_data), proposed_states)

    def read_get_localized_text_request(self, message_data: ReceivedMessageData) -> LocalizedTextsRequest:
        body_node = message_data.p_msg.body_node
        requested_handles = body_node.xpath('*/msg:Ref/text()',
                                            namespaces=namespaces.nsmap)  # handle strings 0...n
        requested_versions = body_node.xpath('*/msg:Version/text()',
                                             namespaces=namespaces.nsmap)  # unsigned long int 0..1
        requested_langs = body_node.xpath('*/msg:Lang/text()',
                                          namespaces=namespaces.nsmap)  # unsigned long int 0..n
        text_widths = body_node.xpath('*/msg:TextWidth/text()',
                                      namespaces=namespaces.nsmap)  # strings 0..n
        number_of_lines = body_node.xpath('*/msg:NumberOfLines/text()',
                                          namespaces=namespaces.nsmap)  # int 0..n
        return LocalizedTextsRequest(requested_handles, requested_versions, requested_langs, text_widths,
                                     number_of_lines)
