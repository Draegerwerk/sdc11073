import copy
import urllib
from abc import abstractmethod, ABC
from collections import namedtuple
from typing import List

from lxml import etree as etree_

from sdc11073 import namespaces
from sdc11073 import pmtypes
from .. import isoduration
from sdc11073.pysoap.soapenvelope import ReceivedSoap12Envelope


# from sdc11073.sdcclient.operations import OperationResult

class MdibStructureError(Exception):
    pass


OperationResult = namedtuple('OperationResult', 'transaction_id invocation_state error errorMsg soapEnvelope')
SubscriptionEndResult = namedtuple('SubscriptionEndResult', 'status_list, reason_list, reference_parameter_list')


class AbstractMessageReader(ABC):
    class ReceivedMessageData:
        """This class contains all data of a received Message"""

        def __init__(self, raw_data):
            self.raw_data = raw_data  # e g. a soap envelope
            self.mdib_version = None
            self.sequence_id = None
            self.action = None
            self.msg_name = None
            self.descriptor_containers = None
            self.state_containers = None

    def __init__(self, logger, log_prefix=''):
        self._logger = logger
        self._log_prefix = log_prefix

    @staticmethod
    @abstractmethod
    def read_getmddescription_request(request) -> List[str]:
        """ returns a list of handles"""

    @abstractmethod
    def read_mddescription(self, node, mdib):
        """ returns DescriptorContainer instances"""

    @abstractmethod
    def read_getmdstate_request(request) -> List[str]:
        """ returns a list of handles"""

    @abstractmethod
    def read_mdstate(self, node, mdib, additional_descriptor_containers=None):
        """ returns StateContainer instances"""




class MessageReader(AbstractMessageReader):
    """ This class does all the conversions from DOM trees (body of SOAP messages) to MDIB objects."""

    def read_received_message(self, sdc_definitions, xml_text):
        normalized_xml = sdc_definitions.normalize_xml_text(xml_text)
        envelope = ReceivedSoap12Envelope(normalized_xml)
        return self.mk_received_message(envelope)
        # data = self.ReceivedMessageData(envelope)
        # data.action = envelope.address.action
        # q_name = envelope.msg_name
        # data.msg_name = q_name.localname if q_name else None
        # if envelope.msg_node is not None:
        #     data.mdib_version = int(envelope.msg_node.get('MdibVersion', '0'))
        #     data.sequence_id = envelope.msg_node.get('SequenceId')
        # return data

    def mk_received_message(self, envelope):
        data = self.ReceivedMessageData(envelope)
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

    @staticmethod
    def read_getmddescription_request(request) -> List[str]:
        """
        :param request: a soap envelope
        :return : a list of requested Handles
        """
        return request.body_node.xpath('*/msg:HandleRef/text()', namespaces=namespaces.nsmap)

    def read_mddescription(self, node, mdib):
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
                    container = self._mk_descriptorcontainer_from_node(child_node, p_handle, mdib)
                    descriptions.append(container)
                    add_children(child_node)

        # iterate over tree, collect all handles of vmds, channels and metric descriptors
        all_mds = mddescription_node.findall(namespaces.domTag('Mds'))
        for mds_node in all_mds:
            mds = self._mk_descriptorcontainer_from_node(mds_node, None, mdib)
            descriptions.append(mds)
            add_children(mds_node)
        return descriptions

    @staticmethod
    def read_getmdstate_request(request) -> List[str]:
        """
        :param request: a soap envelope
        :return : a list of requested Handles
        """
        return request.body_node.xpath('*/msg:HandleRef/text()', namespaces=namespaces.nsmap)

    def read_mdstate(self, node, mdib, additional_descriptor_containers=None):
        """
        Parses a GetMdStateResponse or the MdState part of GetMdibResponse
        :param node: A node that contains MdState nodes
        :param additional_descriptor_containers: a list of descriptor containers that can also be used for state creation
                (typically used if descriptors and states are created in the same transaction. In that case the descriptors are not yet part of mdib.)
        :return: a list of state containers
        """
        state_containers = []
        mdstate_nodes = node.xpath('//dom:MdState', namespaces=namespaces.nsmap)
        if mdstate_nodes:
            all_states = mdstate_nodes[0].findall(namespaces.domTag('State'))
            for state in all_states:
                try:
                    state_containers.append(self.mk_statecontainer_from_node(
                        state, mdib, additional_descriptor_containers=additional_descriptor_containers))
                except MdibStructureError as ex:
                    self._logger.error('{}read_mdstate: cannot create: {}', self._log_prefix, ex)
        return state_containers

    def read_contextstates(self, getcontextstates_response_node, mdib):
        """ Creates Context State Containers from dom tree.
        :param getcontextstates_response_node: node "getContextStatesResponse" of getContextStates.
        @return: a list of state containers
        """
        states = []
        context_state_nodes = list(getcontextstates_response_node)  # list of msg:ContextStatenodes
        for context_state_node in context_state_nodes:
            # hard remame to dom:State
            context_state_node.tag = namespaces.domTag('State')
            try:
                state_container = self.mk_statecontainer_from_node(context_state_node, mdib)
                states.append(state_container)
            except MdibStructureError as ex:
                self._logger.error('{}read_contextstates: cannot create: {}', self._log_prefix, ex)
        return states

    @staticmethod
    def _mk_descriptorcontainer_from_node(node, parent_handle, mdib):
        """

        :param node: a descriptor node
        :param parent_handle: the handle of the parent
        :param mdib: an mdib
        :return: a DescriptorContainer object representing the content of node
        """
        node_type = node.get(namespaces.QN_TYPE)
        if node_type is not None:
            node_type = namespaces.text_to_qname(node_type, node.nsmap)
        else:
            node_type = etree_.QName(node.tag)
        cls = mdib.sdc_definitions.get_descriptor_container_class(node_type)
        return cls.from_node(mdib.nsmapper, node, parent_handle)

    @classmethod
    def mk_statecontainer_from_node(cls, node, mdib, forced_type=None, additional_descriptor_containers=None):
        """
        :param node: a etree node
        :param forcedType: if given, the QName that shall be used for class instantiation instead of the data in node
        """
        if forced_type is not None:
            node_type = forced_type
        else:
            node_type = node.get(namespaces.QN_TYPE)
            if node_type is not None:
                node_type = namespaces.text_to_qname(node_type, node.nsmap)

        descriptor_handle = node.get('DescriptorHandle')
        descriptor_container = mdib.descriptions.handle.get_one(descriptor_handle, allow_none=True)
        if descriptor_container is None:
            if additional_descriptor_containers is not None:
                corresponding_descriptors = [d for d in additional_descriptor_containers if
                                             d.handle == descriptor_handle]
            else:
                corresponding_descriptors = None
            if corresponding_descriptors is None or len(corresponding_descriptors) == 0:
                raise MdibStructureError(
                    'new state {}: descriptor with handle "{}" does not exist!'.format(node_type.localname,
                                                                                       descriptor_handle))
            descriptor_container = corresponding_descriptors[0]
        st_cls = mdib.sdc_definitions.get_state_container_class(node_type)
        if node.tag != namespaces.domTag('State'):
            node = copy.copy(node)  # make a copy, do not modify the original report
            node.tag = namespaces.domTag('State')
        state = st_cls(mdib.nsmapper, descriptor_container)
        cls._init_state_from_node(state, node)
        state.node = node
        return state

    @classmethod
    def _mk_realtime_sample_array_states(cls, node, mdib):
        return cls.mk_statecontainer_from_node(node, mdib, namespaces.domTag('RealTimeSampleArrayMetricState'))

    @staticmethod
    def _init_state_from_node(container, node):
        """ update members.
        """
        # update all ContainerProperties
        for _, cprop in container.sorted_container_properties():
            cprop.update_from_node(container, node)

    def _mk_statecontainers_from_reportpart(self, reportpart_node, mdib):
        containers = []
        for child_node in reportpart_node:
            desc_h = child_node.get('DescriptorHandle')
            if desc_h is None:
                self._logger.error('{}_on_episodic_component_report: missing descriptor handle in {}!',
                                   self._log_prefix,
                                   lambda: etree_.tostring(child_node))  # pylint: disable=cell-var-from-loop
            else:
                containers.append(self.mk_statecontainer_from_node(child_node, mdib))
        return containers

    def read_notification(self, message_data, mdib):
        """Fill message_data with state containers and descriptor containers from notification"""
        actions = mdib.sdc_definitions.Actions
        if message_data.action == actions.Waveform:
            message_data.state_containers = self.read_waveform_report(message_data.raw_data.msg_node, mdib)
        pass

    def read_waveform_report(self, message_data, mdib):
        return self._read_waveform_report(message_data.raw_data.msg_node, mdib)

    def _read_waveform_report(self, report_node, mdib):
        """
        Parses a waveform report
        :param report_node: A waveform report etree
        :return: a list of StateContainer objects
        """
        states = []
        all_samplearrays = list(report_node)
        for samplearray in all_samplearrays:
            if samplearray.tag.endswith('State'):  # ignore everything else, e.g. Extension
                states.append(self._mk_realtime_sample_array_states(samplearray, mdib))
        return states

    def read_periodicmetric_report(self, message_data, mdib):
        return self._read_episodicmetric_report(message_data.raw_data.msg_node, mdib)

    def read_episodicmetric_report(self, message_data, mdib):
        return self._read_episodicmetric_report(message_data.raw_data.msg_node, mdib)

    def _read_episodicmetric_report(self, report_node, mdib):
        """
        Parses an episodic metric report
        :param report_node:  An episodic metric report etree
        :return: a list of StateContainer objects
        """
        states = []
        reportpart_nodes = report_node.xpath('msg:ReportPart', namespaces=namespaces.nsmap)
        for reportpart_node in reportpart_nodes:
            states.extend(self._mk_statecontainers_from_reportpart(reportpart_node, mdib))
        return states

    def read_periodicalert_report(self, message_data, mdib):
        return self._read_episodic_alert_report_node(message_data.raw_data.msg_node, mdib)

    def read_episodicalert_report(self, message_data, mdib):
        return self._read_episodic_alert_report_node(message_data.raw_data.msg_node, mdib)

    def _read_episodic_alert_report_node(self, report_node, mdib):
        """
        Parses an episodic alert report
        :param report_node:  An episodic alert report etree
        :return: a list of StateContainer objects
        """
        states = []
        all_alerts = report_node.xpath('msg:ReportPart/msg:AlertState', namespaces=namespaces.nsmap)
        for alert in all_alerts:
            states.append(self.mk_statecontainer_from_node(alert, mdib))
        return states

    def read_operationalstate_report(self, message_data, mdib):
        return self.read_operational_state_report_node(message_data.raw_data.msg_node, mdib)

    def read_operational_state_report_node(self, report_node, mdib):
        """
        Parses an operational state report
        :param report_node:  An operational state report etree
        :return: a list of StateContainer objects
        """
        states = []
        found_nodes = report_node.xpath('msg:ReportPart/msg:OperationState', namespaces=namespaces.nsmap)
        for found_node in found_nodes:
            states.append(self.mk_statecontainer_from_node(found_node, mdib))
        return states

    def read_episodic_context_report(self, message_data, mdib):
        return self._read_episodic_context_report(message_data.raw_data.msg_node, mdib)

    def _read_episodic_context_report(self, report_node, mdib):
        """
        Parses an episodic context report
        :param report_node:  An episodic context report etree
        :return: a list of StateContainer objects
        """
        states = []
        found_nodes = report_node.xpath('msg:ReportPart', namespaces=namespaces.nsmap)
        for found_node in found_nodes:
            states.extend(self._mk_statecontainers_from_reportpart(found_node, mdib))
        return states

    def read_periodic_component_report(self, message_data, mdib):
        return self._read_episodic_component_report(message_data.raw_data.msg_node, mdib)

    def read_episodic_component_report(self, message_data, mdib):
        return self._read_episodic_component_report(message_data.raw_data.msg_node, mdib)

    def _read_episodic_component_report(self, report_node, mdib):
        """
        Parses an episodic component report
        :param report_node:  An episodic component report etree
        :return: a list of StateContainer objects
        """
        states = []
        found_nodes = report_node.xpath('msg:ReportPart/msg:ComponentState', namespaces=namespaces.nsmap)
        for found_node in found_nodes:
            states.append(self.mk_statecontainer_from_node(found_node, mdib))
        return states

    def read_description_modification_report(self, message_data, mdib):
        return self._read_description_modification_report(message_data.raw_data.msg_node, mdib)

    def _read_description_modification_report(self, report_node, mdib):
        """
        Parses a description modification report
        :param report_node:  A description modification report etree
        :return: a list of DescriptorContainer objects
        """
        descriptors_list = []
        report_parts = list(report_node)  # list of msg:ReportPart nodes
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
                descr_container = self._mk_descriptorcontainer_from_node(descriptor_node, parent_descriptor, mdib)
                descriptors[modification_type][0].append(descr_container)
            state_nodes = report_part.findall(namespaces.msgTag('State'))
            for state_node in state_nodes:
                state_container = self.mk_statecontainer_from_node(state_node, mdib,
                                                                   additional_descriptor_containers=
                                                                   descriptors[modification_type][0])
                descriptors[modification_type][1].append(state_container)
        return descriptors_list

    def read_operation_response(self, message_data):
        return self._read_operation_response(message_data.raw_data)

    @staticmethod
    def _read_operation_response(envelope):
        transaction_id = envelope.msg_node.xpath('msg:InvocationInfo/msg:TransactionId/text()',
                                                 namespaces=namespaces.nsmap)[0]
        invocation_state = envelope.msg_node.xpath('msg:InvocationInfo/msg:InvocationState/text()',
                                                   namespaces=namespaces.nsmap)[0]
        errors = envelope.msg_node.xpath('msg:InvocationInfo/msg:InvocationError/text()',
                                         namespaces=namespaces.nsmap)
        error_msgs = envelope.msg_node.xpath('msg:InvocationInfo/msg:InvocationErrorMessage/text()',
                                             namespaces=namespaces.nsmap)
        return OperationResult(int(transaction_id), invocation_state, ''.join(errors), ''.join(error_msgs), envelope)

    def read_operation_invoked_report(self, message_data):
        return self._read_operation_invoked_report(message_data.raw_data)

    @staticmethod
    def _read_operation_invoked_report(envelope):
        transaction_id = envelope.msg_node.xpath('msg:ReportPart/msg:InvocationInfo/msg:TransactionId/text()',
                                                 namespaces=namespaces.nsmap)[0]
        invocation_state = envelope.msg_node.xpath('msg:ReportPart/msg:InvocationInfo/msg:InvocationState/text()',
                                                   namespaces=namespaces.nsmap)[0]
        errors = envelope.msg_node.xpath('msg:ReportPart/msg:InvocationInfo/msg:InvocationError/text()',
                                         namespaces=namespaces.nsmap)
        error_msgs = envelope.msg_node.xpath('msg:ReportPart/msg:InvocationInfo/msg:InvocationErrorMessage/text()',
                                             namespaces=namespaces.nsmap)
        return OperationResult(int(transaction_id), invocation_state, ''.join(errors), ''.join(error_msgs), envelope)

    def read_subscribe_response(self, message_data):
        return self.read_subscribe_response_envelope(message_data.raw_data)

    def read_subscribe_response_envelope(self, envelope):
        msg_node = envelope.msg_node
        if msg_node.tag == namespaces.wseTag('SubscribeResponse'):
            address = msg_node.xpath('wse:SubscriptionManager/wsa:Address/text()', namespaces=namespaces.nsmap)
            reference_params = msg_node.xpath('wse:SubscriptionManager/wsa:ReferenceParameters',
                                              namespaces=namespaces.nsmap)
            expires = msg_node.xpath('wse:Expires/text()', namespaces=namespaces.nsmap)

            subscription_manager_address = urllib.parse.urlparse(address[0])
            expire_seconds = isoduration.parse_duration(expires[0])
            return subscription_manager_address, reference_params, expire_seconds
        return None

    def read_renew_request(self, request_data):
        expires = request_data.envelope.body_node.xpath('wse:Renew/wse:Expires/text()', namespaces=namespaces.nsmap)
        if len(expires) == 0:
            expires = None
        else:
            expires = isoduration.parse_duration(str(expires[0]))

        reference_parameters_node = request_data.envelope.header_node.find(namespaces.wsaTag('ReferenceParameters'),
                                                                           namespaces=namespaces.nsmap)
        if reference_parameters_node is None:
            identifier_node = None
        else:
            identifier_node = reference_parameters_node[0]
            #identifier_node = request_data.envelope.header_node.find(_DevSubscription.IDENT_TAG, namespaces=nsmap)
        path_suffix = '/'.join(request_data.path_elements)  # not consumed path elements
        #return _mk_dispatch_identifier(identifier_node, path_suffix)


    def read_renew_response_envelope(self, envelope):
        expires = envelope.body_node.xpath('wse:RenewResponse/wse:Expires/text()', namespaces=namespaces.nsmap)
        if len(expires) == 0:
            return None
        expire_seconds = isoduration.parse_duration(expires[0])
        return expire_seconds

    def read_get_status_response_envelope(self, envelope):
        expires = envelope.body_node.xpath('wse:GetStatusResponse/wse:Expires/text()', namespaces=namespaces.nsmap)
        if len(expires) == 0:
            return None
        expire_seconds = isoduration.parse_duration(expires[0])
        return expire_seconds

    def read_subscription_end_message(self, message_data):
        return self._read_subscription_end_message(message_data.raw_data)

    def _read_subscription_end_message(self, envelope):
        status_list = envelope.body_node.xpath('wse:SubscriptionEnd/wse:Status/text()', namespaces=namespaces.nsmap)
        reason_list = envelope.body_node.xpath('wse:SubscriptionEnd/wse:Reason/text()', namespaces=namespaces.nsmap)
        reference_params_node = envelope.address.reference_parameters_node
        if reference_params_node is None:
            reference_parameters = None
        else:
            reference_parameters = reference_params_node[:]
        #subscr_ident_list = envelope.header_node.findall(_ClSubscription.IDENT_TAG, namespaces=namespaces.nsmap)

        return SubscriptionEndResult(status_list, reason_list, reference_parameters)

    @staticmethod
    def read_wsdl(wsdl_string):
        """ make am ElementTree instance"""
        return etree_.fromstring(wsdl_string, parser=etree_.ETCompatXMLParser(resolve_entities=False))
