import uuid
import weakref
from collections import defaultdict
from io import BytesIO
from typing import Optional

from lxml import etree as etree_

from .msgreader import validate_node
from .soapenvelope import Soap12Envelope
from .. import isoduration
from ..addressing import ReferenceParameters, EndpointReferenceType, Address
from ..dpws import DeviceEventingFilterDialectURI
from ..dpws import DeviceMetadataDialectURI, DeviceRelationshipTypeURI
from ..exceptions import ApiUsageError
from ..namespaces import EventingActions
from ..namespaces import QN_TYPE
from ..namespaces import WSA_ANONYMOUS
from ..schema_resolver import SchemaResolver
from ..schema_resolver import mk_schema_validator

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

    def mk_soap_message(self, addr_to: str, action, payload):
        soap_envelope = Soap12Envelope(self._ns_hlp.partial_map(self._ns_hlp.MSG))
        soap_envelope.set_address(Address(action=action, addr_to=addr_to))
        soap_envelope.payload_element = payload
        return CreatedMessage(soap_envelope, self)

    def mk_reply_soap_message(self, addr_to: Address, payload):
        soap_envelope = Soap12Envelope(self._ns_hlp.partial_map(self._ns_hlp.MSG))
        soap_envelope.set_address(addr_to)
        soap_envelope.payload_element = payload
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

    def _mk_endpoint_reference_sub_node(self, endpoint_reference, parent_node):
        node = etree_.SubElement(parent_node, self._ns_hlp.wsaTag('Address'))
        node.text = endpoint_reference.address
        if endpoint_reference.reference_parameters.has_parameters:
            reference_parameters_node = etree_.SubElement(parent_node, self._ns_hlp.wsaTag('ReferenceParameters'))
            reference_parameters_node.extend(endpoint_reference.reference_parameters.parameters)
        # ToDo: what about this metadata thing???
        # if self.metadata_node is not None:
        #    root_node.append(self.metadata_node)

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
        for parameter in address.reference_parameters.parameters:
            header_node.append(parameter)

    @staticmethod
    def _add_reference_params_to_header(soap_envelope, reference_parameters):
        """ add references for requests to device (renew, getstatus, unsubscribe)"""
        if reference_parameters.has_parameters:
            for element in reference_parameters.parameters:
                # mandatory attribute acc. to ws_addressing SOAP Binding (https://www.w3.org/TR/2006/REC-ws-addr-soap-20060509/)
                element.set('IsReferenceParameter', 'true')
                soap_envelope.add_header_element(element)

    def _validate_node(self, node):
        if self._validate:
            validate_node(node, self._xml_schema, self._logger)


class MessageFactoryClient(MessageFactory):
    """This class creates all messages that a client needs to create"""

    def mk_transfer_get_message(self, addr_to) -> CreatedMessage:
        envelope = Soap12Envelope(self._ns_hlp.ns_map)
        envelope.set_address(Address(action=f'{self._ns_hlp.WXF.namespace}/Get',
                                     addr_to=addr_to))
        return CreatedMessage(envelope, self)

    def mk_get_metadata_message(self, addr_to) -> CreatedMessage:
        soap_envelope = Soap12Envelope(self._ns_hlp.ns_map)
        soap_envelope.set_address(
            Address(action='http://schemas.xmlsoap.org/ws/2004/09/mex/GetMetadata/Request',
                    addr_to=addr_to))
        soap_envelope.payload_element = etree_.Element('{http://schemas.xmlsoap.org/ws/2004/09/mex}GetMetadata')
        return CreatedMessage(soap_envelope, self)

    def mk_subscribe_message(self, addr_to,
                             notifyto_url, notify_to_identifier,
                             endto_url, endto_identifier,
                             expire_minutes,
                             subscribe_filter,
                             any_elements: Optional[list] = None,
                             any_attributes: Optional[dict] = None) -> CreatedMessage:
        soap_envelope = Soap12Envelope(self._ns_hlp.partial_map(self._ns_hlp.WSE))
        soap_envelope.set_address(Address(action=EventingActions.Subscribe, addr_to=addr_to))
        if notify_to_identifier is None:
            notify_to = EndpointReferenceType(notifyto_url, reference_parameters=None)
        else:
            notify_to = EndpointReferenceType(notifyto_url,
                                              reference_parameters=ReferenceParameters([notify_to_identifier]))

        subscribe_node = etree_.Element(self._ns_hlp.wseTag('Subscribe'),
                                        nsmap=self._ns_hlp.partial_map(self._ns_hlp.WSE, self._ns_hlp.WSA))

        # EndTo is an optional element
        if endto_url is not None:
            if endto_identifier is None:
                end_to = EndpointReferenceType(endto_url, reference_parameters=None)
            else:
                end_to = EndpointReferenceType(endto_url, reference_parameters=ReferenceParameters([endto_identifier]))
            end_to_node = etree_.SubElement(subscribe_node, self._ns_hlp.wseTag('EndTo'))
            self._mk_endpoint_reference_sub_node(end_to, end_to_node)

        # Delivery is mandatory
        delivery = etree_.SubElement(subscribe_node, self._ns_hlp.wseTag('Delivery'))
        delivery.set('Mode', f'{self._ns_hlp.WSE.namespace}/DeliveryModes/Push')
        notify_to_node = etree_.SubElement(delivery, self._ns_hlp.wseTag('NotifyTo'))
        self._mk_endpoint_reference_sub_node(notify_to, notify_to_node)

        # Expires is optional
        if expire_minutes is not None:
            exp = etree_.SubElement(subscribe_node, self._ns_hlp.wseTag('Expires'))
            exp.text = isoduration.duration_string(expire_minutes * 60)

        # Filter is optional
        if subscribe_filter is not None:
            fil = etree_.SubElement(subscribe_node, self._ns_hlp.wseTag('Filter'))
            fil.set('Dialect', DeviceEventingFilterDialectURI.ACTION)
            fil.text = subscribe_filter

        # any_elements is optional
        if any_elements is not None:
            subscribe_node.extend(any_elements)

        # any_attributes is optional
        if any_attributes is not None:
            for name, value in any_attributes.items():
                subscribe_node[name] = value
        soap_envelope.payload_element = subscribe_node
        return CreatedMessage(soap_envelope, self)

    def mk_renew_message(self, addr_to: str, dev_reference_param: ReferenceParameters,
                         expire_minutes: int) -> CreatedMessage:
        nsh = self._ns_hlp
        soap_envelope = Soap12Envelope(nsh.partial_map(nsh.WSE))
        soap_envelope.set_address(Address(action=EventingActions.Renew, addr_to=addr_to))
        self._add_reference_params_to_header(soap_envelope, dev_reference_param)
        renew_node = etree_.Element(nsh.wseTag('Renew'), nsmap=nsh.partial_map(nsh.WSE))
        expires_node = etree_.SubElement(renew_node, nsh.wseTag('Expires'), nsmap=nsh.partial_map(nsh.WSE))
        expires_node.text = isoduration.duration_string(expire_minutes * 60)
        soap_envelope.payload_element = renew_node
        return CreatedMessage(soap_envelope, self)

    def mk_get_status_message(self, addr_to: str, dev_reference_param: ReferenceParameters) -> CreatedMessage:
        soap_envelope = Soap12Envelope(self._ns_hlp.partial_map(self._ns_hlp.WSE))
        soap_envelope.set_address(
            Address(action=EventingActions.GetStatus, addr_to=addr_to))
        self._add_reference_params_to_header(soap_envelope, dev_reference_param)
        soap_envelope.payload_element = etree_.Element(self._ns_hlp.wseTag('GetStatus'))
        return CreatedMessage(soap_envelope, self)

    def mk_unsubscribe_message(self, addr_to: str, dev_reference_param: ReferenceParameters) -> CreatedMessage:
        soap_envelope = Soap12Envelope(self._ns_hlp.partial_map(self._ns_hlp.WSE))
        soap_envelope.set_address(Address(action=EventingActions.Unsubscribe, addr_to=addr_to))
        self._add_reference_params_to_header(soap_envelope, dev_reference_param)
        soap_envelope.payload_element = etree_.Element(self._ns_hlp.wseTag('Unsubscribe'))
        return CreatedMessage(soap_envelope, self)


class MessageFactoryDevice(MessageFactory):
    """This class creates all messages that a device needs to create"""

    def mk_get_metadata_response_message(self, message_data, this_device, this_model,
                                         dpws_host, dpws_hosted_services: dict) -> CreatedMessage:
        nsh = self._ns_hlp
        response = Soap12Envelope(nsh.partial_map(nsh.WXF))
        reply_address = message_data.p_msg.address.mk_reply_address(f'{nsh.WXF.namespace}/GetResponse')
        reply_address.addr_to = WSA_ANONYMOUS
        reply_address.message_id = uuid.uuid4().urn
        response.set_address(reply_address)
        metadata_node = etree_.Element(nsh.wsxTag('Metadata'),
                                       nsmap=nsh.partial_map(nsh.MSG, nsh.SDC, nsh.DPWS, nsh.WSX))

        # ThisModel
        metadata_section_node = etree_.SubElement(metadata_node,
                                                  nsh.wsxTag('MetadataSection'),
                                                  attrib={'Dialect': DeviceMetadataDialectURI.THIS_MODEL})
        self._mk_this_model_sub_node(this_model, metadata_section_node)

        # ThisDevice
        metadata_section_node = etree_.SubElement(metadata_node,
                                                  nsh.wsxTag('MetadataSection'),
                                                  attrib={'Dialect': DeviceMetadataDialectURI.THIS_DEVICE})
        self._mk_this_device_sub_node(this_device, metadata_section_node)

        # Relationship
        metadata_section_node = etree_.SubElement(metadata_node,
                                                  nsh.wsxTag('MetadataSection'),
                                                  attrib={'Dialect': DeviceMetadataDialectURI.RELATIONSHIP})
        relationship_node = etree_.SubElement(metadata_section_node,
                                              nsh.dpwsTag('Relationship'),
                                              attrib={'Type': DeviceRelationshipTypeURI.HOST})
        self._mk_host_service_type_sub_node(dpws_host, relationship_node)

        # add all hosted services:
        for service in dpws_hosted_services.values():
            hosted_service_type = service.mk_dpws_hosted_instance()
            self._mk_hosted_service_type_sub_node(hosted_service_type, relationship_node)
        response.payload_element = metadata_node
        return CreatedMessage(response, self)

    def mk_hosted_get_metadata_response_message(self, message_data, dpws_host,
                                                hosted_service_type, location_text) -> CreatedMessage:
        nsh = self._ns_hlp
        response = Soap12Envelope()
        reply_address = message_data.p_msg.address.mk_reply_address(
            'http://schemas.xmlsoap.org/ws/2004/09/mex/GetMetadata/Response')
        response.set_address(reply_address)

        metadata_node = etree_.Element(nsh.wsxTag('Metadata'),
                                       nsmap=(nsh.partial_map(nsh.WXF, nsh.SDC)))

        # Relationship
        metadata_section_node = etree_.SubElement(metadata_node,
                                                  nsh.wsxTag('MetadataSection'),
                                                  attrib={'Dialect': DeviceMetadataDialectURI.RELATIONSHIP})

        relationship_node = etree_.SubElement(metadata_section_node,
                                              nsh.dpwsTag('Relationship'),
                                              attrib={'Type': DeviceRelationshipTypeURI.HOST})
        self._mk_host_service_type_sub_node(dpws_host, relationship_node)

        self._mk_hosted_service_type_sub_node(hosted_service_type, relationship_node)

        metadata_section_node = etree_.SubElement(metadata_node,
                                                  nsh.wsxTag('MetadataSection'),
                                                  attrib={'Dialect': nsh.WSDL.namespace})
        location_node = etree_.SubElement(metadata_section_node,
                                          nsh.wsxTag('Location'))
        location_node.text = location_text
        response.payload_element = metadata_node
        return CreatedMessage(response, self)

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

    def mk_get_mdib_response_node(self, mdib, include_context_states) -> etree_.Element:
        nsh = self._ns_hlp
        if include_context_states:
            mdib_node, mdib_version_group = mdib.reconstruct_mdib_with_context_states()
        else:
            mdib_node, mdib_version_group = mdib.reconstruct_mdib()
        get_mdib_response_node = etree_.Element(self._msg_names.GetMdibResponse,
                                                nsmap=nsh.partial_map(nsh.MSG, nsh.PM, nsh.XSI))
        self._set_mdib_version_group(get_mdib_response_node, mdib_version_group)
        get_mdib_response_node.append(mdib_node)
        return get_mdib_response_node

    def mk_get_mddescription_response_message(self, message_data, mdib, requested_handles) -> CreatedMessage:
        """For simplification reason this implementation returns either all descriptors or none."""
        nsh = self._ns_hlp
        request = message_data.p_msg
        return_all = len(requested_handles) == 0  # if we have handles, we need to check them
        my_namespaces = nsh.partial_map(nsh.S12, nsh.WSA, nsh.MSG, nsh.PM)
        response_envelope = Soap12Envelope(my_namespaces)
        reply_address = request.address.mk_reply_address(
            #action=self._get_action_string(mdib.sdc_definitions., 'GetMdDescriptionResponse'))
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

    def mk_subscribe_response_message(self, request_data, subscription, base_urls) -> CreatedMessage:
        nsh = self._ns_hlp
        response = Soap12Envelope(
            nsh.partial_map(nsh.PM, nsh.S12, nsh.WSA, nsh.WSE))
        reply_address = request_data.message_data.p_msg.address.mk_reply_address(EventingActions.SubscribeResponse)
        response.set_address(reply_address)
        subscribe_response_node = etree_.Element(nsh.wseTag('SubscribeResponse'))
        subscription_manager_node = etree_.SubElement(subscribe_response_node, nsh.wseTag('SubscriptionManager'))
        path = '/'.join(request_data.consumed_path_elements)
        path_suffix = '' if subscription.path_suffix is None else f'/{subscription.path_suffix}'
        subscription_address = f'{base_urls[0].scheme}://{base_urls[0].netloc}/{path}{path_suffix}'
        epr = EndpointReferenceType(address=subscription_address,
                                    reference_parameters=subscription.reference_parameters)
        self._mk_endpoint_reference_sub_node(epr, subscription_manager_node)
        expires_node = etree_.SubElement(subscribe_response_node, nsh.wseTag('Expires'))
        expires_node.text = subscription.expire_string  # simply confirm request
        response.payload_element = subscribe_response_node
        ret = CreatedMessage(response, self)
        self._logger.debug('on_subscribe_request returns {}', lambda: self.serialize_message(ret).decode('utf-8'))
        return ret

    def mk_unsubscribe_response_message(self, request_data) -> CreatedMessage:
        nsh = self._ns_hlp
        response = Soap12Envelope(
            nsh.partial_map(nsh.PM, nsh.S12, nsh.WSA, nsh.WSE))
        reply_address = request_data.message_data.p_msg.address.mk_reply_address(EventingActions.UnsubscribeResponse)
        response.set_address(reply_address)
        # response has empty body
        return CreatedMessage(response, self)

    def mk_renew_response_message(self, request_data, remaining_seconds) -> CreatedMessage:
        nsh = self._ns_hlp
        response = Soap12Envelope(nsh.partial_map(nsh.S12, nsh.WSA, nsh.WSE))
        reply_address = request_data.message_data.p_msg.address.mk_reply_address(EventingActions.RenewResponse)
        response.set_address(reply_address)
        renew_response_node = etree_.Element(nsh.wseTag('RenewResponse'))
        expires_node = etree_.SubElement(renew_response_node, nsh.wseTag('Expires'))
        expires_node.text = isoduration.duration_string(remaining_seconds)
        response.payload_element = renew_response_node
        return CreatedMessage(response, self)

    def mk_getstatus_response_message(self, request_data, remaining_seconds) -> CreatedMessage:
        nsh = self._ns_hlp
        response = Soap12Envelope(nsh.partial_map(nsh.S12, nsh.WSA, nsh.WSE))
        reply_address = request_data.message_data.p_msg.address.mk_reply_address(EventingActions.GetStatusResponse)
        response.set_address(reply_address)
        renew_response_node = etree_.Element(nsh.wseTag('GetStatusResponse'))
        expires_node = etree_.SubElement(renew_response_node, nsh.wseTag('Expires'))
        expires_node.text = isoduration.duration_string(remaining_seconds)
        response.payload_element = renew_response_node
        return CreatedMessage(response, self)

    def mk_notification_end_message(self, subscription, my_addr, code, reason) -> CreatedMessage:
        nsh = self._ns_hlp
        soap_envelope = Soap12Envelope(nsh.partial_map(nsh.S12, nsh.WSA, nsh.WSE))
        subscription_end_node = etree_.Element(nsh.wseTag('SubscriptionEnd'),
                                               nsmap=nsh.partial_map(nsh.WSE, nsh.WSA, nsh.XML))
        subscription_manager_node = etree_.SubElement(subscription_end_node, nsh.wseTag('SubscriptionManager'))
        epr = EndpointReferenceType(address=my_addr, reference_parameters=subscription.reference_parameters)
        self._mk_endpoint_reference_sub_node(epr, subscription_manager_node)
        # remark: optionally one could add own address and identifier here ...
        status_node = etree_.SubElement(subscription_end_node, nsh.wseTag('Status'))
        status_node.text = f'wse:{code}'
        reason_node = etree_.SubElement(subscription_end_node, nsh.wseTag('Reason'),
                                        attrib={nsh.xmlTag('lang'): 'en-US'})
        reason_node.text = reason
        soap_envelope.payload_element = subscription_end_node

        to_addr = subscription.end_to_address or subscription.notify_to_address
        addr = Address(addr_to=to_addr,
                       action=EventingActions.SubscriptionEnd,
                       addr_from=None,
                       reply_to=None,
                       fault_to=None,
                       reference_parameters=None)
        soap_envelope.set_address(addr)
        ref_params = subscription.end_to_ref_params or subscription.notify_ref_params
        for ref_param_node in ref_params.parameters:
            # mandatory attribute acc. to ws_addressing SOAP Binding (https://www.w3.org/TR/2006/REC-ws-addr-soap-20060509/)
            ref_param_node.set(nsh.wsaTag('IsReferenceParameter'), 'true')
            soap_envelope.add_header_element(ref_param_node)
        return CreatedMessage(soap_envelope, self)

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

    @staticmethod
    def _separate_states_by_source_mds(states) -> dict:
        lookup = defaultdict(list)
        for state in states:
            lookup[state.source_mds].append(state)
        if None in lookup:
            raise ValueError(f'States {[st.DescriptorHandle for st in lookup[None]]} have no source mds')
        return lookup

    def _fill_episodic_report_body(self, report, states):
        lookup = self._separate_states_by_source_mds(states)
        for source_mds_handle, states in lookup.items():
            report_part = report.add_report_part()
            report_part.SourceMds = source_mds_handle
            report_part.values_list.extend(states)

    def _fill_periodic_report_body(self, report, report_parts):
        for tmp in report_parts:
            lookup = self._separate_states_by_source_mds(tmp.states)
            for source_mds_handle, states in lookup.items():
                report_part = report.add_report_part()
                report_part.SourceMds = source_mds_handle
                report_part.values_list.extend(states)

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

    def mk_notification_message(self, ws_addr, message_node, reference_params: ReferenceParameters,
                                doc_nsmap) -> CreatedMessage:
        envelope = Soap12Envelope(doc_nsmap)
        envelope.payload_element = message_node
        envelope.set_address(ws_addr)
        for node in reference_params.parameters:
            envelope.add_header_element(node)
        return CreatedMessage(envelope, self)

    def _mk_this_model_sub_node(self, this_model, parent_node):
        nsh = self._ns_hlp
        this_model_node = etree_.SubElement(parent_node, nsh.dpwsTag('ThisModel'),
                                            nsmap=nsh.partial_map(nsh.DPWS))
        for lang, name in this_model.manufacturer.items():
            manufacturer_node = etree_.SubElement(this_model_node, nsh.dpwsTag('Manufacturer'))
            manufacturer_node.text = name
            if lang is not None:
                manufacturer_node.set(_LANGUAGE_ATTR, lang)

        manufacturer_url_node = etree_.SubElement(this_model_node, nsh.dpwsTag('ManufacturerUrl'))
        manufacturer_url_node.text = this_model.manufacturer_url

        for lang, name in this_model.model_name.items():
            model_name_node = etree_.SubElement(this_model_node, nsh.dpwsTag('ModelName'))
            model_name_node.text = name
            if lang is not None:
                model_name_node.set(_LANGUAGE_ATTR, lang)

        model_number_node = etree_.SubElement(this_model_node, nsh.dpwsTag('ModelNumber'))
        model_number_node.text = this_model.model_number
        model_url_node = etree_.SubElement(this_model_node, nsh.dpwsTag('ModelUrl'))
        model_url_node.text = this_model.model_url
        presentation_url_node = etree_.SubElement(this_model_node, nsh.dpwsTag('PresentationUrl'))
        presentation_url_node.text = this_model.presentation_url

    def _mk_this_device_sub_node(self, this_device, parent_node):
        nsh = self._ns_hlp
        this_device_node = etree_.SubElement(parent_node, nsh.dpwsTag('ThisDevice'),
                                             nsmap=nsh.partial_map(nsh.DPWS))
        for lang, name in this_device.friendly_name.items():
            friendly_name = etree_.SubElement(this_device_node, nsh.dpwsTag('FriendlyName'))
            friendly_name.text = name
            if lang not in (None, ''):
                friendly_name.set(_LANGUAGE_ATTR, lang)
        firmware_version = etree_.SubElement(this_device_node, nsh.dpwsTag('FirmwareVersion'))
        firmware_version.text = this_device.firmware_version
        serial_number = etree_.SubElement(this_device_node, nsh.dpwsTag('SerialNumber'))
        serial_number.text = this_device.serial_number

    def _mk_host_service_type_sub_node(self, host_service_type, parent_node):
        nsh = self._ns_hlp
        _ns = nsh.partial_map(nsh.DPWS, nsh.WSA)
        # reverse lookup( key is namespace, value is prefix)
        res = {}
        for key, value in _ns.items():
            res[value] = key
        for key, value in parent_node.nsmap.items():
            res[value] = key

        # must explicitly add namespaces of types to Host node, because list of QName is not handled by lxml
        types_texts = []
        if host_service_type.types:
            for q_name in host_service_type.types:
                prefix = res.get(q_name.namespace)
                if not prefix:
                    # create a random prefix
                    prefix = f'_dpwsh{len(_ns)}'
                    _ns[prefix] = q_name.namespace
                types_texts.append(f'{prefix}:{q_name.localname}')

        host_node = etree_.SubElement(parent_node, nsh.dpwsTag('Host'))
        ep_ref_node = etree_.SubElement(host_node, nsh.wsaTag('EndpointReference'))
        self._mk_endpoint_reference_sub_node(host_service_type.endpoint_reference, ep_ref_node)
        if types_texts:
            types_node = etree_.SubElement(host_node, nsh.dpwsTag('Types'),
                                           nsmap=_ns)  # add also namespace ns_hlp that were locally generated
            types_node.text = ' '.join(types_texts)

    def _mk_hosted_service_type_sub_node(self, hosted_service_type, parent_node):
        nsh = self._ns_hlp
        hosted_node = etree_.SubElement(parent_node, nsh.dpwsTag('Hosted'))
        ep_ref_node = etree_.SubElement(hosted_node, nsh.wsaTag('EndpointReference'))
        for ep_ref in hosted_service_type.endpoint_references:
            self._mk_endpoint_reference_sub_node(ep_ref, ep_ref_node)
        if hosted_service_type.types:
            types_text = ' '.join([nsh.doc_name_from_qname(t) for t in hosted_service_type.types])
            types_node = etree_.SubElement(hosted_node, nsh.dpwsTag('Types'))
            types_node.text = types_text
        service_node = etree_.SubElement(hosted_node, nsh.dpwsTag('ServiceId'))
        service_node.text = hosted_service_type.service_id
