import uuid
import weakref
from io import BytesIO

from lxml import etree as etree_

from .soapenvelope import Soap12Envelope
from .msgreader import validate_node
from .. import isoduration
from ..addressing import ReferenceParameters, EndpointReferenceType, Address
from ..schema_resolver import mk_schema_validator
from ..schema_resolver import SchemaResolver
from ..dpws import DeviceEventingFilterDialectURI
from ..dpws import DeviceMetadataDialectURI, DeviceRelationshipTypeURI
from ..namespaces import EventingActions
from ..namespaces import Prefixes
from ..namespaces import WSA_ANONYMOUS
from ..namespaces import msgTag, wseTag, wsaTag, wsxTag, xmlTag, s12Tag, dpwsTag, wsdTag, QN_TYPE
from ..namespaces import nsmap, DocNamespaceHelper, docname_from_qname
from ..exceptions import ApiUsageError

_LANGUAGE_ATTR = '{http://www.w3.org/XML/1998/namespace}lang'


class CreatedMessage:
    def __init__(self, message, msg_factory):
        self.p_msg = message
        self.msg_factory = msg_factory

    def serialize_message(self, pretty=False, normalized=False, request_manipulator=None, validate=True):
        return self.msg_factory.serialize_message(self, pretty, normalized, request_manipulator, validate)


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
        self._namespace_helper = DocNamespaceHelper()
        self._xml_schema = mk_schema_validator(SchemaResolver(sdc_definitions))

    def register_mdib(self, mdib):
        """Factory sometimes must know the mdib data (e.g. Set service, activate method).
        :param mdib: the current mdib
        """
        if mdib is not None and self._mdib_wref is not None:
            raise ApiUsageError('MessageFactory has already an registered mdib')
        self._mdib_wref = None if mdib is None else weakref.ref(mdib)

    def serialize_message(self, message: CreatedMessage, pretty=False, normalized=False,
                          request_manipulator=None, validate=True) -> bytes:
        """

        :param message: a soap envelope
        :param pretty:
        :param normalized: id True, the internal namespaces are used (__BICEPS_MessageModel__, .... )
        :param request_manipulator: can modify data before sending
        :param validate: if False, no validation is performed, independent of constructor setting
        :return: bytes
        """
        p_msg = message.p_msg
        tmp = BytesIO()
        root = etree_.Element(s12Tag('Envelope'), nsmap=p_msg.nsmap)

        header_node = etree_.SubElement(root, s12Tag('Header'))
        if p_msg.address:
            self._mk_header_address(p_msg.address, header_node)
        header_node.extend(p_msg.header_nodes)
        body_node = etree_.SubElement(root, s12Tag('Body'), nsmap=p_msg.nsmap)
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
        ret = tmp.getvalue()
        if normalized:
            return ret
        return self._sdc_definitions.denormalize_xml_text(ret)

    def mk_fault_message(self, message_data, soap_fault,
                         action_string=f'{Prefixes.WSA.namespace}/fault') -> CreatedMessage:
        soap_envelope = Soap12Envelope(Prefixes.partial_map(Prefixes.S12, Prefixes.WSA, Prefixes.WSE))
        reply_address = message_data.p_msg.address.mk_reply_address(action_string)
        soap_envelope.set_address(reply_address)
        fault_node = etree_.Element(s12Tag('Fault'))
        code_node = etree_.SubElement(fault_node, s12Tag('Code'))
        value_node = etree_.SubElement(code_node, s12Tag('Value'))
        value_node.text = f's12:{soap_fault.code}'
        if soap_fault.sub_code is not None:
            subcode_node = etree_.SubElement(code_node, s12Tag('Subcode'))
            sub_value_node = etree_.SubElement(subcode_node, s12Tag('Value'))
            sub_value_node.text = docname_from_qname(soap_fault.sub_code, nsmap)
        reason_node = etree_.SubElement(fault_node, s12Tag('Reason'))
        reason_text_node = etree_.SubElement(reason_node, s12Tag('Text'))
        reason_text_node.set(xmlTag('lang'), 'en-US')
        reason_text_node.text = soap_fault.reason
        if soap_fault.details is not None:
            detail_node = etree_.SubElement(fault_node, s12Tag('Detail'))
            detail_node.set(xmlTag('lang'), 'en-US')
            det_data_node = etree_.SubElement(detail_node, 'data')
            det_data_node.text = soap_fault.details
        soap_envelope.payload_element = fault_node
        return CreatedMessage(soap_envelope, self)

    @staticmethod
    def _mk_endpoint_reference_sub_node(endpoint_reference, parent_node):
        node = etree_.SubElement(parent_node, wsaTag('Address'))
        node.text = endpoint_reference.address
        if endpoint_reference.reference_parameters.has_parameters:
            reference_parameters_node = etree_.SubElement(parent_node, wsaTag('ReferenceParameters'))
            reference_parameters_node.extend(endpoint_reference.reference_parameters.parameters)
        # ToDo: what about this metadata thing???
        # if self.metadata_node is not None:
        #    root_node.append(self.metadata_node)

    @staticmethod
    def _mk_header_address(address, header_node):
        # To (OPTIONAL), defaults to anonymous
        node = etree_.SubElement(header_node, wsaTag('To'), attrib={s12Tag('mustUnderstand'): 'true'})
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
        node = etree_.SubElement(header_node, wsaTag('Action'), attrib={s12Tag('mustUnderstand'): 'true'})
        node.text = address.action
        # MessageID (OPTIONAL)
        if address.message_id:
            node = etree_.SubElement(header_node, wsaTag('MessageID'))
            node.text = address.message_id
        # RelatesTo (OPTIONAL)
        if address.relates_to:
            node = etree_.SubElement(header_node, wsaTag('RelatesTo'))
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
        envelope = Soap12Envelope(nsmap)
        envelope.set_address(Address(action=f'{Prefixes.WXF.namespace}/Get',
                                     addr_to=addr_to))
        return CreatedMessage(envelope, self)

    def mk_get_metadata_message(self, addr_to) -> CreatedMessage:
        soap_envelope = Soap12Envelope(nsmap)
        soap_envelope.set_address(
            Address(action='http://schemas.xmlsoap.org/ws/2004/09/mex/GetMetadata/Request',
                    addr_to=addr_to))
        soap_envelope.payload_element = etree_.Element('{http://schemas.xmlsoap.org/ws/2004/09/mex}GetMetadata')
        return CreatedMessage(soap_envelope, self)


    def mk_get_descriptor_message(self, addr_to, port_type, requested_handles) -> CreatedMessage:
        """
        :param addr_to: to-field value in address
        :param port_type: needed to construct the action string
        :param requested_handles: a list of strings
        :return: a SoapEnvelope
        """
        method = 'GetMdState'
        return self._mk_get_method_message(addr_to, port_type, method, params=self._handles2params(requested_handles))

    def mk_get_mdib_message(self, addr_to, port_type) -> CreatedMessage:
        """
        :param addr_to: to-field value in address
        :param port_type: needed to construct the action string
        :return: a SoapEnvelope
        """
        method = 'GetMdib'
        return self._mk_get_method_message(addr_to, port_type, method)

    def mk_get_mddescription_message(self, addr_to, port_type, requested_handles=None) -> CreatedMessage:
        """
        :param addr_to: to-field value in address
        :param port_type: needed to construct the action string
        :param requested_handles: a list of strings
        :return: a SoapEnvelope
        """
        method = 'GetMdDescription'
        return self._mk_get_method_message(addr_to, port_type, method, params=self._handles2params(requested_handles))

    def mk_get_mdstate_message(self, addr_to, port_type, requested_handles=None) -> CreatedMessage:
        """
        :param addr_to: to-field value in address
        :param port_type: needed to construct the action string
        :param requested_handles: a list of strings
        :return: a SoapEnvelope
        """
        method = 'GetMdState'
        return self._mk_get_method_message(addr_to, port_type, method, params=self._handles2params(requested_handles))

    def mk_get_containmenttree_message(self, addr_to, port_type, requested_handles) -> CreatedMessage:
        """
        :param addr_to: to-field value in address
        :param port_type: needed to construct the action string
        :param requested_handles: a list of strings
        :return: a SoapEnvelope
        """
        method = 'GetContainmentTree'
        return self._mk_get_method_message(addr_to, port_type, method, params=self._handles2params(requested_handles))

    def mk_get_contextstates_message(self, addr_to, port_type, requested_handles=None) -> CreatedMessage:
        """
        :param addr_to: to-field value in address
        :param port_type: needed to construct the action string
        :param requested_handles: a list of strings
        :return: a SoapEnvelope
        """
        requestparams = []
        msg_names = self._sdc_definitions.data_model.msg_names
        if requested_handles:
            for handle in requested_handles:
                requestparams.append(etree_.Element(msg_names.HandleRef,
                                                    attrib={QN_TYPE: f'{Prefixes.MSG.prefix}:HandleRef'},
                                                    nsmap=Prefixes.partial_map(Prefixes.MSG, Prefixes.PM)))
                requestparams[-1].text = handle
        method = 'GetContextStates'
        return self._mk_get_method_message(addr_to, port_type, method, params=requestparams)

    def mk_get_contextstates_by_identification_message(self, addr_to, port_type, identifications) -> CreatedMessage:
        """
        :param addr_to: to-field value in address
        :param port_type: needed to construct the action string
        :param identifications: list of identifiers (type: InstanceIdentifier from pmtypes)
        :return: a SoapEnvelope
        """
        msg_names = self._sdc_definitions.data_model.msg_names
        requestparams = []
        if identifications:
            for identification in identifications:
                requestparams.append(identification.as_etree_node(
                    qname=msg_names.Identification, nsmap=Prefixes.partial_map(Prefixes.MSG, Prefixes.PM)))
        method = 'GetContextStatesByIdentification'
        return self._mk_get_method_message(addr_to, port_type, method, params=requestparams)

    def mk_set_value_message(self, addr_to, port_type, operation_handle,
                             requested_numeric_value) -> CreatedMessage:
        """
        :param addr_to: to-field value in address
        :param port_type: needed to construct the action string
        :param operation_handle: the handle of operation that is called
        :param requested_numeric_value: a string
        :return: a SoapEnvelope
        """
        msg_names = self._sdc_definitions.data_model.msg_names
        requested_value_node = etree_.Element(msg_names.RequestedNumericValue,
                                              attrib={QN_TYPE: f'{Prefixes.XSD.prefix}:decimal'})
        requested_value_node.text = str(requested_numeric_value)
        method = 'SetValue'
        return self._mk_set_method_message(addr_to, port_type, method,
                                           operation_handle,
                                           [requested_value_node],
                                           additional_namespaces=[Prefixes.XSD])

    def mk_set_string_message(self, addr_to, port_type, operation_handle, requested_string) -> CreatedMessage:
        """
        :param addr_to: to-field value in address
        :param port_type: needed to construct the action string
        :param operation_handle: the handle of operation that is called
        :param requested_string: a string
        :return: a SoapEnvelope
        """
        msg_names = self._sdc_definitions.data_model.msg_names
        requested_string_node = etree_.Element(msg_names.RequestedStringValue,
                                               attrib={QN_TYPE: f'{Prefixes.XSD.prefix}:string'})
        requested_string_node.text = requested_string
        method = 'SetString'
        return self._mk_set_method_message(addr_to, port_type, method, operation_handle, [requested_string_node],
                                           additional_namespaces=[Prefixes.XSD])

    def mk_set_alert_message(self, nsmapper, addr_to, port_type, operation_handle,
                             proposed_alert_states) -> CreatedMessage:
        """
        :param nsmapper:
        :param addr_to: to-field value in address
        :param port_type: needed to construct the action string
        :param operation_handle: the handle of operation that is called
        :param proposed_alert_states: a list of AbstractAlertStateContainer or derived class
        :return: a SoapEnvelope
        """
        msg_names = self._sdc_definitions.data_model.msg_names
        _proposed_states = [p.mk_copy() for p in proposed_alert_states]
        for state in _proposed_states:
            state.nsmapper = DocNamespaceHelper()  # use my namespaces
        _proposed_state_nodes = [p.mk_state_node(msg_names.ProposedAlertState, nsmapper) for p in _proposed_states]
        method = 'SetAlertState'
        return self._mk_set_method_message(addr_to, port_type, method, operation_handle, _proposed_state_nodes)

    def mk_set_metric_state_message(self, nsmapper, addr_to, port_type, operation_handle,
                                    proposed_metric_states) -> CreatedMessage:
        """
        :param nsmapper:
        :param addr_to: to-field value in address
        :param port_type: needed to construct the action string
        :param operation_handle: the handle of operation that is called
        :param proposed_metric_states: a list of AbstractMetricStateContainer or derived class
        :return: a SoapEnvelope
        """
        msg_names = self._sdc_definitions.data_model.msg_names
        _proposed_states = [p.mk_copy() for p in proposed_metric_states]
        for state in _proposed_states:
            state.nsmapper = nsmapper  # use my namespaces
        _proposed_state_nodes = [p.mk_state_node(msg_names.ProposedMetricState, nsmapper) for p in _proposed_states]
        return self._mk_set_method_message(addr_to, port_type, 'SetMetricState', operation_handle,
                                           _proposed_state_nodes)

    def mk_set_component_state_message(self, nsmapper, addr_to, port_type, operation_handle,
                                       proposed_component_states) -> CreatedMessage:
        """
        :param nsmapper:
        :param addr_to: to-field value in address
        :param port_type: needed to construct the action string
        :param operation_handle: the handle of operation that is called
        :param proposed_component_states: a list of AbstractComponentStateContainers or derived class
        :return: a SoapEnvelope
        """
        msg_names = self._sdc_definitions.data_model.msg_names
        _proposed_states = [p.mk_copy() for p in proposed_component_states]
        for state in _proposed_states:
            state.nsmapper = nsmapper  # use my namespaces
        _proposed_state_nodes = [p.mk_state_node(msg_names.ProposedComponentState, nsmapper) for p in _proposed_states]
        return self._mk_set_method_message(addr_to, port_type, 'SetComponentState', operation_handle,
                                           _proposed_state_nodes)

    def mk_set_context_state_message(self, nsmapper, addr_to, port_type, operation_handle,
                                     proposed_context_states) -> CreatedMessage:
        """
        :param nsmapper:
        :param addr_to: to-field value in address
        :param port_type: needed to construct the action string
        :param operation_handle: the handle of operation that is called
        :param proposed_context_states: a list of AbstractContextStateContainers or derived class
        :return: a SoapEnvelope
        """
        msg_names = self._sdc_definitions.data_model.msg_names
        _proposed_states = [p.mk_copy() for p in proposed_context_states]
        for state in _proposed_states:
            # BICEPS: if handle == DescriptorHandle, it means insert.
            if state.Handle is None:
                state.Handle = state.DescriptorHandle
            state.nsmapper = DocNamespaceHelper()  # use my namespaces
        _proposed_state_nodes = [p.mk_state_node(msg_names.ProposedContextState,
                                                 nsmapper) for p in _proposed_states]

        return self._mk_set_method_message(addr_to, port_type, 'SetContextState', operation_handle,
                                           _proposed_state_nodes)

    def mk_activate_message(self, addr_to, port_type, operation_handle, arguments=None) -> CreatedMessage:
        """
        :param addr_to: to-field value in address
        :param port_type: needed to construct the action string
        :param operation_handle: the handle of operation that is called
        :param arguments: a list of strings or None
        :return: a SoapEnvelope
        """
        msg_names = self._sdc_definitions.data_model.msg_names
        payload_node = etree_.Element(msg_names.Activate, attrib=None, nsmap=nsmap)
        ref = etree_.SubElement(payload_node, msg_names.OperationHandleRef)
        ref.text = operation_handle
        if arguments is not None:
            for argument in arguments:
                argument_node = etree_.SubElement(payload_node, msg_names.Argument)
                arg_val = etree_.SubElement(argument_node, msg_names.ArgValue)
                arg_val.text = argument
        action = self.get_action_string(port_type, 'Activate')
        envelope = Soap12Envelope(Prefixes.partial_map(Prefixes.MSG))
        envelope.set_address(Address(action=action, addr_to=addr_to))
        envelope.payload_element = payload_node
        return CreatedMessage(envelope, self)

    def mk_get_localized_text_message(self, addr_to, port_type, refs=None, version=None, langs=None,
                                      text_widths=None, number_of_lines=None) -> CreatedMessage:
        """
        :param addr_to: to-field value in address
        :param port_type: needed to construct the action string
        :param refs: a list of strings or None
        :param version: an unsigned integer or None
        :param langs: a list of strings or None
        :param text_widths: a list of strings or None (each string one of xs, s, m, l, xs, xxs)
        :param number_of_lines: a list of unsigned integers or None
        :return: a SoapEnvelope
        """
        msg_names = self._sdc_definitions.data_model.msg_names
        requestparams = []
        if refs is not None:
            for ref in refs:
                node = etree_.Element(msg_names.Ref)
                node.text = ref
                requestparams.append(node)
        if version is not None:
            node = etree_.Element(msg_names.Version)
            node.text = str(version)
            requestparams.append(node)
        if langs is not None:
            for lang in langs:
                node = etree_.Element(msg_names.Lang)
                node.text = lang
                requestparams.append(node)
        if text_widths is not None:
            for text_width in text_widths:
                node = etree_.Element(msg_names.TextWidth)
                node.text = text_width
                requestparams.append(node)
        if number_of_lines is not None:
            for nol in number_of_lines:
                node = etree_.Element(msg_names.NumberOfLines)
                node.text = nol
                requestparams.append(node)
        method = 'GetLocalizedText'
        return self._mk_get_method_message(addr_to, port_type, method, params=requestparams)

    def mk_get_supported_languages_message(self, addr_to, port_type) -> CreatedMessage:
        """
        :param addr_to: to-field value in address
        :param port_type: needed to construct the action string
        :return: a SoapEnvelope
        """
        method = 'GetSupportedLanguages'
        return self._mk_get_method_message(addr_to, port_type, method)

    def mk_subscribe_message(self, addr_to,
                             notifyto_url, notify_to_identifier,
                             endto_url, endto_identifier,
                             expire_minutes, subscribe_filter) -> CreatedMessage:
        soap_envelope = Soap12Envelope(Prefixes.partial_map(Prefixes.WSE))
        soap_envelope.set_address(Address(action=EventingActions.Subscribe, addr_to=addr_to))
        if notify_to_identifier is None:
            notify_to = EndpointReferenceType(notifyto_url, reference_parameters=None)
        else:
            notify_to = EndpointReferenceType(notifyto_url,
                                              reference_parameters=ReferenceParameters([notify_to_identifier]))

        if endto_identifier is None:
            end_to = EndpointReferenceType(endto_url, reference_parameters=None)
        else:
            end_to = EndpointReferenceType(endto_url, reference_parameters=ReferenceParameters([endto_identifier]))

        subscribe_node = etree_.Element(wseTag('Subscribe'),
                                        nsmap=Prefixes.partial_map(Prefixes.WSE, Prefixes.WSA))
        if end_to is not None:
            end_to_node = etree_.SubElement(subscribe_node, wseTag('EndTo'))
            self._mk_endpoint_reference_sub_node(end_to, end_to_node)
        delivery = etree_.SubElement(subscribe_node, wseTag('Delivery'))
        delivery.set('Mode', f'{Prefixes.WSE.namespace}/DeliveryModes/Push')

        notify_to_node = etree_.SubElement(delivery, wseTag('NotifyTo'))
        self._mk_endpoint_reference_sub_node(notify_to, notify_to_node)

        exp = etree_.SubElement(subscribe_node, wseTag('Expires'))
        exp.text = isoduration.duration_string(expire_minutes * 60)
        fil = etree_.SubElement(subscribe_node, wseTag('Filter'))
        fil.set('Dialect', DeviceEventingFilterDialectURI.ACTION)
        fil.text = subscribe_filter
        soap_envelope.payload_element = subscribe_node
        return CreatedMessage(soap_envelope, self)

    def mk_renew_message(self, addr_to: str, dev_reference_param: ReferenceParameters,
                         expire_minutes: int) -> CreatedMessage:
        soap_envelope = Soap12Envelope(Prefixes.partial_map(Prefixes.WSE))
        soap_envelope.set_address(Address(action=EventingActions.Renew, addr_to=addr_to))
        self._add_reference_params_to_header(soap_envelope, dev_reference_param)
        renew_node = etree_.Element(wseTag('Renew'), nsmap=Prefixes.partial_map(Prefixes.WSE))
        expires_node = etree_.SubElement(renew_node, wseTag('Expires'), nsmap=Prefixes.partial_map(Prefixes.WSE))
        expires_node.text = isoduration.duration_string(expire_minutes * 60)
        soap_envelope.payload_element = renew_node
        return CreatedMessage(soap_envelope, self)

    def mk_get_status_message(self, addr_to: str, dev_reference_param: ReferenceParameters) -> CreatedMessage:
        soap_envelope = Soap12Envelope(Prefixes.partial_map(Prefixes.WSE))
        soap_envelope.set_address(
            Address(action=EventingActions.GetStatus, addr_to=addr_to))
        self._add_reference_params_to_header(soap_envelope, dev_reference_param)
        soap_envelope.payload_element = etree_.Element(wseTag('GetStatus'))
        return CreatedMessage(soap_envelope, self)

    def mk_unsubscribe_message(self, addr_to: str, dev_reference_param: ReferenceParameters) -> CreatedMessage:
        soap_envelope = Soap12Envelope(Prefixes.partial_map(Prefixes.WSE))
        soap_envelope.set_address(Address(action=EventingActions.Unsubscribe, addr_to=addr_to))
        self._add_reference_params_to_header(soap_envelope, dev_reference_param)
        soap_envelope.payload_element = etree_.Element(wseTag('Unsubscribe'))
        return CreatedMessage(soap_envelope, self)

    def get_action_string(self, port_type, method_name):
        actions_lookup = self._sdc_definitions.Actions
        try:
            return getattr(actions_lookup, method_name)
        except AttributeError:  # fallback, if a definition is missing
            return f'{self._sdc_definitions.ActionsNamespace}/{port_type}/{method_name}'

    def _mk_get_method_message(self, addr_to, port_type, method_name, params=None) -> CreatedMessage:
        get_node = etree_.Element(msgTag(method_name))
        soap_envelope = Soap12Envelope(Prefixes.partial_map(Prefixes.MSG))
        action_string = self.get_action_string(port_type, method_name)
        soap_envelope.set_address(Address(action=action_string, addr_to=addr_to))
        if params:
            for param in params:
                get_node.append(param)
        soap_envelope.payload_element = get_node
        return CreatedMessage(soap_envelope, self)

    def _mk_set_method_message(self, addr_to, port_type, method_name, operation_handle, request_nodes,
                               additional_namespaces=None) -> CreatedMessage:
        """ helper to create the soap envelope
        :param addr_to: to-field value in address
        :param port_type: needed to construct the action string
        :param method_name: last element of name of the called action
        :param operation_handle: handle name as string
        :param request_nodes: a list of etree_ nodes that will become sub-element of Method name element
        """
        msg_names = self._sdc_definitions.data_model.msg_names
        set_node = etree_.Element(msgTag(method_name))
        ref = etree_.SubElement(set_node, msg_names.OperationHandleRef,
                                attrib={QN_TYPE: f'{Prefixes.PM.prefix}:HandleRef'},
                                nsmap=Prefixes.partial_map(Prefixes.PM))
        ref.text = operation_handle
        for node in request_nodes:
            set_node.append(node)
        if additional_namespaces:
            my_ns = Prefixes.partial_map(Prefixes.S12, Prefixes.WSA, Prefixes.PM, Prefixes.MSG, *additional_namespaces)
        else:
            my_ns = Prefixes.partial_map(Prefixes.S12, Prefixes.WSA, Prefixes.PM, Prefixes.MSG)

        soap_envelope = Soap12Envelope(my_ns)
        action_string = self.get_action_string(port_type, method_name)
        soap_envelope.set_address(Address(action=action_string, addr_to=addr_to))
        soap_envelope.payload_element = set_node
        return CreatedMessage(soap_envelope, self)

    def _handles2params(self, handles):
        """
        Internal helper, converts handles to dom elements
        :param handles: a list of strings
        :return: a list of etree nodes
        """
        msg_names = self._sdc_definitions.data_model.msg_names
        params = []
        if handles is not None:
            for handle in handles:
                node = etree_.Element(msg_names.HandleRef)
                node.text = handle
                params.append(node)
        return params


class MessageFactoryDevice(MessageFactory):
    """This class creates all messages that a device needs to create"""

    def mk_get_metadata_response_message(self, message_data, this_device, this_model,
                                         dpws_host, dpws_hosted_services) -> CreatedMessage:
        response = Soap12Envelope(Prefixes.partial_map(Prefixes.WXF))
        reply_address = message_data.p_msg.address.mk_reply_address(f'{Prefixes.WXF.namespace}/GetResponse')
        reply_address.addr_to = WSA_ANONYMOUS
        reply_address.message_id = uuid.uuid4().urn
        response.set_address(reply_address)
        metadata_node = etree_.Element(wsxTag('Metadata'),
                                       nsmap=Prefixes.partial_map(Prefixes.MSG, Prefixes.SDC, Prefixes.DPWS,
                                                                  Prefixes.WSX))

        # ThisModel
        metadata_section_node = etree_.SubElement(metadata_node,
                                                  wsxTag('MetadataSection'),
                                                  attrib={'Dialect': DeviceMetadataDialectURI.THIS_MODEL})
        self._mk_this_model_sub_node(this_model, metadata_section_node)

        # ThisDevice
        metadata_section_node = etree_.SubElement(metadata_node,
                                                  wsxTag('MetadataSection'),
                                                  attrib={'Dialect': DeviceMetadataDialectURI.THIS_DEVICE})
        self._mk_this_device_sub_node(this_device, metadata_section_node)

        # Relationship
        metadata_section_node = etree_.SubElement(metadata_node,
                                                  wsxTag('MetadataSection'),
                                                  attrib={'Dialect': DeviceMetadataDialectURI.RELATIONSHIP})
        relationship_node = etree_.SubElement(metadata_section_node,
                                              dpwsTag('Relationship'),
                                              attrib={'Type': DeviceRelationshipTypeURI.HOST})
        self._mk_host_service_type_sub_node(dpws_host, relationship_node)

        # add all hosted services:
        for service in dpws_hosted_services:
            hosted_service_type = service.mk_dpws_hosted_instance()
            self._mk_hosted_service_type_sub_node(hosted_service_type, relationship_node)
        response.payload_element = metadata_node
        return CreatedMessage(response, self)

    def mk_hosted_get_metadata_response_message(self, message_data, dpws_host,
                                                hosted_service_type, location_text) -> CreatedMessage:
        response = Soap12Envelope()
        reply_address = message_data.p_msg.address.mk_reply_address(
            'http://schemas.xmlsoap.org/ws/2004/09/mex/GetMetadata/Response')
        response.set_address(reply_address)

        metadata_node = etree_.Element(wsxTag('Metadata'),
                                       nsmap=(Prefixes.partial_map(Prefixes.WXF, Prefixes.SDC)))

        # Relationship
        metadata_section_node = etree_.SubElement(metadata_node,
                                                  wsxTag('MetadataSection'),
                                                  attrib={'Dialect': DeviceMetadataDialectURI.RELATIONSHIP})

        relationship_node = etree_.SubElement(metadata_section_node,
                                              dpwsTag('Relationship'),
                                              attrib={'Type': DeviceRelationshipTypeURI.HOST})
        self._mk_host_service_type_sub_node(dpws_host, relationship_node)

        self._mk_hosted_service_type_sub_node(hosted_service_type, relationship_node)

        metadata_section_node = etree_.SubElement(metadata_node,
                                                  wsxTag('MetadataSection'),
                                                  attrib={'Dialect': Prefixes.WSDL.namespace})
        location_node = etree_.SubElement(metadata_section_node,
                                          wsxTag('Location'))
        location_node.text = location_text
        response.payload_element = metadata_node
        return CreatedMessage(response, self)

    def mk_probe_matches_response_message(self, message_data, addresses) -> CreatedMessage:
        response = Soap12Envelope()
        reply_address = message_data.p_msg.address.mk_reply_address('{Prefixes.WSD.namespace}/ProbeMatches')
        reply_address.addr_to = WSA_ANONYMOUS
        reply_address.message_id = uuid.uuid4().urn
        response.set_address(reply_address)
        probe_match_node = etree_.Element(wsdTag('Probematch'),
                                          nsmap=Prefixes.partial_map(Prefixes.WSD, Prefixes.DPWS, Prefixes.MDPWS))
        types = etree_.SubElement(probe_match_node, wsdTag('Types'))
        types.text = f'{Prefixes.DPWS.prefix}:Device {Prefixes.MDPWS.prefix}:MedicalDevice'
        scopes = etree_.SubElement(probe_match_node, wsdTag('Scopes'))
        scopes.text = ''
        xaddrs = etree_.SubElement(probe_match_node, wsdTag('XAddrs'))
        xaddrs.text = ' '.join(addresses)
        response.payload_element = probe_match_node
        return CreatedMessage(response, self)

    def mk_get_mdib_response_message(self, message_data, mdib, include_context_states) -> CreatedMessage:
        msg_names = self._sdc_definitions.data_model.msg_names
        request = message_data.p_msg
        response = Soap12Envelope(
            Prefixes.partial_map(Prefixes.S12, Prefixes.WSA, Prefixes.PM, Prefixes.MSG, default=Prefixes.PM))
        reply_address = request.address.mk_reply_address(
            action=self._get_action_string(mdib.sdc_definitions, 'GetMdibResponse'))
        response.set_address(reply_address)
        if include_context_states:
            mdib_node = mdib.reconstruct_mdib_with_context_states()
        else:
            mdib_node = mdib.reconstruct_mdib()
        mdib_version_string = mdib_node.get('MdibVersion')  # use same version a in mdib node for response
        sequence_id_string = mdib_node.get('SequenceId')

        get_mdib_response_node = etree_.Element(msg_names.GetMdibResponse,
                                                nsmap=Prefixes.partial_map(Prefixes.MSG,
                                                                           Prefixes.PM,
                                                                           Prefixes.XSI))
        if mdib_version_string:
            get_mdib_response_node.set('MdibVersion', mdib_version_string)
        get_mdib_response_node.set('SequenceId', sequence_id_string)
        get_mdib_response_node.append(mdib_node)
        response.payload_element = get_mdib_response_node
        return CreatedMessage(response, self)

    def mk_get_mddescription_response_message(self, message_data, mdib, requested_handles) -> CreatedMessage:
        """For simplification reason this implementation returns either all descriptors or none."""
        msg_names = self._sdc_definitions.data_model.msg_names
        request = message_data.p_msg
        return_all = len(requested_handles) == 0  # if we have handles, we need to check them
        my_namespaces = mdib.nsmapper.partial_map(Prefixes.S12, Prefixes.WSA, Prefixes.MSG, Prefixes.PM)
        response_envelope = Soap12Envelope(my_namespaces)
        reply_address = request.address.mk_reply_address(
            action=self._get_action_string(mdib.sdc_definitions, 'GetMdDescriptionResponse'))
        response_envelope.set_address(reply_address)

        response_node = etree_.Element(msg_names.GetMdDescriptionResponse, nsmap=nsmap)

        for handle in requested_handles:
            # if at least one requested handle is valid, return all.
            if mdib.descriptions.handle.get_one(handle, allow_none=True) is not None:
                return_all = True
                break
        if return_all:
            md_description_node, mdib_version = mdib.reconstruct_md_description()
            md_description_node.tag = msg_names.MdDescription  # rename according to message
            mdib_version_string = str(mdib_version)
        else:
            md_description_node = etree_.Element(msg_names.MdDescription)
            mdib_version_string = None
        sequence_id_string = mdib.sequence_id
        if mdib_version_string:
            response_node.set('MdibVersion', mdib_version_string)
        response_node.set('SequenceId', sequence_id_string)
        response_node.append(md_description_node)
        response_envelope.payload_element = response_node
        return CreatedMessage(response_envelope, self)

    def mk_get_mdstate_response_message(self, message_data, action, mdib_version, sequence_id,
                                        state_containers) -> CreatedMessage:
        msg_names = self._sdc_definitions.data_model.msg_names
        pm_names = self._sdc_definitions.data_model.pm_names
        request = message_data.p_msg
        response_envelope = Soap12Envelope(
            Prefixes.partial_map(Prefixes.S12, Prefixes.WSA, Prefixes.PM, Prefixes.MSG, default=Prefixes.PM))
        reply_address = request.address.mk_reply_address(
            action=action)
        response_envelope.set_address(reply_address)
        response_node = etree_.Element(msg_names.GetMdStateResponse, nsmap=nsmap)
        response_node.set('MdibVersion', str(mdib_version))
        response_node.set('SequenceId', sequence_id)
        md_state_node = etree_.Element(msg_names.MdState, attrib=None,
                                       nsmap=Prefixes.partial_map(Prefixes.MSG, Prefixes.PM))
        for state_container in state_containers:
            md_state_node.append(state_container.mk_state_node(pm_names.State,
                                                               self._namespace_helper))

        response_node.append(md_state_node)
        response_envelope.payload_element = response_node
        return CreatedMessage(response_envelope, self)

    def mk_get_context_states_response_message(self, message_data, action, mdib_version, sequence_id, state_containers,
                                               nsmapper) -> CreatedMessage:
        msg_names = self._sdc_definitions.data_model.msg_names
        response = Soap12Envelope(
            nsmapper.partial_map(Prefixes.S12, Prefixes.WSA, Prefixes.PM, Prefixes.MSG))
        reply_address = message_data.p_msg.address.mk_reply_address(
            action=action)
        response.set_address(reply_address)
        response_node = etree_.Element(msg_names.GetContextStatesResponse)
        response_node.set('MdibVersion', str(mdib_version))
        response_node.set('SequenceId', sequence_id)
        tag = msg_names.ContextState
        for container in state_containers:
            node = container.mk_state_node(tag, nsmapper)
            response_node.append(node)
        response.payload_element = response_node
        return CreatedMessage(response, self)

    def mk_get_localized_texts_response_message(self, message_data, action, mdib_version, sequence_id, texts,
                                                nsmapper) -> CreatedMessage:
        msg_names = self._sdc_definitions.data_model.msg_names
        response_envelope = Soap12Envelope(
            nsmapper.partial_map(Prefixes.S12, Prefixes.WSA, Prefixes.PM, Prefixes.MSG))
        reply_address = message_data.p_msg.address.mk_reply_address(action=action)
        response_envelope.set_address(reply_address)
        response_node = etree_.Element(msg_names.GetLocalizedTextResponse)
        response_node.set('MdibVersion', str(mdib_version))
        response_node.set('SequenceId', sequence_id)

        for text in texts:
            response_node.append(text.as_etree_node(msg_names.Text, nsmap=None))
        response_envelope.payload_element = response_node
        return CreatedMessage(response_envelope, self)

    def mk_get_supported_languages_response_message(self, message_data, action, mdib_version, sequence_id, languages,
                                                    nsmapper) -> CreatedMessage:
        msg_names = self._sdc_definitions.data_model.msg_names
        response_envelope = Soap12Envelope(
            nsmapper.partial_map(Prefixes.S12, Prefixes.WSA, Prefixes.PM, Prefixes.MSG))
        reply_address = message_data.p_msg.address.mk_reply_address(action=action)
        response_envelope.set_address(reply_address)
        response_node = etree_.Element(msg_names.GetSupportedLanguagesResponse)
        response_node.set('MdibVersion', str(mdib_version))
        response_node.set('SequenceId', sequence_id)

        for lang in languages:
            node = etree_.SubElement(response_node, msg_names.Lang)
            node.text = lang
        response_envelope.payload_element = response_node
        return CreatedMessage(response_envelope, self)

    def mk_subscribe_response_message(self, request_data, subscription, base_urls) -> CreatedMessage:
        response = Soap12Envelope(
            Prefixes.partial_map(Prefixes.PM, Prefixes.S12, Prefixes.WSA, Prefixes.WSE))
        reply_address = request_data.message_data.p_msg.address.mk_reply_address(EventingActions.SubscribeResponse)
        response.set_address(reply_address)
        subscribe_response_node = etree_.Element(wseTag('SubscribeResponse'))
        subscription_manager_node = etree_.SubElement(subscribe_response_node, wseTag('SubscriptionManager'))
        path = '/'.join(request_data.consumed_path_elements)
        path_suffix = '' if subscription.path_suffix is None else f'/{subscription.path_suffix}'
        subscription_address = f'{base_urls[0].scheme}://{base_urls[0].netloc}/{path}{path_suffix}'
        epr = EndpointReferenceType(address=subscription_address,
                                    reference_parameters=subscription.reference_parameters)
        self._mk_endpoint_reference_sub_node(epr, subscription_manager_node)
        expires_node = etree_.SubElement(subscribe_response_node, wseTag('Expires'))
        expires_node.text = subscription.expire_string  # simply confirm request
        response.payload_element = subscribe_response_node
        ret = CreatedMessage(response, self)
        self._logger.debug('on_subscribe_request returns {}', lambda: self.serialize_message(ret).decode('utf-8'))
        return ret

    def mk_unsubscribe_response_message(self, request_data) -> CreatedMessage:
        response = Soap12Envelope(
            Prefixes.partial_map(Prefixes.PM, Prefixes.S12, Prefixes.WSA, Prefixes.WSE))
        reply_address = request_data.message_data.p_msg.address.mk_reply_address(EventingActions.UnsubscribeResponse)
        response.set_address(reply_address)
        # response has empty body
        return CreatedMessage(response, self)

    def mk_renew_response_message(self, request_data, remaining_seconds) -> CreatedMessage:
        response = Soap12Envelope(Prefixes.partial_map(Prefixes.S12, Prefixes.WSA, Prefixes.WSE))
        reply_address = request_data.message_data.p_msg.address.mk_reply_address(EventingActions.RenewResponse)
        response.set_address(reply_address)
        renew_response_node = etree_.Element(wseTag('RenewResponse'))
        expires_node = etree_.SubElement(renew_response_node, wseTag('Expires'))
        expires_node.text = isoduration.duration_string(remaining_seconds)
        response.payload_element = renew_response_node
        return CreatedMessage(response, self)

    def mk_getstatus_response_message(self, request_data, remaining_seconds) -> CreatedMessage:
        response = Soap12Envelope(Prefixes.partial_map(Prefixes.S12, Prefixes.WSA, Prefixes.WSE))
        reply_address = request_data.message_data.p_msg.address.mk_reply_address(EventingActions.GetStatusResponse)
        response.set_address(reply_address)
        renew_response_node = etree_.Element(wseTag('GetStatusResponse'))
        expires_node = etree_.SubElement(renew_response_node, wseTag('Expires'))
        expires_node.text = isoduration.duration_string(remaining_seconds)
        response.payload_element = renew_response_node
        return CreatedMessage(response, self)

    def mk_notification_end_message(self, subscription, my_addr, code, reason) -> CreatedMessage:
        soap_envelope = Soap12Envelope(Prefixes.partial_map(Prefixes.S12, Prefixes.WSA, Prefixes.WSE))
        subscription_end_node = etree_.Element(wseTag('SubscriptionEnd'),
                                               nsmap=Prefixes.partial_map(Prefixes.WSE, Prefixes.WSA, Prefixes.XML))
        subscription_manager_node = etree_.SubElement(subscription_end_node, wseTag('SubscriptionManager'))
        epr = EndpointReferenceType(address=my_addr, reference_parameters=subscription.reference_parameters)
        self._mk_endpoint_reference_sub_node(epr, subscription_manager_node)
        # remark: optionally one could add own address and identifier here ...
        status_node = etree_.SubElement(subscription_end_node, wseTag('Status'))
        status_node.text = f'wse:{code}'
        reason_node = etree_.SubElement(subscription_end_node, wseTag('Reason'),
                                        attrib={xmlTag('lang'): 'en-US'})
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
            ref_param_node.set(wsaTag('IsReferenceParameter'), 'true')
            soap_envelope.add_header_element(ref_param_node)
        return CreatedMessage(soap_envelope, self)

    def mk_operation_response_message(self, message_data, action, response_name, mdib_version,
                                      sequence_id, transaction_id, invocation_state, invocation_error, error_text,
                                      nsmapper) -> CreatedMessage:
        msg_names = self._sdc_definitions.data_model.msg_names
        request = message_data.p_msg
        response = Soap12Envelope(nsmapper.partial_map(Prefixes.S12, Prefixes.MSG, Prefixes.WSA))
        reply_address = request.address.mk_reply_address(action=action)
        response.set_address(reply_address)
        reply_body_node = etree_.Element(msgTag(response_name),
                                         attrib={'SequenceId': sequence_id,
                                                 'MdibVersion': str(mdib_version)})
        invocation_info_node = etree_.SubElement(reply_body_node, msg_names.InvocationInfo)

        transaction_id_node = etree_.SubElement(invocation_info_node, msg_names.TransactionId)
        invocation_state_node = etree_.SubElement(invocation_info_node, msg_names.InvocationState)

        invocation_state_node.text = invocation_state
        transaction_id_node.text = str(transaction_id)

        if invocation_error is not None:
            invocation_error_node = etree_.SubElement(invocation_info_node, msg_names.InvocationError)
            invocation_error_node.text = invocation_error
        if error_text is not None:
            invocation_error_msg_node = etree_.SubElement(invocation_info_node,
                                                          msg_names.InvocationErrorMessage)
            invocation_error_msg_node.text = error_text
        response.payload_element = reply_body_node
        return CreatedMessage(response, self)

    def mk_realtime_samples_report_body(self, mdib_version, sequence_id, realtime_sample_states, nsmapper) -> etree_.Element:
        msg_names = self._sdc_definitions.data_model.msg_names
        ns_map = nsmapper.partial_map(Prefixes.PM, Prefixes.MSG, Prefixes.XSI, Prefixes.EXT, Prefixes.XML)
        body_node = etree_.Element(msg_names.WaveformStream,
                                   attrib={'SequenceId': sequence_id,
                                           'MdibVersion': str(mdib_version)},
                                   nsmap=ns_map)
        for state in realtime_sample_states:
            state_node = state.mk_state_node(msg_names.State, nsmapper, set_xsi_type=False)
            body_node.append(state_node)
        return body_node

    def mk_episodic_metric_report_body(self, mdib_version, sequence_id, states, nsmapper) -> etree_.Element:
        msg_names = self._sdc_definitions.data_model.msg_names
        return self._mk_report_body(msg_names.EpisodicMetricReport,
                                    msg_names.MetricState,
                                    states, nsmapper, mdib_version, sequence_id)

    def mk_periodic_metric_report_body(self, mdib_version, sequence_id, report_parts, nsmapper) -> etree_.Element:
        msg_names = self._sdc_definitions.data_model.msg_names
        return self._mk__periodic_report_body(msg_names.PeriodicMetricReport,
                                              msg_names.MetricState,
                                              report_parts, nsmapper, mdib_version, sequence_id)

    def mk_episodic_operational_state_report_body(self, mdib_version, sequence_id, states, nsmapper) -> etree_.Element:
        msg_names = self._sdc_definitions.data_model.msg_names
        return self._mk_report_body(msg_names.EpisodicOperationalStateReport,
                                    msg_names.OperationState,
                                    states, nsmapper, mdib_version, sequence_id)

    def mk_periodic_operational_state_report_body(self, mdib_version, sequence_id,
                                                  report_parts, ns_map) -> etree_.Element:
        msg_names = self._sdc_definitions.data_model.msg_names
        return self._mk__periodic_report_body(msg_names.PeriodicOperationalStateReport,
                                              msg_names.OperationState,
                                              report_parts, ns_map, mdib_version, sequence_id)

    def mk_episodic_alert_report_body(self, mdib_version, sequence_id, states, nsmapper) -> etree_.Element:
        msg_names = self._sdc_definitions.data_model.msg_names
        return self._mk_report_body(msg_names.EpisodicAlertReport,
                                    msg_names.AlertState,
                                    states, nsmapper, mdib_version, sequence_id)

    def mk_periodic_alert_report_body(self, mdib_version, sequence_id, report_parts, nsmapper) -> etree_.Element:
        msg_names = self._sdc_definitions.data_model.msg_names
        return self._mk__periodic_report_body(msg_names.PeriodicAlertReport,
                                              msg_names.AlertState,
                                              report_parts, nsmapper, mdib_version, sequence_id)

    def mk_episodic_component_state_report_body(self, mdib_version, sequence_id, states, nsmapper) -> etree_.Element:
        msg_names = self._sdc_definitions.data_model.msg_names
        return self._mk_report_body(msg_names.EpisodicComponentReport,
                                    msg_names.ComponentState,
                                    states, nsmapper, mdib_version, sequence_id)

    def mk_periodic_component_state_report_body(self, mdib_version, sequence_id,
                                                report_parts, nsmapper) -> etree_.Element:
        msg_names = self._sdc_definitions.data_model.msg_names
        return self._mk__periodic_report_body(msg_names.PeriodicComponentReport,
                                              msg_names.ComponentState,
                                              report_parts, nsmapper, mdib_version, sequence_id)

    def mk_episodic_context_report_body(self, mdib_version, sequence_id, states, nsmapper) -> etree_.Element:
        msg_names = self._sdc_definitions.data_model.msg_names
        return self._mk_report_body(msg_names.EpisodicContextReport,
                                    msg_names.ContextState,
                                    states, nsmapper, mdib_version, sequence_id)

    def mk_periodic_context_report_body(self, mdib_version, sequence_id, report_parts, nsmapper) -> etree_.Element:
        msg_names = self._sdc_definitions.data_model.msg_names
        return self._mk__periodic_report_body(msg_names.PeriodicContextReport,
                                              msg_names.ContextState,
                                              report_parts, nsmapper, mdib_version, sequence_id)

    def mk_description_modification_report_body(self, mdib_version, sequence_id, updated, created, deleted,
                                                updated_states, nsmapper) -> etree_.Element:
        msg_names = self._sdc_definitions.data_model.msg_names
        body_node = etree_.Element(msg_names.DescriptionModificationReport,
                                   attrib={'SequenceId': sequence_id,
                                           'MdibVersion': str(mdib_version)},
                                   nsmap=Prefixes.partial_map(Prefixes.MSG, Prefixes.PM))
        self._mk_descriptor_updates_report_part(body_node, 'Upt', updated, updated_states, nsmapper)
        self._mk_descriptor_updates_report_part(body_node, 'Crt', created, updated_states, nsmapper)
        self._mk_descriptor_updates_report_part(body_node, 'Del', deleted, updated_states, nsmapper)
        return body_node

    def _mk_descriptor_updates_report_part(self, parent_node, modification_type, descriptors, updated_states, nsmapper):
        """ Helper that creates ReportPart."""
        # This method creates one ReportPart for every descriptor.
        # An optimization is possible by grouping all descriptors with the same parent handle into one ReportPart.
        # This is not implemented, and I think it is not needed.
        msg_names = self._sdc_definitions.data_model.msg_names
        for descriptor in descriptors:
            report_part = etree_.SubElement(parent_node, msg_names.ReportPart,
                                            attrib={'ModificationType': modification_type})
            if descriptor.parent_handle is not None:  # only Mds can have None
                report_part.set('ParentDescriptor', descriptor.parent_handle)
            report_part.append(descriptor.mk_descriptor_node(tag=msg_names.Descriptor, nsmapper=nsmapper))
            related_state_containers = [s for s in updated_states if s.DescriptorHandle == descriptor.Handle]
            report_part.extend([state.mk_state_node(msg_names.State, nsmapper) for state in related_state_containers])

    def _mk_report_body(self, body_tag, state_tag, states, nsmapper, mdib_version, sequence_id) -> etree_.Element:
        msg_names = self._sdc_definitions.data_model.msg_names
        ns_map = nsmapper.partial_map(Prefixes.PM, Prefixes.MSG, Prefixes.XSI, Prefixes.EXT, Prefixes.XML)
        body_node = etree_.Element(body_tag,
                                   attrib={'SequenceId': sequence_id,
                                           'MdibVersion': str(mdib_version)},
                                   nsmap=ns_map)
        report_part_node = etree_.SubElement(body_node, msg_names.ReportPart)

        for state in states:
            report_part_node.append(state.mk_state_node(state_tag, nsmapper))
        return body_node

    def _mk__periodic_report_body(self, body_tag, state_tag, report_parts, nsmapper,
                                  mdib_version, sequence_id) -> etree_.Element:
        msg_names = self._sdc_definitions.data_model.msg_names
        ns_map = nsmapper.partial_map(Prefixes.PM, Prefixes.MSG, Prefixes.XSI, Prefixes.EXT, Prefixes.XML)
        body_node = etree_.Element(body_tag,
                                   attrib={'SequenceId': sequence_id,
                                           'MdibVersion': str(mdib_version)},
                                   nsmap=ns_map)
        for part in report_parts:
            report_part_node = etree_.SubElement(body_node, msg_names.ReportPart)
            for state in part.states:
                report_part_node.append(state.mk_state_node(state_tag, nsmapper))
        return body_node

    def mk_operation_invoked_report_body(self, mdib_version, sequence_id,
                                         operation_handle_ref, transaction_id, invocation_state,
                                         error, error_message, nsmapper) -> etree_.Element:
        msg_names = self._sdc_definitions.data_model.msg_names
        ns_map = nsmapper.partial_map(Prefixes.PM, Prefixes.MSG)
        body_node = etree_.Element(msg_names.OperationInvokedReport,
                                   attrib={'SequenceId': sequence_id,
                                           'MdibVersion': str(mdib_version)},
                                   nsmap=ns_map)
        report_part_node = etree_.SubElement(body_node,
                                             msg_names.ReportPart,
                                             attrib={'OperationHandleRef': operation_handle_ref})
        invocation_info_node = etree_.SubElement(report_part_node, msg_names.InvocationInfo)
        invocation_source_node = etree_.SubElement(report_part_node, msg_names.InvocationSource,
                                                   attrib={'Root': Prefixes.SDC.namespace,
                                                           'Extension': 'AnonymousSdcParticipant'})
        # implemented only SDC R0077 for value of invocationSourceNode:
        # Root =  "http://standards.ieee.org/downloads/11073/11073-20701-2018"
        # Extension = "AnonymousSdcParticipant".
        # a known participant (R0078) is currently not supported
        # ToDo: implement R0078
        transaction_id_node = etree_.SubElement(invocation_info_node, msg_names.TransactionId)
        transaction_id_node.text = str(transaction_id)
        operation_state_node = etree_.SubElement(invocation_info_node, msg_names.InvocationState)
        operation_state_node.text = str(invocation_state)
        if error is not None:
            error_node = etree_.SubElement(invocation_info_node, msg_names.InvocationError)
            error_node.text = str(error)
        if error_message is not None:
            error_message_node = etree_.SubElement(invocation_info_node, msg_names.InvocationErrorMessage)
            error_message_node.text = str(error_message)
        return body_node

    def mk_notification_message(self, ws_addr, message_node, reference_params: ReferenceParameters,
                                doc_nsmap) -> CreatedMessage:
        envelope = Soap12Envelope(doc_nsmap)
        envelope.payload_element = message_node
        envelope.set_address(ws_addr)
        for node in reference_params.parameters:
            envelope.add_header_element(node)
        return CreatedMessage(envelope, self)

    @staticmethod
    def _get_action_string(sdc_definitions, method_name):
        actions_lookup = sdc_definitions.Actions
        return getattr(actions_lookup, method_name)

    @staticmethod
    def _mk_this_model_sub_node(this_model, parent_node):
        this_model_node = etree_.SubElement(parent_node, dpwsTag('ThisModel'),
                                            nsmap=Prefixes.partial_map(Prefixes.DPWS))
        for lang, name in this_model.manufacturer.items():
            manufacturer_node = etree_.SubElement(this_model_node, dpwsTag('Manufacturer'))
            manufacturer_node.text = name
            if lang is not None:
                manufacturer_node.set(_LANGUAGE_ATTR, lang)

        manufacturer_url_node = etree_.SubElement(this_model_node, dpwsTag('ManufacturerUrl'))
        manufacturer_url_node.text = this_model.manufacturer_url

        for lang, name in this_model.model_name.items():
            model_name_node = etree_.SubElement(this_model_node, dpwsTag('ModelName'))
            model_name_node.text = name
            if lang is not None:
                model_name_node.set(_LANGUAGE_ATTR, lang)

        model_number_node = etree_.SubElement(this_model_node, dpwsTag('ModelNumber'))
        model_number_node.text = this_model.model_number
        model_url_node = etree_.SubElement(this_model_node, dpwsTag('ModelUrl'))
        model_url_node.text = this_model.model_url
        presentation_url_node = etree_.SubElement(this_model_node, dpwsTag('PresentationUrl'))
        presentation_url_node.text = this_model.presentation_url

    @staticmethod
    def _mk_this_device_sub_node(this_device, parent_node):
        this_device_node = etree_.SubElement(parent_node, dpwsTag('ThisDevice'),
                                             nsmap=Prefixes.partial_map(Prefixes.DPWS))
        for lang, name in this_device.friendly_name.items():
            friendly_name = etree_.SubElement(this_device_node, dpwsTag('FriendlyName'))
            friendly_name.text = name
            if lang not in (None, ''):
                friendly_name.set(_LANGUAGE_ATTR, lang)
        firmware_version = etree_.SubElement(this_device_node, dpwsTag('FirmwareVersion'))
        firmware_version.text = this_device.firmware_version
        serial_number = etree_.SubElement(this_device_node, dpwsTag('SerialNumber'))
        serial_number.text = this_device.serial_number

    @classmethod
    def _mk_host_service_type_sub_node(cls, host_service_type, parent_node):
        _ns = Prefixes.partial_map(Prefixes.DPWS, Prefixes.WSA)
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

        host_node = etree_.SubElement(parent_node, dpwsTag('Host'))
        ep_ref_node = etree_.SubElement(host_node, wsaTag('EndpointReference'))
        # for ep_ref in host_service_type.endpoint_references:
        #     cls._mk_endpoint_reference_sub_node(ep_ref, ep_ref_node)
        cls._mk_endpoint_reference_sub_node(host_service_type.endpoint_reference, ep_ref_node)
        if types_texts:
            types_node = etree_.SubElement(host_node, dpwsTag('Types'),
                                           nsmap=_ns)  # add also namespace prefixes that were locally generated
            types_node.text = ' '.join(types_texts)

    @classmethod
    def _mk_hosted_service_type_sub_node(cls, hosted_service_type, parent_node):
        hosted_node = etree_.SubElement(parent_node, dpwsTag('Hosted'))
        ep_ref_node = etree_.SubElement(hosted_node, wsaTag('EndpointReference'))
        for ep_ref in hosted_service_type.endpoint_references:
            cls._mk_endpoint_reference_sub_node(ep_ref, ep_ref_node)
        if hosted_service_type.types:
            types_text = ' '.join([docname_from_qname(t, parent_node.nsmap) for t in hosted_service_type.types])
            types_node = etree_.SubElement(hosted_node, dpwsTag('Types'))
            types_node.text = types_text
        service_node = etree_.SubElement(hosted_node, dpwsTag('ServiceId'))
        service_node.text = hosted_service_type.service_id
