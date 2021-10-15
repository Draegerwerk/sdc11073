import copy
import uuid
import weakref
from abc import abstractmethod, ABC

from lxml import etree as etree_

from . import soapenvelope
from .. import isoduration
from ..namespaces import EventingActions
from ..namespaces import Prefixes
from ..namespaces import WSA_ANONYMOUS
from ..namespaces import domTag, msgTag, wseTag, wsaTag, wsxTag, xmlTag, dpwsTag, QN_TYPE
from ..namespaces import nsmap, DocNamespaceHelper


def _handles2params(handles):
    """
    Internal helper, converts handles to dom elements
    :param handles: a list of strings
    :return: a list of etree nodes
    """
    params = []
    if handles is not None:
        for handle in handles:
            node = etree_.Element(msgTag('HandleRef'))
            node.text = handle
            params.append(node)
    return params


class AbstractMessageFactory(ABC):
    def __init__(self, sdc_definitions, logger):
        self._logger = logger
        self._sdc_definitions = sdc_definitions
        self._mdib_wref = None

    @abstractmethod
    def register_mdib(self, mdib):
        """Factory sometimes must know the mdib data (e.g. Set service, activate method).
        :param mdib: the current mdib
        """


class SoapMessageFactory(AbstractMessageFactory):

    def register_mdib(self, mdib):
        """Factory sometimes must know the mdib data (e.g. Set service, activate method).
        :param mdib: the current mdib
        """
        if mdib is not None and self._mdib_wref is not None:
            raise RuntimeError('SoapMessageFactory has already an registered mdib')
        self._mdib_wref = None if mdib is None else weakref.ref(mdib)


class MessageFactoryClient(SoapMessageFactory):
    @staticmethod
    def _add_device_references(soap_envelope, dev_reference_param):
        ''' add references for requests to device (renew, getstatus, unsubscribe)'''
        if dev_reference_param is not None:
            for element in dev_reference_param:
                element_ = copy.copy(element)
                # mandatory attribute acc. to ws_addressing SOAP Binding (https://www.w3.org/TR/2006/REC-ws-addr-soap-20060509/)
                element_.set('IsReferenceParameter', 'true')
                soap_envelope.add_header_element(element_)

    def _mk_get_method_envelope(self, addr_to, port_type, method_name, params=None):
        body_node = etree_.Element(msgTag(method_name))
        soap_envelope = soapenvelope.Soap12Envelope(Prefixes.partial_map(Prefixes.S12, Prefixes.WSA, Prefixes.MSG))
        action_string = self.get_action_string(port_type, method_name)
        soap_envelope.set_address(soapenvelope.WsAddress(action=action_string, addr_to=addr_to))
        if params:
            for param in params:
                body_node.append(param)
        soap_envelope.add_body_element(body_node)
        return soap_envelope

    def _mk_setmethod_envelope(self, addr_to, port_type, method_name, operation_handle, request_nodes,
                               additional_namespaces=None):
        """ helper to create the soap envelope
        :param addr_to: to-field value in address
        :param port_type: needed to construct the action string
        :param method_name: last element of name of the called action
        :param operation_handle: handle name as string
        :param request_nodes: a list of etree_ nodes that will become sub-element of Method name element
        """
        body_node = etree_.Element(msgTag(method_name))
        ref = etree_.SubElement(body_node, msgTag('OperationHandleRef'),
                                attrib={QN_TYPE: '{}:HandleRef'.format(Prefixes.PM.prefix)},
                                nsmap=Prefixes.partial_map(Prefixes.PM))
        ref.text = operation_handle
        for node in request_nodes:
            body_node.append(node)
        if additional_namespaces:
            my_ns = Prefixes.partial_map(Prefixes.S12, Prefixes.WSA, Prefixes.PM, Prefixes.MSG, *additional_namespaces)
        else:
            my_ns = Prefixes.partial_map(Prefixes.S12, Prefixes.WSA, Prefixes.PM, Prefixes.MSG)

        soap_envelope = soapenvelope.Soap12Envelope(my_ns)
        action_string = self.get_action_string(port_type, method_name)
        soap_envelope.set_address(soapenvelope.WsAddress(action=action_string, addr_to=addr_to))
        soap_envelope.add_body_element(body_node)
        return soap_envelope

    @staticmethod
    def mk_getmetadata_envelope(addr_to):
        soap_envelope = soapenvelope.Soap12Envelope(nsmap)
        soap_envelope.set_address(
            soapenvelope.WsAddress(action='http://schemas.xmlsoap.org/ws/2004/09/mex/GetMetadata/Request',
                                   addr_to=addr_to))
        soap_envelope.add_body_element(etree_.Element('{http://schemas.xmlsoap.org/ws/2004/09/mex}GetMetadata'))
        return soap_envelope

    def mk_getdescriptor_envelope(self, addr_to, port_type, requested_handles):
        """
        :param addr_to: to-field value in address
        :param port_type: needed to construct the action string
        :param requested_handles: a list of strings
        :return: a SoapEnvelope
        """
        method = 'GetMdState'
        return self._mk_get_method_envelope(addr_to, port_type, method, params=_handles2params(requested_handles))

    def mk_getmdib_envelope(self, addr_to, port_type):
        """
        :param addr_to: to-field value in address
        :param port_type: needed to construct the action string
        :return: a SoapEnvelope
        """
        method = 'GetMdib'
        return self._mk_get_method_envelope(addr_to, port_type, method)

    def mk_getmddescription_envelope(self, addr_to, port_type, requested_handles=None):
        """
        :param addr_to: to-field value in address
        :param port_type: needed to construct the action string
        :param requested_handles: a list of strings
        :return: a SoapEnvelope
        """
        method = 'GetMdDescription'
        return self._mk_get_method_envelope(addr_to, port_type, method, params=_handles2params(requested_handles))

    def mk_getmdstate_envelope(self, addr_to, port_type, requested_handles=None):
        """
        :param addr_to: to-field value in address
        :param port_type: needed to construct the action string
        :param requested_handles: a list of strings
        :return: a SoapEnvelope
        """
        method = 'GetMdState'
        return self._mk_get_method_envelope(addr_to, port_type, method, params=_handles2params(requested_handles))

    def mk_getcontainmenttree_envelope(self, addr_to, port_type, requested_handles):
        """
        :param addr_to: to-field value in address
        :param port_type: needed to construct the action string
        :param requested_handles: a list of strings
        :return: a SoapEnvelope
        """
        method = 'GetContainmentTree'
        return self._mk_get_method_envelope(addr_to, port_type, method, params=_handles2params(requested_handles))

    def mk_getcontextstates_envelope(self, addr_to, port_type, requested_handles=None):
        """
        :param addr_to: to-field value in address
        :param port_type: needed to construct the action string
        :param requested_handles: a list of strings
        :return: a SoapEnvelope
        """
        requestparams = []
        if requested_handles:
            for handle in requested_handles:
                requestparams.append(etree_.Element(msgTag('HandleRef'),
                                                    attrib={QN_TYPE: '{}:HandleRef'.format(Prefixes.MSG.prefix)},
                                                    nsmap=Prefixes.partial_map(Prefixes.MSG, Prefixes.PM)))
                requestparams[-1].text = handle
        method = 'GetContextStates'
        return self._mk_get_method_envelope(addr_to, port_type, method, params=requestparams)

    def mk_getcontextstates_by_identification_envelope(self, addr_to, port_type, identifications):
        """
        :param addr_to: to-field value in address
        :param port_type: needed to construct the action string
        :param identifications: list of identifiers (type: InstanceIdentifier from pmtypes)
        :return: a SoapEnvelope
        """
        requestparams = []
        if identifications:
            for identification in identifications:
                requestparams.append(identification.as_etree_node(
                    qname=msgTag('Identification'), nsmap=Prefixes.partial_map(Prefixes.MSG, Prefixes.PM)))
        method = 'GetContextStatesByIdentification'
        return self._mk_get_method_envelope(addr_to, port_type, method, params=requestparams)

    def mk_requestednumericvalue_envelope(self, addr_to, port_type, operation_handle, requested_numeric_value):
        """
        :param addr_to: to-field value in address
        :param port_type: needed to construct the action string
        :param operation_handle: the handle of operation that is called
        :param requested_numeric_value: a string
        :return: a SoapEnvelope
        """
        requested_value_node = etree_.Element(msgTag('RequestedNumericValue'),
                                              attrib={QN_TYPE: '{}:decimal'.format(Prefixes.XSD.prefix)})
        requested_value_node.text = str(requested_numeric_value)
        method = 'SetValue'
        return self._mk_setmethod_envelope(addr_to, port_type, method,
                                           operation_handle,
                                           [requested_value_node],
                                           additional_namespaces=[Prefixes.XSD])

    def mk_requestedstring_envelope(self, addr_to, port_type, operation_handle, requested_string):
        """
        :param addr_to: to-field value in address
        :param port_type: needed to construct the action string
        :param operation_handle: the handle of operation that is called
        :param requested_string: a string
        :return: a SoapEnvelope
        """
        requested_string_node = etree_.Element(msgTag('RequestedStringValue'),
                                               attrib={QN_TYPE: '{}:string'.format(Prefixes.XSD.prefix)})
        requested_string_node.text = requested_string
        method = 'SetString'
        return self._mk_setmethod_envelope(addr_to, port_type, method, operation_handle, [requested_string_node],
                                           additional_namespaces=[Prefixes.XSD])

    def mk_setalert_envelope(self, nsmapper, addr_to, port_type, operation_handle, proposed_alert_states):
        """
        :param addr_to: to-field value in address
        :param port_type: needed to construct the action string
        :param operation_handle: the handle of operation that is called
        :param proposed_alert_states: a list of AbstractAlertStateContainer or derived class
        :return: a SoapEnvelope
        """
        _proposed_states = [p.mk_copy() for p in proposed_alert_states]
        for state in _proposed_states:
            state.nsmapper = DocNamespaceHelper()  # use my namespaces
        _proposed_state_nodes = [p.mk_state_node(msgTag('ProposedAlertState'), nsmapper) for p in _proposed_states]
        method = 'SetAlertState'
        return self._mk_setmethod_envelope(addr_to, port_type, method, operation_handle, _proposed_state_nodes)

    def mk_setmetricstate_envelope(self, nsmapper, addr_to, port_type, operation_handle, proposed_metric_states):
        """
        :param addr_to: to-field value in address
        :param port_type: needed to construct the action string
        :param operation_handle: the handle of operation that is called
        :param proposed_metric_states: a list of AbstractMetricStateContainer or derived class
        :return: a SoapEnvelope
        """
        _proposed_states = [p.mk_copy() for p in proposed_metric_states]
        nsmapper = DocNamespaceHelper()
        for state in _proposed_states:
            state.nsmapper = nsmapper  # use my namespaces
        _proposed_state_nodes = [p.mk_state_node(msgTag('ProposedMetricState'), nsmapper) for p in _proposed_states]
        return self._mk_setmethod_envelope(addr_to, port_type, 'SetMetricState', operation_handle,
                                           _proposed_state_nodes)

    def mk_setcomponentstate_envelope(self, nsmapper, addr_to, port_type, operation_handle, proposed_component_states):
        """
        :param addr_to: to-field value in address
        :param port_type: needed to construct the action string
        :param operation_handle: the handle of operation that is called
        :param proposed_component_states: a list of AbstractComponentStateContainers or derived class
        :return: a SoapEnvelope
        """
        _proposed_states = [p.mk_copy() for p in proposed_component_states]
        # nsmapper = DocNamespaceHelper()
        for state in _proposed_states:
            state.nsmapper = nsmapper  # use my namespaces
        _proposed_state_nodes = [p.mk_state_node(msgTag('ProposedComponentState'), nsmapper) for p in _proposed_states]
        return self._mk_setmethod_envelope(addr_to, port_type, 'SetComponentState', operation_handle,
                                           _proposed_state_nodes)

    def mk_setcontextstate_envelope(self, nsmapper, addr_to, port_type, operation_handle, proposed_context_states):
        """
        :param addr_to: to-field value in address
        :param port_type: needed to construct the action string
        :param operation_handle: the handle of operation that is called
        :param proposed_context_states: a list of AbstractContextStateContainers or derived class
        :return: a SoapEnvelope
        """
        _proposed_states = [p.mk_copy() for p in proposed_context_states]
        for state in _proposed_states:
            # BICEPS: if handle == descriptorHandle, it means insert.
            if state.Handle is None:
                state.Handle = state.DescriptorHandle
            state.nsmapper = DocNamespaceHelper()  # use my namespaces
        _proposed_state_nodes = [p.mk_state_node(msgTag('ProposedContextState'),
                                                 nsmapper,
                                                 update_descriptor_version=False) for p in _proposed_states]

        return self._mk_setmethod_envelope(addr_to, port_type, 'SetContextState', operation_handle,
                                           _proposed_state_nodes)

    def mk_activate_envelope(self, addr_to, port_type, operation_handle, arguments=None):
        """
        :param addr_to: to-field value in address
        :param port_type: needed to construct the action string
        :param operation_handle: the handle of operation that is called
        :param arguments: a list of strings or None
        :return: a SoapEnvelope
        """
        body_node = etree_.Element(msgTag('Activate'), attrib=None, nsmap=nsmap)
        ref = etree_.SubElement(body_node, msgTag('OperationHandleRef'))
        ref.text = operation_handle
        if arguments is not None:
            for argument in arguments:
                argument_node = etree_.SubElement(body_node, msgTag('Argument'))
                arg_val = etree_.SubElement(argument_node, msgTag('ArgValue'))
                arg_val.text = argument
        soap_envelope = self._mk_soapenvelope(addr_to, port_type, 'Activate', body_node)
        return soap_envelope

    def mk_getlocalizedtext_envelope(self, addr_to, port_type, refs=None, version=None, langs=None,
                                     text_widths=None, number_of_lines=None):
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
        requestparams = []
        if refs is not None:
            for ref in refs:
                node = etree_.Element(msgTag('Ref'))
                node.text = ref
                requestparams.append(node)
        if version is not None:
            node = etree_.Element(msgTag('Version'))
            node.text = str(version)
            requestparams.append(node)
        if langs is not None:
            for lang in langs:
                node = etree_.Element(msgTag('Lang'))
                node.text = lang
                requestparams.append(node)
        if text_widths is not None:
            for text_width in text_widths:
                node = etree_.Element(msgTag('TextWidth'))
                node.text = text_width
                requestparams.append(node)
        if number_of_lines is not None:
            for nol in number_of_lines:
                node = etree_.Element(msgTag('NumberOfLines'))
                node.text = nol
                requestparams.append(node)
        method = 'GetLocalizedText'
        return self._mk_get_method_envelope(addr_to, port_type, method, params=requestparams)

    def mk_get_supported_languages_envelope(self, addr_to, port_type):
        """
        :param addr_to: to-field value in address
        :param port_type: needed to construct the action string
        :return: a SoapEnvelope
        """
        method = 'GetSupportedLanguages'
        return self._mk_get_method_envelope(addr_to, port_type, method)

    @staticmethod
    def mk_subscribe_envelope(addr_to,
                              notifyto_url, notify_to_identifier,
                              endto_url, endto_identifier,
                              expire_minutes, subscribe_filter):
        soap_envelope = soapenvelope.Soap12Envelope(Prefixes.partial_map(Prefixes.S12, Prefixes.WSA, Prefixes.WSE))
        soap_envelope.set_address(soapenvelope.WsAddress(action=EventingActions.Subscribe, addr_to=addr_to))
        if notify_to_identifier is None:
            notify_to = soapenvelope.WsaEndpointReferenceType(notifyto_url, reference_parameters_node=None)
        else:
            notify_to = soapenvelope.WsaEndpointReferenceType(notifyto_url,
                                                              reference_parameters_node=[notify_to_identifier])

        if endto_identifier is None:
            end_to = soapenvelope.WsaEndpointReferenceType(endto_url, reference_parameters_node=None)
        else:
            end_to = soapenvelope.WsaEndpointReferenceType(endto_url,
                                                           reference_parameters_node=[endto_identifier])

        body = soapenvelope.WsSubscribe(notify_to=notify_to,
                                        end_to=end_to,
                                        expires=expire_minutes * 60,
                                        filter_=subscribe_filter)
        soap_envelope.add_body_object(body)
        return soap_envelope

    def mk_renew_envelope(self, addr_to, dev_reference_param, expire_minutes):
        soap_envelope = soapenvelope.Soap12Envelope(Prefixes.partial_map(Prefixes.S12, Prefixes.WSA, Prefixes.WSE))
        soap_envelope.set_address(soapenvelope.WsAddress(action=EventingActions.Renew, addr_to=addr_to))
        self._add_device_references(soap_envelope, dev_reference_param)
        renew_node = etree_.Element(wseTag('Renew'), nsmap=Prefixes.partial_map(Prefixes.WSE))
        expires_node = etree_.SubElement(renew_node, wseTag('Expires'), nsmap=Prefixes.partial_map(Prefixes.WSE))
        expires_node.text = isoduration.duration_string(expire_minutes * 60)
        soap_envelope.add_body_element(renew_node)
        return soap_envelope

    def mk_getstatus_envelope(self, addr_to, dev_reference_param):
        soap_envelope = soapenvelope.Soap12Envelope(Prefixes.partial_map(Prefixes.S12, Prefixes.WSA, Prefixes.WSE))
        soap_envelope.set_address(
            soapenvelope.WsAddress(action=EventingActions.GetStatus, addr_to=addr_to))
        self._add_device_references(soap_envelope, dev_reference_param)
        body_node = etree_.Element(wseTag('GetStatus'))
        soap_envelope.add_body_element(body_node)
        return soap_envelope

    def mk_unsubscribe_envelope(self, addr_to, dev_reference_param):
        soap_envelope = soapenvelope.Soap12Envelope(Prefixes.partial_map(Prefixes.S12, Prefixes.WSA, Prefixes.WSE))
        soap_envelope.set_address(soapenvelope.WsAddress(action=EventingActions.Unsubscribe, addr_to=addr_to))
        self._add_device_references(soap_envelope, dev_reference_param)
        soap_envelope.add_body_element(etree_.Element(wseTag('Unsubscribe')))
        return soap_envelope

    def get_action_string(self, port_type, method_name):
        actions_lookup = self._sdc_definitions.Actions
        try:
            return getattr(actions_lookup, method_name)
        except AttributeError:  # fallback, if a definition is missing
            return '{}/{}/{}'.format(self._sdc_definitions.ActionsNamespace, port_type, method_name)

    def _mk_soapenvelope(self, addr_to, port_type, method_name, body_node=None, additional_headers=None):
        action = self.get_action_string(port_type, method_name)
        envelope = soapenvelope.Soap12Envelope(Prefixes.partial_map(Prefixes.S12, Prefixes.MSG, Prefixes.WSA))
        envelope.set_address(soapenvelope.WsAddress(action=action, addr_to=addr_to))
        if additional_headers is not None:
            for header in additional_headers:
                envelope.add_header_object(header)
        if body_node is not None:
            envelope.add_body_element(body_node)
        return envelope


class MessageFactoryDevice(SoapMessageFactory):
    def mk_get_metadata_response_envelope(self, message_data, this_device, this_model, dpws_host, dpws_hosted_services,
                                          dpws_schema, nsmapper):
        def validate_dpws(node):
            if dpws_schema is None:
                return
            try:
                dpws_schema.assertValid(node)
            except etree_.DocumentInvalid as ex:
                tmp_str = etree_.tostring(node, pretty_print=True).decode('utf-8')
                self._logger.error('invalid dpws: {}\ndata = {}', ex, tmp_str)
                raise

        response = soapenvelope.Soap12Envelope(nsmapper.doc_ns_map)
        reply_address = message_data.p_msg.address.mk_reply_address('{}/GetResponse'.format(Prefixes.WXF.namespace))
        reply_address.addr_to = WSA_ANONYMOUS
        reply_address.message_id = uuid.uuid4().urn
        response.add_header_object(reply_address)
        metadata_node = etree_.Element(wsxTag('Metadata'), nsmap=nsmapper.partial_map(Prefixes.SDC, Prefixes.DPWS))

        # ThisModel
        metadata_section_node = etree_.SubElement(metadata_node,
                                                  wsxTag('MetadataSection'),
                                                  attrib={'Dialect': '{}/ThisModel'.format(nsmap['dpws'])})
        this_model.as_etree_subnode(metadata_section_node)
        validate_dpws(metadata_section_node[-1])

        # ThisDevice
        metadata_section_node = etree_.SubElement(metadata_node,
                                                  wsxTag('MetadataSection'),
                                                  attrib={'Dialect': '{}/ThisDevice'.format(nsmap['dpws'])})
        this_device.as_etree_subnode(metadata_section_node)

        validate_dpws(metadata_section_node[-1])

        # Relationship
        metadata_section_node = etree_.SubElement(metadata_node,
                                                  wsxTag('MetadataSection'),
                                                  attrib={'Dialect': '{}/Relationship'.format(nsmap['dpws'])})
        relationship_node = etree_.SubElement(metadata_section_node,
                                              dpwsTag('Relationship'),
                                              attrib={'Type': '{}/host'.format(nsmap['dpws'])})

        dpws_host.as_etree_subnode(relationship_node)
        validate_dpws(relationship_node[-1])

        # add all hosted services:
        for service in dpws_hosted_services:
            service.mk_dpws_hosted_instance().as_etree_subnode(relationship_node)
            validate_dpws(relationship_node[-1])
        response.add_body_element(metadata_node)
        return response

    def mk_get_mdib_response_envelope(self, message_data, mdib, include_contextstates):
        request = message_data.p_msg
        nsmapper = mdib.nsmapper
        response_envelope = soapenvelope.Soap12Envelope(
            nsmapper.partial_map(Prefixes.S12, Prefixes.WSA, Prefixes.PM, Prefixes.MSG))
        reply_address = request.address.mk_reply_address(
            action=self._get_action_string(mdib.sdc_definitions, 'GetMdibResponse'))
        response_envelope.add_header_object(reply_address)
        if include_contextstates:
            mdib_node = mdib.reconstruct_mdib_with_context_states()
        else:
            mdib_node = mdib.reconstruct_mdib()
        mdib_version_string = mdib_node.get('MdibVersion')  # use same version a in mdib node for response
        sequence_id_string = mdib_node.get('SequenceId')

        get_mdib_response_node = etree_.Element(msgTag('GetMdibResponse'),
                                                nsmap=Prefixes.partial_map(Prefixes.MSG, Prefixes.PM))
        if mdib_version_string:
            get_mdib_response_node.set('MdibVersion', mdib_version_string)
        get_mdib_response_node.set('SequenceId', sequence_id_string)
        get_mdib_response_node.append(mdib_node)
        response_envelope.add_body_element(get_mdib_response_node)
        return response_envelope

    def mk_getmddescription_response_envelope(self, message_data, mdib, requested_handles):
        """For simplification reason this implementation returns either all descriptors or none."""
        request = message_data.p_msg
        return_all = len(requested_handles) == 0  # if we have handles, we need to check them
        my_namespaces = mdib.nsmapper.partial_map(Prefixes.S12, Prefixes.WSA, Prefixes.MSG, Prefixes.PM)
        response_envelope = soapenvelope.Soap12Envelope(my_namespaces)
        reply_address = request.address.mk_reply_address(
            action=self._get_action_string(mdib.sdc_definitions, 'GetMdDescriptionResponse'))
        response_envelope.add_header_object(reply_address)

        response_node = etree_.Element(msgTag('GetMdDescriptionResponse'), nsmap=nsmap)

        for handle in requested_handles:
            # if at least one requested handle is valid, return all.
            if mdib.descriptions.handle.get_one(handle, allow_none=True) is not None:
                return_all = True
                break
        if return_all:
            md_description_node, mdib_version = mdib.reconstruct_md_description()
            md_description_node.tag = msgTag('MdDescription')  # rename according to message
            mdib_version_string = str(mdib_version)
        else:
            md_description_node = etree_.Element(msgTag('MdDescription'))
            mdib_version_string = None
        sequence_id_string = mdib.sequence_id
        if mdib_version_string:
            response_node.set('MdibVersion', mdib_version_string)
        response_node.set('SequenceId', sequence_id_string)
        response_node.append(md_description_node)
        response_envelope.add_body_element(response_node)
        return response_envelope

    def mk_get_mdstate_response_envelope(self, message_data, action, mdib_version, sequence_id, state_containers,
                                         nsmapper):
        request = message_data.p_msg
        response_envelope = soapenvelope.Soap12Envelope(
            nsmapper.partial_map(Prefixes.S12, Prefixes.WSA, Prefixes.PM, Prefixes.MSG))
        reply_address = request.address.mk_reply_address(
            action=action)  # self._get_action_string(mdib.sdc_definitions, 'GetMdStateResponse'))
        response_envelope.add_header_object(reply_address)
        response_node = etree_.Element(msgTag('GetMdStateResponse'), nsmap=nsmap)
        response_node.set('MdibVersion', str(mdib_version))
        response_node.set('SequenceId', sequence_id)
        md_state_node = etree_.Element(msgTag('MdState'), attrib=None, nsmap=nsmapper.doc_ns_map)
        for state_container in state_containers:
            md_state_node.append(state_container.mk_state_node(domTag('State'), nsmapper))

        response_node.append(md_state_node)
        response_envelope.add_body_element(response_node)
        return response_envelope

    def mk_get_context_states_response_envelope(self, message_data, action, mdib_version, sequence_id, state_containers,
                                                nsmapper):
        response = soapenvelope.Soap12Envelope(
            nsmapper.partial_map(Prefixes.S12, Prefixes.WSA, Prefixes.PM, Prefixes.MSG))
        reply_address = message_data.p_msg.address.mk_reply_address(
            action=action)
        response.add_header_object(reply_address)
        response_node = etree_.Element(msgTag('GetContextStatesResponse'))
        response_node.set('MdibVersion', str(mdib_version))
        response_node.set('SequenceId', sequence_id)
        tag = msgTag('ContextState')
        for container in state_containers:
            node = container.mk_state_node(tag, nsmapper)
            response_node.append(node)
            node.tag = msgTag('ContextState')
        response.add_body_element(response_node)
        return response

    def mk_get_localized_texts_response_envelope(self, message_data, action, mdib_version, sequence_id, texts,
                                                 nsmapper):
        response_envelope = soapenvelope.Soap12Envelope(
            nsmapper.partial_map(Prefixes.S12, Prefixes.WSA, Prefixes.PM, Prefixes.MSG))
        reply_address = message_data.p_msg.address.mk_reply_address(action=action)
        response_envelope.add_header_object(reply_address)
        response_node = etree_.Element(msgTag('GetLocalizedTextResponse'))
        response_node.set('MdibVersion', str(mdib_version))
        response_node.set('SequenceId', sequence_id)

        for text in texts:
            response_node.append(text.as_etree_node(msgTag('Text'), nsmap=None))
        response_envelope.add_body_element((response_node))
        return response_envelope

    def mk_get_supported_languages_response_envelope(self, message_data, action, mdib_version, sequence_id, languages,
                                                     nsmapper):
        response_envelope = soapenvelope.Soap12Envelope(
            nsmapper.partial_map(Prefixes.S12, Prefixes.WSA, Prefixes.PM, Prefixes.MSG))
        reply_address = message_data.p_msg.address.mk_reply_address(action=action)
        response_envelope.add_header_object(reply_address)
        response_node = etree_.Element(msgTag('GetSupportedLanguagesResponse'))
        response_node.set('MdibVersion', str(mdib_version))
        response_node.set('SequenceId', sequence_id)

        for lang in languages:
            node = etree_.SubElement(response_node, msgTag('Lang'))
            node.text = lang
        response_envelope.add_body_element(response_node)
        return response_envelope

    def mk_subscribe_response_envelope(self, request_data, subscription, base_urls):
        response = soapenvelope.Soap12Envelope(
            Prefixes.partial_map(Prefixes.PM, Prefixes.S12, Prefixes.WSA, Prefixes.WSE))
        reply_address = request_data.message_data.p_msg.address.mk_reply_address(EventingActions.SubscribeResponse)
        response.add_header_object(reply_address)
        subscribe_response_node = etree_.Element(wseTag('SubscribeResponse'))
        subscription_manager_node = etree_.SubElement(subscribe_response_node, wseTag('SubscriptionManager'))
        # child of subscription manager is the endpoint reference of the subscription manager (wsa:EndpointReferenceType)
        if subscription.reference_parameter is not None:
            reference_parameters_node = etree_.Element(wsaTag('ReferenceParameters'))
            reference_parameters_node.append(copy.copy(subscription.reference_parameter))
        else:
            reference_parameters_node = None
        path = '/'.join(request_data.consumed_path_elements)
        path_suffix = '' if subscription.path_suffix is None else f'/{subscription.path_suffix}'
        subscription_address = f'{base_urls[0].scheme}://{base_urls[0].netloc}/{path}{path_suffix}'
        epr = soapenvelope.WsaEndpointReferenceType(address=subscription_address,
                                                    reference_parameters_node=reference_parameters_node)
        epr.as_etree_subnode(subscription_manager_node)
        expires_node = etree_.SubElement(subscribe_response_node, wseTag('Expires'))
        expires_node.text = subscription.expire_string  # simply confirm request
        response.add_body_element(subscribe_response_node)
        self._logger.debug('on_subscribe_request returns {}', lambda: response.as_xml(pretty=False))
        return response

    def mk_unsubscribe_response_envelope(self, request_data):
        response = soapenvelope.Soap12Envelope(
            Prefixes.partial_map(Prefixes.PM, Prefixes.S12, Prefixes.WSA, Prefixes.WSE))
        reply_address = request_data.message_data.p_msg.address.mk_reply_address(EventingActions.UnsubscribeResponse)
        response.add_header_object(reply_address)
        # response has empty body
        return response

    def mk_renew_response_envelope(self, request_data, remaining_seconds):
        response = soapenvelope.Soap12Envelope(Prefixes.partial_map(Prefixes.S12, Prefixes.WSA, Prefixes.WSE))
        reply_address = request_data.message_data.p_msg.address.mk_reply_address(EventingActions.RenewResponse)
        response.add_header_object(reply_address)
        renew_response_node = etree_.Element(wseTag('RenewResponse'))
        expires_node = etree_.SubElement(renew_response_node, wseTag('Expires'))
        expires_node.text = isoduration.duration_string(remaining_seconds)
        response.add_body_element(renew_response_node)
        return response

    def mk_getstatus_response_envelope(self, request_data, remaining_seconds):
        response = soapenvelope.Soap12Envelope(Prefixes.partial_map(Prefixes.S12, Prefixes.WSA, Prefixes.WSE))
        reply_address = request_data.message_data.p_msg.address.mk_reply_address(EventingActions.GetStatusResponse)
        response.add_header_object(reply_address)
        renew_response_node = etree_.Element(wseTag('GetStatusResponse'))
        expires_node = etree_.SubElement(renew_response_node, wseTag('Expires'))
        expires_node.text = isoduration.duration_string(remaining_seconds)
        response.add_body_element(renew_response_node)
        return response

    def mk_notification_end_report(self, subscription, my_addr, code, reason):
        soap_envelope = soapenvelope.Soap12Envelope(Prefixes.partial_map(Prefixes.S12, Prefixes.WSA, Prefixes.WSE))

        subscription_end_node = etree_.Element(wseTag('SubscriptionEnd'),
                                               nsmap=Prefixes.partial_map(Prefixes.WSE, Prefixes.WSA, Prefixes.XML))
        subscription_manager_node = etree_.SubElement(subscription_end_node, wseTag('SubscriptionManager'))
        # child of Subscriptionmanager is the endpoint reference of the subscription manager (wsa:EndpointReferenceType)
        if subscription.reference_parameter:
            reference_parameters_node = etree_.Element(wsaTag('ReferenceParameters'))
            reference_parameters_node.append(copy.copy(subscription.reference_parameter))
        else:
            reference_parameters_node = None
        epr = soapenvelope.WsaEndpointReferenceType(address=my_addr,
                                                    reference_parameters_node=reference_parameters_node)
        epr.as_etree_subnode(subscription_manager_node)

        # remark: optionally one could add own address and identifier here ...
        status_node = etree_.SubElement(subscription_end_node, wseTag('Status'))
        status_node.text = 'wse:{}'.format(code)
        reason_node = etree_.SubElement(subscription_end_node, wseTag('Reason'),
                                        attrib={xmlTag('lang'): 'en-US'})
        reason_node.text = reason
        soap_envelope.add_body_element(subscription_end_node)

        to_addr = subscription.end_to_address or subscription.notify_to_address
        addr = soapenvelope.WsAddress(addr_to=to_addr,
                                      action=EventingActions.SubscriptionEnd,
                                      addr_from=None,
                                      reply_to=None,
                                      fault_to=None,
                                      reference_parameters_node=None)
        soap_envelope.set_address(addr)
        ref_nodes = subscription.end_to_ref_nodes or subscription.notify_ref_nodes
        for ident_node in ref_nodes:
            ident_node_ = copy.copy(ident_node)
            # mandatory attribute acc. to ws_addressing SOAP Binding (https://www.w3.org/TR/2006/REC-ws-addr-soap-20060509/)
            ident_node_.set(wsaTag('IsReferenceParameter'), 'true')
            soap_envelope.add_header_element(ident_node_)

        return soap_envelope

    def mk_operation_response_envelope(self, message_data, action, response_name, mdib_version,
                                       sequence_id, transaction_id, invocation_state, invocation_error, error_text,
                                       nsmapper):
        request = message_data.p_msg
        response = soapenvelope.Soap12Envelope(nsmapper.partial_map(Prefixes.S12, Prefixes.MSG, Prefixes.WSA))
        reply_address = request.address.mk_reply_address(action=action)
        response.add_header_object(reply_address)
        reply_body_node = etree_.Element(msgTag(response_name),
                                         attrib={'SequenceId': sequence_id,
                                                 'MdibVersion': str(mdib_version)})
        invocation_info_node = etree_.SubElement(reply_body_node, msgTag('InvocationInfo'))

        transaction_id_node = etree_.SubElement(invocation_info_node, msgTag('TransactionId'))
        invocation_state_node = etree_.SubElement(invocation_info_node, msgTag('InvocationState'))

        invocation_state_node.text = invocation_state
        transaction_id_node.text = str(transaction_id)

        if invocation_error is not None:
            invocation_error_node = etree_.SubElement(invocation_info_node, msgTag('InvocationError'))
            invocation_error_node.text = invocation_error
        if error_text is not None:
            invocation_error_msg_node = etree_.SubElement(invocation_info_node,
                                                          msgTag('InvocationErrorMessage'))
            invocation_error_msg_node.text = error_text

        response.add_body_element(reply_body_node)
        return response

    @staticmethod
    def mk_realtime_samples_report_body(mdib_version, sequence_id, realtime_sample_states, nsmapper):
        ns_map = nsmapper.partial_map(Prefixes.PM, Prefixes.MSG, Prefixes.XSI, Prefixes.EXT, Prefixes.XML)

        body_node = etree_.Element(msgTag('WaveformStream'),
                                   attrib={'SequenceId': sequence_id,
                                           'MdibVersion': str(mdib_version)},
                                   nsmap=ns_map)

        for state in realtime_sample_states:
            state_node = state.mk_state_node(msgTag('State'), nsmapper, set_xsi_type=False)
            body_node.append(state_node)
        return body_node

    def mk_episodic_metric_report_body(self, mdib_version, sequence_id, states, nsmapper):
        return self._mk_report_body(msgTag('EpisodicMetricReport'),
                                    msgTag('MetricState'),
                                    states, nsmapper, mdib_version, sequence_id)

    def mk_periodic_metric_report_body(self, mdib_version, sequence_id, report_parts, nsmapper):
        return self._mk__periodic_report_body(msgTag('PeriodicMetricReport'),
                                              msgTag('MetricState'),
                                              report_parts, nsmapper, mdib_version, sequence_id)

    def mk_episodic_operational_state_report_body(self, mdib_version, sequence_id, states, nsmapper):
        return self._mk_report_body(msgTag('EpisodicOperationalStateReport'),
                                    msgTag('OperationState'),
                                    states, nsmapper, mdib_version, sequence_id)

    def mk_periodic_operational_state_report_body(self, mdib_version, sequence_id, report_parts, ns_map):
        return self._mk__periodic_report_body(msgTag('PeriodicOperationalStateReport'),
                                              msgTag('OperationState'),
                                              report_parts, ns_map, mdib_version, sequence_id)

    def mk_episodic_alert_report_body(self, mdib_version, sequence_id, states, nsmapper):
        return self._mk_report_body(msgTag('EpisodicAlertReport'),
                                    msgTag('AlertState'),
                                    states, nsmapper, mdib_version, sequence_id)

    def mk_periodic_alert_report_body(self, mdib_version, sequence_id, report_parts, nsmapper):
        return self._mk__periodic_report_body(msgTag('PeriodicAlertReport'),
                                              msgTag('AlertState'),
                                              report_parts, nsmapper, mdib_version, sequence_id)

    def mk_episodic_component_state_report_body(self, mdib_version, sequence_id, states, nsmapper):
        return self._mk_report_body(msgTag('EpisodicComponentReport'),
                                    msgTag('ComponentState'),
                                    states, nsmapper, mdib_version, sequence_id)

    def mk_periodic_component_state_report_body(self, mdib_version, sequence_id, report_parts, nsmapper):
        return self._mk__periodic_report_body(msgTag('PeriodicComponentReport'),
                                              msgTag('ComponentState'),
                                              report_parts, nsmapper, mdib_version, sequence_id)

    def mk_episodic_context_report_body(self, mdib_version, sequence_id, states, nsmapper):
        return self._mk_report_body(msgTag('EpisodicContextReport'),
                                    msgTag('ContextState'),
                                    states, nsmapper, mdib_version, sequence_id)

    def mk_periodic_context_report_body(self, mdib_version, sequence_id, report_parts, nsmapper):
        return self._mk__periodic_report_body(msgTag('PeriodicContextReport'),
                                              msgTag('ContextState'),
                                              report_parts, nsmapper, mdib_version, sequence_id)

    def mk_description_modification_report_body(self, mdib_version, sequence_id, updated, created, deleted,
                                                updated_states, nsmapper):
        body_node = etree_.Element(msgTag('DescriptionModificationReport'),
                                   attrib={'SequenceId': sequence_id,
                                           'MdibVersion': str(mdib_version)},
                                   nsmap=Prefixes.partial_map(Prefixes.MSG, Prefixes.PM))
        self._mk_descriptor_updates_report_part(body_node, 'Upt', updated, updated_states, nsmapper)
        self._mk_descriptor_updates_report_part(body_node, 'Crt', created, updated_states, nsmapper)
        self._mk_descriptor_updates_report_part(body_node, 'Del', deleted, updated_states, nsmapper)
        return body_node

    @staticmethod
    def _mk_descriptor_updates_report_part(parent_node, modification_type, descriptors, updated_states, nsmapper):
        """ Helper that creates ReportPart."""
        # This method creates one ReportPart for every descriptor.
        # An optimization is possible by grouping all descriptors with the same parent handle into one ReportPart.
        # This is not implemented, and I think it is not needed.
        for descriptor in descriptors:
            report_part = etree_.SubElement(parent_node, msgTag('ReportPart'),
                                            attrib={'ModificationType': modification_type})
            if descriptor.parent_handle is not None:  # only Mds can have None
                report_part.set('ParentDescriptor', descriptor.parent_handle)
            report_part.append(descriptor.mk_descriptor_node(tag=msgTag('Descriptor'), nsmapper=nsmapper))
            related_state_containers = [s for s in updated_states if s.descriptorHandle == descriptor.handle]
            state_name = msgTag('State')
            report_part.extend([state.mk_state_node(state_name, nsmapper) for state in related_state_containers])

    @staticmethod
    def _mk_report_body(body_tag, state_tag, states, nsmapper, mdib_version, sequence_id):
        ns_map = nsmapper.partial_map(Prefixes.PM, Prefixes.MSG, Prefixes.XSI, Prefixes.EXT, Prefixes.XML)
        body_node = etree_.Element(body_tag,
                                   attrib={'SequenceId': sequence_id,
                                           'MdibVersion': str(mdib_version)},
                                   nsmap=ns_map)
        report_part_node = etree_.SubElement(body_node, msgTag('ReportPart'))

        for state in states:
            report_part_node.append(state.mk_state_node(state_tag, nsmapper, update_descriptor_version=False))
        return body_node

    @staticmethod
    def _mk__periodic_report_body(body_tag, state_tag, report_parts, nsmapper, mdib_version, sequence_id):
        ns_map = nsmapper.partial_map(Prefixes.PM, Prefixes.MSG, Prefixes.XSI, Prefixes.EXT, Prefixes.XML)
        body_node = etree_.Element(body_tag,
                                   attrib={'SequenceId': sequence_id,
                                           'MdibVersion': str(mdib_version)},
                                   nsmap=ns_map)
        for part in report_parts:
            report_part_node = etree_.SubElement(body_node, msgTag('ReportPart'))
            for state in part.states:
                report_part_node.append(state.mk_state_node(state_tag, nsmapper))
        return body_node

    @staticmethod
    def mk_operation_invoked_report_body(mdib_version, sequence_id,
                                         operation_handle_ref, transaction_id, invocation_state,
                                         error, error_message, nsmapper):
        ns_map = nsmapper.partial_map(Prefixes.PM, Prefixes.MSG)
        body_node = etree_.Element(msgTag('OperationInvokedReport'),
                                   attrib={'SequenceId': sequence_id,
                                           'MdibVersion': str(mdib_version)},
                                   nsmap=ns_map)
        report_part_node = etree_.SubElement(body_node,
                                             msgTag('ReportPart'),
                                             attrib={'OperationHandleRef': operation_handle_ref})
        invocation_info_node = etree_.SubElement(report_part_node, msgTag('InvocationInfo'))
        invocation_source_node = etree_.SubElement(report_part_node, msgTag('InvocationSource'),
                                                   attrib={'Root': Prefixes.SDC.namespace,
                                                           'Extension': 'AnonymousSdcParticipant'})
        # implemented only SDC R0077 for value of invocationSourceNode:
        # Root =  "http://standards.ieee.org/downloads/11073/11073-20701-2018"
        # Extension = "AnonymousSdcParticipant".
        # a known participant (R0078) is currently not supported
        # ToDo: implement R0078
        transaction_id_node = etree_.SubElement(invocation_info_node, msgTag('TransactionId'))
        transaction_id_node.text = str(transaction_id)
        operation_state_node = etree_.SubElement(invocation_info_node, msgTag('InvocationState'))
        operation_state_node.text = str(invocation_state)
        if error is not None:
            error_node = etree_.SubElement(invocation_info_node, msgTag('InvocationError'))
            error_node.text = str(error)
        if error_message is not None:
            error_message_node = etree_.SubElement(invocation_info_node, msgTag('InvocationErrorMessage'))
            error_message_node.text = str(error_message)
        return body_node

    @staticmethod
    def mk_notification_report(ws_addr, body_node, ident_nodes, doc_nsmap):
        envelope = soapenvelope.Soap12Envelope(doc_nsmap)
        envelope.add_body_element(body_node)
        envelope.set_address(ws_addr)
        for ident_node in ident_nodes:
            envelope.add_header_element(ident_node)
        return envelope

    @staticmethod
    def _get_action_string(sdc_definitions, method_name):
        actions_lookup = sdc_definitions.Actions
        return getattr(actions_lookup, method_name)
