import time
import traceback
from collections import namedtuple, OrderedDict
from io import BytesIO

from lxml import etree as etree_

from .exceptions import InvalidActionError, FunctionNotImplementedError
from .. import loghelper
from .. import pmtypes
from .. import pysoap
from ..namespaces import Prefixes
from ..namespaces import msgTag, domTag, s12Tag, wsxTag, wseTag, dpwsTag, mdpwsTag, nsmap

_msg_prefix = Prefixes.MSG.prefix

_wsdl_ns = Prefixes.WSDL.namespace
_wsdl_message = etree_.QName(_wsdl_ns, 'message')
_wsdl_part = etree_.QName(_wsdl_ns, 'part')
_wsdl_operation = etree_.QName(_wsdl_ns, 'operation')

_WSP_NS = 'http://www.w3.org/ns/ws-policy'
_WSP_PREFIX = 'wsp'

# DiscoveryType, only used in SDC
_DISCOVERY_TYPE_NS = "http://standards.ieee.org/downloads/11073/11073-10207-2017"

_WSDL_S12 = "http://schemas.xmlsoap.org/wsdl/soap12/"  # old soap 12 namespace, used in wsdl 1.1. used only for wsdl

# WSDL Generation:
# types to allow declaration of a wsdl data per service
WSDLMessageDescription = namedtuple('WSDLMessageDescription', 'name parameters ')
WSDLOperationBinding = namedtuple('WSDLOperationBinding', 'name input output')


def etree_from_file(path):
    parser = etree_.ETCompatXMLParser(resolve_entities=False)
    with open(path, 'rb') as opened_file:
        xml_text = opened_file.read()
    return etree_.fromstring(xml_text, parser=parser, base_url=path)


class SOAPActionDispatcher:
    def __init__(self, log_prefix=None):
        self._soap_action_callbacks = {}
        self._get_callbacks = {}
        self._logger = loghelper.get_logger_adapter('sdc.device.{}'.format(self.__class__.__name__), log_prefix)

    def register_action_callback(self, action, func):
        self._soap_action_callbacks[action] = func

    def register_get_callback(self, path, query, func):
        if path.endswith('/'):
            path = path[:-1]
        self._get_callbacks[(path, query)] = func

    def dispatch_soap_request(self, path, http_header, envelope):
        begin = time.monotonic()
        action = envelope.address.action
        func = self.get_action_handler(action)
        if func is None:
            raise InvalidActionError(envelope)
        returned_envelope = func(http_header, envelope)
        duration = time.monotonic() - begin
        self._logger.debug('incoming soap action "{}" to {}: duration={:.3f}sec.', action, path, duration)
        return returned_envelope

    def dispatch_get_request(self, parse_result, http_header):  # pylint:disable=unused-argument
        begin = time.monotonic()
        path = parse_result.path
        if path.endswith('/'):
            path = path[:-1]
        key = (path, parse_result.query)
        func = self._get_callbacks.get(key)
        if func is not None:
            self._logger.debug('dispatch_get_request:path="{}" ,function="{}"', key, func.__name__)
            result = func()
            duration = time.monotonic() - begin
            self._logger.debug('dispatch_get_request:duration="{:.4f}"', duration)
            return result
        self._logger.error('dispatch_get_request:path="{}" ,no handler found!', key)
        raise KeyError('dispatch_get_request:path="{}" ,no handler found!'.format(key))

    def get_action_handler(self, action):
        """ returns a callable or None"""
        return self._soap_action_callbacks.get(action)

    def get_actions(self):
        """ returns a list of action strings that can be handled."""
        return list(self._soap_action_callbacks.keys())


class _SOAPActionDispatcherWithSubDispatchers(SOAPActionDispatcher):
    """ receiver of all messages"""

    def __init__(self, sub_dispatchers=None):
        super().__init__()
        self._sub_dispatchers = sub_dispatchers or []  # chained SOAPActionDispatcher

    def get_action_handler(self, action):
        """ returns a callable or None"""
        func = self._soap_action_callbacks.get(action)
        if func is not None:
            return func
        for sub_dispatcher in self._sub_dispatchers:
            func = sub_dispatcher.get_action_handler(action)
            if func is not None:
                return func
        return None

    def get_actions(self):
        """ returns a list of action strings that can be handled."""
        actions = list(self._soap_action_callbacks.keys())
        for sub_dispatcher in self._sub_dispatchers:
            actions.extend(sub_dispatcher.get_actions())
        return actions

    def __repr__(self):
        return '{} actions={}'.format(self.__class__.__name__, self._soap_action_callbacks.keys())


class EventService(_SOAPActionDispatcherWithSubDispatchers):
    """ A service that offers subscriptions"""

    def __init__(self, sdc_device, sub_dispatchers, offered_subscriptions):
        super().__init__(sub_dispatchers)

        self._sdc_device = sdc_device
        self._subscriptions_manager = sdc_device.subscriptions_manager
        self._offered_subscriptions = offered_subscriptions
        self.register_action_callback('{}/Subscribe'.format(Prefixes.WSE.namespace), self._on_subscribe)
        self.register_action_callback('{}/Unsubscribe'.format(Prefixes.WSE.namespace), self._on_unsubscribe)
        self.register_action_callback('{}/GetStatus'.format(Prefixes.WSE.namespace), self._on_get_status)
        self.register_action_callback('{}/Renew'.format(Prefixes.WSE.namespace), self._on_renew_status)
        self.epr = None

    def _on_subscribe(self, http_header, envelope):
        subscription_filter_nodes = envelope.body_node.xpath(
            "//wse:Filter[@Dialect='{}/Action']".format(Prefixes.DPWS.namespace),
            namespaces=nsmap)
        if len(subscription_filter_nodes) != 1:
            raise Exception
        subscription_filters = subscription_filter_nodes[0].text
        for subscription_filter in subscription_filters.split():
            if subscription_filter not in self._offered_subscriptions:
                raise Exception('{}::{}: "{}" is not in offered subscriptions: {}'.format(self.__class__.__name__,
                                                                                          self.epr,
                                                                                          subscription_filter,
                                                                                          self._offered_subscriptions))
        returned_envelope = self._subscriptions_manager.on_subscribe_request(http_header, envelope, self.epr)
        self._validate_eventing_response(returned_envelope)
        return returned_envelope

    def _on_unsubscribe(self, http_header, envelope):  # pylint:disable=unused-argument

        returned_envelope = self._subscriptions_manager.on_unsubscribe_request(envelope)
        self._validate_eventing_response(returned_envelope)
        return returned_envelope

    def _on_get_status(self, http_header, envelope):  # pylint:disable=unused-argument
        returned_envelope = self._subscriptions_manager.on_get_status_request(envelope)
        self._validate_eventing_response(returned_envelope)
        return returned_envelope

    def _on_renew_status(self, http_header, envelope):  # pylint:disable=unused-argument
        returned_envelope = self._subscriptions_manager.on_renew_request(envelope)
        self._validate_eventing_response(returned_envelope)
        return returned_envelope

    def _validate_eventing_response(self, returned_envelope):
        returned_envelope.build_doc()
        body_node_children = list(returned_envelope.body_node)
        if len(body_node_children) == 0:
            return
        if body_node_children[0].tag == s12Tag('Fault'):
            schema = self._s12_schema
        else:
            schema = self._evt_schema
        returned_envelope.validate_body(schema)

    @property
    def _evt_schema(self):
        return None if not self._sdc_device.shall_validate else self._sdc_device.mdib.biceps_schema.eventing_schema

    @property
    def _s12_schema(self):
        return None if not self._sdc_device.shall_validate else self._sdc_device.mdib.biceps_schema.soap12_schema


class DPWSHostedService(EventService):
    """ An Endpoint (url) with one or more DPWS Types"""

    def __init__(self, sdc_device, base_urls, path_suffix, sub_dispatchers, offered_subscriptions):
        """
        @param base_urls: urlparse.SplitResult instances. They define the base addresses of this service
        """
        super().__init__(sdc_device, sub_dispatchers, offered_subscriptions)

        self._base_urls = base_urls
        self._mdib = sdc_device.mdib
        self._my_port_types = [p.port_type_string for p in sub_dispatchers]
        self._wsdl_string = self._mk_wsdl_string()
        my_uuid = base_urls[0].path
        for sub_dispatcher in sub_dispatchers:
            sub_dispatcher.hosting_service = self
        self.epr = '/{}/{}'.format(my_uuid, path_suffix)  # end point reference
        endpoint_references_list = []
        for addr in base_urls:
            endpoint_references_list.append(
                pysoap.soapenvelope.WsaEndpointReferenceType('{}/{}'.format(addr.geturl(), path_suffix)))
        porttype_ns = sdc_device.mdib.sdc_definitions.PortTypeNamespace
        # little bit ugly: normalize_xml_text needs bytes, not string. and it looks for namespace in "".
        _normalized = sdc_device.mdib.sdc_definitions.normalize_xml_text(b'"' + porttype_ns.encode('utf-8') + b'"')
        porttype_ns = _normalized[1:-1].decode('utf-8')
        # porttype_ns = sdc_device.mdib.sdc_definitions.normalize_xml_text(b'"' + porttype_ns.encode('utf-8') + b'"')[
        #               1:-1].decode('utf-8')
        self.hosted_inf = pysoap.soapenvelope.DPWSHosted(
            endpoint_references_list=endpoint_references_list,
            types_list=[etree_.QName(porttype_ns, p) for p in self._my_port_types],
            service_id=self._my_port_types[0])

        self.register_action_callback('{}/GetMetadata/Request'.format(Prefixes.WSX.namespace), self._on_get_metadata)
        self.register_get_callback(path=self.epr, query='wsdl', func=self._on_get_wsdl)

    def _on_get_wsdl(self):
        """ return wsdl"""
        self._logger.debug('_onGetWsdl returns {}', self._wsdl_string)
        return self._wsdl_string

    def _mk_wsdl_string(self):
        biceps_schema = self._sdc_device.mdib.biceps_schema
        sdc_definitions = self._sdc_device.mdib.sdc_definitions
        my_nsmap = Prefixes.partial_map(Prefixes.MSG, Prefixes.PM, Prefixes.WSA, Prefixes.WSE, Prefixes.DPWS)
        my_nsmap['tns'] = Prefixes.SDC.namespace
        my_nsmap['dt'] = _DISCOVERY_TYPE_NS
        porttype_prefix = 'tns'
        my_nsmap['wsdl'] = _wsdl_ns
        my_nsmap['s12'] = _WSDL_S12
        my_nsmap[_WSP_PREFIX] = _WSP_NS
        wsdl_definitions = etree_.Element(etree_.QName(_wsdl_ns, 'definitions'),
                                          nsmap=my_nsmap,
                                          attrib={'targetNamespace': sdc_definitions.PortTypeNamespace})

        types = etree_.SubElement(wsdl_definitions, etree_.QName(_wsdl_ns, 'types'))
        # remove annotations from schemas, this reduces wsdl size from 280kb to 100kb!
        ext_schema_ = etree_from_file(sdc_definitions.SchemaFilePaths.ExtensionPointSchemaFile)
        ext_schema = self._remove_annotations(ext_schema_)
        pm_schema_ = etree_from_file(sdc_definitions.SchemaFilePaths.ParticipantModelSchemaFile)
        participant_schema = self._remove_annotations(pm_schema_)
        bmm_schema_ = etree_from_file(sdc_definitions.SchemaFilePaths.MessageModelSchemaFile)
        message_schema = self._remove_annotations(bmm_schema_)
        types.append(ext_schema)
        types.append(participant_schema)
        types.append(message_schema)
        # append all message nodes
        for sub_dispatcher in self._sub_dispatchers:
            sub_dispatcher.add_wsdl_messages(wsdl_definitions)
        for sub_dispatcher in self._sub_dispatchers:
            sub_dispatcher.add_wsdl_port_type(wsdl_definitions)
        for sub_dispatcher in self._sub_dispatchers:
            sub_dispatcher.add_wsdl_binding(wsdl_definitions, porttype_prefix)

        return sdc_definitions.denormalize_xml_text(etree_.tostring(wsdl_definitions))

    def _remove_annotations(self, root_node):
        remove_annotations_string = b'''<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                                      xmlns:xs="http://www.w3.org/2001/XMLSchema">
          <xsl:output method="xml" indent="yes"/>

          <xsl:template match="@* | node()">
            <xsl:copy>
              <xsl:apply-templates select="@* | node()"/>
            </xsl:copy>
          </xsl:template>

          <xsl:template match="xs:annotation" />
        </xsl:stylesheet>'''
        remove_annotations_doc = etree_.parse(BytesIO(remove_annotations_string))
        remove_annotations_xslt = etree_.XSLT(remove_annotations_doc)
        return remove_annotations_xslt(root_node).getroot()

    def _on_get_metadata(self, http_header, request):
        _nsm = self._mdib.nsmapper
        response = pysoap.soapenvelope.Soap12Envelope(_nsm.doc_ns_map)
        reply_address = request.address.mk_reply_address(
            'http://schemas.xmlsoap.org/ws/2004/09/mex/GetMetadata/Response')
        response.add_header_object(reply_address)

        metadata_node = etree_.Element(wsxTag('Metadata'), nsmap=_nsm.doc_ns_map)

        # Relationship
        metadata_section_node = etree_.SubElement(metadata_node,
                                                  wsxTag('MetadataSection'),
                                                  attrib={'Dialect': '{}/Relationship'.format(Prefixes.DPWS.namespace)})

        relationship_node = etree_.SubElement(metadata_section_node,
                                              dpwsTag('Relationship'),
                                              attrib={'Type': '{}/host'.format(Prefixes.DPWS.namespace)})
        self._sdc_device.dpws_host.as_etree_subnode(relationship_node)

        self.hosted_inf.as_etree_subnode(relationship_node)

        metadata_section_node = etree_.SubElement(metadata_node,
                                                  wsxTag('MetadataSection'),
                                                  attrib={'Dialect': _wsdl_ns})
        location_node = etree_.SubElement(metadata_section_node,
                                          wsxTag('Location'))
        # determine the correct location of wsdl, depending on call
        host = http_header['Host']  # this is the address that was called.
        my_base_urls = [u for u in self._base_urls if u.netloc == host]
        my_base_url = my_base_urls[0] if len(my_base_urls) > 0 else self._base_urls[0]
        location_node.text = '{}://{}{}/?wsdl'.format(my_base_url.scheme,
                                                      my_base_url.netloc,
                                                      self.epr)
        response.add_body_element(metadata_node)
        response.validate_body(self._mdib.biceps_schema.mex_schema)
        return response

    def __repr__(self):
        return '{} epr={} Porttypes={}'.format(self.__class__.__name__, self.epr,
                                               [dp.port_type_string for dp in self._sub_dispatchers])


class DPWSPortTypeImpl(SOAPActionDispatcher):
    """ Base class of all PortType implementations"""
    WSDLOperationBindings = ()  # overwrite in derived classes
    WSDLMessageDescriptions = ()  # overwrite in derived classes

    def __init__(self, port_type_string, sdc_device):
        """
        :param port_type_string: port type without namespace, e.g 'Get'
        :param sdcDevice:
        """
        super().__init__()
        self.port_type_string = port_type_string
        self._sdc_device = sdc_device
        self._mdib = sdc_device.mdib
        self.hosting_service = None  # the parent

    def _get_action_string(self, method_name):
        actions_lookup = self._mdib.sdc_definitions.Actions
        return getattr(actions_lookup, method_name)

    @property
    def _bmm_schema(self):
        return None if not self._sdc_device.shall_validate else self._sdc_device.mdib.biceps_schema.message_schema

    @property
    def _mex_schema(self):
        return None if not self._sdc_device.shall_validate else self._sdc_device.mdib.biceps_schema.mex_schema

    @property
    def _evt_schema(self):
        return None if not self._sdc_device.shall_validate else self._sdc_device.mdib.biceps_schema.eventing_schema

    @property
    def _s12_schema(self):
        return None if not self._sdc_device.shall_validate else self._sdc_device.mdib.biceps_schema.soap12_schema

    def add_wsdl_port_type(self, parent_node):
        raise NotImplementedError

    def __repr__(self):
        return '{} Porttype={} actions={}'.format(self.__class__.__name__, self.port_type_string,
                                                  self._soap_action_callbacks.keys())

    def add_wsdl_messages(self, parent_node):
        """
        add wsdl:message node to parent_node.
        xml looks like this:
        <wsdl:message name="GetMdDescription">
            <wsdl:part element="msg:GetMdDescription" name="parameters" />
        </wsdl:message>
        :param parent_node:
        :return:
        """
        for msg in self.WSDLMessageDescriptions:
            elem = etree_.SubElement(parent_node, _wsdl_message, attrib={'name': msg.name})
            for element_name in msg.parameters:
                etree_.SubElement(elem, _wsdl_part,
                                  attrib={'name': 'parameters',
                                          'element': element_name})

    def add_wsdl_binding(self, parent_node, porttype_prefix):
        """
        add wsdl:binding node to parent_node.
        xml looks like this:
        <wsdl:binding name="GetBinding" type="msg:Get">
            <s12:binding style="document" transport="http://schemas.xmlsoap.org/soap/http" />
            <wsdl:operation name="GetMdib">
                <s12:operation soapAction="http://p11073-10207/draft6/msg/2016/12/08/Get/GetMdib" />
                <wsdl:input>
                    <s12:body use="literal" />
                </wsdl:input>
                <wsdl:output>
                    <s12:body use="literal" />
                </wsdl:output>
            </wsdl:operation>
            ...
        </wsdl:binding>
        :param parent_node:
        :param porttype_prefix:
        :return:
        """
        v_ref = self._sdc_device.mdib.sdc_definitions
        wsdl_binding = etree_.SubElement(parent_node, etree_.QName(_wsdl_ns, 'binding'),
                                         attrib={'name': self.port_type_string + 'Binding',
                                                 'type': '{}:{}'.format(porttype_prefix, self.port_type_string)})
        etree_.SubElement(wsdl_binding, etree_.QName(_WSDL_S12, 'binding'),
                          attrib={'style': 'document', 'transport': 'http://schemas.xmlsoap.org/soap/http'})
        # ToDo: wsp:policy?
        for wsdl_op in self.WSDLOperationBindings:
            wsdl_operation = etree_.SubElement(wsdl_binding, etree_.QName(_wsdl_ns, 'operation'),
                                               attrib={'name': wsdl_op.name})
            etree_.SubElement(wsdl_operation, etree_.QName(_WSDL_S12, 'operation'),
                              attrib={'soapAction': '{}/{}/{}'.format(v_ref.ActionsNamespace,
                                                                      self.port_type_string,
                                                                      wsdl_op.name)})
            if wsdl_op.input is not None:
                wsdl_input = etree_.SubElement(wsdl_operation, etree_.QName(_wsdl_ns, 'input'))
                etree_.SubElement(wsdl_input, etree_.QName(_WSDL_S12, 'body'), attrib={'use': wsdl_op.input})
            if wsdl_op.output is not None:
                wsdl_output = etree_.SubElement(wsdl_operation, etree_.QName(_wsdl_ns, 'output'))
                etree_.SubElement(wsdl_output, etree_.QName(_WSDL_S12, 'body'), attrib={'use': wsdl_op.output})
        _add_policy_dpws_profile(wsdl_binding)


def _mk_wsdl_operation(parent_node, operation_name, input_message_name, output_message_name, fault):
    elem = etree_.SubElement(parent_node, _wsdl_operation, attrib={'name': operation_name})
    if input_message_name is not None:
        etree_.SubElement(elem, etree_.QName(_wsdl_ns, 'input'),
                          attrib={'message': '{}:{}'.format('tns', input_message_name),
                                  })
    if output_message_name is not None:
        etree_.SubElement(elem, etree_.QName(_wsdl_ns, 'output'),
                          attrib={'message': '{}:{}'.format('tns', output_message_name),
                                  })
    if fault is not None:
        fault_name, message_name, action = fault  # unpack 3 parameters
        etree_.SubElement(elem, etree_.QName(_wsdl_ns, 'fault'),
                          attrib={'name': fault_name,
                                  'message': '{}:{}'.format('tns', message_name),
                                  })
    return elem


def mk_wsdl_two_way_operation(parent_node, operation_name, input_message_name=None, output_message_name=None,
                              fault=None):
    # has input and output
    input_msg_name = input_message_name or operation_name  # defaults to operation name
    output_msg_name = output_message_name or operation_name + 'Response'  # defaults to operation name + "Response"
    return _mk_wsdl_operation(parent_node, operation_name=operation_name, input_message_name=input_msg_name,
                              output_message_name=output_msg_name, fault=fault)


def _mk_wsdl_one_way_operation(parent_node, operation_name, output_message_name=None, fault=None):
    # has only output
    output_msg_name = output_message_name or operation_name  # defaults to operation name
    return _mk_wsdl_operation(parent_node, operation_name=operation_name, input_message_name=None,
                              output_message_name=output_msg_name, fault=fault)


def _add_policy_dpws_profile(parent_node):
    """
    :param parent_node:
    :return: <wsp:Policy>
            <dpws:Profile wsp:Optional="true"/>
            <mdpws:Profile wsp:Optional="true"/>
          </wsp:Policy>
    """
    wsp_policy_node = etree_.SubElement(parent_node, etree_.QName(_WSP_NS, 'Policy'), attrib=None)
    _ = etree_.SubElement(wsp_policy_node, dpwsTag('Profile'), attrib={etree_.QName(_WSP_NS, 'Optional'): 'true'})
    _ = etree_.SubElement(wsp_policy_node, mdpwsTag('Profile'), attrib={etree_.QName(_WSP_NS, 'Optional'): 'true'})


class GetService(DPWSPortTypeImpl):
    WSDLMessageDescriptions = (WSDLMessageDescription('GetMdState', ('{}:GetMdState'.format(_msg_prefix),)),
                               WSDLMessageDescription('GetMdStateResponse',
                                                      ('{}:GetMdStateResponse'.format(_msg_prefix),)),
                               WSDLMessageDescription('GetMdib', ('{}:GetMdib'.format(_msg_prefix),)),
                               WSDLMessageDescription('GetMdibResponse', ('{}:GetMdibResponse'.format(_msg_prefix),)),
                               WSDLMessageDescription('GetMdDescription', ('{}:GetMdDescription'.format(_msg_prefix),)),
                               WSDLMessageDescription('GetMdDescriptionResponse',
                                                      ('{}:GetMdDescriptionResponse'.format(_msg_prefix),)),
                               )
    WSDLOperationBindings = (WSDLOperationBinding('GetMdState', 'literal', 'literal'),
                             WSDLOperationBinding('GetMdib', 'literal', 'literal'),
                             WSDLOperationBinding('GetMdDescription', 'literal', 'literal'),)

    def __init__(self, port_type_string, sdcDevice):
        super().__init__(port_type_string, sdcDevice)
        actions = self._mdib.sdc_definitions.Actions
        self.register_action_callback(actions.GetMdState, self._on_get_md_state)
        self.register_action_callback(actions.GetMdib, self._on_get_mdib)
        self.register_action_callback(actions.GetMdDescription, self._on_get_md_description)

    def _on_get_md_state(self, http_header, request):  # pylint:disable=unused-argument
        self._logger.debug('_on_get_md_state')
        requested_handles = self._sdc_device.msg_reader.read_getmdstate_request(request)
        if len(requested_handles) > 0:
            self._logger.info('_on_get_md_state requested Handles:{}', requested_handles)

        # get the requested state containers from mdib
        state_containers = []
        with self._mdib.mdib_lock:
            if len(requested_handles) == 0:
                # MessageModel: If the HANDLE reference list is empty, all states in the MDIB SHALL be included in the result list.
                state_containers.extend(self._mdib.states.objects)
                if self._sdc_device.contextstates_in_getmdib:
                    state_containers.extend(self._mdib.context_states.objects)
            else:
                if self._sdc_device.contextstates_in_getmdib:
                    for handle in requested_handles:
                        try:
                            # If a HANDLE reference does match a multi state HANDLE, the corresponding multi state SHALL be included in the result list
                            state_containers.append(self._mdib.context_states.handle.getOne(handle))
                        except RuntimeError:
                            # If a HANDLE reference does match a descriptor HANDLE, all states that belong to the corresponding descriptor SHALL be included in the result list
                            state_containers.extend(self._mdib.states.descriptorHandle.get(handle, []))
                            state_containers.extend(self._mdib.context_states.descriptorHandle.get(handle, []))
                else:
                    for handle in requested_handles:
                        state_containers.extend(self._mdib.states.descriptorHandle.get(handle, []))

                self._logger.info('_on_get_md_state requested Handles:{} found {} states', requested_handles,
                                  len(state_containers))

            response_envelope = self._sdc_device.msg_factory.mk_getmdstate_response_envelope(
                request, self._mdib, state_containers)
        self._logger.debug('_on_get_md_state returns {}', lambda: response_envelope.as_xml(pretty=False))
        response_envelope.validate_body(self._bmm_schema)
        return response_envelope

    def _on_get_mdib(self, http_header, request):  # pylint:disable=unused-argument
        self._logger.debug('_on_get_mdib')
        response_envelope = self._sdc_device.msg_factory.mk_getmdib_response_envelope(
            request, self._mdib, self._sdc_device.contextstates_in_getmdib)

        self._logger.debug('_on_get_mdib returns {}', lambda: response_envelope.as_xml(pretty=False))
        try:
            response_envelope.validate_body(self._bmm_schema)
        except Exception as ex:
            self._logger.error('_on_get_mdib: invalid body:{}', traceback.format_exc())
            raise
        return response_envelope

    def _on_get_md_description(self, http_header, request):  # pylint:disable=unused-argument
        """
        MdDescription comprises the requested set of MDS descriptors. Which MDS descriptors are included depends on the msg:GetMdDescription/msg:HandleRef list:
        - If the HANDLE reference list is empty, all MDS descriptors SHALL be included in the result list.
        - If a HANDLE reference does match an MDS descriptor, it SHALL be included in the result list.
        - If a HANDLE reference does not match an MDS descriptor (any other descriptor), the MDS descriptor that is in the parent tree of the HANDLE reference SHOULD be included in the result list.
        """
        # currently this implementation only supports a single mds.
        # => if at least one handle matches any descriptor, the one mds is returned, otherwise empty payload

        self._logger.debug('_on_get_md_description')
        requested_handles = self._sdc_device.msg_reader.read_getmddescription_request(request)
        if len(requested_handles) > 0:
            self._logger.info('_on_get_md_description requested Handles:{}', requested_handles)
        response_envelope = self._sdc_device.msg_factory.mk_getmddescription_response_envelope(
            request, self._sdc_device.mdib, requested_handles
        )
        self._logger.debug('_on_get_md_description returns {}', lambda: response_envelope.as_xml(pretty=False))
        response_envelope.validate_body(self._bmm_schema)
        return response_envelope

    def add_wsdl_port_type(self, parent_node):
        """
        add wsdl:portType node to parent_node.
        xml looks like this:
        <wsdl:portType name="GetService" dpws:DiscoveryType="dt:ServiceProvider">
          <wsdl:operation name="GetMdState">
            <wsdl:input message="msg:GetMdState"/>
            <wsdl:output message="msg:GetMdStateResponse"/>
          </wsdl:operation>
          <wsp:Policy>
            <dpws:Profile wsp:Optional="true"/>
          </wsp:Policy>
          ...
        </wsdl:portType>
        :param parent_node:
        :return:
        """
        if 'dt' in parent_node.nsmap:
            port_type = etree_.SubElement(parent_node, etree_.QName(_wsdl_ns, 'portType'),
                                          attrib={'name': self.port_type_string,
                                                  dpwsTag('DiscoveryType'): 'dt:ServiceProvider'})
        else:
            port_type = etree_.SubElement(parent_node, etree_.QName(_wsdl_ns, 'portType'),
                                          attrib={'name': self.port_type_string})
        mk_wsdl_two_way_operation(port_type, operation_name='GetMdState')
        mk_wsdl_two_way_operation(port_type, operation_name='GetMdib')
        mk_wsdl_two_way_operation(port_type, operation_name='GetMdDescription')


class ContainmentTreeService(DPWSPortTypeImpl):
    WSDLMessageDescriptions = (WSDLMessageDescription('GetDescriptor', ('{}:GetDescriptor'.format(_msg_prefix),)),
                               WSDLMessageDescription('GetDescriptorResponse',
                                                      ('{}:GetDescriptorResponse'.format(_msg_prefix),)),
                               WSDLMessageDescription('GetContainmentTree',
                                                      ('{}:GetContainmentTreeResponse'.format(_msg_prefix),)),
                               WSDLMessageDescription('GetContainmentTreeResponse',
                                                      ('{}:GetContainmentTreeResponse'.format(_msg_prefix),)),
                               )
    WSDLOperationBindings = (WSDLOperationBinding('GetDescriptor', 'literal', 'literal'),
                             WSDLOperationBinding('GetContainmentTree', 'literal', 'literal'))

    def __init__(self, port_type_string, sdcDevice):
        super().__init__(port_type_string, sdcDevice)
        actions = self._mdib.sdc_definitions.Actions
        self.register_action_callback(actions.GetContainmentTree, self._on_get_containment_tree)
        self.register_action_callback(actions.GetDescriptor, self._on_get_descriptor)

    def _on_get_containment_tree(self, http_header, request):
        # ToDo: implement, currently method only raises a soap fault
        raise FunctionNotImplementedError(request)

    def _on_get_descriptor(self, http_header, request):
        # ToDo: implement, currently method only raises a soap fault
        raise FunctionNotImplementedError(request)

    def add_wsdl_port_type(self, parent_node):
        port_type = etree_.SubElement(parent_node, etree_.QName(_wsdl_ns, 'portType'),
                                      attrib={'name': self.port_type_string,
                                              dpwsTag('DiscoveryType'): 'dt:ServiceProvider'})
        mk_wsdl_two_way_operation(port_type, operation_name='GetDescriptor')
        mk_wsdl_two_way_operation(port_type, operation_name='GetContainmentTree')


class SetService(DPWSPortTypeImpl):
    WSDLMessageDescriptions = (WSDLMessageDescription('Activate', ('{}:Activate'.format(_msg_prefix),)),
                               WSDLMessageDescription('ActivateResponse', ('{}:ActivateResponse'.format(_msg_prefix),)),
                               WSDLMessageDescription('SetString', ('{}:SetString'.format(_msg_prefix),)),
                               WSDLMessageDescription('SetStringResponse',
                                                      ('{}:SetStringResponse'.format(_msg_prefix),)),
                               WSDLMessageDescription('SetComponentState',
                                                      ('{}:SetComponentState'.format(_msg_prefix),)),
                               WSDLMessageDescription('SetComponentStateResponse',
                                                      ('{}:SetComponentStateResponse'.format(_msg_prefix),)),
                               WSDLMessageDescription('SetAlertState', ('{}:SetAlertState'.format(_msg_prefix),)),
                               WSDLMessageDescription('SetAlertStateResponse',
                                                      ('{}:SetAlertStateResponse'.format(_msg_prefix),)),
                               WSDLMessageDescription('SetMetricState', ('{}:SetMetricState'.format(_msg_prefix),)),
                               WSDLMessageDescription('SetMetricStateResponse',
                                                      ('{}:SetMetricStateResponse'.format(_msg_prefix),)),
                               WSDLMessageDescription('SetValue', ('{}:SetValue'.format(_msg_prefix),)),
                               WSDLMessageDescription('SetValueResponse', ('{}:SetValueResponse'.format(_msg_prefix),)),
                               WSDLMessageDescription('OperationInvokedReport',
                                                      ('{}:OperationInvokedReport'.format(_msg_prefix),)),
                               )
    WSDLOperationBindings = (WSDLOperationBinding('Activate', 'literal', 'literal'),  # fault?
                             WSDLOperationBinding('SetString', 'literal', 'literal'),  # fault?
                             WSDLOperationBinding('SetComponentState', 'literal', 'literal'),  # fault?
                             WSDLOperationBinding('SetAlertState', 'literal', 'literal'),  # fault?
                             WSDLOperationBinding('SetMetricState', 'literal', 'literal'),  # fault?
                             WSDLOperationBinding('SetValue', 'literal', 'literal'),  # fault?
                             WSDLOperationBinding('OperationInvokedReport', None, 'literal'),
                             )

    def __init__(self, port_type_string, sdcDevice):
        super().__init__(port_type_string, sdcDevice)
        actions = self._mdib.sdc_definitions.Actions
        self.register_action_callback(actions.Activate, self._on_activate)
        self.register_action_callback(actions.SetValue, self._on_set_value)
        self.register_action_callback(actions.SetString, self._on_set_string)
        self.register_action_callback(actions.SetMetricState, self._on_set_metric_state)
        self.register_action_callback(actions.SetAlertState, self._on_set_alert_state)
        self.register_action_callback(actions.SetComponentState, self._on_set_component_state)

    def _on_activate(self, http_header, request):  # pylint:disable=unused-argument
        """Handler for Active calls.
        It enques an operation and generates the expected operation invoked report. """
        argument = request.body_node.xpath('*/msg:Argument/msg:ArgValue/text()', namespaces=nsmap)
        return self._handle_operation_request(request, 'ActivateResponse', argument)

    def _on_set_value(self, http_header, request):  # pylint:disable=unused-argument
        """Handler for SetValue calls.
        It enqueues an operation and generates the expected operation invoked report. """
        self._logger.info('_on_set_value')
        value_nodes = request.body_node.xpath('*/msg:RequestedNumericValue', namespaces=nsmap)
        if value_nodes:
            argument = float(value_nodes[0].text)
        else:
            argument = None
        ret = self._handle_operation_request(request, 'SetValueResponse', argument)
        self._logger.info('_on_set_value done')
        return ret

    def _on_set_string(self, http_header, request):  # pylint:disable=unused-argument
        """Handler for SetString calls.
        It enqueues an operation and generates the expected operation invoked report."""
        self._logger.debug('_on_set_string')
        string_node = request.body_node.xpath('*/msg:RequestedStringValue', namespaces=nsmap)
        if string_node:
            argument = str(string_node[0].text)
        else:
            argument = None
        return self._handle_operation_request(request, 'SetStringResponse', argument)

    def _on_set_metric_state(self, http_header, request):  # pylint:disable=unused-argument
        """Handler for SetMetricState calls.
        It enqueues an operation and generates the expected operation invoked report."""
        self._logger.debug('_on_set_metric_state')
        proposed_state_nodes = request.body_node.xpath('*/msg:ProposedMetricState', namespaces=nsmap)
        msg_reader = self._mdib.msg_reader
        argument = [msg_reader.mk_statecontainer_from_node(m, self._mdib) for m in proposed_state_nodes]
        return self._handle_operation_request(request, 'SetMetricStateResponse', argument)

    def _on_set_alert_state(self, http_header, request):  # pylint:disable=unused-argument
        """Handler for SetMetricState calls.
        It enqueues an operation and generates the expected operation invoked report."""
        self._logger.debug('_on_set_alert_state')
        proposed_state_nodes = request.body_node.xpath('*/msg:ProposedAlertState', namespaces=nsmap)
        if len(proposed_state_nodes) > 1:  # schema allows exactly one ProposedAlertState:
            raise ValueError(
                'only one ProposedAlertState argument allowed, found {}'.format(len(proposed_state_nodes)))
        if len(proposed_state_nodes) == 0:
            raise ValueError('no ProposedAlertState argument found')
        msg_reader = self._mdib.msg_reader
        argument = msg_reader.mk_statecontainer_from_node(proposed_state_nodes[0], self._mdib)

        return self._handle_operation_request(request, 'SetAlertStateResponse', argument)

    def _on_set_component_state(self, http_header, request):  # pylint:disable=unused-argument
        """Handler for SetMetricState calls.
        It enqueues an operation and generates the expected operation invoked report."""
        self._logger.debug('_on_set_component_state')
        proposed_state_nodes = request.body_node.xpath('*/msg:ProposedComponentState', namespaces=nsmap)
        msg_reader = self._mdib.msg_reader
        argument = [msg_reader.mk_statecontainer_from_node(p, self._mdib) for p in proposed_state_nodes]
        return self._handle_operation_request(request, 'SetComponentStateResponse', argument)

    def _handle_operation_request(self, request, response_name, argument):
        """
        It enqueues an operation and generate the expected operation invoked report.
        :param request:
        :param responseName:
        :param argument:
        :return:
        """
        response = pysoap.soapenvelope.Soap12Envelope(self._mdib.nsmapper.partial_map(Prefixes.S12, Prefixes.WSA))
        reply_address = request.address.mk_reply_address(action=self._get_action_string(response_name))
        response.add_header_object(reply_address)
        reply_body_node = etree_.Element(msgTag(response_name),
                                         attrib={'SequenceId': self._mdib.sequence_id,
                                                 'MdibVersion': str(self._mdib.mdib_version)},
                                         nsmap=Prefixes.partial_map(Prefixes.MSG))
        invocation_info_node = etree_.SubElement(reply_body_node, msgTag('InvocationInfo'))

        transaction_id_node = etree_.SubElement(invocation_info_node, msgTag('TransactionId'))
        invocation_state_node = etree_.SubElement(invocation_info_node, msgTag('InvocationState'))

        error_texts = []

        operation_handle_refs = request.body_node.xpath('*/msg:OperationHandleRef/text()', namespaces=nsmap)
        operation = None
        if len(operation_handle_refs) == 1:
            operation_handle_ref = operation_handle_refs[0]
            operation = self._sdc_device.get_operation_by_handle(operation_handle_ref)
            if operation is None:
                error_texts.append('operation not known: "{}"'.format(operation_handle_ref))
            else:
                request_tag = request.body_node[0].tag
                if request_tag != operation.OP_QNAME:
                    self._logger.error(f'operation types mismatch operation handle ={operation_handle_ref}!')
                    error_texts.append(
                        f'mismatch Operation {operation_handle_ref}: expect {operation.OP_QNAME}, got {request_tag}')
        else:
            error_texts.append('no OperationHandleRef found in Request')

        if error_texts:
            self._logger.warn('_handle_operation_request: error_texts = {}'.format(error_texts))

            invocation_state_node.text = pmtypes.InvocationState.FAILED
            transaction_id_node.text = '0'
            operation_error_node = etree_.SubElement(invocation_info_node, msgTag('InvocationError'))
            operation_error_node.text = pmtypes.InvocationError.INVALID_VALUE
            operation_error_msg_node = etree_.SubElement(invocation_info_node,
                                                         msgTag('InvocationErrorMessage'))
            operation_error_msg_node.text = '; '.join(error_texts)
        else:
            self._logger.info('_handle_operation_request: enqueued')
            transaction_id = self._sdc_device.enqueue_operation(operation, request, argument)
            transaction_id_node.text = str(transaction_id)
            invocation_state_node.text = pmtypes.InvocationState.WAIT

        response.add_body_element(reply_body_node)
        response.validate_body(self._bmm_schema)
        return response

    def add_wsdl_port_type(self, parent_node):
        port_type = etree_.SubElement(parent_node, etree_.QName(_wsdl_ns, 'portType'),
                                      attrib={'name': self.port_type_string,
                                              dpwsTag('DiscoveryType'): 'dt:ServiceProvider',
                                              wseTag('EventSource'): 'true'})
        mk_wsdl_two_way_operation(port_type, operation_name='Activate')
        mk_wsdl_two_way_operation(port_type, operation_name='SetString')
        mk_wsdl_two_way_operation(port_type, operation_name='SetComponentState')
        mk_wsdl_two_way_operation(port_type, operation_name='SetAlertState')
        mk_wsdl_two_way_operation(port_type, operation_name='SetMetricState')
        mk_wsdl_two_way_operation(port_type, operation_name='SetValue')
        _mk_wsdl_one_way_operation(port_type, operation_name='OperationInvokedReport')


class WaveformService(DPWSPortTypeImpl):
    WSDLMessageDescriptions = (WSDLMessageDescription('Waveform', ('{}:WaveformStreamReport'.format(_msg_prefix),)),)
    WSDLOperationBindings = (WSDLOperationBinding('Waveform', None, 'literal'),)

    def add_wsdl_port_type(self, parent_node):
        port_type = etree_.SubElement(parent_node, etree_.QName(_wsdl_ns, 'portType'),
                                      attrib={'name': self.port_type_string,
                                              dpwsTag('DiscoveryType'): 'dt:ServiceProvider',
                                              wseTag('EventSource'): 'true'})
        _mk_wsdl_one_way_operation(port_type, operation_name='Waveform')


class StateEventService(DPWSPortTypeImpl):
    WSDLMessageDescriptions = (
        WSDLMessageDescription('EpisodicAlertReport', ('{}:EpisodicAlertReport'.format(_msg_prefix),)),
        WSDLMessageDescription('SystemErrorReport', ('{}:SystemErrorReport'.format(_msg_prefix),)),
        WSDLMessageDescription('PeriodicAlertReport', ('{}:PeriodicAlertReport'.format(_msg_prefix),)),
        WSDLMessageDescription('EpisodicComponentReport', ('{}:EpisodicComponentReport'.format(_msg_prefix),)),
        WSDLMessageDescription('PeriodicOperationalStateReport',
                               ('{}:PeriodicOperationalStateReport'.format(_msg_prefix),)),
        WSDLMessageDescription('PeriodicComponentReport', ('{}:PeriodicComponentReport'.format(_msg_prefix),)),
        WSDLMessageDescription('EpisodicOperationalStateReport',
                               ('{}:EpisodicOperationalStateReport'.format(_msg_prefix),)),
        WSDLMessageDescription('PeriodicMetricReport', ('{}:PeriodicMetricReport'.format(_msg_prefix),)),
        WSDLMessageDescription('EpisodicMetricReport', ('{}:EpisodicMetricReport'.format(_msg_prefix),)),
    )

    WSDLOperationBindings = (WSDLOperationBinding('EpisodicAlertReport', None, 'literal'),
                             WSDLOperationBinding('SystemErrorReport', None, 'literal'),
                             WSDLOperationBinding('PeriodicAlertReport', None, 'literal'),
                             WSDLOperationBinding('EpisodicComponentReport', None, 'literal'),
                             WSDLOperationBinding('PeriodicOperationalStateReport', None, 'literal'),
                             WSDLOperationBinding('PeriodicComponentReport', None, 'literal'),
                             WSDLOperationBinding('EpisodicOperationalStateReport', None, 'literal'),
                             WSDLOperationBinding('PeriodicMetricReport', None, 'literal'),
                             WSDLOperationBinding('EpisodicMetricReport', None, 'literal'),
                             )

    def add_wsdl_port_type(self, parent_node):
        port_type = etree_.SubElement(parent_node, etree_.QName(_wsdl_ns, 'portType'),
                                      attrib={'name': self.port_type_string,
                                              dpwsTag('DiscoveryType'): 'dt:ServiceProvider',
                                              wseTag('EventSource'): 'true'})
        _mk_wsdl_one_way_operation(port_type, operation_name='EpisodicAlertReport')
        _mk_wsdl_one_way_operation(port_type, operation_name='SystemErrorReport')
        _mk_wsdl_one_way_operation(port_type, operation_name='PeriodicAlertReport')
        _mk_wsdl_one_way_operation(port_type, operation_name='EpisodicComponentReport')
        _mk_wsdl_one_way_operation(port_type, operation_name='PeriodicOperationalStateReport')
        _mk_wsdl_one_way_operation(port_type, operation_name='PeriodicComponentReport')
        _mk_wsdl_one_way_operation(port_type, operation_name='EpisodicOperationalStateReport')
        _mk_wsdl_one_way_operation(port_type, operation_name='PeriodicMetricReport')
        _mk_wsdl_one_way_operation(port_type, operation_name='EpisodicMetricReport')


class ContextService(DPWSPortTypeImpl):
    WSDLMessageDescriptions = (WSDLMessageDescription('SetContextState', ('{}:SetContextState'.format(_msg_prefix),)),
                               WSDLMessageDescription('SetContextStateResponse',
                                                      ('{}:SetContextStateResponse'.format(_msg_prefix),)),
                               WSDLMessageDescription('GetContextStates', ('{}:GetContextStates'.format(_msg_prefix),)),
                               WSDLMessageDescription('GetContextStatesResponse',
                                                      ('{}:GetContextStatesResponse'.format(_msg_prefix),)),
                               WSDLMessageDescription('EpisodicContextReport',
                                                      ('{}:EpisodicContextReport'.format(_msg_prefix),)),
                               WSDLMessageDescription('PeriodicContextReport',
                                                      ('{}:PeriodicContextReport'.format(_msg_prefix),)),
                               )
    WSDLOperationBindings = (WSDLOperationBinding('SetContextState', 'literal', 'literal'),  # ToDo: generate wsdl:fault
                             WSDLOperationBinding('GetContextStates', 'literal', 'literal'),
                             WSDLOperationBinding('EpisodicContextReport', None, 'literal'),
                             WSDLOperationBinding('PeriodicContextReport', None, 'literal'),
                             )

    def __init__(self, port_type_string, sdcDevice):
        super().__init__(port_type_string, sdcDevice)
        actions = self._mdib.sdc_definitions.Actions
        self.register_action_callback(actions.SetContextState, self._on_set_context_state)
        self.register_action_callback(actions.GetContextStates, self._on_get_context_states)

    def _on_set_context_state(self, http_header, request):  # pylint:disable=unused-argument
        """ enqueues an operation and returns a 'wait' reponse."""
        response = pysoap.soapenvelope.Soap12Envelope(
            self._mdib.nsmapper.partial_map(Prefixes.S12, Prefixes.PM, Prefixes.WSA, Prefixes.MSG))
        reply_address = request.address.mk_reply_address(action=self._get_action_string('SetContextStateResponse'))
        response.add_header_object(reply_address)
        reply_body_node = etree_.Element(msgTag('SetContextStateResponse'),
                                         nsmap=Prefixes.partial_map(Prefixes.MSG),
                                         attrib={'SequenceId': self._mdib.sequence_id,
                                                 'MdibVersion': str(self._mdib.mdib_version)})
        invocation_info_node = etree_.SubElement(reply_body_node,
                                                 msgTag('InvocationInfo'))
        transaction_id_node = etree_.SubElement(invocation_info_node, msgTag('TransactionId'))
        invocation_state_node = etree_.SubElement(invocation_info_node, msgTag('InvocationState'))

        error_texts = []

        operation_handle_refs = request.body_node.xpath('msg:SetContextState/msg:OperationHandleRef/text()',
                                                        namespaces=nsmap)
        if len(operation_handle_refs) == 1:
            operation_handle_ref = operation_handle_refs[0]
            operation = self._sdc_device.get_operation_by_handle(operation_handle_ref)
            if operation is None:
                error_texts.append('operation "{}" not known'.format(operation_handle_ref))
        elif len(operation_handle_refs) > 1:
            error_texts.append('multiple OperationHandleRefs found: "{}"'.format(operation_handle_refs))
        else:
            error_texts.append('no OperationHandleRef found')

        if error_texts:
            invocation_state_node.text = pmtypes.InvocationState.FAILED
            transaction_id_node.text = '0'
            operation_error_node = etree_.SubElement(invocation_info_node, msgTag('InvocationError'))
            operation_error_node.text = pmtypes.InvocationError.INVALID_VALUE
            operation_error_msg_node = etree_.SubElement(invocation_info_node,
                                                         msgTag('InvocationErrorMessage'))
            operation_error_msg_node.text = '; '.join(error_texts)
        else:
            proposed_context_state_nodes = request.body_node.xpath('*/msg:ProposedContextState',
                                                                   namespaces=nsmap)
            msg_reader = self._mdib.msg_reader
            argument = [msg_reader.mk_statecontainer_from_node(p, self._mdib) for p in proposed_context_state_nodes]
            transaction_id = self._sdc_device.enqueue_operation(operation, request, argument)
            transaction_id_node.text = str(transaction_id)
            invocation_state_node.text = pmtypes.InvocationState.WAIT

        response.add_body_element(reply_body_node)
        response.validate_body(self._bmm_schema)
        return response

    def _on_get_context_states(self, http_header, request):  # pylint:disable=unused-argument
        self._logger.debug('_on_get_context_states')
        requested_handles = request.body_node.xpath('*/msg:HandleRef/text()', namespaces=nsmap)
        if len(requested_handles) > 0:
            self._logger.info('_on_get_context_states requested Handles:{}', requested_handles)
        nsmapper = self._mdib.nsmapper
        response = pysoap.soapenvelope.Soap12Envelope(
            nsmapper.partial_map(Prefixes.S12, Prefixes.WSA, Prefixes.PM, Prefixes.MSG))
        reply_address = request.address.mk_reply_address(action=self._get_action_string('GetContextStatesResponse'))
        response.add_header_object(reply_address)
        response_node = etree_.Element(msgTag('GetContextStatesResponse'))
        with self._mdib.mdib_lock:
            response_node.set('MdibVersion', str(self._mdib.mdib_version))
            response_node.set('SequenceId', self._mdib.sequence_id)
            if len(requested_handles) == 0:
                # MessageModel: If the HANDLE reference list is empty, all states in the MDIB SHALL be included in the result list.
                context_state_containers = list(self._mdib.context_states.objects)
            else:
                context_state_containers_lookup = OrderedDict()  # lookup to avoid double entries
                for handle in requested_handles:
                    # If a HANDLE reference does match a multi state HANDLE,
                    # the corresponding multi state SHALL be included in the result list
                    tmp = self._mdib.context_states.handle.getOne(handle, allowNone=True)
                    if tmp:
                        tmp = [tmp]
                    if not tmp:
                        # If a HANDLE reference does match a descriptor HANDLE,
                        # all states that belong to the corresponding descriptor SHALL be included in the result list
                        tmp = self._mdib.context_states.descriptorHandle.get(handle)
                    if not tmp:
                        # R5042: If a HANDLE reference from the msg:GetContextStates/msg:HandleRef list does match an
                        # MDS descriptor, then all context states that are part of this MDS SHALL be included in the result list.
                        descr = self._mdib.descriptions.handle.getOne(handle, allowNone=True)
                        if descr:
                            if descr.NODETYPE == domTag('MdsDescriptor'):
                                tmp = list(self._mdib.context_states.objects)
                    if tmp:
                        for state in tmp:
                            context_state_containers_lookup[state.Handle] = state
                context_state_containers = context_state_containers_lookup.values()
            tag = msgTag('ContextState')
            if context_state_containers:
                for container in context_state_containers:
                    node = container.mk_state_node(tag)
                    response_node.append(node)
                    node.tag = msgTag('ContextState')
        response.add_body_element(response_node)
        self._logger.debug('_on_get_context_states returns {}', lambda: response.as_xml(pretty=False))
        response.validate_body(self._bmm_schema)
        return response

    def add_wsdl_port_type(self, parent_node):
        port_type = etree_.SubElement(parent_node, etree_.QName(_wsdl_ns, 'portType'),
                                      attrib={'name': self.port_type_string,
                                              dpwsTag('DiscoveryType'): 'dt:ServiceProvider',
                                              wseTag('EventSource'): 'true'})
        mk_wsdl_two_way_operation(port_type, operation_name='SetContextState')
        mk_wsdl_two_way_operation(port_type, operation_name='GetContextStates')
        _mk_wsdl_one_way_operation(port_type, operation_name='EpisodicContextReport')
        _mk_wsdl_one_way_operation(port_type, operation_name='PeriodicContextReport')


class DescriptionEventService(DPWSPortTypeImpl):
    WSDLMessageDescriptions = (
        WSDLMessageDescription('DescriptionModificationReport',
                               ('{}:DescriptionModificationReport'.format(_msg_prefix),)),
    )
    WSDLOperationBindings = (WSDLOperationBinding('DescriptionModificationReport', None, 'literal'),
                             )

    def add_wsdl_port_type(self, parent_node):
        port_type = etree_.SubElement(parent_node, etree_.QName(_wsdl_ns, 'portType'),
                                      attrib={'name': self.port_type_string,
                                              dpwsTag('DiscoveryType'): 'dt:ServiceProvider',
                                              wseTag('EventSource'): 'true'})
        _mk_wsdl_one_way_operation(port_type, operation_name='DescriptionModificationReport')
