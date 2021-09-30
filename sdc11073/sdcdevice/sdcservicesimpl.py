import traceback
from collections import namedtuple, OrderedDict

from lxml import etree as etree_

from .exceptions import FunctionNotImplementedError
from .hostedserviceimpl import WSP_NS, WSDL_S12
from .. import loghelper
from .. import pmtypes
from ..namespaces import Prefixes
from ..namespaces import domTag, wseTag, dpwsTag, mdpwsTag

_msg_prefix = Prefixes.MSG.prefix

_wsdl_ns = Prefixes.WSDL.namespace
_wsdl_message = etree_.QName(_wsdl_ns, 'message')
_wsdl_part = etree_.QName(_wsdl_ns, 'part')
_wsdl_operation = etree_.QName(_wsdl_ns, 'operation')

# WSDL Generation:
# types to allow declaration of a wsdl data per service
WSDLMessageDescription = namedtuple('WSDLMessageDescription', 'name parameters ')
WSDLOperationBinding = namedtuple('WSDLOperationBinding', 'name input output')


class DPWSPortTypeImpl:
    """ Base class of all PortType implementations. Its resposibilities are:
        - handling of messages
        - creation of wsdl information.
        Handlers are registered in the hosting service instance. """
    WSDLOperationBindings = ()  # overwrite in derived classes
    WSDLMessageDescriptions = ()  # overwrite in derived classes

    def __init__(self, port_type_string, sdc_device, log_prefix=None):
        """
        :param port_type_string: port type without namespace, e.g 'Get'
        :param sdc_device:
        """
        self.port_type_string = port_type_string
        self._sdc_device = sdc_device
        self._mdib = sdc_device.mdib
        self._logger = loghelper.get_logger_adapter('sdc.device.{}'.format(self.__class__.__name__), log_prefix)
        self.hosting_service = None  # the parent

    def register_handlers(self, hosting_service):
        """Register callbacks in hosting_service"""
        self.hosting_service = hosting_service

    @property
    def actions(self):  # just a shortcut
        return self._mdib.sdc_definitions.Actions

    @property
    def _bmm_schema(self):
        return None if not self._sdc_device.shall_validate else self._sdc_device.mdib.schema_validators.message_schema

    @property
    def _mex_schema(self):
        return None if not self._sdc_device.shall_validate else self._sdc_device.mdib.schema_validators.mex_schema

    @property
    def _evt_schema(self):
        return None if not self._sdc_device.shall_validate else self._sdc_device.mdib.schema_validators.eventing_schema

    @property
    def _s12_schema(self):
        return None if not self._sdc_device.shall_validate else self._sdc_device.mdib.schema_validators.soap12_schema

    def add_wsdl_port_type(self, parent_node):
        raise NotImplementedError

    def _mk_port_type_node(self, parent_node, is_event_source=False):
        if 'dt' in parent_node.nsmap:
            port_type = etree_.SubElement(parent_node, etree_.QName(_wsdl_ns, 'portType'),
                                          attrib={'name': self.port_type_string,
                                                  dpwsTag('DiscoveryType'): 'dt:ServiceProvider'})
        else:
            port_type = etree_.SubElement(parent_node, etree_.QName(_wsdl_ns, 'portType'),
                                          attrib={'name': self.port_type_string})
        if is_event_source:
            port_type.attrib[wseTag('EventSource')] = 'true'
        return port_type

    def __repr__(self):
        return '{} Porttype={}'.format(self.__class__.__name__, self.port_type_string)

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
        etree_.SubElement(wsdl_binding, etree_.QName(WSDL_S12, 'binding'),
                          attrib={'style': 'document', 'transport': 'http://schemas.xmlsoap.org/soap/http'})
        _add_policy_dpws_profile(wsdl_binding)
        for wsdl_op in self.WSDLOperationBindings:
            wsdl_operation = etree_.SubElement(wsdl_binding, etree_.QName(_wsdl_ns, 'operation'),
                                               attrib={'name': wsdl_op.name})
            etree_.SubElement(wsdl_operation, etree_.QName(WSDL_S12, 'operation'),
                              attrib={'soapAction': '{}/{}/{}'.format(v_ref.ActionsNamespace,
                                                                      self.port_type_string,
                                                                      wsdl_op.name)})
            if wsdl_op.input is not None:
                wsdl_input = etree_.SubElement(wsdl_operation, etree_.QName(_wsdl_ns, 'input'))
                etree_.SubElement(wsdl_input, etree_.QName(WSDL_S12, 'body'), attrib={'use': wsdl_op.input})
            if wsdl_op.output is not None:
                wsdl_output = etree_.SubElement(wsdl_operation, etree_.QName(_wsdl_ns, 'output'))
                etree_.SubElement(wsdl_output, etree_.QName(WSDL_S12, 'body'), attrib={'use': wsdl_op.output})


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

    def register_handlers(self, hosting_service):
        super().register_handlers(hosting_service)
        actions = self._sdc_device.mdib.sdc_definitions.Actions
        hosting_service.register_post_handler(actions.GetMdState, self._on_get_md_state)
        hosting_service.register_post_handler(actions.GetMdib, self._on_get_mdib)
        hosting_service.register_post_handler(actions.GetMdDescription, self._on_get_md_description)
        hosting_service.register_post_handler('GetMdState', self._on_get_md_state)
        hosting_service.register_post_handler('GetMdib', self._on_get_mdib)
        hosting_service.register_post_handler('GetMdDescription', self._on_get_md_description)

    def _on_get_md_state(self, request_data):
        self._logger.debug('_on_get_md_state')
        requested_handles = self._sdc_device.msg_reader.read_getmdstate_request(request_data.message_data)
        if len(requested_handles) > 0:
            self._logger.info('_on_get_md_state requested Handles:{}', requested_handles)

        # get the requested state containers from mdib
        state_containers = []
        with self._mdib.mdib_lock:
            mdib_version = self._mdib.mdib_version
            sequence_id = self._mdib.sequence_id
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
                            state_containers.append(self._mdib.context_states.handle.get_one(handle))
                        except (KeyError, ValueError):
                            # If a HANDLE reference does match a descriptor HANDLE, all states that belong to the corresponding descriptor SHALL be included in the result list
                            state_containers.extend(self._mdib.states.descriptorHandle.get(handle, []))
                            state_containers.extend(self._mdib.context_states.descriptorHandle.get(handle, []))
                else:
                    for handle in requested_handles:
                        state_containers.extend(self._mdib.states.descriptorHandle.get(handle, []))

                self._logger.info('_on_get_md_state requested Handles:{} found {} states', requested_handles,
                                  len(state_containers))

            response_envelope = self._sdc_device.msg_factory.mk_get_mdstate_response_envelope(
                request_data.message_data, self.actions.GetMdStateResponse,
                mdib_version, sequence_id, state_containers, self._mdib.nsmapper)
        self._logger.debug('_on_get_md_state returns {}', lambda: response_envelope.as_xml(pretty=False))
        response_envelope.validate_body(self._bmm_schema)
        return response_envelope

    def _on_get_mdib(self, request_data):
        self._logger.debug('_on_get_mdib')
        response_envelope = self._sdc_device.msg_factory.mk_get_mdib_response_envelope(
            request_data.message_data, self._mdib, self._sdc_device.contextstates_in_getmdib)

        self._logger.debug('_on_get_mdib returns {}', lambda: response_envelope.as_xml(pretty=False))
        try:
            response_envelope.validate_body(self._bmm_schema)
        except Exception:
            self._logger.error('_on_get_mdib: invalid body:{}', traceback.format_exc())
            raise
        return response_envelope

    def _on_get_md_description(self, request_data):
        """
        MdDescription comprises the requested set of MDS descriptors. Which MDS descriptors are included depends on the msg:GetMdDescription/msg:HandleRef list:
        - If the HANDLE reference list is empty, all MDS descriptors SHALL be included in the result list.
        - If a HANDLE reference does match an MDS descriptor, it SHALL be included in the result list.
        - If a HANDLE reference does not match an MDS descriptor (any other descriptor), the MDS descriptor that is in the parent tree of the HANDLE reference SHOULD be included in the result list.
        """
        # currently this implementation only supports a single mds.
        # => if at least one handle matches any descriptor, the one mds is returned, otherwise empty payload

        self._logger.debug('_on_get_md_description')
        requested_handles = self._sdc_device.msg_reader.read_getmddescription_request(request_data.message_data)
        if len(requested_handles) > 0:
            self._logger.info('_on_get_md_description requested Handles:{}', requested_handles)
        response_envelope = self._sdc_device.msg_factory.mk_getmddescription_response_envelope(
            request_data.message_data, self._sdc_device.mdib, requested_handles
        )
        self._logger.debug('_on_get_md_description returns {}', lambda: response_envelope.as_xml(pretty=False))
        response_envelope.validate_body(self._bmm_schema)
        return response_envelope

    def add_wsdl_port_type(self, parent_node):
        """
        add wsdl:portType node to parent_node.
        xml looks like this:
        <wsdl:portType name="GetService" dpws:DiscoveryType="dt:ServiceProvider">
          <wsp:Policy>
            <dpws:Profile wsp:Optional="true"/>
          </wsp:Policy>
          <wsdl:operation name="GetMdState">
            <wsdl:input message="msg:GetMdState"/>
            <wsdl:output message="msg:GetMdStateResponse"/>
          </wsdl:operation>
          ...
        </wsdl:portType>
        :param parent_node:
        :return:
        """
        port_type = self._mk_port_type_node(parent_node)
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

    def register_handlers(self, hosting_service):
        super().register_handlers(hosting_service)
        actions = self._mdib.sdc_definitions.Actions
        hosting_service.register_post_handler(actions.GetContainmentTree, self._on_get_containment_tree)
        hosting_service.register_post_handler(actions.GetDescriptor, self._on_get_descriptor)
        hosting_service.register_post_handler('GetContainmentTree', self._on_get_containment_tree)
        hosting_service.register_post_handler('GetDescriptor', self._on_get_descriptor)

    def _on_get_containment_tree(self, request_data):
        # ToDo: implement, currently method only raises a soap fault
        raise FunctionNotImplementedError(request_data.message_data.raw_data)

    def _on_get_descriptor(self, request_data):
        # ToDo: implement, currently method only raises a soap fault
        raise FunctionNotImplementedError(request_data.message_data.raw_data)

    def add_wsdl_port_type(self, parent_node):
        port_type = self._mk_port_type_node(parent_node)
        mk_wsdl_two_way_operation(port_type, operation_name='GetDescriptor')
        mk_wsdl_two_way_operation(port_type, operation_name='GetContainmentTree')


class ServiceWithOperations(DPWSPortTypeImpl):
    def _handle_operation_request(self, message_data, response_name, operation_request):
        """
        It enqueues an operation and generate the expected operation invoked report.
        :param request:
        :param response_name:
        :param operation_request:
        :return:
        """
        action = getattr(self.actions, response_name)
        invocation_error = None
        error_text = None
        operation = self._sdc_device.get_operation_by_handle(operation_request.operation_handle)
        if operation is None:
            error_text = f'operation "{operation_request.operation_handle}" not known'
            self._logger.warn('_handle_operation_request: error = {}'.format(error_text))
            transaction_id = 0
            invocation_state = pmtypes.InvocationState.FAILED
            invocation_error = pmtypes.InvocationError.INVALID_VALUE
        else:
            transaction_id = self._sdc_device.enqueue_operation(operation, message_data.raw_data, operation_request)
            self._logger.info(f'_handle_operation_request: enqueued, transaction id = {transaction_id}')
            invocation_state = pmtypes.InvocationState.WAIT

        response = self._sdc_device.msg_factory.mk_operation_response_envelope(
            message_data, action, response_name, self._mdib.mdib_version, self._mdib.sequence_id,
            transaction_id, invocation_state, invocation_error, error_text, self._mdib.nsmapper)

        response.validate_body(self._bmm_schema)
        return response


class SetService(ServiceWithOperations):
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

    def register_handlers(self, hosting_service):
        super().register_handlers(hosting_service)
        actions = self._mdib.sdc_definitions.Actions
        hosting_service.register_post_handler(actions.Activate, self._on_activate)
        hosting_service.register_post_handler(actions.SetValue, self._on_set_value)
        hosting_service.register_post_handler(actions.SetString, self._on_set_string)
        hosting_service.register_post_handler(actions.SetMetricState, self._on_set_metric_state)
        hosting_service.register_post_handler(actions.SetAlertState, self._on_set_alert_state)
        hosting_service.register_post_handler(actions.SetComponentState, self._on_set_component_state)
        hosting_service.register_post_handler('Activate', self._on_activate)
        hosting_service.register_post_handler('SetValue', self._on_set_value)
        hosting_service.register_post_handler('SetString', self._on_set_string)
        hosting_service.register_post_handler('SetMetricState', self._on_set_metric_state)
        hosting_service.register_post_handler('SetAlertState', self._on_set_alert_state)
        hosting_service.register_post_handler('SetComponentState', self._on_set_component_state)

    def _on_activate(self, request_data):  # pylint:disable=unused-argument
        """Handler for Active calls.
        It enques an operation and generates the expected operation invoked report. """
        operation_request = self._sdc_device.msg_reader.read_activate_request(request_data.message_data)
        operation_descriptor = self._mdib.descriptions.handle.get_one(operation_request.operation_handle)
        # convert arguments to python types; need operation descriptor for this.
        operation_request = self._sdc_device.msg_reader.convert_activate_arguments(operation_descriptor,
                                                                                   operation_request)
        return self._handle_operation_request(request_data.message_data, 'ActivateResponse',
                                              operation_request)

    def _on_set_value(self, request_data):  # pylint:disable=unused-argument
        """Handler for SetValue calls.
        It enqueues an operation and generates the expected operation invoked report. """
        self._logger.info('_on_set_value')
        operation_request = self._sdc_device.msg_reader.read_set_value_request(request_data.message_data)
        ret = self._handle_operation_request(request_data.message_data, 'SetValueResponse', operation_request)
        self._logger.info('_on_set_value done')
        return ret

    def _on_set_string(self, request_data):  # pylint:disable=unused-argument
        """Handler for SetString calls.
        It enqueues an operation and generates the expected operation invoked report."""
        self._logger.debug('_on_set_string')
        operation_request = self._sdc_device.msg_reader.read_set_string_request(request_data.message_data)
        return self._handle_operation_request(request_data.message_data, 'SetStringResponse',
                                              operation_request)

    def _on_set_metric_state(self, request_data):  # pylint:disable=unused-argument
        """Handler for SetMetricState calls.
        It enqueues an operation and generates the expected operation invoked report."""
        self._logger.debug('_on_set_metric_state')
        operation_request = self._sdc_device.msg_reader.read_set_metric_state_request(request_data.message_data)
        return self._handle_operation_request(request_data.message_data,
                                              'SetMetricStateResponse',
                                              operation_request)

    def _on_set_alert_state(self, request_data):  # pylint:disable=unused-argument
        """Handler for SetMetricState calls.
        It enqueues an operation and generates the expected operation invoked report."""
        self._logger.debug('_on_set_alert_state')
        operation_request = self._sdc_device.msg_reader.read_set_alert_state_request(request_data.message_data)
        return self._handle_operation_request(request_data.message_data,
                                              'SetAlertStateResponse',
                                              operation_request)

    def _on_set_component_state(self, request_data):  # pylint:disable=unused-argument
        """Handler for SetComponentState calls.
        It enqueues an operation and generates the expected operation invoked report."""
        self._logger.debug('_on_set_component_state')
        operation_request = self._sdc_device.msg_reader.read_set_component_state_request(request_data.message_data)
        return self._handle_operation_request(request_data.message_data,
                                              'SetComponentStateResponse',
                                              operation_request)

    def add_wsdl_port_type(self, parent_node):
        port_type = self._mk_port_type_node(parent_node, True)
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
        port_type = self._mk_port_type_node(parent_node, True)
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
        port_type = self._mk_port_type_node(parent_node, True)
        _mk_wsdl_one_way_operation(port_type, operation_name='EpisodicAlertReport')
        _mk_wsdl_one_way_operation(port_type, operation_name='SystemErrorReport')
        _mk_wsdl_one_way_operation(port_type, operation_name='PeriodicAlertReport')
        _mk_wsdl_one_way_operation(port_type, operation_name='EpisodicComponentReport')
        _mk_wsdl_one_way_operation(port_type, operation_name='PeriodicOperationalStateReport')
        _mk_wsdl_one_way_operation(port_type, operation_name='PeriodicComponentReport')
        _mk_wsdl_one_way_operation(port_type, operation_name='EpisodicOperationalStateReport')
        _mk_wsdl_one_way_operation(port_type, operation_name='PeriodicMetricReport')
        _mk_wsdl_one_way_operation(port_type, operation_name='EpisodicMetricReport')


class ContextService(ServiceWithOperations):
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

    def register_handlers(self, hosting_service):
        super().register_handlers(hosting_service)
        actions = self._mdib.sdc_definitions.Actions
        hosting_service.register_post_handler(actions.SetContextState, self._on_set_context_state)
        hosting_service.register_post_handler(actions.GetContextStates, self._on_get_context_states)
        hosting_service.register_post_handler('SetContextState', self._on_set_context_state)
        hosting_service.register_post_handler('GetContextStates', self._on_get_context_states)

    def _on_set_context_state(self, request_data):
        operation_request = self._sdc_device.msg_reader.read_set_context_state_request(request_data.message_data)
        return self._handle_operation_request(request_data.message_data,
                                              'SetContextStateResponse',
                                              operation_request)

    def _on_get_context_states(self, request_data):
        self._logger.debug('_on_get_context_states')
        requested_handles = self._sdc_device.msg_reader.read_get_context_states_request(request_data.message_data)
        if len(requested_handles) > 0:
            self._logger.info('_on_get_context_states requested Handles:{}', requested_handles)
        with self._mdib.mdib_lock:
            mdib_version = self._mdib.mdib_version
            sequence_id = self._mdib.sequence_id
            if len(requested_handles) == 0:
                # MessageModel: If the HANDLE reference list is empty, all states in the MDIB SHALL be included in the result list.
                context_state_containers = list(self._mdib.context_states.objects)
            else:
                context_state_containers_lookup = OrderedDict()  # lookup to avoid double entries
                for handle in requested_handles:
                    # If a HANDLE reference does match a multi state HANDLE,
                    # the corresponding multi state SHALL be included in the result list
                    tmp = self._mdib.context_states.handle.get_one(handle, allow_none=True)
                    if tmp:
                        tmp = [tmp]
                    if not tmp:
                        # If a HANDLE reference does match a descriptor HANDLE,
                        # all states that belong to the corresponding descriptor SHALL be included in the result list
                        tmp = self._mdib.context_states.descriptorHandle.get(handle)
                    if not tmp:
                        # R5042: If a HANDLE reference from the msg:GetContextStates/msg:HandleRef list does match an
                        # MDS descriptor, then all context states that are part of this MDS SHALL be included in the result list.
                        descr = self._mdib.descriptions.handle.get_one(handle, allow_none=True)
                        if descr:
                            if descr.NODETYPE == domTag('MdsDescriptor'):
                                tmp = list(self._mdib.context_states.objects)
                    if tmp:
                        for state in tmp:
                            context_state_containers_lookup[state.Handle] = state
                context_state_containers = context_state_containers_lookup.values()
        return self._sdc_device.msg_factory.mk_get_context_states_response_envelope(
            request_data.message_data, self.actions.GetContextStatesResponse, mdib_version, sequence_id,
            context_state_containers, self._mdib.nsmapper)

    def add_wsdl_port_type(self, parent_node):
        port_type = self._mk_port_type_node(parent_node, True)
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
        port_type = self._mk_port_type_node(parent_node, True)
        _mk_wsdl_one_way_operation(port_type, operation_name='DescriptionModificationReport')


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
        fault_name, message_name, _ = fault  # unpack 3 parameters
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
    wsp_policy_node = etree_.SubElement(parent_node, etree_.QName(WSP_NS, 'Policy'), attrib=None)
    _ = etree_.SubElement(wsp_policy_node, dpwsTag('Profile'), attrib={etree_.QName(WSP_NS, 'Optional'): 'true'})
    _ = etree_.SubElement(wsp_policy_node, mdpwsTag('Profile'), attrib={etree_.QName(WSP_NS, 'Optional'): 'true'})
