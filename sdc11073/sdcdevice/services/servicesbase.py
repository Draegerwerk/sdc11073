from collections import namedtuple

from lxml import etree as etree_

from ..hostedserviceimpl import WSP_NS, WSDL_S12
from ... import loghelper
from ... import pmtypes
from ...namespaces import Prefixes
from ...namespaces import wseTag, dpwsTag, mdpwsTag

msg_prefix = Prefixes.MSG.prefix

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
        self._logger = loghelper.get_logger_adapter(f'sdc.device.{self.__class__.__name__}', log_prefix)
        self.hosting_service = None  # the parent

    def register_handlers(self, hosting_service):
        """Register callbacks in hosting_service"""
        self.hosting_service = hosting_service

    @property
    def actions(self):  # just a shortcut
        return self._mdib.sdc_definitions.Actions

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
        return f'{self.__class__.__name__} Porttype={self.port_type_string}'

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
                                                 'type': f'{porttype_prefix}:{self.port_type_string}'})
        etree_.SubElement(wsdl_binding, etree_.QName(WSDL_S12, 'binding'),
                          attrib={'style': 'document', 'transport': 'http://schemas.xmlsoap.org/soap/http'})
        _add_policy_dpws_profile(wsdl_binding)
        for wsdl_op in self.WSDLOperationBindings:
            wsdl_operation = etree_.SubElement(wsdl_binding, etree_.QName(_wsdl_ns, 'operation'),
                                               attrib={'name': wsdl_op.name})
            etree_.SubElement(wsdl_operation, etree_.QName(WSDL_S12, 'operation'),
                              attrib={'soapAction': f'{v_ref.ActionsNamespace}/{self.port_type_string}/{wsdl_op.name}'})
            if wsdl_op.input is not None:
                wsdl_input = etree_.SubElement(wsdl_operation, etree_.QName(_wsdl_ns, 'input'))
                etree_.SubElement(wsdl_input, etree_.QName(WSDL_S12, 'body'), attrib={'use': wsdl_op.input})
            if wsdl_op.output is not None:
                wsdl_output = etree_.SubElement(wsdl_operation, etree_.QName(_wsdl_ns, 'output'))
                etree_.SubElement(wsdl_output, etree_.QName(WSDL_S12, 'body'), attrib={'use': wsdl_op.output})


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
            self._logger.warn('_handle_operation_request: error = {}', error_text)
            transaction_id = 0
            invocation_state = pmtypes.InvocationState.FAILED
            invocation_error = pmtypes.InvocationError.INVALID_VALUE
        else:
            transaction_id = self._sdc_device.enqueue_operation(operation, message_data.p_msg, operation_request)
            self._logger.info(f'_handle_operation_request: enqueued, transaction id = {transaction_id}')
            invocation_state = pmtypes.InvocationState.WAIT

        response = self._sdc_device.msg_factory.mk_operation_response_message(
            message_data, action, response_name, self._mdib.mdib_version, self._mdib.sequence_id,
            transaction_id, invocation_state, invocation_error, error_text, self._mdib.nsmapper)
        return response


def _mk_wsdl_operation(parent_node, operation_name, input_message_name, output_message_name, fault):
    elem = etree_.SubElement(parent_node, _wsdl_operation, attrib={'name': operation_name})
    if input_message_name is not None:
        etree_.SubElement(elem, etree_.QName(_wsdl_ns, 'input'),
                          attrib={'message': f'tns:{input_message_name}',
                                  })
    if output_message_name is not None:
        etree_.SubElement(elem, etree_.QName(_wsdl_ns, 'output'),
                          attrib={'message': f'tns:{output_message_name}',
                                  })
    if fault is not None:
        fault_name, message_name, _ = fault  # unpack 3 parameters
        etree_.SubElement(elem, etree_.QName(_wsdl_ns, 'fault'),
                          attrib={'name': fault_name,
                                  'message': f'tns:{message_name}',
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
