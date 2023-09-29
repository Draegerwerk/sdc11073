from __future__ import annotations

from collections import namedtuple
from typing import TYPE_CHECKING, ClassVar

from lxml import etree as etree_

from sdc11073 import loghelper
from sdc11073.namespaces import PrefixesEnum

if TYPE_CHECKING:
    from sdc11073 import xml_utils
    from sdc11073.dispatch.request import RequestData
    from sdc11073.namespaces import PrefixNamespace
    from sdc11073.pysoap.msgfactory import CreatedMessage
    from sdc11073.xml_types.msg_types import AbstractSet, AbstractSetResponse

msg_prefix = PrefixesEnum.MSG.prefix

WSP_NS = PrefixesEnum.WSP.namespace
WSDL_S12 = PrefixesEnum.WSDL12.namespace  # old soap 12 namespace, used in wsdl 1.1. used only for wsdl

_wsdl_ns = PrefixesEnum.WSDL.namespace
_wsdl_message = etree_.QName(_wsdl_ns, 'message')
_wsdl_part = etree_.QName(_wsdl_ns, 'part')
_wsdl_operation = etree_.QName(_wsdl_ns, 'operation')

# WSDL Generation:
# types to allow declaration of a wsdl data per service
WSDLMessageDescription = namedtuple('WSDLMessageDescription', 'name parameters ')
WSDLOperationBinding = namedtuple('WSDLOperationBinding', 'name input output')


class DPWSPortTypeBase:
    """Base class of all PortType implementations.

    Its responsibilities are:
    - handling of messages
    - creation of wsdl information.
    Handlers are registered in the hosting service instance.
    """

    port_type_name: etree_.QName | None = None
    WSDLOperationBindings = ()  # overwrite in derived classes
    WSDLMessageDescriptions = ()  # overwrite in derived classes
    additional_namespaces: ClassVar[list[PrefixNamespace]] = []  # for special namespaces

    def __init__(self, sdc_device, log_prefix: str | None = None):
        """:param sdc_device: the sdc device
        :param log_prefix: optional string
        """
        self._sdc_device = sdc_device
        self._mdib = sdc_device.mdib
        self._sdc_definitions = self._mdib.sdc_definitions
        self._msg_factory = self._sdc_device.msg_factory
        self._data_model = sdc_device.mdib.sdc_definitions.data_model
        self._logger = loghelper.get_logger_adapter(f'sdc.device.{self.__class__.__name__}', log_prefix)
        self.hosting_service = None  # the parent
        # calculate offered subscriptions from WSDLOperationBindings
        self.offered_subscriptions = self._mk_offered_subscriptions()

    def register_hosting_service(self, dpws_hosted_service):
        """Register callbacks in hosting_service."""
        self.hosting_service = dpws_hosted_service

    @property
    def actions(self):  # just a shortcut
        return self._mdib.sdc_definitions.Actions

    def add_wsdl_port_type(self, parent_node):
        raise NotImplementedError

    def _mk_port_type_node(self, parent_node: xml_utils.LxmlElement,
                           is_event_source: bool = False) -> xml_utils.LxmlElement:
        """Needed for wsdl message
        :param parent_node: where to add data
        :param is_event_source: true if port type provides notification
        :return: the new created node (is already child of parent_node).
        """
        if self.port_type_name is None:
            raise ValueError('self.port_type_name is not set, cannot create port type node')

        if 'dt' in parent_node.nsmap:
            port_type = etree_.SubElement(parent_node, etree_.QName(_wsdl_ns, 'portType'),
                                          attrib={'name': self.port_type_name.localname,
                                                  PrefixesEnum.DPWS.tag('DiscoveryType'): 'dt:ServiceProvider'})
        else:
            port_type = etree_.SubElement(parent_node, etree_.QName(_wsdl_ns, 'portType'),
                                          attrib={'name': self.port_type_name.localname})
        if is_event_source:
            port_type.attrib[PrefixesEnum.WSE.tag('EventSource')] = 'true'
        return port_type

    def __repr__(self):
        return f'{self.__class__.__name__} Porttype={self.port_type_name!s}'

    def add_wsdl_messages(self, parent_node):
        """Add wsdl:message node to parent_node.
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
        """Add wsdl:binding node to parent_node.
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
        p_type = self.port_type_name.localname
        wsdl_binding = etree_.SubElement(parent_node, etree_.QName(_wsdl_ns, 'binding'),
                                         attrib={'name': p_type + 'Binding',
                                                 'type': f'{porttype_prefix}:{p_type}'})
        etree_.SubElement(wsdl_binding, etree_.QName(WSDL_S12, 'binding'),
                          attrib={'style': 'document', 'transport': 'http://schemas.xmlsoap.org/soap/http'})
        _add_policy_dpws_profile(wsdl_binding)
        for wsdl_op in self.WSDLOperationBindings:
            wsdl_operation = etree_.SubElement(wsdl_binding, etree_.QName(_wsdl_ns, 'operation'),
                                               attrib={'name': wsdl_op.name})
            etree_.SubElement(wsdl_operation, etree_.QName(WSDL_S12, 'operation'),
                              attrib={'soapAction': f'{v_ref.ActionsNamespace}/{p_type}/{wsdl_op.name}'})
            if wsdl_op.input is not None:
                wsdl_input = etree_.SubElement(wsdl_operation, etree_.QName(_wsdl_ns, 'input'))
                etree_.SubElement(wsdl_input, etree_.QName(WSDL_S12, 'body'), attrib={'use': wsdl_op.input})
            if wsdl_op.output is not None:
                wsdl_output = etree_.SubElement(wsdl_operation, etree_.QName(_wsdl_ns, 'output'))
                etree_.SubElement(wsdl_output, etree_.QName(WSDL_S12, 'body'), attrib={'use': wsdl_op.output})

    def _mk_offered_subscriptions(self) -> list:
        """Takes action strings from sdc_definitions.Actions.
        The name of the WSDLOperationBinding is used to reference the action string.
        """
        actions = self._sdc_device.mdib.sdc_definitions.Actions
        offered_subscriptions = []
        for bdg in self.WSDLOperationBindings:
            if bdg.input is None:
                action_string = getattr(actions, bdg.name)
                offered_subscriptions.append(action_string)
        return offered_subscriptions


class ServiceWithOperations(DPWSPortTypeBase):

    def _handle_operation_request(self, request_data: RequestData,
                                  request: AbstractSet,
                                  set_response: AbstractSetResponse) -> CreatedMessage:
        """Handle thew operation request by forwarding it to provider."""
        data_model = self._sdc_definitions.data_model
        operation = self._sdc_device.get_operation_by_handle(request.OperationHandleRef)
        transaction_id = self._sdc_device.generate_transaction_id()
        set_response.InvocationInfo.TransactionId = transaction_id
        if operation is None:
            error_text = f'no handler registered for "{request.OperationHandleRef}"'
            self._logger.warning('handle operation request: {}', error_text)
            set_response.InvocationInfo.InvocationState = data_model.msg_types.InvocationState.FAILED
            set_response.InvocationInfo.InvocationError = data_model.msg_types.InvocationError.INVALID_VALUE
            set_response.InvocationInfo.add_error_message(error_text)
        else:
            invocation_state = self._sdc_device.handle_operation_request(operation,
                                                                         request_data.message_data.p_msg,
                                                                         request,
                                                                         transaction_id)
            self._logger.info('operation request "{}" handled, transaction id = {}, invocation-state={}',
                              request.OperationHandleRef, set_response.InvocationInfo.TransactionId, invocation_state)
            set_response.InvocationInfo.InvocationState = invocation_state

        set_response.MdibVersion = self._mdib.mdib_version
        set_response.SequenceId = self._mdib.sequence_id
        set_response.InstanceId = self._mdib.instance_id
        return self._sdc_device.msg_factory.mk_reply_soap_message(request_data, set_response)


def _mk_wsdl_operation(parent_node, operation_name, input_message_name, output_message_name) -> xml_utils.LxmlElement:
    elem = etree_.SubElement(parent_node, _wsdl_operation, attrib={'name': operation_name})
    if input_message_name is not None:
        etree_.SubElement(elem, etree_.QName(_wsdl_ns, 'input'),
                          attrib={'message': f'tns:{input_message_name}',
                                  })
    if output_message_name is not None:
        etree_.SubElement(elem, etree_.QName(_wsdl_ns, 'output'),
                          attrib={'message': f'tns:{output_message_name}',
                                  })
    return elem


def mk_wsdl_two_way_operation(parent_node: xml_utils.LxmlElement,
                              operation_name: str,
                              input_message_name: str | None = None,
                              output_message_name: str | None = None) -> xml_utils.LxmlElement:
    """A helper for wsdl generation. A two-way-operation defines a 'normal' request and response operation.
    :param parent_node: info shall be added to this node
    :param operation_name: a string
    :param input_message_name: only needed if message name is not equal to operation_name
    :param output_message_name: only needed if message name is not equal to operation_name + "Response"
    :return:
    """
    input_msg_name = input_message_name or operation_name  # defaults to operation name
    output_msg_name = output_message_name or operation_name + 'Response'  # defaults to operation name + "Response"
    return _mk_wsdl_operation(parent_node,
                              operation_name=operation_name,
                              input_message_name=input_msg_name,
                              output_message_name=output_msg_name)


def mk_wsdl_one_way_operation(parent_node: xml_utils.LxmlElement,
                              operation_name: str,
                              output_message_name: str | None = None) -> xml_utils.LxmlElement:
    """A helper for wsdl generation. A one-way-operation is a subscription.
    :param parent_node: info shall be added to this node
    :param operation_name: a string
    :param output_message_name: only needed if message name is not equal to operation_name
    :return:
    """
    output_msg_name = output_message_name or operation_name  # defaults to operation name
    return _mk_wsdl_operation(parent_node,
                              operation_name=operation_name,
                              input_message_name=None,
                              output_message_name=output_msg_name)


def _add_policy_dpws_profile(parent_node):
    """:param parent_node:
    :return: <wsp:Policy>
            <dpws:Profile wsp:Optional="true"/>
            <mdpws:Profile wsp:Optional="true"/>
          </wsp:Policy>
    """
    wsp_policy_node = etree_.SubElement(parent_node, etree_.QName(WSP_NS, 'Policy'), attrib=None)
    _ = etree_.SubElement(wsp_policy_node, PrefixesEnum.DPWS.tag('Profile'),
                          attrib={etree_.QName(WSP_NS, 'Optional'): 'true'})
    _ = etree_.SubElement(wsp_policy_node, PrefixesEnum.MDPWS.tag('Profile'),
                          attrib={etree_.QName(WSP_NS, 'Optional'): 'true'})
