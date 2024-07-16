from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

from sdc11073.dispatch import DispatchKey
from sdc11073.namespaces import PrefixesEnum

from .porttypebase import (
    ServiceWithOperations,
    WSDLMessageDescription,
    WSDLOperationBinding,
    mk_wsdl_one_way_operation,
    mk_wsdl_two_way_operation,
    msg_prefix,
)

if TYPE_CHECKING:
    from enum import Enum

    from sdc11073.provider.sco import OperationDefinition
    from sdc11073.mdib.mdibbase import MdibVersionGroup


@runtime_checkable
class SetServiceProtocol(Protocol):
    def notify_operation(self,
                         operation: OperationDefinition,
                         transaction_id: int,
                         invocation_state: Enum,
                         mdib_version_group: MdibVersionGroup,
                         operation_target: str | None = None,
                         error: Enum | None = None,
                         error_message: str | None = None):
        ...


class SetService(ServiceWithOperations):
    port_type_name = PrefixesEnum.SDC.tag('SetService')
    WSDLMessageDescriptions = (WSDLMessageDescription('Activate',
                                                      (f'{msg_prefix}:Activate',)),
                               WSDLMessageDescription('ActivateResponse',
                                                      (f'{msg_prefix}:ActivateResponse',)),
                               WSDLMessageDescription('SetString',
                                                      (f'{msg_prefix}:SetString',)),
                               WSDLMessageDescription('SetStringResponse',
                                                      (f'{msg_prefix}:SetStringResponse',)),
                               WSDLMessageDescription('SetComponentState',
                                                      (f'{msg_prefix}:SetComponentState',)),
                               WSDLMessageDescription('SetComponentStateResponse',
                                                      (f'{msg_prefix}:SetComponentStateResponse',)),
                               WSDLMessageDescription('SetAlertState',
                                                      (f'{msg_prefix}:SetAlertState',)),
                               WSDLMessageDescription('SetAlertStateResponse',
                                                      (f'{msg_prefix}:SetAlertStateResponse',)),
                               WSDLMessageDescription('SetMetricState',
                                                      (f'{msg_prefix}:SetMetricState',)),
                               WSDLMessageDescription('SetMetricStateResponse',
                                                      (f'{msg_prefix}:SetMetricStateResponse',)),
                               WSDLMessageDescription('SetValue',
                                                      (f'{msg_prefix}:SetValue',)),
                               WSDLMessageDescription('SetValueResponse',
                                                      (f'{msg_prefix}:SetValueResponse',)),
                               WSDLMessageDescription('OperationInvokedReport',
                                                      (f'{msg_prefix}:OperationInvokedReport',)),
                               )
    WSDLOperationBindings = (WSDLOperationBinding('Activate', 'literal', 'literal'),  # fault?
                             WSDLOperationBinding('SetString', 'literal', 'literal'),  # fault?
                             WSDLOperationBinding('SetComponentState', 'literal', 'literal'),  # fault?
                             WSDLOperationBinding('SetAlertState', 'literal', 'literal'),  # fault?
                             WSDLOperationBinding('SetMetricState', 'literal', 'literal'),  # fault?
                             WSDLOperationBinding('SetValue', 'literal', 'literal'),  # fault?
                             WSDLOperationBinding('OperationInvokedReport', None, 'literal'),
                             )

    def register_hosting_service(self, hosting_service):
        super().register_hosting_service(hosting_service)
        actions = self._mdib.sdc_definitions.Actions
        msg_names = self._mdib.sdc_definitions.data_model.msg_names
        hosting_service.register_post_handler(DispatchKey(actions.Activate, msg_names.Activate),
                                              self._on_activate)
        hosting_service.register_post_handler(DispatchKey(actions.SetValue, msg_names.SetValue),
                                              self._on_set_value)
        hosting_service.register_post_handler(DispatchKey(actions.SetString, msg_names.SetString),
                                              self._on_set_string)
        hosting_service.register_post_handler(DispatchKey(actions.SetMetricState, msg_names.SetMetricState),
                                              self._on_set_metric_state)
        hosting_service.register_post_handler(DispatchKey(actions.SetAlertState, msg_names.SetAlertState),
                                              self._on_set_alert_state)
        hosting_service.register_post_handler(DispatchKey(actions.SetComponentState, msg_names.SetComponentState),
                                              self._on_set_component_state)

    def _on_activate(self, request_data):
        """Handler for Active calls.
        It enqueues an operation and generates the expected operation invoked report.
        """
        data_model = self._sdc_definitions.data_model
        msg_node = request_data.message_data.p_msg.msg_node
        activate = data_model.msg_types.Activate.from_node(msg_node)
        # ToDo: convert arguments to specific python types
        response = data_model.msg_types.ActivateResponse()
        return self._handle_operation_request(request_data, activate, response)

    def _on_set_value(self, request_data):
        """Handler for SetValue calls.
        It enqueues an operation and generates the expected operation invoked report.
        """
        data_model = self._sdc_definitions.data_model
        self._logger.debug('_on_set_value')
        msg_node = request_data.message_data.p_msg.msg_node
        set_value = data_model.msg_types.SetValue.from_node(msg_node)
        response = data_model.msg_types.SetValueResponse()
        return self._handle_operation_request(request_data, set_value, response)

    def _on_set_string(self, request_data):
        """Handler for SetString calls.
        It enqueues an operation and generates the expected operation invoked report.
        """
        data_model = self._sdc_definitions.data_model
        self._logger.debug('_on_set_string')
        msg_node = request_data.message_data.p_msg.msg_node
        set_string = data_model.msg_types.SetString.from_node(msg_node)
        response = data_model.msg_types.SetStringResponse()
        return self._handle_operation_request(request_data, set_string, response)

    def _on_set_metric_state(self, request_data):
        """Handler for SetMetricState calls.
        It enqueues an operation and generates the expected operation invoked report.
        """
        data_model = self._sdc_definitions.data_model
        self._logger.debug('_on_set_metric_state')
        msg_node = request_data.message_data.p_msg.msg_node
        set_metric_state = data_model.msg_types.SetMetricState.from_node(msg_node)
        response = data_model.msg_types.SetMetricStateResponse()
        return self._handle_operation_request(request_data, set_metric_state, response)

    def _on_set_alert_state(self, request_data):
        """Handler for SetMetricState calls.
        It enqueues an operation and generates the expected operation invoked report.
        """
        data_model = self._sdc_definitions.data_model
        self._logger.debug('_on_set_alert_state')
        msg_node = request_data.message_data.p_msg.msg_node
        set_alert_state = data_model.msg_types.SetAlertState.from_node(msg_node)
        response = data_model.msg_types.SetAlertStateResponse()
        return self._handle_operation_request(request_data, set_alert_state, response)

    def _on_set_component_state(self, request_data):
        """Handler for SetComponentState calls.
        It enqueues an operation and generates the expected operation invoked report.
        """
        data_model = self._sdc_definitions.data_model
        self._logger.debug('_on_set_component_state')
        msg_node = request_data.message_data.p_msg.msg_node
        set_component_state = data_model.msg_types.SetComponentState.from_node(msg_node)
        response = data_model.msg_types.SetComponentStateResponse()
        return self._handle_operation_request(request_data, set_component_state, response)

    def notify_operation(self,
                         operation: OperationDefinition,
                         transaction_id: int,
                         invocation_state: Enum,
                         mdib_version_group: MdibVersionGroup,
                         operation_target: str | None = None,
                         error: Enum | None = None,
                         error_message: str | None = None):
        data_model = self._sdc_definitions.data_model
        nsh = data_model.ns_helper
        operation_handle_ref = operation.handle
        subscription_mgr = self.hosting_service.subscriptions_manager
        report = data_model.msg_types.OperationInvokedReport()
        report.set_mdib_version_group(mdib_version_group)
        report_part = report.add_report_part()
        report_part.InvocationInfo.TransactionId = transaction_id
        report_part.InvocationInfo.InvocationState = invocation_state
        if error is not None:
            report_part.InvocationInfo.InvocationError = error
        if error_message is not None:
            report_part.InvocationInfo.InvocationErrorMessage.append(data_model.pm_types.LocalizedText(error_message))
        # implemented is only SDC R0077 for value of invocationSource:
        # Extension = "AnonymousSdcParticipant".
        # a known participant (R0078) is currently not supported
        # ToDo: implement R0078
        report_part.InvocationSource = data_model.pm_types.InstanceIdentifier(
            nsh.SDC.namespace, extension_string='AnonymousSdcParticipant')
        report_part.OperationHandleRef = operation_handle_ref
        report_part.OperationTarget = operation_target
        ns_map = nsh.partial_map(nsh.PM, nsh.MSG, nsh.XSI, nsh.EXT, nsh.XML)
        body_node = report.as_etree_node(report.NODETYPE, ns_map)
        self._logger.info(
            'notify_operation transaction={} operation_handle_ref={}, operationState={}, error={}, errorMessage={}',
            transaction_id, operation_handle_ref, invocation_state, error, error_message)
        subscription_mgr.send_to_subscribers(body_node, report.action.value, mdib_version_group)

    def handled_actions(self) -> List[str]:
        return [self._sdc_device.sdc_definitions.Actions.OperationInvokedReport]

    def add_wsdl_port_type(self, parent_node):
        port_type = self._mk_port_type_node(parent_node, True)
        mk_wsdl_two_way_operation(port_type, operation_name='Activate')
        mk_wsdl_two_way_operation(port_type, operation_name='SetString')
        mk_wsdl_two_way_operation(port_type, operation_name='SetComponentState')
        mk_wsdl_two_way_operation(port_type, operation_name='SetAlertState')
        mk_wsdl_two_way_operation(port_type, operation_name='SetMetricState')
        mk_wsdl_two_way_operation(port_type, operation_name='SetValue')
        mk_wsdl_one_way_operation(port_type, operation_name='OperationInvokedReport')
