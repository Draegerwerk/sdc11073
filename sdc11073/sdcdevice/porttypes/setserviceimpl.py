from __future__ import annotations

from typing import TYPE_CHECKING, Optional, List, Protocol, runtime_checkable

from .porttypebase import ServiceWithOperations, WSDLMessageDescription, WSDLOperationBinding
from .porttypebase import mk_wsdl_two_way_operation, _mk_wsdl_one_way_operation, msg_prefix
from ..hostedserviceimpl import DispatchKey

if TYPE_CHECKING:
    from ..sco import OperationDefinition, InvocationState
    from enum import Enum
    from ...namespaces import NamespaceHelper


@runtime_checkable
class SetServiceProtocol(Protocol):
    def notify_operation(self, operation: OperationDefinition,
                         transaction_id: int,
                         invocation_state: InvocationState,
                         mdib_version_group,
                         nsmapper: NamespaceHelper,
                         error: Optional[Enum] = None,
                         error_message: Optional[str] = None):
        ...


class SetService(ServiceWithOperations):
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
        self._logger.debug('_on_set_value')
        operation_request = self._sdc_device.msg_reader.read_set_value_request(request_data.message_data)
        ret = self._handle_operation_request(request_data.message_data, 'SetValueResponse', operation_request)
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

    def notify_operation(self,
                         operation: OperationDefinition,
                         transaction_id: int,
                         invocation_state: InvocationState,
                         mdib_version_group,
                         nsmapper: NamespaceHelper,
                         error: Optional[Enum] = None,
                         error_message: Optional[str] = None):
        operation_handle_ref = operation.handle
        subscription_mgr = self.hosting_service.subscriptions_manager
        action = self._sdc_definitions.Actions.OperationInvokedReport
        body_node = self._msg_factory.mk_operation_invoked_report_body(
            mdib_version_group, operation_handle_ref, transaction_id, invocation_state, error, error_message)
        self._logger.info(
            'notify_operation transaction={} operation_handle_ref={}, operationState={}, error={}, errorMessage={}',
            transaction_id, operation_handle_ref, invocation_state, error, error_message)
        subscription_mgr.send_to_subscribers(body_node, action, mdib_version_group, nsmapper, 'notify_operation')

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
        _mk_wsdl_one_way_operation(port_type, operation_name='OperationInvokedReport')
