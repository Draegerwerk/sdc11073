from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING

from sdc11073.dispatch import DispatchKey
from sdc11073.namespaces import PrefixesEnum
from sdc11073.xml_types import msg_qnames
from sdc11073.xml_types.actions import Actions
from sdc11073.xml_types.addressing_types import HeaderInformationBlock

from .serviceclientbase import HostedServiceClient

if TYPE_CHECKING:
    from concurrent.futures import Future

    from sdc11073.consumer.manipulator import RequestManipulatorProtocol
    from sdc11073.mdib.statecontainers import AbstractStateProtocol
    from sdc11073.xml_types.msg_types import Argument


class SetServiceClient(HostedServiceClient):
    """Client for SetService."""

    port_type_name = PrefixesEnum.SDC.tag('SetService')
    notifications = (DispatchKey(Actions.OperationInvokedReport, msg_qnames.OperationInvokedReport),)

    def set_numeric_value(self, operation_handle: str,
                          requested_numeric_value: Decimal | float | int | str,
                          request_manipulator: RequestManipulatorProtocol | None = None) -> Future:
        """Send a GetSupportedLanguages request.

        :param operation_handle: a string
        :param requested_numeric_value: decimal, int, float or a string representing a decimal number
        :param request_manipulator:
        :return: a Future object
        """
        data_model = self._sdc_definitions.data_model
        self._logger.info('set_numeric_value operation_handle={} requested_numeric_value={}',  # noqa: PLE1205
                          operation_handle, requested_numeric_value)
        request = data_model.msg_types.SetValue()
        request.OperationHandleRef = operation_handle
        if isinstance(requested_numeric_value, float):
            # convert to string first in order to limit possible excessive number of digits
            request.RequestedNumericValue = Decimal(str(requested_numeric_value))
        else:
            request.RequestedNumericValue = Decimal(requested_numeric_value)
        inf = HeaderInformationBlock(action=request.action, addr_to=self.endpoint_reference.Address)
        message = self._msg_factory.mk_soap_message(inf, payload=request)
        return self._call_operation(message, request_manipulator=request_manipulator)

    def set_string(self, operation_handle: str,
                   requested_string: str,
                   request_manipulator: RequestManipulatorProtocol | None = None) -> Future:
        """Send a SetString request.

        :param operation_handle: a string
        :param requested_string: a string
        :param request_manipulator:
        :return: a Future object
        """
        data_model = self._sdc_definitions.data_model
        self._logger.info('set_string operation_handle={} requested_string={}',  # noqa: PLE1205
                          operation_handle, requested_string)
        request = data_model.msg_types.SetString()
        request.OperationHandleRef = operation_handle
        request.RequestedStringValue = requested_string
        inf = HeaderInformationBlock(action=request.action, addr_to=self.endpoint_reference.Address)
        message = self._msg_factory.mk_soap_message(inf, payload=request)
        return self._call_operation(message, request_manipulator=request_manipulator)

    def set_alert_state(self, operation_handle: str,
                        proposed_alert_state: AbstractStateProtocol,
                        request_manipulator: RequestManipulatorProtocol | None = None) -> Future:
        """Send a GetSupportedLanguages request.

        :param operation_handle: the handle of the operation to be called.
        :param proposed_alert_state: domainmodel.AbstractAlertState instance or a list of them
        :param request_manipulator:
        :return: a Future object
        """
        data_model = self._sdc_definitions.data_model
        self._logger.info('set_alert_state operation_handle={} requestedAlertState={}',  # noqa: PLE1205
                          operation_handle, proposed_alert_state)
        request = data_model.msg_types.SetAlertState()
        request.OperationHandleRef = operation_handle
        request.ProposedAlertState = proposed_alert_state
        inf = HeaderInformationBlock(action=request.action, addr_to=self.endpoint_reference.Address)
        message = self._msg_factory.mk_soap_message(inf, payload=request)
        return self._call_operation(message, request_manipulator=request_manipulator)

    def set_metric_state(self, operation_handle: str,
                         proposed_metric_states: list[AbstractStateProtocol],
                         request_manipulator: RequestManipulatorProtocol | None = None) -> Future:
        """Send a GetSupportedLanguages request.

        :param operation_handle: the handle of the operation to be called.
        :param proposed_metric_states: a list of domainmodel.AbstractMetricState instance or derived class
        :param request_manipulator:
        :return: a Future object
        """
        data_model = self._sdc_definitions.data_model
        self._logger.info('set_metric_state operation_handle={} requestedMetricState={}',  # noqa: PLE1205
                          operation_handle, proposed_metric_states)
        request = data_model.msg_types.SetMetricState()
        request.OperationHandleRef = operation_handle
        request.ProposedMetricState.extend(proposed_metric_states)
        inf = HeaderInformationBlock(action=request.action, addr_to=self.endpoint_reference.Address)
        message = self._msg_factory.mk_soap_message(inf, payload=request)
        return self._call_operation(message, request_manipulator=request_manipulator)

    def activate(self, operation_handle: str,
                 arguments: list[Argument] | None = None,
                 request_manipulator: RequestManipulatorProtocol | None = None) -> Future:
        """Send an Activate request.

        :param operation_handle: the handle of the operation to be called.
        :param arguments: a list of strings or None
        :param request_manipulator:
        :return: a concurrent.futures.Future object
        """
        data_model = self._sdc_definitions.data_model
        self._logger.info('activate handle={} arguments={}', operation_handle, arguments)  # noqa: PLE1205
        request = data_model.msg_types.Activate()
        request.OperationHandleRef = operation_handle
        if arguments is not None:
            for arg_value in arguments:
                request.add_argument(arg_value)
        inf = HeaderInformationBlock(action=request.action, addr_to=self.endpoint_reference.Address)
        message = self._msg_factory.mk_soap_message(inf, payload=request)
        return self._call_operation(message, request_manipulator=request_manipulator)

    def set_component_state(self, operation_handle: str,
                            proposed_component_states: list[AbstractStateProtocol],
                            request_manipulator: RequestManipulatorProtocol | None = None) -> Future:
        """Send an SetComponentState request.

        The set_component_state method corresponds to the SetComponentStateOperation objects in the MDIB
        and allows to insert or modify context states.
        """
        data_model = self._sdc_definitions.data_model
        tmp = ', '.join([f'{st.__class__.__name__} (DescriptorHandle={st.DescriptorHandle})'
                         for st in proposed_component_states])
        self._logger.info('set_component_state {}', tmp)  # noqa: PLE1205
        request = data_model.msg_types.SetComponentState()
        request.OperationHandleRef = operation_handle
        request.ProposedComponentState.extend(proposed_component_states)
        inf = HeaderInformationBlock(action=request.action, addr_to=self.endpoint_reference.Address)
        message = self._msg_factory.mk_soap_message(inf, payload=request)
        self._logger.debug('set_component_state sends {}', lambda: message.serialize(pretty=True))  # noqa: PLE1205
        return self._call_operation(message, request_manipulator=request_manipulator)
