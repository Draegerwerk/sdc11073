from __future__ import annotations
from concurrent.futures import Future
from typing import Optional, TYPE_CHECKING

from .serviceclientbase import HostedServiceClient, GetRequestResult
from ...dispatch import DispatchKey
from ...exceptions import ApiUsageError
from ...xml_types import msg_qnames
from ...xml_types.actions import Actions
from ...xml_types.addressing_types import HeaderInformationBlock

if TYPE_CHECKING:
    from sdc11073.sdcclient.manipulator import RequestManipulatorProtocol
class ContextServiceClient(HostedServiceClient):
    notifications = (DispatchKey(Actions.EpisodicContextReport, msg_qnames.EpisodicContextReport),
                     DispatchKey(Actions.PeriodicContextReport, msg_qnames.PeriodicContextReport))

    def mk_proposed_context_object(self, descriptor_handle: str, handle: Optional[str] = None):
        """
        Helper method that create a state that can be used in set_context_state operation
        :param descriptor_handle: the descriptor for which a state shall be created or updated
        :param handle: if None, a new object with default values is created (INSERT operation).
                       Else a copy of an existing state with this handle is returned.
        :return: a context state instance
        """
        data_model = self._sdc_definitions.data_model
        mdib = self._mdib_wref()
        if mdib is None:
            raise ApiUsageError('no mdib information')
        context_descriptor_container = mdib.descriptions.handle.get_one(descriptor_handle)
        if handle is None:
            cls = data_model.get_state_container_class(context_descriptor_container.STATE_QNAME)
            obj = cls(descriptor_container=context_descriptor_container)
            obj.Handle = descriptor_handle  # this indicates that this is a new context state
        else:
            _obj = mdib.context_states.handle.get_one(handle)
            obj = _obj.mk_copy()
        return obj

    def set_context_state(self, operation_handle: str,
                          proposed_context_states: list,
                          request_manipulator: Optional[RequestManipulatorProtocol] = None) -> Future:
        """ Calls a SetContextState operation
        :param operation_handle: the descriptor for which a state shall be created or updated
        :param proposed_context_states: list of context states
        :param request_manipulator: see documentation of RequestManipulatorProtocol
        :return: a concurrent.futures.Future object
        """
        data_model = self._sdc_definitions.data_model
        tmp = ', '.join([f'{st.__class__.__name__}(DescriptorHandle={st.DescriptorHandle}, handle={st.Handle})'
                         for st in proposed_context_states])
        self._logger.info('set_context_state {}', tmp)
        request = data_model.msg_types.SetContextState()
        request.OperationHandleRef = operation_handle
        request.ProposedContextState.extend(proposed_context_states)
        inf = HeaderInformationBlock(action=request.action, addr_to=self.endpoint_reference.Address)
        message = self._msg_factory.mk_soap_message(inf, payload=request)
        return self._call_operation(message, request_manipulator=request_manipulator)

    def get_context_states(self, handles: Optional[list[str]] = None,
                           request_manipulator: Optional[RequestManipulatorProtocol] = None) -> GetRequestResult:
        """
        :param handles: a list of handles
        :param request_manipulator: see documentation of RequestManipulatorProtocol
        :return: result of the call
        """
        data_model = self._sdc_definitions.data_model
        request = data_model.msg_types.GetContextStates()
        if handles is not None:
            request.HandleRef.extend(handles)
        inf = HeaderInformationBlock(action=request.action, addr_to=self.endpoint_reference.Address)
        message = self._msg_factory.mk_soap_message(inf, payload=request)
        received_message_data = self.post_message(message, request_manipulator=request_manipulator)
        cls = received_message_data.msg_reader.msg_types.GetContextStatesResponse
        report = cls.from_node(received_message_data.p_msg.msg_node)
        return GetRequestResult(received_message_data, report)

    def get_context_state_by_identification(self, identifications,
                                            context_type=None,
                                            request_manipulator: Optional[RequestManipulatorProtocol] = None) -> GetRequestResult:
        """
        :param identifications: list of identifiers (type: InstanceIdentifier from pmtypes)
        :param context_type: Type to query
        :param request_manipulator: see documentation of RequestManipulatorProtocol
        :return:
        """
        data_model = self._sdc_definitions.data_model
        request = data_model.msg_types.GetContextStatesByIdentification()
        if identifications is not None:
            request.Identification.extend(identifications)
        request.ContextType = context_type
        inf = HeaderInformationBlock(action=request.action, addr_to=self.endpoint_reference.Address)
        message = self._msg_factory.mk_soap_message(inf, payload=request)
        received_message_data = self.post_message(message, request_manipulator=request_manipulator)
        cls = data_model.msg_types.GetContextStatesByIdentificationResponse
        report = cls.from_node(received_message_data.p_msg.msg_node)
        return GetRequestResult(received_message_data, report)

    def get_context_state_by_filter(self, filters: list[str],
                                    request_manipulator: Optional[RequestManipulatorProtocol] = None) -> GetRequestResult:
        """
        :param filters: list strings
        :param request_manipulator: see documentation of RequestManipulatorProtocol
        :return: GetRequestResult
        """
        data_model = self._sdc_definitions.data_model
        request = data_model.msg_types.GetContextStatesByFilter()
        request.Filter.extend(filters)
        inf = HeaderInformationBlock(action=request.action, addr_to=self.endpoint_reference.Address)
        message = self._msg_factory.mk_soap_message(inf, payload=request)
        received_message_data = self.post_message(message, request_manipulator=request_manipulator)
        cls = data_model.msg_types.GetContextStatesByFilterResponse
        report = cls.from_node(received_message_data.p_msg.msg_node)
        return GetRequestResult(received_message_data, report)
