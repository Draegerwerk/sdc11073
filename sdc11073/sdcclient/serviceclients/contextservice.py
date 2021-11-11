from .serviceclientbase import HostedServiceClient, GetRequestResult
from concurrent.futures import Future

class ContextServiceClient(HostedServiceClient):
    subscribeable_actions = ('EpisodicContextReport', 'PeriodicContextReport')

    def mk_proposed_context_object(self, descriptor_handle, handle=None):
        """
        Helper method that create a state that can be used in set_context_state operation
        :param descriptor_handle: the descriptor for which a state shall be created or updated
        :param handle: if None, a new object with default values is created (INSERT operation).
                       Otherwise a copy of an existing state with this handle is returned.
        :return: a context state instance
        """
        mdib = self._mdib_wref()
        if mdib is None:
            raise RuntimeError('no mdib information')
        context_descriptor_container = mdib.descriptions.handle.get_one(descriptor_handle)
        if handle is None:
            cls = self._sdc_definitions.get_state_container_class(context_descriptor_container.STATE_QNAME)
            obj = cls(descriptor_container=context_descriptor_container)
            obj.Handle = descriptor_handle  # this indicates that this is a new context state
        else:
            _obj = mdib.context_states.handle.get_one(handle)
            obj = _obj.mk_copy()
        return obj

    def set_context_state(self, operation_handle, proposed_context_states, request_manipulator=None) -> Future:
        """
        @return: a concurrent.futures.Future object
        """
        tmp = ', '.join([f'{st.__class__.__name__}(descriptorHandle={st.descriptorHandle}, handle={st.Handle})'
                         for st in proposed_context_states])
        self._logger.info('set_context_state {}', tmp)
        message = self._msg_factory.mk_setcontextstate_message(self._nsmapper,
                                                               self.endpoint_reference.address, self.porttype,
                                                               operation_handle, proposed_context_states)
        return self._call_operation(message, request_manipulator=request_manipulator)

    def get_context_states(self, handles=None, request_manipulator=None) -> GetRequestResult:
        """
        :param handles: a list of handles
        """
        message = self._msg_factory.mk_getcontextstates_message(
            self.endpoint_reference.address, self.porttype, handles)
        received_message_data = self._call_get_method(
            message, 'GetContextStates', request_manipulator=request_manipulator)
        received_message_data.p_msg.validate_body(self._bmm_schema)
        context_state_containers = received_message_data.msg_reader.read_context_states(received_message_data)
        return GetRequestResult(received_message_data, context_state_containers)

    def get_context_state_by_identification(self, identifications, context_type=None,
                                            request_manipulator=None) -> GetRequestResult:
        """
        :param identifications: list of identifiers (type: InstanceIdentifier from pmtypes)
        :param context_type: Type to query
        :return:
        """
        message = self._msg_factory.mk_getcontextstates_by_identification_message(
            self.endpoint_reference.address, self.porttype, identifications)
        received_message_data = self._call_get_method(
            message, 'GetContextStatesByIdentification', request_manipulator=request_manipulator)
        received_message_data.p_msg.validate_body(self._bmm_schema)
        context_state_containers = received_message_data.msg_reader.read_context_states(received_message_data)
        return GetRequestResult(received_message_data, context_state_containers)
