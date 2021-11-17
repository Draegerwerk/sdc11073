from collections import OrderedDict
from ...namespaces import domTag
from .servicesbase import ServiceWithOperations, WSDLMessageDescription, WSDLOperationBinding, msg_prefix
from .servicesbase import mk_wsdl_two_way_operation, _mk_wsdl_one_way_operation


class ContextService(ServiceWithOperations):
    WSDLMessageDescriptions = (WSDLMessageDescription('SetContextState',
                                                      (f'{msg_prefix}:SetContextState',)),
                               WSDLMessageDescription('SetContextStateResponse',
                                                      (f'{msg_prefix}:SetContextStateResponse',)),
                               WSDLMessageDescription('GetContextStates',
                                                      (f'{msg_prefix}:GetContextStates',)),
                               WSDLMessageDescription('GetContextStatesResponse',
                                                      (f'{msg_prefix}:GetContextStatesResponse',)),
                               WSDLMessageDescription('EpisodicContextReport',
                                                      (f'{msg_prefix}:EpisodicContextReport',)),
                               WSDLMessageDescription('PeriodicContextReport',
                                                      (f'{msg_prefix}:PeriodicContextReport',)),
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
        return self._sdc_device.msg_factory.mk_get_context_states_response_message(
            request_data.message_data, self.actions.GetContextStatesResponse, mdib_version, sequence_id,
            context_state_containers, self._mdib.nsmapper)

    def add_wsdl_port_type(self, parent_node):
        port_type = self._mk_port_type_node(parent_node, True)
        mk_wsdl_two_way_operation(port_type, operation_name='SetContextState')
        mk_wsdl_two_way_operation(port_type, operation_name='GetContextStates')
        _mk_wsdl_one_way_operation(port_type, operation_name='EpisodicContextReport')
        _mk_wsdl_one_way_operation(port_type, operation_name='PeriodicContextReport')
