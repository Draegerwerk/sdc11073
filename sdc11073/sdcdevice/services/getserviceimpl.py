from .servicesbase import DPWSPortTypeImpl, WSDLMessageDescription, WSDLOperationBinding, mk_wsdl_two_way_operation
from .servicesbase import msg_prefix

class GetService(DPWSPortTypeImpl):
    WSDLMessageDescriptions = (WSDLMessageDescription('GetMdState',
                                                      (f'{msg_prefix}:GetMdState',)),
                               WSDLMessageDescription('GetMdStateResponse',
                                                      (f'{msg_prefix}:GetMdStateResponse',)),
                               WSDLMessageDescription('GetMdib',
                                                      (f'{msg_prefix}:GetMdib',)),
                               WSDLMessageDescription('GetMdibResponse',
                                                      (f'{msg_prefix}:GetMdibResponse',)),
                               WSDLMessageDescription('GetMdDescription',
                                                      (f'{msg_prefix}:GetMdDescription',)),
                               WSDLMessageDescription('GetMdDescriptionResponse',
                                                      (f'{msg_prefix}:GetMdDescriptionResponse',)),
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

            response = self._sdc_device.msg_factory.mk_get_mdstate_response_message(
                request_data.message_data, self.actions.GetMdStateResponse,
                mdib_version, sequence_id, state_containers, self._mdib.nsmapper)
        self._logger.debug('_on_get_md_state returns {}',
                           lambda: response.serialize_message())
        return response

    def _on_get_mdib(self, request_data):
        self._logger.debug('_on_get_mdib')
        response = self._sdc_device.msg_factory.mk_get_mdib_response_message(
            request_data.message_data, self._mdib, self._sdc_device.contextstates_in_getmdib)
        self._logger.debug('_on_get_mdib returns {}', lambda: response.serialize_message())
        return response

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
        requested_handles = request_data.message_data.msg_reader.read_getmddescription_request(request_data.message_data)
        if len(requested_handles) > 0:
            self._logger.info('_on_get_md_description requested Handles:{}', requested_handles)
        response = self._sdc_device.msg_factory.mk_get_mddescription_response_message(
            request_data.message_data, self._sdc_device.mdib, requested_handles
        )
        self._logger.debug('_on_get_md_description returns {}',
                           lambda: response.serialize_message())
        return response

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
