from .porttypebase import DPWSPortTypeBase, WSDLMessageDescription, WSDLOperationBinding, mk_wsdl_two_way_operation
from .porttypebase import msg_prefix
from sdc11073.dispatch import DispatchKey
from sdc11073.namespaces import PrefixesEnum


class GetService(DPWSPortTypeBase):
    port_type_name = PrefixesEnum.SDC.tag('GetService')
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

    def register_hosting_service(self, hosting_service):
        super().register_hosting_service(hosting_service)
        actions = self._sdc_device.mdib.sdc_definitions.Actions
        msg_names = self._sdc_device.mdib.sdc_definitions.data_model.msg_names
        hosting_service.register_post_handler(DispatchKey(actions.GetMdState, msg_names.GetMdState),
                                              self._on_get_md_state)
        hosting_service.register_post_handler(DispatchKey(actions.GetMdib, msg_names.GetMdib),
                                              self._on_get_mdib)
        hosting_service.register_post_handler(DispatchKey(actions.GetMdDescription, msg_names.GetMdDescription),
                                              self._on_get_md_description)

    def _on_get_md_state(self, request_data):
        data_model = self._sdc_definitions.data_model
        msg_node = request_data.message_data.p_msg.msg_node
        get_md_state = data_model.msg_types.GetMdState.from_node(msg_node)
        requested_handles = get_md_state.HandleRef
        if len(requested_handles) > 0:
            self._logger.debug('_on_get_md_state from {} req. handles:{}', request_data.peer_name, requested_handles)
        else:
            self._logger.debug('_on_get_md_state from {}', request_data.peer_name)

        # get the requested state containers from mdib
        state_containers = []
        with self._mdib.mdib_lock:
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
                            state_containers.extend(self._mdib.states.descriptor_handle.get(handle, []))
                            state_containers.extend(self._mdib.context_states.descriptor_handle.get(handle, []))
                else:
                    for handle in requested_handles:
                        state_containers.extend(self._mdib.states.descriptor_handle.get(handle, []))

                self._logger.debug('_on_get_md_state requested Handles:{} found {} states', requested_handles,
                                   len(state_containers))

        factory = self._sdc_device.msg_factory
        response = data_model.msg_types.GetMdStateResponse()
        response.MdState.State.extend(state_containers)
        response.set_mdib_version_group(self._mdib.mdib_version_group)
        created_message = factory.mk_reply_soap_message(request_data, response)
        self._logger.debug('_on_get_md_state returns {}',
                           lambda: created_message.serialize())
        return created_message

    def _on_get_mdib(self, request_data):
        self._logger.debug('_on_get_mdib')
        if self._sdc_device.contextstates_in_getmdib:
            mdib_node, mdib_version_group = self._mdib.reconstruct_mdib_with_context_states()
        else:
            mdib_node, mdib_version_group = self._mdib.reconstruct_mdib()
        response = self._data_model.msg_types.GetMdibResponse()
        response.set_mdib_version_group(mdib_version_group)
        response.Mdib = mdib_node
        response = self._sdc_device.msg_factory.mk_reply_soap_message(request_data, response)
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
        data_model = self._sdc_definitions.data_model

        self._logger.debug('_on_get_md_description')
        msg_node = request_data.message_data.p_msg.msg_node
        get_md_state = data_model.msg_types.GetMdDescription.from_node(msg_node)
        requested_handles = get_md_state.HandleRef
        if len(requested_handles) > 0:
            self._logger.info('_on_get_md_description requested Handles:{}', requested_handles)
        response = self.mk_get_mddescription_response_message(
            request_data, self._sdc_device.mdib, requested_handles)
        self._logger.debug('_on_get_md_description returns {}',
                           lambda: response.serialize())
        return response

    def mk_get_mddescription_response_message(self, request_data, mdib, requested_handles):
        """For simplification reason this implementation returns either all descriptors or none."""
        return_all = len(requested_handles) == 0  # if we have handles, we need to check them
        dummy_response = self._sdc_definitions.data_model.msg_types.GetMdDescriptionResponse()
        dummy_response.set_mdib_version_group(mdib.mdib_version_group)
        response = self._sdc_device.msg_factory.mk_reply_soap_message(request_data, dummy_response)
        # now add to payload_element
        response_node = response.p_msg.payload_element
        for handle in requested_handles:
            # if at least one requested handle is valid, return all.
            if mdib.descriptions.handle.get_one(handle, allow_none=True) is not None:
                return_all = True
                break
        if return_all:
            md_description_node, mdib_version_group = mdib.reconstruct_md_description()
            # append all children of md_description_node to msg_names.MdDescription node in response
            response_node[0].extend(md_description_node[:])
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
