from .serviceclientbase import HostedServiceClient, GetRequestResult

class GetServiceClient(HostedServiceClient):

    def get_mdib(self, request_manipulator=None) -> GetRequestResult:
        data_model = self._sdc_definitions.data_model
        nsh = data_model.ns_helper
        request = data_model.msg_types.GetMdib()
        payload_element = request.as_etree_node(request.NODETYPE,
                                                nsh.partial_map(nsh.MSG, nsh.PM))
        message = self._msg_factory.mk_soap_message(self.endpoint_reference.address,
                                                    request.action,
                                                    payload_element)
        received_message_data = self.post_message(message, request_manipulator=request_manipulator)
        result = received_message_data.msg_reader.read_get_mdib_response(received_message_data)
        return GetRequestResult(received_message_data, result)

    def get_md_description(self, requested_handles=None, request_manipulator=None) -> GetRequestResult:
        """
        :param requested_handles: None if all states shall be requested, otherwise a list of handles
        """
        data_model = self._sdc_definitions.data_model
        nsh = data_model.ns_helper
        request = data_model.msg_types.GetMdDescription()
        if requested_handles is not None:
            request.HandleRef.extend(requested_handles)
        payload_element = request.as_etree_node(request.NODETYPE,
                                                nsh.partial_map(nsh.MSG, nsh.PM))

        message = self._msg_factory.mk_soap_message(self.endpoint_reference.address,
                                                    request.action,
                                                    payload_element)
        received_message_data = self.post_message(message, request_manipulator=request_manipulator)
        cls = data_model.msg_types.GetMdDescriptionResponse
        report = cls.from_node(received_message_data.p_msg.msg_node)

        return GetRequestResult(received_message_data, report)

    def get_md_state(self, requested_handles=None, request_manipulator=None) -> GetRequestResult:
        """
        :param requested_handles: None if all states shall be requested, otherwise a list of handles
        """
        data_model = self._sdc_definitions.data_model
        nsh = data_model.ns_helper
        request = data_model.msg_types.GetMdState()
        if requested_handles is not None:
            request.HandleRef.extend(requested_handles)
        payload_element = request.as_etree_node(request.NODETYPE,
                                                nsh.partial_map(nsh.MSG, nsh.PM))
        message = self._msg_factory.mk_soap_message(self.endpoint_reference.address,
                                                    request.action,
                                                    payload_element)
        received_message_data = self.post_message(message, request_manipulator=request_manipulator)
        cls = data_model.msg_types.GetMdStateResponse
        report = cls.from_node(received_message_data.p_msg.msg_node)
        return GetRequestResult(received_message_data, report)
