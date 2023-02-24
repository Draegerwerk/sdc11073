from .serviceclientbase import HostedServiceClient, GetRequestResult


class CTreeServiceClient(HostedServiceClient):

    def get_descriptor(self, handles, request_manipulator=None) -> GetRequestResult:
        """

        :param handles: a list of strings
        :return: a list of etree nodes
        """
        data_model = self._sdc_definitions.data_model
        request = data_model.msg_types.GetDescriptor()
        request.HandleRef.extend(handles)
        message = self._msg_factory.mk_soap_message(self.endpoint_reference.Address, request)
        received_message_data = self.post_message(message, request_manipulator=request_manipulator)
        cls = data_model.msg_types.GetDescriptorResponse
        report = cls.from_node(received_message_data.p_msg.msg_node)
        return GetRequestResult(received_message_data, report)

    def get_containment_tree(self, handles, request_manipulator=None) -> GetRequestResult:
        """

        :param handles: a list of strings
        :return: a list of etree nodes
        """
        data_model = self._sdc_definitions.data_model
        request = data_model.msg_types.GetContainmentTree()
        request.HandleRef.extend(handles)
        message = self._msg_factory.mk_soap_message(self.endpoint_reference.Address, request)
        received_message_data = self.post_message(message,request_manipulator=request_manipulator)
        cls = data_model.msg_types.GetContainmentTreeResponse
        report = cls.from_node(received_message_data.p_msg.msg_node)
        return GetRequestResult(received_message_data, report)
