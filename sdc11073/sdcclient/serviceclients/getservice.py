from .serviceclientbase import HostedServiceClient, GetRequestResult

class GetServiceClient(HostedServiceClient):

    def get_mdib(self, request_manipulator=None) -> GetRequestResult:
        message = self._msg_factory.mk_get_mdib_message(self.endpoint_reference.address)
        received_message_data = self.post_message(message, request_manipulator=request_manipulator)
        result = received_message_data.msg_reader.read_get_mdib_response(received_message_data)
        return GetRequestResult(received_message_data, result)

    def get_md_description(self, requested_handles=None, request_manipulator=None) -> GetRequestResult:
        """
        :param requested_handles: None if all states shall be requested, otherwise a list of handles
        """
        message = self._msg_factory.mk_get_mddescription_message(self.endpoint_reference.address, requested_handles)
        received_message_data = self.post_message(message, request_manipulator=request_manipulator)
        descriptors = received_message_data.msg_reader.read_get_mddescription_response(received_message_data)
        return GetRequestResult(received_message_data, descriptors)

    def get_md_state(self, requested_handles=None, request_manipulator=None) -> GetRequestResult:
        """
        :param requested_handles: None if all states shall be requested, otherwise a list of handles
        """
        message = self._msg_factory.mk_get_mdstate_message(self.endpoint_reference.address, requested_handles)
        received_message_data = self.post_message(message, request_manipulator=request_manipulator)
        states = received_message_data.msg_reader.read_get_mdstate_response(received_message_data)
        return GetRequestResult(received_message_data, states)
