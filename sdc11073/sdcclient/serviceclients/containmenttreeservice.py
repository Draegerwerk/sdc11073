from .serviceclientbase import HostedServiceClient, GetRequestResult


class CTreeServiceClient(HostedServiceClient):

    def get_descriptor(self, handles, request_manipulator=None) -> GetRequestResult:
        """

        :param handles: a list of strings
        :return: a list of etree nodes
        """
        message = self._msg_factory.mk_get_descriptor_message(
            self.endpoint_reference.address, handles)
        message_data = self.post_message(message, request_manipulator=request_manipulator)
        descriptors = message_data.msg_reader.read_get_descriptor_response(message_data)
        return GetRequestResult(message_data, descriptors)

    def get_containment_tree(self, handles, request_manipulator=None) -> GetRequestResult:
        """

        :param handles: a list of strings
        :return: a list of etree nodes
        """
        message = self._msg_factory.mk_get_containmenttree_message(self.endpoint_reference.address, handles)
        received_message_data = self.post_message(message,request_manipulator=request_manipulator)
        descriptors = received_message_data.msg_reader.read_get_containment_tree_response(received_message_data)
        return GetRequestResult(received_message_data, descriptors)
