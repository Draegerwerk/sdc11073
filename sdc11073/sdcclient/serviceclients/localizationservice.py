from .serviceclientbase import HostedServiceClient, GetRequestResult


class LocalizationServiceClient(HostedServiceClient):

    def get_localized_texts(self, refs=None, version=None, langs=None, text_widths=None, number_of_lines=None,
                            request_manipulator=None) -> GetRequestResult:
        message = self._msg_factory.mk_get_localized_text_message(self.endpoint_reference.address,
                                                                  refs, version, langs, text_widths, number_of_lines)
        received_message_data = self.post_message(message, request_manipulator=request_manipulator)
        result = received_message_data.msg_reader.read_get_localized_text_response(received_message_data)
        return GetRequestResult(received_message_data, result)

    def get_supported_languages(self, request_manipulator=None) -> GetRequestResult:
        message = self._msg_factory.mk_get_supported_languages_message(self.endpoint_reference.address)
        received_message_data = self.post_message(message, request_manipulator=request_manipulator)
        result = received_message_data.msg_reader.read_get_supported_languages_response(received_message_data)
        return GetRequestResult(received_message_data, result)
