from .hostedservice import HostedServiceClient


class LocalizationServiceClient(HostedServiceClient):

    def get_localized_texts(self, refs=None, version=None, langs=None, text_widths=None, number_of_lines=None,
                            request_manipulator=None):
        envelope = self._msg_factory.mk_getlocalizedtext_envelope(self.endpoint_reference.address, self.porttype,
                                                                  refs, version, langs, text_widths, number_of_lines)
        message_data = self._call_get_method(envelope, 'GetLocalizedText',
                                             request_manipulator=request_manipulator)
        result = message_data.msg_reader.read_get_localized_text_response(message_data)
        return result

    def get_supported_languages(self, request_manipulator=None):
        envelope = self._msg_factory.mk_get_supported_languages_envelope(self.endpoint_reference.address, self.porttype)
        message_data = self._call_get_method(envelope, 'GetSupportedLanguages', request_manipulator=request_manipulator)
        result = message_data.msg_reader.read_get_supported_languages_response(message_data)
        return result
