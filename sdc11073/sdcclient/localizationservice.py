from .hostedservice import HostedServiceClient
from ..pmtypes import LocalizedText


class LocalizationServiceClient(HostedServiceClient):

    def _get_localized_text_response(self, refs=None, version=None, langs=None, text_widths=None, number_of_lines=None,
                                     request_manipulator=None):
        '''

        :param refs: a list of strings or None
        :param version: an unsigned integer or None
        :param langs: a list of strings or None
        :param text_widths: a list of strings or None (each string one of xs, s, m, l, xs, xxs)
        :param number_of_lines: a list of unsigned integers or None
        :param request_manipulator:
        :return: a list of LocalizedText objects
        '''
        envelope = self._msg_factory.mk_getlocalizedtext_envelope(self.endpoint_reference.address, self.porttype,
                                                                  refs, version, langs, text_widths, number_of_lines)
        result_envelope = self._call_get_method(envelope, 'GetLocalizedText',
                                                request_manipulator=request_manipulator)
        return result_envelope

    def get_localized_text_node(self, refs=None, version=None, langs=None, text_widths=None, number_of_lines=None,
                                request_manipulator=None):
        return self._get_localized_text_response(refs, version, langs, text_widths, number_of_lines,
                                                 request_manipulator).msg_node

    def _get_localized_texts(self, refs=None, version=None, langs=None, text_widths=None, number_of_lines=None,
                             request_manipulator=None):
        result = []
        response_node = self._get_localized_text_response(refs, version, langs, text_widths, number_of_lines,
                                                          request_manipulator).msg_node
        if response_node is not None:
            for element in response_node:
                l_text = LocalizedText.from_node(element)
                result.append(l_text)
        return result

    def _get_supported_languages(self, request_manipulator=None):
        envelope = self._msg_factory.mk_getsupportedlanguages_envelope(
            self.endpoint_reference.address, self.porttype)
        return self._call_get_method(envelope, 'GetSupportedLanguages', request_manipulator=request_manipulator)

    def get_supported_languages(self, request_manipulator=None):
        result_envelope = self._get_supported_languages(request_manipulator)
        result = []
        for element in result_envelope.msg_node:
            result.append(str(element.text))
        return result

    def get_supported_language_nodes(self, request_manipulator=None):
        result_envelope = self._get_supported_languages(request_manipulator)
        return result_envelope.msg_node
