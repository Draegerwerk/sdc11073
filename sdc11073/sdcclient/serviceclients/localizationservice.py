from .serviceclientbase import HostedServiceClient, GetRequestResult


class LocalizationServiceClient(HostedServiceClient):

    def get_localized_texts(self, refs=None, version=None, langs=None, text_widths=None, number_of_lines=None,
                            request_manipulator=None) -> GetRequestResult:
        data_model = self._sdc_definitions.data_model
        nsh = data_model.ns_helper
        request = data_model.msg_types.GetLocalizedText()
        if refs is not None:
            request.Ref.extend(refs)
        if version is not None:
            request.Version = version
        if langs is not None:
            request.Lang.extend(langs)
        if text_widths is not None:
            request.TextWidth.extend(text_widths)
        if number_of_lines is not None:
            request.NumberOfLines.extend(number_of_lines)
        payload_element = request.as_etree_node(request.NODETYPE,
                                                nsh.partial_map(nsh.MSG, nsh.PM))
        message = self._msg_factory.mk_soap_message(self.endpoint_reference.address,
                                                    request.action,
                                                    payload_element)
        received_message_data = self.post_message(message, request_manipulator=request_manipulator)
        cls = data_model.msg_types.GetLocalizedTextResponse
        result = cls.from_node(received_message_data.p_msg.msg_node)

        return GetRequestResult(received_message_data, result)

    def get_supported_languages(self, request_manipulator=None) -> GetRequestResult:
        data_model = self._sdc_definitions.data_model
        nsh = data_model.ns_helper
        request = data_model.msg_types.GetSupportedLanguages()
        payload_element = request.as_etree_node(request.NODETYPE,
                                                nsh.partial_map(nsh.MSG, nsh.PM))
        message = self._msg_factory.mk_soap_message(self.endpoint_reference.address,
                                                    request.action,
                                                    payload_element)
        received_message_data = self.post_message(message, request_manipulator=request_manipulator)
        cls = data_model.msg_types.GetSupportedLanguagesResponse
        result = cls.from_node(received_message_data.p_msg.msg_node)
        return GetRequestResult(received_message_data, result)
