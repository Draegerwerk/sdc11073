from __future__ import annotations

from typing import TYPE_CHECKING

from sdc11073.namespaces import PrefixesEnum
from sdc11073.xml_types.addressing_types import HeaderInformationBlock

from .serviceclientbase import GetRequestResult, HostedServiceClient

if TYPE_CHECKING:
    from sdc11073.consumer.manipulator import RequestManipulatorProtocol
    from sdc11073.xml_types.pm_types import LocalizedTextWidth


class LocalizationServiceClient(HostedServiceClient):
    """Client for LocalizationService."""

    port_type_name = PrefixesEnum.SDC.tag('LocalizationService')

    def get_localized_texts(self, refs: list[str] | None = None,  # noqa: PLR0913
                            version: int | None = None,
                            langs: list[str] | None = None,
                            text_widths: list[LocalizedTextWidth] | None = None,
                            number_of_lines: list[int] | None = None,
                            request_manipulator: RequestManipulatorProtocol | None = None) -> GetRequestResult:
        """Send a GetLocalizedText request.

        :param refs: optional list of reference names of the texts that are requested.
        :param version: optional revision of the referenced text that is requested.
        :param langs: optional list of language identifiers.
        :param text_widths: optional list of LocalizedTextWidth enums
        :param number_of_lines: optional list of integers.
        :param request_manipulator: see documentation of RequestManipulatorProtocol
        """
        data_model = self._sdc_definitions.data_model
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
        inf = HeaderInformationBlock(action=request.action, addr_to=self.endpoint_reference.Address)
        message = self._msg_factory.mk_soap_message(inf, payload=request)
        received_message_data = self.post_message(message, request_manipulator=request_manipulator)
        cls = data_model.msg_types.GetLocalizedTextResponse
        result = cls.from_node(received_message_data.p_msg.msg_node)

        return GetRequestResult(received_message_data, result)

    def get_supported_languages(self,
                                request_manipulator: RequestManipulatorProtocol | None = None) -> GetRequestResult:
        """Send a GetSupportedLanguages request."""
        data_model = self._sdc_definitions.data_model
        request = data_model.msg_types.GetSupportedLanguages()
        inf = HeaderInformationBlock(action=request.action, addr_to=self.endpoint_reference.Address)
        message = self._msg_factory.mk_soap_message(inf, payload=request)
        received_message_data = self.post_message(message, request_manipulator=request_manipulator)
        cls = data_model.msg_types.GetSupportedLanguagesResponse
        result = cls.from_node(received_message_data.p_msg.msg_node)
        return GetRequestResult(received_message_data, result)
