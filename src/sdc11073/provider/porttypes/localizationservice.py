from __future__ import annotations

import contextlib
from collections import defaultdict
from typing import TYPE_CHECKING, List, Optional, Union
from .porttypebase import DPWSPortTypeBase
from .porttypebase import WSDLMessageDescription, WSDLOperationBinding
from .porttypebase import mk_wsdl_two_way_operation, msg_prefix
from sdc11073.dispatch import DispatchKey
from sdc11073.namespaces import PrefixesEnum

if TYPE_CHECKING:
    from sdc11073.xml_types.pm_types import LocalizedText

def _tw2i(text_width_string):
    """ text width to int"""
    lookup = {'xs': 0, 's': 1, 'm': 2, 'l': 3, 'xl': 4, 'xxl': 5, None: 999}
    return lookup[text_width_string]


def _calc_number_of_lines(text):
    # definition of a line in Participant Model:
    # ...a line is defined as the content of the text from either the beginning of the text or the beginning of
    # a previous line until the next occurrence of period mark, question mark, exclamation mark, or paragraph.
    # TBD: naive approach?
    return len(text.split('\n'))


def _text_width_filter(localized_texts, width: int):
    candidates = [v for v in localized_texts if _tw2i(v.TextWidth) <= width]
    if candidates:
        candidates.sort(key=lambda obj: _tw2i(obj.TextWidth) or -1)
    return candidates


def _n_o_l_filter(localized_texts, n_o_l):
    candidates = [v for v in localized_texts if v.n_o_l <= n_o_l]
    if candidates:
        candidates.sort(key=lambda obj: obj.n_o_l or -1)
    return candidates


class LocalizationStorage:
    def __init__(self, localized_texts: Optional[List[LocalizedText]] = None):
        self._localized_texts = defaultdict(list)  # key = handle, value = list of LocalizedText objects
        if localized_texts:
            self.add(*localized_texts)

    def add(self, *localized_texts: LocalizedText):
        for text in localized_texts:
            self._localized_texts[text.Ref].append(text)

    def filter_localized_texts(self, requested_handles: Union[List[str], None],
                               requested_version: Union[int, None],
                               requested_langs: Union[List[str], None],
                               text_widths: Union[List[str], None],
                               number_of_lines: Union[List[int], None]):
        """

        :param requested_handles: list of handles
        :param requested_version: an integer or None
        :param requested_langs: list of language strings
        :param text_widths: a list of chars (s, xs, ...)
        :param number_of_lines: a list of integers, 0...n
        :return: a list of LocalizedText instances
        """
        # make integers for text_widths and number_of_lines
        if text_widths is None:
            text_widths = []

        i_text_widths = [_tw2i(w) for w in text_widths]

        if number_of_lines is None:
            number_of_lines = []
        if requested_handles is None:
            requested_handles = []
        i_nls = [int(line) for line in number_of_lines]

        if len(requested_handles) == 0:
            # If there is no Ref ELEMENT given in the request MESSAGE, then all texts are returned in
            # msg:GetLocalizedTextResponse/msg:Text
            handles = list(self._localized_texts.keys())
        else:
            # If there is at least one Ref ELEMENT given, then msg:GetLocalizedTextResponse/msg:Text contains all texts
            # that match the Ref elements of the msg:GetLocalizedText request MESSAGE.
            handles = requested_handles

        # create a flat list of all localized texts with the requested handles
        texts = []
        for handle in handles:
            with contextlib.suppress(KeyError):
                texts.extend(self._localized_texts[handle])

        # filter languages:
        if requested_langs is not None and len(requested_langs) > 0:
            texts = [t for t in texts if t.Lang in requested_langs]

        # filter requested versions. We need to do it per language, therefore create a lookup with (ref,lang) as key
        tmp_dict = defaultdict(list)
        for text in texts:
            tmp_dict[(text.Ref, text.Lang)].append(text)
        texts = []

        effective_requested_version = requested_version
        if requested_version is None:
            # determine the highest available Version in the storage
            all_versions = []
            for value in self._localized_texts.values():
                all_versions.extend(value)
            if len(all_versions) == 0:
                # ToDo: why return here?
                return []  # there is nothing
            all_versions = [a.Version for a in all_versions if a.Version is not None]
            if len(all_versions) > 0:
                effective_requested_version = max(all_versions)

        # If the referenced text is not available in the specific version, then
        # msg:GetLocalizedTextResponse/msg:Text is empty
        for _, value_list in tmp_dict.items():
            texts.extend([v for v in value_list if v.Version == effective_requested_version])

        # - If there is no NumberOfLines ELEMENT given in the request MESSAGE, then all texts independent of the number
        #   of lines are returned in msg:GetLocalizedTextResponse/msg:Text.
        # - If there is at least one NumberOfLines ELEMENT given, msg:GetLocalizedTextResponse/msg:Text contains texts
        #   that match the number of lines defined by the NumberOfLines elements of the msg:GetLocalizedText request
        #   MESSAGE. Matching in this case means that the number of lines in the text is less or equal to the
        #   NumberOfLines elements.

        if len(i_text_widths) > 0 or len(number_of_lines) > 0:
            if len(number_of_lines) > 0:
                # calculate number of lines for every localized text and add is as member to the object
                for text in texts:
                    text.n_o_l = _calc_number_of_lines(text.text)

            # create again dictionary by ref and language:
            tmp_dict = defaultdict(list)
            for text in texts:
                tmp_dict[(text.Ref, text.Lang)].append(text)
            tmp = []

            if len(i_text_widths) > 0 and len(number_of_lines) > 0:
                # now find for each combination of (width, lines) list the best match
                for value_list in tmp_dict.values():
                    # candidates = []
                    for text_width in i_text_widths:
                        candidates1 = _text_width_filter(value_list,
                                                         text_width)  # returns sorted list of smaller elements
                        for lines_cnt in i_nls:
                            candidates2 = _n_o_l_filter(candidates1, lines_cnt)
                            if len(candidates2) > 0:
                                candidates2.sort(key=lambda obj: obj.TextWidth * obj.n_o_l)  # sort by area size
                                tmp.append(candidates2[-1])  # use the largest one
            elif len(i_text_widths) > 0:
                # filter only text widths
                for value_list in tmp_dict.values():
                    for text_width in i_text_widths:
                        candidates = _text_width_filter(value_list,
                                                        text_width)  # returns sorted list of smaller elements
                        if candidates:
                            tmp.append(candidates[-1])  # use the largest one

            elif len(number_of_lines) > 0:
                # filter only number of lines
                for value_list in tmp_dict.values():
                    for lines_cnt in i_nls:
                        candidates = _n_o_l_filter(value_list, lines_cnt)
                        if candidates:
                            tmp.append(candidates[-1])  # use the largest one
            texts = list(tmp)
        return texts

    def get_supported_languages(self):
        texts = self._flat_list()
        result = set()
        for text in texts:
            result.add(str(text.Lang))
        return list(result)

    def _flat_list(self, ref_list=None):
        if ref_list is None:
            # If there is no Ref ELEMENT given in the request MESSAGE, then all texts are returned in
            # msg:GetLocalizedTextResponse/msg:Text
            handles = list(self._localized_texts.keys())
        else:
            # If there is at least one Ref ELEMENT given, then msg:GetLocalizedTextResponse/msg:Text contains all texts
            # that match the Ref elements of the msg:GetLocalizedText request MESSAGE.
            handles = ref_list

        # create a flat list of all localized texts with the requested handles
        texts = []
        for handle in handles:
            with contextlib.suppress(KeyError):
                texts.extend(self._localized_texts[handle])
        return texts


class LocalizationService(DPWSPortTypeBase):
    port_type_name = PrefixesEnum.SDC.tag('LocalizationService')
    WSDLMessageDescriptions = (WSDLMessageDescription('GetLocalizedText',
                                                      (f'{msg_prefix}:GetLocalizedText',)),
                               WSDLMessageDescription('GetLocalizedTextResponse',
                                                      (f'{msg_prefix}:GetLocalizedTextResponse',)),
                               WSDLMessageDescription('GetSupportedLanguages',
                                                      (f'{msg_prefix}:GetSupportedLanguages',)),
                               WSDLMessageDescription('GetSupportedLanguagesResponse',
                                                      (f'{msg_prefix}:GetSupportedLanguagesResponse',)),
                               )
    WSDLOperationBindings = (WSDLOperationBinding('GetLocalizedText', 'literal', 'literal'),
                             WSDLOperationBinding('GetSupportedLanguages', 'literal', 'literal'),)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.localization_storage = LocalizationStorage()

    def register_hosting_service(self, hosting_service):
        super().register_hosting_service(hosting_service)
        actions = self._mdib.sdc_definitions.Actions
        msg_names = self._mdib.sdc_definitions.data_model.msg_names
        hosting_service.register_post_handler(DispatchKey(actions.GetLocalizedText, msg_names.GetLocalizedText),
                                              self._on_get_localized_text)
        hosting_service.register_post_handler(
            DispatchKey(actions.GetSupportedLanguages, msg_names.GetSupportedLanguages),
            self._on_get_supported_languages)

    def _on_get_localized_text(self, request_data):
        data_model = self._sdc_definitions.data_model
        self._logger.debug('_on_get_localized_text')
        cls = data_model.msg_types.GetLocalizedText
        get_localized_text = cls.from_node(request_data.message_data.p_msg.msg_node)
        texts = self.localization_storage.filter_localized_texts(get_localized_text.Ref,
                                                                 get_localized_text.Version,
                                                                 get_localized_text.Lang,
                                                                 get_localized_text.TextWidth,
                                                                 get_localized_text.NumberOfLines)
        response = data_model.msg_types.GetLocalizedTextResponse()
        response.Text.extend(texts)
        response.set_mdib_version_group(self._mdib.mdib_version_group)
        response_envelope = self._sdc_device.msg_factory.mk_reply_soap_message(request_data, response)
        return response_envelope

    def _on_get_supported_languages(self, request_data):
        data_model = self._sdc_definitions.data_model
        self._logger.debug('_on_get_supported_languages')
        languages = self.localization_storage.get_supported_languages()
        response = data_model.msg_types.GetSupportedLanguagesResponse()
        response.Lang.extend(languages)
        response.set_mdib_version_group(self._mdib.mdib_version_group)
        response_envelope = self._sdc_device.msg_factory.mk_reply_soap_message(request_data, response)
        return response_envelope

    def add_wsdl_port_type(self, parent_node):
        port_type = self._mk_port_type_node(parent_node)
        mk_wsdl_two_way_operation(port_type, operation_name='GetLocalizedText')
        mk_wsdl_two_way_operation(port_type, operation_name='GetSupportedLanguages')
