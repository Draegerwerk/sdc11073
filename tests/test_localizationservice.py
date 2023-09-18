import unittest
from itertools import product

from sdc11073.xml_types.pm_types import LocalizedText, LocalizedTextWidth
from sdc11073.provider.porttypes import localizationservice


class TestLocationService(unittest.TestCase):
    def setUp(self):
        self.width_list = [LocalizedTextWidth.XS,
                           LocalizedTextWidth.S,
                           LocalizedTextWidth.M,
                           LocalizedTextWidth.XL,
                           LocalizedTextWidth.XXL,
                           None]  # 'l' is missing
        self.lines_list = [1, 2, 3, 4, None]
        self.versions_list = [1, 2, None]
        self.lang_list = ['en-en', 'de-de', 'ru-ru', None]
        self.ref_list = ['a', 'b', 'c', 'd', 'e']
        self.localization_storage = localizationservice.LocalizationStorage()
        for ref in self.ref_list:
            for lang, version, width, lines in product(self.lang_list, self.versions_list, self.width_list,
                                                       self.lines_list):
                _ = localizationservice._tw2i(width) if width else 1
                if lines in (None, 1):
                    text = ref
                else:
                    text = '\n'.join([ref] * lines)
                self.localization_storage.add(
                    LocalizedText(text, lang=lang, ref=ref, version=version, text_width=width))

    def test_noFilter(self):
        texts = self.localization_storage.filter_localized_texts(requested_handles=None,
                                                                 requested_version=None,
                                                                 requested_langs=None,
                                                                 text_widths=None,
                                                                 number_of_lines=None)
        self.assertEqual(len(texts), len(self.ref_list) * len(self.lang_list) * len(self.width_list) * len(
            self.lines_list))  # one version per ref and language
        for t in texts:
            self.assertTrue(t.Version == 2)  # highest version only

    def test_refFilter(self):
        handles = ['b', 'c', 'something_different']
        texts = self.localization_storage.filter_localized_texts(requested_handles=handles,
                                                                 requested_version=None,
                                                                 requested_langs=None,
                                                                 text_widths=None,
                                                                 number_of_lines=None)
        self.assertEqual(len(texts), 2 * len(self.lang_list) * len(self.width_list) * len(self.lines_list))
        for t in texts:
            self.assertTrue(t.Ref in handles)

    def test_versionFilter(self):
        for version in (1, 2):
            texts = self.localization_storage.filter_localized_texts(requested_handles=None,
                                                                     requested_version=version,
                                                                     requested_langs=None,
                                                                     text_widths=None,
                                                                     number_of_lines=None)
            self.assertEqual(len(texts), len(self.ref_list) * len(self.lang_list) * len(self.width_list) * len(
                self.lines_list))  # one version per ref and language
            for t in texts:
                self.assertTrue(t.Version == version)

        texts = self.localization_storage.filter_localized_texts(requested_handles=None,
                                                                 requested_version=3,
                                                                 requested_langs=None,
                                                                 text_widths=None,
                                                                 number_of_lines=None)
        self.assertEqual(len(texts), 0)

    def test_langFilter(self):
        for lang in ('en-en', 'de-de'):
            texts = self.localization_storage.filter_localized_texts(requested_handles=None,
                                                                     requested_version=None,
                                                                     requested_langs=[lang],
                                                                     text_widths=None,
                                                                     number_of_lines=None)
            self.assertEqual(len(texts), len(self.ref_list) * len(self.width_list) * len(self.lines_list))
            for t in texts:
                self.assertTrue(t.Lang == lang)

        langs = ('en-en', 'de-de')
        texts = self.localization_storage.filter_localized_texts(requested_handles=None,
                                                                 requested_version=None,
                                                                 requested_langs=langs,
                                                                 text_widths=None,
                                                                 number_of_lines=None)
        self.assertEqual(len(texts), len(self.ref_list) * len(self.width_list) * len(self.lines_list) * 2)

        for t in texts:
            self.assertTrue(t.Lang in langs)

    def test_widthFilter(self):
        for width in ('s', 'xs', 'm', 'xl', 'xxl'):
            texts = self.localization_storage.filter_localized_texts(requested_handles=None,
                                                                     requested_version=None,
                                                                     requested_langs=None,
                                                                     text_widths=[width],
                                                                     number_of_lines=None)
            self.assertEqual(len(texts), len(self.ref_list) * len(self.lang_list))
            for t in texts:
                self.assertEqual(t.TextWidth, width)
            texts = self.localization_storage.filter_localized_texts(requested_handles=None,
                                                                     requested_version=None,
                                                                     requested_langs=None,
                                                                     text_widths=[width],
                                                                     number_of_lines=None)
            self.assertEqual(len(texts), len(self.ref_list) * len(self.lang_list))
            for t in texts:
                self.assertEqual(t.TextWidth, width)

            widths = ['l', 'xl']
            texts = self.localization_storage.filter_localized_texts(requested_handles=None,
                                                                     requested_version=None,
                                                                     requested_langs=None,
                                                                     text_widths=widths,
                                                                     number_of_lines=None)
            self.assertEqual(len(texts), len(self.ref_list) * len(self.lang_list) * 2)
            for t in texts:
                self.assertIn(t.TextWidth, ('m', 'xl'))

    def test_linesFilter(self):
        for line in (1, 2, 3, 4):
            texts = self.localization_storage.filter_localized_texts(requested_handles=None,
                                                                     requested_version=None,
                                                                     requested_langs=None,
                                                                     text_widths=None,
                                                                     number_of_lines=[line])
            for t in texts:
                self.assertTrue(t.n_o_l == line)
            self.assertEqual(len(texts), len(self.ref_list) * len(self.lang_list))

            lines = [1, 3]
            texts = self.localization_storage.filter_localized_texts(requested_handles=None,
                                                                     requested_version=None,
                                                                     requested_langs=None,
                                                                     text_widths=None,
                                                                     number_of_lines=lines)
            self.assertEqual(len(texts), len(self.ref_list) * len(self.lang_list) * 2)
            for t in texts:
                self.assertTrue(t.n_o_l in lines)

    def test_width_and_lines_filter(self):
        for width, line in product(('s', 'xs', 'm', 'xl', 'xxl'), (1, 2, 3, 4)):
            texts = self.localization_storage.filter_localized_texts(requested_handles=None,
                                                                     requested_version=None,
                                                                     requested_langs=None,
                                                                     text_widths=[width],
                                                                     number_of_lines=[line])
            for t in texts:
                self.assertLessEqual(t.n_o_l, line)
                self.assertLessEqual(localizationservice._tw2i(t.TextWidth), localizationservice._tw2i(width))
            self.assertEqual(len(texts), len(self.ref_list) * len(self.lang_list))

        widths = ['xs', 'xl']
        lines = [2, 4]
        texts = self.localization_storage.filter_localized_texts(requested_handles=None,
                                                                 requested_version=None,
                                                                 requested_langs=None,
                                                                 text_widths=widths,
                                                                 number_of_lines=lines)
        self.assertEqual(len(texts), len(self.ref_list) * len(self.lang_list) * 4)
