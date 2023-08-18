import collections
import unittest
from typing import Any
from unittest import mock

import lxml
from xmldiff import actions as xml_diff_actions
from xmldiff import main as xml_diff

from sdc11073.xml_types import pm_types, xml_structure


class TestPmTypes(unittest.TestCase):

    def test_coded_value(self):
        c1 = pm_types.CodedValue('42')
        c2 = pm_types.CodedValue('42', coding_system='abc')
        self.assertTrue(c1.is_equivalent(pm_types.CodedValue('42')))
        # test with explicit coding system
        self.assertTrue(c1.is_equivalent(pm_types.Coding('42', pm_types.DEFAULT_CODING_SYSTEM, None)))
        # test with explicit coding system and different version
        self.assertFalse(c1.is_equivalent(pm_types.Coding('42', pm_types.DEFAULT_CODING_SYSTEM, '1')))
        # it shall be possible to compare with a Coding instance
        self.assertTrue(c1.is_equivalent(pm_types.Coding('42')))
        # different coding system
        self.assertFalse(c2.is_equivalent(pm_types.Coding('42')))

        # if two CodedValue instances are compared, the translations shall also be handled
        c2.Translation.append(pm_types.Translation('41'))
        self.assertTrue(c2.is_equivalent(pm_types.Coding('41')))
        c3 = pm_types.CodedValue('44')
        c3.Translation.append(pm_types.Translation('41'))  # same translation as c2
        self.assertTrue(c2.is_equivalent(c3))

    def test_have_matching_codes(self):
        c1 = pm_types.CodedValue('42', coding_system='abc')
        c1.Translation.append(pm_types.Translation('41'))
        self.assertTrue(pm_types.have_matching_codes(c1, pm_types.Coding('42', coding_system='abc')))
        self.assertTrue(pm_types.have_matching_codes(c1, pm_types.Coding('41')))
        self.assertFalse(pm_types.have_matching_codes(c1, pm_types.Coding('41', coding_system='abc')))

        c2 = pm_types.CodedValue('xxx', coding_system='abc')
        c2.Translation.append(pm_types.Translation('41'))
        self.assertTrue(pm_types.have_matching_codes(c1, c2))

    def test_allowed_value(self):
        """Verify that value is an empty string if text of Value node is empty."""
        text = """<pm:AllowedValue xmlns:pm="http://standards.ieee.org/downloads/11073/11073-10207-2017/participant">
                <pm:Value>{}</pm:Value>
                <pm:Type Code="202890">
                </pm:Type>
              </pm:AllowedValue>
"""
        node = lxml.etree.fromstring(text.format(''))
        allowed_value1 = pm_types.AllowedValue.from_node(node)
        self.assertEqual(allowed_value1.Value, '')
        generated_node = allowed_value1.as_etree_node(lxml.etree.QName('foo', 'bar'), {})
        self.assertEqual('', generated_node[0].text)

        node = lxml.etree.fromstring(text.format('foobar'))
        allowed_value2 = pm_types.AllowedValue.from_node(node)
        self.assertEqual(allowed_value2.Value, 'foobar')

    def test_activate_operation_descriptor_argument(self):
        """Verify that ActivateOperationDescriptorArgument is correctly instantiated from node."""
        text = """<pm:Argument xmlns:pm="http://standards.ieee.org/downloads/11073/11073-10207-2017/participant">
                      <pm:ArgName Code="202890"></pm:ArgName>
                      <pm:Arg xmlns:dd="dummy">dd:Something</pm:Arg>
                  </pm:Argument>
        """
        node = lxml.etree.fromstring(text.format(''))
        arg = pm_types.ActivateOperationDescriptorArgument.from_node(node)
        self.assertEqual(arg.ArgName, pm_types.CodedValue("202890"))
        self.assertEqual(arg.Arg, lxml.etree.QName("dummy", "Something"))


class TestExtensions(unittest.TestCase):

    def test_compare_extensions(self):
        xml = b"""
        <pm:Identification xmlns:pm="http://standards.ieee.org/downloads/11073/11073-10207-2017/participant"
                           Root="urn:uuid:90beab82-f160-4e2f-b3b2-ed8cfcf5e205"
                           Extension="123.234.424">
            <ext:Extension xmlns:ext="http://standards.ieee.org/downloads/11073/11073-10207-2017/extension">
                <foo someattr="somevalue"/>
                <bar anotherattr="differentvalue"/>
            </ext:Extension>
        </pm:Identification>
        """
        self.assertNotEqual(lxml.etree.fromstring(xml), lxml.etree.fromstring(xml))
        inst1 = pm_types.InstanceIdentifier.from_node(lxml.etree.fromstring(xml))
        inst2 = pm_types.InstanceIdentifier.from_node(lxml.etree.fromstring(xml))
        self.assertEqual(inst1.ExtExtension, inst2.ExtExtension)
        self.assertEqual(inst1, inst2)

        another_xml = b"""
                <pm:Identification xmlns:pm="http://standards.ieee.org/downloads/11073/11073-10207-2017/participant"
                                   Root="urn:uuid:90beab82-f160-4e2f-b3b2-ed8cfcf5e205"
                                   Extension="123.234.424">
                    <ext:Extension xmlns:ext="http://standards.ieee.org/downloads/11073/11073-10207-2017/extension">
                        <foo someattr="somevalue"/>
                        <bar anotherattr="differentvalue2"/>
                    </ext:Extension>
                </pm:Identification>
                """
        inst2 = pm_types.InstanceIdentifier.from_node(lxml.etree.fromstring(another_xml))
        self.assertNotEqual(inst1.ExtExtension, inst2.ExtExtension)
        self.assertNotEqual(inst1, inst2)

    def test_order(self):
        xml1 = b"""
        <pm:Identification xmlns:pm="http://standards.ieee.org/downloads/11073/11073-10207-2017/participant"
                           Root="urn:uuid:90beab82-f160-4e2f-b3b2-ed8cfcf5e205"
                           Extension="123.234.424">
            <ext:Extension xmlns:ext="http://standards.ieee.org/downloads/11073/11073-10207-2017/extension">
                <foo someattr="somevalue"/>
                <bar anotherattr="differentvalue"/>
            </ext:Extension>
        </pm:Identification>
        """
        xml2 = b"""
        <pm:Identification xmlns:pm="http://standards.ieee.org/downloads/11073/11073-10207-2017/participant"
                           Root="urn:uuid:90beab82-f160-4e2f-b3b2-ed8cfcf5e205"
                           Extension="123.234.424">
            <ext:Extension xmlns:ext="http://standards.ieee.org/downloads/11073/11073-10207-2017/extension">
                <bar anotherattr="differentvalue"/>
                <foo someattr="somevalue"/>
            </ext:Extension>
        </pm:Identification>
        """
        inst1 = pm_types.InstanceIdentifier.from_node(lxml.etree.fromstring(xml1))
        inst2 = pm_types.InstanceIdentifier.from_node(lxml.etree.fromstring(xml2))
        self.assertNotEqual(inst1.ExtExtension, inst2.ExtExtension)
        self.assertNotEqual(inst1, inst2)

    def test_fails_with_qname(self):
        xml1 = lxml.etree.fromstring(b"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<ext:Extension xmlns:ext="http://standards.ieee.org/downloads/11073/11073-10207-2017/extension"
        xmlns:what="123.456.789">
        <what:ItIsNotKnown>
                <what:Unknown>what:lorem</what:Unknown>
        </what:ItIsNotKnown>
</ext:Extension>""")
        xml2 = lxml.etree.fromstring(b"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<ext:Extension xmlns:ext="http://standards.ieee.org/downloads/11073/11073-10207-2017/extension"
        xmlns:who="123.456.789">
        <who:ItIsNotKnown>
                <who:Unknown>who:lorem</who:Unknown>
        </who:ItIsNotKnown>
</ext:Extension>""")
        self.assertNotEqual(lxml.etree.tostring(xml1), lxml.etree.tostring(xml2))
        inst1 = xml_structure.ExtensionLocalValue(collections.OrderedDict([(xml1.tag, xml1)]))
        inst2 = xml_structure.ExtensionLocalValue(collections.OrderedDict([(xml2.tag, xml2)]))
        self.assertNotEqual(inst1, inst2)

    def test_ignore_insert_namespaces(self):
        xml1 = lxml.etree.fromstring(b"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<ext:Extension xmlns:ext="http://standards.ieee.org/downloads/11073/11073-10207-2017/extension">
<what:ItIsNotKnown xmlns:what="123.456.789"><what:Unknown>What does this mean?</what:Unknown></what:ItIsNotKnown>
</ext:Extension>""")
        xml2 = lxml.etree.fromstring(b"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<ext:Extension xmlns:ext="http://standards.ieee.org/downloads/11073/11073-10207-2017/extension" xmlns:what="123.456.789">
<what:ItIsNotKnown><what:Unknown>What does this mean?</what:Unknown></what:ItIsNotKnown>
</ext:Extension>""")
        self.assertNotEqual(lxml.etree.tostring(xml1), lxml.etree.tostring(xml2))
        inst1 = xml_structure.ExtensionLocalValue(collections.OrderedDict([(xml1.tag, xml1)]))
        inst2 = xml_structure.ExtensionLocalValue(collections.OrderedDict([(xml2.tag, xml2)]))
        self.assertEqual(inst1, inst2)

        def _return_only_insert_namespace_error(*_: Any, **__: Any) -> list[xml_diff_actions.InsertNamespace]:
            return []

        placeholder_xml = lxml.etree.Element('a')
        placeholder = collections.OrderedDict([(placeholder_xml.tag, placeholder_xml)])
        with mock.patch.object(xml_diff, 'diff_trees') as diff_mock:
            diff_mock.side_effect = _return_only_insert_namespace_error
            self.assertEqual(xml_structure.ExtensionLocalValue(placeholder),
                             xml_structure.ExtensionLocalValue(placeholder))
            self.assertEqual(2, diff_mock.call_count)

        def _return_only_insert_namespace_error(*_: Any, **__: Any) -> list[xml_diff_actions.InsertNamespace]:
            return [xml_diff_actions.InsertNamespace('', '')]

        with mock.patch.object(xml_diff, 'diff_trees') as diff_mock:
            diff_mock.side_effect = _return_only_insert_namespace_error
            self.assertEqual(xml_structure.ExtensionLocalValue(placeholder),
                             xml_structure.ExtensionLocalValue(placeholder))
            self.assertEqual(2, diff_mock.call_count)

        def _return_only_insert_namespace_error(*_: Any, **__: Any) -> list[xml_diff_actions.InsertNamespace]:
            return [xml_diff_actions.InsertNamespace('', ''), object()]

        with mock.patch.object(xml_diff, 'diff_trees') as diff_mock:
            diff_mock.side_effect = _return_only_insert_namespace_error
            self.assertNotEqual(xml_structure.ExtensionLocalValue(placeholder),
                                xml_structure.ExtensionLocalValue(placeholder))
            self.assertEqual(2, diff_mock.call_count)

    def test_different_length(self):
        inst1 = xml_structure.ExtensionLocalValue(collections.OrderedDict([(mock.MagicMock(), mock.MagicMock()),
                                                                           (mock.MagicMock(), mock.MagicMock())]))
        inst2 = xml_structure.ExtensionLocalValue(collections.OrderedDict([(mock.MagicMock(), mock.MagicMock())]))
        self.assertNotEqual(inst1, inst2)

    def test_different_keys(self):
        inst1 = xml_structure.ExtensionLocalValue(collections.OrderedDict([(1, mock.MagicMock())]))
        inst2 = xml_structure.ExtensionLocalValue(collections.OrderedDict([(2, mock.MagicMock())]))
        self.assertNotEqual(inst1, inst2)

    def test_compare_non_xml(self):
        inst1 = xml_structure.ExtensionLocalValue(collections.OrderedDict([(1, '1')]))
        inst2 = xml_structure.ExtensionLocalValue(collections.OrderedDict([(1, '1')]))
        self.assertEqual(inst1, inst2)

        inst3 = xml_structure.ExtensionLocalValue(collections.OrderedDict([(1, '2')]))
        self.assertNotEqual(inst1, inst3)

    def test_ignore_comments(self):
        xml1 = lxml.etree.fromstring(b"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<ext:Extension xmlns:ext="http://standards.ieee.org/downloads/11073/11073-10207-2017/extension">
<what:ItIsNotKnown xmlns:what="123.456.789"><what:Unknown>What does this mean?</what:Unknown></what:ItIsNotKnown>
<!--This is an xml comment and should be ignored during comparison-->
</ext:Extension>""")
        xml2 = lxml.etree.fromstring(b"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<ext:Extension xmlns:ext="http://standards.ieee.org/downloads/11073/11073-10207-2017/extension">
<what:ItIsNotKnown xmlns:what="123.456.789"><what:Unknown>What does this mean?</what:Unknown></what:ItIsNotKnown>
</ext:Extension>""")
        inst1 = xml_structure.ExtensionLocalValue(collections.OrderedDict([(xml1.tag, xml1)]))
        inst2 = xml_structure.ExtensionLocalValue(collections.OrderedDict([(xml2.tag, xml2)]))
        self.assertEqual(inst1, inst2)
