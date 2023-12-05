import unittest
from unittest import mock
from lxml.etree import QName, fromstring, tostring

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
        node = fromstring(text.format('')) # noqa: S320
        allowed_value1 = pm_types.AllowedValue.from_node(node)
        self.assertEqual(allowed_value1.Value, '')
        generated_node = allowed_value1.as_etree_node(QName('foo', 'bar'), {})
        self.assertEqual('', generated_node[0].text)

        node = fromstring(text.format('foobar')) # noqa: S320
        allowed_value2 = pm_types.AllowedValue.from_node(node)
        self.assertEqual(allowed_value2.Value, 'foobar')

    def test_activate_operation_descriptor_argument(self):
        """Verify that ActivateOperationDescriptorArgument is correctly instantiated from node."""
        text = """<pm:Argument xmlns:pm="http://standards.ieee.org/downloads/11073/11073-10207-2017/participant">
                      <pm:ArgName Code="202890"></pm:ArgName>
                      <pm:Arg xmlns:dd="dummy">dd:Something</pm:Arg>
                  </pm:Argument>
        """
        node = fromstring(text.format('')) # noqa: S320
        arg = pm_types.ActivateOperationDescriptorArgument.from_node(node)
        self.assertEqual(arg.ArgName, pm_types.CodedValue("202890"))
        self.assertEqual(arg.Arg, QName("dummy", "Something"))
        # verify that as_etree_node -> from_node conversion creates an identical arg
        node2 = arg.as_etree_node(
            QName("http://standards.ieee.org/downloads/11073/11073-10207-2017/participant", 'Argument'),
            ns_map={"pm": "http://standards.ieee.org/downloads/11073/11073-10207-2017/participant"})
        arg2 = pm_types.ActivateOperationDescriptorArgument.from_node(node2)
        self.assertEqual(arg, arg2)


class TestExtensions(unittest.TestCase):

    def test_compare_extensions(self):
        xml = b"""
        <pm:Identification xmlns:pm="http://standards.ieee.org/downloads/11073/11073-10207-2017/participant"
                           Root="urn:uuid:90beab82-f160-4e2f-b3b2-ed8cfcf5e205"
                           Extension="123.234.424">
            <ext:Extension xmlns:ext="http://standards.ieee.org/downloads/11073/11073-10207-2017/extension">
                <foo someattr="somevalue"/>
                    <foo_child childattr="somechild"/>
                <bar anotherattr="differentvalue"/>
            </ext:Extension>
        </pm:Identification>
        """
        self.assertNotEqual(fromstring(xml), fromstring(xml))  # noqa: S320
        inst1 = pm_types.InstanceIdentifier.from_node(fromstring(xml)) # noqa: S320
        inst2 = pm_types.InstanceIdentifier.from_node(fromstring(xml)) # noqa: S320
        self.assertEqual(inst1.ExtExtension, inst2.ExtExtension)
        self.assertEqual(inst1, inst2)
        self.assertEqual(inst1.ExtExtension, tuple(inst2.ExtExtension))

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
        inst2 = pm_types.InstanceIdentifier.from_node(fromstring(another_xml)) # noqa: S320
        self.assertNotEqual(inst1.ExtExtension, inst2.ExtExtension)
        self.assertNotEqual(inst1, inst2)

    def test_compare_extension_with_other_types(self):
        xml1 = b"""
            <ext:Extension xmlns:ext="http://standards.ieee.org/downloads/11073/11073-10207-2017/extension">
                <foo someattr="somevalue"/>
            </ext:Extension>
        """
        xml1 = fromstring(xml1)  # noqa: S320

        inst1 = xml_structure.ExtensionLocalValue([xml1])
        self.assertFalse(inst1 == 42)
        self.assertFalse(inst1 == [41])

    def test_element_order(self):
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
        inst1 = pm_types.InstanceIdentifier.from_node(fromstring(xml1)) # noqa: S320
        inst2 = pm_types.InstanceIdentifier.from_node(fromstring(xml2)) # noqa: S320
        self.assertNotEqual(inst1.ExtExtension, inst2.ExtExtension)
        self.assertNotEqual(inst1, inst2)

    def test_attribute_order(self):
        xml1 = b"""
        <pm:Identification xmlns:pm="http://standards.ieee.org/downloads/11073/11073-10207-2017/participant"
                           Root="urn:uuid:90beab82-f160-4e2f-b3b2-ed8cfcf5e205"
                           Extension="123.234.424">
            <ext:Extension xmlns:ext="http://standards.ieee.org/downloads/11073/11073-10207-2017/extension">
                <foo someattr="somevalue" anotherattr="differentvalue"/>
            </ext:Extension>
        </pm:Identification>
        """
        xml2 = b"""
        <pm:Identification xmlns:pm="http://standards.ieee.org/downloads/11073/11073-10207-2017/participant"
                           Root="urn:uuid:90beab82-f160-4e2f-b3b2-ed8cfcf5e205"
                           Extension="123.234.424">
            <ext:Extension xmlns:ext="http://standards.ieee.org/downloads/11073/11073-10207-2017/extension">
                <foo anotherattr="differentvalue" someattr="somevalue"/>
            </ext:Extension>
        </pm:Identification>
        """
        inst1 = pm_types.InstanceIdentifier.from_node(fromstring(xml1)) # noqa: S320
        inst2 = pm_types.InstanceIdentifier.from_node(fromstring(xml2)) # noqa: S320
        self.assertEqual(inst1.ExtExtension, inst2.ExtExtension)
        self.assertEqual(inst1, inst2)


    def test_fails_with_qname(self):
        xml1 = fromstring(b"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<ext:Extension xmlns:ext="http://standards.ieee.org/downloads/11073/11073-10207-2017/extension"
        xmlns:what="123.456.789">
        <what:ItIsNotKnown>
                <what:Unknown>what:lorem</what:Unknown>
        </what:ItIsNotKnown>
</ext:Extension>""") # noqa: S320
        xml2 = fromstring(b"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<ext:Extension xmlns:ext="http://standards.ieee.org/downloads/11073/11073-10207-2017/extension"
        xmlns:who="123.456.789">
        <who:ItIsNotKnown>
                <who:Unknown>who:lorem</who:Unknown>
        </who:ItIsNotKnown>
</ext:Extension>""") # noqa: S320
        self.assertNotEqual(tostring(xml1), tostring(xml2))
        inst1 = xml_structure.ExtensionLocalValue([xml1])
        inst2 = xml_structure.ExtensionLocalValue([xml2])
        self.assertNotEqual(inst1, inst2)

    def test_ignore_not_needed_namespaces(self):
        xml1 = fromstring(b"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<ext:Extension xmlns:ext="http://standards.ieee.org/downloads/11073/11073-10207-2017/extension" 
xmlns:pm="http://standards.ieee.org/downloads/11073/11073-10207-2017/participant">
<what:ItIsNotKnown xmlns:what="123.456.789"><what:Unknown>What does this mean?</what:Unknown></what:ItIsNotKnown>
</ext:Extension>""") # noqa: S320
        xml2 = fromstring(b"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<ext:Extension xmlns:ext="http://standards.ieee.org/downloads/11073/11073-10207-2017/extension" xmlns:what="123.456.789">
<what:ItIsNotKnown><what:Unknown>What does this mean?</what:Unknown></what:ItIsNotKnown>
</ext:Extension>""") # noqa: S320
        self.assertNotEqual(tostring(xml1), tostring(xml2))
        inst1 = xml_structure.ExtensionLocalValue([xml1])
        inst2 = xml_structure.ExtensionLocalValue([xml2])
        self.assertEqual(inst1, inst2)

    def test_different_length(self):
        inst1 = xml_structure.ExtensionLocalValue([mock.MagicMock(), mock.MagicMock()])
        inst2 = xml_structure.ExtensionLocalValue([mock.MagicMock()])
        self.assertNotEqual(inst1, inst2)

    def test_ignore_comments(self):
        xml1 = fromstring(b"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<ext:Extension xmlns:ext="http://standards.ieee.org/downloads/11073/11073-10207-2017/extension">
<what:ItIsNotKnown xmlns:what="123.456.789"><what:Unknown>What does this mean?</what:Unknown></what:ItIsNotKnown>
<!--This is an xml comment and should be ignored during comparison-->
</ext:Extension>""") # noqa: S320
        xml2 = fromstring(b"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<ext:Extension xmlns:ext="http://standards.ieee.org/downloads/11073/11073-10207-2017/extension">
<what:ItIsNotKnown xmlns:what="123.456.789"><what:Unknown>What does this mean?</what:Unknown></what:ItIsNotKnown>
</ext:Extension>""") # noqa: S320
        inst1 = xml_structure.ExtensionLocalValue([xml1])
        inst2 = xml_structure.ExtensionLocalValue([xml2])
        self.assertEqual(inst1, inst2)

    def test_custom_compare_method(self):
        xml1 = b"""
            <ext:Extension xmlns:ext="http://standards.ieee.org/downloads/11073/11073-10207-2017/extension">
                <foo someattr="somevalue"/>
            </ext:Extension>
        """
        xml2 = b"""
            <ext:Extension xmlns:ext="http://standards.ieee.org/downloads/11073/11073-10207-2017/extension">
                <bar anotherattr="differentvalue"/>
            </ext:Extension>
        """
        xml1 = fromstring(xml1) # noqa: S320
        xml2 = fromstring(xml2) # noqa: S320

        inst1 = xml_structure.ExtensionLocalValue([xml1])
        inst2 = xml_structure.ExtensionLocalValue([xml2])
        self.assertNotEqual(inst1, inst2)

        def _my_comparer(_, __): # noqa: ANN001 ANN202
            return True

        orig_method = xml_structure.ExtensionLocalValue.compare_method
        xml_structure.ExtensionLocalValue.compare_method = _my_comparer
        self.assertEqual(inst1, inst2)
        xml_structure.ExtensionLocalValue.compare_method = orig_method

    def test_cdata(self):
        xml1 = b"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
                        <ext:Extension xmlns:ext="http://standards.ieee.org/downloads/11073/11073-10207-2017/extension">
                        <what:ItIsNotKnown xmlns:what="123.456.789">
                        <![CDATA[<some test data & stuff>]]>
                        <what:Unknown>What does this mean?<![CDATA[Test this CDATA section]]></what:Unknown></what:ItIsNotKnown>
                        </ext:Extension>"""
        xml2 = b"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
                        <ext:Extension xmlns:ext="http://standards.ieee.org/downloads/11073/11073-10207-2017/extension">
                        <who:ItIsNotKnown xmlns:who="123.456.789">
                        <![CDATA[<some test data & stuff>]]>
                        <who:Unknown>What does this mean?<![CDATA[Test this CDATA section]]></who:Unknown></who:ItIsNotKnown>
                        </ext:Extension>"""
        xml1 = fromstring(xml1) # noqa: S320
        xml2 = fromstring(xml2) # noqa: S320

        inst1 = xml_structure.ExtensionLocalValue([xml1])
        inst2 = xml_structure.ExtensionLocalValue([xml2])
        self.assertEqual(inst1, inst2)

    def test_comparison_subelements(self):
        xml1 = b"""
            <ext:Extension xmlns:ext="http://standards.ieee.org/downloads/11073/11073-10207-2017/extension">
                <foo someattr="somevalue">
                    <foo_subelement subelement="value"/>
                </foo>
                <bar anotherattr="differentvalue"/>
            </ext:Extension>
        """
        xml2 = b"""
            <ext:Extension xmlns:ext="http://standards.ieee.org/downloads/11073/11073-10207-2017/extension">
                <foo someattr="somevalue">
                    <foo_subelement subelement="fiff_value"/>
                </foo>
                <bar anotherattr="differentvalue"/>
            </ext:Extension>
        """
        xml1 = fromstring(xml1) # noqa: S320
        xml2 = fromstring(xml2) # noqa: S320

        inst1 = xml_structure.ExtensionLocalValue([xml1])
        inst2 = xml_structure.ExtensionLocalValue([xml2])
        self.assertNotEqual(inst1, inst2)

    def test_assign_iterable(self):
        xml1 = b"""
        <pm:Identification xmlns:pm="http://standards.ieee.org/downloads/11073/11073-10207-2017/participant"
                           Root="urn:uuid:90beab82-f160-4e2f-b3b2-ed8cfcf5e205"
                           Extension="123.234.424">
            <ext:Extension xmlns:ext="http://standards.ieee.org/downloads/11073/11073-10207-2017/extension">
                <foo someattr="somevalue" anotherattr="differentvalue"/>
            </ext:Extension>
        </pm:Identification>
        """
        xml2 = b"""
        <pm:Identification xmlns:pm="http://standards.ieee.org/downloads/11073/11073-10207-2017/participant"
                           Root="urn:uuid:90beab82-f160-4e2f-b3b2-ed8cfcf5e205"
                           Extension="123.234.424">
            <ext:Extension xmlns:ext="http://standards.ieee.org/downloads/11073/11073-10207-2017/extension">
                <bar someattr="somevalue"/>
            </ext:Extension>
        </pm:Identification>
        """
        xml3 = b"""
        <pm:Identification xmlns:pm="http://standards.ieee.org/downloads/11073/11073-10207-2017/participant"
                           Root="urn:uuid:90beab82-f160-4e2f-b3b2-ed8cfcf5e205"
                           Extension="123.234.424">
        </pm:Identification>
        """

        inst1 = pm_types.InstanceIdentifier.from_node(fromstring(xml1)) # noqa: S320
        inst2 = pm_types.InstanceIdentifier.from_node(fromstring(xml2)) # noqa: S320
        inst3 = pm_types.InstanceIdentifier.from_node(fromstring(xml3)) # noqa: S320
        self.assertNotEqual(inst1.ExtExtension, inst2.ExtExtension)
        self.assertEqual(len(inst3.ExtExtension), 0)
        # assign a tuple with values from inst1 to inst3
        inst3.ExtExtension = tuple(inst1.ExtExtension)
        self.assertTrue(isinstance(inst3.ExtExtension, xml_structure.ExtensionLocalValue))
        self.assertEqual(inst1.ExtExtension, inst3.ExtExtension)
        # assign a generator with values from inst2 to inst3
        def my_generator():
            yield from inst2.ExtExtension
        inst3.ExtExtension = my_generator()
        self.assertEqual(inst2.ExtExtension, inst3.ExtExtension)


    def test_mixed_content_is_ignored(self):
        xml1 = fromstring(b"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<ext:Extension xmlns:ext="http://standards.ieee.org/downloads/11073/11073-10207-2017/extension"
        xmlns:what="123.456.789">
        <what:ItIsNotKnown><what:Unknown>what:lorem</what:Unknown></what:ItIsNotKnown>
</ext:Extension>""") # noqa: S320
        xml2 = fromstring(b"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<ext:Extension xmlns:ext="http://standards.ieee.org/downloads/11073/11073-10207-2017/extension"
        xmlns:what="123.456.789">
        <what:ItIsNotKnown>
        dsafasdf
        <what:Unknown>what:lorem</what:Unknown>
        </what:ItIsNotKnown>
</ext:Extension>""") # noqa: S320
        inst1 = xml_structure.ExtensionLocalValue([xml1])
        inst2 = xml_structure.ExtensionLocalValue([xml2])
        self.assertEqual(inst1, inst2)
