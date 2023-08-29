import unittest
from unittest import mock
from lxml.etree import fromstring, tostring, QName
from sdc11073 import pmtypes
from sdc11073.mdib.containerproperties import ExtensionLocalValue


class TestPmtypes(unittest.TestCase):

    def test_CodedValue(self):
        c1 = pmtypes.CodedValue(42)
        c2 = pmtypes.CodedValue(42, codingsystem='abc')
        # compare with simple string or int shall imply default coding system
        self.assertEqual(c1, 42)
        self.assertEqual(c1, '42')
        # if CodedValue does not have default coding systen, this compare shall return False
        self.assertNotEqual(c2, 42)
        # it shall be possible to compare with a Coding instance
        self.assertEqual(c1, pmtypes.Coding('42', pmtypes.DefaultCodingSystem, None))

        # if two CodedValue instances are compared, the translations shall also be handled
        c2.Translation.append(pmtypes.T_Translation(41))
        self.assertEqual(c2, 41)
        c3 = pmtypes.CodedValue(42)
        c3.Translation.append(pmtypes.T_Translation(41)) # same translation as c2
        self.assertEqual(c2, c3)

    def test_base_demographics(self):
        """Verify that Middlename is instantiated correctly as a list of strings"""
        bd = pmtypes.BaseDemographics()
        self.assertEqual(bd.Middlename, [])
        bd = pmtypes.BaseDemographics(middlenames='foo')
        self.assertEqual(bd.Middlename, ['foo'])
        bd = pmtypes.BaseDemographics(middlenames=['foo', 'bar'])
        self.assertEqual(bd.Middlename, ['foo', 'bar'])


class TestExtensions(unittest.TestCase):

    def test_compare_extensions(self):
        xml = b"""
        <pm:Identification xmlns:pm="http://standards.ieee.org/downloads/11073/11073-10207-2017/participant"
                           Root="urn:uuid:90beab82-f160-4e2f-b3b2-ed8cfcf5e205"
                           Extension="123.234.424">
            <ext:Extension xmlns:ext="__ExtensionPoint__">
                <foo someattr="somevalue"/>
                    <foo_child childattr="somechild"/>
                <bar anotherattr="differentvalue"/>
            </ext:Extension>
        </pm:Identification>
        """
        self.assertNotEqual(fromstring(xml), fromstring(xml))
        inst1 = pmtypes.InstanceIdentifier.fromNode(fromstring(xml))
        inst2 = pmtypes.InstanceIdentifier.fromNode(fromstring(xml))
        self.assertEqual(inst1.ext_Extension, inst2.ext_Extension)
        self.assertEqual(inst1, inst2)

        another_xml = b"""
                <pm:Identification xmlns:pm="http://standards.ieee.org/downloads/11073/11073-10207-2017/participant"
                                   Root="urn:uuid:90beab82-f160-4e2f-b3b2-ed8cfcf5e205"
                                   Extension="123.234.424">
                    <ext:Extension xmlns:ext="__ExtensionPoint__">
                        <foo someattr="somevalue"/>
                        <bar anotherattr="differentvalue2"/>
                    </ext:Extension>
                </pm:Identification>
                """
        inst2 = pmtypes.InstanceIdentifier.fromNode(fromstring(another_xml))
        self.assertNotEqual(inst1.ext_Extension, inst2.ext_Extension)
        self.assertNotEqual(inst1, inst2)

    def test_element_order(self):
        xml1 = b"""
        <pm:Identification xmlns:pm="http://standards.ieee.org/downloads/11073/11073-10207-2017/participant"
                           Root="urn:uuid:90beab82-f160-4e2f-b3b2-ed8cfcf5e205"
                           Extension="123.234.424">
            <ext:Extension xmlns:ext="__ExtensionPoint__">
                <foo someattr="somevalue"/>
                <bar anotherattr="differentvalue"/>
            </ext:Extension>
        </pm:Identification>
        """
        xml2 = b"""
        <pm:Identification xmlns:pm="http://standards.ieee.org/downloads/11073/11073-10207-2017/participant"
                           Root="urn:uuid:90beab82-f160-4e2f-b3b2-ed8cfcf5e205"
                           Extension="123.234.424">
            <ext:Extension xmlns:ext="__ExtensionPoint__">
                <bar anotherattr="differentvalue"/>
                <foo someattr="somevalue"/>
            </ext:Extension>
        </pm:Identification>
        """
        inst1 = pmtypes.InstanceIdentifier.fromNode(fromstring(xml1)) # noqa: S320
        inst2 = pmtypes.InstanceIdentifier.fromNode(fromstring(xml2)) # noqa: S320
        self.assertNotEqual(inst1.ext_Extension, inst2.ext_Extension)
        self.assertNotEqual(inst1, inst2)

    def test_attribute_order(self):
        xml1 = b"""
        <pm:Identification xmlns:pm="http://standards.ieee.org/downloads/11073/11073-10207-2017/participant"
                           Root="urn:uuid:90beab82-f160-4e2f-b3b2-ed8cfcf5e205"
                           Extension="123.234.424">
            <ext:Extension xmlns:ext="__ExtensionPoint__">
                <foo someattr="somevalue" anotherattr="differentvalue"/>
            </ext:Extension>
        </pm:Identification>
        """
        xml2 = b"""
        <pm:Identification xmlns:pm="http://standards.ieee.org/downloads/11073/11073-10207-2017/participant"
                           Root="urn:uuid:90beab82-f160-4e2f-b3b2-ed8cfcf5e205"
                           Extension="123.234.424">
            <ext:Extension xmlns:ext="__ExtensionPoint__">
                <foo anotherattr="differentvalue" someattr="somevalue"/>
            </ext:Extension>
        </pm:Identification>
        """
        inst1 = pmtypes.InstanceIdentifier.fromNode(fromstring(xml1)) # noqa: S320
        inst2 = pmtypes.InstanceIdentifier.fromNode(fromstring(xml2)) # noqa: S320
        self.assertEqual(inst1.ext_Extension, inst2.ext_Extension)
        self.assertEqual(inst1, inst2)


    def test_fails_with_qname(self):
        xml1 = fromstring(b"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<ext:Extension xmlns:ext="__ExtensionPoint__"
        xmlns:what="123.456.789">
        <what:ItIsNotKnown>
                <what:Unknown>what:lorem</what:Unknown>
        </what:ItIsNotKnown>
</ext:Extension>""") # noqa: S320
        xml2 = fromstring(b"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<ext:Extension xmlns:ext="__ExtensionPoint__"
        xmlns:who="123.456.789">
        <who:ItIsNotKnown>
                <who:Unknown>who:lorem</who:Unknown>
        </who:ItIsNotKnown>
</ext:Extension>""") # noqa: S320
        self.assertNotEqual(tostring(xml1), tostring(xml2))
        inst1 = ExtensionLocalValue([xml1])
        inst2 = ExtensionLocalValue([xml2])
        self.assertNotEqual(inst1, inst2)

    def test_ignore_not_needed_namespaces(self):
        xml1 = fromstring(b"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<ext:Extension xmlns:ext="__ExtensionPoint__" 
xmlns:pm="http://standards.ieee.org/downloads/11073/11073-10207-2017/participant">
<what:ItIsNotKnown xmlns:what="123.456.789"><what:Unknown>What does this mean?</what:Unknown></what:ItIsNotKnown>
</ext:Extension>""") # noqa: S320
        xml2 = fromstring(b"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<ext:Extension xmlns:ext="__ExtensionPoint__" xmlns:what="123.456.789">
<what:ItIsNotKnown><what:Unknown>What does this mean?</what:Unknown></what:ItIsNotKnown>
</ext:Extension>""") # noqa: S320
        self.assertNotEqual(tostring(xml1), tostring(xml2))
        inst1 = ExtensionLocalValue([xml1])
        inst2 = ExtensionLocalValue([xml2])
        self.assertEqual(inst1, inst2)

    def test_different_length(self):
        inst1 = ExtensionLocalValue([mock.MagicMock(), mock.MagicMock()])
        inst2 = ExtensionLocalValue([mock.MagicMock()])
        self.assertNotEqual(inst1, inst2)

    def test_ignore_comments(self):
        xml1 = fromstring(b"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<ext:Extension xmlns:ext="__ExtensionPoint__">
<what:ItIsNotKnown xmlns:what="123.456.789"><what:Unknown>What does this mean?</what:Unknown></what:ItIsNotKnown>
<!--This is an xml comment and should be ignored during comparison-->
</ext:Extension>""") # noqa: S320
        xml2 = fromstring(b"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<ext:Extension xmlns:ext="__ExtensionPoint__">
<what:ItIsNotKnown xmlns:what="123.456.789"><what:Unknown>What does this mean?</what:Unknown></what:ItIsNotKnown>
</ext:Extension>""") # noqa: S320
        inst1 = ExtensionLocalValue([xml1])
        inst2 = ExtensionLocalValue([xml2])
        self.assertEqual(inst1, inst2)

    def test_custom_compare_method(self):
        xml1 = b"""
            <ext:Extension xmlns:ext="__ExtensionPoint__">
                <foo someattr="somevalue"/>
            </ext:Extension>
        """
        xml2 = b"""
            <ext:Extension xmlns:ext="__ExtensionPoint__">
                <bar anotherattr="differentvalue"/>
            </ext:Extension>
        """
        xml1 = fromstring(xml1) # noqa: S320
        xml2 = fromstring(xml2) # noqa: S320

        inst1 = ExtensionLocalValue([xml1])
        inst2 = ExtensionLocalValue([xml2])
        self.assertNotEqual(inst1, inst2)

        def _my_comparer(_, __): # noqa: ANN001 ANN202
            return True

        orig_method = ExtensionLocalValue.compare_method
        ExtensionLocalValue.compare_method = _my_comparer
        self.assertEqual(inst1, inst2)
        ExtensionLocalValue.compare_method = orig_method

    def test_cdata(self):
        xml1 = b"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
                        <ext:Extension xmlns:ext="__ExtensionPoint__">
                        <what:ItIsNotKnown xmlns:what="123.456.789">
                        <![CDATA[<some test data & stuff>]]>
                        <what:Unknown>What does this mean?<![CDATA[Test this CDATA section]]></what:Unknown></what:ItIsNotKnown>
                        </ext:Extension>"""
        xml2 = b"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
                        <ext:Extension xmlns:ext="__ExtensionPoint__">
                        <who:ItIsNotKnown xmlns:who="123.456.789">
                        <![CDATA[<some test data & stuff>]]>
                        <who:Unknown>What does this mean?<![CDATA[Test this CDATA section]]></who:Unknown></who:ItIsNotKnown>
                        </ext:Extension>"""
        xml1 = fromstring(xml1) # noqa: S320
        xml2 = fromstring(xml2) # noqa: S320

        inst1 = ExtensionLocalValue([xml1])
        inst2 = ExtensionLocalValue([xml2])
        self.assertEqual(inst1, inst2)

    def test_comparison_subelements(self):
        xml1 = b"""
            <ext:Extension xmlns:ext="__ExtensionPoint__">
                <foo someattr="somevalue">
                    <foo_subelement subelement="value"/>
                </foo>
                <bar anotherattr="differentvalue"/>
            </ext:Extension>
        """
        xml2 = b"""
            <ext:Extension xmlns:ext="__ExtensionPoint__">
                <foo someattr="somevalue">
                    <foo_subelement subelement="fiff_value"/>
                </foo>
                <bar anotherattr="differentvalue"/>
            </ext:Extension>
        """
        xml1 = fromstring(xml1) # noqa: S320
        xml2 = fromstring(xml2) # noqa: S320

        inst1 = ExtensionLocalValue([xml1])
        inst2 = ExtensionLocalValue([xml2])
        self.assertNotEqual(inst1, inst2)

    def test_mixed_content_is_ignored(self):
        xml1 = fromstring(b"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<ext:Extension xmlns:ext="__ExtensionPoint__"
        xmlns:what="123.456.789">
        <what:ItIsNotKnown><what:Unknown>what:lorem</what:Unknown></what:ItIsNotKnown>
</ext:Extension>""") # noqa: S320
        xml2 = fromstring(b"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<ext:Extension xmlns:ext="__ExtensionPoint__"
        xmlns:what="123.456.789">
        <what:ItIsNotKnown>
        dsafasdf
        <what:Unknown>what:lorem</what:Unknown>
        </what:ItIsNotKnown>
</ext:Extension>""") # noqa: S320
        inst1 = ExtensionLocalValue([xml1])
        inst2 = ExtensionLocalValue([xml2])
        self.assertEqual(inst1, inst2)