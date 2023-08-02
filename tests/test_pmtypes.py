import unittest

from lxml.etree import fromstring, QName

from sdc11073.xml_types import pm_types


class TestPmTypes(unittest.TestCase):

    def test_CodedValue(self):
        c1 = pm_types.CodedValue('42')
        c2 = pm_types.CodedValue('42', coding_system='abc')
        # compare with simple string or int shall return False
        self.assertFalse(c1.equals(42))
        self.assertFalse(c1.equals('42'))
        # if CodedValue does not have default coding system, this compare shall return False
        self.assertFalse(c2.equals(42))
        # it shall be possible to compare with a Coding instance
        self.assertTrue(c1.equals(pm_types.Coding('42', pm_types.DEFAULT_CODING_SYSTEM, None)))

        # if two CodedValue instances are compared, the translations shall also be handled
        c2.Translation.append(pm_types.T_Translation('41'))
        self.assertNotEqual(c2, 41)
        c3 = pm_types.CodedValue('42')
        c3.Translation.append(pm_types.T_Translation('41'))  # same translation as c2
        self.assertTrue(c2.equals(c3))

    def test_have_matching_codes(self):
        c1 = pm_types.CodedValue('42', coding_system='abc')
        c1.Translation.append(pm_types.T_Translation('41'))
        self.assertTrue(pm_types.have_matching_codes(c1, pm_types.Coding('42', coding_system='abc')))
        self.assertTrue(pm_types.have_matching_codes(c1, pm_types.Coding('41')))
        self.assertFalse(pm_types.have_matching_codes(c1, pm_types.Coding('41', coding_system='abc')))

        c2 = pm_types.CodedValue('xxx', coding_system='abc')
        c2.Translation.append(pm_types.T_Translation('41'))
        self.assertTrue(pm_types.have_matching_codes(c1, c2))

    def test_allowed_value(self):
        """Verify that value is an empty string if text of Value node is empty."""
        text = """<pm:AllowedValue xmlns:pm="http://standards.ieee.org/downloads/11073/11073-10207-2017/participant">
                <pm:Value>{}</pm:Value>
                <pm:Type Code="202890">
                </pm:Type>
              </pm:AllowedValue>
"""
        node = fromstring(text.format(''))
        allowed_value1 = pm_types.AllowedValue.from_node(node)
        self.assertEqual(allowed_value1.Value, '')
        generated_node = allowed_value1.as_etree_node(QName('foo', 'bar'), {})
        self.assertEqual('', generated_node[0].text )

        node = fromstring(text.format('foobar'))
        allowed_value2 = pm_types.AllowedValue.from_node(node)
        self.assertEqual(allowed_value2.Value, 'foobar')

    def test_activate_operation_descriptor_argument(self):
        """Verify that ActivateOperationDescriptorArgument is correctly insyantiated from node"""
        text = """<pm:Argument xmlns:pm="http://standards.ieee.org/downloads/11073/11073-10207-2017/participant">
                      <pm:ArgName Code="202890"></pm:ArgName>
                      <pm:Arg xmlns:dd="dummy">dd:Something</pm:Arg>
                  </pm:Argument>
        """
        node = fromstring(text.format(''))
        arg = pm_types.ActivateOperationDescriptorArgument.from_node(node)
        self.assertEqual(arg.ArgName, pm_types.CodedValue("202890"))
        self.assertEqual(arg.Arg, QName("dummy", "Something"))

