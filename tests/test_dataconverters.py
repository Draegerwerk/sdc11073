import unittest
from decimal import Decimal
from lxml import etree as etree_
from sdc11073 import dataconverters
from sdc11073.mdib.containerproperties import NodeAttributeListProperty

class TestDataConverters(unittest.TestCase):

    def test_decimal_converter(self):
        try:
            before = dataconverters.DecimalConverter.USE_DECIMAL_TYPE
            dataconverters.DecimalConverter.USE_DECIMAL_TYPE = False
            self.assertEqual(dataconverters.DecimalConverter.toPy('123'), 123)
            self.assertEqual(dataconverters.DecimalConverter.toPy('123.45'), 123.45)

            dataconverters.DecimalConverter.USE_DECIMAL_TYPE = True
            self.assertEqual(dataconverters.DecimalConverter.toPy('123'), Decimal('123'))
            self.assertEqual(dataconverters.DecimalConverter.toPy('123.450'), Decimal('123.45'))

            # toXML method should handle floats, ints and Decimals always identically
            for use_decimal_type in (True, False):
                dataconverters.DecimalConverter.USE_DECIMAL_TYPE = use_decimal_type
                self.assertEqual(dataconverters.DecimalConverter.toXML(42), '42')
                self.assertEqual(dataconverters.DecimalConverter.toXML(42.1), '42.1')
                self.assertEqual(dataconverters.DecimalConverter.toXML(Decimal('42.1')), '42.1')
                self.assertEqual(dataconverters.DecimalConverter.toXML(Decimal('42.0')), '42')
                self.assertEqual(dataconverters.DecimalConverter.toXML(Decimal('42.100')), '42.1')
        finally:
            dataconverters.DecimalConverter.USE_DECIMAL_TYPE = before # reset flag

    def test_timestamp_converter(self):
        self.assertEqual(dataconverters.TimestampConverter.toPy('10000'), 10)
        self.assertEqual(dataconverters.TimestampConverter.toPy('10001'), 10.001)
        self.assertEqual(dataconverters.TimestampConverter.toXML(10.0), '10000')
        self.assertEqual(dataconverters.TimestampConverter.toXML(10), '10000')
        self.assertEqual(dataconverters.TimestampConverter.toXML(10.001), '10001')

    def test_boolean_converter(self):
        self.assertEqual(dataconverters.BooleanConverter.toPy('true'), True)
        self.assertEqual(dataconverters.BooleanConverter.toPy('false'), False)
        self.assertEqual(dataconverters.BooleanConverter.toPy('1'), True)
        self.assertEqual(dataconverters.BooleanConverter.toPy('0'), False)
        self.assertEqual(dataconverters.BooleanConverter.toPy(' true '), True)
        self.assertEqual(dataconverters.BooleanConverter.toPy('\rfalse'), False)
        self.assertEqual(dataconverters.BooleanConverter.toPy(' \r  1 \n'), True)
        self.assertEqual(dataconverters.BooleanConverter.toPy('\t0'), False)
        self.assertEqual(dataconverters.BooleanConverter.toXML(True), 'true')
        self.assertEqual(dataconverters.BooleanConverter.toXML(42), 'true')
        self.assertEqual(dataconverters.BooleanConverter.toXML(0), 'false')
        self.assertEqual(dataconverters.BooleanConverter.toXML(False), 'false')
        self.assertEqual(dataconverters.BooleanConverter.toXML(None), 'false')

        for invalid in ['foo', " ", "2", " 42 ", "tr ue", '\u00a0true']:
            with self.assertRaises(ValueError):
                dataconverters.BooleanConverter.toPy(invalid)

        for invalid in [1, 0, None]:
            with self.assertRaises(AttributeError):
                dataconverters.BooleanConverter.toPy(invalid)


class TestNodeAttributeListProperty(unittest.TestCase):

    def test_get_py_value_from_node(self):
        prop = NodeAttributeListProperty('Foo')
        for xml_value, expected in [('a', ['a']),
                                    ('a b c', ['a', 'b', 'c']),
                                    ('  a   b  ', ['a', 'b']),
                                    ('\ta\r\nb\n\n\tc\r', ['a', 'b', 'c']),
                                    ('', []),
                                    ('\n', []),
                                    ('\r', []),
                                    ('\t', []),
                                    (' \t\r\n ', []),
                                    ('a b', ['a b']),  # non-breaking space is no xml whitespace
                                    (' \u00a0true \u00a0false\n none', ['\u00a0true', '\u00a0false', 'none']),
                                    ]:
            node = etree_.Element('Node')
            node.set('Foo', xml_value)
            self.assertEqual(prop.getPyValueFromNode(node), expected, msg=repr(xml_value))

        # missing attribute returns default value
        self.assertIsNone(prop.getPyValueFromNode(etree_.Element('Node')))

    def test_get_py_value_from_sub_node(self):
        sub_name = etree_.QName('Sub')
        prop = NodeAttributeListProperty('Foo', subElementNames=[sub_name])
        node = etree_.Element('Node')
        sub_node = etree_.SubElement(node, sub_name)
        sub_node.set('Foo', ' a\tb ')
        self.assertEqual(prop.getPyValueFromNode(node), ['a', 'b'])

        # missing sub element returns default value
        self.assertIsNone(prop.getPyValueFromNode(etree_.Element('Node')))
