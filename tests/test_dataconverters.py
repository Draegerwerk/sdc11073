"""Unit tests for dataconverters module."""

import unittest
from decimal import Decimal

from sdc11073.xml_types import dataconverters


class TestDataConverters(unittest.TestCase):
    def test_decimal_converter(self):
        before = dataconverters.DecimalConverter.USE_DECIMAL_TYPE
        try:
            dataconverters.DecimalConverter.USE_DECIMAL_TYPE = False
            self.assertEqual(dataconverters.DecimalConverter.to_py('123'), 123)
            self.assertEqual(dataconverters.DecimalConverter.to_py('123.45'), 123.45)

            dataconverters.DecimalConverter.USE_DECIMAL_TYPE = True
            self.assertEqual(dataconverters.DecimalConverter.to_py('123'), Decimal(123))
            self.assertEqual(dataconverters.DecimalConverter.to_py('123.450'), Decimal('123.45'))

            # to_xml method should handle floats, ints and Decimals always identically
            for use_decimal_type in (True, False):
                dataconverters.DecimalConverter.USE_DECIMAL_TYPE = use_decimal_type
                self.assertEqual(dataconverters.DecimalConverter.to_xml(42), '42')
                self.assertEqual(dataconverters.DecimalConverter.to_xml(42.1), '42.1')
                self.assertEqual(dataconverters.DecimalConverter.to_xml(Decimal('42.1')), '42.1')
                self.assertEqual(dataconverters.DecimalConverter.to_xml(Decimal('42.0')), '42')
                self.assertEqual(dataconverters.DecimalConverter.to_xml(Decimal('42.100')), '42.1')
                self.assertEqual(dataconverters.DecimalConverter.to_xml(Decimal('0E-15')), '0')
                self.assertEqual(dataconverters.DecimalConverter.to_xml(Decimal('1.23E-1')), '0.123')
        finally:
            dataconverters.DecimalConverter.USE_DECIMAL_TYPE = before  # reset flag

    def test_timestamp_converter(self):
        self.assertEqual(dataconverters.TimestampConverter.to_py('10000'), 10)
        self.assertEqual(dataconverters.TimestampConverter.to_py('10001'), 10.001)
        self.assertEqual(dataconverters.TimestampConverter.to_xml(10.0), '10000')
        self.assertEqual(dataconverters.TimestampConverter.to_xml(10), '10000')
        self.assertEqual(dataconverters.TimestampConverter.to_xml(10.001), '10001')

    def test_boolean_converter(self):
        self.assertEqual(dataconverters.BooleanConverter.to_py('true'), True)
        self.assertEqual(dataconverters.BooleanConverter.to_py('foo'), False)
        self.assertEqual(dataconverters.BooleanConverter.to_py('false'), False)
        self.assertEqual(dataconverters.BooleanConverter.to_py(''), False)
        self.assertEqual(dataconverters.BooleanConverter.to_py(None), False)
        self.assertEqual(dataconverters.BooleanConverter.to_xml(False), 'false')
        self.assertEqual(dataconverters.BooleanConverter.to_xml(None), 'false')
        self.assertEqual(dataconverters.BooleanConverter.to_xml(0), 'false')
        self.assertEqual(dataconverters.BooleanConverter.to_xml(True), 'true')
        self.assertEqual(dataconverters.BooleanConverter.to_xml(42), 'true')
