import unittest
from decimal import Decimal
from sdc11073 import dataconverters

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
        self.assertEqual(dataconverters.BooleanConverter.toPy('foo'), False)
        self.assertEqual(dataconverters.BooleanConverter.toPy('false'), False)
        self.assertEqual(dataconverters.BooleanConverter.toPy(''), False)
        self.assertEqual(dataconverters.BooleanConverter.toPy(None), False)
        self.assertEqual(dataconverters.BooleanConverter.toXML(False), 'false')
        self.assertEqual(dataconverters.BooleanConverter.toXML(None), 'false')
        self.assertEqual(dataconverters.BooleanConverter.toXML(0), 'false')
        self.assertEqual(dataconverters.BooleanConverter.toXML(True), 'true')
        self.assertEqual(dataconverters.BooleanConverter.toXML(42), 'true')
