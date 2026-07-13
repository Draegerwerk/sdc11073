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

    def test_timestamp_converter_check_valid(self):
        # accepts float, int and Decimal, including zero
        for value in (0, 0.0, Decimal(0), 10, 10.001, Decimal('10.001')):
            self.assertIsNone(dataconverters.TimestampConverter.check_valid(value))
        # None is always allowed
        self.assertIsNone(dataconverters.TimestampConverter.check_valid(None))
        # wrong types are rejected
        for value in ('10', b'10', [10], object()):
            with self.assertRaises(ValueError):
                dataconverters.TimestampConverter.check_valid(value)
        # negative values are rejected
        for value in (-1, -0.001, Decimal('-1')):
            with self.assertRaises(ValueError):
                dataconverters.TimestampConverter.check_valid(value)

    def test_timestamp_converter_check_valid_disabled(self):
        # when STRICT_VALUE_CHECK is off, invalid values pass silently
        before = dataconverters.STRICT_VALUE_CHECK
        try:
            dataconverters.STRICT_VALUE_CHECK = False
            self.assertIsNone(dataconverters.TimestampConverter.check_valid('not a number'))
            self.assertIsNone(dataconverters.TimestampConverter.check_valid(-1))
        finally:
            dataconverters.STRICT_VALUE_CHECK = before

    def test_integer_converter_check_valid(self):
        # accepts int values, including negative and zero
        for value in (0, 1, -1, 1 << 40):
            self.assertIsNone(dataconverters.IntegerConverter.check_valid(value))
        # None is always allowed
        self.assertIsNone(dataconverters.IntegerConverter.check_valid(None))
        # non-int types are rejected
        for value in (1.0, Decimal(1), '1', [1], object()):
            with self.assertRaises(ValueError):
                dataconverters.IntegerConverter.check_valid(value)

    def test_integer_converter_check_valid_disabled(self):
        # when STRICT_VALUE_CHECK is off, invalid values pass silently
        before = dataconverters.STRICT_VALUE_CHECK
        try:
            dataconverters.STRICT_VALUE_CHECK = False
            self.assertIsNone(dataconverters.IntegerConverter.check_valid('not an int'))
        finally:
            dataconverters.STRICT_VALUE_CHECK = before

    def test_unsigned_int_converter_check_valid(self):
        converter = dataconverters.UnsignedIntConverter
        # accepts int values within [0, MAX]
        for value in (0, 1, 42, converter.MAX):
            self.assertIsNone(converter.check_valid(value))
        # None is always allowed
        self.assertIsNone(converter.check_valid(None))
        # negative and out-of-range ints are rejected
        for value in (-1, -(1 << 40), converter.MAX + 1):
            with self.assertRaises(ValueError):
                converter.check_valid(value)
        # non-int types are rejected (including values that are numerically in range)
        for value in (1.0, Decimal(1), '5', [1], object()):
            with self.assertRaises(ValueError):
                converter.check_valid(value)

    def test_unsigned_int_converter_check_valid_disabled(self):
        # when STRICT_VALUE_CHECK is off, invalid values pass silently
        before = dataconverters.STRICT_VALUE_CHECK
        try:
            dataconverters.STRICT_VALUE_CHECK = False
            self.assertIsNone(dataconverters.UnsignedIntConverter.check_valid(-1))
            self.assertIsNone(dataconverters.UnsignedIntConverter.check_valid('5'))
        finally:
            dataconverters.STRICT_VALUE_CHECK = before

    def test_boolean_converter_check_valid(self):
        # accepts bool values
        for value in (True, False):
            self.assertIsNone(dataconverters.BooleanConverter.check_valid(value))
        # None is always allowed
        self.assertIsNone(dataconverters.BooleanConverter.check_valid(None))
        # non-bool types are rejected, including ints (0/1 are not bools)
        for value in (0, 1, 1.0, 'true', [True], object()):
            with self.assertRaises(ValueError):
                dataconverters.BooleanConverter.check_valid(value)

    def test_boolean_converter_check_valid_disabled(self):
        # when STRICT_VALUE_CHECK is off, invalid values pass silently
        before = dataconverters.STRICT_VALUE_CHECK
        try:
            dataconverters.STRICT_VALUE_CHECK = False
            self.assertIsNone(dataconverters.BooleanConverter.check_valid(1))
        finally:
            dataconverters.STRICT_VALUE_CHECK = before

    def test_duration_converter_check_valid(self):
        # accepts int, float and Decimal values (including negative and zero)
        for value in (0, 1, -1, 1.5, Decimal('2.5')):
            self.assertIsNone(dataconverters.DurationConverter.check_valid(value))
        # None is always allowed
        self.assertIsNone(dataconverters.DurationConverter.check_valid(None))
        # non-numeric types are rejected
        for value in ('1.5', b'1', [1], object()):
            with self.assertRaises(ValueError):
                dataconverters.DurationConverter.check_valid(value)

    def test_duration_converter_check_valid_disabled(self):
        # when STRICT_VALUE_CHECK is off, invalid values pass silently
        before = dataconverters.STRICT_VALUE_CHECK
        try:
            dataconverters.STRICT_VALUE_CHECK = False
            self.assertIsNone(dataconverters.DurationConverter.check_valid('not a number'))
        finally:
            dataconverters.STRICT_VALUE_CHECK = before

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
