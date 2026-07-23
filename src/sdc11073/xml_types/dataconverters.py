"""Converters that translate between XML string representations and Python values."""

from __future__ import annotations

from decimal import Decimal
from typing import Any, NoReturn, Protocol

from sdc11073.xml_types import isoduration

STRICT_VALUE_CHECK = True


class DataConverterProtocol(Protocol):
    """Protocol for converters between XML string values and Python values."""

    def to_py(self, xml_value: str) -> Any:
        """Convert a single XML string value to its Python representation.

        :param xml_value: the XML string value.
        :return: the Python representation.
        """

    def to_xml(self, py_value: Any) -> str:
        """Convert a Python value to its XML string representation.

        :param py_value: the Python value.
        :return: the XML string representation.
        """

    def check_valid(self, py_value: Any) -> None:
        """Verify that the Python value is valid for this converter.

        :param py_value: the Python value to check.
        """

    def elem_to_py(self, xml_value: str) -> Any:
        """Convert a single list element from XML to its Python representation.

        :param xml_value: the XML string value of the element.
        :return: the Python representation of the element.
        """


class NullConverter:
    """Pass-through converter that performs no conversion and no validation."""

    @staticmethod
    def to_py(xml_value: str) -> str:
        """Return the XML value unchanged.

        :param xml_value: the XML string value.
        :return: the unchanged value.
        """
        return xml_value

    @staticmethod
    def to_xml(py_value: str) -> str:
        """Return the Python value unchanged.

        :param py_value: the Python value.
        :return: the unchanged value.
        """
        return py_value

    @staticmethod
    def check_valid(py_value: Any) -> None:
        """Accept any value without validation.

        :param py_value: the Python value to check.
        """

    @staticmethod
    def elem_to_py(xml_value: str) -> str:
        """Return the list element unchanged.

        :param xml_value: the XML string value of the element.
        :return: the unchanged value.
        """
        return xml_value


class ClassCheckConverter(NullConverter):
    """No conversion, only type checking."""

    def __init__(self, *klass: type) -> None:
        """Store the classes that values are checked against.

        :param klass: the classes that valid values may be instances of.
        """
        self._klass = klass

    def check_valid(self, py_value: Any) -> None:
        """Verify that the value is an instance of one of the configured classes.

        :param py_value: the Python value to check.
        """
        if STRICT_VALUE_CHECK and py_value is not None and not isinstance(py_value, self._klass):
            msg = f'Value can only be {[cls.__name__ for cls in self._klass]}, got {type(py_value)}'
            raise ValueError(msg)


class EnumConverter(NullConverter):
    """Convert between enums and strings."""

    def __init__(self, klass: type) -> None:
        """Store the enum class used for conversion.

        :param klass: the enum class.
        """
        self._klass = klass

    def to_py(self, xml_value: str) -> Any:
        """Convert an XML string value to an enum member.

        :param xml_value: the XML string value.
        :return: the corresponding enum member.
        """
        return self._klass(xml_value)

    def to_xml(self, py_value: Any) -> str:
        """Convert an enum member to its XML string value.

        :param py_value: the enum member (or plain value).
        :return: the XML string value.
        """
        return py_value.value if hasattr(py_value, 'value') else py_value

    def check_valid(self, py_value: Any) -> None:
        """Verify that the value is an instance of the configured enum class.

        :param py_value: the Python value to check.
        """
        if STRICT_VALUE_CHECK and py_value is not None:
            if not isinstance(py_value, self._klass):
                msg = f'Value can only be {self._klass.__name__}, got {type(py_value)}'
                raise ValueError(msg)


class StringConverter(NullConverter):
    """Convert an XML string to a python string and None to an empty string."""

    @staticmethod
    def to_py(xml_value: str | None) -> str:
        """Convert an XML string value to a Python string, mapping None to an empty string.

        :param xml_value: the XML string value or None.
        :return: the string value.
        """
        return xml_value or ''

    @staticmethod
    def check_valid(py_value: Any) -> None:
        """Verify that the value is a string.

        :param py_value: the Python value to check.
        """
        if STRICT_VALUE_CHECK and py_value is not None:
            if not isinstance(py_value, str):
                msg = f'Value can only be str, got {type(py_value)}'
                raise ValueError(msg)


class ListConverter(NullConverter):
    """Convert list elements from and to XML."""

    def __init__(self, element_converter: DataConverterProtocol) -> None:
        """Store the converter used for each list element.

        :param element_converter: the converter applied to individual elements.
        """
        if not hasattr(element_converter, 'check_valid'):
            raise TypeError
        self._element_converter = element_converter

    def check_valid(self, py_value: Any) -> None:
        """Verify that the value is a list and that each element is valid.

        :param py_value: the Python value to check.
        """
        if STRICT_VALUE_CHECK and py_value is not None:
            if not isinstance(py_value, list):
                msg = f'Value must be an instance of {type(list)}, got {type(py_value)}'
                raise ValueError(msg)
            for elem in py_value:
                self._element_converter.check_valid(elem)

    def to_py(self) -> Any:
        """Raise NotImplementedError; use :meth:`elem_to_py` for list elements."""
        raise NotImplementedError

    def to_xml(self) -> Any:
        """Raise NotImplementedError; use :meth:`elem_to_xml` for list elements."""
        raise NotImplementedError

    def elem_to_py(self, xml_value: str) -> Any:
        """Convert a single list element from XML to its Python representation.

        :param xml_value: the XML string value of the element.
        :return: the Python representation of the element.
        """
        return self._element_converter.to_py(xml_value)

    def elem_to_xml(self, py_value: Any) -> str:
        """Convert a single list element from Python to its XML representation.

        :param py_value: the Python value of the element.
        :return: the XML string representation of the element.
        """
        return self._element_converter.to_xml(py_value)


class TimestampConverter(NullConverter):
    """BICEPS Timestamp.

    XML representation: integer, representing timestamp in milliseconds
    Python representation: float in seconds
    """

    @classmethod
    def to_py(cls, xml_value: str) -> float | None:
        """Convert an XML timestamp in milliseconds to seconds.

        :param xml_value: the XML string value in milliseconds, or None.
        :return: the timestamp in seconds, or None.
        """
        if xml_value is None:
            return None
        return int(xml_value) / 1000

    @staticmethod
    def to_xml(py_value: float | Decimal) -> str:
        """Convert a timestamp in seconds to an XML string in milliseconds.

        :param py_value: the timestamp in seconds.
        :return: the XML string value in milliseconds.
        """
        return str(int(py_value * 1000))

    @staticmethod
    def check_valid(py_value: Any) -> None:
        """Verify that the value is a non-negative number.

        :param py_value: the Python value to check.
        """
        if STRICT_VALUE_CHECK and py_value is not None:
            if not isinstance(py_value, (float, int, Decimal)):
                msg = f'Timestamp can only be float, integer or Decimal, got {type(py_value)}'
                raise ValueError(msg)
            if py_value < 0:
                msg = f'Timestamp can only have positive values, got {py_value}'
                raise ValueError(msg)


class DecimalConverter(NullConverter):
    """Convert between XML decimal strings and Python Decimal (or float/int) values."""

    USE_DECIMAL_TYPE = True

    @classmethod
    def to_py(cls, xml_value: str) -> Decimal | int | float | None:
        """Convert an XML decimal string to a Python numeric value.

        :param xml_value: the XML string value, or None.
        :return: a Decimal (or float/int) value, or None.
        """
        if xml_value is None:
            return None
        if cls.USE_DECIMAL_TYPE:
            return Decimal(xml_value)
        if '.' in xml_value:
            return float(xml_value)
        return int(xml_value)

    @staticmethod
    def _float_to_xml(py_value: float) -> str:
        # round value to handle float inaccuracies
        if abs(py_value) >= 100:  # noqa: PLR2004 - order-of-magnitude threshold selecting decimal precision
            xml_value = f'{round(py_value, 1):.1f}'
        elif abs(py_value) >= 10:  # noqa: PLR2004 - order-of-magnitude threshold selecting decimal precision
            xml_value = f'{round(py_value, 2):.2f}'
        else:
            xml_value = f'{round(py_value, 3):.3f}'
        return xml_value

    @classmethod
    def _decimal_to_xml(cls, py_value: Decimal) -> str:
        xml_value = str(py_value)
        if 'E' in xml_value or 'e' in xml_value:
            # no exp form allowed in xml
            return cls._float_to_xml(float(py_value))
        return xml_value

    @classmethod
    def to_xml(cls, py_value: Decimal | float) -> str:
        """Convert a Python numeric value to an XML decimal string.

        :param py_value: the Decimal, float or int value.
        :return: the XML string representation, without exponent form.
        """
        if isinstance(py_value, float):
            xml_value = cls._float_to_xml(py_value)
        elif isinstance(py_value, Decimal):
            xml_value = cls._decimal_to_xml(py_value)
        else:
            xml_value = str(py_value)

        if '.' in xml_value:
            # Limit number of digits, because standard says:
            # All ·minimally conforming· processors ·must· support decimal numbers with a minimum of
            # 18 decimal digits (i.e., with a ·totalDigits· of 18).
            head, tail = xml_value.split('.')
            tail = tail[: 18 - len(head)]
            xml_value = f'{head}.{tail}' if tail else head
            # remove trailing zeros after decimal point
            while '.' in xml_value and xml_value[-1] in ('0', '.'):
                xml_value = xml_value[:-1]
        return xml_value

    @staticmethod
    def check_valid(py_value: Any) -> None:
        """Verify that the value is a Decimal.

        :param py_value: the Python value to check.
        """
        if STRICT_VALUE_CHECK and py_value is not None:
            if not isinstance(py_value, Decimal):
                msg = f'expected a decimal, got {type(py_value)}'
                raise ValueError(msg)


class IntegerConverter(NullConverter):
    """Convert between XML integer strings and Python int values."""

    @staticmethod
    def to_py(xml_value: str) -> int | None:
        """Convert an XML integer string to a Python int.

        :param xml_value: the XML string value, or None.
        :return: the int value, or None.
        """
        if xml_value is None:
            return None
        return int(xml_value)

    @staticmethod
    def to_xml(py_value: int) -> str:
        """Convert a Python int to an XML integer string.

        :param py_value: the int value.
        :return: the XML string representation.
        """
        return str(py_value)

    @staticmethod
    def check_valid(py_value: Any) -> None:
        """Verify that the value is an integer.

        :param py_value: the Python value to check.
        """
        if STRICT_VALUE_CHECK and py_value is not None:
            if not isinstance(py_value, int):
                msg = f'expected an integer, got {type(py_value)}'
                raise ValueError(msg)


class UnsignedIntConverter(IntegerConverter):
    """Convert between XML strings and Python 32-bit unsigned int values."""

    MAX = 1 << 32

    @classmethod
    def check_valid(cls, py_value: Any) -> None:
        """Verify that the value is a non-negative integer within the 32-bit range.

        :param py_value: the Python value to check.
        """
        if STRICT_VALUE_CHECK and py_value is not None:
            if not (isinstance(py_value, int) and 0 <= py_value <= cls.MAX):
                msg = f'expected an unsigned integer, got {type(py_value)} value={py_value}'
                raise ValueError(msg)


class UnsignedLongConverter(IntegerConverter):
    """Convert between XML strings and Python 64-bit unsigned int values."""

    MAX = 1 << 64


class BooleanConverter(NullConverter):
    """Convert between XML boolean strings and Python bool values."""

    @staticmethod
    def to_py(xml_value: str) -> bool:
        """Convert an XML boolean string to a Python bool.

        :param xml_value: the XML string value.
        :return: True if the value is ``'true'`` or ``'1'``, otherwise False.
        """
        return xml_value in ('true', '1')

    @staticmethod
    def to_xml(py_value: bool) -> str:
        """Convert a Python bool to an XML boolean string.

        :param py_value: the bool value.
        :return: ``'true'`` or ``'false'``.
        """
        if py_value:
            return 'true'
        return 'false'

    @staticmethod
    def check_valid(py_value: Any) -> None:
        """Verify that the value is a boolean.

        :param py_value: the Python value to check.
        """
        if STRICT_VALUE_CHECK and py_value is not None:
            if not isinstance(py_value, bool):
                msg = f'expected a boolean, got {type(py_value)}'
                raise ValueError(msg)


class DurationConverter(NullConverter):
    """Convert between XML ISO 8601 duration strings and Python duration values."""

    @staticmethod
    def to_py(xml_value: str) -> isoduration.ParsedDurationType | None:
        """Convert an XML ISO 8601 duration string to a Python duration.

        :param xml_value: the XML string value, or None.
        :return: the parsed duration, or None.
        """
        if xml_value is None:
            return None
        return isoduration.parse_duration(xml_value)

    @staticmethod
    def to_xml(py_value: isoduration.DurationType) -> str:
        """Convert a Python duration to an XML ISO 8601 duration string.

        :param py_value: the duration value.
        :return: the XML string representation.
        """
        return isoduration.duration_string(py_value)

    @staticmethod
    def check_valid(py_value: Any) -> None:
        """Verify that the value is a number.

        :param py_value: the Python value to check.
        """
        if STRICT_VALUE_CHECK and py_value is not None:
            if not isinstance(py_value, (int, float, Decimal)):
                msg = f'expected a number, got {type(py_value)}'
                raise ValueError(msg)
