from __future__ import annotations

from decimal import Decimal
from typing import Protocol, Any

from . import isoduration

STRICT_VALUE_CHECK = True


class DataConverterProtocol(Protocol):
    def to_py(self, xml_value: str):
        ...

    def to_xml(self, py_value: Any) -> str:
        ...

    def check_valid(self, py_value: Any):
        ...

    def elem_to_py(self, xml_value: str) -> Any:
        ...


class NullConverter:
    @staticmethod
    def to_py(xml_value):
        return xml_value

    @staticmethod
    def to_xml(py_value):
        return py_value

    @staticmethod
    def check_valid(py_value):
        pass

    @staticmethod
    def elem_to_py(xml_value):
        return xml_value


class ClassCheckConverter(NullConverter):
    """No conversion, only type checking"""

    def __init__(self, *klass):
        self._klass = klass

    def check_valid(self, py_value):
        if STRICT_VALUE_CHECK and py_value is not None:
            for cls in self._klass:
                if isinstance(py_value, cls):
                    return
            raise ValueError(f'Value can only be {[cls.__name__ for cls in self._klass]}, got {type(py_value)}')


class EnumConverter(NullConverter):
    """
    Converts between enums and strings
    """

    def __init__(self, klass):
        self._klass = klass

    def to_py(self, xml_value):
        value = self._klass(xml_value)
        return value

    def to_xml(self, py_value):
        return py_value.value if hasattr(py_value, 'value') else py_value

    def check_valid(self, py_value) -> bool:
        if STRICT_VALUE_CHECK and py_value is not None:
            if not isinstance(py_value, self._klass):
                raise ValueError(f'Value can only be {self._klass.__name__}, got {type(py_value)}')


class StringConverter(NullConverter):
    """Convert None to empty string, everything else is unchanged."""

    @staticmethod
    def to_py(xml_value):
        return xml_value or ''

    @staticmethod
    def check_valid(py_value) -> bool:
        if STRICT_VALUE_CHECK and py_value is not None:
            if not isinstance(py_value, str):
                raise ValueError(f'Value can only be str, got {type(py_value)}')


class ListConverter(NullConverter):
    """Each element in list is checked and converted with provided element_converter."""

    def __init__(self, element_converter):
        if not hasattr(element_converter, 'check_valid'):
            raise TypeError
        self._element_converter = element_converter

    def check_valid(self, py_value) -> bool:
        if STRICT_VALUE_CHECK and py_value is not None:
            if not isinstance(py_value, list):
                raise ValueError(f'Value must be an instance of {type(list)}, got {type(py_value)}')
            for elem in py_value:
                self._element_converter.check_valid(elem)

    def to_py(self):
        raise NotImplementedError

    def to_xml(self):
        raise NotImplementedError

    def elem_to_py(self, xml_value):
        return self._element_converter.to_py(xml_value)

    def elem_to_xml(self, py_value):
        return self._element_converter.to_xml(py_value)


class TimestampConverter(NullConverter):
    """BICEPS Timestamp.

     XML representation: integer, representing timestamp in milliseconds
     Python representation: float in seconds
    """

    @classmethod
    def to_py(cls, xml_value: str) -> float | None:
        if xml_value is None:
            return None
        return int(xml_value) / 1000

    @staticmethod
    def to_xml(py_value) -> str:
        return str(int(py_value * 1000))

    @staticmethod
    def check_valid(py_value):
        if STRICT_VALUE_CHECK and py_value is not None:
            if not isinstance(py_value, (float, int, Decimal)):
                raise ValueError(f'Timestamp can only be float, integer or Decimal, got {type(py_value)}')
            if py_value < 0:
                raise ValueError(f'Timestamp can only have positive values, got {py_value}')


class DecimalConverter(NullConverter):
    USE_DECIMAL_TYPE = True

    @classmethod
    def to_py(cls, xml_value: str) -> Decimal | int | float:
        if xml_value is None:
            return None
        if cls.USE_DECIMAL_TYPE:
            return Decimal(xml_value)
        if '.' in xml_value:
            return float(xml_value)
        return int(xml_value)

    @staticmethod
    def _float_to_xml(py_value: Decimal | int | float) -> str:
        # round value to handle float inaccuracies
        if abs(py_value) >= 100:
            xml_value = f'{round(py_value, 1):.1f}'
        elif abs(py_value) >= 10:
            xml_value = f'{round(py_value, 2):.2f}'
        else:
            xml_value = f'{round(py_value, 3):.3f}'
        return xml_value

    @classmethod
    def _decimal_to_xml(cls, py_value):
        xml_value = str(py_value)
        if 'E' in xml_value or 'e' in xml_value:
            # no exp form allowed in xml
            return cls._float_to_xml(float(py_value))
        return xml_value

    @classmethod
    def to_xml(cls, py_value):
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
            tail = tail[:18 - len(head)]
            if tail:
                xml_value = f'{head}.{tail}'
            else:
                xml_value = head
            # remove trailing zeros after decimal point
            while '.' in xml_value and xml_value[-1] in ('0', '.'):
                xml_value = xml_value[:-1]
        return xml_value

    @staticmethod
    def check_valid(py_value):
        if STRICT_VALUE_CHECK and py_value is not None:
            if not isinstance(py_value, Decimal):
                raise ValueError(f'expected a decimal, got {type(py_value)}')


class IntegerConverter(NullConverter):
    @staticmethod
    def to_py(xml_value: str) -> int:
        if xml_value is None:
            return None
        return int(xml_value)

    @staticmethod
    def to_xml(py_value: int) -> str:
        return str(py_value)

    @staticmethod
    def check_valid(py_value):
        if STRICT_VALUE_CHECK and py_value is not None:
            if not isinstance(py_value, int):
                raise ValueError(f'expected an integer, got {type(py_value)}')


class UnsignedIntConverter(IntegerConverter):
    MAX = 1 << 32

    @classmethod
    def check_valid(cls, py_value):
        if STRICT_VALUE_CHECK and py_value is not None:
            if not (isinstance(py_value, int) or py_value < 0 or py_value > cls.MAX):
                raise ValueError(f'expected an unsigned integer, got {type(py_value)} value={py_value}')


class UnsignedLongConverter(IntegerConverter):
    MAX = 1 << 64


class BooleanConverter(NullConverter):
    @staticmethod
    def to_py(xml_value: str) -> bool:
        return xml_value in ('true', '1')

    @staticmethod
    def to_xml(py_value: bool) -> str:
        if py_value:
            return 'true'
        return 'false'

    @staticmethod
    def check_valid(py_value):
        if STRICT_VALUE_CHECK and py_value is not None:
            if not isinstance(py_value, bool):
                raise ValueError(f'expected a boolean, got {type(py_value)}')


class DurationConverter(NullConverter):
    @staticmethod
    def to_py(xml_value: str)-> isoduration.ParsedDurationType | None:
        if xml_value is None:
            return None
        return isoduration.parse_duration(xml_value)

    @staticmethod
    def to_xml(py_value: isoduration.DurationType) -> str:
        return isoduration.duration_string(py_value)

    @staticmethod
    def check_valid(py_value: Any):
        if STRICT_VALUE_CHECK and py_value is not None:
            if not isinstance(py_value, (int, float, Decimal)):
                raise ValueError(f'expected a boolean, got {type(py_value)}')
