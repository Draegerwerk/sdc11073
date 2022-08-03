from decimal import Decimal

from sdc11073 import isoduration


class NullConverter:
    @staticmethod
    def to_py(xml_value):
        return xml_value

    @staticmethod
    def to_xml(py_value):
        return py_value


class TimestampConverter:
    """ XML representation: integer, representing timestamp in milliseconds
     Python representation: float in seconds
    """
    @classmethod
    def to_py(cls, xml_value):
        if xml_value is None:
            return None
        return float(xml_value) / 1000.0

    @staticmethod
    def to_xml(py_value):
        ms_value = int(py_value * 1000)
        return str(ms_value)


class DecimalConverter:
    USE_DECIMAL_TYPE = True

    @classmethod
    def to_py(cls, xml_value):
        if xml_value is None:
            return None
        if cls.USE_DECIMAL_TYPE:
            return Decimal(xml_value)
        if '.' in xml_value:
            return float(xml_value)
        return int(xml_value)

    @staticmethod
    def to_xml(py_value):
        if isinstance(py_value, float):
            # round value to handle float inaccuracies
            if abs(py_value) >= 100:
                xml_value = f'{round(py_value, 1):.1f}'
            elif abs(py_value) >= 10:
                xml_value = f'{round(py_value, 2):.2f}'
            else:
                xml_value = f'{round(py_value, 3):.3f}'
        elif isinstance(py_value, Decimal):
            xml_value = str(py_value) # converting to str never returns exponential representation
            if '.' in xml_value:
                # Limit number of digits, because standard says:
                # All ·minimally conforming· processors ·must· support decimal numbers with a minimum of
                # 18 decimal digits (i.e., with a ·totalDigits· of 18).
                head, tail = xml_value.split('.')
                tail = tail[:18-len(head)]
                if tail:
                    xml_value = f'{head}.{tail}'
                else:
                    xml_value = head
        else:
            xml_value = str(py_value)
        # remove trailing zeros after decimal point
        while '.' in xml_value and xml_value[-1] in ('0', '.'):
            xml_value = xml_value[:-1]
        return xml_value


class IntegerConverter:
    @staticmethod
    def to_py(xml_value):
        if xml_value is None:
            return None
        return int(xml_value)

    @staticmethod
    def to_xml(py_value):
        return str(py_value)


class BooleanConverter:
    @staticmethod
    def to_py(xml_value):
        return xml_value == 'true'

    @staticmethod
    def to_xml(py_value):
        if py_value:
            return 'true'
        return 'false'


class DurationConverter:
    @staticmethod
    def to_py(xml_value):
        if xml_value is None:
            return None
        return isoduration.parse_duration(xml_value)

    @staticmethod
    def to_xml(py_value):
        return isoduration.duration_string(py_value)
