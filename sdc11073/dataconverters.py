from sdc11073 import isoduration
from decimal import Decimal

class NullConverter(object):
    @staticmethod
    def toPy(xmlValue):
        return xmlValue
    @staticmethod
    def toXML(pyValue):
        return pyValue



class TimestampConverter(object):
    ''' XML representation: integer, representing timestamp in milliseconds
     Python representation: float in seconds
    '''
    @staticmethod
    def toPy(xmlValue):
        return float(xmlValue) / 1000.0
    @staticmethod
    def toXML(pyValue):
        ms_value = int(pyValue*1000)
        return str(ms_value)


class DecimalConverter(object):
    USE_DECIMAL_TYPE = True

    @classmethod
    def toPy(cls, xmlValue):
        if cls.USE_DECIMAL_TYPE:
            return Decimal(xmlValue)
        else:
            if '.' in xmlValue:
                return float(xmlValue)
            else:
                return int(xmlValue)

    @staticmethod
    def toXML(pyValue):
        if isinstance(pyValue, float):
            # round value to handle float inaccuracies
            if abs(pyValue) >= 100:
                xmlValue = '{:.1f}'.format(round(pyValue, 1))
            elif abs(pyValue) >= 10:
                xmlValue = '{:.2f}'.format(round(pyValue, 2))
            else:
                xmlValue = '{:.3f}'.format(round(pyValue, 3))
        elif isinstance(pyValue, Decimal):
            # assume Decimal is exact, no rounding errors
            # Decimal has no method to force string representation without exponential notion.
            # => convert to float and use :f string formatting (6 digits after decimal point, which should be good enough)
            xmlValue = f'{pyValue:f}'
        else:
            xmlValue = str(pyValue)
        # remove trailing zeros after decimal point
        while '.' in xmlValue and xmlValue[-1] in ('0', '.'):
            xmlValue = xmlValue[:-1]
        return xmlValue


class IntegerConverter(object):
    @staticmethod
    def toPy(xmlValue):
        return int(xmlValue)
    @staticmethod
    def toXML(pyValue):
        return str(pyValue)



class BooleanConverter(object):
    @staticmethod
    def toPy(xmlValue):
        return xmlValue == 'true'
    @staticmethod
    def toXML(pyValue):
        if pyValue:
            return 'true'
        else:
            return 'false'


class DurationConverter(object):
    @staticmethod
    def toPy(xmlValue):
        if xmlValue is None:
            return None
        return isoduration.parse_duration(xmlValue)
    @staticmethod
    def toXML(pyValue):
        return isoduration.durationString(pyValue)
