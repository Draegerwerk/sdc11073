from sdc11073 import isoduration


class NullConverter(object):
    @staticmethod
    def toPy(xmlValue):
        return xmlValue
    @staticmethod
    def toXML(pyValue):
        return pyValue



class TimestampConverter(object):
    ''' XML representation: integer, representing timestamo in milliseconds
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
    @staticmethod
    def toPy(xmlValue):
        if '.' in xmlValue:
            return float(xmlValue)
        else:
            return int(xmlValue)
        
    @staticmethod
    def toXML(pyValue):
        """ This method rounds the decimal value to a reasonable precision. """
        if isinstance(pyValue, float):
            if abs(pyValue) >=100:
                xmlValue = '{:.1f}'.format(round(pyValue, 1))
            elif abs(pyValue) >=10:
                xmlValue = '{:.2f}'.format(round(pyValue, 2))
            else:
                xmlValue = '{:.3f}'.format(round(pyValue, 3))
            while '.' in xmlValue and xmlValue[-1] in ('0', '.'):
                xmlValue = xmlValue[:-1]
        else:
            xmlValue = str(pyValue)
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
