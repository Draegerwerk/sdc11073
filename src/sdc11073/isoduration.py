import re
from datetime import timedelta
from datetime import datetime, date, tzinfo
from decimal import Decimal
from collections import namedtuple

ISO8601_PERIOD_REGEX = re.compile(
    r"^(?P<sign>[+-])?"
    r"P(?!\b)"
    r"(?P<years>[0-9]+([,.][0-9]+)?Y)?"
    r"(?P<months>[0-9]+([,.][0-9]+)?M)?"
    r"(?P<weeks>[0-9]+([,.][0-9]+)?W)?"
    r"(?P<days>[0-9]+([,.][0-9]+)?D)?"
    r"((?P<separator>T)(?P<hours>[0-9]+([,.][0-9]+)?H)?"
    r"(?P<minutes>[0-9]+([,.][0-9]+)?M)?"
    r"(?P<seconds>[0-9]+([,.][0-9]+)?S)?)?$")
# regular expression to parse ISO duration strings.


def parse_duration(datestring):
    """
    Parses an ISO 8601 durations into a float value containing seconds.
    The following duration formats are supported:
      -PnnW                  duration in weeks
      -PnnYnnMnnDTnnHnnMnnS  complete duration specification
    Years and month are not supported, values must be zero! 
    """
    if not isinstance(datestring, str):
        raise TypeError("Expecting a string %r" % datestring)
    match = ISO8601_PERIOD_REGEX.match(datestring)
    if not match:
        raise ValueError("Unable to parse duration string %r" % datestring)
    groups = match.groupdict()
    for key, val in groups.items():
        if key not in ('separator', 'sign'):
            if val is None:
                groups[key] = "0n"
            # print groups[key]
            if key in ('years', 'months'):
                groups[key] = Decimal(groups[key][:-1].replace(',', '.'))
            else:
                # these values are passed into a timedelta object,
                # which works with floats.
                groups[key] = float(groups[key][:-1].replace(',', '.'))
    if groups["years"] != 0 or groups["months"] != 0:
        raise ValueError("Unable to parse duration string %r (Non zero year or month)" % datestring)
    else:
        ret = timedelta(days=groups["days"], hours=groups["hours"],
                        minutes=groups["minutes"], seconds=groups["seconds"],
                        weeks=groups["weeks"])
        if groups["sign"] == '-':
            ret = timedelta(0) - ret
        return ret.total_seconds()


def durationString(seconds):
    sign = '-' if seconds < 0 else ''
    fract = abs(seconds - int(seconds))
    seconds = abs(int(seconds))
    minutes, sec = divmod(seconds,60)
    hours, minutes= divmod(minutes,60)
    days, hours = divmod(hours, 24)
#    sec += fract
    if fract == 0:
        fract_string = ''
    else:
        fract_string = f'{fract:.9f}'[1:] # starting from dot char
        while fract_string[-1] == '0':
            fract_string = fract_string[:-1]
    if days == 0:
        return '{}PT{}H{}M{}{}S'.format(sign, hours, minutes, sec, fract_string)
    else:
        return '{}P0Y0M{}DT{}H{}M{}{}S'.format(sign, days, hours, minutes, sec, fract_string)


##### Date Time ######
GYearMonth = namedtuple('GYearMonth', 'year month')
GYear = namedtuple('GYear', 'year')

ZERO = timedelta(0)
class UTC(tzinfo):
    """Fixed offset in minutes east from UTC."""
    def __init__(self, offset_minutes, tzname=None):
        self._offset = timedelta(minutes=offset_minutes)
        self._tzname = tzname

    def utcoffset(self, dt): #pylint:disable=unused-argument
        return self._offset

    def tzname(self, dt): #pylint:disable=unused-argument
        return self._tzname

    def dst(self, dt): #pylint:disable=unused-argument
        return ZERO


# regular expression to parse ISO 8601 date / datetime strings.

_tz_regex_str = '(?P<tz>Z|((?P<tz_sign>[+-])(?P<tz_hours>[0-9]{1,2}):(?P<tz_minutes>[0-9]{1,2})))?'
_date_regex_str = '(?P<year>[0-9]{4})-?(?P<month>1[0-2]|0[1-9])-?(?P<day>[0-9]{1,2})'
_time_regex_str = '(?P<hour>2[0-3]|[01][0-9]):?(?P<minute>[0-5][0-9]):(?P<second>[0-5][0-9]([.][0-9]+)?)'
_datetime_regex = re.compile('^'+_date_regex_str + '(T' + _time_regex_str + _tz_regex_str + ')?')
_datetime_regex_relaxed = re.compile('^'+_date_regex_str + '([T, ]' + _time_regex_str + _tz_regex_str + ')?') # allows space between date and time
_year_month_regex = re.compile('^(?P<year>[0-9]{4})(-(?P<month>1[0-2]|0[1-9]))?')


def parse_date_time(date_time_string, strict=True):
    try:
        if strict:
            d = _datetime_regex.match(date_time_string)
        else:
            d = _datetime_regex_relaxed.match(date_time_string)
        if d is not None:
            groups = d.groupdict()
            year, month, day = int(groups['year']), int(groups['month']), int(groups['day'])
            if groups['hour'] is None: # only a date, no time
                return date(year, month, day)

            tz_1st = groups['tz']
            if tz_1st is None:
                tz_info = None
            elif tz_1st == 'Z':
                tz_info = UTC(0, 'UTC')
            elif tz_1st[0] in ('+', '-'):
                tz_hours = int(groups['tz_hours'])
                tz_minutes = int(groups['tz_minutes'])
                offset = tz_hours*60+tz_minutes
                if tz_1st[0] == '-':
                    offset *= -1
                tz_info = UTC(offset, 'unknown')

            hour = int(groups['hour'])
            minute = int(groups.get('minute', '00'))
            second = float(groups.get('second', '0.0'))
            sec, microsec = int(second), int((second - int(second))*1000000)
            value = datetime(year, month, day, hour, minute, sec, microsec, tz_info)
            return value

        d = _year_month_regex.match(date_time_string)
        if d is not None:
            groups = d.groupdict()
            year, month = groups['year'], groups['month']
            if month is None:
                value = GYear(int(year))
            else:
                value = GYearMonth(int(year), int(month))
            return value

        raise ValueError('Could not parse date string = "{}"'.format(date_time_string))
    except ValueError:
        return None


def _mkSecondsString(date_object):
    if date_object.microsecond > 0:
        seconds = float(date_object.second) + float(date_object.microsecond)/1e6
        secondsString = '{:06.03f}'.format(seconds)
        #remove trailing zeros
        while secondsString[-1] == '0':
            secondsString = secondsString[:-1]
    else:
        secondsString = '{:02d}'.format(date_object.second)
    return secondsString

def _mkTzString(date_object):
    tz_string = ''
    if date_object.tzinfo:
        delta = date_object.tzinfo.utcoffset(0)
        tz_seconds = delta.seconds + (3600 * 24) * delta.days
        if tz_seconds == 0:
            tz_string = 'Z'
        if tz_seconds != 0:
            minutes, sec = divmod(abs(tz_seconds), 60)
            hours, minutes = divmod(minutes, 60)
            tz_string = '{}{:02d}:{:02d}'.format('+' if tz_seconds > 0 else '-', hours, minutes)
    return tz_string

def date_time_string(date_object):
    if hasattr(date_object, 'hour'): # datetime object
        datestring = '{:4d}-{:02d}-{:02d}T{:02d}:{:02d}:{}{}'.format(date_object.year,
                                                                     date_object.month,
                                                                     date_object.day,
                                                                     date_object.hour,
                                                                     date_object.minute,
                                                                     _mkSecondsString(date_object),
                                                                     _mkTzString(date_object))
    elif hasattr(date_object, 'day'): #date object
        datestring = '{:4d}-{:02d}-{:02d}'.format(date_object.year,
                                                  date_object.month,
                                                  date_object.day)
    elif hasattr(date_object, 'month'): #GYearMonth object
        datestring = '{:4d}-{:02d}'.format(date_object.year, date_object.month)
    elif hasattr(date_object, 'year'): #GYear object
        datestring = '{:4d}'.format(date_object.year)
    else:
        raise ValueError('cannot convert {} to ISO8601 datetime string'.format(date_object.__class__.__name__))
    return datestring