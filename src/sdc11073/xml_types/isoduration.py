"""ISO 8601 duration and date / date time parsing and formatting."""

from __future__ import annotations

import datetime
import re
from decimal import Decimal
from typing import NamedTuple, Union

ISO8601_PERIOD_REGEX = re.compile(
    r'^(?P<sign>[+-])?'
    r'P(?!\b)'
    r'(?P<years>[0-9]+([,.][0-9]+)?Y)?'
    r'(?P<months>[0-9]+([,.][0-9]+)?M)?'
    r'(?P<weeks>[0-9]+([,.][0-9]+)?W)?'
    r'(?P<days>[0-9]+([,.][0-9]+)?D)?'
    r'((?P<separator>T)(?P<hours>[0-9]+([,.][0-9]+)?H)?'
    r'(?P<minutes>[0-9]+([,.][0-9]+)?M)?'
    r'(?P<seconds>[0-9]+([,.][0-9]+)?S)?)?$',
)

# regular expression to parse ISO duration strings.

DurationType = Union[Decimal, int, float]
ParsedDurationType = Union[int, float]


def parse_duration(date_string: str) -> ParsedDurationType:
    """Parse an ISO 8601 durations into a float value containing seconds.

    The following duration formats are supported:
      -PnnW                  duration in weeks
      -PnnYnnMnnDTnnHnnMnnS  complete duration specification
    Years and month are not supported, values must be zero!
    """
    if not isinstance(date_string, str):
        msg = f'Expecting a string {date_string}'
        raise TypeError(msg)
    match = ISO8601_PERIOD_REGEX.match(date_string)
    if not match:
        msg = f'Unable to parse duration string {date_string}'
        raise ValueError(msg)
    groups = match.groupdict()
    for key, val in groups.items():
        if key not in ('separator', 'sign'):
            if val is None:
                groups[key] = '0n'
            if key in ('years', 'months'):
                groups[key] = Decimal(groups[key][:-1].replace(',', '.'))
            else:
                # these values are passed into a timedelta object,
                # which works with floats.
                groups[key] = float(groups[key][:-1].replace(',', '.'))
    if groups['years'] != 0 or groups['months'] != 0:
        msg = f'Unable to parse duration string {date_string} (Non zero year or month)'
        raise ValueError(msg)
    ret = datetime.timedelta(
        days=groups['days'],
        hours=groups['hours'],
        minutes=groups['minutes'],
        seconds=groups['seconds'],
        weeks=groups['weeks'],
    )
    if groups['sign'] == '-':
        ret = datetime.timedelta(0) - ret
    return ret.total_seconds()


def duration_string(seconds: DurationType) -> str:
    """Create an ISO 8601 durations value containing seconds."""
    sign = '-' if seconds < 0 else ''
    fraction = abs(seconds - int(seconds))
    seconds = abs(int(seconds))
    minutes, sec = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    days, hours = divmod(hours, 24)
    if fraction == 0:
        fraction_string = ''
    else:
        fraction_string = f'{fraction:.9f}'[1:]  # starting from dot char
        while fraction_string[-1] == '0':
            fraction_string = fraction_string[:-1]
    if days == 0:
        return f'{sign}PT{hours}H{minutes}M{sec}{fraction_string}S'
    return f'{sign}P0Y0M{days}DT{hours}H{minutes}M{sec}{fraction_string}S'


##### Date Time ######
class GYearMonth(NamedTuple):  # noqa: D101
    year: int
    month: int


class GYear(NamedTuple):  # noqa: D101
    year: int


# regular expression to parse ISO 8601 date / datetime strings.

_TZ_REGEX_STR = '(?P<tz>Z|((?P<tz_sign>[+-])(?P<tz_hours>[0-9]{1,2}):(?P<tz_minutes>[0-9]{1,2})))?'
_DATE_REGEX_STR = '(?P<year>[0-9]{4})-?(?P<month>1[0-2]|0[1-9])-?(?P<day>[0-9]{1,2})'
_TIME_REGEX_STR = '(?P<hour>2[0-3]|[01][0-9]):?(?P<minute>[0-5][0-9]):(?P<second>[0-5][0-9]([.][0-9]+)?)'
_DATETIME_REGEX = re.compile('^' + _DATE_REGEX_STR + '(T' + _TIME_REGEX_STR + _TZ_REGEX_STR + ')?')
_DATETIME_REGEX_RELAXED = re.compile(
    '^' + _DATE_REGEX_STR + '([T, ]' + _TIME_REGEX_STR + _TZ_REGEX_STR + ')?',
)  # allows space between date and time
_year_month_regex = re.compile('^(?P<year>[0-9]{4})(-(?P<month>1[0-2]|0[1-9]))?$')

DateTypeUnion = Union[GYear, GYearMonth, datetime.date, datetime.datetime]


def parse_date_time(date_time_str: str, strict: bool = True) -> DateTypeUnion | None:
    """Parse a date time string.

    String can be  xsd:dateTime, xsd:date, xsd:gYearMonth or xsd:gYear.
    """
    d_t = _DATETIME_REGEX.match(date_time_str) if strict else _DATETIME_REGEX_RELAXED.match(date_time_str)
    if d_t is not None:
        groups = d_t.groupdict()
        year, month, day = int(groups['year']), int(groups['month']), int(groups['day'])
        # year is 0000 is correct xml but not applicable in python
        # https://www.w3.org/TR/xmlschema11-2/#dateTime (biceps uses xml schema v1.1)
        if year < datetime.MINYEAR or year > datetime.MAXYEAR:
            msg = f'Year {year} is out of range for datetime object {[datetime.MINYEAR, datetime.MAXYEAR]}'
            raise ValueError(msg)
        if groups['hour'] is None:  # only a date, no time
            return datetime.date(year, month, day)

        tz_1st = groups['tz']
        tz_info = None
        if tz_1st is not None:
            if tz_1st == 'Z':
                tz_info = datetime.timezone.utc
            elif tz_1st[0] in ('+', '-'):
                tz_hours = int(groups['tz_hours'])
                tz_minutes = int(groups['tz_minutes'])
                offset = tz_hours * 60 + tz_minutes
                if tz_1st[0] == '-':
                    offset *= -1
                tz_info = datetime.timezone(datetime.timedelta(minutes=offset), 'unknown')

        hour = int(groups['hour'])
        minute = int(groups.get('minute', '00'))
        second = float(groups.get('second', '0.0'))
        sec, micro_sec = int(second), int((second - int(second)) * 1000000)
        return datetime.datetime(year, month, day, hour, minute, sec, micro_sec, tz_info)

    d_t = _year_month_regex.match(date_time_str)
    if d_t is not None:
        groups = d_t.groupdict()
        year, month = groups['year'], groups['month']
        return GYear(int(year)) if month is None else GYearMonth(int(year), int(month))

    return None


def _mk_seconds_string(date_object: DateTypeUnion) -> str:
    if date_object.microsecond > 0:
        seconds = float(date_object.second) + float(date_object.microsecond) / 1e6
        seconds_string = f'{seconds:06.03f}'
        # remove trailing zeros
        while seconds_string[-1] == '0':
            seconds_string = seconds_string[:-1]
    else:
        seconds_string = f'{date_object.second:02d}'
    return seconds_string


def _mk_tz_string(date_object: datetime.datetime) -> str:
    tz_string = ''
    delta = date_object.utcoffset()
    if delta is not None:
        tz_seconds = delta.seconds + (3600 * 24) * delta.days
        if tz_seconds == 0:
            tz_string = 'Z'
        if tz_seconds != 0:
            minutes, _ = divmod(abs(tz_seconds), 60)
            hours, minutes = divmod(minutes, 60)
            sign = '+' if tz_seconds > 0 else '-'
            tz_string = f'{sign}{hours:02d}:{minutes:02d}'
    return tz_string


def date_time_string(date_object: DateTypeUnion) -> str:
    """Convert date time to str."""
    if hasattr(date_object, 'hour'):  # datetime object
        date_string = (
            f'{date_object.year:4d}-{date_object.month:02d}-{date_object.day:02d}T{date_object.hour:02d}:'
            f'{date_object.minute:02d}:{_mk_seconds_string(date_object)}{_mk_tz_string(date_object)}'
        )
    elif hasattr(date_object, 'day'):  # date object
        date_string = f'{date_object.year:4d}-{date_object.month:02d}-{date_object.day:02d}'
    elif hasattr(date_object, 'month'):  # GYearMonth object
        date_string = f'{date_object.year:4d}-{date_object.month:02d}'
    elif hasattr(date_object, 'year'):  # GYear object
        date_string = f'{date_object.year:4d}'
    else:
        msg = f'cannot convert {date_object.__class__.__name__} to ISO8601 datetime string'
        raise ValueError(msg)
    return date_string
