"""ISO 8601 duration and date / date time parsing and formatting."""

from __future__ import annotations

import datetime
import decimal
import enum
import re
import typing
from typing import TYPE_CHECKING, NamedTuple

import isodate

if TYPE_CHECKING:
    from collections.abc import Mapping


class _RegexKeys(enum.StrEnum):
    """Regex group names for date time parsing."""

    YEAR = 'year'
    MONTH = 'month'
    DAY = 'day'
    HOUR = 'hour'
    MINUTE = 'minute'
    SECOND = 'second'
    EOD = 'eod'  # end of day
    TZ_INFO = 'tz_info'
    TZ_SIGN = 'tz_sign'
    TZ_HOUR = 'tz_hour'
    TZ_MINUTE = 'tz_minute'


# https://www.w3.org/TR/xmlschema11-2/#rf-lexicalMappings-datetime
__YEAR_FRAG__ = rf'(?P<{_RegexKeys.YEAR}>-?[1-9]\d\d\d+|0\d\d\d)'
__MONTH_FRAG__ = rf'(?P<{_RegexKeys.MONTH}>0[1-9]|1[0-2])'
__DAY_FRAG__ = rf'(?P<{_RegexKeys.DAY}>0[1-9]|[12]\d|3[01])'
__HOUR_FRAG__ = rf'(?P<{_RegexKeys.HOUR}>[01]\d|2[0-3])'
__MINUTE_FRAG__ = rf'(?P<{_RegexKeys.MINUTE}>[0-5]\d)'
__SECOND_FRAG__ = rf'(?P<{_RegexKeys.SECOND}>[0-5]\d(\.\d+)?)'
__END_OF_DAY_FRAG__ = rf'(?P<{_RegexKeys.EOD}>24\:00\:00(\.0+)?)'
# allow >14:00 here but check it manually later
__TIMEZONE_FRAG__ = (
    rf'((?P<{_RegexKeys.TZ_INFO}>Z)|(?P<{_RegexKeys.TZ_SIGN}>[+-])'
    rf'((?P<{_RegexKeys.TZ_HOUR}>[0]\d|1[0-4]):(?P<{_RegexKeys.TZ_MINUTE}>[0-5]\d)))'
)

DATETIME_PATTERN: typing.Final[re.Pattern[str]] = re.compile(
    rf'^{__YEAR_FRAG__}'
    rf'(?:-{__MONTH_FRAG__}'
    rf'(?:-{__DAY_FRAG__}'
    rf'(?:T(?:{__HOUR_FRAG__}:{__MINUTE_FRAG__}:{__SECOND_FRAG__}|{__END_OF_DAY_FRAG__}))?'
    rf')?'
    rf')?'
    rf'{__TIMEZONE_FRAG__}?$',
)


DurationType = decimal.Decimal | int | float
ParsedDurationType = float


def parse_duration(date_string: str) -> ParsedDurationType:
    """Parse an ISO 8601 durations into a float value containing seconds.

    The following duration formats are supported:
      -PnnW                  duration in weeks
      -PnnYnnMnnDTnnHnnMnnS  complete duration specification
    Years and month are not supported, values must be zero!
    """
    duration = isodate.parse_duration(date_string)
    if isinstance(duration, isodate.Duration):
        msg = f'Duration {date_string} with years or months is not supported'
        raise ValueError(msg)  # noqa: TRY004
    return duration.total_seconds()


def duration_string(seconds: DurationType) -> str:
    """Create an ISO 8601 durations value containing seconds."""
    return isodate.duration_isoformat(datetime.timedelta(seconds=float(seconds)))


##### Date Time ######
class GYearMonth(NamedTuple):  # noqa: D101
    year: int
    month: int

    tzinfo: datetime.tzinfo | None = None


class GYear(NamedTuple):  # noqa: D101
    year: int

    tzinfo: datetime.tzinfo | None = None


DateTypeUnion = GYear | GYearMonth | datetime.datetime


def _parse_seconds(second_str: str) -> tuple[int, int]:
    """Parse seconds string into seconds and microseconds."""
    if '.' in second_str:
        sec_str, micro_str = second_str.split('.')
        seconds = int(sec_str)
        microseconds = int(micro_str)
    else:
        seconds = int(second_str)
        microseconds = 0
    return seconds, microseconds


def _parse_integer(value: str) -> int | None:
    return int(value) if value is not None else None


def _parse_tz(groups: Mapping[str, str]) -> datetime.timezone | None:
    tz_info = groups.get(_RegexKeys.TZ_INFO)
    if tz_info is not None:
        return datetime.UTC
    tz_sign = groups.get(_RegexKeys.TZ_SIGN)
    if tz_sign is None:
        return None
    tz_hour = groups[_RegexKeys.TZ_HOUR]
    tz_minute = groups[_RegexKeys.TZ_MINUTE]
    tz_hour = int(tz_hour)
    tz_minute = int(tz_minute)
    if tz_hour == 14 and tz_minute != 0:  # noqa: PLR2004
        msg = 'Timezone hour is 14 but minute is not zero'
        raise ValueError(msg)
    offset = tz_hour * 60 + tz_minute
    if tz_sign == '-':
        offset *= -1
    return datetime.timezone(datetime.timedelta(minutes=offset))


def parse_date_time(date_time_str: str) -> DateTypeUnion | None:
    """Parse a date time string.

    String can be xsd:dateTime, xsd:date, xsd:gYearMonth or xsd:gYear.
    """
    match = DATETIME_PATTERN.match(date_time_str)
    if match is None:
        return None
    groups = match.groupdict()
    year = int(groups['year'])
    # year is 0000 is correct xml but not applicable in python
    # https://www.w3.org/TR/xmlschema11-2/#dateTime (biceps uses xml schema v1.1)
    if year < datetime.MINYEAR or year > datetime.MAXYEAR:
        msg = f'Year {year} is out of range for datetime object {[datetime.MINYEAR, datetime.MAXYEAR]}'
        raise ValueError(msg)
    tz = _parse_tz(groups)
    month = _parse_integer(groups.get(_RegexKeys.MONTH))
    if month is None:
        return GYear(year=year, tzinfo=tz)
    day = _parse_integer(groups.get(_RegexKeys.DAY))
    if day is None:
        return GYearMonth(year=year, month=month, tzinfo=tz)
    hour = _parse_integer(groups.get(_RegexKeys.HOUR))
    minute = _parse_integer(groups.get(_RegexKeys.MINUTE))
    second = groups.get(_RegexKeys.SECOND)
    if second is None:
        microsecond = None
    else:
        second, microsecond = _parse_seconds(second)
    eod = groups.get(_RegexKeys.EOD)
    parsed = datetime.datetime(
        year=year,
        month=month,
        day=day,
        hour=hour or 0,
        minute=minute or 0,
        second=second or 0,
        microsecond=microsecond or 0,
        tzinfo=tz,
    )
    if eod is not None:
        parsed += datetime.timedelta(days=1)
    return parsed


def _tz_to_string(tz: datetime.tzinfo | None) -> str:
    if tz is None:
        return ''
    delta = tz.utcoffset(None)
    if delta is None:
        return ''
    if delta == datetime.timedelta(0):
        return 'Z'  # utc
    tz_seconds = int(delta.total_seconds())
    hours, remainder = divmod(abs(tz_seconds), 3600)
    minutes = remainder // 60
    sign = '+' if tz_seconds >= 0 else '-'
    return f'{sign}{hours:02d}:{minutes:02d}'


def date_time_string(date_object: DateTypeUnion) -> str:
    """Convert date time to str."""
    if isinstance(date_object, GYear):
        return f'{date_object.year:04d}{_tz_to_string(date_object.tzinfo)}'
    if isinstance(date_object, GYearMonth):
        return f'{date_object.year:04d}-{date_object.month:02d}{_tz_to_string(date_object.tzinfo)}'
    if isinstance(date_object, datetime.datetime):
        if date_object.time() == datetime.time():
            base = date_object.strftime('%Y-%m-%d')
        else:
            base = date_object.strftime('%Y-%m-%dT%H:%M:%S')
            if date_object.microsecond != 0:
                base += f'.{date_object.microsecond}'
        return f'{base}{_tz_to_string(date_object.tzinfo)}'
    msg = f'Unsupported date object type {type(date_object)}'
    raise TypeError(msg)
