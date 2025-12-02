"""ISO 8601 duration and date / date time parsing and formatting."""

from __future__ import annotations

import dataclasses
import datetime
import decimal
import enum
import io
import re
import typing
from typing import TYPE_CHECKING

import isodate

if TYPE_CHECKING:
    from collections.abc import Mapping


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
__YEAR_FRAG__ = rf'(?P<{_RegexKeys.YEAR}>-?(?:[1-9]\d\d\d+|0\d\d\d))'
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

MAX_MONTH: typing.Final[int] = 12
MAX_DAY: typing.Final[int] = 31
MAX_HOUR: typing.Final[int] = 23
MAX_MINUTE: typing.Final[int] = 59
MAX_SECOND: typing.Final[float] = 60.0  # due to floats, this max value is exclusive


@dataclasses.dataclass(frozen=True)
class XsdDatetime:
    """xsd:gYear, xsd:gYearMonth, xsd:date and xsd:dateTime."""

    year: int
    month: int | None
    day: int | None
    hour: int | None
    minute: int | None
    second: float | None
    end_of_day: bool = False
    tz_info: datetime.tzinfo | None = None

    def __validate_date(self):
        if self.month is not None and not (1 <= self.month <= MAX_MONTH):
            msg = f'{self.month} is not a valid month'
            raise ValueError(msg)
        if self.day is not None:
            if not (1 <= self.day <= MAX_DAY):
                msg = f'{self.day} is not a valid day'
                raise ValueError(msg)
            if self.month is None:
                raise ValueError('day cannot be present without month')

    def __validate_time(self):
        if self.hour is not None:
            if not (0 <= self.hour <= MAX_HOUR):
                msg = f'{self.hour} is not a valid hour'
                raise ValueError(msg)
            if self.day is None or self.minute is None or self.second is None:
                raise ValueError('hour cannot be present without day, minute and second')
        if self.minute is not None:
            if not (0 <= self.minute <= MAX_MINUTE):
                msg = f'{self.minute} is not a valid minute'
                raise ValueError(msg)
            if self.hour is None or self.second is None:
                raise ValueError('minute cannot be present without hour and second')
        if self.second is not None:
            if not (0.0 <= self.second < MAX_SECOND):
                msg = f'{self.second} is not a valid second'
                raise ValueError(msg)
            if self.hour is None or self.minute is None:
                raise ValueError('second cannot be present without hour and minute')

    def __validate_eod(self):
        if self.end_of_day:
            if self.hour is not None or self.minute is not None or self.second is not None:
                raise ValueError('end_of_day cannot be true if hour, minute or second is present')
            if self.day is None:
                raise ValueError('end_of_day cannot be true if day is not present')

    def __post_init__(self):
        self.__validate_date()
        self.__validate_time()
        self.__validate_eod()

    def __str__(self) -> str:
        """Convert date time to str."""
        parsed = io.StringIO()
        sign = '-' if self.year < 0 else ''
        parsed.write(f'{sign}{abs(self.year):04d}')  # ensure that the year is at least 4 digits (without the sign)
        if self.month is not None:
            parsed.write(f'-{self.month:02d}')
        if self.day is not None:
            parsed.write(f'-{self.day:02d}')
        if self.end_of_day:
            parsed.write('T24:00:00')
        elif self.hour is not None and self.minute is not None and self.second is not None:
            parsed.write(f'T{self.hour:02d}:{self.minute:02d}:')
            # ensure that all decimal places are present (e.g. prevent scientific notation)
            s = format(decimal.Decimal(repr(self.second)), 'f').rstrip('0').rstrip('.')
            parsed.write(f'0{s}' if self.second < 10.0 else s)  # noqa: PLR2004

        parsed.write(_tz_to_string(self.tz_info))
        return parsed.getvalue()


def _parse_integer(value: str) -> int | None:
    return int(value) if value is not None else None


def _parse_float(value: str) -> int | None:
    return float(value) if value is not None else None


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


def parse_date_time(date_time_str: str) -> XsdDatetime | None:
    """Parse a date time string.

    String can be xsd:dateTime, xsd:date, xsd:gYearMonth or xsd:gYear.
    """
    match = DATETIME_PATTERN.match(date_time_str)
    if match is None:
        return None
    groups = match.groupdict()
    year = int(groups['year'])
    tz = _parse_tz(groups)
    month = _parse_integer(groups.get(_RegexKeys.MONTH))
    day = _parse_integer(groups.get(_RegexKeys.DAY))
    hour = _parse_integer(groups.get(_RegexKeys.HOUR))
    minute = _parse_integer(groups.get(_RegexKeys.MINUTE))
    second = _parse_float(groups.get(_RegexKeys.SECOND))
    return XsdDatetime(
        year=year,
        month=month,
        day=day,
        hour=hour,
        minute=minute,
        second=second,
        end_of_day=groups.get(_RegexKeys.EOD) is not None,
        tz_info=tz,
    )


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
