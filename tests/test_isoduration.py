"""Unit tests for isoduration module."""

import datetime
import decimal
import re
import sys
from typing import Literal

import pytest
from hypothesis import assume, given
from hypothesis import strategies as st

from sdc11073.xml_types import isoduration


@given(st.timedeltas(max_value=datetime.timedelta(microseconds=-1)))
def test_negative_durations_are_not_allowed(timedelta: datetime.timedelta) -> None:
    """Test that negative durations raise ValueError."""
    with pytest.raises(ValueError, match='Negative durations are not supported'):
        isoduration.duration_string(timedelta.total_seconds())


@given(second=st.floats(min_value=0, allow_nan=False, allow_infinity=False))
def test_duration_parsing(second: float) -> None:
    """Test that durations can be converted to string and back."""
    duration_string = isoduration.duration_string(second)
    duration_seconds = isoduration.parse_duration(duration_string)
    if duration_seconds != second:
        print(duration_seconds)
    assert duration_seconds == second


@pytest.mark.parametrize('duration', ['P1Y2M', '-P3Y', 'P0Y5M', 'P2Y0M', 'P1Y2M3DT4H5M6S'])
def test_parse_duration_raises_value_error_for_years_months(duration: str) -> None:
    """Test that durations with years or months raise ValueError."""
    with pytest.raises(ValueError, match=f'Duration {duration} with years or months is not supported'):
        isoduration.parse_duration(duration)


@st.composite
def timezones(draw: st.DrawFn) -> datetime.tzinfo:
    timezone = draw(st.timezones())
    delta = timezone.utcoffset(None)
    if delta is None:
        return datetime.UTC if sys.version_info >= (3, 11) else datetime.timezone.utc
    return datetime.timezone(datetime.timedelta(seconds=delta.total_seconds()))


@st.composite
def times(draw: st.DrawFn) -> tuple[int, int, float]:
    hour = draw(hours())
    minute = draw(minutes())
    second = draw(seconds())
    return hour, minute, second


@st.composite
def years(draw: st.DrawFn) -> int:
    return draw(st.integers())


@st.composite
def months(draw: st.DrawFn) -> int:
    return draw(st.integers(min_value=1, max_value=isoduration.MAX_MONTH))


@st.composite
def days(draw: st.DrawFn) -> int:
    return draw(st.integers(min_value=1, max_value=isoduration.MAX_DAY))


@st.composite
def hours(draw: st.DrawFn) -> int:
    return draw(st.integers(min_value=0, max_value=isoduration.MAX_HOUR))


@st.composite
def minutes(draw: st.DrawFn) -> int:
    return draw(st.integers(min_value=0, max_value=isoduration.MAX_MINUTE))


@st.composite
def seconds(draw: st.DrawFn) -> float:
    return float(
        format(
            decimal.Decimal(
                repr(
                    draw(
                        st.floats(
                            min_value=0,
                            max_value=isoduration.MAX_SECOND,
                            allow_nan=False,
                            allow_infinity=False,
                            exclude_max=True,
                        ),
                    ),
                ),
            ),
            'f',
        ),
    )


@st.composite
def xsd_datetimes(draw: st.DrawFn) -> isoduration.XsdDatetime:
    year = draw(years())
    tz = draw(st.none() | timezones())
    month = draw(st.none() | months())
    day = draw(st.none() | days()) if month is not None else None
    if day is not None:
        eod = draw(st.booleans())
        if eod:
            hour = None
            minute = None
            second = None
        else:
            hour, minute, second = draw(times())
    else:
        eod = False
        hour = None
        minute = None
        second = None

    return isoduration.XsdDatetime(
        year=year,
        month=month,
        day=day,
        hour=hour,
        minute=minute,
        second=second,
        end_of_day=eod,
        tz_info=tz,
    )


@given(dt=xsd_datetimes())
def test_date_time(dt: isoduration.XsdDatetime) -> None:
    """Test that XsdDatetime string representation can be parsed back."""
    assert dt == isoduration.parse_date_time(str(dt))


def _make_datetime(**overrides: str | float | bool | datetime.tzinfo) -> isoduration.XsdDatetime:
    base = {
        'year': None,
        'month': None,
        'day': None,
        'hour': None,
        'minute': None,
        'second': None,
        'end_of_day': False,
        'tz_info': None,
    }
    base.update(overrides)
    return isoduration.XsdDatetime(**base)


@given(year=years(), month=st.integers().filter(lambda x: x < 1 or x > isoduration.MAX_MONTH))
def test_xsddatetime_invalid_month_raises_value_error(year: int, month: int) -> None:
    """Test that invalid month raises ValueError."""
    with pytest.raises(ValueError, match=f'{month} is not a valid month'):
        _make_datetime(year=year, month=month)


@given(year=years(), month=months(), day=st.integers().filter(lambda x: x < 1 or x > isoduration.MAX_DAY))
def test_xsddatetime_invalid_day_raises_value_error(year: int, month: int, day: int) -> None:
    """Test that invalid day raises ValueError."""
    with pytest.raises(ValueError, match=f'{day} is not a valid day'):
        _make_datetime(year=year, month=month, day=day)


@given(
    year=years(),
    month=months(),
    day=days(),
    hour=st.integers().filter(lambda x: x < 0 or x > isoduration.MAX_HOUR),
    minute=minutes(),
    second=seconds(),
)
def test_xsddatetime_invalid_hour_raises_value_error(  # noqa: PLR0913
    year: int, month: int, day: int, hour: int, minute: int, second: float
) -> None:
    """Test that invalid hour raises ValueError."""
    with pytest.raises(ValueError, match=f'{hour} is not a valid hour'):
        _make_datetime(year=year, month=month, day=day, hour=hour, minute=minute, second=second)


@given(
    year=years(),
    month=months(),
    day=days(),
    hour=hours(),
    minute=st.integers().filter(lambda x: x < 0 or x > isoduration.MAX_MINUTE),
    second=seconds(),
)
def test_xsddatetime_invalid_minute_raises_value_error(  # noqa: PLR0913
    year: int,
    month: int,
    day: int,
    hour: int,
    minute: int,
    second: float,
) -> None:
    """Test that invalid minute raises ValueError."""
    with pytest.raises(ValueError, match=f'{minute} is not a valid minute'):
        _make_datetime(year=year, month=month, day=day, hour=hour, minute=minute, second=second)


@given(
    year=years(),
    month=months(),
    day=days(),
    hour=hours(),
    minute=minutes(),
    second=st.floats().filter(lambda x: x < 0.0 or x >= isoduration.MAX_SECOND),
)
def test_xsddatetime_invalid_second_raises_value_error(  # noqa: PLR0913
    year: int,
    month: int,
    day: int,
    hour: int,
    minute: int,
    second: float,
) -> None:
    """Test that invalid second raises ValueError."""
    with pytest.raises(ValueError, match=f'{re.escape(repr(second))} is not a valid second'):
        _make_datetime(year=year, month=month, day=day, hour=hour, minute=minute, second=second)


@given(year=years(), day=days())
def test_xsddatetime_day_without_month_raises_value_error(year: int, day: int) -> None:
    """Test that day without month raises ValueError."""
    with pytest.raises(ValueError, match='day cannot be present without month'):
        _make_datetime(year=year, day=day)


@given(
    year=years(),
    month=months(),
    day=days(),
    hour=st.none() | hours(),
    minute=st.none() | minutes(),
    second=st.none() | seconds(),
)
def test_xsddatetime_time_requires_hours_minutes_second(  # noqa: PLR0913
    year: int,
    month: int,
    day: int,
    hour: int | None,
    minute: int | None,
    second: int | None,
) -> None:
    """Test that partial time components raise ValueError."""
    all_times = all(p is not None for p in (hour, minute, second))
    any_times = any(p is not None for p in (hour, minute, second))
    assume(not all_times)
    assume(any_times)
    with pytest.raises(ValueError, match='hour, minute and second must all be set together with day'):
        _make_datetime(year=year, month=month, day=day, hour=hour, minute=minute, second=second)


@given(
    year=years(),
    month=months(),
    day=days(),
    hour=hours(),
    minute=minutes(),
    second=seconds(),
)
def test_xsddatetime_end_of_day_excludes_time_components(  # noqa: PLR0913
    year: int,
    month: int,
    day: int,
    hour: int,
    minute: int,
    second: int,
) -> None:
    """Test that end_of_day=True with hour, minute or second raises ValueError."""
    with pytest.raises(ValueError, match='end_of_day cannot be true if hour, minute or second is present'):
        _make_datetime(year=year, month=month, day=day, hour=hour, minute=minute, second=second, end_of_day=True)


@given(year=years(), month=months())
def test_xsddatetime_end_of_day_requires_day(year: int, month: int) -> None:
    """Test that end_of_day=True without day raises ValueError."""
    with pytest.raises(ValueError, match='end_of_day cannot be true if day is not present'):
        _make_datetime(year=year, month=month, end_of_day=True)


@given(year=years(), tz_minute=minutes().filter(lambda x: x != 0), sign=st.sampled_from(['-', '+']))
def test_timezone_needs_to_be_14_00_at_max(year: int, tz_minute: int, sign: Literal['-', '+']) -> None:
    """Test that timezone offsets greater than 14:00h raise ValueError."""
    multiplier = 1 if sign == '+' else -1
    with pytest.raises(ValueError, match='Timezone offset is greater than 14:00h'):
        isoduration.XsdDatetime(
            year=year,
            tz_info=datetime.timezone(datetime.timedelta(hours=multiplier * 14, minutes=multiplier * tz_minute)),
        )
    with pytest.raises(ValueError, match='Timezone hour is 14 but minute is not zero'):
        isoduration.parse_date_time(f'{"-" if year < 0 else ""}{abs(year):04d}{sign}14:{tz_minute:02d}')


class MyTimezone(datetime.tzinfo):
    def utcoffset(self, _: datetime.datetime | None) -> None:
        return None

    def tzname(self, _: datetime.datetime | None) -> str:
        return ''

    def dst(self, _: datetime.datetime | None) -> None:
        return None


@given(year=years())
def test_empty_string_for_utcoffset_none(year: int) -> None:
    """Test that a tzinfo with utcoffset returning None results in no timezone string."""
    tz = MyTimezone()
    dt = isoduration.XsdDatetime(year=year, tz_info=tz)
    expected = f'{"-" if year < 0 else ""}{abs(year):04d}'
    assert str(dt) == expected

    assert isoduration._tz_to_string(tz) == ''
