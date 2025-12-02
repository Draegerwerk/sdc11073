"""Unit tests for isoduration module."""

import datetime
import decimal
import re
import sys

import pytest
from hypothesis import assume, given
from hypothesis import strategies as st

from sdc11073.xml_types import isoduration


@given(timedelta=st.timedeltas())
def test_duration_parsing(timedelta: datetime.timedelta) -> None:
    duration_string = isoduration.duration_string(timedelta.total_seconds())
    seconds = isoduration.parse_duration(duration_string)
    assert seconds == timedelta.total_seconds()


@pytest.mark.parametrize('duration', ['P1Y2M', '-P3Y', 'P0Y5M', 'P2Y0M', 'P1Y2M3DT4H5M6S'])
def test_parse_duration_raises_value_error_for_years_months(duration: str) -> None:
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
    with pytest.raises(ValueError, match=f'{month} is not a valid month'):
        _make_datetime(year=year, month=month)


@given(year=years(), month=months(), day=st.integers().filter(lambda x: x < 1 or x > isoduration.MAX_DAY))
def test_xsddatetime_invalid_day_raises_value_error(year: int, month: int, day: int) -> None:
    with pytest.raises(ValueError, match=f'{day} is not a valid day'):
        _make_datetime(year=year, month=month, day=day)


@given(year=years(), month=months(), day=days(), hour=st.integers().filter(lambda x: x < 0 or x > isoduration.MAX_HOUR))
def test_xsddatetime_invalid_hour_raises_value_error(year: int, month: int, day: int, hour: int) -> None:
    with pytest.raises(ValueError, match=f'{hour} is not a valid hour'):
        _make_datetime(year=year, month=month, day=day, hour=hour)


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
    with pytest.raises(ValueError, match=f'{re.escape(repr(second))} is not a valid second'):
        _make_datetime(year=year, month=month, day=day, hour=hour, minute=minute, second=second)


@given(year=years(), day=days())
def test_xsddatetime_day_without_month_raises_value_error(year: int, day: int) -> None:
    with pytest.raises(ValueError, match='day cannot be present without month'):
        _make_datetime(year=year, day=day)


@given(year=years(), month=months(), hour=hours(), minute=minutes(), second=seconds())
def test_xsddatetime_hour_requires_day(year: int, month: int, hour: int, minute: int, second: int) -> None:
    with pytest.raises(ValueError, match='hour cannot be present without day, minute and second'):
        _make_datetime(year=year, month=month, hour=hour, minute=minute, second=second)


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
    all_times = all(p is not None for p in (hour, minute, second))
    any_times = any(p is not None for p in (hour, minute, second))
    assume(not all_times)
    assume(any_times)
    with pytest.raises(ValueError, match='cannot be present without'):
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
    with pytest.raises(ValueError, match='end_of_day cannot be true if hour, minute or second is present'):
        _make_datetime(year=year, month=month, day=day, hour=hour, minute=minute, second=second, end_of_day=True)


@given(year=years(), month=months())
def test_xsddatetime_end_of_day_requires_day(year: int, month: int) -> None:
    with pytest.raises(ValueError, match='end_of_day cannot be true if day is not present'):
        _make_datetime(year=year, month=month, end_of_day=True)
