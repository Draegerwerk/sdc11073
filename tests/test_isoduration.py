"""Unit tests for isoduration module."""

import datetime

import pytest
from hypothesis import given
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
        return datetime.UTC
    return datetime.timezone(datetime.timedelta(seconds=delta.total_seconds()))


@st.composite
def g_years(draw: st.DrawFn) -> isoduration.GYear:
    year = draw(st.integers(min_value=datetime.MINYEAR, max_value=datetime.MAXYEAR))
    tz = draw(
        st.one_of(
            st.just(None),
            timezones(),
        ),
    )
    return isoduration.GYear(year=year, tzinfo=tz)


@st.composite
def g_year_months(draw: st.DrawFn) -> isoduration.GYearMonth:
    year = draw(st.integers(min_value=datetime.MINYEAR, max_value=datetime.MAXYEAR))
    month = draw(st.integers(min_value=1, max_value=12))
    tz = draw(
        st.one_of(
            st.just(None),
            timezones(),
        ),
    )
    return isoduration.GYearMonth(year=year, month=month, tzinfo=tz)


@given(dt=st.datetimes(timezones=st.one_of(st.just(None), timezones())) | g_years() | g_year_months())
def test_date_time(dt: isoduration.DateTypeUnion) -> None:
    parsed = isoduration.date_time_string(dt)
    assert dt == isoduration.parse_date_time(parsed)
