#[allow(unused)]
use zarrs_metadata_ext::data_type::NumpyTimeUnit;

mod datetime64;
mod timedelta64;

#[cfg(feature = "chrono")]
pub(super) fn chrono_timedelta_to_int(
    timedelta: chrono::TimeDelta,
    unit: NumpyTimeUnit,
    scale_factor: i64,
) -> Option<i64> {
    match unit {
        NumpyTimeUnit::Generic | NumpyTimeUnit::Year | NumpyTimeUnit::Month => None,
        NumpyTimeUnit::Week => timedelta.num_days().checked_mul(7),
        NumpyTimeUnit::Day => Some(timedelta.num_days()),
        NumpyTimeUnit::Hour => Some(timedelta.num_hours()),
        NumpyTimeUnit::Minute => Some(timedelta.num_minutes()),
        NumpyTimeUnit::Second => Some(timedelta.num_seconds()),
        NumpyTimeUnit::Millisecond => Some(timedelta.num_milliseconds()),
        NumpyTimeUnit::Microsecond => timedelta.num_microseconds(),
        NumpyTimeUnit::Nanosecond => timedelta.num_nanoseconds(),
        NumpyTimeUnit::Picosecond => (timedelta / 1_000).num_nanoseconds(),
        NumpyTimeUnit::Femtosecond => (timedelta / 1_000_000).num_nanoseconds(),
        NumpyTimeUnit::Attosecond => (timedelta / 1_000_000_000).num_nanoseconds(),
    }
    .and_then(|i| i.checked_div(scale_factor))
}

#[cfg(feature = "chrono")]
pub(super) fn int_to_chrono_timedelta(
    i: i64,
    unit: NumpyTimeUnit,
    scale_factor: i64,
) -> Option<chrono::TimeDelta> {
    use chrono::TimeDelta;
    match unit {
        NumpyTimeUnit::Generic | NumpyTimeUnit::Year | NumpyTimeUnit::Month => None, // year/month units are not strictly correct with this API
        NumpyTimeUnit::Week => TimeDelta::try_weeks(i * scale_factor),
        NumpyTimeUnit::Day => TimeDelta::try_days(i * scale_factor),
        NumpyTimeUnit::Hour => TimeDelta::try_hours(i * scale_factor),
        NumpyTimeUnit::Minute => TimeDelta::try_minutes(i * scale_factor),
        NumpyTimeUnit::Second => TimeDelta::try_seconds(i * scale_factor),
        NumpyTimeUnit::Millisecond => TimeDelta::try_milliseconds(i * scale_factor),
        NumpyTimeUnit::Microsecond => Some(TimeDelta::microseconds(i * scale_factor)),
        NumpyTimeUnit::Nanosecond => Some(TimeDelta::nanoseconds(i * scale_factor)),
        NumpyTimeUnit::Picosecond => TimeDelta::try_milliseconds((i * scale_factor) / 1_000),
        NumpyTimeUnit::Femtosecond => TimeDelta::try_milliseconds((i * scale_factor) / 1_000_000),
        NumpyTimeUnit::Attosecond => {
            TimeDelta::try_milliseconds((i * scale_factor) / 1_000_000_000)
        }
    }
}

#[cfg(feature = "jiff")]
pub(super) fn jiff_duration_to_int(
    duration: jiff::SignedDuration,
    unit: NumpyTimeUnit,
    scale_factor: i64,
) -> Result<i64, jiff::Error> {
    use jiff::Unit;
    let span = jiff::Span::try_from(duration)?;
    let delta = match unit {
        NumpyTimeUnit::Year => span.total(Unit::Year)?,
        NumpyTimeUnit::Month => span.total(Unit::Month)?,
        NumpyTimeUnit::Week => span.total(Unit::Week)?,
        NumpyTimeUnit::Day => span.total(Unit::Day)?,
        NumpyTimeUnit::Hour => span.total(Unit::Hour)?,
        NumpyTimeUnit::Minute => span.total(Unit::Minute)?,
        NumpyTimeUnit::Second => span.total(Unit::Second)?,
        NumpyTimeUnit::Millisecond => span.total(Unit::Millisecond)?,
        NumpyTimeUnit::Microsecond => span.total(Unit::Microsecond)?,
        NumpyTimeUnit::Nanosecond => span.total(Unit::Nanosecond)?,
        NumpyTimeUnit::Picosecond => span.total(Unit::Nanosecond)? / 1e3,
        NumpyTimeUnit::Femtosecond => span.total(Unit::Nanosecond)? / 1e6,
        NumpyTimeUnit::Attosecond => span.total(Unit::Nanosecond)? / 1e9,
        NumpyTimeUnit::Generic => Err(jiff::Error::from_args(format_args!(
            "datetime64 generic unit is not supported"
        )))?,
    };
    #[allow(clippy::cast_possible_truncation)]
    let delta: i64 = delta.trunc() as i64 / scale_factor;
    Ok(delta)
}

#[cfg(feature = "jiff")]
pub(super) fn int_to_jiff_duration(
    i: i64,
    unit: NumpyTimeUnit,
    scale_factor: i64,
) -> Result<jiff::SignedDuration, jiff::Error> {
    const EPOCH: jiff::civil::Date = jiff::civil::date(1970, 1, 1);
    let span = jiff::Span::new();
    match unit {
        NumpyTimeUnit::Generic => Err(jiff::Error::from_args(format_args!(
            "datetime64 generic unit is not supported"
        )))?,
        NumpyTimeUnit::Year => span.try_years(i * scale_factor)?,
        NumpyTimeUnit::Month => span.try_months(i * scale_factor)?,
        NumpyTimeUnit::Week => span.try_weeks(i * scale_factor)?,
        NumpyTimeUnit::Day => span.try_days(i * scale_factor)?,
        NumpyTimeUnit::Hour => span.try_hours(i * scale_factor)?,
        NumpyTimeUnit::Minute => span.try_minutes(i * scale_factor)?,
        NumpyTimeUnit::Second => span.try_seconds(i * scale_factor)?,
        NumpyTimeUnit::Millisecond => span.try_milliseconds(i * scale_factor)?,
        NumpyTimeUnit::Microsecond => span.try_microseconds(i * scale_factor)?,
        NumpyTimeUnit::Nanosecond => span.try_nanoseconds(i * scale_factor)?,
        NumpyTimeUnit::Picosecond => span.try_nanoseconds((i * scale_factor) / 1_000)?,
        NumpyTimeUnit::Femtosecond => span.try_nanoseconds((i * scale_factor) / 1_000_000)?,
        NumpyTimeUnit::Attosecond => span.try_nanoseconds((i * scale_factor) / 1_000_000_000)?,
    }
    .to_duration(EPOCH)
}
