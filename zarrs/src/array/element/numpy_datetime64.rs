#![allow(unused_imports)]

use crate::array::{ArrayBytes, ArrayError, DataType, Element, ElementOwned};

// #[cfg(feature = "chrono")]
// impl<Tz: chrono::TimeZone> Element for chrono::DateTime<Tz> {
//     fn into_array_bytes<'a>(
//         data_type: &DataType,
//         elements: &'a [Self],
//     ) -> Result<ArrayBytes<'a>, ArrayError> {
//         todo!()
//     }

//     fn validate_data_type(data_type: &DataType) -> Result<(), ArrayError> {
//         todo!()
//     }
// }

// #[cfg(feature = "chrono")]
// impl<Tz: chrono::TimeZone> ElementOwned for chrono::DateTime<Tz> {
//     fn from_array_bytes(
//         data_type: &DataType,
//         bytes: ArrayBytes<'_>,
//     ) -> Result<Vec<Self>, ArrayError> {
//         todo!()
//     }
// }

// #[cfg(feature = "datetime")]
// impl Element for datetime::Instant {
//     fn into_array_bytes<'a>(
//         data_type: &DataType,
//         elements: &'a [Self],
//     ) -> Result<ArrayBytes<'a>, ArrayError> {
//         todo!()
//     }

//     fn validate_data_type(data_type: &DataType) -> Result<(), ArrayError> {
//         todo!()
//     }
// }

// #[cfg(feature = "datetime")]
// impl ElementOwned for datetime::Instant {
//     fn from_array_bytes(
//         data_type: &DataType,
//         bytes: ArrayBytes<'_>,
//     ) -> Result<Vec<Self>, ArrayError> {
//         todo!()
//     }
// }

#[cfg(feature = "jiff")]
impl Element for jiff::Timestamp {
    fn into_array_bytes<'a>(
        data_type: &DataType,
        elements: &'a [Self],
    ) -> Result<ArrayBytes<'a>, ArrayError> {
        use jiff::{Span, Timestamp, Unit};

        // Self::validate_data_type(data_type)?;
        let DataType::NumpyDateTime64 { unit, scale_factor } = data_type else {
            return Err(ArrayError::IncompatibleElementType);
        };
        let scale_factor = i64::from(scale_factor.get());
        let mut bytes: Vec<u8> = Vec::with_capacity(elements.len() * size_of::<u64>());
        let error = |e: jiff::Error| ArrayError::Other(e.to_string());
        for element in elements {
            if element == &Timestamp::MIN {
                bytes.extend_from_slice(&i64::MIN.to_ne_bytes());
            } else {
                use zarrs_metadata_ext::data_type::NumpyDateTime64DataTypeUnit;

                let span =
                    Span::try_from(element.duration_since(Timestamp::UNIX_EPOCH)).map_err(error)?;
                #[allow(clippy::cast_possible_truncation)]
                let value = match unit {
                    NumpyDateTime64DataTypeUnit::Year => span.total(Unit::Year).map_err(error)?,
                    NumpyDateTime64DataTypeUnit::Month => span.total(Unit::Month).map_err(error)?,
                    NumpyDateTime64DataTypeUnit::Week => span.total(Unit::Week).map_err(error)?,
                    NumpyDateTime64DataTypeUnit::Day => span.total(Unit::Day).map_err(error)?,
                    NumpyDateTime64DataTypeUnit::Hour => span.total(Unit::Hour).map_err(error)?,
                    NumpyDateTime64DataTypeUnit::Minute => {
                        span.total(Unit::Minute).map_err(error)?
                    }
                    NumpyDateTime64DataTypeUnit::Second => {
                        span.total(Unit::Second).map_err(error)?
                    }
                    NumpyDateTime64DataTypeUnit::Millisecond => {
                        span.total(Unit::Millisecond).map_err(error)?
                    }
                    NumpyDateTime64DataTypeUnit::Microsecond => {
                        span.total(Unit::Microsecond).map_err(error)?
                    }
                    NumpyDateTime64DataTypeUnit::Nanosecond => {
                        span.total(Unit::Nanosecond).map_err(error)?
                    }
                    NumpyDateTime64DataTypeUnit::Picosecond => {
                        span.total(Unit::Nanosecond).map_err(error)? / 1e3
                    }
                    NumpyDateTime64DataTypeUnit::Femtosecond => {
                        span.total(Unit::Nanosecond).map_err(error)? / 1e6
                    }
                    NumpyDateTime64DataTypeUnit::Attosecond => {
                        span.total(Unit::Nanosecond).map_err(error)? / 1e9
                    }
                    NumpyDateTime64DataTypeUnit::Generic => Err(error(jiff::Error::from_args(
                        format_args!("datetime64 generic unit is not supported"),
                    )))?,
                }
                .trunc() as i64;
                let value = value / scale_factor;
                bytes.extend_from_slice(&value.to_ne_bytes());
            }
        }
        Ok(bytes.into())
    }

    fn validate_data_type(data_type: &DataType) -> Result<(), ArrayError> {
        if matches!(
            data_type,
            DataType::NumpyDateTime64 {
                unit: _,
                scale_factor: _
            }
        ) {
            Ok(())
        } else {
            Err(ArrayError::IncompatibleElementType)
        }
    }
}

#[cfg(feature = "jiff")]
impl ElementOwned for jiff::Timestamp {
    fn from_array_bytes(
        data_type: &DataType,
        bytes: ArrayBytes<'_>,
    ) -> Result<Vec<Self>, ArrayError> {
        use crate::array::convert_from_bytes_slice;
        use jiff::{SignedDuration, Span, Timestamp};
        use zarrs_metadata_ext::data_type::NumpyDateTime64DataTypeUnit;

        // Self::validate_data_type(data_type)?;
        let DataType::NumpyDateTime64 { unit, scale_factor } = data_type else {
            return Err(ArrayError::IncompatibleElementType);
        };
        let scale_factor = i64::from(scale_factor.get());

        let bytes = bytes.into_fixed()?;
        let elements = convert_from_bytes_slice::<i64>(&bytes);

        let timestamps = elements
            .into_iter()
            .map(|i| {
                if i == i64::MIN {
                    Ok(Timestamp::MIN)
                } else {
                    const EPOCH: jiff::civil::Date = jiff::civil::date(1970, 1, 1);
                    Timestamp::from_duration(match unit {
                        NumpyDateTime64DataTypeUnit::Generic => Err(jiff::Error::from_args(
                            format_args!("datetime64 generic unit is not supported"),
                        ))?,
                        NumpyDateTime64DataTypeUnit::Year => {
                            Span::new().try_years(i * scale_factor)?.to_duration(EPOCH)
                        }
                        NumpyDateTime64DataTypeUnit::Month => {
                            Span::new().try_months(i * scale_factor)?.to_duration(EPOCH)
                        }
                        NumpyDateTime64DataTypeUnit::Week => {
                            Span::new().try_weeks(i * scale_factor)?.to_duration(EPOCH)
                        }
                        NumpyDateTime64DataTypeUnit::Day => {
                            Span::new().try_days(i * scale_factor)?.to_duration(EPOCH)
                        }
                        NumpyDateTime64DataTypeUnit::Hour => {
                            SignedDuration::try_from(Span::new().try_hours(i * scale_factor)?)
                        }
                        NumpyDateTime64DataTypeUnit::Minute => {
                            SignedDuration::try_from(Span::new().try_minutes(i * scale_factor)?)
                        }
                        NumpyDateTime64DataTypeUnit::Second => {
                            SignedDuration::try_from(Span::new().try_seconds(i * scale_factor)?)
                        }
                        NumpyDateTime64DataTypeUnit::Millisecond => SignedDuration::try_from(
                            Span::new().try_milliseconds(i * scale_factor)?,
                        ),
                        NumpyDateTime64DataTypeUnit::Microsecond => SignedDuration::try_from(
                            Span::new().try_microseconds(i * scale_factor)?,
                        ),
                        NumpyDateTime64DataTypeUnit::Nanosecond => {
                            SignedDuration::try_from(Span::new().try_nanoseconds(i * scale_factor)?)
                        }
                        NumpyDateTime64DataTypeUnit::Picosecond => SignedDuration::try_from(
                            Span::new().try_nanoseconds((i * scale_factor) / 1_000)?,
                        ),
                        NumpyDateTime64DataTypeUnit::Femtosecond => SignedDuration::try_from(
                            Span::new().try_nanoseconds((i * scale_factor) / 1_000_000)?,
                        ),
                        NumpyDateTime64DataTypeUnit::Attosecond => SignedDuration::try_from(
                            Span::new().try_nanoseconds((i * scale_factor) / 1_000_000_000)?,
                        ),
                    }?)
                }
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| ArrayError::Other(e.to_string()))?;
        Ok(timestamps)
    }
}
