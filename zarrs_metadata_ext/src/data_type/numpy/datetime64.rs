use std::num::NonZeroU32;

use derive_more::Display;
use serde::{Deserialize, Serialize};
use zarrs_metadata::ConfigurationSerialize;

/// A `numpy.datetime64` data type unit.
#[allow(missing_docs)]
#[derive(Serialize, Deserialize, Clone, Copy, Eq, PartialEq, Debug, Display)]
pub enum NumpyDateTime64DataTypeUnit {
    #[serde(rename = "generic")]
    Generic,
    #[serde(rename = "Y")]
    Year,
    #[serde(rename = "M")]
    Month,
    #[serde(rename = "W")]
    Week,
    #[serde(rename = "D")]
    Day,
    #[serde(rename = "h")]
    Hour,
    #[serde(rename = "m")]
    Minute,
    #[serde(rename = "s")]
    Second,
    #[serde(rename = "ms")]
    Millisecond,
    #[serde(rename = "us", alias = "μs")]
    Microsecond,
    #[serde(rename = "ns")]
    Nanosecond,
    #[serde(rename = "ps")]
    Picosecond,
    #[serde(rename = "fs")]
    Femtosecond,
    #[serde(rename = "as")]
    Attosecond,
}

#[cfg(feature = "jiff")]
impl From<jiff::Unit> for NumpyDateTime64DataTypeUnit {
    fn from(unit: jiff::Unit) -> Self {
        match unit {
            jiff::Unit::Year => Self::Year,
            jiff::Unit::Month => Self::Month,
            jiff::Unit::Week => Self::Week,
            jiff::Unit::Day => Self::Day,
            jiff::Unit::Hour => Self::Hour,
            jiff::Unit::Minute => Self::Minute,
            jiff::Unit::Second => Self::Second,
            jiff::Unit::Millisecond => Self::Millisecond,
            jiff::Unit::Microsecond => Self::Microsecond,
            jiff::Unit::Nanosecond => Self::Nanosecond,
        }
    }
}

#[cfg(feature = "jiff")]
impl TryFrom<NumpyDateTime64DataTypeUnit> for jiff::Unit {
    type Error = NumpyDateTime64DataTypeUnit;
    fn try_from(unit: NumpyDateTime64DataTypeUnit) -> Result<Self, Self::Error> {
        match unit {
            NumpyDateTime64DataTypeUnit::Generic
            | NumpyDateTime64DataTypeUnit::Picosecond
            | NumpyDateTime64DataTypeUnit::Femtosecond
            | NumpyDateTime64DataTypeUnit::Attosecond => Err(unit),
            NumpyDateTime64DataTypeUnit::Year => Ok(Self::Year),
            NumpyDateTime64DataTypeUnit::Month => Ok(Self::Month),
            NumpyDateTime64DataTypeUnit::Week => Ok(Self::Week),
            NumpyDateTime64DataTypeUnit::Day => Ok(Self::Day),
            NumpyDateTime64DataTypeUnit::Hour => Ok(Self::Hour),
            NumpyDateTime64DataTypeUnit::Minute => Ok(Self::Minute),
            NumpyDateTime64DataTypeUnit::Second => Ok(Self::Second),
            NumpyDateTime64DataTypeUnit::Millisecond => Ok(Self::Millisecond),
            NumpyDateTime64DataTypeUnit::Microsecond => Ok(Self::Microsecond),
            NumpyDateTime64DataTypeUnit::Nanosecond => Ok(Self::Nanosecond),
        }
    }
}

/// The `numpy.datetime64` data type configuration.
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug, Display)]
#[serde(deny_unknown_fields)]
#[display("{}", serde_json::to_string(self).unwrap_or_default())]
pub struct NumpyDateTime64DataTypeConfiguration {
    /// The `NumPy` temporal unit.
    pub unit: NumpyDateTime64DataTypeUnit,
    /// The `NumPy` scale factor.
    pub scale_factor: NonZeroU32, // 31
}

impl ConfigurationSerialize for NumpyDateTime64DataTypeConfiguration {}
