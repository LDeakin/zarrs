//! Zarr fill value metadata.
//!
//! See <https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html#fill-value>.
//!
//! Fill values metadata is serialized/deserialized into [`FillValueMetadata`].
//!
//! The interpretation of fill values is data type dependent, so this is handled in [`DataTypeExtension::fill_value_from_metadata`](crate::array::data_type::DataTypeExtension::fill_value_from_metadata).
//! Fill value metadata is created with [`DataTypeExtension::metadata_fill_value`](crate::array::data_type::DataTypeExtension::metadata_fill_value).

use derive_more::{Display, From};
use num::traits::float::FloatCore;
use serde::{Deserialize, Serialize};

/// Fill value metadata.
#[derive(Serialize, Deserialize, Clone, PartialEq, Debug, Display)]
#[serde(untagged)]
pub enum FillValueMetadata {
    /// A boolean value.
    Bool(bool),
    /// An unsigned integer.
    Uint(u64), // FIXME: UInt for consistency?
    /// A signed integer.
    Int(i64),
    /// A float.
    Float(FillValueFloat),
    /// A complex number.
    #[display(fmt = "{{re:{_0}, im:{_1}}}")]
    Complex(FillValueFloat, FillValueFloat),
    /// A raw data type.
    #[display(fmt = "{_0:?}")]
    ByteArray(Vec<u8>),
}

impl TryFrom<&str> for FillValueMetadata {
    type Error = serde_json::Error;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        serde_json::from_str(s)
    }
}

/// A float fill value.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, From, Display)]
#[serde(untagged)]
pub enum FillValueFloat {
    /// A float number.
    Float(f64),
    /// A hex string specifying the byte representation of the floating point number as an unsigned integer.
    HexString(HexString),
    /// A string representation of a non finite value.
    NonFinite(FillValueFloatStringNonFinite),
}

/// A hex string.
#[derive(Debug, Clone, Eq, PartialEq, From)]
pub struct HexString(Vec<u8>);

impl HexString {
    /// Return the hex string as a byte slice.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl core::fmt::Display for HexString {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

fn bytes_to_hex_string(v: &[u8]) -> String {
    let mut string = String::with_capacity(2 + v.len() * 2);
    string.push('0');
    string.push('x');
    for byte in v {
        string.push(char::from_digit((byte / 16).into(), 16).unwrap());
        string.push(char::from_digit((byte % 16).into(), 16).unwrap());
    }
    string
}

fn hex_string_to_bytes(s: &str) -> Option<Vec<u8>> {
    if s.starts_with("0x") && s.len() % 2 == 0 {
        (2..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16))
            .collect::<Result<Vec<_>, _>>()
            .ok()
    } else {
        None
    }
}

impl serde::Serialize for HexString {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let string = bytes_to_hex_string(&self.0);
        s.serialize_str(&string)
    }
}

impl<'de> serde::Deserialize<'de> for HexString {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        Ok(Self(hex_string_to_bytes(&s).ok_or(
            serde::de::Error::custom("not a valid hex string"),
        )?))
    }
}

/// A string representation of a non finite value.
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq, Display)]
pub enum FillValueFloatStringNonFinite {
    /// Positive infinity.
    #[serde(rename = "Infinity")]
    PosInfinity,
    /// Negative infinity.
    #[serde(rename = "-Infinity")]
    NegInfinity,
    /// NaN (not-a-number).
    #[serde(rename = "NaN")]
    NaN,
}

impl FillValueMetadata {
    /// Convert the fill value to bool.
    #[must_use]
    pub fn try_as_bool(&self) -> Option<bool> {
        match self {
            FillValueMetadata::Bool(bool) => Some(*bool),
            _ => None,
        }
    }

    /// Convert the fill value to int.
    #[must_use]
    pub fn try_as_int<T: std::convert::TryFrom<i64> + std::convert::TryFrom<u64>>(
        &self,
    ) -> Option<T> {
        match self {
            FillValueMetadata::Int(int) => T::try_from(*int).ok(),
            FillValueMetadata::Uint(uint) => T::try_from(*uint).ok(),
            _ => None,
        }
    }

    /// Convert the fill value to uint.
    #[must_use]
    pub fn try_as_uint<T: std::convert::TryFrom<i64> + std::convert::TryFrom<u64>>(
        &self,
    ) -> Option<T> {
        match self {
            FillValueMetadata::Int(int) => T::try_from(*int).ok(),
            FillValueMetadata::Uint(uint) => T::try_from(*uint).ok(),
            _ => None,
        }
    }

    /// Convert the fill value to float.
    #[must_use]
    pub fn try_as_float<T: FloatCore>(&self) -> Option<T> {
        match self {
            FillValueMetadata::Float(float) => {
                use FillValueFloat as F;
                match float {
                    F::Float(float) => T::from(*float),
                    F::HexString(hex_string) => {
                        let bytes = hex_string.as_bytes();
                        if bytes.len() == core::mem::size_of::<T>() {
                            // NOTE: Cleaner way of doing this?
                            if core::mem::size_of::<T>() == core::mem::size_of::<f32>() {
                                T::from(f32::from_be_bytes(bytes.try_into().unwrap_or_default()))
                            } else if core::mem::size_of::<T>() == core::mem::size_of::<f64>() {
                                T::from(f64::from_be_bytes(bytes.try_into().unwrap_or_default()))
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    }
                    F::NonFinite(nonfinite) => {
                        use FillValueFloatStringNonFinite as NF;
                        Some(match nonfinite {
                            NF::PosInfinity => T::infinity(),
                            NF::NegInfinity => T::neg_infinity(),
                            NF::NaN => T::nan(),
                        })
                    }
                }
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fill_value_metadata_bool_false() {
        let json = r#"false"#;
        let metadata: FillValueMetadata = serde_json::from_str(json).unwrap();
        assert_eq!(json, serde_json::to_string(&metadata).unwrap());
        match metadata {
            FillValueMetadata::Bool(fill_value) => {
                assert!(!fill_value);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn fill_value_metadata_bool_true() {
        let json = r#"true"#;
        let metadata: FillValueMetadata = serde_json::from_str(json).unwrap();
        assert_eq!(json, serde_json::to_string(&metadata).unwrap());
        match metadata {
            FillValueMetadata::Bool(fill_value) => {
                assert!(fill_value);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn fill_value_metadata_uint() {
        let json = r#"7"#;
        let metadata: FillValueMetadata = serde_json::from_str(json).unwrap();
        assert_eq!(json, serde_json::to_string(&metadata).unwrap());
        match metadata {
            FillValueMetadata::Uint(fill_value) => {
                assert_eq!(fill_value, 7);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn fill_value_metadata_int() {
        let json = r#"-7"#;
        let metadata: FillValueMetadata = serde_json::from_str(json).unwrap();
        assert_eq!(json, serde_json::to_string(&metadata).unwrap());
        match metadata {
            FillValueMetadata::Int(fill_value) => {
                assert_eq!(fill_value, -7);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn fill_value_metadata_float_number() {
        let json = r#"7.5"#;
        let metadata: FillValueMetadata = serde_json::from_str(json).unwrap();
        assert_eq!(json, serde_json::to_string(&metadata).unwrap());
        match metadata {
            FillValueMetadata::Float(FillValueFloat::Float(fill_value)) => {
                assert_eq!(fill_value, 7.5);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn fill_value_metadata_float_infinity() {
        let json = r#""Infinity""#;
        let metadata: FillValueMetadata = serde_json::from_str(json).unwrap();
        assert_eq!(json, serde_json::to_string(&metadata).unwrap());
        match metadata {
            FillValueMetadata::Float(FillValueFloat::NonFinite(fill_value)) => {
                assert_eq!(fill_value, FillValueFloatStringNonFinite::PosInfinity);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn fill_value_metadata_float_neg_infinity() {
        let json = r#""-Infinity""#;
        let metadata: FillValueMetadata = serde_json::from_str(json).unwrap();
        assert_eq!(json, serde_json::to_string(&metadata).unwrap());
        match metadata {
            FillValueMetadata::Float(FillValueFloat::NonFinite(fill_value)) => {
                assert_eq!(fill_value, FillValueFloatStringNonFinite::NegInfinity);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn fill_value_metadata_float_nan() {
        let json = r#""NaN""#;
        let metadata: FillValueMetadata = serde_json::from_str(json).unwrap();
        assert_eq!(json, serde_json::to_string(&metadata).unwrap());
        match metadata {
            FillValueMetadata::Float(FillValueFloat::NonFinite(fill_value)) => {
                assert_eq!(fill_value, FillValueFloatStringNonFinite::NaN)
            }
            _ => panic!(),
        }
    }

    #[test]
    fn fill_value_metadata_float_hex_string() {
        let json = r#""0x7fc00000""#;
        let metadata: FillValueMetadata = serde_json::from_str(json).unwrap();
        assert_eq!(json, serde_json::to_string(&metadata).unwrap());
        match metadata {
            FillValueMetadata::Float(FillValueFloat::HexString(fill_value)) => {
                assert_eq!(fill_value.0, f32::NAN.to_be_bytes())
            }
            _ => panic!(),
        }
    }

    #[test]
    fn fill_value_metadata_float_complex() {
        let json = r#"["0x7fc00000","NaN"]"#;
        let metadata: FillValueMetadata = serde_json::from_str(json).unwrap();
        assert_eq!(json, serde_json::to_string(&metadata).unwrap());
        match metadata {
            FillValueMetadata::Complex(re, im) => {
                match re {
                    FillValueFloat::HexString(fill_value) => {
                        assert_eq!(fill_value.0, f32::NAN.to_be_bytes());
                    }
                    _ => panic!(),
                };
                match im {
                    FillValueFloat::NonFinite(fill_value) => {
                        assert_eq!(fill_value, FillValueFloatStringNonFinite::NaN);
                    }
                    _ => panic!(),
                };
            }
            _ => panic!(),
        }
    }

    #[test]
    fn fill_value_metadata_raw_bytes() {
        let json = r#"[0,1,2,3]"#;
        let metadata: FillValueMetadata = serde_json::from_str(json).unwrap();
        assert_eq!(json, serde_json::to_string(&metadata).unwrap());
        match metadata {
            FillValueMetadata::ByteArray(fill_value) => {
                assert_eq!(fill_value, [0, 1, 2, 3]);
            }
            _ => panic!(),
        }
    }
}
