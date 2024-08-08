//! Zarr V3 fill value metadata.
//!
//! See <https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html#fill-value>.
//!
//! Fill values metadata is serialised/deserialised into [`FillValueMetadata`].
//!
//! The interpretation of fill values is data type dependent, so this is handled in [`DataTypeExtension::fill_value_from_metadata`](crate::array::data_type::DataTypeExtension::fill_value_from_metadata).
//! Fill value metadata is created with [`DataTypeExtension::metadata_fill_value`](crate::array::data_type::DataTypeExtension::metadata_fill_value).

use derive_more::{Display, From};
use half::{bf16, f16};
use num::traits::float::FloatCore;
use serde::{Deserialize, Serialize};

use crate::array::{ZARR_NAN_BF16, ZARR_NAN_F16, ZARR_NAN_F32, ZARR_NAN_F64};

/// Fill value metadata.
#[derive(Serialize, Deserialize, Clone, PartialEq, Debug, Display)]
#[serde(untagged)]
pub enum FillValueMetadata {
    /// A boolean value.
    Bool(bool),
    /// An unsigned integer.
    UInt(u64),
    /// A signed integer.
    Int(i64),
    /// A float.
    Float(FillValueFloat),
    /// An array of integers. Suitable for raw (`r<N>`) and `binary` data types.
    #[display("{_0:?}")]
    ByteArray(Vec<u8>),
    /// A complex number.
    #[display("{{re:{_0}, im:{_1}}}")]
    Complex(FillValueFloat, FillValueFloat),
    /// A string.
    String(String),
    /// An unsupported fill value.
    Unsupported(serde_json::Value),
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

impl FillValueFloat {
    fn to_float<T: FloatCore>(&self) -> Option<T> {
        match self {
            Self::Float(float) => T::from(*float),
            Self::HexString(hex_string) => {
                let bytes: &[u8] = hex_string.as_be_bytes();
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
            Self::NonFinite(nonfinite) => {
                use FillValueFloatStringNonFinite as NF;
                match nonfinite {
                    NF::PosInfinity => Some(T::infinity()),
                    NF::NegInfinity => Some(T::neg_infinity()),
                    NF::NaN => Some(T::nan()),
                }
            }
        }
    }
}

/// A hex string.
#[derive(Debug, Clone, Eq, PartialEq, From)]
pub struct HexString(Vec<u8>);

impl HexString {
    /// Create a new [`HexString`]
    #[must_use]
    pub fn new(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    /// Return the hex string as a big endian byte slice.
    #[must_use]
    pub fn as_be_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl From<&HexString> for String {
    fn from(value: &HexString) -> Self {
        bytes_to_hex_string(value.as_be_bytes())
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

fn hex_string_to_be_bytes(s: &str) -> Option<Vec<u8>> {
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
        Ok(Self(hex_string_to_be_bytes(&s).ok_or_else(|| {
            serde::de::Error::custom("not a valid hex string")
        })?))
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

impl From<&FillValueFloatStringNonFinite> for String {
    fn from(value: &FillValueFloatStringNonFinite) -> Self {
        match value {
            FillValueFloatStringNonFinite::PosInfinity => "Infinity",
            FillValueFloatStringNonFinite::NegInfinity => "-Infinity",
            FillValueFloatStringNonFinite::NaN => "NaN",
        }
        .to_string()
    }
}

impl FillValueMetadata {
    /// Convert the fill value to a [`bool`].
    #[must_use]
    pub const fn try_as_bool(&self) -> Option<bool> {
        match self {
            Self::Bool(bool) => Some(*bool),
            _ => None,
        }
    }

    /// Convert the fill value to an signed integer.
    #[must_use]
    pub fn try_as_int<T: std::convert::TryFrom<i64> + std::convert::TryFrom<u64>>(
        &self,
    ) -> Option<T> {
        match self {
            Self::Int(int) => T::try_from(*int).ok(),
            Self::UInt(uint) => T::try_from(*uint).ok(),
            _ => None,
        }
    }

    /// Convert the fill value to an unsigned integer.
    #[must_use]
    pub fn try_as_uint<T: std::convert::TryFrom<i64> + std::convert::TryFrom<u64>>(
        &self,
    ) -> Option<T> {
        match self {
            Self::Int(int) => T::try_from(*int).ok(),
            Self::UInt(uint) => T::try_from(*uint).ok(),
            _ => None,
        }
    }

    /// Convert the fill value to a float.
    #[must_use]
    pub fn try_as_float<T: FloatCore>(&self) -> Option<T> {
        match self {
            Self::Int(int) => num::traits::cast(*int),
            Self::UInt(uint) => num::traits::cast(*uint),
            Self::Float(float) => {
                use FillValueFloat as F;
                match float {
                    F::Float(float) => T::from(*float),
                    F::HexString(hex_string) => {
                        let bytes = hex_string.as_be_bytes();
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
                        match nonfinite {
                            NF::PosInfinity => Some(T::infinity()),
                            NF::NegInfinity => Some(T::neg_infinity()),
                            NF::NaN => {
                                if core::mem::size_of::<T>() == core::mem::size_of::<f32>() {
                                    T::from(ZARR_NAN_F32)
                                } else if core::mem::size_of::<T>() == core::mem::size_of::<f64>() {
                                    T::from(ZARR_NAN_F64)
                                } else {
                                    None
                                }
                            }
                        }
                    }
                }
            }
            _ => None,
        }
    }

    /// Convert the fill value to a complex number (float pair).
    #[must_use]
    pub fn try_as_float_pair<T: FloatCore>(&self) -> Option<(T, T)> {
        match self {
            Self::Complex(re, im) => {
                if let (Some(re), Some(im)) = (re.to_float::<T>(), im.to_float::<T>()) {
                    Some((re, im))
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    /// Convert the fill value to a [`struct@f16`].
    #[must_use]
    pub fn try_as_float16(&self) -> Option<f16> {
        match self {
            Self::Int(int) =>
            {
                #[allow(clippy::cast_precision_loss)]
                Some(f16::from_f64(*int as f64))
            }
            Self::UInt(uint) =>
            {
                #[allow(clippy::cast_precision_loss)]
                Some(f16::from_f64(*uint as f64))
            }
            Self::Float(float) => {
                use FillValueFloat as F;
                match float {
                    F::Float(float) => Some(f16::from_f64(*float)),
                    F::HexString(hex_string) => {
                        let bytes = hex_string.as_be_bytes();
                        bytes
                            .try_into()
                            .map_or(None, |bytes| Some(f16::from_be_bytes(bytes)))
                    }
                    F::NonFinite(nonfinite) => {
                        use FillValueFloatStringNonFinite as NF;
                        Some(match nonfinite {
                            NF::PosInfinity => f16::INFINITY,
                            NF::NegInfinity => f16::NEG_INFINITY,
                            NF::NaN => ZARR_NAN_F16,
                        })
                    }
                }
            }
            _ => None,
        }
    }

    /// Convert the fill value to a [`bf16`].
    #[must_use]
    pub fn try_as_bfloat16(&self) -> Option<bf16> {
        match self {
            Self::Int(int) =>
            {
                #[allow(clippy::cast_precision_loss)]
                Some(bf16::from_f64(*int as f64))
            }
            Self::UInt(uint) =>
            {
                #[allow(clippy::cast_precision_loss)]
                Some(bf16::from_f64(*uint as f64))
            }
            Self::Float(float) => {
                use FillValueFloat as F;
                match float {
                    F::Float(float) => Some(bf16::from_f64(*float)),
                    F::HexString(hex_string) => {
                        let bytes = hex_string.as_be_bytes();
                        bytes
                            .try_into()
                            .map_or(None, |bytes| Some(bf16::from_be_bytes(bytes)))
                    }
                    F::NonFinite(nonfinite) => {
                        use FillValueFloatStringNonFinite as NF;
                        Some(match nonfinite {
                            NF::PosInfinity => bf16::INFINITY,
                            NF::NegInfinity => bf16::NEG_INFINITY,
                            NF::NaN => ZARR_NAN_BF16,
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
    use crate::array::{DataType, FillValue};

    use super::*;

    #[test]
    fn fill_value_metadata_bool_false() {
        let json = r#"false"#;
        let metadata: FillValueMetadata = json.try_into().unwrap();
        assert_eq!(json, serde_json::to_string(&metadata).unwrap());
        match metadata {
            FillValueMetadata::Bool(fill_value) => {
                assert!(!fill_value);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn fill_value_metadata_bool_true() {
        let json = r#"true"#;
        let metadata: FillValueMetadata = json.try_into().unwrap();
        assert_eq!(json, serde_json::to_string(&metadata).unwrap());
        match metadata {
            FillValueMetadata::Bool(fill_value) => {
                assert!(fill_value);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn fill_value_metadata_uint() {
        let json = r#"7"#;
        let metadata: FillValueMetadata = json.try_into().unwrap();
        assert_eq!(json, serde_json::to_string(&metadata).unwrap());
        match metadata {
            FillValueMetadata::UInt(fill_value) => {
                assert_eq!(fill_value, 7);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn fill_value_metadata_int() {
        let json = r#"-7"#;
        let metadata: FillValueMetadata = json.try_into().unwrap();
        assert_eq!(json, serde_json::to_string(&metadata).unwrap());
        match metadata {
            FillValueMetadata::Int(fill_value) => {
                assert_eq!(fill_value, -7);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn fill_value_metadata_float_number() {
        let json = r#"7.5"#;
        let metadata: FillValueMetadata = json.try_into().unwrap();
        assert_eq!(json, serde_json::to_string(&metadata).unwrap());
        match metadata {
            FillValueMetadata::Float(FillValueFloat::Float(fill_value)) => {
                assert_eq!(fill_value, 7.5);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn fill_value_metadata_float_infinity() {
        let json = r#""Infinity""#;
        let metadata: FillValueMetadata = json.try_into().unwrap();
        assert_eq!(json, serde_json::to_string(&metadata).unwrap());
        match metadata {
            FillValueMetadata::Float(FillValueFloat::NonFinite(fill_value)) => {
                assert_eq!(fill_value, FillValueFloatStringNonFinite::PosInfinity);
            }
            _ => unreachable!(),
        }

        let pos_inf = FillValueFloat::NonFinite(FillValueFloatStringNonFinite::PosInfinity)
            .to_float::<f32>()
            .unwrap();
        assert!(pos_inf.is_infinite() && pos_inf.is_sign_positive());
    }

    #[test]
    fn fill_value_metadata_float_neg_infinity() {
        let json = r#""-Infinity""#;
        let metadata: FillValueMetadata = json.try_into().unwrap();
        assert_eq!(json, serde_json::to_string(&metadata).unwrap());
        match metadata {
            FillValueMetadata::Float(FillValueFloat::NonFinite(fill_value)) => {
                assert_eq!(fill_value, FillValueFloatStringNonFinite::NegInfinity);
            }
            _ => unreachable!(),
        }

        let neg_inf = FillValueFloat::NonFinite(FillValueFloatStringNonFinite::NegInfinity)
            .to_float::<f32>()
            .unwrap();
        assert!(neg_inf.is_infinite() && neg_inf.is_sign_negative());
    }

    #[test]
    fn fill_value_metadata_float_nan() {
        let json = r#""NaN""#;
        let metadata: FillValueMetadata = json.try_into().unwrap();
        assert_eq!(json, serde_json::to_string(&metadata).unwrap());
        match metadata {
            FillValueMetadata::Float(FillValueFloat::NonFinite(fill_value)) => {
                assert_eq!(fill_value, FillValueFloatStringNonFinite::NaN);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn fill_value_metadata_float_nan_standard() {
        let json = r#""0x7fc00000""#;
        let metadata: FillValueMetadata = json.try_into().unwrap();
        let FillValueMetadata::Float(FillValueFloat::HexString(hex_string)) = metadata else {
            unreachable!()
        };
        let fill_value: f32 = f32::from_be_bytes(hex_string.as_be_bytes().try_into().unwrap());
        assert!(fill_value.is_nan());
        let fill_value = FillValue::from(fill_value);
        let fill_value_metadata = DataType::Float32.metadata_fill_value(&fill_value);
        let FillValueMetadata::Float(FillValueFloat::NonFinite(fill_value)) = fill_value_metadata
        else {
            unreachable!()
        };
        assert_eq!(fill_value, FillValueFloatStringNonFinite::NaN);

        assert!(FillValueFloat::HexString(HexString(
            hex_string_to_be_bytes(&"0x7fc00000").unwrap()
        ))
        .to_float::<f32>()
        .unwrap()
        .is_nan());
    }

    #[test]
    fn fill_value_metadata_float_nan_nonstandard() {
        let json = r#""0x7fc00001""#;
        let metadata: FillValueMetadata = json.try_into().unwrap();
        let FillValueMetadata::Float(FillValueFloat::HexString(hex_string)) = metadata else {
            unreachable!()
        };
        let fill_value: f32 = f32::from_be_bytes(hex_string.as_be_bytes().try_into().unwrap());
        assert!(fill_value.is_nan());
        let fill_value = FillValue::from(fill_value);
        let fill_value_metadata = DataType::Float32.metadata_fill_value(&fill_value);
        let FillValueMetadata::Float(FillValueFloat::HexString(_hex_string)) = fill_value_metadata
        else {
            unreachable!()
        };
    }

    #[test]
    fn fill_value_metadata_float_hex_string() {
        let json = r#""0x7fc00000""#;
        let metadata: FillValueMetadata = json.try_into().unwrap();
        assert_eq!(json, serde_json::to_string(&metadata).unwrap());
        match metadata {
            FillValueMetadata::Float(FillValueFloat::HexString(fill_value)) => {
                assert_eq!(fill_value.0, f32::NAN.to_be_bytes());
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn fill_value_metadata_float_complex() {
        let json = r#"["0x7fc00000","NaN"]"#;
        let metadata: FillValueMetadata = json.try_into().unwrap();
        assert_eq!(json, serde_json::to_string(&metadata).unwrap());
        match metadata {
            FillValueMetadata::Complex(re, im) => {
                match re {
                    FillValueFloat::HexString(fill_value) => {
                        assert_eq!(fill_value.0, f32::NAN.to_be_bytes());
                    }
                    _ => unreachable!(),
                };
                match im {
                    FillValueFloat::NonFinite(fill_value) => {
                        assert_eq!(fill_value, FillValueFloatStringNonFinite::NaN);
                    }
                    _ => unreachable!(),
                };
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn fill_value_metadata_raw_bytes() {
        let json = r#"[0,1,2,3]"#;
        let metadata: FillValueMetadata = json.try_into().unwrap();
        assert_eq!(json, serde_json::to_string(&metadata).unwrap());
        match metadata {
            FillValueMetadata::ByteArray(fill_value) => {
                assert_eq!(fill_value, [0, 1, 2, 3]);
            }
            _ => unreachable!(),
        }
    }

    // Null is not currently supported, so recognise it as unknown fill value metadata
    #[test]
    fn fill_value_metadata_null() {
        let json = r#"null"#;
        let metadata: FillValueMetadata = json.try_into().unwrap();
        assert_eq!(json, serde_json::to_string(&metadata).unwrap());
        match metadata {
            FillValueMetadata::Unsupported(fill_value) => {
                assert!(fill_value.is_null())
            }
            _ => unreachable!(),
        }
    }

    // A negative single byte, so recognise it as unknown fill value metadata
    #[test]
    fn fill_value_metadata_neg_array1() {
        let json = r#"[-5]"#;
        let metadata: FillValueMetadata = json.try_into().unwrap();
        assert_eq!(json, serde_json::to_string(&metadata).unwrap());
        match metadata {
            FillValueMetadata::Unsupported(fill_value) => {
                assert!(fill_value.is_array())
            }
            _ => unreachable!(),
        }
    }

    // Two negative -> complex
    #[test]
    fn fill_value_metadata_neg_array2() {
        let json = r#"[-5, -5]"#;
        let metadata: FillValueMetadata = json.try_into().unwrap();
        assert_ne!(json, serde_json::to_string(&metadata).unwrap()); // [-5.0, -5.0]
        match metadata {
            FillValueMetadata::Complex(_re, _im) => {}
            _ => unreachable!(),
        }
    }

    // Single array element > u8::MAX is currently unknown
    #[test]
    fn fill_value_metadata_large_array() {
        let json = r#"[256]"#;
        let metadata: FillValueMetadata = json.try_into().unwrap();
        assert_eq!(json, serde_json::to_string(&metadata).unwrap());
        match metadata {
            FillValueMetadata::Unsupported(fill_value) => {
                assert!(fill_value.is_array())
            }
            _ => unreachable!(),
        }
    }
}
