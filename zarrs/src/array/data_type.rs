//! Zarr data types.
//!
//! See <https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html#data-types>.

use derive_more::From;
use half::{bf16, f16};
use thiserror::Error;

use crate::metadata::v3::array::{
    data_type::{DataTypeMetadataV3, DataTypeSize},
    fill_value::{
        bfloat16_to_fill_value, float16_to_fill_value, float32_to_fill_value,
        float64_to_fill_value, FillValueFloat, FillValueMetadataV3,
    },
};

use super::FillValue;

/// A data type.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
#[rustfmt::skip]
pub enum DataType {
    /// `bool` Boolean.
    Bool,
    /// `int8` Integer in `[-2^7, 2^7-1]`.
    Int8,
    /// `int16` Integer in `[-2^15, 2^15-1]`.
    Int16,
    /// `int32` Integer in `[-2^31, 2^31-1]`.
    Int32,
    /// `int64` Integer in `[-2^63, 2^63-1]`.
    Int64,
    /// `uint8` Integer in `[0, 2^8-1]`.
    UInt8,
    /// `uint16` Integer in `[0, 2^16-1]`.
    UInt16,
    /// `uint32` Integer in `[0, 2^32-1]`.
    UInt32,
    /// `uint64` Integer in `[0, 2^64-1]`.
    UInt64,
    /// `float16` IEEE 754 half-precision floating point: sign bit, 5 bits exponent, 10 bits mantissa.
    Float16,
    /// `float32` IEEE 754 single-precision floating point: sign bit, 8 bits exponent, 23 bits mantissa.
    Float32,
    /// `float64` IEEE 754 double-precision floating point: sign bit, 11 bits exponent, 52 bits mantissa.
    Float64,
    /// `bfloat16` brain floating point data type: sign bit, 5 bits exponent, 10 bits mantissa.
    BFloat16,
    /// `complex64` real and complex components are each IEEE 754 single-precision floating point.
    Complex64,
    /// `complex128` real and complex components are each IEEE 754 double-precision floating point.
    Complex128,
    /// `r*` raw bits, variable size given by *, limited to be a multiple of 8.
    RawBits(usize), // the stored usize is the size in bytes
    /// A UTF-8 encoded string.
    String,
    /// Variable-sized binary data.
    Binary,
}

/// An unsupported data type error.
#[derive(Debug, Error, From)]
#[error("unsupported data type {_0}")]
pub struct UnsupportedDataTypeError(String);

/// A fill value metadata incompatibility error.
#[derive(Debug, Error)]
#[error("incompatible fill value {1} for data type {0}")]
pub struct IncompatibleFillValueMetadataError(String, FillValueMetadataV3);

/// A fill value incompatibility error.
#[derive(Debug, Error)]
#[error("incompatible fill value {1} for data type {0}")]
pub struct IncompatibleFillValueError(String, FillValue);

impl IncompatibleFillValueError {
    /// Create a new incompatible fill value error.
    #[must_use]
    pub const fn new(data_type_name: String, fill_value: FillValue) -> Self {
        Self(data_type_name, fill_value)
    }
}

impl DataType {
    /// Returns the identifier.
    #[must_use]
    pub const fn identifier(&self) -> &'static str {
        match self {
            Self::Bool => "bool",
            Self::Int8 => "int8",
            Self::Int16 => "int16",
            Self::Int32 => "int32",
            Self::Int64 => "int64",
            Self::UInt8 => "uint8",
            Self::UInt16 => "uint16",
            Self::UInt32 => "uint32",
            Self::UInt64 => "uint64",
            Self::Float16 => "float16",
            Self::Float32 => "float32",
            Self::Float64 => "float64",
            Self::BFloat16 => "bfloat16",
            Self::Complex64 => "complex64",
            Self::Complex128 => "complex128",
            Self::RawBits(_usize) => "r*",
            Self::String => "string",
            Self::Binary => "binary",
            // Self::Extension(extension) => extension.identifier(),
        }
    }

    /// Returns the name.
    #[must_use]
    pub fn name(&self) -> String {
        match self {
            Self::RawBits(size) => format!("r{}", size * 8),
            // Self::Extension(extension) => extension.name(),
            _ => self.identifier().to_string(),
        }
    }

    /// Returns the metadata.
    #[must_use]
    pub fn metadata(&self) -> DataTypeMetadataV3 {
        match self {
            Self::Bool => DataTypeMetadataV3::Bool,
            Self::Int8 => DataTypeMetadataV3::Int8,
            Self::Int16 => DataTypeMetadataV3::Int16,
            Self::Int32 => DataTypeMetadataV3::Int32,
            Self::Int64 => DataTypeMetadataV3::Int64,
            Self::UInt8 => DataTypeMetadataV3::UInt8,
            Self::UInt16 => DataTypeMetadataV3::UInt16,
            Self::UInt32 => DataTypeMetadataV3::UInt32,
            Self::UInt64 => DataTypeMetadataV3::UInt64,
            Self::Float16 => DataTypeMetadataV3::Float16,
            Self::Float32 => DataTypeMetadataV3::Float32,
            Self::Float64 => DataTypeMetadataV3::Float64,
            Self::BFloat16 => DataTypeMetadataV3::BFloat16,
            Self::Complex64 => DataTypeMetadataV3::Complex64,
            Self::Complex128 => DataTypeMetadataV3::Complex128,
            Self::RawBits(size) => DataTypeMetadataV3::RawBits(*size),
            Self::String => DataTypeMetadataV3::String,
            Self::Binary => DataTypeMetadataV3::Binary,
        }
    }

    /// Returns the [`DataTypeSize`].
    #[must_use]
    pub const fn size(&self) -> DataTypeSize {
        match self {
            Self::Bool | Self::Int8 | Self::UInt8 => DataTypeSize::Fixed(1),
            Self::Int16 | Self::UInt16 | Self::Float16 | Self::BFloat16 => DataTypeSize::Fixed(2),
            Self::Int32 | Self::UInt32 | Self::Float32 => DataTypeSize::Fixed(4),
            Self::Int64 | Self::UInt64 | Self::Float64 | Self::Complex64 => DataTypeSize::Fixed(8),
            Self::Complex128 => DataTypeSize::Fixed(16),
            Self::RawBits(size) => DataTypeSize::Fixed(*size),
            Self::String | Self::Binary => DataTypeSize::Variable,
            // Self::Extension(extension) => extension.size(),
        }
    }

    /// Returns the size in bytes of a fixed-size data type, otherwise returns [`None`].
    #[must_use]
    pub const fn fixed_size(&self) -> Option<usize> {
        match self.size() {
            DataTypeSize::Fixed(size) => Some(size),
            DataTypeSize::Variable => None,
        }
    }

    /// Create a data type from metadata.
    ///
    /// # Errors
    ///
    /// Returns [`UnsupportedDataTypeError`] if the metadata is invalid or not associated with a registered data type plugin.
    pub fn from_metadata(metadata: &DataTypeMetadataV3) -> Result<Self, UnsupportedDataTypeError> {
        match metadata {
            DataTypeMetadataV3::Bool => Ok(Self::Bool),
            DataTypeMetadataV3::Int8 => Ok(Self::Int8),
            DataTypeMetadataV3::Int16 => Ok(Self::Int16),
            DataTypeMetadataV3::Int32 => Ok(Self::Int32),
            DataTypeMetadataV3::Int64 => Ok(Self::Int64),
            DataTypeMetadataV3::UInt8 => Ok(Self::UInt8),
            DataTypeMetadataV3::UInt16 => Ok(Self::UInt16),
            DataTypeMetadataV3::UInt32 => Ok(Self::UInt32),
            DataTypeMetadataV3::UInt64 => Ok(Self::UInt64),
            DataTypeMetadataV3::Float16 => Ok(Self::Float16),
            DataTypeMetadataV3::Float32 => Ok(Self::Float32),
            DataTypeMetadataV3::Float64 => Ok(Self::Float64),
            DataTypeMetadataV3::BFloat16 => Ok(Self::BFloat16),
            DataTypeMetadataV3::Complex64 => Ok(Self::Complex64),
            DataTypeMetadataV3::Complex128 => Ok(Self::Complex128),
            DataTypeMetadataV3::RawBits(size) => Ok(Self::RawBits(*size)),
            DataTypeMetadataV3::String => Ok(Self::String),
            DataTypeMetadataV3::Binary => Ok(Self::Binary),
            DataTypeMetadataV3::Unknown(metadata) => {
                Err(UnsupportedDataTypeError(metadata.to_string()))
            }
            _ => Err(UnsupportedDataTypeError(metadata.to_string())),
        }
    }

    /// Create a fill value from metadata.
    ///
    /// # Errors
    ///
    /// Returns [`IncompatibleFillValueMetadataError`] if the fill value is incompatible with the data type.
    pub fn fill_value_from_metadata(
        &self,
        fill_value: &FillValueMetadataV3,
    ) -> Result<FillValue, IncompatibleFillValueMetadataError> {
        use FillValue as FV;
        let err = || IncompatibleFillValueMetadataError(self.name(), fill_value.clone());
        match self {
            Self::Bool => Ok(FV::from(fill_value.try_as_bool().ok_or_else(err)?)),
            Self::Int8 => Ok(FV::from(fill_value.try_as_int::<i8>().ok_or_else(err)?)),
            Self::Int16 => Ok(FV::from(fill_value.try_as_int::<i16>().ok_or_else(err)?)),
            Self::Int32 => Ok(FV::from(fill_value.try_as_int::<i32>().ok_or_else(err)?)),
            Self::Int64 => Ok(FV::from(fill_value.try_as_int::<i64>().ok_or_else(err)?)),
            Self::UInt8 => Ok(FV::from(fill_value.try_as_uint::<u8>().ok_or_else(err)?)),
            Self::UInt16 => Ok(FV::from(fill_value.try_as_uint::<u16>().ok_or_else(err)?)),
            Self::UInt32 => Ok(FV::from(fill_value.try_as_uint::<u32>().ok_or_else(err)?)),
            Self::UInt64 => Ok(FV::from(fill_value.try_as_uint::<u64>().ok_or_else(err)?)),
            Self::Float16 => Ok(FV::from(fill_value.try_as_float16().ok_or_else(err)?)),
            Self::Float32 => Ok(FV::from(fill_value.try_as_float::<f32>().ok_or_else(err)?)),
            Self::Float64 => Ok(FV::from(fill_value.try_as_float::<f64>().ok_or_else(err)?)),
            Self::BFloat16 => Ok(FV::from(fill_value.try_as_bfloat16().ok_or_else(err)?)),
            Self::Complex64 => {
                let (re, im) = fill_value.try_as_float_pair::<f32>().ok_or_else(err)?;
                Ok(FV::from(num::complex::Complex32::new(re, im)))
            }
            Self::Complex128 => {
                let (re, im) = fill_value.try_as_float_pair::<f64>().ok_or_else(err)?;
                Ok(FV::from(num::complex::Complex64::new(re, im)))
            }
            Self::RawBits(size) => {
                if let FillValueMetadataV3::ByteArray(bytes) = fill_value {
                    if bytes.len() == *size {
                        return Ok(FillValue::new(bytes.clone()));
                    }
                }
                Err(err())
            }
            Self::Binary => {
                if let FillValueMetadataV3::ByteArray(bytes) = fill_value {
                    Ok(FillValue::new(bytes.clone()))
                } else {
                    Err(err())
                }
            }
            // Self::Extension(extension) => extension.fill_value_from_metadata(fill_value),
            Self::String => match fill_value {
                FillValueMetadataV3::String(string) => {
                    Ok(FillValue::new(string.as_bytes().to_vec()))
                }
                FillValueMetadataV3::Float(float) => match float {
                    FillValueFloat::HexString(hex_string) => Ok(String::from(hex_string).into()),
                    FillValueFloat::NonFinite(non_finite) => Ok(String::from(non_finite).into()),
                    FillValueFloat::Float(_) => Err(err()),
                },
                FillValueMetadataV3::ByteArray(bytes) => Ok(FillValue::new(bytes.clone())),
                _ => Err(err()),
            },
        }
    }

    /// Create fill value metadata.
    ///
    /// # Panics
    ///
    /// Panics if the metadata cannot be created from the fill value.
    /// This would indicate an implementation error with a data type.
    #[must_use]
    pub fn metadata_fill_value(&self, fill_value: &FillValue) -> FillValueMetadataV3 {
        let bytes = fill_value.as_ne_bytes();
        match self {
            Self::Bool => FillValueMetadataV3::Bool(bytes[0] != 0),
            Self::Int8 => {
                FillValueMetadataV3::Int(i64::from(i8::from_ne_bytes(bytes.try_into().unwrap())))
            }
            Self::Int16 => {
                FillValueMetadataV3::Int(i64::from(i16::from_ne_bytes(bytes.try_into().unwrap())))
            }
            Self::Int32 => {
                FillValueMetadataV3::Int(i64::from(i32::from_ne_bytes(bytes.try_into().unwrap())))
            }
            Self::Int64 => FillValueMetadataV3::Int(i64::from_ne_bytes(bytes.try_into().unwrap())),
            Self::UInt8 => {
                FillValueMetadataV3::UInt(u64::from(u8::from_ne_bytes(bytes.try_into().unwrap())))
            }
            Self::UInt16 => {
                FillValueMetadataV3::UInt(u64::from(u16::from_ne_bytes(bytes.try_into().unwrap())))
            }
            Self::UInt32 => {
                FillValueMetadataV3::UInt(u64::from(u32::from_ne_bytes(bytes.try_into().unwrap())))
            }
            Self::UInt64 => {
                FillValueMetadataV3::UInt(u64::from_ne_bytes(bytes.try_into().unwrap()))
            }
            Self::Float16 => {
                let fill_value = f16::from_ne_bytes(fill_value.as_ne_bytes().try_into().unwrap());
                FillValueMetadataV3::Float(float16_to_fill_value(fill_value))
            }
            Self::Float32 => FillValueMetadataV3::Float(float32_to_fill_value(f32::from_ne_bytes(
                bytes.try_into().unwrap(),
            ))),
            Self::Float64 => FillValueMetadataV3::Float(float64_to_fill_value(f64::from_ne_bytes(
                bytes.try_into().unwrap(),
            ))),
            Self::BFloat16 => {
                let fill_value = bf16::from_ne_bytes(fill_value.as_ne_bytes().try_into().unwrap());
                FillValueMetadataV3::Float(bfloat16_to_fill_value(fill_value))
            }
            Self::Complex64 => {
                let re = f32::from_ne_bytes(bytes[0..4].try_into().unwrap());
                let im = f32::from_ne_bytes(bytes[4..8].try_into().unwrap());
                FillValueMetadataV3::Complex(float32_to_fill_value(re), float32_to_fill_value(im))
            }
            Self::Complex128 => {
                let re = f64::from_ne_bytes(bytes[0..8].try_into().unwrap());
                let im = f64::from_ne_bytes(bytes[8..16].try_into().unwrap());
                FillValueMetadataV3::Complex(float64_to_fill_value(re), float64_to_fill_value(im))
            }
            Self::RawBits(size) => {
                debug_assert_eq!(fill_value.as_ne_bytes().len(), *size);
                FillValueMetadataV3::ByteArray(fill_value.as_ne_bytes().to_vec())
            }
            // DataType::Extension(extension) => extension.metadata_fill_value(fill_value),
            Self::String => FillValueMetadataV3::String(
                String::from_utf8(fill_value.as_ne_bytes().to_vec()).unwrap(),
            ),
            Self::Binary => FillValueMetadataV3::ByteArray(fill_value.as_ne_bytes().to_vec()),
        }
    }
}

impl TryFrom<DataTypeMetadataV3> for DataType {
    type Error = UnsupportedDataTypeError;

    fn try_from(metadata: DataTypeMetadataV3) -> Result<Self, Self::Error> {
        Self::from_metadata(&metadata)
    }
}

impl core::fmt::Display for DataType {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        write!(f, "{}", self.name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::metadata::v3::array::{
        fill_value::{FillValueFloatStringNonFinite, HexString},
        nan_representations::{ZARR_NAN_BF16, ZARR_NAN_F16, ZARR_NAN_F32, ZARR_NAN_F64},
    };

    #[test]
    fn data_type_unknown() {
        let json = r#""unknown""#;
        let metadata: DataTypeMetadataV3 = serde_json::from_str(json).unwrap();
        assert_eq!(
            DataType::from_metadata(&metadata).unwrap_err().to_string(),
            "unsupported data type unknown"
        );
        assert!(DataType::try_from(metadata).is_err());
    }

    #[test]
    fn data_type_bool() {
        let json = r#""bool""#;
        let metadata: DataTypeMetadataV3 = serde_json::from_str(json).unwrap();
        let data_type = DataType::from_metadata(&metadata).unwrap();
        assert_eq!(format!("{}", data_type), "bool");
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        assert_eq!(data_type, DataType::Bool);

        let metadata = serde_json::from_str::<FillValueMetadataV3>("true").unwrap();
        let fill_value = data_type.fill_value_from_metadata(&metadata).unwrap();
        assert_eq!(fill_value.as_ne_bytes(), u8::from(true).to_ne_bytes());
        assert_eq!(metadata, data_type.metadata_fill_value(&fill_value));

        let fillvalue = data_type
            .fill_value_from_metadata(
                &serde_json::from_str::<FillValueMetadataV3>("false").unwrap(),
            )
            .unwrap();
        assert_eq!(fillvalue.as_ne_bytes(), u8::from(false).to_ne_bytes());
    }

    #[test]
    fn data_type_int8() {
        let json = r#""int8""#;
        let metadata: DataTypeMetadataV3 = serde_json::from_str(json).unwrap();
        let data_type = DataType::from_metadata(&metadata).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        assert_eq!(data_type, DataType::Int8);

        let metadata = serde_json::from_str::<FillValueMetadataV3>("-7").unwrap();
        let fill_value = data_type.fill_value_from_metadata(&metadata).unwrap();
        assert_eq!(fill_value.as_ne_bytes(), (-7i8).to_ne_bytes());
        assert_eq!(metadata, data_type.metadata_fill_value(&fill_value));

        assert_eq!(
            data_type
                .fill_value_from_metadata(
                    &serde_json::from_str::<FillValueMetadataV3>("7").unwrap()
                )
                .unwrap()
                .as_ne_bytes(),
            7i8.to_ne_bytes()
        );
    }

    #[test]
    fn data_type_int16() {
        let json = r#""int16""#;
        let metadata: DataTypeMetadataV3 = serde_json::from_str(json).unwrap();
        let data_type = DataType::from_metadata(&metadata).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        assert_eq!(data_type, DataType::Int16);

        let metadata = serde_json::from_str::<FillValueMetadataV3>("-7").unwrap();
        let fill_value = data_type.fill_value_from_metadata(&metadata).unwrap();
        assert_eq!(fill_value.as_ne_bytes(), (-7i16).to_ne_bytes());
        assert_eq!(metadata, data_type.metadata_fill_value(&fill_value));

        assert_eq!(
            data_type
                .fill_value_from_metadata(
                    &serde_json::from_str::<FillValueMetadataV3>("7").unwrap()
                )
                .unwrap()
                .as_ne_bytes(),
            7i16.to_ne_bytes()
        );
    }

    #[test]
    fn data_type_int32() {
        let json = r#""int32""#;
        let metadata: DataTypeMetadataV3 = serde_json::from_str(json).unwrap();
        let data_type = DataType::from_metadata(&metadata).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        assert_eq!(data_type, DataType::Int32);

        let metadata = serde_json::from_str::<FillValueMetadataV3>("-7").unwrap();
        let fill_value = data_type.fill_value_from_metadata(&metadata).unwrap();
        assert_eq!(fill_value.as_ne_bytes(), (-7i32).to_ne_bytes());
        assert_eq!(metadata, data_type.metadata_fill_value(&fill_value));

        assert_eq!(
            data_type
                .fill_value_from_metadata(
                    &serde_json::from_str::<FillValueMetadataV3>("7").unwrap()
                )
                .unwrap()
                .as_ne_bytes(),
            7i32.to_ne_bytes()
        );
    }

    #[test]
    fn data_type_int64() {
        let json = r#""int64""#;
        let metadata: DataTypeMetadataV3 = serde_json::from_str(json).unwrap();
        let data_type = DataType::from_metadata(&metadata).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        assert_eq!(data_type, DataType::Int64);

        let metadata = serde_json::from_str::<FillValueMetadataV3>("-7").unwrap();
        let fill_value = data_type.fill_value_from_metadata(&metadata).unwrap();
        assert_eq!(fill_value.as_ne_bytes(), (-7i64).to_ne_bytes());
        assert_eq!(metadata, data_type.metadata_fill_value(&fill_value));

        assert_eq!(
            data_type
                .fill_value_from_metadata(
                    &serde_json::from_str::<FillValueMetadataV3>("7").unwrap()
                )
                .unwrap()
                .as_ne_bytes(),
            7i64.to_ne_bytes()
        );
    }

    #[test]
    fn data_type_uint8() {
        let json = r#""uint8""#;
        let metadata: DataTypeMetadataV3 = serde_json::from_str(json).unwrap();
        let data_type = DataType::from_metadata(&metadata).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        assert_eq!(data_type, DataType::UInt8);

        let metadata = serde_json::from_str::<FillValueMetadataV3>("7").unwrap();
        let fill_value = data_type.fill_value_from_metadata(&metadata).unwrap();
        assert_eq!(fill_value.as_ne_bytes(), 7u8.to_ne_bytes());
        assert_eq!(metadata, data_type.metadata_fill_value(&fill_value));
    }

    #[test]
    fn data_type_uint16() {
        let json = r#""uint16""#;
        let metadata: DataTypeMetadataV3 = serde_json::from_str(json).unwrap();
        let data_type = DataType::from_metadata(&metadata).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        assert_eq!(data_type, DataType::UInt16);

        let metadata = serde_json::from_str::<FillValueMetadataV3>("7").unwrap();
        let fill_value = data_type.fill_value_from_metadata(&metadata).unwrap();
        assert_eq!(fill_value.as_ne_bytes(), 7u16.to_ne_bytes());
        assert_eq!(metadata, data_type.metadata_fill_value(&fill_value));
    }

    #[test]
    fn data_type_uint32() {
        let json = r#""uint32""#;
        let metadata: DataTypeMetadataV3 = serde_json::from_str(json).unwrap();
        let data_type = DataType::from_metadata(&metadata).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        assert_eq!(data_type, DataType::UInt32);

        let metadata = serde_json::from_str::<FillValueMetadataV3>("7").unwrap();
        let fill_value = data_type.fill_value_from_metadata(&metadata).unwrap();
        assert_eq!(fill_value.as_ne_bytes(), 7u32.to_ne_bytes());
        assert_eq!(metadata, data_type.metadata_fill_value(&fill_value));
    }

    #[test]
    fn data_type_uint64() {
        let json = r#""uint64""#;
        let metadata: DataTypeMetadataV3 = serde_json::from_str(json).unwrap();
        let data_type = DataType::from_metadata(&metadata).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        assert_eq!(data_type, DataType::UInt64);

        let metadata = serde_json::from_str::<FillValueMetadataV3>("7").unwrap();
        let fill_value = data_type.fill_value_from_metadata(&metadata).unwrap();
        assert_eq!(fill_value.as_ne_bytes(), 7u64.to_ne_bytes());
        assert_eq!(metadata, data_type.metadata_fill_value(&fill_value));
    }

    #[test]
    fn data_type_float32() {
        let json = r#""float32""#;
        let metadata: DataTypeMetadataV3 = serde_json::from_str(json).unwrap();
        let data_type = DataType::from_metadata(&metadata).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        assert_eq!(data_type, DataType::Float32);

        let metadata = serde_json::from_str::<FillValueMetadataV3>("-7.0").unwrap();
        let fill_value = data_type.fill_value_from_metadata(&metadata).unwrap();
        assert_eq!(fill_value.as_ne_bytes(), (-7.0f32).to_ne_bytes());
        assert_eq!(metadata, data_type.metadata_fill_value(&fill_value));

        assert_eq!(
            data_type
                .fill_value_from_metadata(
                    &serde_json::from_str::<FillValueMetadataV3>(r#""NaN""#).unwrap()
                )
                .unwrap()
                .as_ne_bytes(),
            ZARR_NAN_F32.to_ne_bytes()
        );

        assert_eq!(
            data_type
                .fill_value_from_metadata(
                    &serde_json::from_str::<FillValueMetadataV3>(r#""0x7fc00000""#).unwrap()
                )
                .unwrap()
                .as_ne_bytes(),
            f32::NAN.to_ne_bytes()
        );

        assert_eq!(
            data_type
                .fill_value_from_metadata(
                    &serde_json::from_str::<FillValueMetadataV3>(r#""Infinity""#).unwrap()
                )
                .unwrap()
                .as_ne_bytes(),
            f32::INFINITY.to_ne_bytes()
        );

        assert_eq!(
            data_type
                .fill_value_from_metadata(
                    &serde_json::from_str::<FillValueMetadataV3>(r#""-Infinity""#).unwrap()
                )
                .unwrap()
                .as_ne_bytes(),
            f32::NEG_INFINITY.to_ne_bytes()
        );
    }

    #[test]
    fn data_type_float64() {
        let json = r#""float64""#;
        let metadata: DataTypeMetadataV3 = serde_json::from_str(json).unwrap();
        let data_type = DataType::from_metadata(&metadata).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        assert_eq!(data_type, DataType::Float64);

        let metadata = serde_json::from_str::<FillValueMetadataV3>("-7.0").unwrap();
        let fill_value = data_type.fill_value_from_metadata(&metadata).unwrap();
        assert_eq!(fill_value.as_ne_bytes(), (-7.0f64).to_ne_bytes());
        assert_eq!(metadata, data_type.metadata_fill_value(&fill_value));

        assert_eq!(
            data_type
                .fill_value_from_metadata(
                    &serde_json::from_str::<FillValueMetadataV3>(r#""0x7FF8000000000000""#)
                        .unwrap()
                )
                .unwrap()
                .as_ne_bytes(),
            ZARR_NAN_F64.to_ne_bytes()
        );

        assert_eq!(
            data_type
                .fill_value_from_metadata(
                    &serde_json::from_str::<FillValueMetadataV3>(r#""NaN""#).unwrap()
                )
                .unwrap()
                .as_ne_bytes(),
            ZARR_NAN_F64.to_ne_bytes()
        );

        assert_eq!(
            data_type
                .fill_value_from_metadata(
                    &serde_json::from_str::<FillValueMetadataV3>(r#""Infinity""#).unwrap()
                )
                .unwrap()
                .as_ne_bytes(),
            f64::INFINITY.to_ne_bytes()
        );

        assert_eq!(
            data_type
                .fill_value_from_metadata(
                    &serde_json::from_str::<FillValueMetadataV3>(r#""-Infinity""#).unwrap()
                )
                .unwrap()
                .as_ne_bytes(),
            f64::NEG_INFINITY.to_ne_bytes()
        );
    }

    #[test]
    fn data_type_float16() {
        use half::f16;

        let json = r#""float16""#;
        let metadata: DataTypeMetadataV3 = serde_json::from_str(json).unwrap();
        let data_type = DataType::from_metadata(&metadata).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        assert_eq!(data_type.identifier(), "float16");

        let metadata = serde_json::from_str::<FillValueMetadataV3>("-7.0").unwrap();
        let fill_value = data_type.fill_value_from_metadata(&metadata).unwrap();
        assert_eq!(
            fill_value.as_ne_bytes(),
            f16::from_f32_const(-7.0).to_ne_bytes()
        );
        assert_eq!(metadata, data_type.metadata_fill_value(&fill_value));

        assert_eq!(
            data_type
                .fill_value_from_metadata(
                    &serde_json::from_str::<FillValueMetadataV3>(r#""NaN""#).unwrap()
                )
                .unwrap()
                .as_ne_bytes(),
            f16::NAN.to_ne_bytes()
        );

        assert_eq!(
            data_type
                .fill_value_from_metadata(
                    &serde_json::from_str::<FillValueMetadataV3>(r#""Infinity""#).unwrap()
                )
                .unwrap()
                .as_ne_bytes(),
            f16::INFINITY.to_ne_bytes()
        );

        assert_eq!(
            data_type
                .fill_value_from_metadata(
                    &serde_json::from_str::<FillValueMetadataV3>(r#""-Infinity""#).unwrap()
                )
                .unwrap()
                .as_ne_bytes(),
            f16::NEG_INFINITY.to_ne_bytes()
        );
    }

    #[test]
    fn data_type_bfloat16() {
        use half::bf16;

        let json = r#""bfloat16""#;
        let metadata: DataTypeMetadataV3 = serde_json::from_str(json).unwrap();
        let data_type = DataType::from_metadata(&metadata).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        assert_eq!(data_type.identifier(), "bfloat16");

        let metadata = serde_json::from_str::<FillValueMetadataV3>("-7.0").unwrap();
        let fill_value = data_type.fill_value_from_metadata(&metadata).unwrap();
        assert_eq!(
            fill_value.as_ne_bytes(),
            bf16::from_f32_const(-7.0).to_ne_bytes()
        );
        assert_eq!(metadata, data_type.metadata_fill_value(&fill_value));

        assert_eq!(
            data_type
                .fill_value_from_metadata(
                    // &serde_json::from_str::<FillValueMetadataV3>(r#""0x7E00""#).unwrap()
                    &serde_json::from_str::<FillValueMetadataV3>(r#""0x7FC0""#).unwrap()
                )
                .unwrap()
                .as_ne_bytes(),
            ZARR_NAN_BF16.to_ne_bytes()
        );

        assert_eq!(
            data_type
                .fill_value_from_metadata(
                    &serde_json::from_str::<FillValueMetadataV3>(r#""NaN""#).unwrap()
                )
                .unwrap()
                .as_ne_bytes(),
            ZARR_NAN_BF16.to_ne_bytes()
        );

        assert_eq!(
            data_type
                .fill_value_from_metadata(
                    &serde_json::from_str::<FillValueMetadataV3>(r#""Infinity""#).unwrap()
                )
                .unwrap()
                .as_ne_bytes(),
            bf16::INFINITY.to_ne_bytes()
        );

        assert_eq!(
            data_type
                .fill_value_from_metadata(
                    &serde_json::from_str::<FillValueMetadataV3>(r#""-Infinity""#).unwrap()
                )
                .unwrap()
                .as_ne_bytes(),
            bf16::NEG_INFINITY.to_ne_bytes()
        );
    }

    #[test]
    fn data_type_complex64() {
        let json = r#""complex64""#;
        let metadata: DataTypeMetadataV3 = serde_json::from_str(json).unwrap();
        let data_type = DataType::from_metadata(&metadata).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        assert_eq!(data_type, DataType::Complex64);

        let metadata =
            serde_json::from_str::<FillValueMetadataV3>(r#"[-7.0, "Infinity"]"#).unwrap();
        let fill_value = data_type.fill_value_from_metadata(&metadata).unwrap();
        assert_eq!(
            fill_value.as_ne_bytes(),
            (-7.0f32)
                .to_ne_bytes()
                .iter()
                .chain(f32::INFINITY.to_ne_bytes().iter())
                .copied()
                .collect::<Vec<u8>>()
        );
        assert_eq!(metadata, data_type.metadata_fill_value(&fill_value));
    }

    #[test]
    fn data_type_complex128() {
        let json = r#""complex128""#;
        let metadata: DataTypeMetadataV3 = serde_json::from_str(json).unwrap();
        let data_type = DataType::from_metadata(&metadata).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        assert_eq!(data_type, DataType::Complex128);

        let metadata =
            serde_json::from_str::<FillValueMetadataV3>(r#"[-7.0, "Infinity"]"#).unwrap();
        let fill_value = data_type.fill_value_from_metadata(&metadata).unwrap();
        assert_eq!(
            fill_value.as_ne_bytes(),
            (-7.0f64)
                .to_ne_bytes()
                .iter()
                .chain(f64::INFINITY.to_ne_bytes().iter())
                .copied()
                .collect::<Vec<u8>>()
        );
        assert_eq!(metadata, data_type.metadata_fill_value(&fill_value));
    }

    #[test]
    fn data_type_r8() {
        let json = r#""r8""#;
        let metadata: DataTypeMetadataV3 = serde_json::from_str(json).unwrap();
        let data_type = DataType::from_metadata(&metadata).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        assert_eq!(data_type.identifier(), "r*");
        assert_eq!(data_type.name().as_str(), "r8");
        assert_eq!(data_type.size(), DataTypeSize::Fixed(1));

        let metadata = serde_json::from_str::<FillValueMetadataV3>("[7]").unwrap();
        let fill_value = data_type.fill_value_from_metadata(&metadata).unwrap();
        assert_eq!(fill_value.as_ne_bytes(), 7u8.to_ne_bytes());
        assert_eq!(metadata, data_type.metadata_fill_value(&fill_value));
    }

    #[test]
    fn data_type_r16() {
        let json = r#""r16""#;
        let metadata: DataTypeMetadataV3 = serde_json::from_str(json).unwrap();
        let data_type = DataType::from_metadata(&metadata).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        assert_eq!(data_type.identifier(), "r*");
        assert_eq!(data_type.name().as_str(), "r16");
        assert_eq!(data_type.size(), DataTypeSize::Fixed(2));

        let metadata = serde_json::from_str::<FillValueMetadataV3>("[0, 255]").unwrap();
        let fill_value = data_type.fill_value_from_metadata(&metadata).unwrap();
        assert_eq!(
            fill_value.as_ne_bytes(), // NOTE: Raw value bytes are always read as-is.
            &[0u8, 255u8]
        );
        assert_eq!(metadata, data_type.metadata_fill_value(&fill_value));
    }

    #[test]
    fn data_type_unknown1() {
        let json = r#"
    {
        "name": "datetime",
        "configuration": {
            "unit": "ns"
        }
    }"#;
        let metadata = serde_json::from_str::<DataTypeMetadataV3>(json).unwrap();
        println!("{json:?}");
        println!("{metadata:?}");
        assert_eq!(metadata.name(), "datetime");
        assert!(DataType::from_metadata(&metadata).is_err());
    }

    #[test]
    fn data_type_unknown2() {
        let json = r#""datetime""#;
        let metadata: DataTypeMetadataV3 = serde_json::from_str(json).unwrap();
        println!("{json:?}");
        println!("{metadata:?}");
        assert_eq!(metadata.name(), "datetime");
        assert!(DataType::from_metadata(&metadata).is_err());
    }

    #[test]
    fn data_type_unknown3() {
        let json = r#""ra""#;
        let metadata: DataTypeMetadataV3 = serde_json::from_str(json).unwrap();
        println!("{json:?}");
        println!("{metadata:?}");
        assert_eq!(metadata.name(), "ra");
        assert!(DataType::from_metadata(&metadata).is_err());
    }

    #[test]
    fn data_type_invalid() {
        let json = r#"
    {
        "name": "datetime",
        "notconfiguration": {
            "unit": "ns"
        }
    }"#;
        assert!(serde_json::from_str::<DataTypeMetadataV3>(json).is_err());
    }

    #[test]
    fn data_type_raw_bits1() {
        let json = r#""r16""#;
        let metadata = serde_json::from_str::<DataTypeMetadataV3>(json).unwrap();
        let data_type: DataType = DataType::from_metadata(&metadata).unwrap();
        assert_eq!(data_type.size(), DataTypeSize::Fixed(2));
    }

    #[test]
    fn data_type_raw_bits2() {
        let json = r#"
    {
        "name": "r16"
    }"#;
        let metadata = serde_json::from_str::<DataTypeMetadataV3>(json).unwrap();
        let data_type: DataType = DataType::from_metadata(&metadata).unwrap();
        assert_eq!(data_type.size(), DataTypeSize::Fixed(2));
    }

    #[test]
    fn data_type_raw_bits_failure1() {
        let json = r#"
    {
        "name": "r5"
    }"#;
        let metadata = serde_json::from_str::<DataTypeMetadataV3>(json).unwrap();
        assert!(DataType::from_metadata(&metadata).is_err());
    }

    #[test]
    fn incompatible_fill_value_metadata() {
        let json = r#""bool""#;
        let metadata: DataTypeMetadataV3 = serde_json::from_str(json).unwrap();
        let data_type = DataType::from_metadata(&metadata).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        assert_eq!(data_type, DataType::Bool);

        let metadata = serde_json::from_str::<FillValueMetadataV3>("1").unwrap();
        assert_eq!(
            data_type
                .fill_value_from_metadata(&metadata)
                .unwrap_err()
                .to_string(),
            "incompatible fill value 1 for data type bool"
        );
    }

    #[test]
    fn incompatible_raw_bits_metadata() {
        let json = r#""r16""#;
        let metadata: DataTypeMetadataV3 = serde_json::from_str(json).unwrap();
        let data_type = DataType::from_metadata(&metadata).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        assert_eq!(data_type, DataType::RawBits(2));

        let metadata = serde_json::from_str::<FillValueMetadataV3>("[123]").unwrap();
        assert_eq!(
            data_type
                .fill_value_from_metadata(&metadata)
                .unwrap_err()
                .to_string(),
            "incompatible fill value [123] for data type r16"
        );
    }

    #[test]
    fn float_fill_value() {
        assert_eq!(
            float16_to_fill_value(f16::INFINITY),
            FillValueFloat::NonFinite(FillValueFloatStringNonFinite::PosInfinity)
        );
        assert_eq!(
            float16_to_fill_value(f16::NEG_INFINITY),
            FillValueFloat::NonFinite(FillValueFloatStringNonFinite::NegInfinity)
        );
        assert_eq!(
            float16_to_fill_value(ZARR_NAN_F16),
            FillValueFloat::NonFinite(FillValueFloatStringNonFinite::NaN)
        );
        let f16_nan_alt = unsafe { std::mem::transmute::<u16, f16>(0b01_11111_000000001) };
        assert!(f16_nan_alt.is_nan());
        assert_eq!(
            float16_to_fill_value(f16_nan_alt),
            FillValueFloat::HexString(HexString::new(vec![126, 1]))
        );
        assert_eq!(
            bfloat16_to_fill_value(bf16::INFINITY),
            FillValueFloat::NonFinite(FillValueFloatStringNonFinite::PosInfinity)
        );
        assert_eq!(
            bfloat16_to_fill_value(bf16::NEG_INFINITY),
            FillValueFloat::NonFinite(FillValueFloatStringNonFinite::NegInfinity)
        );
        assert_eq!(
            bfloat16_to_fill_value(ZARR_NAN_BF16),
            FillValueFloat::NonFinite(FillValueFloatStringNonFinite::NaN)
        );
        let bf16_nan_alt = unsafe { std::mem::transmute::<u16, bf16>(0b0_01111_11111000001) };
        assert!(bf16_nan_alt.is_nan());
        assert_eq!(
            bfloat16_to_fill_value(bf16_nan_alt),
            FillValueFloat::HexString(HexString::new(vec![127, 193]))
        );
        assert_eq!(
            float32_to_fill_value(f32::INFINITY),
            FillValueFloat::NonFinite(FillValueFloatStringNonFinite::PosInfinity)
        );
        assert_eq!(
            float32_to_fill_value(f32::NEG_INFINITY),
            FillValueFloat::NonFinite(FillValueFloatStringNonFinite::NegInfinity)
        );
        assert_eq!(
            float32_to_fill_value(ZARR_NAN_F32),
            FillValueFloat::NonFinite(FillValueFloatStringNonFinite::NaN)
        );
        let f32_nan_alt =
            unsafe { std::mem::transmute::<u32, f32>(0b0_11111111_10000000000000000000001) };
        assert!(f32_nan_alt.is_nan());
        assert_eq!(
            float32_to_fill_value(f32_nan_alt),
            FillValueFloat::HexString(HexString::new(vec![127, 192, 0, 1]))
        );
        assert_eq!(
            float64_to_fill_value(f64::INFINITY),
            FillValueFloat::NonFinite(FillValueFloatStringNonFinite::PosInfinity)
        );
        assert_eq!(
            float64_to_fill_value(f64::NEG_INFINITY),
            FillValueFloat::NonFinite(FillValueFloatStringNonFinite::NegInfinity)
        );
        assert_eq!(
            float64_to_fill_value(ZARR_NAN_F64),
            FillValueFloat::NonFinite(FillValueFloatStringNonFinite::NaN)
        );
        let f64_nan_alt = unsafe {
            std::mem::transmute::<u64, f64>(
                0b0_11111111111_1000000000000000000000000000000000000000000000000001,
            )
        };
        assert!(f64_nan_alt.is_nan());
        assert_eq!(
            float64_to_fill_value(f64_nan_alt),
            FillValueFloat::HexString(HexString::new(vec![127, 248, 0, 0, 0, 0, 0, 1]))
        );
    }

    #[test]
    fn incompatible_fill_value() {
        let err = IncompatibleFillValueError::new("bool".to_string(), FillValue::from(1.0f32));
        assert_eq!(
            err.to_string(),
            "incompatible fill value [0, 0, 128, 63] for data type bool"
        );
    }

    #[test]
    fn fill_value_from_metadata_failure() {
        let metadata = serde_json::from_str::<FillValueMetadataV3>("1").unwrap();
        assert!(DataType::Bool.fill_value_from_metadata(&metadata).is_err());
        let metadata = serde_json::from_str::<FillValueMetadataV3>("false").unwrap();
        assert!(DataType::Int8.fill_value_from_metadata(&metadata).is_err());
        assert!(DataType::Int16.fill_value_from_metadata(&metadata).is_err());
        assert!(DataType::Int32.fill_value_from_metadata(&metadata).is_err());
        assert!(DataType::Int64.fill_value_from_metadata(&metadata).is_err());
        assert!(DataType::UInt8.fill_value_from_metadata(&metadata).is_err());
        assert!(DataType::UInt16
            .fill_value_from_metadata(&metadata)
            .is_err());
        assert!(DataType::UInt32
            .fill_value_from_metadata(&metadata)
            .is_err());
        assert!(DataType::UInt64
            .fill_value_from_metadata(&metadata)
            .is_err());
        assert!(DataType::Float16
            .fill_value_from_metadata(&metadata)
            .is_err());
        assert!(DataType::Float32
            .fill_value_from_metadata(&metadata)
            .is_err());
        assert!(DataType::Float64
            .fill_value_from_metadata(&metadata)
            .is_err());
        assert!(DataType::BFloat16
            .fill_value_from_metadata(&metadata)
            .is_err());
        assert!(DataType::Complex64
            .fill_value_from_metadata(&metadata)
            .is_err());
        assert!(DataType::Complex128
            .fill_value_from_metadata(&metadata)
            .is_err());
        assert!(DataType::RawBits(1)
            .fill_value_from_metadata(&metadata)
            .is_err());
    }

    #[test]
    fn data_type_string() {
        let json = r#""string""#;
        let metadata: DataTypeMetadataV3 = serde_json::from_str(json).unwrap();
        let data_type = DataType::from_metadata(&metadata).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        assert_eq!(data_type.identifier(), "string");
        assert_eq!(data_type.name().as_str(), "string");
        assert_eq!(data_type.size(), DataTypeSize::Variable);

        let metadata = serde_json::from_str::<FillValueMetadataV3>(r#""hello world""#).unwrap();
        let fill_value = data_type.fill_value_from_metadata(&metadata).unwrap();
        assert_eq!(fill_value.as_ne_bytes(), "hello world".as_bytes(),);
        assert_eq!(metadata, data_type.metadata_fill_value(&fill_value));

        let metadata = serde_json::from_str::<FillValueMetadataV3>(
            r#"[104, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100]"#,
        )
        .unwrap();
        let fill_value = data_type.fill_value_from_metadata(&metadata).unwrap();
        assert_eq!(fill_value.as_ne_bytes(), "hello world".as_bytes(),);
        assert_ne!(metadata, data_type.metadata_fill_value(&fill_value)); // metadata is byte array rep, that is okay

        let metadata = serde_json::from_str::<FillValueMetadataV3>(r#""Infinity""#).unwrap();
        let fill_value = data_type.fill_value_from_metadata(&metadata).unwrap();
        assert_eq!(fill_value.as_ne_bytes(), "Infinity".as_bytes(),);
        assert_ne!(metadata, data_type.metadata_fill_value(&fill_value)); // metadata is float rep, that is okay

        let metadata = serde_json::from_str::<FillValueMetadataV3>(r#""0x7fc00000""#).unwrap();
        let fill_value = data_type.fill_value_from_metadata(&metadata).unwrap();
        assert_eq!(fill_value.as_ne_bytes(), "0x7fc00000".as_bytes(),);
        assert_ne!(metadata, data_type.metadata_fill_value(&fill_value)); // metadata is float rep, that is okay
    }
}
