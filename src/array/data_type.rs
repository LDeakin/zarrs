//! Zarr data types.
//!
//! See <https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html#data-types>.

use derive_more::From;
use half::{bf16, f16};
use serde::de::Error;
use thiserror::Error;

use crate::{
    array::{ZARR_NAN_BF16, ZARR_NAN_F16, ZARR_NAN_F32, ZARR_NAN_F64},
    metadata::{
        v3::fill_value::{
            FillValueFloat, FillValueFloatStringNonFinite, FillValueMetadata, HexString,
        },
        v3::MetadataV3,
    },
};

use super::FillValue;

/// A data type.
#[derive(Clone, Debug)]
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

    // /// An extension data type.
    // Extension(Box<dyn DataTypeExtension>),
}

/// An unsupported data type error.
#[derive(Debug, Error, From)]
#[error("unsupported data type {_0}")]
pub struct UnsupportedDataTypeError(String);

impl PartialEq for DataType {
    fn eq(&self, other: &Self) -> bool {
        self.name() == other.name()
    }
}

impl Eq for DataType {}

impl<'de> serde::Deserialize<'de> for DataType {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let metadata = MetadataV3::deserialize(d)?;
        Self::from_metadata(&metadata).map_err(|err| D::Error::custom(err.to_string()))
    }
}

// /// A data type plugin.
// pub type DataTypePlugin = Plugin<Box<dyn DataTypeExtension>>;
// inventory::collect!(DataTypePlugin);

/// A fill value metadata incompatibility error.
#[derive(Debug, Error)]
#[error("incompatible fill value {1} for data type {0}")]
pub struct IncompatibleFillValueMetadataError(String, FillValueMetadata);

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

/// The size of a data type.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum DataTypeSize {
    /// Fixed size (in bytes).
    Fixed(usize),
    /// Variable sized.
    ///
    /// <https://github.com/zarr-developers/zeps/pull/47>
    Variable,
}

/// Extension data type traits.
pub trait DataTypeExtension: dyn_clone::DynClone + core::fmt::Debug + Send + Sync {
    /// Returns the identifier.
    fn identifier(&self) -> &'static str;

    /// Returns the name.
    fn name(&self) -> String;

    /// Returns the data type size in bytes.
    fn size(&self) -> DataTypeSize;

    /// Returns the data type metadata.
    fn metadata(&self) -> MetadataV3;

    /// Create a fill value from metadata.
    ///
    /// # Errors
    ///
    /// Returns [`IncompatibleFillValueMetadataError`] if the fill value is incompatible with the data type.
    fn fill_value_from_metadata(
        &self,
        fill_value: &FillValueMetadata,
    ) -> Result<FillValue, IncompatibleFillValueMetadataError>;

    /// Return the fill value metadata.
    #[must_use]
    fn metadata_fill_value(&self, fill_value: &FillValue) -> FillValueMetadata;
}

dyn_clone::clone_trait_object!(DataTypeExtension);

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
    pub fn metadata(&self) -> MetadataV3 {
        MetadataV3::new(&self.name())
        // match self {
        //     // Self::Extension(extension) => extension.metadata(),
        //     _ => MetadataV3::new(&self.name()),
        // }
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
    pub fn from_metadata(metadata: &MetadataV3) -> Result<Self, UnsupportedDataTypeError> {
        let name = metadata.name();

        match name {
            "bool" => return Ok(Self::Bool),
            "int8" => return Ok(Self::Int8),
            "int16" => return Ok(Self::Int16),
            "int32" => return Ok(Self::Int32),
            "int64" => return Ok(Self::Int64),
            "uint8" => return Ok(Self::UInt8),
            "uint16" => return Ok(Self::UInt16),
            "uint32" => return Ok(Self::UInt32),
            "uint64" => return Ok(Self::UInt64),
            "float16" => return Ok(Self::Float16),
            "float32" => return Ok(Self::Float32),
            "float64" => return Ok(Self::Float64),
            "bfloat16" => return Ok(Self::BFloat16),
            "complex64" => return Ok(Self::Complex64),
            "complex128" => return Ok(Self::Complex128),
            "string" => return Ok(Self::String),
            "binary" => return Ok(Self::Binary),
            _ => {}
        };

        if name.starts_with('r') {
            if let Ok(size_bits) = metadata.name()[1..].parse::<usize>() {
                if size_bits % 8 == 0 {
                    let size_bytes = size_bits / 8;
                    return Ok(Self::RawBits(size_bytes));
                }
            }
        }

        Err(UnsupportedDataTypeError(name.to_string()))

        // for plugin in inventory::iter::<DataTypePlugin> {
        //     if plugin.match_name(metadata.name()) {
        //         return Ok(DataType::Extension(plugin.create(metadata)?));
        //     }
        // }
        // Err(PluginCreateError::Unsupported {
        //     name: metadata.name().to_string(),
        //     plugin_type: "data type".to_string(),
        // })
    }

    /// Create a fill value from metadata.
    ///
    /// # Errors
    ///
    /// Returns [`IncompatibleFillValueMetadataError`] if the fill value is incompatible with the data type.
    pub fn fill_value_from_metadata(
        &self,
        fill_value: &FillValueMetadata,
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
                if let FillValueMetadata::ByteArray(bytes) = fill_value {
                    if bytes.len() == *size {
                        return Ok(FillValue::new(bytes.clone()));
                    }
                }
                Err(err())
            }
            Self::Binary => {
                if let FillValueMetadata::ByteArray(bytes) = fill_value {
                    Ok(FillValue::new(bytes.clone()))
                } else {
                    Err(err())
                }
            }
            // Self::Extension(extension) => extension.fill_value_from_metadata(fill_value),
            Self::String => match fill_value {
                FillValueMetadata::String(string) => Ok(FillValue::new(string.as_bytes().to_vec())),
                FillValueMetadata::Float(float) => match float {
                    FillValueFloat::HexString(hex_string) => Ok(String::from(hex_string).into()),
                    FillValueFloat::NonFinite(non_finite) => Ok(String::from(non_finite).into()),
                    FillValueFloat::Float(_) => Err(err()),
                },
                FillValueMetadata::ByteArray(bytes) => Ok(FillValue::new(bytes.clone())),
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
    pub fn metadata_fill_value(&self, fill_value: &FillValue) -> FillValueMetadata {
        let bytes = fill_value.as_ne_bytes();
        match self {
            Self::Bool => FillValueMetadata::Bool(bytes[0] != 0),
            Self::Int8 => {
                FillValueMetadata::Int(i64::from(i8::from_ne_bytes(bytes.try_into().unwrap())))
            }
            Self::Int16 => {
                FillValueMetadata::Int(i64::from(i16::from_ne_bytes(bytes.try_into().unwrap())))
            }
            Self::Int32 => {
                FillValueMetadata::Int(i64::from(i32::from_ne_bytes(bytes.try_into().unwrap())))
            }
            Self::Int64 => FillValueMetadata::Int(i64::from_ne_bytes(bytes.try_into().unwrap())),
            Self::UInt8 => {
                FillValueMetadata::UInt(u64::from(u8::from_ne_bytes(bytes.try_into().unwrap())))
            }
            Self::UInt16 => {
                FillValueMetadata::UInt(u64::from(u16::from_ne_bytes(bytes.try_into().unwrap())))
            }
            Self::UInt32 => {
                FillValueMetadata::UInt(u64::from(u32::from_ne_bytes(bytes.try_into().unwrap())))
            }
            Self::UInt64 => FillValueMetadata::UInt(u64::from_ne_bytes(bytes.try_into().unwrap())),
            Self::Float16 => {
                let fill_value = f16::from_ne_bytes(fill_value.as_ne_bytes().try_into().unwrap());
                FillValueMetadata::Float(float16_to_fill_value(fill_value))
            }
            Self::Float32 => FillValueMetadata::Float(float32_to_fill_value(f32::from_ne_bytes(
                bytes.try_into().unwrap(),
            ))),
            Self::Float64 => FillValueMetadata::Float(float64_to_fill_value(f64::from_ne_bytes(
                bytes.try_into().unwrap(),
            ))),
            Self::BFloat16 => {
                let fill_value = bf16::from_ne_bytes(fill_value.as_ne_bytes().try_into().unwrap());
                FillValueMetadata::Float(bfloat16_to_fill_value(fill_value))
            }
            Self::Complex64 => {
                let re = f32::from_ne_bytes(bytes[0..4].try_into().unwrap());
                let im = f32::from_ne_bytes(bytes[4..8].try_into().unwrap());
                FillValueMetadata::Complex(float32_to_fill_value(re), float32_to_fill_value(im))
            }
            Self::Complex128 => {
                let re = f64::from_ne_bytes(bytes[0..8].try_into().unwrap());
                let im = f64::from_ne_bytes(bytes[8..16].try_into().unwrap());
                FillValueMetadata::Complex(float64_to_fill_value(re), float64_to_fill_value(im))
            }
            Self::RawBits(size) => {
                debug_assert_eq!(fill_value.as_ne_bytes().len(), *size);
                FillValueMetadata::ByteArray(fill_value.as_ne_bytes().to_vec())
            }
            // DataType::Extension(extension) => extension.metadata_fill_value(fill_value),
            Self::String => FillValueMetadata::String(
                String::from_utf8(fill_value.as_ne_bytes().to_vec()).unwrap(),
            ),
            Self::Binary => FillValueMetadata::ByteArray(fill_value.as_ne_bytes().to_vec()),
        }
    }
}

fn float32_to_fill_value(f: f32) -> FillValueFloat {
    if f.is_infinite() && f.is_sign_positive() {
        FillValueFloatStringNonFinite::PosInfinity.into()
    } else if f.is_infinite() && f.is_sign_negative() {
        FillValueFloatStringNonFinite::NegInfinity.into()
    } else if f.to_bits() == ZARR_NAN_F32.to_bits() {
        FillValueFloatStringNonFinite::NaN.into()
    } else if f.is_nan() {
        HexString::from(f.to_be_bytes().to_vec()).into()
    } else {
        f64::from(f).into()
    }
}

fn float64_to_fill_value(f: f64) -> FillValueFloat {
    if f.is_infinite() && f.is_sign_positive() {
        FillValueFloatStringNonFinite::PosInfinity.into()
    } else if f.is_infinite() && f.is_sign_negative() {
        FillValueFloatStringNonFinite::NegInfinity.into()
    } else if f.to_bits() == ZARR_NAN_F64.to_bits() {
        FillValueFloatStringNonFinite::NaN.into()
    } else if f.is_nan() {
        HexString::from(f.to_be_bytes().to_vec()).into()
    } else {
        f.into()
    }
}

fn float16_to_fill_value(f: f16) -> FillValueFloat {
    if f.is_infinite() && f.is_sign_positive() {
        FillValueFloatStringNonFinite::PosInfinity.into()
    } else if f.is_infinite() && f.is_sign_negative() {
        FillValueFloatStringNonFinite::NegInfinity.into()
    } else if f.to_bits() == ZARR_NAN_F16.to_bits() {
        FillValueFloatStringNonFinite::NaN.into()
    } else if f.is_nan() {
        HexString::from(f.to_be_bytes().to_vec()).into()
    } else {
        f64::from(f).into()
    }
}

fn bfloat16_to_fill_value(f: bf16) -> FillValueFloat {
    if f.is_infinite() && f.is_sign_positive() {
        FillValueFloatStringNonFinite::PosInfinity.into()
    } else if f.is_infinite() && f.is_sign_negative() {
        FillValueFloatStringNonFinite::NegInfinity.into()
    } else if f.to_bits() == ZARR_NAN_BF16.to_bits() {
        FillValueFloatStringNonFinite::NaN.into()
    } else if f.is_nan() {
        HexString::from(f.to_be_bytes().to_vec()).into()
    } else {
        f64::from(f).into()
    }
}

impl TryFrom<MetadataV3> for DataType {
    type Error = UnsupportedDataTypeError;

    fn try_from(metadata: MetadataV3) -> Result<Self, Self::Error> {
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

    #[test]
    fn data_type_unknown() {
        let json = r#""unknown""#;
        let metadata: MetadataV3 = serde_json::from_str(json).unwrap();
        assert_eq!(
            DataType::from_metadata(&metadata).unwrap_err().to_string(),
            "unsupported data type unknown"
        );
        assert!(DataType::try_from(metadata).is_err());
    }

    #[test]
    fn data_type_bool() {
        let json = r#""bool""#;
        let metadata: MetadataV3 = serde_json::from_str(json).unwrap();
        let data_type = DataType::from_metadata(&metadata).unwrap();
        assert_eq!(format!("{}", data_type), "bool");
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        assert_eq!(data_type, DataType::Bool);

        let metadata = serde_json::from_str::<FillValueMetadata>("true").unwrap();
        let fill_value = data_type.fill_value_from_metadata(&metadata).unwrap();
        assert_eq!(fill_value.as_ne_bytes(), u8::from(true).to_ne_bytes());
        assert_eq!(metadata, data_type.metadata_fill_value(&fill_value));

        let fillvalue = data_type
            .fill_value_from_metadata(&serde_json::from_str::<FillValueMetadata>("false").unwrap())
            .unwrap();
        assert_eq!(fillvalue.as_ne_bytes(), u8::from(false).to_ne_bytes());
    }

    #[test]
    fn data_type_int8() {
        let json = r#""int8""#;
        let metadata: MetadataV3 = serde_json::from_str(json).unwrap();
        let data_type = DataType::from_metadata(&metadata).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        assert_eq!(data_type, DataType::Int8);

        let metadata = serde_json::from_str::<FillValueMetadata>("-7").unwrap();
        let fill_value = data_type.fill_value_from_metadata(&metadata).unwrap();
        assert_eq!(fill_value.as_ne_bytes(), (-7i8).to_ne_bytes());
        assert_eq!(metadata, data_type.metadata_fill_value(&fill_value));

        assert_eq!(
            data_type
                .fill_value_from_metadata(&serde_json::from_str::<FillValueMetadata>("7").unwrap())
                .unwrap()
                .as_ne_bytes(),
            7i8.to_ne_bytes()
        );
    }

    #[test]
    fn data_type_int16() {
        let json = r#""int16""#;
        let metadata: MetadataV3 = serde_json::from_str(json).unwrap();
        let data_type = DataType::from_metadata(&metadata).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        assert_eq!(data_type, DataType::Int16);

        let metadata = serde_json::from_str::<FillValueMetadata>("-7").unwrap();
        let fill_value = data_type.fill_value_from_metadata(&metadata).unwrap();
        assert_eq!(fill_value.as_ne_bytes(), (-7i16).to_ne_bytes());
        assert_eq!(metadata, data_type.metadata_fill_value(&fill_value));

        assert_eq!(
            data_type
                .fill_value_from_metadata(&serde_json::from_str::<FillValueMetadata>("7").unwrap())
                .unwrap()
                .as_ne_bytes(),
            7i16.to_ne_bytes()
        );
    }

    #[test]
    fn data_type_int32() {
        let json = r#""int32""#;
        let metadata: MetadataV3 = serde_json::from_str(json).unwrap();
        let data_type = DataType::from_metadata(&metadata).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        assert_eq!(data_type, DataType::Int32);

        let metadata = serde_json::from_str::<FillValueMetadata>("-7").unwrap();
        let fill_value = data_type.fill_value_from_metadata(&metadata).unwrap();
        assert_eq!(fill_value.as_ne_bytes(), (-7i32).to_ne_bytes());
        assert_eq!(metadata, data_type.metadata_fill_value(&fill_value));

        assert_eq!(
            data_type
                .fill_value_from_metadata(&serde_json::from_str::<FillValueMetadata>("7").unwrap())
                .unwrap()
                .as_ne_bytes(),
            7i32.to_ne_bytes()
        );
    }

    #[test]
    fn data_type_int64() {
        let json = r#""int64""#;
        let metadata: MetadataV3 = serde_json::from_str(json).unwrap();
        let data_type = DataType::from_metadata(&metadata).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        assert_eq!(data_type, DataType::Int64);

        let metadata = serde_json::from_str::<FillValueMetadata>("-7").unwrap();
        let fill_value = data_type.fill_value_from_metadata(&metadata).unwrap();
        assert_eq!(fill_value.as_ne_bytes(), (-7i64).to_ne_bytes());
        assert_eq!(metadata, data_type.metadata_fill_value(&fill_value));

        assert_eq!(
            data_type
                .fill_value_from_metadata(&serde_json::from_str::<FillValueMetadata>("7").unwrap())
                .unwrap()
                .as_ne_bytes(),
            7i64.to_ne_bytes()
        );
    }

    #[test]
    fn data_type_uint8() {
        let json = r#""uint8""#;
        let metadata: MetadataV3 = serde_json::from_str(json).unwrap();
        let data_type = DataType::from_metadata(&metadata).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        assert_eq!(data_type, DataType::UInt8);

        let metadata = serde_json::from_str::<FillValueMetadata>("7").unwrap();
        let fill_value = data_type.fill_value_from_metadata(&metadata).unwrap();
        assert_eq!(fill_value.as_ne_bytes(), 7u8.to_ne_bytes());
        assert_eq!(metadata, data_type.metadata_fill_value(&fill_value));
    }

    #[test]
    fn data_type_uint16() {
        let json = r#""uint16""#;
        let metadata: MetadataV3 = serde_json::from_str(json).unwrap();
        let data_type = DataType::from_metadata(&metadata).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        assert_eq!(data_type, DataType::UInt16);

        let metadata = serde_json::from_str::<FillValueMetadata>("7").unwrap();
        let fill_value = data_type.fill_value_from_metadata(&metadata).unwrap();
        assert_eq!(fill_value.as_ne_bytes(), 7u16.to_ne_bytes());
        assert_eq!(metadata, data_type.metadata_fill_value(&fill_value));
    }

    #[test]
    fn data_type_uint32() {
        let json = r#""uint32""#;
        let metadata: MetadataV3 = serde_json::from_str(json).unwrap();
        let data_type = DataType::from_metadata(&metadata).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        assert_eq!(data_type, DataType::UInt32);

        let metadata = serde_json::from_str::<FillValueMetadata>("7").unwrap();
        let fill_value = data_type.fill_value_from_metadata(&metadata).unwrap();
        assert_eq!(fill_value.as_ne_bytes(), 7u32.to_ne_bytes());
        assert_eq!(metadata, data_type.metadata_fill_value(&fill_value));
    }

    #[test]
    fn data_type_uint64() {
        let json = r#""uint64""#;
        let metadata: MetadataV3 = serde_json::from_str(json).unwrap();
        let data_type = DataType::from_metadata(&metadata).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        assert_eq!(data_type, DataType::UInt64);

        let metadata = serde_json::from_str::<FillValueMetadata>("7").unwrap();
        let fill_value = data_type.fill_value_from_metadata(&metadata).unwrap();
        assert_eq!(fill_value.as_ne_bytes(), 7u64.to_ne_bytes());
        assert_eq!(metadata, data_type.metadata_fill_value(&fill_value));
    }

    #[test]
    fn data_type_float32() {
        let json = r#""float32""#;
        let metadata: MetadataV3 = serde_json::from_str(json).unwrap();
        let data_type = DataType::from_metadata(&metadata).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        assert_eq!(data_type, DataType::Float32);

        let metadata = serde_json::from_str::<FillValueMetadata>("-7.0").unwrap();
        let fill_value = data_type.fill_value_from_metadata(&metadata).unwrap();
        assert_eq!(fill_value.as_ne_bytes(), (-7.0f32).to_ne_bytes());
        assert_eq!(metadata, data_type.metadata_fill_value(&fill_value));

        assert_eq!(
            data_type
                .fill_value_from_metadata(
                    &serde_json::from_str::<FillValueMetadata>(r#""NaN""#).unwrap()
                )
                .unwrap()
                .as_ne_bytes(),
            ZARR_NAN_F32.to_ne_bytes()
        );

        assert_eq!(
            data_type
                .fill_value_from_metadata(
                    &serde_json::from_str::<FillValueMetadata>(r#""0x7fc00000""#).unwrap()
                )
                .unwrap()
                .as_ne_bytes(),
            f32::NAN.to_ne_bytes()
        );

        assert_eq!(
            data_type
                .fill_value_from_metadata(
                    &serde_json::from_str::<FillValueMetadata>(r#""Infinity""#).unwrap()
                )
                .unwrap()
                .as_ne_bytes(),
            f32::INFINITY.to_ne_bytes()
        );

        assert_eq!(
            data_type
                .fill_value_from_metadata(
                    &serde_json::from_str::<FillValueMetadata>(r#""-Infinity""#).unwrap()
                )
                .unwrap()
                .as_ne_bytes(),
            f32::NEG_INFINITY.to_ne_bytes()
        );
    }

    #[test]
    fn data_type_float64() {
        let json = r#""float64""#;
        let metadata: MetadataV3 = serde_json::from_str(json).unwrap();
        let data_type = DataType::from_metadata(&metadata).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        assert_eq!(data_type, DataType::Float64);

        let metadata = serde_json::from_str::<FillValueMetadata>("-7.0").unwrap();
        let fill_value = data_type.fill_value_from_metadata(&metadata).unwrap();
        assert_eq!(fill_value.as_ne_bytes(), (-7.0f64).to_ne_bytes());
        assert_eq!(metadata, data_type.metadata_fill_value(&fill_value));

        assert_eq!(
            data_type
                .fill_value_from_metadata(
                    &serde_json::from_str::<FillValueMetadata>(r#""0x7FF8000000000000""#).unwrap()
                )
                .unwrap()
                .as_ne_bytes(),
            ZARR_NAN_F64.to_ne_bytes()
        );

        assert_eq!(
            data_type
                .fill_value_from_metadata(
                    &serde_json::from_str::<FillValueMetadata>(r#""NaN""#).unwrap()
                )
                .unwrap()
                .as_ne_bytes(),
            ZARR_NAN_F64.to_ne_bytes()
        );

        assert_eq!(
            data_type
                .fill_value_from_metadata(
                    &serde_json::from_str::<FillValueMetadata>(r#""Infinity""#).unwrap()
                )
                .unwrap()
                .as_ne_bytes(),
            f64::INFINITY.to_ne_bytes()
        );

        assert_eq!(
            data_type
                .fill_value_from_metadata(
                    &serde_json::from_str::<FillValueMetadata>(r#""-Infinity""#).unwrap()
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
        let metadata: MetadataV3 = serde_json::from_str(json).unwrap();
        let data_type = DataType::from_metadata(&metadata).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        assert_eq!(data_type.identifier(), "float16");

        let metadata = serde_json::from_str::<FillValueMetadata>("-7.0").unwrap();
        let fill_value = data_type.fill_value_from_metadata(&metadata).unwrap();
        assert_eq!(
            fill_value.as_ne_bytes(),
            f16::from_f32_const(-7.0).to_ne_bytes()
        );
        assert_eq!(metadata, data_type.metadata_fill_value(&fill_value));

        assert_eq!(
            data_type
                .fill_value_from_metadata(
                    &serde_json::from_str::<FillValueMetadata>(r#""NaN""#).unwrap()
                )
                .unwrap()
                .as_ne_bytes(),
            f16::NAN.to_ne_bytes()
        );

        assert_eq!(
            data_type
                .fill_value_from_metadata(
                    &serde_json::from_str::<FillValueMetadata>(r#""Infinity""#).unwrap()
                )
                .unwrap()
                .as_ne_bytes(),
            f16::INFINITY.to_ne_bytes()
        );

        assert_eq!(
            data_type
                .fill_value_from_metadata(
                    &serde_json::from_str::<FillValueMetadata>(r#""-Infinity""#).unwrap()
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
        let metadata: MetadataV3 = serde_json::from_str(json).unwrap();
        let data_type = DataType::from_metadata(&metadata).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        assert_eq!(data_type.identifier(), "bfloat16");

        let metadata = serde_json::from_str::<FillValueMetadata>("-7.0").unwrap();
        let fill_value = data_type.fill_value_from_metadata(&metadata).unwrap();
        assert_eq!(
            fill_value.as_ne_bytes(),
            bf16::from_f32_const(-7.0).to_ne_bytes()
        );
        assert_eq!(metadata, data_type.metadata_fill_value(&fill_value));

        assert_eq!(
            data_type
                .fill_value_from_metadata(
                    // &serde_json::from_str::<FillValueMetadata>(r#""0x7E00""#).unwrap()
                    &serde_json::from_str::<FillValueMetadata>(r#""0x7FC0""#).unwrap()
                )
                .unwrap()
                .as_ne_bytes(),
            ZARR_NAN_BF16.to_ne_bytes()
        );

        assert_eq!(
            data_type
                .fill_value_from_metadata(
                    &serde_json::from_str::<FillValueMetadata>(r#""NaN""#).unwrap()
                )
                .unwrap()
                .as_ne_bytes(),
            ZARR_NAN_BF16.to_ne_bytes()
        );

        assert_eq!(
            data_type
                .fill_value_from_metadata(
                    &serde_json::from_str::<FillValueMetadata>(r#""Infinity""#).unwrap()
                )
                .unwrap()
                .as_ne_bytes(),
            bf16::INFINITY.to_ne_bytes()
        );

        assert_eq!(
            data_type
                .fill_value_from_metadata(
                    &serde_json::from_str::<FillValueMetadata>(r#""-Infinity""#).unwrap()
                )
                .unwrap()
                .as_ne_bytes(),
            bf16::NEG_INFINITY.to_ne_bytes()
        );
    }

    #[test]
    fn data_type_complex64() {
        let json = r#""complex64""#;
        let metadata: MetadataV3 = serde_json::from_str(json).unwrap();
        let data_type = DataType::from_metadata(&metadata).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        assert_eq!(data_type, DataType::Complex64);

        let metadata = serde_json::from_str::<FillValueMetadata>(r#"[-7.0, "Infinity"]"#).unwrap();
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
        let metadata: MetadataV3 = serde_json::from_str(json).unwrap();
        let data_type = DataType::from_metadata(&metadata).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        assert_eq!(data_type, DataType::Complex128);

        let metadata = serde_json::from_str::<FillValueMetadata>(r#"[-7.0, "Infinity"]"#).unwrap();
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
        let metadata: MetadataV3 = serde_json::from_str(json).unwrap();
        let data_type = DataType::from_metadata(&metadata).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        assert_eq!(data_type.identifier(), "r*");
        assert_eq!(data_type.name().as_str(), "r8");
        assert_eq!(data_type.size(), DataTypeSize::Fixed(1));

        let metadata = serde_json::from_str::<FillValueMetadata>("[7]").unwrap();
        let fill_value = data_type.fill_value_from_metadata(&metadata).unwrap();
        assert_eq!(fill_value.as_ne_bytes(), 7u8.to_ne_bytes());
        assert_eq!(metadata, data_type.metadata_fill_value(&fill_value));
    }

    #[test]
    fn data_type_r16() {
        let json = r#""r16""#;
        let metadata: MetadataV3 = serde_json::from_str(json).unwrap();
        let data_type = DataType::from_metadata(&metadata).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        assert_eq!(data_type.identifier(), "r*");
        assert_eq!(data_type.name().as_str(), "r16");
        assert_eq!(data_type.size(), DataTypeSize::Fixed(2));

        let metadata = serde_json::from_str::<FillValueMetadata>("[0, 255]").unwrap();
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
        let metadata = serde_json::from_str::<MetadataV3>(json).unwrap();
        println!("{json:?}");
        println!("{metadata:?}");
        assert_eq!(metadata.name(), "datetime");
        assert!(DataType::from_metadata(&metadata).is_err());
    }

    #[test]
    fn data_type_unknown2() {
        let json = r#""datetime""#;
        let metadata: MetadataV3 = serde_json::from_str(json).unwrap();
        println!("{json:?}");
        println!("{metadata:?}");
        assert_eq!(metadata.name(), "datetime");
        assert!(DataType::from_metadata(&metadata).is_err());
    }

    #[test]
    fn data_type_unknown3() {
        let json = r#""ra""#;
        let metadata: MetadataV3 = serde_json::from_str(json).unwrap();
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
        assert!(serde_json::from_str::<MetadataV3>(json).is_err());
    }

    #[test]
    fn data_type_raw_bits1() {
        let json = r#""r16""#;
        let metadata = serde_json::from_str::<MetadataV3>(json).unwrap();
        let data_type: DataType = DataType::from_metadata(&metadata).unwrap();
        assert_eq!(data_type.size(), DataTypeSize::Fixed(2));
    }

    #[test]
    fn data_type_raw_bits2() {
        let json = r#"
    {
        "name": "r16"
    }"#;
        let metadata = serde_json::from_str::<MetadataV3>(json).unwrap();
        let data_type: DataType = DataType::from_metadata(&metadata).unwrap();
        assert_eq!(data_type.size(), DataTypeSize::Fixed(2));
    }

    #[test]
    fn data_type_raw_bits_failure1() {
        let json = r#"
    {
        "name": "r5"
    }"#;
        let metadata = serde_json::from_str::<MetadataV3>(json).unwrap();
        assert!(DataType::from_metadata(&metadata).is_err());
    }

    #[test]
    fn incompatible_fill_value_metadata() {
        let json = r#""bool""#;
        let metadata: MetadataV3 = serde_json::from_str(json).unwrap();
        let data_type = DataType::from_metadata(&metadata).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        assert_eq!(data_type, DataType::Bool);

        let metadata = serde_json::from_str::<FillValueMetadata>("1").unwrap();
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
        let metadata: MetadataV3 = serde_json::from_str(json).unwrap();
        let data_type = DataType::from_metadata(&metadata).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        assert_eq!(data_type, DataType::RawBits(2));

        let metadata = serde_json::from_str::<FillValueMetadata>("[123]").unwrap();
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
        let metadata = serde_json::from_str::<FillValueMetadata>("1").unwrap();
        assert!(DataType::Bool.fill_value_from_metadata(&metadata).is_err());
        let metadata = serde_json::from_str::<FillValueMetadata>("false").unwrap();
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
        let metadata: MetadataV3 = serde_json::from_str(json).unwrap();
        let data_type = DataType::from_metadata(&metadata).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        assert_eq!(data_type.identifier(), "string");
        assert_eq!(data_type.name().as_str(), "string");
        assert_eq!(data_type.size(), DataTypeSize::Variable);

        let metadata = serde_json::from_str::<FillValueMetadata>(r#""hello world""#).unwrap();
        let fill_value = data_type.fill_value_from_metadata(&metadata).unwrap();
        assert_eq!(fill_value.as_ne_bytes(), "hello world".as_bytes(),);
        assert_eq!(metadata, data_type.metadata_fill_value(&fill_value));

        let metadata = serde_json::from_str::<FillValueMetadata>(
            r#"[104, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100]"#,
        )
        .unwrap();
        let fill_value = data_type.fill_value_from_metadata(&metadata).unwrap();
        assert_eq!(fill_value.as_ne_bytes(), "hello world".as_bytes(),);
        assert_ne!(metadata, data_type.metadata_fill_value(&fill_value)); // metadata is byte array rep, that is okay

        let metadata = serde_json::from_str::<FillValueMetadata>(r#""Infinity""#).unwrap();
        let fill_value = data_type.fill_value_from_metadata(&metadata).unwrap();
        assert_eq!(fill_value.as_ne_bytes(), "Infinity".as_bytes(),);
        assert_ne!(metadata, data_type.metadata_fill_value(&fill_value)); // metadata is float rep, that is okay

        let metadata = serde_json::from_str::<FillValueMetadata>(r#""0x7fc00000""#).unwrap();
        let fill_value = data_type.fill_value_from_metadata(&metadata).unwrap();
        assert_eq!(fill_value.as_ne_bytes(), "0x7fc00000".as_bytes(),);
        assert_ne!(metadata, data_type.metadata_fill_value(&fill_value)); // metadata is float rep, that is okay
    }
}
