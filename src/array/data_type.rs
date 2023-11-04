//! Zarr data types.
//!
//! See <https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html#data-types>.

use derive_more::From;
use half::{bf16, f16};
use thiserror::Error;

use crate::{metadata::Metadata, ZARR_NAN_F32, ZARR_NAN_F64};

use super::{
    fill_value_metadata::{
        FillValueFloat, FillValueFloatStringNonFinite, FillValueMetadata, HexString,
    },
    FillValue,
};

/// A data type.
#[derive(Clone, Debug)]
#[non_exhaustive]
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

                    // /// An extension data type.
                    // Extension(Box<dyn DataTypeExtension>),
}

/// An unsupported data type error.
#[derive(Debug, Error, From)]
#[error("data type {_0} is unsupported")]
pub struct UnsupportedDataTypeError(String);

impl PartialEq for DataType {
    fn eq(&self, other: &Self) -> bool {
        self.name() == other.name()
    }
}

impl Eq for DataType {}

// /// A data type plugin.
// pub type DataTypePlugin = Plugin<Box<dyn DataTypeExtension>>;
// inventory::collect!(DataTypePlugin);

/// A fill value metadata incompatibility error.
#[derive(Debug, Error)]
#[error("incompatible fill value {1} for data type {0}")]
pub struct IncompatibleFillValueErrorMetadataError(String, FillValueMetadata);

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

/// Extension data type traits.
pub trait DataTypeExtension: dyn_clone::DynClone + core::fmt::Debug + Send + Sync {
    /// Returns the identifier.
    fn identifier(&self) -> &'static str;

    /// Returns the name.
    fn name(&self) -> String;

    /// Returns the size in bytes.
    fn size(&self) -> usize;

    /// Returns the data type metadata.
    fn metadata(&self) -> Metadata;

    /// Create a fill value from metadata.
    ///
    /// # Errors
    ///
    /// Returns [`IncompatibleFillValueError`] if the fill value is incompatible with the data type.
    fn fill_value_from_metadata(
        &self,
        fill_value: &FillValueMetadata,
    ) -> Result<FillValue, IncompatibleFillValueErrorMetadataError>;

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
    pub fn metadata(&self) -> Metadata {
        Metadata::new(&self.name())
        // match self {
        //     // Self::Extension(extension) => extension.metadata(),
        //     _ => Metadata::new(&self.name()),
        // }
    }

    /// Returns the size in bytes.
    #[must_use]
    pub const fn size(&self) -> usize {
        match self {
            Self::Bool | Self::Int8 | Self::UInt8 => 1,
            Self::Int16 | Self::UInt16 | Self::Float16 | Self::BFloat16 => 2,
            Self::Int32 | Self::UInt32 | Self::Float32 => 4,
            Self::Int64 | Self::UInt64 | Self::Float64 | Self::Complex64 => 8,
            Self::Complex128 => 16,
            Self::RawBits(size) => *size,
            // Self::Extension(extension) => extension.size(),
        }
    }

    /// Create a data type from metadata.
    ///
    /// # Errors
    ///
    /// Returns [`UnsupportedDataTypeError`] if the metadata is invalid or not associated with a registered data type plugin.
    pub fn from_metadata(metadata: &Metadata) -> Result<Self, UnsupportedDataTypeError> {
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
        // })
    }

    /// Create a fill value from metadata.
    ///
    /// # Errors
    ///
    /// Returns [`IncompatibleFillValueErrorMetadataError`] if the fill value is incompatible with the data type.
    pub fn fill_value_from_metadata(
        &self,
        fill_value: &FillValueMetadata,
    ) -> Result<FillValue, IncompatibleFillValueErrorMetadataError> {
        use FillValue as FV;
        let err = || IncompatibleFillValueErrorMetadataError(self.name(), fill_value.clone());
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
                Err(IncompatibleFillValueErrorMetadataError(
                    self.name(),
                    fill_value.clone(),
                ))
            } // Self::Extension(extension) => extension.fill_value_from_metadata(fill_value),
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
                FillValueMetadata::Float(float16_to_fill_value_float(fill_value))
            }
            Self::Float32 => FillValueMetadata::Float(float32_to_fill_value(f32::from_ne_bytes(
                bytes.try_into().unwrap(),
            ))),
            Self::Float64 => FillValueMetadata::Float(float64_to_fill_value(f64::from_ne_bytes(
                bytes.try_into().unwrap(),
            ))),
            Self::BFloat16 => {
                let fill_value = bf16::from_ne_bytes(fill_value.as_ne_bytes().try_into().unwrap());
                FillValueMetadata::Float(bfloat16_to_fill_value_float(fill_value))
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
            } // DataType::Extension(extension) => extension.metadata_fill_value(fill_value),
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

fn float16_to_fill_value_float(f: f16) -> FillValueFloat {
    if f.is_infinite() && f.is_sign_positive() {
        FillValueFloatStringNonFinite::PosInfinity.into()
    } else if f.is_infinite() && f.is_sign_negative() {
        FillValueFloatStringNonFinite::NegInfinity.into()
    } else if f.is_nan() {
        FillValueFloatStringNonFinite::NaN.into()
    } else {
        f64::from(f).into()
    }
}

fn bfloat16_to_fill_value_float(f: bf16) -> FillValueFloat {
    if f.is_infinite() && f.is_sign_positive() {
        FillValueFloatStringNonFinite::PosInfinity.into()
    } else if f.is_infinite() && f.is_sign_negative() {
        FillValueFloatStringNonFinite::NegInfinity.into()
    } else if f.is_nan() {
        FillValueFloatStringNonFinite::NaN.into()
    } else {
        f64::from(f).into()
    }
}

impl TryFrom<Metadata> for DataType {
    type Error = UnsupportedDataTypeError;

    fn try_from(metadata: Metadata) -> Result<Self, Self::Error> {
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
    use crate::{ZARR_NAN_BF16, ZARR_NAN_F32, ZARR_NAN_F64};

    use super::*;

    #[test]
    fn data_type_unknown() {
        let json = r#""unknown""#;
        let metadata: Metadata = serde_json::from_str(json).unwrap();
        assert!(DataType::from_metadata(&metadata).is_err());
    }

    #[test]
    fn data_type_bool() {
        let json = r#""bool""#;
        let metadata: Metadata = serde_json::from_str(json).unwrap();
        let data_type = DataType::from_metadata(&metadata).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        assert_eq!(data_type, DataType::Bool);

        assert_eq!(
            data_type
                .fill_value_from_metadata(
                    &serde_json::from_str::<FillValueMetadata>(r#"true"#).unwrap()
                )
                .unwrap()
                .as_ne_bytes(),
            &[u8::from(true)]
        );
        assert_eq!(
            data_type
                .fill_value_from_metadata(
                    &serde_json::from_str::<FillValueMetadata>(r#"false"#).unwrap()
                )
                .unwrap()
                .as_ne_bytes(),
            &[u8::from(false)]
        );
    }

    #[test]
    fn data_type_int8() {
        let json = r#""int8""#;
        let metadata: Metadata = serde_json::from_str(json).unwrap();
        let data_type = DataType::from_metadata(&metadata).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        assert_eq!(data_type, DataType::Int8);

        assert_eq!(
            data_type
                .fill_value_from_metadata(&serde_json::from_str::<FillValueMetadata>("7").unwrap())
                .unwrap()
                .as_ne_bytes(),
            7i8.to_ne_bytes()
        );

        assert_eq!(
            data_type
                .fill_value_from_metadata(&serde_json::from_str::<FillValueMetadata>("-7").unwrap())
                .unwrap()
                .as_ne_bytes(),
            (-7i8).to_ne_bytes()
        );
    }

    #[test]
    fn data_type_uint8() {
        let json = r#""uint8""#;
        let metadata: Metadata = serde_json::from_str(json).unwrap();
        let data_type = DataType::from_metadata(&metadata).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        assert_eq!(data_type, DataType::UInt8);

        assert_eq!(
            data_type
                .fill_value_from_metadata(&serde_json::from_str::<FillValueMetadata>("7").unwrap())
                .unwrap()
                .as_ne_bytes(),
            7u8.to_ne_bytes()
        );
    }

    #[test]
    fn data_type_float32() {
        let json = r#""float32""#;
        let metadata: Metadata = serde_json::from_str(json).unwrap();
        let data_type = DataType::from_metadata(&metadata).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        assert_eq!(data_type, DataType::Float32);

        assert_eq!(
            data_type
                .fill_value_from_metadata(
                    &serde_json::from_str::<FillValueMetadata>("-7.0").unwrap()
                )
                .unwrap()
                .as_ne_bytes(),
            (-7.0f32).to_ne_bytes()
        );

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
        let metadata: Metadata = serde_json::from_str(json).unwrap();
        let data_type = DataType::from_metadata(&metadata).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        assert_eq!(data_type, DataType::Float64);

        assert_eq!(
            data_type
                .fill_value_from_metadata(
                    &serde_json::from_str::<FillValueMetadata>("-7.0").unwrap()
                )
                .unwrap()
                .as_ne_bytes(),
            (-7.0f64).to_ne_bytes()
        );

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
        let metadata: Metadata = serde_json::from_str(json).unwrap();
        let data_type = DataType::from_metadata(&metadata).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        assert_eq!(data_type.identifier(), "float16");

        assert_eq!(
            data_type
                .fill_value_from_metadata(
                    &serde_json::from_str::<FillValueMetadata>("-7.0").unwrap()
                )
                .unwrap()
                .as_ne_bytes(),
            (f16::from_f32_const(-7.0)).to_ne_bytes()
        );

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
        let metadata: Metadata = serde_json::from_str(json).unwrap();
        let data_type = DataType::from_metadata(&metadata).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        assert_eq!(data_type.identifier(), "bfloat16");

        assert_eq!(
            data_type
                .fill_value_from_metadata(
                    &serde_json::from_str::<FillValueMetadata>("-7.0").unwrap()
                )
                .unwrap()
                .as_ne_bytes(),
            (bf16::from_f32_const(-7.0)).to_ne_bytes()
        );

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
        let metadata: Metadata = serde_json::from_str(json).unwrap();
        let data_type = DataType::from_metadata(&metadata).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        assert_eq!(data_type, DataType::Complex64);

        assert_eq!(
            data_type
                .fill_value_from_metadata(
                    &serde_json::from_str::<FillValueMetadata>(r#"[-7.0, "Infinity"]"#).unwrap()
                )
                .unwrap()
                .as_ne_bytes(),
            (-7.0f32)
                .to_ne_bytes()
                .iter()
                .chain(f32::INFINITY.to_ne_bytes().iter())
                .copied()
                .collect::<Vec<u8>>()
        );
    }

    #[test]
    fn data_type_complex128() {
        let json = r#""complex128""#;
        let metadata: Metadata = serde_json::from_str(json).unwrap();
        let data_type = DataType::from_metadata(&metadata).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        assert_eq!(data_type, DataType::Complex128);

        assert_eq!(
            data_type
                .fill_value_from_metadata(
                    &serde_json::from_str::<FillValueMetadata>(r#"[-7.0, "Infinity"]"#).unwrap()
                )
                .unwrap()
                .as_ne_bytes(),
            (-7.0f64)
                .to_ne_bytes()
                .iter()
                .chain(f64::INFINITY.to_ne_bytes().iter())
                .copied()
                .collect::<Vec<u8>>()
        );
    }

    #[test]
    fn data_type_r8() {
        let json = r#""r8""#;
        let metadata: Metadata = serde_json::from_str(json).unwrap();
        let data_type = DataType::from_metadata(&metadata).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        assert_eq!(data_type.identifier(), "r*");
        assert_eq!(data_type.name().as_str(), "r8");
        assert_eq!(data_type.size(), 1);

        assert_eq!(
            data_type
                .fill_value_from_metadata(
                    &serde_json::from_str::<FillValueMetadata>(r#"[7]"#).unwrap()
                )
                .unwrap()
                .as_ne_bytes(),
            (7u8).to_ne_bytes()
        );
    }

    #[test]
    fn data_type_r16() {
        let json = r#""r16""#;
        let metadata: Metadata = serde_json::from_str(json).unwrap();
        let data_type = DataType::from_metadata(&metadata).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        assert_eq!(data_type.identifier(), "r*");
        assert_eq!(data_type.name().as_str(), "r16");
        assert_eq!(data_type.size(), 2);

        assert_eq!(
            data_type
                .fill_value_from_metadata(
                    &serde_json::from_str::<FillValueMetadata>(r#"[0, 255]"#).unwrap()
                )
                .unwrap()
                .as_ne_bytes(), // NOTE: Raw value bytes are always read as-is.
            &[0u8, 255u8]
        );
    }

    #[test]
    pub fn data_type_unknown1() {
        let json = r#"
    {
        "name": "datetime",
        "configuration": {
            "unit": "ns"
        }
    }"#;
        let metadata = serde_json::from_str::<Metadata>(json).unwrap();
        println!("{json:?}");
        println!("{metadata:?}");
        assert_eq!(metadata.name(), "datetime");
    }

    #[test]
    pub fn data_type_unknown2() {
        let json = r#""datetime""#;
        let metadata: Metadata = serde_json::from_str(json).unwrap();
        println!("{json:?}");
        println!("{metadata:?}");
        assert_eq!(metadata.name(), "datetime");
    }

    #[test]
    pub fn data_type_invalid() {
        let json = r#"
    {
        "name": "datetime",
        "notconfiguration": {
            "unit": "ns"
        }
    }"#;
        assert!(serde_json::from_str::<Metadata>(json).is_err());
    }

    #[test]
    pub fn data_type_raw_bits1() {
        let json = r#""r16""#;
        let metadata = serde_json::from_str::<Metadata>(json).unwrap();
        let data_type: DataType = DataType::from_metadata(&metadata).unwrap();
        assert_eq!(data_type.size(), 2);
    }

    #[test]
    pub fn data_type_raw_bits2() {
        let json = r#"
    {
        "name": "r16"
    }"#;
        let metadata = serde_json::from_str::<Metadata>(json).unwrap();
        let data_type: DataType = DataType::from_metadata(&metadata).unwrap();
        assert_eq!(data_type.size(), 2);
    }
}
