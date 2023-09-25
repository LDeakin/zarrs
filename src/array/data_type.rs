//! Zarr data types.
//!
//! See <https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html#data-types>.

#[cfg(feature = "raw_bits")]
mod raw_bits;
#[cfg(feature = "raw_bits")]
pub use raw_bits::RawBitsDataType;

#[cfg(feature = "float16")]
mod float16;
#[cfg(feature = "float16")]
pub use float16::Float16DataType;

#[cfg(feature = "bfloat16")]
mod bfloat16;
#[cfg(feature = "bfloat16")]
pub use bfloat16::Bfloat16DataType;

use thiserror::Error;

use crate::{
    metadata::Metadata,
    plugin::{Plugin, PluginCreateError},
};

use super::{
    fill_value_metadata::{FillValueFloat, FillValueFloatStringNonFinite, FillValueMetadata},
    FillValue,
};

/// A data type.
#[derive(Clone, Debug)]
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
    /// `float32` IEEE 754 single-precision floating point: sign bit, 8 bits exponent, 23 bits mantissa.
    Float32,
    /// `float64` IEEE 754 double-precision floating point: sign bit, 11 bits exponent, 52 bits mantissa.
    Float64,
    /// `complex64` real and complex components are each IEEE 754 single-precision floating point.
    Complex64,
    /// `complex128` real and complex components are each IEEE 754 double-precision floating point.
    Complex128,
    /// An optional or extension data type.
    Extension(Box<dyn DataTypeExtension>),
}

/// A data type plugin.
pub type DataTypePlugin = Plugin<Box<dyn DataTypeExtension>>;
inventory::collect!(DataTypePlugin);

/// Create a data type from metadata.
///
/// # Errors
///
/// Returns [`PluginCreateError`] if the metadata is invalid or not associated with a registered data type plugin.
pub fn try_create_data_type(metadata: &Metadata) -> Result<DataType, PluginCreateError> {
    let name = metadata.name();

    match name {
        "bool" => return Ok(DataType::Bool),
        "int8" => return Ok(DataType::Int8),
        "int16" => return Ok(DataType::Int16),
        "int32" => return Ok(DataType::Int32),
        "int64" => return Ok(DataType::Int64),
        "uint8" => return Ok(DataType::UInt8),
        "uint16" => return Ok(DataType::UInt16),
        "uint32" => return Ok(DataType::UInt32),
        "uint64" => return Ok(DataType::UInt64),
        "float32" => return Ok(DataType::Float32),
        "float64" => return Ok(DataType::Float64),
        "complex64" => return Ok(DataType::Complex64),
        "complex128" => return Ok(DataType::Complex128),
        _ => {}
    };

    for plugin in inventory::iter::<DataTypePlugin> {
        if plugin.match_name(metadata.name()) {
            return Ok(DataType::Extension(plugin.create(metadata)?));
        }
    }
    Err(PluginCreateError::Unsupported {
        name: metadata.name().to_string(),
    })
}

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
    pub fn new(data_type_name: String, fill_value: FillValue) -> Self {
        IncompatibleFillValueError(data_type_name, fill_value)
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
    fn try_create_fill_value(
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
    pub fn identifier(&self) -> &'static str {
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
            Self::Float32 => "float32",
            Self::Float64 => "float64",
            Self::Complex64 => "complex64",
            Self::Complex128 => "complex128",
            Self::Extension(extension) => extension.identifier(),
        }
    }

    /// Returns the name.
    #[must_use]
    pub fn name(&self) -> String {
        match self {
            Self::Extension(extension) => extension.name(),
            _ => self.identifier().to_string(),
        }
    }

    /// Returns the metadata.
    #[must_use]
    pub fn metadata(&self) -> Metadata {
        match self {
            Self::Extension(extension) => extension.metadata(),
            _ => Metadata::new(self.identifier()),
        }
    }

    /// Returns the size in bytes.
    #[must_use]
    pub fn size(&self) -> usize {
        match self {
            Self::Bool | Self::Int8 | Self::UInt8 => 1,
            Self::Int16 | Self::UInt16 => 2,
            Self::Int32 | Self::UInt32 | Self::Float32 => 4,
            Self::Int64 | Self::UInt64 | Self::Float64 | Self::Complex64 => 8,
            Self::Complex128 => 16,
            Self::Extension(extension) => extension.size(),
        }
    }

    /// Create a fill value from metadata.
    ///
    /// # Errors
    ///
    /// Returns [`IncompatibleFillValueErrorMetadataError`] if the fill value is incompatible with the data type.
    pub fn try_create_fill_value(
        &self,
        fill_value: &FillValueMetadata,
    ) -> Result<FillValue, IncompatibleFillValueErrorMetadataError> {
        use FillValue as FV;
        let err =
            || IncompatibleFillValueErrorMetadataError(self.name().to_string(), fill_value.clone());
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
            Self::Float32 => Ok(FV::from(fill_value.try_as_float::<f32>().ok_or_else(err)?)),
            Self::Float64 => Ok(FV::from(fill_value.try_as_float::<f64>().ok_or_else(err)?)),
            Self::Complex64 => Ok(FV::from(num::complex::Complex32::new(
                fill_value.try_as_float::<f32>().ok_or_else(err)?,
                fill_value.try_as_float::<f32>().ok_or_else(err)?,
            ))),
            Self::Complex128 => Ok(FV::from(num::complex::Complex64::new(
                fill_value.try_as_float::<f64>().ok_or_else(err)?,
                fill_value.try_as_float::<f64>().ok_or_else(err)?,
            ))),
            Self::Extension(extension) => extension.try_create_fill_value(fill_value),
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
            DataType::Bool => FillValueMetadata::Bool(bytes[0] != 0),
            DataType::Int8 => {
                FillValueMetadata::Int(i64::from(i8::from_ne_bytes(bytes.try_into().unwrap())))
            }
            DataType::Int16 => {
                FillValueMetadata::Int(i64::from(i16::from_ne_bytes(bytes.try_into().unwrap())))
            }
            DataType::Int32 => {
                FillValueMetadata::Int(i64::from(i32::from_ne_bytes(bytes.try_into().unwrap())))
            }
            DataType::Int64 => {
                FillValueMetadata::Int(i64::from_ne_bytes(bytes.try_into().unwrap()))
            }
            DataType::UInt8 => {
                FillValueMetadata::Uint(u64::from(u8::from_ne_bytes(bytes.try_into().unwrap())))
            }
            DataType::UInt16 => {
                FillValueMetadata::Uint(u64::from(u16::from_ne_bytes(bytes.try_into().unwrap())))
            }
            DataType::UInt32 => {
                FillValueMetadata::Uint(u64::from(u32::from_ne_bytes(bytes.try_into().unwrap())))
            }
            DataType::UInt64 => {
                FillValueMetadata::Uint(u64::from_ne_bytes(bytes.try_into().unwrap()))
            }
            DataType::Float32 => FillValueMetadata::Float(float_to_fill_value(f32::from_ne_bytes(
                bytes.try_into().unwrap(),
            ))),
            DataType::Float64 => FillValueMetadata::Float(float_to_fill_value(f64::from_ne_bytes(
                bytes.try_into().unwrap(),
            ))),
            DataType::Complex64 => {
                let re = f32::from_ne_bytes(bytes[0..4].try_into().unwrap());
                let im = f32::from_ne_bytes(bytes[4..8].try_into().unwrap());
                FillValueMetadata::Complex(float_to_fill_value(re), float_to_fill_value(im))
            }
            DataType::Complex128 => {
                let re = f64::from_ne_bytes(bytes[0..8].try_into().unwrap());
                let im = f64::from_ne_bytes(bytes[8..16].try_into().unwrap());
                FillValueMetadata::Complex(float_to_fill_value(re), float_to_fill_value(im))
            }
            DataType::Extension(extension) => extension.metadata_fill_value(fill_value),
        }
    }
}

fn float_to_fill_value<F: num::Float>(f: F) -> FillValueFloat
where
    f64: From<F>,
{
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
    type Error = PluginCreateError;

    fn try_from(metadata: Metadata) -> Result<Self, Self::Error> {
        try_create_data_type(&metadata)
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
        let metadata: Metadata = serde_json::from_str(json).unwrap();
        assert!(try_create_data_type(&metadata).is_err());
    }

    #[test]
    fn data_type_bool() {
        let json = r#""bool""#;
        let metadata: Metadata = serde_json::from_str(json).unwrap();
        let data_type = try_create_data_type(&metadata).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        match data_type {
            DataType::Bool => {}
            _ => panic!(),
        }
    }

    #[test]
    fn data_type_int8() {
        let json = r#""int8""#;
        let metadata: Metadata = serde_json::from_str(json).unwrap();
        let data_type = try_create_data_type(&metadata).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        match data_type {
            DataType::Int8 => {}
            _ => panic!(),
        }
    }

    #[test]
    fn data_type_uint8() {
        let json = r#""uint8""#;
        let metadata: Metadata = serde_json::from_str(json).unwrap();
        let data_type = try_create_data_type(&metadata).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        match data_type {
            DataType::UInt8 => {}
            _ => panic!(),
        }
    }

    #[test]
    fn data_type_float32() {
        let json = r#""float32""#;
        let metadata: Metadata = serde_json::from_str(json).unwrap();
        let data_type = try_create_data_type(&metadata).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        match data_type {
            DataType::Float32 => {}
            _ => panic!(),
        }
    }

    #[cfg(feature = "float16")]
    #[test]
    fn data_type_float16() {
        let json = r#""float16""#;
        let metadata: Metadata = serde_json::from_str(json).unwrap();
        let data_type = try_create_data_type(&metadata).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        assert_eq!(data_type.identifier(), "float16");
    }

    #[cfg(feature = "bfloat16")]
    #[test]
    fn data_type_bfloat16() {
        let json = r#""bfloat16""#;
        let metadata: Metadata = serde_json::from_str(json).unwrap();
        let data_type = try_create_data_type(&metadata).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        assert_eq!(data_type.identifier(), "bfloat16");
    }

    #[test]
    fn data_type_complex64() {
        let json = r#""complex64""#;
        let metadata: Metadata = serde_json::from_str(json).unwrap();
        let data_type = try_create_data_type(&metadata).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        match data_type {
            DataType::Complex64 => {}
            _ => panic!(),
        }
    }

    #[test]
    fn data_type_complex128() {
        let json = r#""complex128""#;
        let metadata: Metadata = serde_json::from_str(json).unwrap();
        let data_type = try_create_data_type(&metadata).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        match data_type {
            DataType::Complex128 => {}
            _ => panic!(),
        }
    }

    #[cfg(feature = "raw_bits")]
    #[test]
    fn data_type_r8() {
        let json = r#""r8""#;
        let metadata: Metadata = serde_json::from_str(json).unwrap();
        let data_type = try_create_data_type(&metadata).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        assert_eq!(data_type.identifier(), "r*");
        assert_eq!(data_type.name().as_str(), "r8");
        assert_eq!(data_type.size(), 1);
    }

    #[cfg(feature = "raw_bits")]
    #[test]
    fn data_type_r16() {
        let json = r#""r16""#;
        let metadata: Metadata = serde_json::from_str(json).unwrap();
        let data_type = try_create_data_type(&metadata).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        assert_eq!(data_type.identifier(), "r*");
        assert_eq!(data_type.name().as_str(), "r16");
        assert_eq!(data_type.size(), 2);
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

    #[cfg(feature = "raw_bits")]
    #[test]
    pub fn data_type_raw_bits1() {
        let json = r#""r16""#;
        let metadata = serde_json::from_str::<Metadata>(json).unwrap();
        let data_type: DataType = try_create_data_type(&metadata).unwrap();
        assert_eq!(data_type.size(), 2);
    }

    #[cfg(feature = "raw_bits")]
    #[test]
    pub fn data_type_raw_bits2() {
        let json = r#"
    {
        "name": "r16"
    }"#;
        let metadata = serde_json::from_str::<Metadata>(json).unwrap();
        let data_type: DataType = try_create_data_type(&metadata).unwrap();
        assert_eq!(data_type.size(), 2);
    }
}
