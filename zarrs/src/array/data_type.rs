//! Zarr data types.
//!
//! See <https://zarr-specs.readthedocs.io/en/latest/v3/core/index.html#data-types>.
//!
//! This submodule re-exports much of the [`zarrs_data_type`] crate.
//!
//! Custom data types can be implemented by registering structs that implement the traits of [`zarrs_data_type`].
//! A custom data type guide can be found in [The `zarrs` book](https://book.zarrs.dev).

use std::{fmt::Debug, mem::discriminant, sync::Arc};

pub use zarrs_data_type::{
    DataTypeExtension, DataTypeExtensionBytesCodec, DataTypeExtensionBytesCodecError,
    DataTypeExtensionError, DataTypeExtensionPackBitsCodec, DataTypeFillValueError,
    DataTypeFillValueMetadataError, DataTypePlugin, FillValue,
};
use zarrs_metadata::{
    extension::ExtensionAliasesDataTypeV3,
    v3::{
        array::{
            data_type::{self},
            fill_value::FillValueMetadataV3,
        },
        MetadataV3,
    },
    DataTypeSize,
};
use zarrs_plugin::{PluginCreateError, PluginUnsupportedError};

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
    Bytes,
    /// An extension data type.
    Extension(Arc<dyn DataTypeExtension>)
}

impl PartialEq for DataType {
    fn eq(&self, other: &Self) -> bool {
        match (&self, other) {
            (DataType::RawBits(a), DataType::RawBits(b)) => a == b,
            (DataType::Extension(a), DataType::Extension(b)) => {
                a.name() == b.name() && a.configuration() == b.configuration()
            }
            _ => discriminant(self) == discriminant(other),
        }
    }
}

impl Eq for DataType {}

impl DataType {
    /// Returns the name.
    #[must_use]
    pub fn name(&self) -> String {
        match self {
            Self::Bool => data_type::BOOL.to_string(),
            Self::Int8 => data_type::INT8.to_string(),
            Self::Int16 => data_type::INT16.to_string(),
            Self::Int32 => data_type::INT32.to_string(),
            Self::Int64 => data_type::INT64.to_string(),
            Self::UInt8 => data_type::UINT8.to_string(),
            Self::UInt16 => data_type::UINT16.to_string(),
            Self::UInt32 => data_type::UINT32.to_string(),
            Self::UInt64 => data_type::UINT64.to_string(),
            Self::Float16 => data_type::FLOAT16.to_string(),
            Self::Float32 => data_type::FLOAT32.to_string(),
            Self::Float64 => data_type::FLOAT64.to_string(),
            Self::BFloat16 => data_type::BFLOAT16.to_string(),
            Self::Complex64 => data_type::COMPLEX64.to_string(),
            Self::Complex128 => data_type::COMPLEX128.to_string(),
            Self::RawBits(size) => format!("r{}", size * 8),
            Self::String => data_type::STRING.to_string(),
            Self::Bytes => data_type::BYTES.to_string(),
            Self::Extension(extension) => extension.name(),
        }
    }

    /// Returns the metadata.
    #[must_use]
    pub fn metadata(&self) -> MetadataV3 {
        match self {
            Self::Bool => MetadataV3::new(data_type::BOOL),
            Self::Int8 => MetadataV3::new(data_type::INT8),
            Self::Int16 => MetadataV3::new(data_type::INT16),
            Self::Int32 => MetadataV3::new(data_type::INT32),
            Self::Int64 => MetadataV3::new(data_type::INT64),
            Self::UInt8 => MetadataV3::new(data_type::UINT8),
            Self::UInt16 => MetadataV3::new(data_type::UINT16),
            Self::UInt32 => MetadataV3::new(data_type::UINT32),
            Self::UInt64 => MetadataV3::new(data_type::UINT64),
            Self::Float16 => MetadataV3::new(data_type::FLOAT16),
            Self::Float32 => MetadataV3::new(data_type::FLOAT32),
            Self::Float64 => MetadataV3::new(data_type::FLOAT64),
            Self::BFloat16 => MetadataV3::new(data_type::BFLOAT16),
            Self::Complex64 => MetadataV3::new(data_type::COMPLEX64),
            Self::Complex128 => MetadataV3::new(data_type::COMPLEX128),
            Self::RawBits(size) => MetadataV3::new(format!("r{}", size * 8)),
            Self::String => MetadataV3::new(data_type::STRING),
            Self::Bytes => MetadataV3::new(data_type::BYTES),
            Self::Extension(ext) => {
                MetadataV3::new_with_configuration(ext.name(), ext.configuration())
            }
        }
    }

    /// Returns the [`DataTypeSize`].
    #[must_use]
    pub fn size(&self) -> DataTypeSize {
        match self {
            Self::Bool | Self::Int8 | Self::UInt8 => DataTypeSize::Fixed(1),
            Self::Int16 | Self::UInt16 | Self::Float16 | Self::BFloat16 => DataTypeSize::Fixed(2),
            Self::Int32 | Self::UInt32 | Self::Float32 => DataTypeSize::Fixed(4),
            Self::Int64 | Self::UInt64 | Self::Float64 | Self::Complex64 => DataTypeSize::Fixed(8),
            Self::Complex128 => DataTypeSize::Fixed(16),
            Self::RawBits(size) => DataTypeSize::Fixed(*size),
            Self::String | Self::Bytes => DataTypeSize::Variable,
            Self::Extension(extension) => extension.size(),
        }
    }

    /// Returns the size in bytes of a fixed-size data type, otherwise returns [`None`].
    #[must_use]
    pub fn fixed_size(&self) -> Option<usize> {
        match self.size() {
            DataTypeSize::Fixed(size) => Some(size),
            DataTypeSize::Variable => None,
        }
    }

    /// Create a data type from metadata.
    ///
    /// # Errors
    ///
    /// Returns [`PluginCreateError`] if the metadata is invalid or not associated with a registered data type plugin.
    pub fn from_metadata(
        metadata: &MetadataV3,
        data_type_aliases: &ExtensionAliasesDataTypeV3,
    ) -> Result<Self, PluginCreateError> {
        if !metadata.must_understand() {
            return Err(PluginCreateError::Other(
                r#"data type must not have `"must_understand": false`"#.to_string(),
            ));
        }

        if metadata.configuration_is_none_or_empty() {
            match metadata.name() {
                data_type::BOOL => return Ok(Self::Bool),
                data_type::INT8 => return Ok(Self::Int8),
                data_type::INT16 => return Ok(Self::Int16),
                data_type::INT32 => return Ok(Self::Int32),
                data_type::INT64 => return Ok(Self::Int64),
                data_type::UINT8 => return Ok(Self::UInt8),
                data_type::UINT16 => return Ok(Self::UInt16),
                data_type::UINT32 => return Ok(Self::UInt32),
                data_type::UINT64 => return Ok(Self::UInt64),
                data_type::FLOAT16 => return Ok(Self::Float16),
                data_type::FLOAT32 => return Ok(Self::Float32),
                data_type::FLOAT64 => return Ok(Self::Float64),
                data_type::BFLOAT16 => return Ok(Self::BFloat16),
                data_type::COMPLEX64 => return Ok(Self::Complex64),
                data_type::COMPLEX128 => return Ok(Self::Complex128),
                data_type::STRING => return Ok(Self::String),
                data_type::BYTES => return Ok(Self::Bytes),
                name => {
                    if name.starts_with('r') && name.len() > 1 {
                        if let Ok(size_bits) = metadata.name()[1..].parse::<usize>() {
                            if size_bits % 8 == 0 {
                                let size_bytes = size_bits / 8;
                                return Ok(Self::RawBits(size_bytes));
                            }
                            return Err(PluginUnsupportedError::new(
                                name.to_string(),
                                "data type".to_string(),
                            )
                            .into());
                        }
                    }
                }
            }
        }

        // Try an extension
        let identifier = data_type_aliases.identifier(metadata.name());
        for plugin in inventory::iter::<DataTypePlugin> {
            if plugin.match_name(identifier) {
                return plugin.create(&metadata.clone()).map(DataType::Extension);
            }
        }

        // The data type is not supported
        Err(
            PluginUnsupportedError::new(metadata.name().to_string(), "data type".to_string())
                .into(),
        )
    }

    /// Create a fill value from metadata.
    ///
    /// # Errors
    ///
    /// Returns [`DataTypeFillValueMetadataError`] if the fill value is incompatible with the data type.
    pub fn fill_value_from_metadata(
        &self,
        fill_value: &FillValueMetadataV3,
    ) -> Result<FillValue, DataTypeFillValueMetadataError> {
        use FillValue as FV;
        let err0 = || DataTypeFillValueMetadataError::new(self.name(), fill_value.clone());
        let err = |_| DataTypeFillValueMetadataError::new(self.name(), fill_value.clone());
        match self {
            Self::Bool => Ok(FV::from(fill_value.as_bool().ok_or_else(err0)?)),
            Self::Int8 => {
                let int = fill_value.as_i64().ok_or_else(err0)?;
                let int = i8::try_from(int).map_err(err)?;
                Ok(FV::from(int))
            }
            Self::Int16 => {
                let int = fill_value.as_i64().ok_or_else(err0)?;
                let int = i16::try_from(int).map_err(err)?;
                Ok(FV::from(int))
            }
            Self::Int32 => {
                let int = fill_value.as_i64().ok_or_else(err0)?;
                let int = i32::try_from(int).map_err(err)?;
                Ok(FV::from(int))
            }
            Self::Int64 => {
                let int = fill_value.as_i64().ok_or_else(err0)?;
                Ok(FV::from(int))
            }
            Self::UInt8 => {
                let int = fill_value.as_u64().ok_or_else(err0)?;
                let int = u8::try_from(int).map_err(err)?;
                Ok(FV::from(int))
            }
            Self::UInt16 => {
                let int = fill_value.as_u64().ok_or_else(err0)?;
                let int = u16::try_from(int).map_err(err)?;
                Ok(FV::from(int))
            }
            Self::UInt32 => {
                let int = fill_value.as_u64().ok_or_else(err0)?;
                let int = u32::try_from(int).map_err(err)?;
                Ok(FV::from(int))
            }
            Self::UInt64 => {
                let int = fill_value.as_u64().ok_or_else(err0)?;
                Ok(FV::from(int))
            }
            Self::BFloat16 => Ok(FV::from(fill_value.as_bf16().ok_or_else(err0)?)),
            Self::Float16 => Ok(FV::from(fill_value.as_f16().ok_or_else(err0)?)),
            Self::Float32 => Ok(FV::from(fill_value.as_f32().ok_or_else(err0)?)),
            Self::Float64 => Ok(FV::from(fill_value.as_f64().ok_or_else(err0)?)),
            Self::Complex64 => {
                if let [re, im] = fill_value.as_array().ok_or_else(err0)? {
                    let re = re.as_f32().ok_or_else(err0)?;
                    let im = im.as_f32().ok_or_else(err0)?;
                    Ok(FV::from(num::complex::Complex32::new(re, im)))
                } else {
                    Err(err0())?
                }
            }
            Self::Complex128 => {
                if let [re, im] = fill_value.as_array().ok_or_else(err0)? {
                    let re = re.as_f64().ok_or_else(err0)?;
                    let im = im.as_f64().ok_or_else(err0)?;
                    Ok(FV::from(num::complex::Complex64::new(re, im)))
                } else {
                    Err(err0())?
                }
            }
            Self::RawBits(size) => {
                let bytes = fill_value.as_bytes().ok_or_else(err0)?;
                if bytes.len() == *size {
                    Ok(FV::from(bytes))
                } else {
                    Err(err0())?
                }
            }
            Self::Bytes => {
                let bytes = fill_value.as_bytes().ok_or_else(err0)?;
                Ok(FV::from(bytes))
            }
            Self::String => Ok(FV::from(fill_value.as_str().ok_or_else(err0)?)),
            Self::Extension(ext) => ext.fill_value(fill_value),
        }
    }

    /// Create fill value metadata.
    ///
    /// # Errors
    ///
    /// Returns an [`DataTypeFillValueError`] if the metadata cannot be created from the fill value.
    #[allow(clippy::too_many_lines)]
    pub fn metadata_fill_value(
        &self,
        fill_value: &FillValue,
    ) -> Result<FillValueMetadataV3, DataTypeFillValueError> {
        let error = || DataTypeFillValueError::new(self.name(), fill_value.clone());
        match self {
            Self::Bool => {
                let bytes: [u8; 1] = fill_value.as_ne_bytes().try_into().map_err(|_| error())?;
                match bytes[0] {
                    0 => Ok(FillValueMetadataV3::from(false)),
                    1 => Ok(FillValueMetadataV3::from(true)),
                    _ => Err(error()),
                }
            }
            Self::Int8 => {
                let bytes: [u8; 1] = fill_value.as_ne_bytes().try_into().map_err(|_| error())?;
                let number = i8::from_ne_bytes(bytes);
                Ok(FillValueMetadataV3::from(number))
            }
            Self::Int16 => {
                let bytes: [u8; 2] = fill_value.as_ne_bytes().try_into().map_err(|_| error())?;
                let number = i16::from_ne_bytes(bytes);
                Ok(FillValueMetadataV3::from(number))
            }
            Self::Int32 => {
                let bytes: [u8; 4] = fill_value.as_ne_bytes().try_into().map_err(|_| error())?;
                let number = i32::from_ne_bytes(bytes);
                Ok(FillValueMetadataV3::from(number))
            }
            Self::Int64 => {
                let bytes: [u8; 8] = fill_value.as_ne_bytes().try_into().map_err(|_| error())?;
                let number = i64::from_ne_bytes(bytes);
                Ok(FillValueMetadataV3::from(number))
            }
            Self::UInt8 => {
                let bytes: [u8; 1] = fill_value.as_ne_bytes().try_into().map_err(|_| error())?;
                let number = u8::from_ne_bytes(bytes);
                Ok(FillValueMetadataV3::from(number))
            }
            Self::UInt16 => {
                let bytes: [u8; 2] = fill_value.as_ne_bytes().try_into().map_err(|_| error())?;
                let number = u16::from_ne_bytes(bytes);
                Ok(FillValueMetadataV3::from(number))
            }
            Self::UInt32 => {
                let bytes: [u8; 4] = fill_value.as_ne_bytes().try_into().map_err(|_| error())?;
                let number = u32::from_ne_bytes(bytes);
                Ok(FillValueMetadataV3::from(number))
            }
            Self::UInt64 => {
                let bytes: [u8; 8] = fill_value.as_ne_bytes().try_into().map_err(|_| error())?;
                let number = u64::from_ne_bytes(bytes);
                Ok(FillValueMetadataV3::from(number))
            }
            Self::Float16 => {
                let bytes: [u8; 2] = fill_value.as_ne_bytes().try_into().map_err(|_| error())?;
                let number = half::f16::from_ne_bytes(bytes);
                Ok(FillValueMetadataV3::from(number))
            }
            Self::Float32 => {
                let bytes: [u8; 4] = fill_value.as_ne_bytes().try_into().map_err(|_| error())?;
                let number = f32::from_ne_bytes(bytes);
                Ok(FillValueMetadataV3::from(number))
            }
            Self::Float64 => {
                let bytes: [u8; 8] = fill_value.as_ne_bytes().try_into().map_err(|_| error())?;
                let number = f64::from_ne_bytes(bytes);
                Ok(FillValueMetadataV3::from(number))
            }
            Self::BFloat16 => {
                let bytes: [u8; 2] = fill_value.as_ne_bytes().try_into().map_err(|_| error())?;
                let number = half::bf16::from_ne_bytes(bytes);
                Ok(FillValueMetadataV3::from(number))
            }
            Self::Complex64 => {
                let bytes: &[u8; 8] = fill_value.as_ne_bytes().try_into().map_err(|_| error())?;
                let re = f32::from_ne_bytes(unsafe { bytes[0..4].try_into().unwrap_unchecked() });
                let im = f32::from_ne_bytes(unsafe { bytes[4..8].try_into().unwrap_unchecked() });
                let re = FillValueMetadataV3::from(re);
                let im = FillValueMetadataV3::from(im);
                Ok(FillValueMetadataV3::from([re, im]))
            }
            Self::Complex128 => {
                let bytes: &[u8; 16] = fill_value.as_ne_bytes().try_into().map_err(|_| error())?;
                let re = f64::from_ne_bytes(unsafe { bytes[0..8].try_into().unwrap_unchecked() });
                let im = f64::from_ne_bytes(unsafe { bytes[8..16].try_into().unwrap_unchecked() });
                let re = FillValueMetadataV3::from(re);
                let im = FillValueMetadataV3::from(im);
                Ok(FillValueMetadataV3::from([re, im]))
            }
            Self::RawBits(size) => {
                let bytes = fill_value.as_ne_bytes();
                if bytes.len() == *size {
                    Ok(FillValueMetadataV3::from(bytes))
                } else {
                    Err(error())
                }
            }
            Self::String => Ok(FillValueMetadataV3::from(
                String::from_utf8(fill_value.as_ne_bytes().to_vec()).map_err(|_| error())?,
            )),
            Self::Bytes => Ok(FillValueMetadataV3::from(fill_value.as_ne_bytes().to_vec())),
            Self::Extension(extension) => extension.metadata_fill_value(fill_value),
        }
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

    use half::bf16;
    use zarrs_metadata::v3::array::{
        data_type,
        nan_representations::{ZARR_NAN_BF16, ZARR_NAN_F16, ZARR_NAN_F32, ZARR_NAN_F64},
    };

    #[test]
    fn data_type_unknown() {
        let json = r#""unknown""#;
        let metadata: MetadataV3 = serde_json::from_str(json).unwrap();
        assert_eq!(
            DataType::from_metadata(&metadata, &ExtensionAliasesDataTypeV3::default())
                .unwrap_err()
                .to_string(),
            "data type unknown is not supported"
        );
    }

    #[test]
    fn data_type_must_understand_false() {
        let json = r#"{"name":"unknown","must_understand": false}"#;
        let metadata: MetadataV3 = serde_json::from_str(json).unwrap();
        assert_eq!(
            DataType::from_metadata(&metadata, &ExtensionAliasesDataTypeV3::default())
                .unwrap_err()
                .to_string(),
            r#"data type must not have `"must_understand": false`"#
        );
    }

    #[test]
    fn data_type_bool() {
        let json = r#""bool""#;
        let metadata: MetadataV3 = serde_json::from_str(json).unwrap();
        let data_type =
            DataType::from_metadata(&metadata, &ExtensionAliasesDataTypeV3::default()).unwrap();
        assert_eq!(format!("{}", data_type), data_type::BOOL);
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        assert_eq!(data_type, DataType::Bool);

        let metadata = serde_json::from_str::<FillValueMetadataV3>("true").unwrap();
        let fill_value = data_type.fill_value_from_metadata(&metadata).unwrap();
        assert_eq!(fill_value.as_ne_bytes(), u8::from(true).to_ne_bytes());
        assert_eq!(
            metadata,
            data_type.metadata_fill_value(&fill_value).unwrap()
        );

        let metadata = serde_json::from_str::<FillValueMetadataV3>("false").unwrap();
        let fill_value = data_type.fill_value_from_metadata(&metadata).unwrap();
        assert_eq!(fill_value.as_ne_bytes(), u8::from(false).to_ne_bytes());
        assert_eq!(
            metadata,
            data_type.metadata_fill_value(&fill_value).unwrap()
        );
    }

    #[test]
    fn data_type_int8() {
        let json = r#""int8""#;
        let metadata: MetadataV3 = serde_json::from_str(json).unwrap();
        let data_type =
            DataType::from_metadata(&metadata, &ExtensionAliasesDataTypeV3::default()).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        assert_eq!(data_type, DataType::Int8);

        let metadata = serde_json::from_str::<FillValueMetadataV3>("-7").unwrap();
        let fill_value = data_type.fill_value_from_metadata(&metadata).unwrap();
        assert_eq!(fill_value.as_ne_bytes(), (-7i8).to_ne_bytes());
        assert_eq!(
            metadata,
            data_type.metadata_fill_value(&fill_value).unwrap()
        );

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
        let metadata: MetadataV3 = serde_json::from_str(json).unwrap();
        let data_type =
            DataType::from_metadata(&metadata, &ExtensionAliasesDataTypeV3::default()).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        assert_eq!(data_type, DataType::Int16);

        let metadata = serde_json::from_str::<FillValueMetadataV3>("-7").unwrap();
        let fill_value = data_type.fill_value_from_metadata(&metadata).unwrap();
        assert_eq!(fill_value.as_ne_bytes(), (-7i16).to_ne_bytes());
        assert_eq!(
            metadata,
            data_type.metadata_fill_value(&fill_value).unwrap()
        );

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
        let metadata: MetadataV3 = serde_json::from_str(json).unwrap();
        let data_type =
            DataType::from_metadata(&metadata, &ExtensionAliasesDataTypeV3::default()).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        assert_eq!(data_type, DataType::Int32);

        let metadata = serde_json::from_str::<FillValueMetadataV3>("-7").unwrap();
        let fill_value = data_type.fill_value_from_metadata(&metadata).unwrap();
        assert_eq!(fill_value.as_ne_bytes(), (-7i32).to_ne_bytes());
        assert_eq!(
            metadata,
            data_type.metadata_fill_value(&fill_value).unwrap()
        );

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
        let metadata: MetadataV3 = serde_json::from_str(json).unwrap();
        let data_type =
            DataType::from_metadata(&metadata, &ExtensionAliasesDataTypeV3::default()).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        assert_eq!(data_type, DataType::Int64);

        let metadata = serde_json::from_str::<FillValueMetadataV3>("-7").unwrap();
        let fill_value = data_type.fill_value_from_metadata(&metadata).unwrap();
        assert_eq!(fill_value.as_ne_bytes(), (-7i64).to_ne_bytes());
        assert_eq!(
            metadata,
            data_type.metadata_fill_value(&fill_value).unwrap()
        );

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
        let metadata: MetadataV3 = serde_json::from_str(json).unwrap();
        let data_type =
            DataType::from_metadata(&metadata, &ExtensionAliasesDataTypeV3::default()).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        assert_eq!(data_type, DataType::UInt8);

        let metadata = serde_json::from_str::<FillValueMetadataV3>("7").unwrap();
        let fill_value = data_type.fill_value_from_metadata(&metadata).unwrap();
        assert_eq!(fill_value.as_ne_bytes(), 7u8.to_ne_bytes());
        assert_eq!(
            metadata,
            data_type.metadata_fill_value(&fill_value).unwrap()
        );
    }

    #[test]
    fn data_type_uint16() {
        let json = r#""uint16""#;
        let metadata: MetadataV3 = serde_json::from_str(json).unwrap();
        let data_type =
            DataType::from_metadata(&metadata, &ExtensionAliasesDataTypeV3::default()).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        assert_eq!(data_type, DataType::UInt16);

        let metadata = serde_json::from_str::<FillValueMetadataV3>("7").unwrap();
        let fill_value = data_type.fill_value_from_metadata(&metadata).unwrap();
        assert_eq!(fill_value.as_ne_bytes(), 7u16.to_ne_bytes());
        assert_eq!(
            metadata,
            data_type.metadata_fill_value(&fill_value).unwrap()
        );
    }

    #[test]
    fn data_type_uint32() {
        let json = r#""uint32""#;
        let metadata: MetadataV3 = serde_json::from_str(json).unwrap();
        let data_type =
            DataType::from_metadata(&metadata, &ExtensionAliasesDataTypeV3::default()).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        assert_eq!(data_type, DataType::UInt32);

        let metadata = serde_json::from_str::<FillValueMetadataV3>("7").unwrap();
        let fill_value = data_type.fill_value_from_metadata(&metadata).unwrap();
        assert_eq!(fill_value.as_ne_bytes(), 7u32.to_ne_bytes());
        assert_eq!(
            metadata,
            data_type.metadata_fill_value(&fill_value).unwrap()
        );
    }

    #[test]
    fn data_type_uint64() {
        let json = r#""uint64""#;
        let metadata: MetadataV3 = serde_json::from_str(json).unwrap();
        let data_type =
            DataType::from_metadata(&metadata, &ExtensionAliasesDataTypeV3::default()).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        assert_eq!(data_type, DataType::UInt64);

        let metadata = serde_json::from_str::<FillValueMetadataV3>("7").unwrap();
        let fill_value = data_type.fill_value_from_metadata(&metadata).unwrap();
        assert_eq!(fill_value.as_ne_bytes(), 7u64.to_ne_bytes());
        assert_eq!(
            metadata,
            data_type.metadata_fill_value(&fill_value).unwrap()
        );
    }

    #[test]
    fn data_type_float32() {
        let json = r#""float32""#;
        let metadata: MetadataV3 = serde_json::from_str(json).unwrap();
        let data_type =
            DataType::from_metadata(&metadata, &ExtensionAliasesDataTypeV3::default()).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        assert_eq!(data_type, DataType::Float32);

        let metadata = serde_json::from_str::<FillValueMetadataV3>("-7.0").unwrap();
        let fill_value = data_type.fill_value_from_metadata(&metadata).unwrap();
        assert_eq!(fill_value.as_ne_bytes(), (-7.0f32).to_ne_bytes());
        assert_eq!(
            metadata,
            data_type.metadata_fill_value(&fill_value).unwrap()
        );

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
        let metadata: MetadataV3 = serde_json::from_str(json).unwrap();
        let data_type =
            DataType::from_metadata(&metadata, &ExtensionAliasesDataTypeV3::default()).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        assert_eq!(data_type, DataType::Float64);

        let metadata = serde_json::from_str::<FillValueMetadataV3>("-7.0").unwrap();
        let fill_value = data_type.fill_value_from_metadata(&metadata).unwrap();
        assert_eq!(fill_value.as_ne_bytes(), (-7.0f64).to_ne_bytes());
        assert_eq!(
            metadata,
            data_type.metadata_fill_value(&fill_value).unwrap()
        );

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
        let metadata: MetadataV3 = serde_json::from_str(json).unwrap();
        let data_type =
            DataType::from_metadata(&metadata, &ExtensionAliasesDataTypeV3::default()).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        assert_eq!(data_type.name(), "float16");

        let metadata = serde_json::from_str::<FillValueMetadataV3>("-7.0").unwrap();
        let fill_value = data_type.fill_value_from_metadata(&metadata).unwrap();
        assert_eq!(
            fill_value.as_ne_bytes(),
            f16::from_f32_const(-7.0).to_ne_bytes()
        );
        assert_eq!(
            metadata,
            data_type.metadata_fill_value(&fill_value).unwrap()
        );

        assert_eq!(
            data_type
                .fill_value_from_metadata(
                    &serde_json::from_str::<FillValueMetadataV3>(r#""NaN""#).unwrap()
                )
                .unwrap()
                .as_ne_bytes(),
            ZARR_NAN_F16.to_ne_bytes()
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
        let metadata: MetadataV3 = serde_json::from_str(json).unwrap();
        let data_type =
            DataType::from_metadata(&metadata, &ExtensionAliasesDataTypeV3::default()).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        assert_eq!(data_type.name(), "bfloat16");

        let metadata = serde_json::from_str::<FillValueMetadataV3>("-7.0").unwrap();
        let fill_value = data_type.fill_value_from_metadata(&metadata).unwrap();
        assert_eq!(
            fill_value.as_ne_bytes(),
            bf16::from_f32_const(-7.0).to_ne_bytes()
        );
        assert_eq!(
            metadata,
            data_type.metadata_fill_value(&fill_value).unwrap()
        );

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
        let metadata: MetadataV3 = serde_json::from_str(json).unwrap();
        let data_type =
            DataType::from_metadata(&metadata, &ExtensionAliasesDataTypeV3::default()).unwrap();
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
        assert_eq!(
            metadata,
            data_type.metadata_fill_value(&fill_value).unwrap()
        );

        let metadata = serde_json::from_str::<FillValueMetadataV3>(r#"-7.0"#).unwrap();
        assert!(data_type.fill_value_from_metadata(&metadata).is_err())
    }

    #[test]
    fn data_type_complex128() {
        let json = r#""complex128""#;
        let metadata: MetadataV3 = serde_json::from_str(json).unwrap();
        let data_type =
            DataType::from_metadata(&metadata, &ExtensionAliasesDataTypeV3::default()).unwrap();
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
        assert_eq!(
            metadata,
            data_type.metadata_fill_value(&fill_value).unwrap()
        );

        let metadata = serde_json::from_str::<FillValueMetadataV3>(r#"-7.0"#).unwrap();
        assert!(data_type.fill_value_from_metadata(&metadata).is_err())
    }

    #[test]
    fn data_type_r8() {
        let json = r#""r8""#;
        let metadata: MetadataV3 = serde_json::from_str(json).unwrap();
        let data_type =
            DataType::from_metadata(&metadata, &ExtensionAliasesDataTypeV3::default()).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        assert_eq!(data_type.name(), "r8");
        assert_eq!(data_type.size(), DataTypeSize::Fixed(1));

        let metadata = serde_json::from_str::<FillValueMetadataV3>("[7]").unwrap();
        let fill_value = data_type.fill_value_from_metadata(&metadata).unwrap();
        assert_eq!(fill_value.as_ne_bytes(), 7u8.to_ne_bytes());
        assert_eq!(
            metadata,
            data_type.metadata_fill_value(&fill_value).unwrap()
        );
    }

    #[test]
    fn data_type_r16() {
        let json = r#""r16""#;
        let metadata: MetadataV3 = serde_json::from_str(json).unwrap();
        let data_type =
            DataType::from_metadata(&metadata, &ExtensionAliasesDataTypeV3::default()).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        assert_eq!(data_type.name(), "r16");
        assert_eq!(data_type.size(), DataTypeSize::Fixed(2));

        let metadata = serde_json::from_str::<FillValueMetadataV3>("[0, 255]").unwrap();
        let fill_value = data_type.fill_value_from_metadata(&metadata).unwrap();
        assert_eq!(
            fill_value.as_ne_bytes(), // NOTE: Raw value bytes are always read as-is.
            &[0u8, 255u8]
        );
        assert_eq!(
            metadata,
            data_type.metadata_fill_value(&fill_value).unwrap()
        );
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
        assert!(
            DataType::from_metadata(&metadata, &ExtensionAliasesDataTypeV3::default()).is_err()
        );
    }

    #[test]
    fn data_type_unknown2() {
        let json = r#""datetime""#;
        let metadata: MetadataV3 = serde_json::from_str(json).unwrap();
        println!("{json:?}");
        println!("{metadata:?}");
        assert_eq!(metadata.name(), "datetime");
        assert!(
            DataType::from_metadata(&metadata, &ExtensionAliasesDataTypeV3::default()).is_err()
        );
    }

    #[test]
    fn data_type_unknown3() {
        let json = r#""ra""#;
        let metadata: MetadataV3 = serde_json::from_str(json).unwrap();
        println!("{json:?}");
        println!("{metadata:?}");
        assert_eq!(metadata.name(), "ra");
        assert!(
            DataType::from_metadata(&metadata, &ExtensionAliasesDataTypeV3::default()).is_err()
        );
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
        let data_type: DataType =
            DataType::from_metadata(&metadata, &ExtensionAliasesDataTypeV3::default()).unwrap();
        assert_eq!(data_type.size(), DataTypeSize::Fixed(2));
    }

    #[test]
    fn data_type_raw_bits2() {
        let json = r#"
    {
        "name": "r16"
    }"#;
        let metadata = serde_json::from_str::<MetadataV3>(json).unwrap();
        let data_type: DataType =
            DataType::from_metadata(&metadata, &ExtensionAliasesDataTypeV3::default()).unwrap();
        assert_eq!(data_type.size(), DataTypeSize::Fixed(2));
    }

    #[test]
    fn data_type_raw_bits_failure1() {
        let json = r#"
    {
        "name": "r5"
    }"#;
        let metadata = serde_json::from_str::<MetadataV3>(json).unwrap();
        assert!(
            DataType::from_metadata(&metadata, &ExtensionAliasesDataTypeV3::default()).is_err()
        );
    }

    #[test]
    fn incompatible_fill_value_metadata() {
        let json = r#""bool""#;
        let metadata: MetadataV3 = serde_json::from_str(json).unwrap();
        let data_type =
            DataType::from_metadata(&metadata, &ExtensionAliasesDataTypeV3::default()).unwrap();
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
        let metadata: MetadataV3 = serde_json::from_str(json).unwrap();
        let data_type =
            DataType::from_metadata(&metadata, &ExtensionAliasesDataTypeV3::default()).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        assert_eq!(data_type, DataType::RawBits(2));

        let metadata = serde_json::from_str::<FillValueMetadataV3>("[123]").unwrap();
        assert_eq!(serde_json::to_string(&metadata).unwrap(), "[123]");
        // assert_eq!(metadata.to_string(), "[123]");
        let fill_value_err = data_type.fill_value_from_metadata(&metadata).unwrap_err();
        assert_eq!(
            fill_value_err.to_string(),
            "incompatible fill value [123] for data type r16"
        );
    }

    #[test]
    fn float_fill_value() {
        assert_eq!(
            FillValueMetadataV3::from(half::f16::INFINITY),
            serde_json::from_str(r#""Infinity""#).unwrap()
        );
        assert_eq!(
            FillValueMetadataV3::from(half::f16::NEG_INFINITY),
            serde_json::from_str(r#""-Infinity""#).unwrap()
        );
        assert_eq!(
            FillValueMetadataV3::from(ZARR_NAN_F16),
            serde_json::from_str(r#""NaN""#).unwrap()
        );
        let f16_nan_alt = unsafe { std::mem::transmute::<u16, half::f16>(0b01_11111_000000001) };
        assert!(f16_nan_alt.is_nan());
        assert_eq!(
            FillValueMetadataV3::from(f16_nan_alt),
            serde_json::from_str(r#""0x7e01""#).unwrap()
        );
        assert_eq!(
            FillValueMetadataV3::from(bf16::INFINITY),
            serde_json::from_str(r#""Infinity""#).unwrap()
        );
        assert_eq!(
            FillValueMetadataV3::from(bf16::NEG_INFINITY),
            serde_json::from_str(r#""-Infinity""#).unwrap()
        );
        assert_eq!(
            FillValueMetadataV3::from(ZARR_NAN_BF16),
            serde_json::from_str(r#""NaN""#).unwrap()
        );
        let bf16_nan_alt = unsafe { std::mem::transmute::<u16, bf16>(0b0_01111_11111000001) };
        assert!(bf16_nan_alt.is_nan());
        assert_eq!(
            FillValueMetadataV3::from(bf16_nan_alt),
            serde_json::from_str(r#""0x7fc1""#).unwrap()
        );
        assert_eq!(
            FillValueMetadataV3::from(f32::INFINITY),
            serde_json::from_str(r#""Infinity""#).unwrap()
        );
        assert_eq!(
            FillValueMetadataV3::from(f32::NEG_INFINITY),
            serde_json::from_str(r#""-Infinity""#).unwrap()
        );
        assert_eq!(
            FillValueMetadataV3::from(ZARR_NAN_F32),
            serde_json::from_str(r#""NaN""#).unwrap()
        );
        let f32_nan_alt =
            unsafe { std::mem::transmute::<u32, f32>(0b0_11111111_10000000000000000000001) };
        assert!(f32_nan_alt.is_nan());
        assert_eq!(
            FillValueMetadataV3::from(f32_nan_alt),
            serde_json::from_str(r#""0x7fc00001""#).unwrap()
        );
        assert_eq!(
            FillValueMetadataV3::from(f64::INFINITY),
            serde_json::from_str(r#""Infinity""#).unwrap()
        );
        assert_eq!(
            FillValueMetadataV3::from(f64::NEG_INFINITY),
            serde_json::from_str(r#""-Infinity""#).unwrap()
        );
        assert_eq!(
            FillValueMetadataV3::from(ZARR_NAN_F64),
            serde_json::from_str(r#""NaN""#).unwrap()
        );
        let f64_nan_alt = unsafe {
            std::mem::transmute::<u64, f64>(
                0b0_11111111111_1000000000000000000000000000000000000000000000000001,
            )
        };
        assert!(f64_nan_alt.is_nan());
        assert_eq!(
            FillValueMetadataV3::from(f64_nan_alt),
            serde_json::from_str(r#""0x7ff8000000000001""#).unwrap()
        );
    }

    #[test]
    fn incompatible_fill_value() {
        let err = DataTypeFillValueError::new(data_type::BOOL.to_string(), FillValue::from(1.0f32));
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
        let metadata: MetadataV3 = serde_json::from_str(json).unwrap();
        let data_type =
            DataType::from_metadata(&metadata, &ExtensionAliasesDataTypeV3::default()).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        assert_eq!(data_type.name(), "string");
        assert_eq!(data_type.size(), DataTypeSize::Variable);

        let metadata = serde_json::from_str::<FillValueMetadataV3>(r#""hello world""#).unwrap();
        let fill_value = data_type.fill_value_from_metadata(&metadata).unwrap();
        assert_eq!(fill_value.as_ne_bytes(), "hello world".as_bytes(),);
        assert_eq!(
            metadata,
            data_type.metadata_fill_value(&fill_value).unwrap()
        );

        let metadata = serde_json::from_str::<FillValueMetadataV3>(r#""Infinity""#).unwrap();
        let fill_value = data_type.fill_value_from_metadata(&metadata).unwrap();
        assert_eq!(fill_value.as_ne_bytes(), "Infinity".as_bytes(),);
        assert_eq!(
            metadata,
            data_type.metadata_fill_value(&fill_value).unwrap()
        );

        let metadata = serde_json::from_str::<FillValueMetadataV3>(r#""0x7fc00000""#).unwrap();
        let fill_value = data_type.fill_value_from_metadata(&metadata).unwrap();
        assert_eq!(fill_value.as_ne_bytes(), "0x7fc00000".as_bytes(),);
        assert_eq!(
            metadata,
            data_type.metadata_fill_value(&fill_value).unwrap()
        );
    }

    #[test]
    fn data_type_bytes() {
        let json = r#""bytes""#;
        let metadata: MetadataV3 = serde_json::from_str(json).unwrap();
        let data_type =
            DataType::from_metadata(&metadata, &ExtensionAliasesDataTypeV3::default()).unwrap();
        assert_eq!(json, serde_json::to_string(&data_type.metadata()).unwrap());
        assert_eq!(data_type.name(), "bytes");
        assert_eq!(data_type.size(), DataTypeSize::Variable);

        let metadata = serde_json::from_str::<FillValueMetadataV3>(r#"[0, 1, 2, 3]"#).unwrap();
        let fill_value = data_type.fill_value_from_metadata(&metadata).unwrap();
        assert_eq!(fill_value.as_ne_bytes(), [0, 1, 2, 3],);
        assert_eq!(
            metadata,
            data_type.metadata_fill_value(&fill_value).unwrap()
        );
    }
}
