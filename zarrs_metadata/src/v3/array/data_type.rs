//! Zarr V3 data type metadata.
//!
//! See <https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html#data-types>.

use crate::v3::MetadataV3;

/// A data type.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
#[rustfmt::skip]
pub enum DataTypeMetadataV3 {
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
    /// An unknown extension data type.
    Extension(MetadataV3),
}

impl serde::Serialize for DataTypeMetadataV3 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.metadata().serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for DataTypeMetadataV3 {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let metadata = MetadataV3::deserialize(d)?;
        Ok(Self::from_metadata(&metadata))
    }
}

// /// A data type plugin.
// pub type DataTypePlugin = Plugin<Box<dyn DataTypeExtension>>;
// inventory::collect!(DataTypePlugin);

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

// dyn_clone::clone_trait_object!(DataTypeExtension);

impl DataTypeMetadataV3 {
    /// Returns the name.
    #[must_use]
    pub fn name(&self) -> String {
        match self {
            Self::Bool => "bool".to_string(),
            Self::Int8 => "int8".to_string(),
            Self::Int16 => "int16".to_string(),
            Self::Int32 => "int32".to_string(),
            Self::Int64 => "int64".to_string(),
            Self::UInt8 => "uint8".to_string(),
            Self::UInt16 => "uint16".to_string(),
            Self::UInt32 => "uint32".to_string(),
            Self::UInt64 => "uint64".to_string(),
            Self::Float16 => "float16".to_string(),
            Self::Float32 => "float32".to_string(),
            Self::Float64 => "float64".to_string(),
            Self::BFloat16 => "bfloat16".to_string(),
            Self::Complex64 => "complex64".to_string(),
            Self::Complex128 => "complex128".to_string(),
            Self::String => "string".to_string(),
            Self::Bytes => "bytes".to_string(),
            Self::RawBits(size) => format!("r{}", size * 8),
            Self::Extension(metadata) => metadata.name().to_string(),
        }
    }

    /// Returns the metadata.
    #[must_use]
    pub fn metadata(&self) -> MetadataV3 {
        match self {
            Self::Extension(metadata) => metadata.clone(),
            _ => MetadataV3::new(&self.name()),
        }
    }

    /// Create a data type from metadata.
    #[must_use]
    pub fn from_metadata(metadata: &MetadataV3) -> Self {
        let name = metadata.name();

        match name {
            "bool" => return Self::Bool,
            "int8" => return Self::Int8,
            "int16" => return Self::Int16,
            "int32" => return Self::Int32,
            "int64" => return Self::Int64,
            "uint8" => return Self::UInt8,
            "uint16" => return Self::UInt16,
            "uint32" => return Self::UInt32,
            "uint64" => return Self::UInt64,
            "float16" => return Self::Float16,
            "float32" => return Self::Float32,
            "float64" => return Self::Float64,
            "bfloat16" => return Self::BFloat16,
            "complex64" => return Self::Complex64,
            "complex128" => return Self::Complex128,
            "string" => return Self::String,
            "bytes" => return Self::Bytes,
            _ => {}
        }

        if name.starts_with('r') && name.len() > 1 {
            if let Ok(size_bits) = metadata.name()[1..].parse::<usize>() {
                if size_bits % 8 == 0 {
                    let size_bytes = size_bits / 8;
                    return Self::RawBits(size_bytes);
                }
            }
        }

        Self::Extension(metadata.clone())
    }
}

impl From<MetadataV3> for DataTypeMetadataV3 {
    fn from(metadata: MetadataV3) -> Self {
        Self::from_metadata(&metadata)
    }
}

impl core::fmt::Display for DataTypeMetadataV3 {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        write!(f, "{}", self.name())
    }
}
