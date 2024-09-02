use derive_more::Display;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    v2::MetadataV2, v3::AdditionalFields, ArrayShape, ChunkKeySeparator, ChunkShape, Endianness,
};

/// Zarr V2 codec metadata.
pub mod codec {
    /// `bitround` codec metadata.
    pub mod bitround;
    /// `blosc` codec metadata.
    pub mod blosc;
    /// `bz2` codec metadata.
    pub mod bz2;
    /// `gzip` codec metadata.
    pub mod gzip;
    /// `zfpy` codec metadata.
    pub mod zfpy;
    /// `zstd` codec metadata.
    pub mod zstd;
}

/// Zarr array metadata (storage specification v2).
///
/// An example `JSON` document for a Zarr V2 array:
/// ```json
/// {
///     "chunks": [
///         1000,
///         1000
///     ],
///     "compressor": {
///         "id": "blosc",
///         "cname": "lz4",
///         "clevel": 5,
///         "shuffle": 1
///     },
///     "dtype": "<f8",
///     "fill_value": "NaN",
///     "filters": [
///         {"id": "delta", "dtype": "<f8", "astype": "<f4"}
///     ],
///     "order": "C",
///     "shape": [
///         10000,
///         10000
///     ],
///     "zarr_format": 2
/// }
/// ```
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug, Display)]
#[serde(tag = "node_type", rename = "array")]
#[display("{}", serde_json::to_string(self).unwrap_or_default())]
pub struct ArrayMetadataV2 {
    /// An integer defining the version of the storage specification to which the array adheres. Must be `2`.
    pub zarr_format: monostate::MustBe!(2u64),
    /// An array of integers providing the length of each dimension of the Zarr array.
    pub shape: ArrayShape,
    /// A list of integers defining the length of each dimension of a chunk of the array.
    pub chunks: ChunkShape,
    /// The data type of the Zarr array.
    pub dtype: DataTypeMetadataV2,
    /// A JSON object identifying the primary compression codec and providing configuration parameters, or null if no compressor is to be used.
    pub compressor: Option<MetadataV2>,
    /// A scalar value providing the default value to use for uninitialized portions of the array, or null if no fill value is to be used.
    pub fill_value: FillValueMetadataV2,
    /// Either “C” or “F”, defining the layout of bytes within each chunk of the array.
    pub order: ArrayMetadataV2Order,
    /// A list of JSON objects providing codec configurations, or null if no filters are to be applied.
    #[serde(default)]
    pub filters: Option<Vec<MetadataV2>>,
    /// If present, either the string "." or "/" defining the separator placed between the dimensions of a chunk.
    #[serde(default = "chunk_key_separator_default_zarr_v2")]
    pub dimension_separator: ChunkKeySeparator,
    /// Optional user defined attributes contained in a separate `.zattrs` file.
    #[serde(default, skip_serializing_if = "serde_json::Map::is_empty")]
    pub attributes: serde_json::Map<String, serde_json::Value>,
    /// Additional fields.
    ///
    /// These are not part of Zarr V2, but are retained for compatibility/flexibility.
    #[serde(flatten)]
    pub additional_fields: AdditionalFields,
}

const fn chunk_key_separator_default_zarr_v2() -> ChunkKeySeparator {
    ChunkKeySeparator::Dot
}

/// Structure data type metadata.
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
#[serde(
    from = "DataTypeMetadataV2StructuredTuple",
    into = "DataTypeMetadataV2StructuredTuple"
)]
pub struct DataTypeMetadataV2Structured {
    /// Field name.
    fieldname: String,
    /// Data type.
    datatype: String,
    /// Subarray shape.
    shape: Option<Vec<u64>>,
}

#[derive(Serialize, Deserialize)]
struct DataTypeMetadataV2StructuredTuple(
    String,
    String,
    #[serde(skip_serializing_if = "Option::is_none")] Option<Vec<u64>>,
);

impl From<DataTypeMetadataV2StructuredTuple> for DataTypeMetadataV2Structured {
    fn from(value: DataTypeMetadataV2StructuredTuple) -> Self {
        let DataTypeMetadataV2StructuredTuple(fieldname, datatype, shape) = value;
        Self {
            fieldname,
            datatype,
            shape,
        }
    }
}

impl From<DataTypeMetadataV2Structured> for DataTypeMetadataV2StructuredTuple {
    fn from(value: DataTypeMetadataV2Structured) -> Self {
        Self(value.fieldname, value.datatype, value.shape)
    }
}

/// Zarr V2 data type metadata.
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
#[serde(untagged)]
pub enum DataTypeMetadataV2 {
    /// A simple data type.
    Simple(String),
    /// A structured data type.
    Structured(Vec<DataTypeMetadataV2Structured>),
}

/// A Zarr V2 invalid data type endianness error.
#[derive(Debug, Error)]
#[error("invalid V2 data type for {_0:?} endianness, must begin with |, < or >")]
pub struct DataTypeMetadataV2InvalidEndiannessError(DataTypeMetadataV2);

/// Get the endianness of a Zarr V2 data type.
///
/// # Errors
/// Returns a [`DataTypeMetadataV2InvalidEndiannessError`] if the data type is not supported or the endianness prefix is invalid.
pub fn data_type_metadata_v2_to_endianness(
    data_type: &DataTypeMetadataV2,
) -> Result<Option<Endianness>, DataTypeMetadataV2InvalidEndiannessError> {
    match data_type {
        DataTypeMetadataV2::Simple(data_type_str) => {
            if data_type_str.starts_with('|') {
                Ok(None)
            } else if data_type_str.starts_with('<') {
                Ok(Some(Endianness::Little))
            } else if data_type_str.starts_with('>') {
                Ok(Some(Endianness::Big))
            } else {
                Err(DataTypeMetadataV2InvalidEndiannessError(data_type.clone()))
            }
        }
        DataTypeMetadataV2::Structured(_) => {
            Err(DataTypeMetadataV2InvalidEndiannessError(data_type.clone()))
        }
    }
}

/// A scalar value providing the default value to use for uninitialized portions of the array, or null if no fill value is to be used.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum FillValueMetadataV2 {
    /// No fill value.
    Null,
    /// NaN (not-a-number).
    NaN,
    /// Positive infinity.
    Infinity,
    /// Negative infinity.
    NegInfinity,
    /// A number.
    Number(serde_json::Number),
}

impl<'de> serde::Deserialize<'de> for FillValueMetadataV2 {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum FillValueMetadataV2Type {
            String(String),
            Number(serde_json::Number),
            Null,
        }
        let fill_value = FillValueMetadataV2Type::deserialize(d)?;
        match fill_value {
            FillValueMetadataV2Type::String(string) => match string.as_str() {
                "NaN" => Ok(Self::NaN),
                "Infinity" => Ok(Self::Infinity),
                "-Infinity" => Ok(Self::NegInfinity),
                _ => Err(serde::de::Error::custom("unsupported fill value")),
            },
            FillValueMetadataV2Type::Number(number) => Ok(Self::Number(number)),
            FillValueMetadataV2Type::Null => Ok(Self::Null),
        }
    }
}

impl Serialize for FillValueMetadataV2 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Self::Null => serializer.serialize_none(),
            Self::NaN => serializer.serialize_str("NaN"),
            Self::Infinity => serializer.serialize_str("Infinity"),
            Self::NegInfinity => serializer.serialize_str("-Infinity"),
            Self::Number(number) => number.serialize(serializer),
        }
    }
}

/// The layout of bytes within each chunk of the array.
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub enum ArrayMetadataV2Order {
    /// Row-major order. The last dimension varies fastest.
    C,
    /// Column-major order. The first dimension varies fastest.
    F,
}
