use derive_more::Display;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    array::{ArrayShape, ChunkShape, DataType, Endianness},
    metadata::{
        v3::{
            fill_value::{FillValueFloat, FillValueFloatStringNonFinite, FillValueMetadata},
            ChunkKeySeparator,
        },
        AdditionalFields,
    },
};

use super::MetadataV2;

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
    pub dtype: ArrayMetadataV2DataType,
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
pub enum ArrayMetadataV2DataType {
    /// A simple data type.
    Simple(String),
    /// A structured data type.
    Structured(Vec<DataTypeMetadataV2Structured>),
}

/// An unsupported Zarr V2 data type error.
#[derive(Debug, Error)]
#[error("V2 data type {_0:?} is not supported")]
pub struct DataTypeMetadataV2UnsupportedDataTypeError(ArrayMetadataV2DataType);

/// Convert a Zarr V2 data type to a compatible V3 data type.
///
/// # Errors
/// Returns a [`DataTypeMetadataV2UnsupportedDataTypeError`] if the data type is not supported.
pub fn data_type_metadata_v2_to_v3_data_type(
    data_type: &ArrayMetadataV2DataType,
) -> Result<DataType, DataTypeMetadataV2UnsupportedDataTypeError> {
    match data_type {
        ArrayMetadataV2DataType::Simple(data_type_str) => {
            match data_type_str.as_str() {
                "|b1" => Ok(DataType::Bool),
                "|i1" => Ok(DataType::Int8),
                "<i2" | ">i2" => Ok(DataType::Int16),
                "<i4" | ">i4" => Ok(DataType::Int32),
                "<i8" | ">i8" => Ok(DataType::Int64),
                "|u1" => Ok(DataType::UInt8),
                "<u2" | ">u2" => Ok(DataType::UInt16),
                "<u4" | ">u4" => Ok(DataType::UInt32),
                "<u8" | ">u8" => Ok(DataType::UInt64),
                "<f2" | ">f2" => Ok(DataType::Float16),
                "<f4" | ">f4" => Ok(DataType::Float32),
                "<f8" | ">f8" => Ok(DataType::Float64),
                "<c8" | ">c8" => Ok(DataType::Complex64),
                "<c16" | ">c16" => Ok(DataType::Complex128),
                "|O" => Ok(DataType::String), // LEGACY: This is not part of the spec. The dtype for a PyObject, which is what zarr-python 2 uses for string arrays.
                // TODO "|mX" timedelta
                // TODO "|MX" datetime
                // TODO "|SX" string (fixed length sequence of char)
                // TODO "|UX" string (fixed length sequence of Py_UNICODE)
                // TODO "|VX" other (void * – each item is a fixed-size chunk of memory)
                _ => Err(DataTypeMetadataV2UnsupportedDataTypeError(
                    data_type.clone(),
                )),
            }
        }
        ArrayMetadataV2DataType::Structured(_) => Err(DataTypeMetadataV2UnsupportedDataTypeError(
            data_type.clone(),
        )),
    }
}

/// A Zarr V2 invalid data type endianness error.
#[derive(Debug, Error)]
#[error("invalid V2 data type for {_0:?} endianness, must begin with |, < or >")]
pub struct DataTypeMetadataV2InvalidEndiannessError(ArrayMetadataV2DataType);

/// Get the endianness of a Zarr V2 data type.
///
/// # Errors
/// Returns a [`DataTypeMetadataV2InvalidEndiannessError`] if the data type is not supported or the endianness prefix is invalid.
pub fn data_type_metadata_v2_to_endianness(
    data_type: &ArrayMetadataV2DataType,
) -> Result<Option<Endianness>, DataTypeMetadataV2InvalidEndiannessError> {
    match data_type {
        ArrayMetadataV2DataType::Simple(data_type_str) => {
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
        ArrayMetadataV2DataType::Structured(_) => {
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

/// Convert Zarr V2 fill value metadata to [`FillValueMetadata`].
///
/// Returns [`None`] for [`FillValueMetadataV2::Null`].
#[must_use]
pub fn array_metadata_fill_value_v2_to_v3(
    fill_value: &FillValueMetadataV2,
) -> Option<FillValueMetadata> {
    match fill_value {
        FillValueMetadataV2::Null => None,
        FillValueMetadataV2::NaN => Some(FillValueMetadata::Float(FillValueFloat::NonFinite(
            FillValueFloatStringNonFinite::NaN,
        ))),
        FillValueMetadataV2::Infinity => Some(FillValueMetadata::Float(FillValueFloat::NonFinite(
            FillValueFloatStringNonFinite::PosInfinity,
        ))),
        FillValueMetadataV2::NegInfinity => Some(FillValueMetadata::Float(
            FillValueFloat::NonFinite(FillValueFloatStringNonFinite::NegInfinity),
        )),
        FillValueMetadataV2::Number(number) => {
            if let Some(u) = number.as_u64() {
                Some(FillValueMetadata::UInt(u))
            } else if let Some(i) = number.as_i64() {
                Some(FillValueMetadata::Int(i))
            } else if let Some(f) = number.as_f64() {
                Some(FillValueMetadata::Float(FillValueFloat::Float(f)))
            } else {
                unreachable!("number must be convertible to u64, i64 or f64")
            }
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
