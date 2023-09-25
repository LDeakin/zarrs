//! Zarr array metadata.
//!
//! See <https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html#array-metadata>.

use derive_more::{Display, From};
use serde::{Deserialize, Serialize};

use crate::{
    array::FillValueMetadata,
    metadata::{AdditionalFields, Metadata},
};

use super::DimensionName;

/// Zarr array metadata.
#[derive(Serialize, Deserialize, Clone, PartialEq, Debug, Display, From)]
#[serde(untagged)]
pub enum ArrayMetadata {
    /// Version 3.0.
    V3(ArrayMetadataV3),
}

/// Zarr array metadata (storage specification v3).
///
/// An example `JSON` document for a v3 array:
/// ```json
/// {
///     "zarr_format": 3,
///     "node_type": "array",
///     "shape": [10000, 1000],
///     "dimension_names": ["rows", "columns"],
///     "data_type": "float64",
///     "chunk_grid": {
///         "name": "regular",
///         "configuration": {
///             "chunk_shape": [1000, 100]
///         }
///     },
///     "chunk_key_encoding": {
///         "name": "default",
///         "configuration": {
///             "separator": "/"
///         }
///     },
///     "codecs": [{
///         "name": "gzip",
///         "configuration": {
///             "level": 1
///         }
///     }],
///     "fill_value": "NaN",
///     "attributes": {
///         "foo": 42,
///         "bar": "apples",
///         "baz": [1, 2, 3, 4]
///     }
/// }
/// ```
#[derive(Serialize, Deserialize, Clone, PartialEq, Debug, Display)]
#[serde(tag = "node_type", rename = "array")]
#[display(fmt = "{}", "serde_json::to_string(self).unwrap_or_default()")]
pub struct ArrayMetadataV3 {
    /// An integer defining the version of the storage specification to which the array store adheres.
    pub zarr_format: usize,
    /// A string defining the type of hierarchy node element, must be `array` here.
    #[serde(skip_serializing)]
    pub node_type: String, // Ideally this is serialized after zarr format, and tag serialization is skipped
    /// An array of integers providing the length of each dimension of the Zarr array.
    pub shape: Vec<usize>,
    /// The data type of the Zarr array.
    pub data_type: Metadata,
    /// The chunk grid of the Zarr array.
    pub chunk_grid: Metadata,
    /// The mapping from chunk grid cell coordinates to keys in the underlying store.
    pub chunk_key_encoding: Metadata,
    /// Provides an element value to use for uninitialised portions of the Zarr array.
    ///
    /// Suitable values are dependent on the data type.
    ///
    /// Boolean.
    /// *The value must be a JSON boolean (false or true).*
    ///
    /// Signed integers (`int{8,16,32,64}`) or unsigned integers (`uint{8,16,32,64}`).
    /// *The value must be a JSON number with no fraction or exponent part that is within the representable range of the data type.*
    ///
    /// Floating point numbers (`float{16,32,64}`, `bfloat16`).
    ///  - *A JSON number, that will be rounded to the nearest representable value.*
    ///  - *A JSON string of the form:*
    ///  - *`"Infinity"`, denoting positive infinity;*
    ///  - *`"-Infinity"`, denoting negative infinity;*
    ///  - *`"NaN"`, denoting thenot-a-number (NaN) value where the sign bit is 0 (positive), the most significant bit (MSB) of the mantissa is 1, and all other bits of the mantissa are zero;*
    ///  - *`"0xYYYYYYYY"`, specifying the byte representation of the floating point number as an unsigned integer.
    ///
    /// Complex numbers (`complex{64,128}`)
    /// *The value must be a two-element array, specifying the real and imaginary components respectively, where each component is specified as defined above for floating point number.*
    ///
    /// Raw data types (`r<N>`)
    /// *An array of integers, with length equal to `<N>`, where each integer is in the range `[0, 255]`.*
    pub fill_value: FillValueMetadata,
    /// Specifies a list of codecs to be used for encoding and decoding chunks.
    pub codecs: Vec<Metadata>,
    /// Optional user defined attributes.
    #[serde(default, skip_serializing_if = "serde_json::Map::is_empty")]
    pub attributes: serde_json::Map<String, serde_json::Value>,
    /// An optional list of storage transformers.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub storage_transformers: Vec<Metadata>,
    /// An optional list of dimension names.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dimension_names: Option<Vec<DimensionName>>,
    /// Additional fields.
    #[serde(flatten)]
    pub additional_fields: AdditionalFields,
}

impl ArrayMetadataV3 {
    /// Create a new array metadata.
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new(
        shape: Vec<usize>,
        data_type: Metadata,
        chunk_grid: Metadata,
        chunk_key_encoding: Metadata,
        fill_value: FillValueMetadata,
        codecs: Vec<Metadata>,
        attributes: serde_json::Map<String, serde_json::Value>,
        storage_transformers: Vec<Metadata>,
        dimension_names: Option<Vec<DimensionName>>,
        additional_fields: AdditionalFields,
    ) -> Self {
        Self {
            zarr_format: 3,
            node_type: "array".to_string(),
            shape,
            data_type,
            chunk_grid,
            chunk_key_encoding,
            fill_value,
            codecs,
            attributes,
            storage_transformers,
            dimension_names,
            additional_fields,
        }
    }

    /// Validates that the `zarr_format` field is `3`.
    #[must_use]
    pub fn validate_format(&self) -> bool {
        self.zarr_format == 3
    }

    /// Validates that the `node_type` is `"array"`.
    #[must_use]
    pub fn validate_node_type(&self) -> bool {
        self.node_type == "array"
    }
}
