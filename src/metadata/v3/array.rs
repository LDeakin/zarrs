use derive_more::Display;
use serde::{Deserialize, Serialize};

use super::{AdditionalFields, MetadataV3};
use crate::array::{ArrayShape, DimensionName, FillValueMetadata};

/// Zarr array metadata (storage specification v3).
///
/// An example `JSON` document for a Zarr V3 array:
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
#[display("{}", serde_json::to_string(self).unwrap_or_default())]
pub struct ArrayMetadataV3 {
    /// An integer defining the version of the storage specification to which the array adheres. Must be `3`.
    pub zarr_format: monostate::MustBe!(3u64),
    /// A string defining the type of hierarchy node element, must be `array` here.
    pub node_type: monostate::MustBe!("array"),
    /// An array of integers providing the length of each dimension of the Zarr array.
    pub shape: ArrayShape,
    /// The data type of the Zarr array.
    pub data_type: MetadataV3,
    /// The chunk grid of the Zarr array.
    pub chunk_grid: MetadataV3,
    /// The mapping from chunk grid cell coordinates to keys in the underlying store.
    pub chunk_key_encoding: MetadataV3,
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
    pub codecs: Vec<MetadataV3>,
    /// Optional user defined attributes.
    #[serde(default, skip_serializing_if = "serde_json::Map::is_empty")]
    pub attributes: serde_json::Map<String, serde_json::Value>,
    /// An optional list of storage transformers.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub storage_transformers: Vec<MetadataV3>,
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
        shape: ArrayShape,
        data_type: MetadataV3,
        chunk_grid: MetadataV3,
        chunk_key_encoding: MetadataV3,
        fill_value: FillValueMetadata,
        codecs: Vec<MetadataV3>,
        attributes: serde_json::Map<String, serde_json::Value>,
        storage_transformers: Vec<MetadataV3>,
        dimension_names: Option<Vec<DimensionName>>,
        additional_fields: AdditionalFields,
    ) -> Self {
        Self {
            zarr_format: monostate::MustBe!(3u64),
            node_type: monostate::MustBe!("array"),
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
}
