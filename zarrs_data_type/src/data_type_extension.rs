use std::fmt::Debug;
use zarrs_metadata::{
    v3::{array::fill_value::FillValueMetadataV3, MetadataConfiguration},
    DataTypeSize,
};

use crate::{
    DataTypeExtensionBytesCodec, DataTypeExtensionBytesCodecError, FillValue,
    IncompatibleFillValueError, IncompatibleFillValueMetadataError,
};

/// Traits for a data type extension.
///
/// The in-memory size of a data type can differ between its associated Rust structure and the *serialised* [`ArrayBytes`](https://docs.rs/zarrs/latest/zarrs/array/enum.ArrayBytes.html) passed into the codec pipeline.
/// For example, a Rust struct that has padding bytes can be converted to tightly packed bytes before it is passed into the codec pipeline for encoding, and vice versa for decoding.
///
/// It is recommended to define a concrete structure representing a single element of a custom data type that implements [`Element`](https://docs.rs/zarrs/latest/zarrs/array/trait.Element.html) and [`ElementOwned`](https://docs.rs/zarrs/latest/zarrs/array/trait.ElementOwned.html).
/// These traits have `into_array_bytes` and `from_array_bytes` methods for this purpose that enable custom data types to be used with the [`Array::{store,retrieve}_*_elements`](https://docs.rs/zarrs/latest/zarrs/array/struct.Array.html) variants.
/// These methods should encode data to and from native endianness if endianness is applicable, unless the endianness should be explicitly fixed.
/// Note that codecs that act on numerical data typically expect the data to be in native endianness.
///
/// The [`DataTypeExtensionBytesCodec`] traits methods allow a fixed-size custom data type to be encoded with the `bytes` codec with a requested endianness.
/// These methods are not invoked for variable-size data types, and can be pass-through for a fixed-size data types that use an explicitly fixed endianness or where endianness is not applicable.
///
/// A custom data type must also directly handle conversion of fill value metadata to fill value bytes, and vice versa.
pub trait DataTypeExtension: Debug + Send + Sync {
    /// The name of the data type.
    fn name(&self) -> String;

    /// The configuration of the data type.
    fn configuration(&self) -> MetadataConfiguration;

    /// The size of the data type.
    ///
    /// This size may differ from the size in memory of the data type.
    /// It represents the size of elements passing through array to array and array to bytes codecs in the codec pipeline (i.e., after conversion to [`ArrayBytes`](https://docs.rs/zarrs/latest/zarrs/array/enum.ArrayBytes.html)).
    fn size(&self) -> DataTypeSize;

    /// Create a fill value from metadata.
    ///
    /// # Errors
    /// Returns [`IncompatibleFillValueMetadataError`] if the fill value is incompatible with the data type.
    fn fill_value(
        &self,
        fill_value_metadata: &FillValueMetadataV3,
    ) -> Result<FillValue, IncompatibleFillValueMetadataError>;

    /// Create fill value metadata.
    ///
    /// # Errors
    /// Returns an [`IncompatibleFillValueError`] if the metadata cannot be created from the fill value.
    fn metadata_fill_value(
        &self,
        fill_value: &FillValue,
    ) -> Result<FillValueMetadataV3, IncompatibleFillValueError>;

    /// Return [`DataTypeExtensionBytesCodec`] if the data type supports the `bytes` codec.
    ///
    /// Fixed-size data types are expected to support the `bytes` codec, even if bytes pass through it unmodified.
    ///
    /// The default implementation returns [`DataTypeExtensionError::CodecUnsupported`].
    ///
    /// # Errors
    /// Returns [`DataTypeExtensionError::CodecUnsupported`] if the `bytes` codec is unsupported.
    fn codec_bytes(&self) -> Result<&dyn DataTypeExtensionBytesCodec, DataTypeExtensionError> {
        Err(DataTypeExtensionError::CodecUnsupported {
            data_type: self.name(),
            codec: "bytes".to_string(),
        })
    }
}

/// A data type extension error.
#[derive(Debug, thiserror::Error, derive_more::Display)]
#[non_exhaustive]
pub enum DataTypeExtensionError {
    /// Codec not supported
    #[display("The {codec} codec is not supported by the {data_type} extension data type")]
    CodecUnsupported {
        /// The data type name.
        data_type: String,
        /// The codec name.
        codec: String,
    },
    /// A `bytes` codec error.
    BytesCodec(#[from] DataTypeExtensionBytesCodecError),
}
