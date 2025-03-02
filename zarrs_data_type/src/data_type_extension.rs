use std::{borrow::Cow, fmt::Debug};
use zarrs_metadata::{
    v3::{
        array::{data_type::DataTypeSize, fill_value::FillValueMetadataV3},
        MetadataConfiguration,
    },
    Endianness,
};

use crate::{FillValue, IncompatibleFillValueError, IncompatibleFillValueMetadataError};

/// Traits for a data type extension.
///
/// The size in memory of a data type can differ between the in-memory Rust structure and the [`ArrayBytes`](https://docs.rs/zarrs/latest/zarrs/array/enum.ArrayBytes.html) passed into the codec pipeline.
/// For example, consider a structure that has padding bytes when used in memory in Rust.
/// This can be converted to tightly packed bytes before it is passed into the codec pipeline for encoding, and vice versa for decoding.
///
/// It is recommended to define a concrete structure representing a single element of a custom data type that implements [`Element`](https://docs.rs/zarrs/latest/zarrs/array/trait.Element.html) and [`ElementOwned`](https://docs.rs/zarrs/latest/zarrs/array/trait.ElementOwned.html).
/// These traits have `into_array_bytes` and `from_array_bytes` methods for this purpose that enable custom data types to be used with the [`Array::{store,retrieve}_*_elements`](https://docs.rs/zarrs/latest/zarrs/array/struct.Array.html) variants.
/// These methods should encode data to and from native endianness if endianness is applicable, unless the endianness should be explicitly fixed.
/// Note that codecs that act on numerical data typically expect the data to be in native endianness.
///
/// The [`DataTypeExtension::encode_bytes`] and [`DataTypeExtension::decode_bytes`] methods allow a fixed-size custom data type to be encoded with the `bytes` codec with a requested [`Endianness`].
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
    /// It represents the size of elements passing through array to array and array to bytes codecs in the codec pipeline (i.e., after conversion to [`ArrayBytes`](https://docs.rs/zarrs/latest/zarrs/array/enum.ArrayBytes.html))).
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

    /// Encode the bytes to a specified endianness.
    ///
    /// This is used internally within the `bytes` codec if the data type is fixed size.
    ///
    /// Return [`DataTypeExtensionError::BytesCodecUnsupported`] if the codec does not support the `bytes` codec.
    ///
    /// # Errors
    /// Returns a [`DataTypeExtensionError`] if the `bytes` codec is not supported or `endianness` has not been specified.
    #[allow(unused_variables)]
    fn encode_bytes<'a>(
        &self,
        bytes: Cow<'a, [u8]>,
        endianness: Option<Endianness>,
    ) -> Result<Cow<'a, [u8]>, DataTypeExtensionError>;

    /// Decode bytes from a specified endianness.
    ///
    /// This is used internally within the `bytes` codec if the data type is fixed size.
    ///
    /// Return [`DataTypeExtensionError::BytesCodecUnsupported`] if the codec does not support the `bytes` codec.
    ///
    /// # Errors
    /// Returns a [`DataTypeExtensionError`] if the `bytes` codec is not supported or `endianness` has not been specified.
    #[allow(unused_variables)]
    fn decode_bytes<'a>(
        &self,
        bytes: Cow<'a, [u8]>,
        endianness: Option<Endianness>,
    ) -> Result<Cow<'a, [u8]>, DataTypeExtensionError>;
}

/// A data type error.
#[derive(Debug, thiserror::Error, derive_more::From, derive_more::Display)]
pub enum DataTypeExtensionError {
    /// The endianness was not specified, and it is required for this data type extension.
    EndiannessNotSpecified,
    /// The `bytes` codec is not supported, likely because the data type has a variable length.
    BytesCodecUnsupported,
}
