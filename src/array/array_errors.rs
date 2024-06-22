use thiserror::Error;

use crate::{
    array_subset::{ArraySubset, IncompatibleDimensionalityError},
    metadata::v3::UnsupportedAdditionalFieldError,
    node::NodePathError,
    plugin::PluginCreateError,
    storage::StorageError,
};

use super::{
    codec::CodecError,
    data_type::{
        IncompatibleFillValueError, IncompatibleFillValueMetadataError, UnsupportedDataTypeError,
    },
    ArrayIndices, ArrayShape,
};

/// An array creation error.
#[derive(Debug, Error)]
pub enum ArrayCreateError {
    /// Invalid Zarr format.
    #[error("invalid Zarr format {0}, expected 3")]
    InvalidZarrFormat(usize),
    /// Invalid node type.
    #[error("invalid Zarr format {0}, expected array")]
    InvalidNodeType(String),
    /// An invalid node path
    #[error(transparent)]
    NodePathError(#[from] NodePathError),
    /// Unsupported additional field.
    #[error(transparent)]
    UnsupportedAdditionalFieldError(#[from] UnsupportedAdditionalFieldError),
    /// Unsupported data type.
    #[error(transparent)]
    DataTypeCreateError(UnsupportedDataTypeError),
    /// Invalid fill value.
    #[error(transparent)]
    InvalidFillValue(#[from] IncompatibleFillValueError),
    /// Invalid fill value metadata.
    #[error(transparent)]
    InvalidFillValueMetadata(#[from] IncompatibleFillValueMetadataError),
    /// Error creating codecs.
    #[error(transparent)]
    CodecsCreateError(PluginCreateError),
    /// Storage transformer creation error.
    #[error(transparent)]
    StorageTransformersCreateError(PluginCreateError),
    /// Chunk grid create error.
    #[error(transparent)]
    ChunkGridCreateError(PluginCreateError),
    /// Chunk key encoding create error.
    #[error(transparent)]
    ChunkKeyEncodingCreateError(PluginCreateError),
    /// The dimensionality of the chunk grid does not match the array shape.
    #[error("chunk grid dimensionality {0} does not match array dimensionality {1}")]
    InvalidChunkGridDimensionality(usize, usize),
    /// The number of dimension names does not match the array dimensionality.
    #[error("the number of dimension names {0} does not match array dimensionality {1}")]
    InvalidDimensionNames(usize, usize),
    /// Storage error.
    #[error(transparent)]
    StorageError(#[from] StorageError),
    /// Missing metadata.
    #[error("array metadata is missing")]
    MissingMetadata,
    /// The Zarr V2 array is unsupported.
    #[error("unsupported Zarr V2 array: {_0}")]
    UnsupportedZarrV2Array(String),
}

/// Array errors.
#[derive(Debug, Error)]
pub enum ArrayError {
    /// A store error.
    #[error(transparent)]
    StorageError(#[from] StorageError),
    /// A codec error.
    #[error(transparent)]
    CodecError(#[from] CodecError),
    // /// Invalid array indices.
    // #[error(transparent)]
    // InvalidArrayIndicesError(#[from] InvalidArrayIndicesError),
    /// Invalid chunk grid indices.
    #[error("invalid chunk grid indices: {_0:?}")]
    InvalidChunkGridIndicesError(Vec<u64>),
    /// Incompatible dimensionality.
    #[error(transparent)]
    IncompatibleDimensionalityError(#[from] IncompatibleDimensionalityError),
    /// Incompatible array subset.
    #[error("array subset {_0} is not compatible with array shape {_1:?}")]
    InvalidArraySubset(ArraySubset, ArrayShape),
    /// Incompatible chunk subset.
    #[error("chunk subset {_0} is not compatible with chunk {_1:?} with shape {_2:?}")]
    InvalidChunkSubset(ArraySubset, ArrayIndices, ArrayShape),
    /// An unexpected chunk decoded size.
    #[error("got chunk decoded size {_0:?}, expected {_1:?}")]
    UnexpectedChunkDecodedSize(usize, usize),
    /// An unexpected bytes input size.
    #[error("got bytes with size {_0:?}, expected {_1:?}")]
    InvalidBytesInputSize(usize, u64),
    /// An unexpected chunk decoded shape.
    #[error("got chunk decoded shape {_0:?}, expected {_1:?}")]
    UnexpectedChunkDecodedShape(ArrayShape, ArrayShape),
    /// Incompatible element size.
    #[error("got element size {_0}, expected {_1}")]
    IncompatibleElementSize(usize, usize),
    /// Invalid data shape.
    #[error("data has shape {_0:?}, expected {_1:?}")]
    InvalidDataShape(Vec<usize>, Vec<usize>),
}
