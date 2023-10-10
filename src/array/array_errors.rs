use thiserror::Error;

use crate::{
    array_subset::{ArraySubset, IncompatibleDimensionalityError},
    metadata::UnsupportedAdditionalFieldError,
    node::NodePathError,
    plugin::PluginCreateError,
    storage::StorageError,
};

use super::{
    chunk_grid::{InvalidArrayIndicesError, InvalidChunkGridIndicesError},
    codec::CodecError,
    data_type::{IncompatibleFillValueErrorMetadataError, UnsupportedDataTypeError},
    ArrayIndices, ArrayShape,
};

/// An array creation error.
#[derive(Debug, Error)]
pub enum ArrayCreateError {
    /// Invalid zarr format.
    #[error("invalid zarr format {0}, expected 3")]
    InvalidZarrFormat(usize),
    /// Invalid node type.
    #[error("invalid zarr format {0}, expected array")]
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
    InvalidFillValue(#[from] IncompatibleFillValueErrorMetadataError),
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
    /// An error deserializing the metadata.
    #[error(transparent)]
    MetadataDeserializationError(#[from] serde_json::Error),
    /// Missing metadata.
    #[error("array metadata is missing")]
    MissingMetadata,
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
    /// Invalid array indices.
    #[error(transparent)]
    InvalidArrayIndicesError(#[from] InvalidArrayIndicesError),
    /// Invalid chunk grid indices.
    #[error(transparent)]
    InvalidChunkGridIndicesError(#[from] InvalidChunkGridIndicesError),
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
    /// Transmute error.
    #[error(transparent)]
    TransmuteError(#[from] TransmuteError),
}

/// A non typed version of [`safe_transmute::Error`].
#[derive(Debug, Error)]
pub enum TransmuteError {
    /// The data does not respect the target type’s boundaries.
    #[error(transparent)]
    Guard(safe_transmute::GuardError),
    /// The given data slice is not properly aligned for the target type.
    #[error("the given data slice is not properly aligned for the target type")]
    Unaligned,
    /// The data vector’s element type does not have the same size and minimum alignment as the target type.
    #[error("the data vector’s element type does not have the same size and minimum alignment as the target type")]
    IncompatibleVecTarget,
    /// The data contains an invalid value for the target type.
    #[error("invalid value")]
    InvalidValue,
}

impl<'a, S, T> From<safe_transmute::Error<'a, S, T>> for TransmuteError {
    fn from(error: safe_transmute::Error<'a, S, T>) -> Self {
        match error {
            safe_transmute::Error::Guard(guard) => TransmuteError::Guard(guard),
            safe_transmute::Error::Unaligned(_) => TransmuteError::Unaligned,
            safe_transmute::Error::IncompatibleVecTarget(_) => {
                TransmuteError::IncompatibleVecTarget
            }
            safe_transmute::Error::InvalidValue => TransmuteError::InvalidValue,
        }
    }
}
