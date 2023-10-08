//! Zarr arrays.
//!
//! An array is a node in a Zarr hierarchy used to hold multidimensional array data and associated metadata.
//! See <https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html#array>.
//!
//! Use [`ArrayBuilder`] to setup a new array, or use [`Array::new`] to read and/or write an existing array.
//!
//! An array is defined by the following parameters (which are encoded in its JSON metadata):
//!  - **shape**: defines the length of the array dimensions,
//!  - **data type**: defines the numerical representation array elements,
//!  - **chunk grid**: defines how the array is subdivided into chunks,
//!  - **chunk key encoding**: defines how chunk grid cell coordinates are mapped to keys in a store,
//!  - **fill value**: an element value to use for uninitialised portions of the array.
//!  - **codecs**: used to encode and decode chunks,
//!
//! and optional parameters:
//!  - **attributes**: user-defined attributes,
//!  - **storage transformers**: used to intercept and alter the storage keys and bytes of an array before they reach the underlying physical storage, and
//!  - **dimension names**: defines the names of the array dimensions.
//!
//! See <https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html#array-metadata> for more information on array metadata.
//!
//! The operations available for an array depend on the traits implemented by its backing [storage](crate::storage) (a store or storage transformer).
//! For example,
//!  - [`ReadableStorageTraits`] storage can read array data and metadata.
//!  - [`WritableStorageTraits`] storage can write array data and metadata, and
//!  - both traits are needed to update chunk subsets, such as with [`Array::store_array_subset`].

mod array_builder;
mod array_errors;
mod array_metadata;
mod array_representation;
mod bytes_representation;
pub mod chunk_grid;
pub mod chunk_key_encoding;
pub mod codec;
pub mod data_type;
mod dimension_name;
mod fill_value;
mod fill_value_metadata;

use std::sync::Arc;

pub use self::{
    array_builder::ArrayBuilder,
    array_errors::{ArrayCreateError, ArrayError},
    array_metadata::{ArrayMetadata, ArrayMetadataV3},
    array_representation::ArrayRepresentation,
    bytes_representation::BytesRepresentation,
    chunk_grid::ChunkGrid,
    chunk_key_encoding::ChunkKeyEncoding,
    codec::CodecChain,
    data_type::DataType,
    dimension_name::DimensionName,
    fill_value::FillValue,
    fill_value_metadata::FillValueMetadata,
};

use safe_transmute::TriviallyTransmutable;

use crate::{
    array_subset::{validate_array_subset, ArraySubset},
    metadata::AdditionalFields,
    node::NodePath,
    storage::{
        data_key, meta_key, storage_transformer::StorageTransformerChain, ReadableStorageTraits,
        StorageError, StorageHandle, WritableStorageTraits,
    },
};

use self::{
    array_errors::TransmuteError,
    chunk_grid::InvalidChunkGridIndicesError,
    codec::{ArrayCodecTraits, ArrayToBytesCodecTraits, StoragePartialDecoder},
};

/// An ND index to an element in an array.
pub type ArrayIndices = Vec<u64>;

/// The shape of an array.
pub type ArrayShape = Vec<u64>;

/// An alias for bytes which may or may not be available.
///
/// When a value is read from a store, it returns `MaybeBytes` which is [`None`] if the key is not available.
/// A bytes to bytes codec only decodes `MaybeBytes` holding actual bytes, otherwise the bytes are propagated to the next decoder.
/// An array to bytes partial decoder must take care of converting missing chunks to the fill value.
pub type MaybeBytes = Option<Vec<u8>>;

/// A Zarr array.
///
/// See <https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html#array-metadata>.
///
/// The `shape` and `attributes` of an array are mutable and can be updated after construction.
/// Array metadata must be written explicitly to the store with [`store_metadata`](Array<WritableStorageTraits>::store_metadata), which can be done before or after chunks are written.
#[derive(Debug)]
pub struct Array<TStorage: ?Sized> {
    /// The storage (including storage transformers).
    storage: Arc<TStorage>,
    /// The path of the array in a store.
    path: NodePath,
    /// An array of integers providing the length of each dimension of the Zarr array.
    shape: ArrayShape,
    /// The data type of the Zarr array.
    data_type: DataType,
    /// The chunk grid of the Zarr array.
    chunk_grid: ChunkGrid,
    /// The mapping from chunk grid cell coordinates to keys in the underlying store.
    chunk_key_encoding: ChunkKeyEncoding,
    /// Provides an element value to use for uninitialised portions of the Zarr array. It encodes the underlying data type.
    fill_value: FillValue,
    /// Specifies a list of codecs to be used for encoding and decoding chunks.
    codecs: CodecChain,
    /// Optional user defined attributes.
    attributes: serde_json::Map<String, serde_json::Value>,
    /// An optional list of storage transformers.
    storage_transformers: StorageTransformerChain,
    /// An optional list of dimension names.
    dimension_names: Option<Vec<DimensionName>>,
    /// Additional fields annotated with `"must_understand": false`.
    additional_fields: AdditionalFields,
    /// If true, codecs run with multithreading (where supported)
    parallel_codecs: bool,
}

impl<TStorage: ?Sized> Array<TStorage> {
    /// Create an array in `storage` at `path` with `metadata`.
    /// This does **not** write to the store, use [`store_metadata`](Array<WritableStorageTraits>::store_metadata) to write `metadata` to `storage`.
    ///
    /// # Errors
    ///
    /// Returns [`ArrayCreateError`] if:
    ///  - any metadata is invalid or,
    ///  - a plugin (e.g. data type/chunk grid/chunk key encoding/codec/storage transformer) is invalid.
    pub fn new_with_metadata(
        storage: Arc<TStorage>,
        path: &str,
        metadata: ArrayMetadata,
    ) -> Result<Array<TStorage>, ArrayCreateError> {
        let path = NodePath::new(path)?;

        let ArrayMetadata::V3(metadata) = metadata;
        if !metadata.validate_format() {
            return Err(ArrayCreateError::InvalidZarrFormat(metadata.zarr_format));
        }
        if !metadata.validate_node_type() {
            return Err(ArrayCreateError::InvalidNodeType(metadata.node_type));
        }
        metadata
            .additional_fields
            .validate()
            .map_err(ArrayCreateError::UnsupportedAdditionalFieldError)?;
        let data_type = DataType::from_metadata(&metadata.data_type)
            .map_err(ArrayCreateError::DataTypeCreateError)?;
        let chunk_grid = ChunkGrid::from_metadata(&metadata.chunk_grid)
            .map_err(ArrayCreateError::ChunkGridCreateError)?;
        if chunk_grid.dimensionality() != metadata.shape.len() {
            return Err(ArrayCreateError::InvalidChunkGridDimensionality(
                chunk_grid.dimensionality(),
                metadata.shape.len(),
            ));
        }
        let fill_value = data_type
            .fill_value_from_metadata(&metadata.fill_value)
            .map_err(ArrayCreateError::InvalidFillValue)?;
        let codecs = CodecChain::from_metadata(&metadata.codecs)
            .map_err(ArrayCreateError::CodecsCreateError)?;
        let storage_transformers =
            StorageTransformerChain::from_metadata(&metadata.storage_transformers)
                .map_err(ArrayCreateError::StorageTransformersCreateError)?;
        let chunk_key_encoding = ChunkKeyEncoding::from_metadata(&metadata.chunk_key_encoding)
            .map_err(ArrayCreateError::ChunkKeyEncodingCreateError)?;
        if let Some(dimension_names) = &metadata.dimension_names {
            if dimension_names.len() != metadata.shape.len() {
                return Err(ArrayCreateError::InvalidDimensionNames(
                    dimension_names.len(),
                    metadata.shape.len(),
                ));
            }
        }

        Ok(Self {
            storage,
            path,
            shape: metadata.shape,
            data_type,
            chunk_grid,
            chunk_key_encoding,
            fill_value,
            codecs,
            attributes: metadata.attributes,
            additional_fields: metadata.additional_fields,
            storage_transformers,
            dimension_names: metadata.dimension_names,
            parallel_codecs: true,
        })
    }

    /// Set the shape of the array.
    pub fn set_shape(&mut self, shape: ArrayShape) {
        self.shape = shape;
    }

    /// Mutably borrow the array attributes.
    #[must_use]
    pub fn attributes_mut(&mut self) -> &mut serde_json::Map<String, serde_json::Value> {
        &mut self.attributes
    }

    /// Get the node path.
    #[must_use]
    pub fn path(&self) -> &NodePath {
        &self.path
    }

    /// Get the data type.
    #[must_use]
    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    /// Get the fill value.
    #[must_use]
    pub fn fill_value(&self) -> &FillValue {
        &self.fill_value
    }

    /// Get the array shape.
    #[must_use]
    pub fn shape(&self) -> &[u64] {
        &self.shape
    }

    /// Get the codecs.
    #[must_use]
    pub fn codecs(&self) -> &CodecChain {
        &self.codecs
    }

    /// Get the chunk grid.
    #[must_use]
    pub fn chunk_grid(&self) -> &ChunkGrid {
        &self.chunk_grid
    }

    /// Get the chunk key encoding.
    #[must_use]
    pub fn chunk_key_encoding(&self) -> &ChunkKeyEncoding {
        &self.chunk_key_encoding
    }

    /// Get the storage transformers.
    #[must_use]
    pub fn storage_transformers(&self) -> &StorageTransformerChain {
        &self.storage_transformers
    }

    /// Get the dimension names.
    #[must_use]
    pub fn dimension_names(&self) -> &Option<Vec<DimensionName>> {
        &self.dimension_names
    }

    /// Get the attributes.
    #[must_use]
    pub fn attributes(&self) -> &serde_json::Map<String, serde_json::Value> {
        &self.attributes
    }

    /// Get the additional fields.
    #[must_use]
    pub fn additional_fields(&self) -> &AdditionalFields {
        &self.additional_fields
    }

    /// Returns true if codecs should use multiple threads for encoding and decoding where supported.
    #[must_use]
    pub fn parallel_codecs(&self) -> bool {
        self.parallel_codecs
    }

    /// Set whether or not to use multithreaded codec encoding/decoding.
    ///
    /// It may be advantageous to turn this off if parallelisation is external (e.g. parallel chunk decoding).
    pub fn set_parallel_codecs(&mut self, parallel_codecs: bool) {
        self.parallel_codecs = parallel_codecs;
    }

    /// Create [`ArrayMetadata`].
    #[must_use]
    pub fn metadata(&self) -> ArrayMetadata {
        ArrayMetadataV3::new(
            self.shape().to_vec(),
            self.data_type().metadata(),
            self.chunk_grid().create_metadata(),
            self.chunk_key_encoding().create_metadata(),
            self.data_type().metadata_fill_value(self.fill_value()),
            self.codecs().create_metadatas(),
            self.attributes().clone(),
            self.storage_transformers().create_metadatas(),
            self.dimension_names().clone(),
            self.additional_fields().clone(),
        )
        .into()
    }

    /// Get the chunk array representation at `chunk_index`.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidChunkGridIndicesError`] if the `chunk_indices` or `array_shape` are incompatible with the chunk grid.
    pub fn chunk_array_representation(
        &self,
        chunk_indices: &[u64],
        array_shape: &[u64],
    ) -> Result<ArrayRepresentation, InvalidChunkGridIndicesError> {
        Ok(unsafe {
            ArrayRepresentation::new_unchecked(
                self.chunk_grid().chunk_shape(chunk_indices, array_shape)?,
                self.data_type().clone(),
                self.fill_value().clone(),
            )
        })
    }

    /// Return an array subset indicating the chunks intersecting `array_subset`.
    ///
    /// # Errors
    ///
    /// Returns [`ArrayError`] if the array subset is out of bounds or has an incorrect dimensionality.
    pub fn chunks_in_array_subset(
        &self,
        array_subset: &ArraySubset,
    ) -> Result<ArraySubset, ArrayError> {
        // Find the chunks intersecting this array subset
        let err = || ArrayError::InvalidArraySubset(array_subset.clone(), self.shape().to_vec());
        let chunks_start = self
            .chunk_grid()
            .chunk_indices(array_subset.start(), self.shape())
            .map_err(|_| err())?;
        let chunks_end = self
            .chunk_grid()
            .chunk_indices(&array_subset.end_inc(), self.shape())
            .map_err(|_| err())?;
        Ok(unsafe { ArraySubset::new_with_start_end_inc_unchecked(chunks_start, &chunks_end) })
    }
}

impl<TStorage: ?Sized + ReadableStorageTraits> Array<TStorage> {
    /// Create an array in `storage` at `path`. The metadata is read from the store.
    ///
    /// # Errors
    ///
    /// Returns [`ArrayCreateError`] if there is a storage error or any metadata is invalid.
    pub fn new(storage: Arc<TStorage>, path: &str) -> Result<Self, ArrayCreateError> {
        let node_path = NodePath::new(path)?;
        let metadata: ArrayMetadata = serde_json::from_slice(
            &storage
                .get(&meta_key(&node_path))?
                .ok_or(ArrayCreateError::MissingMetadata)?,
        )?;
        Self::new_with_metadata(storage, path, metadata)
    }

    /// Read and decode the chunk at `chunk_indices` into its bytes.
    ///
    /// # Errors
    ///
    /// Returns an [`ArrayError`] if
    ///  - `chunk_indices` are invalid,
    ///  - there is a codec decoding error, or
    ///  - an underlying store error.
    ///
    /// # Panics
    ///
    /// Panics if the number of elements in the chunk exceeds `usize::MAX`.
    pub fn retrieve_chunk(&self, chunk_indices: &[u64]) -> Result<Vec<u8>, ArrayError> {
        let storage_handle = Arc::new(StorageHandle::new(&*self.storage));
        let storage_transformer = self
            .storage_transformers()
            .create_readable_transformer(storage_handle);
        let chunk_encoded = crate::storage::retrieve_chunk(
            &*storage_transformer,
            self.path(),
            chunk_indices,
            self.chunk_key_encoding(),
        )
        .map_err(ArrayError::StorageError);
        let chunk_representation = self.chunk_array_representation(chunk_indices, self.shape())?;
        match chunk_encoded {
            Ok(Some(chunk_encoded)) => {
                let chunk_decoded = if self.parallel_codecs() {
                    self.codecs()
                        .par_decode(chunk_encoded, &chunk_representation)
                } else {
                    self.codecs().decode(chunk_encoded, &chunk_representation)
                };
                chunk_decoded.map_err(ArrayError::CodecError)
            }
            Ok(None) => {
                let fill_value = chunk_representation.fill_value().as_ne_bytes();
                Ok(fill_value.repeat(chunk_representation.num_elements_usize()))
            }
            Err(error) => Err(error),
        }
    }

    /// Read and decode the chunk at `chunk_indices` into a vector of its elements.
    ///
    /// # Errors
    ///
    /// Returns an [`ArrayError`] if
    ///  - the size of `T` does not match the data type size,
    ///  - the decoded bytes cannot be transmuted,
    ///  - `chunk_indices` are invalid,
    ///  - there is a codec decoding error, or
    ///  - an underlying store error.
    pub fn retrieve_chunk_elements<T: TriviallyTransmutable>(
        &self,
        chunk_indices: &[u64],
    ) -> Result<Vec<T>, ArrayError> {
        if self.data_type.size() != std::mem::size_of::<T>() {
            return Err(ArrayError::IncompatibleElementSize(
                self.data_type.size(),
                std::mem::size_of::<T>(),
            ));
        }

        let bytes = self.retrieve_chunk(chunk_indices)?;
        let elements = safe_transmute::transmute_many_permissive::<T>(&bytes)
            .map_err(TransmuteError::from)?
            .to_vec();
        Ok(elements)
    }

    #[cfg(feature = "ndarray")]
    /// Read and decode the chunk at `chunk_indices` into an ndarray.
    ///
    /// # Errors
    ///
    /// Returns an [`ArrayError`] if:
    ///  - the size of `T` does not match the data type size,
    ///  - the decoded bytes cannot be transmuted,
    ///  - an array subsets is invalid,
    ///  - there is a codec decoding error, or
    ///  - an underlying store error.
    ///
    /// # Panics
    ///
    /// Will panic if a chunk dimension is larger than `usize::MAX`.
    pub fn retrieve_chunk_ndarray<T: safe_transmute::TriviallyTransmutable>(
        &self,
        chunk_indices: &[u64],
    ) -> Result<ndarray::ArrayD<T>, ArrayError> {
        if self.data_type.size() != std::mem::size_of::<T>() {
            return Err(ArrayError::IncompatibleElementSize(
                self.data_type.size(),
                std::mem::size_of::<T>(),
            ));
        }

        let shape = self.chunk_grid().chunk_shape(chunk_indices, self.shape())?;
        let elements = self.retrieve_chunk_elements(chunk_indices)?;
        let length = elements.len();
        ndarray::ArrayD::<T>::from_shape_vec(iter_u64_to_usize(shape.iter()), elements).map_err(
            |_| {
                ArrayError::CodecError(crate::array::codec::CodecError::UnexpectedChunkDecodedSize(
                    length * std::mem::size_of::<T>(),
                    shape.iter().product::<u64>() * std::mem::size_of::<T>() as u64,
                ))
            },
        )
    }

    /// Read and decode the `array_subset` of array into its bytes.
    ///
    /// # Errors
    ///
    /// Returns an [`ArrayError`] if:
    ///  - an array subset is invalid or out of bounds of the array,
    ///  - there is a codec decoding error, or
    ///  - an underlying store error.
    ///
    /// # Panics
    ///
    /// Panics if attempting to reference a byte beyond `usize::MAX`.
    pub fn retrieve_array_subset(&self, array_subset: &ArraySubset) -> Result<Vec<u8>, ArrayError> {
        if !validate_array_subset(array_subset, self.shape()) {
            return Err(ArrayError::InvalidArraySubset(
                array_subset.clone(),
                self.shape().to_vec(),
            ));
        }

        // Allocate the output data
        let element_size = self.fill_value().size() as u64;
        let size_output = usize::try_from(array_subset.num_elements() * element_size).unwrap();
        let mut output: Vec<u8> = vec![0; size_output];

        // Find the chunks intersecting this array subset
        let chunks = self.chunks_in_array_subset(array_subset)?;

        // Read those chunks
        for chunk_indices in chunks.iter_indices() {
            // Get the subset of the array corresponding to the chunk
            let chunk_subset_in_array =
                unsafe { self.chunk_grid().subset_unchecked(&chunk_indices) };

            // Decode the subset of the chunk which intersects array_subset
            let array_subset_in_chunk_subset =
                unsafe { array_subset.in_subset_unchecked(&chunk_subset_in_array) };
            let decoded_bytes =
                self.retrieve_chunk_subset(&chunk_indices, &array_subset_in_chunk_subset)?;

            // Copy decoded bytes to the output
            let chunk_subset_in_array_subset =
                unsafe { chunk_subset_in_array.in_subset_unchecked(array_subset) };
            let mut decoded_offset = 0;
            for (array_subset_element_index, num_elements) in unsafe {
                chunk_subset_in_array_subset
                    .iter_contiguous_linearised_indices_unchecked(array_subset.shape())
            } {
                let output_offset =
                    usize::try_from(array_subset_element_index * element_size).unwrap();
                let length = usize::try_from(num_elements * element_size).unwrap();
                debug_assert!((output_offset + length) <= output.len());
                debug_assert!((decoded_offset + length) <= decoded_bytes.len());
                output[output_offset..output_offset + length]
                    .copy_from_slice(&decoded_bytes[decoded_offset..decoded_offset + length]);
                decoded_offset += length;
            }
        }

        Ok(output)
    }

    /// Read and decode the `array_subset` of array into a vector of its elements.
    ///
    /// # Errors
    ///
    /// Returns an [`ArrayError`] if:
    ///  - the size of `T` does not match the data type size,
    ///  - the decoded bytes cannot be transmuted,
    ///  - an array subset is invalid or out of bounds of the array,
    ///  - there is a codec decoding error, or
    ///  - an underlying store error.
    pub fn retrieve_array_subset_elements<T: TriviallyTransmutable>(
        &self,
        array_subset: &ArraySubset,
    ) -> Result<Vec<T>, ArrayError> {
        if self.data_type.size() != std::mem::size_of::<T>() {
            return Err(ArrayError::IncompatibleElementSize(
                self.data_type.size(),
                std::mem::size_of::<T>(),
            ));
        }

        let bytes = self.retrieve_array_subset(array_subset)?;
        let elements = safe_transmute::transmute_many_permissive::<T>(&bytes)
            .map_err(TransmuteError::from)?
            .to_vec();
        Ok(elements)
    }

    #[cfg(feature = "ndarray")]
    /// Read and decode the `array_subset` of array into an ndarray.
    ///
    /// # Errors
    ///
    /// Returns an [`ArrayError`] if:
    ///  - an array subset is invalid or out of bounds of the array,
    ///  - there is a codec decoding error, or
    ///  - an underlying store error.
    ///
    /// # Panics
    ///
    /// Will panic if any dimension in `chunk_subset` is `usize::MAX` or larger.
    pub fn retrieve_array_subset_ndarray<T: safe_transmute::TriviallyTransmutable>(
        &self,
        array_subset: &ArraySubset,
    ) -> Result<ndarray::ArrayD<T>, ArrayError> {
        if self.data_type.size() != std::mem::size_of::<T>() {
            return Err(ArrayError::IncompatibleElementSize(
                self.data_type.size(),
                std::mem::size_of::<T>(),
            ));
        }

        let elements = self.retrieve_array_subset_elements(array_subset)?;
        let length = elements.len();
        ndarray::ArrayD::<T>::from_shape_vec(
            iter_u64_to_usize(array_subset.shape().iter()),
            elements,
        )
        .map_err(|_| {
            ArrayError::CodecError(crate::array::codec::CodecError::UnexpectedChunkDecodedSize(
                length * self.data_type().size(),
                array_subset.num_elements() * self.data_type().size() as u64,
            ))
        })
    }

    /// Read and decode the `chunk_subset` of the chunk at `chunk_indices` into its bytes.
    ///
    /// # Errors
    ///
    /// Returns an [`ArrayError`] if:
    ///  - the chunk indices are invalid,
    ///  - the chunk subset is invalid,
    ///  - there is a codec decoding error, or
    ///  - an underlying store error.
    ///
    /// # Panics
    ///
    /// Will panic if the number of elements in `chunk_subset` is `usize::MAX` or larger.
    pub fn retrieve_chunk_subset(
        &self,
        chunk_indices: &[u64],
        chunk_subset: &ArraySubset,
    ) -> Result<Vec<u8>, ArrayError> {
        let chunk_representation = self.chunk_array_representation(chunk_indices, self.shape())?;
        if !validate_array_subset(chunk_subset, chunk_representation.shape()) {
            return Err(ArrayError::InvalidArraySubset(
                chunk_subset.clone(),
                self.shape().to_vec(),
            ));
        }

        let storage_handle = Arc::new(StorageHandle::new(&*self.storage));
        let storage_transformer = self
            .storage_transformers()
            .create_readable_transformer(storage_handle);
        let input_handle = Box::new(StoragePartialDecoder::new(
            &*storage_transformer,
            data_key(self.path(), chunk_indices, self.chunk_key_encoding()),
        ));

        let partial_decoder = self.codecs().partial_decoder(input_handle);
        let decoded_bytes = if self.parallel_codecs() {
            partial_decoder.par_partial_decode(&chunk_representation, &[chunk_subset.clone()])
        } else {
            partial_decoder.partial_decode(&chunk_representation, &[chunk_subset.clone()])
        }?;
        Ok(decoded_bytes.concat())
    }

    /// Read and decode the `chunk_subset` of the chunk at `chunk_indices` into its elements.
    ///
    /// # Errors
    ///
    /// Returns an [`ArrayError`] if:
    ///  - the chunk indices are invalid,
    ///  - the chunk subset is invalid,
    ///  - there is a codec decoding error, or
    ///  - an underlying store error.
    pub fn retrieve_chunk_subset_elements<T: TriviallyTransmutable>(
        &self,
        chunk_indices: &[u64],
        chunk_subset: &ArraySubset,
    ) -> Result<Vec<T>, ArrayError> {
        if self.data_type.size() != std::mem::size_of::<T>() {
            return Err(ArrayError::IncompatibleElementSize(
                self.data_type.size(),
                std::mem::size_of::<T>(),
            ));
        }

        let bytes = self.retrieve_chunk_subset(chunk_indices, chunk_subset)?;
        let elements = safe_transmute::transmute_many_permissive::<T>(&bytes)
            .map_err(TransmuteError::from)?
            .to_vec();
        Ok(elements)
    }

    #[cfg(feature = "ndarray")]
    /// Read and decode the `chunk_subset` of the chunk at `chunk_indices` into an ndarray.
    ///
    /// # Errors
    ///
    /// Returns an [`ArrayError`] if:
    ///  - the chunk indices are invalid,
    ///  - the chunk subset is invalid,
    ///  - there is a codec decoding error, or
    ///  - an underlying store error.
    ///
    /// # Panics
    ///
    /// Will panic if the number of elements in `chunk_subset` is `usize::MAX` or larger.
    pub fn retrieve_chunk_subset_ndarray<T: TriviallyTransmutable>(
        &self,
        chunk_indices: &[u64],
        chunk_subset: &ArraySubset,
    ) -> Result<ndarray::ArrayD<T>, ArrayError> {
        let elements = self.retrieve_chunk_subset_elements(chunk_indices, chunk_subset)?;
        let length = elements.len();
        ndarray::ArrayD::from_shape_vec(iter_u64_to_usize(chunk_subset.shape().iter()), elements)
            .map_err(|_| {
                ArrayError::CodecError(crate::array::codec::CodecError::UnexpectedChunkDecodedSize(
                    length * std::mem::size_of::<T>(),
                    chunk_subset.shape().iter().product::<u64>() * std::mem::size_of::<T>() as u64,
                ))
            })
    }
}

impl<TStorage: ?Sized + WritableStorageTraits> Array<TStorage> {
    /// Store metadata.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] if there is an underlying store error.
    pub fn store_metadata(&self) -> Result<(), StorageError> {
        let storage_handle = Arc::new(StorageHandle::new(&*self.storage));
        let storage_transformer = self
            .storage_transformers()
            .create_writable_transformer(storage_handle);
        crate::storage::create_array(&*storage_transformer, self.path(), &self.metadata())
    }

    /// Encode `chunk_bytes` and store at `chunk_indices`.
    ///
    /// A chunk composed entirely of the fill value will not be written to the store.
    ///
    /// # Errors
    ///
    /// Returns an [`ArrayError`] if
    ///  - `chunk_indices` are invalid,
    ///  - the length of `chunk_bytes` is not equal to the expected length (the product of the number of elements in the chunk and the data type size in bytes),
    ///  - there is a codec encoding error, or
    ///  - an underlying store error.
    pub fn store_chunk(&self, chunk_indices: &[u64], chunk_bytes: &[u8]) -> Result<(), ArrayError> {
        // Validation
        let chunk_array_representation =
            self.chunk_array_representation(chunk_indices, self.shape())?;
        if chunk_bytes.len() as u64 != chunk_array_representation.size() {
            return Err(ArrayError::InvalidBytesInputSize(
                chunk_bytes.len(),
                chunk_array_representation.size(),
            ));
        }

        let fill_value = self.fill_value().as_ne_bytes();
        let any_non_fill_value = chunk_bytes
            .chunks_exact(fill_value.len())
            .any(|f| f != fill_value);
        if any_non_fill_value {
            let storage_handle = Arc::new(StorageHandle::new(&*self.storage));
            let storage_transformer = self
                .storage_transformers()
                .create_writable_transformer(storage_handle);
            let chunk_encoded: Vec<u8> = if self.parallel_codecs() {
                self.codecs()
                    .par_encode(chunk_bytes.to_vec(), &chunk_array_representation)
            } else {
                self.codecs()
                    .encode(chunk_bytes.to_vec(), &chunk_array_representation)
            }
            .map_err(ArrayError::CodecError)?;
            crate::storage::store_chunk(
                &*storage_transformer,
                self.path(),
                chunk_indices,
                self.chunk_key_encoding(),
                &chunk_encoded,
            )
            .map_err(ArrayError::StorageError)
        } else {
            Ok(())
        }
    }

    /// Encode `chunk_elements` and store at `chunk_indices`.
    ///
    /// A chunk composed entirely of the fill value will not be written to the store.
    ///
    /// # Errors
    ///
    /// Returns an [`ArrayError`] if
    ///  - the size of  `T` does not match the data type size, or
    ///  - a [`store_chunk`](Array::store_chunk) error condition is met.
    pub fn store_chunk_elements<T: TriviallyTransmutable>(
        &self,
        chunk_indices: &[u64],
        chunk_elements: &[T],
    ) -> Result<(), ArrayError> {
        if self.data_type.size() != std::mem::size_of::<T>() {
            return Err(ArrayError::IncompatibleElementSize(
                self.data_type.size(),
                std::mem::size_of::<T>(),
            ));
        }

        let chunk_bytes = safe_transmute::transmute_to_bytes(chunk_elements);
        self.store_chunk(chunk_indices, chunk_bytes)
    }

    #[cfg(feature = "ndarray")]
    /// Encode `chunk_array` and store at `chunk_indices`.
    ///
    /// # Errors
    ///
    /// Returns an [`ArrayError`] if a [`store_chunk_elements`](Array::store_chunk_elements) error condition is met.
    pub fn store_chunk_ndarray<T: safe_transmute::TriviallyTransmutable>(
        &self,
        chunk_indices: &[u64],
        chunk_array: &ndarray::ArrayViewD<T>,
    ) -> Result<(), ArrayError> {
        if self.data_type.size() != std::mem::size_of::<T>() {
            return Err(ArrayError::IncompatibleElementSize(
                self.data_type.size(),
                std::mem::size_of::<T>(),
            ));
        }
        let shape = chunk_array.shape().iter().map(|u| *u as u64).collect();
        let chunk_shape = self.chunk_grid().chunk_shape(chunk_indices, self.shape())?;
        if shape != chunk_shape {
            return Err(ArrayError::UnexpectedChunkDecodedShape(shape, chunk_shape));
        }

        let chunk_bytes = chunk_array.as_standard_layout();
        if let Some(slice) = chunk_bytes.as_slice() {
            self.store_chunk_elements(chunk_indices, slice)
        } else {
            unreachable!()
        }
    }

    /// Erase the chunk at `chunk_indices`.
    ///
    /// Returns true if the chunk was erased, or false if it did not exist.
    ///
    /// # Errors
    ///
    /// Returns a [`StorageError`] if there is an underlying store error.
    pub fn erase_chunk(&self, chunk_indices: &[u64]) -> Result<bool, StorageError> {
        let storage_handle = Arc::new(StorageHandle::new(&*self.storage));
        let storage_transformer = self
            .storage_transformers()
            .create_writable_transformer(storage_handle);
        crate::storage::erase_chunk(
            &*storage_transformer,
            self.path(),
            chunk_indices,
            self.chunk_key_encoding(),
        )
    }
}

impl<TStorage: ?Sized + ReadableStorageTraits + WritableStorageTraits> Array<TStorage> {
    /// Encode `subset_bytes` and store in `array_subset`.
    ///
    /// Prefer to use [`store_chunk`](Array<WritableStorageTraits>::store_chunk) since this will decode and encode each chunk intersecting `array_subset`.
    ///
    /// # Errors
    ///
    /// Returns an [`ArrayError`] if
    ///  - `array_subset` is invalid or out of bounds of the array,
    ///  - there is a codec encoding error, or
    ///  - an underlying store error.
    pub fn store_array_subset(
        &self,
        array_subset: &ArraySubset,
        subset_bytes: &[u8],
    ) -> Result<(), ArrayError> {
        // Validation
        if !validate_array_subset(array_subset, self.shape()) {
            return Err(ArrayError::InvalidArraySubset(
                array_subset.clone(),
                self.shape().to_vec(),
            ));
        }
        let expected_size = array_subset.num_elements() * self.data_type().size() as u64;
        if subset_bytes.len() as u64 != expected_size {
            return Err(ArrayError::InvalidBytesInputSize(
                subset_bytes.len(),
                expected_size,
            ));
        }

        // Find the chunks intersecting this array subset
        let chunks = self.chunks_in_array_subset(array_subset)?;

        let element_size = self.data_type().size();
        for chunk_indices in chunks.iter_indices() {
            let chunk_subset_in_array =
                unsafe { self.chunk_grid().subset_unchecked(&chunk_indices) };

            if array_subset == &chunk_subset_in_array {
                // A fast path if the array subset matches the chunk subset
                // This skips the internal decoding occurring in store_chunk_subset
                self.store_chunk(&chunk_indices, subset_bytes)?;
            } else {
                let chunk_subset_in_array_subset =
                    unsafe { chunk_subset_in_array.in_subset_unchecked(array_subset) };
                let chunk_subset_bytes = unsafe {
                    chunk_subset_in_array_subset.extract_bytes_unchecked(
                        subset_bytes,
                        array_subset.shape(),
                        element_size,
                    )
                };

                // Store the chunk subset
                let array_subset_in_chunk_subset =
                    unsafe { array_subset.in_subset_unchecked(&chunk_subset_in_array) };

                self.store_chunk_subset(
                    &chunk_indices,
                    &array_subset_in_chunk_subset,
                    &chunk_subset_bytes,
                )?;
            }
        }
        Ok(())
    }

    /// Encode `subset_elements` and store in `array_subset`.
    ///
    /// Prefer to use [`store_chunk`](Array<WritableStorageTraits>::store_chunk) since this will decode and encode each chunk intersecting `array_subset`.
    ///
    /// # Errors
    ///
    /// Returns an [`ArrayError`] if
    ///  - the size of  `T` does not match the data type size, or
    ///  - a [`store_array_subset`](Array::store_array_subset) error condition is met.
    pub fn store_array_subset_elements<T: TriviallyTransmutable>(
        &self,
        array_subset: &ArraySubset,
        subset_elements: &[T],
    ) -> Result<(), ArrayError> {
        if self.data_type.size() != std::mem::size_of::<T>() {
            return Err(ArrayError::IncompatibleElementSize(
                self.data_type.size(),
                std::mem::size_of::<T>(),
            ));
        }

        let subset_bytes = safe_transmute::transmute_to_bytes(subset_elements);
        self.store_array_subset(array_subset, subset_bytes)
    }

    #[cfg(feature = "ndarray")]
    /// Encode `subset_array` and store in the array subset starting at `subset_start`.
    ///
    /// # Errors
    ///
    /// Returns an [`ArrayError`] if a [`store_array_subset_elements`](Array::store_array_subset_elements) error condition is met.
    #[allow(clippy::missing_panics_doc)]
    pub fn store_array_subset_ndarray<T: safe_transmute::TriviallyTransmutable>(
        &self,
        subset_start: &[u64],
        subset_array: &ndarray::ArrayViewD<T>,
    ) -> Result<(), ArrayError> {
        if subset_start.len() != self.chunk_grid().dimensionality() {
            return Err(crate::array_subset::IncompatibleDimensionalityError::new(
                subset_start.len(),
                self.chunk_grid().dimensionality(),
            )
            .into());
        } else if subset_array.shape().len() != self.chunk_grid().dimensionality() {
            return Err(crate::array_subset::IncompatibleDimensionalityError::new(
                subset_array.shape().len(),
                self.chunk_grid().dimensionality(),
            )
            .into());
        }

        let subset = unsafe {
            ArraySubset::new_with_start_shape_unchecked(
                subset_start.to_vec(),
                subset_array.shape().iter().map(|u| *u as u64).collect(),
            )
        };
        let array_standard = subset_array.as_standard_layout();
        let elements = array_standard.as_slice().expect("always valid");
        self.store_array_subset_elements(&subset, elements)
    }

    /// Encode `chunk_subset_bytes` and store in `chunk_subset` of the chunk at `chunk_indices`.
    ///
    /// Prefer to use [`store_chunk`](Array<WritableStorageTraits>::store_chunk) since this will decode the chunk before updating it and reencoding it.
    ///
    /// # Errors
    ///
    /// Returns an [`ArrayError`] if
    ///  - `chunk_subset` is invalid or out of bounds of the chunk,
    ///  - there is a codec encoding error, or
    ///  - an underlying store error.
    ///
    /// # Panics
    ///
    /// Panics if attempting to reference a byte beyond `usize::MAX`.
    pub fn store_chunk_subset(
        &self,
        chunk_indices: &[u64],
        chunk_subset: &ArraySubset,
        chunk_subset_bytes: &[u8],
    ) -> Result<(), ArrayError> {
        // Validation
        let chunk_shape = self.chunk_grid().chunk_shape(chunk_indices, self.shape())?;
        if std::iter::zip(chunk_subset.end_exc(), &chunk_shape)
            .any(|(end_exc, shape)| end_exc > *shape)
        {
            return Err(ArrayError::InvalidChunkSubset(
                chunk_subset.clone(),
                chunk_indices.to_vec(),
                chunk_shape,
            ));
        }
        let expected_length =
            chunk_subset.shape().iter().product::<u64>() * self.data_type().size() as u64;
        if chunk_subset_bytes.len() as u64 != expected_length {
            return Err(ArrayError::InvalidBytesInputSize(
                chunk_subset_bytes.len(),
                expected_length,
            ));
        }

        // Decode the entire chunk
        let mut chunk_bytes = self.retrieve_chunk(chunk_indices)?;

        // Update the intersecting subset of the chunk
        let element_size = self.data_type().size() as u64;
        let mut offset = 0;
        for (chunk_element_index, num_elements) in
            unsafe { chunk_subset.iter_contiguous_linearised_indices_unchecked(&chunk_shape) }
        {
            let chunk_offset = usize::try_from(chunk_element_index * element_size).unwrap();
            let length = usize::try_from(num_elements * element_size).unwrap();
            debug_assert!(chunk_offset + length <= chunk_bytes.len());
            debug_assert!(offset + length <= chunk_subset_bytes.len());
            chunk_bytes[chunk_offset..chunk_offset + length]
                .copy_from_slice(&chunk_subset_bytes[offset..offset + length]);
            offset += length;
        }

        // Store the updated chunk
        self.store_chunk(chunk_indices, &chunk_bytes)
    }

    /// Encode `chunk_subset_elements` and store in `chunk_subset` of the chunk at `chunk_indices`.
    ///
    /// Prefer to use [`store_chunk`](Array<WritableStorageTraits>::store_chunk) since this will decode the chunk before updating it and reencoding it.
    ///
    /// # Errors
    ///
    /// Returns an [`ArrayError`] if
    ///  - the size of  `T` does not match the data type size, or
    ///  - a [`store_chunk_subset`](Array::store_chunk_subset) error condition is met.
    pub fn store_chunk_subset_elements<T: TriviallyTransmutable>(
        &self,
        chunk_indices: &[u64],
        chunk_subset: &ArraySubset,
        chunk_subset_elements: &[T],
    ) -> Result<(), ArrayError> {
        if self.data_type.size() != std::mem::size_of::<T>() {
            return Err(ArrayError::IncompatibleElementSize(
                self.data_type.size(),
                std::mem::size_of::<T>(),
            ));
        }

        let chunk_subset_bytes = safe_transmute::transmute_to_bytes(chunk_subset_elements);
        self.store_chunk_subset(chunk_indices, chunk_subset, chunk_subset_bytes)
    }

    #[cfg(feature = "ndarray")]
    /// Encode `chunk_subset_array` and store in `chunk_subset` of the chunk in the subset starting at `chunk_subset_start`.
    ///
    /// Prefer to use [`store_chunk`](Array<WritableStorageTraits>::store_chunk) since this will decode the chunk before updating it and reencoding it.
    ///
    /// # Errors
    ///
    /// Returns an [`ArrayError`] if a [`store_chunk_subset_elements`](Array::store_chunk_subset_elements) error condition is met.
    #[allow(clippy::missing_panics_doc)]
    pub fn store_chunk_subset_ndarray<T: TriviallyTransmutable>(
        &self,
        chunk_indices: &[u64],
        chunk_subset_start: &[u64],
        chunk_subset_array: &ndarray::ArrayViewD<T>,
    ) -> Result<(), ArrayError> {
        if chunk_subset_start.len() != self.chunk_grid().dimensionality() {
            return Err(crate::array_subset::IncompatibleDimensionalityError::new(
                chunk_subset_start.len(),
                self.chunk_grid().dimensionality(),
            )
            .into());
        } else if chunk_subset_array.shape().len() != self.chunk_grid().dimensionality() {
            return Err(crate::array_subset::IncompatibleDimensionalityError::new(
                chunk_subset_array.shape().len(),
                self.chunk_grid().dimensionality(),
            )
            .into());
        }

        let subset = unsafe {
            ArraySubset::new_with_start_shape_unchecked(
                chunk_subset_start.to_vec(),
                chunk_subset_array
                    .shape()
                    .iter()
                    .map(|u| *u as u64)
                    .collect(),
            )
        };
        let array_standard = chunk_subset_array.as_standard_layout();
        let elements = array_standard.as_slice().expect("always valid");
        self.store_chunk_subset_elements(chunk_indices, &subset, elements)
    }
}

/// Unravel a linearised index to ND indices.
#[doc(hidden)]
#[must_use]
pub fn unravel_index(mut index: u64, shape: &[u64]) -> ArrayIndices {
    let mut indices = Vec::with_capacity(shape.len());
    for dim in shape.iter().rev() {
        indices.push(index % dim);
        index /= dim;
    }
    indices.reverse();
    indices
}

/// Ravel ND indices to a linearised index.
#[doc(hidden)]
#[must_use]
pub fn ravel_indices(indices: &[u64], shape: &[u64]) -> u64 {
    let mut index: u64 = 0;
    let mut count = 1;
    for (i, s) in std::iter::zip(indices, shape).rev() {
        index += i * count;
        count *= s;
    }
    index
}

#[cfg(feature = "ndarray")]
fn iter_u64_to_usize<'a, I: Iterator<Item = &'a u64>>(iter: I) -> Vec<usize> {
    iter.map(|v| usize::try_from(*v).unwrap())
        .collect::<Vec<_>>()
}

#[cfg(test)]
mod tests {
    use crate::storage::store::MemoryStore;

    use super::*;

    #[test]
    fn test_array_metadata_write_read() {
        let store = Arc::new(MemoryStore::new());

        let array_path = "/array";
        let array = ArrayBuilder::new(
            vec![8, 8],
            DataType::UInt8,
            vec![4, 4].into(),
            FillValue::from(0u8),
        )
        .build(store.clone(), array_path)
        .unwrap();
        array.store_metadata().unwrap();

        // let metadata: ArrayMetadata =
        //     serde_json::from_slice(&store.get(&meta_key(&array_path))?)?;
        // println!("{:?}", metadata);

        let metadata = Array::new(store, array_path).unwrap().metadata();
        assert_eq!(metadata, array.metadata());
    }

    #[test]
    fn array_set_shape_and_attributes() {
        let store = MemoryStore::new();
        let array_path = "/group/array";
        let mut array = ArrayBuilder::new(
            vec![8, 8], // array shape
            DataType::Float32,
            vec![4, 4].into(),
            FillValue::from(f32::NAN),
        )
        .bytes_to_bytes_codecs(vec![
            #[cfg(feature = "gzip")]
            Box::new(codec::GzipCodec::new(5).unwrap()),
        ])
        .build(store.into(), array_path)
        .unwrap();

        array.set_shape(vec![16, 16]);
        array
            .attributes_mut()
            .insert("test".to_string(), "apple".into());

        assert_eq!(array.shape(), &[16, 16]);
        assert_eq!(
            array.attributes().get_key_value("test"),
            Some((
                &"test".to_string(),
                &serde_json::Value::String("apple".to_string())
            ))
        );
    }

    #[test]
    fn array_subset_round_trip() {
        let store = Arc::new(MemoryStore::default());
        let array_path = "/array";
        let array = ArrayBuilder::new(
            vec![8, 8], // array shape
            DataType::Float32,
            vec![4, 4].into(), // regular chunk shape
            FillValue::from(1f32),
        )
        .bytes_to_bytes_codecs(vec![
            #[cfg(feature = "gzip")]
            Box::new(codec::GzipCodec::new(5).unwrap()),
        ])
        .storage_transformers(vec![])
        .build(store.clone(), array_path)
        .unwrap();

        array
            .store_array_subset_elements::<f32>(
                &ArraySubset::new_with_start_shape(vec![3, 3], vec![3, 3]).unwrap(),
                &vec![0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9],
            )
            .unwrap();

        let subset_all =
            ArraySubset::new_with_start_shape(vec![0, 0], array.shape().to_vec()).unwrap();
        let data_all = array
            .retrieve_array_subset_elements::<f32>(&subset_all)
            .unwrap();
        assert_eq!(
            data_all,
            vec![
                //     (0,0)       |     (0, 1)
                //0  1    2    3   |4    5    6    7
                1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, // 0
                1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, // 1
                1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, // 2
                1.0, 1.0, 1.0, 0.1, 0.2, 0.3, 1.0, 1.0, //_3____________
                1.0, 1.0, 1.0, 0.4, 0.5, 0.6, 1.0, 1.0, // 4
                1.0, 1.0, 1.0, 0.7, 0.8, 0.9, 1.0, 1.0, // 5 (1, 1)
                1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, // 6
                1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, // 7
            ]
        );
    }
}
