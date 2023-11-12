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
//!
//! By default, the `zarrs` version and a link to its source code is written to the `_zarrs` attribute in array metadata.
//! This functionality can be disabled with [`set_include_zarrs_metadata(false)`](Array::set_include_zarrs_metadata).

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
mod unsafe_cell_slice;

use std::{collections::HashMap, sync::Arc};

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

use parking_lot::Mutex;
use rayon::prelude::{IntoParallelIterator, ParallelIterator};
use safe_transmute::TriviallyTransmutable;
use serde::Serialize;

use crate::{
    array_subset::{ArraySubset, IncompatibleDimensionalityError},
    metadata::AdditionalFields,
    node::NodePath,
    storage::{
        data_key, meta_key, storage_transformer::StorageTransformerChain, ReadableStorageTraits,
        StorageError, StorageHandle, WritableStorageTraits,
    },
};

use self::{
    array_errors::TransmuteError,
    codec::{
        ArrayCodecTraits, ArrayPartialDecoderTraits, ArrayToBytesCodecTraits, StoragePartialDecoder,
    },
    unsafe_cell_slice::UnsafeCellSlice,
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
///
/// # Concurrency
///
/// ### A Single Array Instance (Internal Synchronisation)
/// Arrays support parallel reading and writing, with the level of concurrency dependent on the underlying store.
/// A store must guarantee the following for operations which retrieve or store data from a chunk (or any store value):
///  - a store *may* support multiple concurrent readers of a chunk, and
///  - a store *must* permit only one writer of a chunk at any one time (with no concurrent readers).
///
/// A chunk is locked for writing within the [`Array::store_chunk_subset`] function (which may be called from any of the `store_array_subset` and `store_chunk_subset` variants).
/// This is necessary because this function may internally decode a chunk and later update it. The chunk must not be updated by another thread until this operation has completed.
///
/// For optimum write performance, an array should be written chunk-by-chunk.
/// If a chunk is written more than once, its element values depend on whichever operation wrote to the chunk last.
///
/// Array elements can alternatively be written using the `store_array_subset`/`store_chunk_subset` methods, although this is less efficient.
/// Provided that the array subsets are non-overlapping, all elements are guaranteed to hold the values written.
/// If any array subsets overlap (which ideally should be avoided!), the value of an element in the overlap region depends on whichever operation wrote to its associated chunk last.
/// Consider the case of parallel writing of the following subsets to a 1x6 array and a 1x3 chunk size (**do not do this, it is just an example**):
/// ```text
///    |subset0| < stores element values of 0
/// [ A B C | D E F ] < fill value of 9
///      |subset1| < stores element values of 1
///      |ss2| < stores element values of 2
/// ```
/// Depending on the order in which the chunks were updated within each subset, the array elements could take on the following values:
/// ```text
/// [ A B C | D E F ]
///   9 0 0   0 1 9
///       1   1
///       2
/// ```
///
/// ### Multiple Array Instances (External Synchronisation)
/// The internal synchronisation guarantees provided by an [`Array`] and its underlying store are not applicable if there are multiple array instances referencing the same data.
/// An [`Array`] should only be created once and shared between multiple threads where possible.
///
/// This is not applicable if an array is being written from multiple independent **processes** (e.g. a distributed program on a cluster).
/// For example, without *internal synchronisation* it is possible that elements `B` and `E`  in the above example could end up with the fill value, despite them explicitly being set to `0` and `1` respectively.
///
/// A distributed program with multiple [`Array`] instances should do chunk-by-chunk writing.
/// If a chunk needs to be updated, then external synchronisation is necessary to ensure that only one process updates it at any one time.
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
    /// Chunk locks.
    chunk_locks: Mutex<HashMap<Vec<u64>, Arc<Mutex<()>>>>,
    /// Zarrs metadata.
    include_zarrs_metadata: bool,
}

impl<TStorage: ?Sized> Array<TStorage> {
    /// Create an array in `storage` at `path` with `metadata`.
    /// This does **not** write to the store, use [`store_metadata`](Array<WritableStorageTraits>::store_metadata) to write `metadata` to `storage`.
    ///
    /// # Errors
    /// Returns [`ArrayCreateError`] if:
    ///  - any metadata is invalid or,
    ///  - a plugin (e.g. data type/chunk grid/chunk key encoding/codec/storage transformer) is invalid.
    pub fn new_with_metadata(
        storage: Arc<TStorage>,
        path: &str,
        metadata: ArrayMetadata,
    ) -> Result<Self, ArrayCreateError> {
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
            .map_err(ArrayCreateError::InvalidFillValueMetadata)?;
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
            chunk_locks: Mutex::default(),
            include_zarrs_metadata: true,
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
    pub const fn path(&self) -> &NodePath {
        &self.path
    }

    /// Get the data type.
    #[must_use]
    pub const fn data_type(&self) -> &DataType {
        &self.data_type
    }

    /// Get the fill value.
    #[must_use]
    pub const fn fill_value(&self) -> &FillValue {
        &self.fill_value
    }

    /// Get the array shape.
    #[must_use]
    pub fn shape(&self) -> &[u64] {
        &self.shape
    }

    /// Get the codecs.
    #[must_use]
    pub const fn codecs(&self) -> &CodecChain {
        &self.codecs
    }

    /// Get the chunk grid.
    #[must_use]
    pub const fn chunk_grid(&self) -> &ChunkGrid {
        &self.chunk_grid
    }

    /// Get the chunk key encoding.
    #[must_use]
    pub const fn chunk_key_encoding(&self) -> &ChunkKeyEncoding {
        &self.chunk_key_encoding
    }

    /// Get the storage transformers.
    #[must_use]
    pub const fn storage_transformers(&self) -> &StorageTransformerChain {
        &self.storage_transformers
    }

    /// Get the dimension names.
    #[must_use]
    pub const fn dimension_names(&self) -> &Option<Vec<DimensionName>> {
        &self.dimension_names
    }

    /// Get the attributes.
    #[must_use]
    pub const fn attributes(&self) -> &serde_json::Map<String, serde_json::Value> {
        &self.attributes
    }

    /// Get the additional fields.
    #[must_use]
    pub const fn additional_fields(&self) -> &AdditionalFields {
        &self.additional_fields
    }

    /// Returns true if codecs can use multiple threads for encoding and decoding (where supported).
    #[must_use]
    pub const fn parallel_codecs(&self) -> bool {
        self.parallel_codecs
    }

    /// Enable or disable multithreaded codec encoding/decoding. Enabled by default.
    ///
    /// It may be advantageous to turn this off if parallelisation is external to avoid thrashing.
    pub fn set_parallel_codecs(&mut self, parallel_codecs: bool) {
        self.parallel_codecs = parallel_codecs;
    }

    /// Enable or disable the inclusion of zarrs metadata in the array attributes. Enabled by default.
    ///
    /// Zarrs metadata includes the zarrs version and some parameters.
    pub fn set_include_zarrs_metadata(&mut self, include_zarrs_metadata: bool) {
        self.include_zarrs_metadata = include_zarrs_metadata;
    }

    /// Create [`ArrayMetadata`].
    #[must_use]
    pub fn metadata(&self) -> ArrayMetadata {
        let attributes = if self.include_zarrs_metadata {
            #[derive(Serialize)]
            struct ZarrsMetadata {
                description: String,
                repository: String,
                version: String,
            }
            let zarrs_metadata = ZarrsMetadata {
                description: "This array was created with zarrs".to_string(),
                repository: env!("CARGO_PKG_REPOSITORY").to_string(),
                version: env!("CARGO_PKG_VERSION").to_string(),
            };
            let mut attributes = self.attributes().clone();
            attributes.insert("_zarrs".to_string(), unsafe {
                serde_json::to_value(zarrs_metadata).unwrap_unchecked()
            });
            attributes
        } else {
            self.attributes().clone()
        };

        ArrayMetadataV3::new(
            self.shape().to_vec(),
            self.data_type().metadata(),
            self.chunk_grid().create_metadata(),
            self.chunk_key_encoding().create_metadata(),
            self.data_type().metadata_fill_value(self.fill_value()),
            self.codecs().create_metadatas(),
            attributes,
            self.storage_transformers().create_metadatas(),
            self.dimension_names().clone(),
            self.additional_fields().clone(),
        )
        .into()
    }

    /// Create an array builder matching the parameters of this array
    pub fn builder(&self) -> ArrayBuilder {
        ArrayBuilder::from_array(self)
    }

    /// Return the shape of the chunk grid (i.e., the number of chunks).
    pub fn chunk_grid_shape(&self) -> Option<Vec<u64>> {
        unsafe { self.chunk_grid().grid_shape_unchecked(self.shape()) }
    }

    /// Return the shape of the chunk at `chunk_indices`.
    ///
    /// # Errors
    /// Returns [`ArrayError::InvalidChunkGridIndicesError`] if the `chunk_indices` are incompatible with the chunk grid.
    pub fn chunk_shape(&self, chunk_indices: &[u64]) -> Result<Vec<u64>, ArrayError> {
        self.chunk_grid()
            .chunk_shape(chunk_indices, self.shape())
            .map_err(|_| ArrayError::InvalidChunkGridIndicesError(chunk_indices.to_vec()))?
            .ok_or_else(|| ArrayError::InvalidChunkGridIndicesError(chunk_indices.to_vec()))
    }

    /// Return the array subset of the chunk at `chunk_indices`.
    ///
    /// # Errors
    /// Returns [`ArrayError::InvalidChunkGridIndicesError`] if the `chunk_indices` are incompatible with the chunk grid.
    pub fn chunk_subset(&self, chunk_indices: &[u64]) -> Result<ArraySubset, ArrayError> {
        self.chunk_grid()
            .subset(chunk_indices, self.shape())
            .map_err(|_| ArrayError::InvalidChunkGridIndicesError(chunk_indices.to_vec()))?
            .ok_or_else(|| ArrayError::InvalidChunkGridIndicesError(chunk_indices.to_vec()))
    }

    /// Return the array subset of the chunk at `chunk_indices` bounded by the array shape.
    ///
    /// # Errors
    /// Returns [`ArrayError::InvalidChunkGridIndicesError`] if the `chunk_indices` are incompatible with the chunk grid.
    pub fn chunk_subset_bounded(&self, chunk_indices: &[u64]) -> Result<ArraySubset, ArrayError> {
        let chunk_subset = self.chunk_subset(chunk_indices)?;
        Ok(unsafe { chunk_subset.bound_unchecked(self.shape()) })
    }

    /// Get the chunk array representation at `chunk_index`.
    ///
    /// # Errors
    /// Returns [`ArrayError::InvalidChunkGridIndicesError`] if the `chunk_indices` are incompatible with the chunk grid.
    pub fn chunk_array_representation(
        &self,
        chunk_indices: &[u64],
    ) -> Result<ArrayRepresentation, ArrayError> {
        (self.chunk_grid().chunk_shape(chunk_indices, self.shape())?).map_or_else(
            || {
                Err(ArrayError::InvalidChunkGridIndicesError(
                    chunk_indices.to_vec(),
                ))
            },
            |chunk_shape| {
                Ok(unsafe {
                    ArrayRepresentation::new_unchecked(
                        chunk_shape,
                        self.data_type().clone(),
                        self.fill_value().clone(),
                    )
                })
            },
        )
    }

    /// Return an array subset indicating the chunks intersecting `array_subset`.
    ///
    /// Returns [`None`] if the intersecting chunks cannot be determined.
    ///
    /// # Errors
    /// Returns [`IncompatibleDimensionalityError`] if the array subset has an incorrect dimensionality.
    pub fn chunks_in_array_subset(
        &self,
        array_subset: &ArraySubset,
    ) -> Result<Option<ArraySubset>, IncompatibleDimensionalityError> {
        // Find the chunks intersecting this array subset
        let chunks_start = self
            .chunk_grid()
            .chunk_indices(array_subset.start(), self.shape())?;
        let chunks_end = self
            .chunk_grid()
            .chunk_indices(&array_subset.end_inc(), self.shape())?;
        let chunks_end = chunks_end.map_or_else(|| self.chunk_grid_shape(), Some);

        Ok(
            if let (Some(chunks_start), Some(chunks_end)) = (chunks_start, chunks_end) {
                Some(unsafe {
                    ArraySubset::new_with_start_end_inc_unchecked(chunks_start, chunks_end)
                })
            } else {
                None
            },
        )
    }

    #[must_use]
    fn chunk_mutex(&self, chunk_indices: &[u64]) -> Arc<Mutex<()>> {
        let mut chunk_locks = self.chunk_locks.lock();
        // TODO: Cleanup old locks
        let chunk_mutex = chunk_locks
            .entry(chunk_indices.to_vec())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();
        drop(chunk_locks);
        chunk_mutex
    }
}

impl<TStorage: ?Sized + ReadableStorageTraits> Array<TStorage> {
    /// Create an array in `storage` at `path`. The metadata is read from the store.
    ///
    /// # Errors
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
    /// Returns an [`ArrayError`] if
    ///  - `chunk_indices` are invalid,
    ///  - there is a codec decoding error, or
    ///  - an underlying store error.
    ///
    /// # Panics
    /// Panics if the number of elements in the chunk exceeds `usize::MAX`.
    pub fn retrieve_chunk(&self, chunk_indices: &[u64]) -> Result<Box<[u8]>, ArrayError> {
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
        .map_err(ArrayError::StorageError)?;
        let chunk_representation = self.chunk_array_representation(chunk_indices)?;
        if let Some(chunk_encoded) = chunk_encoded {
            let chunk_decoded = self
                .codecs()
                .decode_opt(chunk_encoded, &chunk_representation, self.parallel_codecs())
                .map_err(ArrayError::CodecError)?;
            let chunk_decoded_size =
                chunk_representation.num_elements_usize() * chunk_representation.data_type().size();
            if chunk_decoded.len() == chunk_decoded_size {
                Ok(chunk_decoded.into_boxed_slice())
            } else {
                Err(ArrayError::UnexpectedChunkDecodedSize(
                    chunk_decoded.len(),
                    chunk_decoded_size,
                ))
            }
        } else {
            let fill_value = chunk_representation.fill_value().as_ne_bytes();
            Ok(fill_value
                .repeat(chunk_representation.num_elements_usize())
                .into_boxed_slice())
        }
    }

    /// Read and decode the chunk at `chunk_indices` into a vector of its elements.
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if
    ///  - the size of `T` does not match the data type size,
    ///  - the decoded bytes cannot be transmuted,
    ///  - `chunk_indices` are invalid,
    ///  - there is a codec decoding error, or
    ///  - an underlying store error.
    pub fn retrieve_chunk_elements<T: TriviallyTransmutable>(
        &self,
        chunk_indices: &[u64],
    ) -> Result<Box<[T]>, ArrayError> {
        if self.data_type.size() != std::mem::size_of::<T>() {
            return Err(ArrayError::IncompatibleElementSize(
                self.data_type.size(),
                std::mem::size_of::<T>(),
            ));
        }

        let bytes = self.retrieve_chunk(chunk_indices)?;
        if safe_transmute::align::check_alignment::<_, T>(&bytes).is_ok() {
            // no-copy path
            let mut bytes = core::mem::ManuallyDrop::new(bytes);
            Ok(unsafe {
                Vec::from_raw_parts(
                    bytes.as_mut_ptr().cast::<T>(),
                    bytes.len() / core::mem::size_of::<T>(),
                    bytes.len(),
                )
            }
            .into_boxed_slice())
        } else {
            let elements = safe_transmute::transmute_many_permissive::<T>(&bytes)
                .map_err(TransmuteError::from)?
                .to_vec()
                .into_boxed_slice();
            Ok(elements)
        }
    }

    #[cfg(feature = "ndarray")]
    /// Read and decode the chunk at `chunk_indices` into an ndarray.
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if:
    ///  - the size of `T` does not match the data type size,
    ///  - the decoded bytes cannot be transmuted,
    ///  - the chunk indices are invalid,
    ///  - there is a codec decoding error, or
    ///  - an underlying store error.
    ///
    /// # Panics
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
        if let Some(shape) = shape {
            let elements = self.retrieve_chunk_elements(chunk_indices)?;
            let length = elements.len();
            ndarray::ArrayD::<T>::from_shape_vec(
                iter_u64_to_usize(shape.iter()),
                elements.into_vec(),
            )
            .map_err(|_| {
                ArrayError::CodecError(crate::array::codec::CodecError::UnexpectedChunkDecodedSize(
                    length * std::mem::size_of::<T>(),
                    shape.iter().product::<u64>() * std::mem::size_of::<T>() as u64,
                ))
            })
        } else {
            Err(ArrayError::InvalidChunkGridIndicesError(
                chunk_indices.to_vec(),
            ))
        }
    }

    fn _decode_chunk_into_array_subset(
        &self,
        chunk_indices: &[u64],
        array_subset: &ArraySubset,
        output: &mut [u8],
    ) -> Result<(), ArrayError> {
        // Get the subset of the array corresponding to the chunk
        let chunk_subset_in_array = unsafe {
            self.chunk_grid()
                .subset_unchecked(chunk_indices, self.shape())
        };
        let Some(chunk_subset_in_array) = chunk_subset_in_array else {
            return Err(ArrayError::InvalidArraySubset(
                array_subset.clone(),
                self.shape().to_vec(),
            ));
        };

        // Decode the subset of the chunk which intersects array_subset
        let array_subset_in_chunk_subset =
            unsafe { array_subset.in_subset_unchecked(&chunk_subset_in_array) };
        let decoded_bytes =
            self.retrieve_chunk_subset(chunk_indices, &array_subset_in_chunk_subset)?;

        // Copy decoded bytes to the output
        let element_size = self.data_type().size() as u64;
        let chunk_subset_in_array_subset =
            unsafe { chunk_subset_in_array.in_subset_unchecked(array_subset) };
        let mut decoded_offset = 0;
        for (array_subset_element_index, num_elements) in unsafe {
            chunk_subset_in_array_subset
                .iter_contiguous_linearised_indices_unchecked(array_subset.shape())
        } {
            let output_offset = usize::try_from(array_subset_element_index * element_size).unwrap();
            let length = usize::try_from(num_elements * element_size).unwrap();
            debug_assert!((output_offset + length) <= output.len());
            debug_assert!((decoded_offset + length) <= decoded_bytes.len());
            output[output_offset..output_offset + length]
                .copy_from_slice(&decoded_bytes[decoded_offset..decoded_offset + length]);
            decoded_offset += length;
        }
        Ok(())
    }

    fn _retrieve_array_subset(
        &self,
        array_subset: &ArraySubset,
        parallel: bool,
    ) -> Result<Box<[u8]>, ArrayError> {
        if array_subset.dimensionality() != self.chunk_grid().dimensionality() {
            return Err(ArrayError::InvalidArraySubset(
                array_subset.clone(),
                self.shape().to_vec(),
            ));
        }

        // Find the chunks intersecting this array subset
        let chunks = self.chunks_in_array_subset(array_subset)?;
        let Some(chunks) = chunks else {
            return Err(ArrayError::InvalidArraySubset(
                array_subset.clone(),
                self.shape().to_vec(),
            ));
        };

        // Retrieve chunk bytes
        let num_chunks = chunks.num_elements();
        match num_chunks {
            0 => Ok(vec![].into_boxed_slice()),
            1 => {
                let chunk_indices = chunks.start();
                let chunk_subset = self.chunk_subset(chunk_indices).unwrap();
                if &chunk_subset == array_subset {
                    // Single chunk fast path if the array subset domain matches the chunk domain
                    self.retrieve_chunk(chunk_indices)
                } else {
                    let size_output = usize::try_from(
                        array_subset.num_elements() * self.data_type().size() as u64,
                    )
                    .unwrap();
                    let mut output = vec![core::mem::MaybeUninit::<u8>::uninit(); size_output];
                    let output_slice = unsafe {
                        std::slice::from_raw_parts_mut(
                            output.as_mut_ptr().cast::<u8>(),
                            size_output,
                        )
                    };
                    self._decode_chunk_into_array_subset(
                        chunk_indices,
                        array_subset,
                        output_slice,
                    )?;
                    #[allow(clippy::transmute_undefined_repr)]
                    let output: Vec<u8> = unsafe { core::mem::transmute(output) };
                    Ok(output.into_boxed_slice())
                }
            }
            _ => {
                // Decode chunks and copy to output
                let size_output =
                    usize::try_from(array_subset.num_elements() * self.data_type().size() as u64)
                        .unwrap();

                // let mut output = vec![0; size_output];
                // let output_slice = output.as_mut_slice();
                let mut output = vec![core::mem::MaybeUninit::<u8>::uninit(); size_output];
                let output_slice = unsafe {
                    std::slice::from_raw_parts_mut(output.as_mut_ptr().cast::<u8>(), size_output)
                };
                if parallel {
                    let output = UnsafeCellSlice::new(output_slice);
                    (0..chunks.shape().iter().product())
                        .into_par_iter()
                        .map(|chunk_index| {
                            std::iter::zip(
                                unravel_index(chunk_index, chunks.shape()),
                                chunks.start(),
                            )
                            .map(|(chunk_indices, chunks_start)| chunk_indices + chunks_start)
                            .collect::<Vec<_>>()
                        })
                        // chunks
                        // .iter_indices()
                        // .par_bridge()
                        .try_for_each(|chunk_indices| {
                            self._decode_chunk_into_array_subset(
                                &chunk_indices,
                                array_subset,
                                unsafe { output.get() },
                            )
                        })?;
                } else {
                    for chunk_indices in chunks.iter_indices() {
                        self._decode_chunk_into_array_subset(
                            &chunk_indices,
                            array_subset,
                            output_slice,
                        )?;
                    }
                }
                #[allow(clippy::transmute_undefined_repr)]
                let output: Vec<u8> = unsafe { core::mem::transmute(output) };
                Ok(output.into_boxed_slice())
            }
        }
    }

    /// Read and decode the `array_subset` of array into its bytes.
    ///
    /// Out-of-bounds elements will have the fill value.
    /// If `parallel` is true, chunks intersecting the array subset are retrieved in parallel.
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if:
    ///  - the `array_subset` dimensionality does not match the chunk grid dimensionality,
    ///  - there is a codec decoding error, or
    ///  - an underlying store error.
    ///
    /// # Panics
    /// Panics if attempting to reference a byte beyond `usize::MAX`.
    pub fn retrieve_array_subset(
        &self,
        array_subset: &ArraySubset,
    ) -> Result<Box<[u8]>, ArrayError> {
        self._retrieve_array_subset(array_subset, false)
    }

    /// Parallel version of [`Array::retrieve_array_subset`].
    #[allow(clippy::missing_panics_doc, clippy::missing_errors_doc)]
    pub fn par_retrieve_array_subset(
        &self,
        array_subset: &ArraySubset,
    ) -> Result<Box<[u8]>, ArrayError> {
        self._retrieve_array_subset(array_subset, true)
    }

    fn _retrieve_array_subset_elements<T: TriviallyTransmutable>(
        &self,
        array_subset: &ArraySubset,
        parallel: bool,
    ) -> Result<Box<[T]>, ArrayError> {
        if self.data_type.size() != std::mem::size_of::<T>() {
            return Err(ArrayError::IncompatibleElementSize(
                self.data_type.size(),
                std::mem::size_of::<T>(),
            ));
        }

        let bytes = self._retrieve_array_subset(array_subset, parallel)?;
        if safe_transmute::align::check_alignment::<_, T>(&bytes).is_ok() {
            // no-copy path
            let mut bytes = core::mem::ManuallyDrop::new(bytes);
            Ok(unsafe {
                Vec::from_raw_parts(
                    bytes.as_mut_ptr().cast::<T>(),
                    bytes.len() / core::mem::size_of::<T>(),
                    bytes.len(),
                )
            }
            .into_boxed_slice())
        } else {
            let elements = safe_transmute::transmute_many_permissive::<T>(&bytes)
                .map_err(TransmuteError::from)?
                .to_vec()
                .into_boxed_slice();
            Ok(elements)
        }
    }

    /// Read and decode the `array_subset` of array into a vector of its elements.
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if:
    ///  - the size of `T` does not match the data type size,
    ///  - the decoded bytes cannot be transmuted,
    ///  - an array subset is invalid or out of bounds of the array,
    ///  - there is a codec decoding error, or
    ///  - an underlying store error.
    pub fn retrieve_array_subset_elements<T: TriviallyTransmutable>(
        &self,
        array_subset: &ArraySubset,
    ) -> Result<Box<[T]>, ArrayError> {
        self._retrieve_array_subset_elements(array_subset, false)
    }

    /// Parallel version of [`Array::retrieve_array_subset_elements`].
    #[allow(clippy::missing_panics_doc, clippy::missing_errors_doc)]
    pub fn par_retrieve_array_subset_elements<T: TriviallyTransmutable>(
        &self,
        array_subset: &ArraySubset,
    ) -> Result<Box<[T]>, ArrayError> {
        self._retrieve_array_subset_elements(array_subset, true)
    }

    #[cfg(feature = "ndarray")]
    fn _retrieve_array_subset_ndarray<T: safe_transmute::TriviallyTransmutable>(
        &self,
        array_subset: &ArraySubset,
        parallel: bool,
    ) -> Result<ndarray::ArrayD<T>, ArrayError> {
        if self.data_type.size() != std::mem::size_of::<T>() {
            return Err(ArrayError::IncompatibleElementSize(
                self.data_type.size(),
                std::mem::size_of::<T>(),
            ));
        }

        let elements = self._retrieve_array_subset_elements(array_subset, parallel)?;
        let length = elements.len();
        ndarray::ArrayD::<T>::from_shape_vec(
            iter_u64_to_usize(array_subset.shape().iter()),
            elements.to_vec(),
        )
        .map_err(|_| {
            ArrayError::CodecError(crate::array::codec::CodecError::UnexpectedChunkDecodedSize(
                length * self.data_type().size(),
                array_subset.num_elements() * self.data_type().size() as u64,
            ))
        })
    }

    #[cfg(feature = "ndarray")]
    /// Read and decode the `array_subset` of array into an ndarray.
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if:
    ///  - an array subset is invalid or out of bounds of the array,
    ///  - there is a codec decoding error, or
    ///  - an underlying store error.
    ///
    /// # Panics
    /// Will panic if any dimension in `chunk_subset` is `usize::MAX` or larger.
    pub fn retrieve_array_subset_ndarray<T: safe_transmute::TriviallyTransmutable>(
        &self,
        array_subset: &ArraySubset,
    ) -> Result<ndarray::ArrayD<T>, ArrayError> {
        self._retrieve_array_subset_ndarray(array_subset, false)
    }

    #[cfg(feature = "ndarray")]
    /// Parallel version of [`Array::retrieve_array_subset_ndarray`].
    #[allow(clippy::missing_panics_doc, clippy::missing_errors_doc)]
    pub fn par_retrieve_array_subset_ndarray<T: safe_transmute::TriviallyTransmutable>(
        &self,
        array_subset: &ArraySubset,
    ) -> Result<ndarray::ArrayD<T>, ArrayError> {
        self._retrieve_array_subset_ndarray(array_subset, true)
    }

    /// Read and decode the `chunk_subset` of the chunk at `chunk_indices` into its bytes.
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if:
    ///  - the chunk indices are invalid,
    ///  - the chunk subset is invalid,
    ///  - there is a codec decoding error, or
    ///  - an underlying store error.
    ///
    /// # Panics
    /// Will panic if the number of elements in `chunk_subset` is `usize::MAX` or larger.
    pub fn retrieve_chunk_subset(
        &self,
        chunk_indices: &[u64],
        chunk_subset: &ArraySubset,
    ) -> Result<Box<[u8]>, ArrayError> {
        let chunk_representation = self.chunk_array_representation(chunk_indices)?;
        if !chunk_subset.inbounds(chunk_representation.shape()) {
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
            storage_transformer,
            data_key(self.path(), chunk_indices, self.chunk_key_encoding()),
        ));

        let decoded_bytes = self
            .codecs()
            .partial_decoder_opt(input_handle, &chunk_representation, self.parallel_codecs())?
            .partial_decode_opt(&[chunk_subset.clone()], self.parallel_codecs())?;

        let total_size = decoded_bytes.iter().map(Vec::len).sum::<usize>();
        let expected_size = chunk_subset.num_elements_usize() * self.data_type().size();
        if total_size == chunk_subset.num_elements_usize() * self.data_type().size() {
            Ok(decoded_bytes.concat().into_boxed_slice())
        } else {
            Err(ArrayError::UnexpectedChunkDecodedSize(
                total_size,
                expected_size,
            ))
        }
    }

    /// Read and decode the `chunk_subset` of the chunk at `chunk_indices` into its elements.
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if:
    ///  - the chunk indices are invalid,
    ///  - the chunk subset is invalid,
    ///  - there is a codec decoding error, or
    ///  - an underlying store error.
    pub fn retrieve_chunk_subset_elements<T: TriviallyTransmutable>(
        &self,
        chunk_indices: &[u64],
        chunk_subset: &ArraySubset,
    ) -> Result<Box<[T]>, ArrayError> {
        if self.data_type.size() != std::mem::size_of::<T>() {
            return Err(ArrayError::IncompatibleElementSize(
                self.data_type.size(),
                std::mem::size_of::<T>(),
            ));
        }

        let bytes = self.retrieve_chunk_subset(chunk_indices, chunk_subset)?;
        if safe_transmute::align::check_alignment::<_, T>(&bytes).is_ok() {
            // no-copy path
            let mut bytes = core::mem::ManuallyDrop::new(bytes);
            Ok(unsafe {
                Vec::from_raw_parts(
                    bytes.as_mut_ptr().cast::<T>(),
                    bytes.len() / core::mem::size_of::<T>(),
                    bytes.len(),
                )
            }
            .into_boxed_slice())
        } else {
            let elements = safe_transmute::transmute_many_permissive::<T>(&bytes)
                .map_err(TransmuteError::from)?
                .to_vec()
                .into_boxed_slice();
            Ok(elements)
        }
    }

    #[cfg(feature = "ndarray")]
    /// Read and decode the `chunk_subset` of the chunk at `chunk_indices` into an ndarray.
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if:
    ///  - the chunk indices are invalid,
    ///  - the chunk subset is invalid,
    ///  - there is a codec decoding error, or
    ///  - an underlying store error.
    ///
    /// # Panics
    /// Will panic if the number of elements in `chunk_subset` is `usize::MAX` or larger.
    pub fn retrieve_chunk_subset_ndarray<T: TriviallyTransmutable>(
        &self,
        chunk_indices: &[u64],
        chunk_subset: &ArraySubset,
    ) -> Result<ndarray::ArrayD<T>, ArrayError> {
        let elements = self.retrieve_chunk_subset_elements(chunk_indices, chunk_subset)?;
        let length = elements.len();
        ndarray::ArrayD::from_shape_vec(
            iter_u64_to_usize(chunk_subset.shape().iter()),
            elements.into_vec(),
        )
        .map_err(|_| {
            ArrayError::CodecError(crate::array::codec::CodecError::UnexpectedChunkDecodedSize(
                length * std::mem::size_of::<T>(),
                chunk_subset.shape().iter().product::<u64>() * std::mem::size_of::<T>() as u64,
            ))
        })
    }

    /// Initialises a partial decoder for the chunk at `chunk_indices` with optional parallelism.
    #[doc(hidden)]
    pub fn partial_decoder_opt<'a>(
        &'a self,
        chunk_indices: &[u64],
        parallel: bool,
    ) -> Result<Box<dyn ArrayPartialDecoderTraits + 'a>, ArrayError> {
        let storage_handle = Arc::new(StorageHandle::new(&*self.storage));
        let storage_transformer = self
            .storage_transformers()
            .create_readable_transformer(storage_handle);
        let input_handle = Box::new(StoragePartialDecoder::new(
            storage_transformer,
            data_key(self.path(), chunk_indices, self.chunk_key_encoding()),
        ));
        let chunk_representation = self.chunk_array_representation(chunk_indices)?;
        Ok(self
            .codecs()
            .partial_decoder_opt(input_handle, &chunk_representation, parallel)?)
    }

    /// Initialises a partial decoder for the chunk at `chunk_indices`.
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if initialisation of the partial decoder fails.
    pub fn partial_decoder<'a>(
        &'a self,
        chunk_indices: &[u64],
    ) -> Result<Box<dyn ArrayPartialDecoderTraits + 'a>, ArrayError> {
        self.partial_decoder_opt(chunk_indices, false)
    }

    /// Initialises a partial decoder for the chunk at `chunk_indices` using multithreading if applicable.
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if initialisation of the partial decoder fails.
    pub fn par_partial_decoder<'a>(
        &'a self,
        chunk_indices: &[u64],
    ) -> Result<Box<dyn ArrayPartialDecoderTraits + 'a>, ArrayError> {
        self.partial_decoder_opt(chunk_indices, true)
    }
}

impl<TStorage: ?Sized + WritableStorageTraits> Array<TStorage> {
    /// Store metadata.
    ///
    /// # Errors
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
    /// Returns an [`ArrayError`] if
    ///  - `chunk_indices` are invalid,
    ///  - the length of `chunk_bytes` is not equal to the expected length (the product of the number of elements in the chunk and the data type size in bytes),
    ///  - there is a codec encoding error, or
    ///  - an underlying store error.
    pub fn store_chunk(
        &self,
        chunk_indices: &[u64],
        chunk_bytes: Vec<u8>,
    ) -> Result<(), ArrayError> {
        // Validation
        let chunk_array_representation = self.chunk_array_representation(chunk_indices)?;
        if chunk_bytes.len() as u64 != chunk_array_representation.size() {
            return Err(ArrayError::InvalidBytesInputSize(
                chunk_bytes.len(),
                chunk_array_representation.size(),
            ));
        }

        let all_fill_value = self.fill_value().equals_all(&chunk_bytes);
        if all_fill_value {
            self.erase_chunk(chunk_indices)?;
            Ok(())
        } else {
            let storage_handle = Arc::new(StorageHandle::new(&*self.storage));
            let storage_transformer = self
                .storage_transformers()
                .create_writable_transformer(storage_handle);
            let chunk_encoded: Vec<u8> = if self.parallel_codecs() {
                self.codecs()
                    .par_encode(chunk_bytes, &chunk_array_representation)
            } else {
                self.codecs()
                    .encode(chunk_bytes, &chunk_array_representation)
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
        }
    }

    /// Encode `chunk_elements` and store at `chunk_indices`.
    ///
    /// A chunk composed entirely of the fill value will not be written to the store.
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if
    ///  - the size of  `T` does not match the data type size, or
    ///  - a [`store_chunk`](Array::store_chunk) error condition is met.
    pub fn store_chunk_elements<T: TriviallyTransmutable>(
        &self,
        chunk_indices: &[u64],
        chunk_elements: Vec<T>,
    ) -> Result<(), ArrayError> {
        if self.data_type.size() != std::mem::size_of::<T>() {
            return Err(ArrayError::IncompatibleElementSize(
                self.data_type.size(),
                std::mem::size_of::<T>(),
            ));
        }

        let chunk_bytes = safe_transmute_to_bytes_vec(chunk_elements);
        self.store_chunk(chunk_indices, chunk_bytes)
    }

    #[cfg(feature = "ndarray")]
    /// Encode `chunk_array` and store at `chunk_indices`.
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if
    ///  - the size of `T` does not match the size of the data type,
    ///  - a [`store_chunk_elements`](Array::store_chunk_elements) error condition is met.
    #[allow(clippy::missing_panics_doc)]
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
        if let Some(chunk_shape) = self.chunk_grid().chunk_shape(chunk_indices, self.shape())? {
            if shape != chunk_shape {
                return Err(ArrayError::UnexpectedChunkDecodedShape(shape, chunk_shape));
            }

            let chunk_array = chunk_array.as_standard_layout();
            let chunk_elements = chunk_array.into_owned().into_raw_vec();
            self.store_chunk_elements(chunk_indices, chunk_elements)
        } else {
            Err(ArrayError::InvalidChunkGridIndicesError(
                chunk_indices.to_vec(),
            ))
        }
    }

    /// Erase the chunk at `chunk_indices`.
    ///
    /// Returns true if the chunk was erased, or false if it did not exist.
    ///
    /// # Errors
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
    fn _store_array_subset(
        &self,
        array_subset: &ArraySubset,
        subset_bytes: Vec<u8>,
        parallel: bool,
    ) -> Result<(), ArrayError> {
        // Validation
        if array_subset.dimensionality() != self.shape().len() {
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
        let Some(chunks) = chunks else {
            return Err(ArrayError::InvalidArraySubset(
                array_subset.clone(),
                self.shape().to_vec(),
            ));
        };
        let num_chunks = chunks.num_elements_usize();
        let element_size = self.data_type().size();
        if num_chunks == 1 {
            let chunk_indices = chunks.start();
            let chunk_subset_in_array = unsafe {
                self.chunk_grid()
                    .subset_unchecked(chunk_indices, self.shape())
                    .unwrap()
            };
            if array_subset == &chunk_subset_in_array {
                // A fast path if the array subset matches the chunk subset
                // This skips the internal decoding occurring in store_chunk_subset
                self.store_chunk(chunk_indices, subset_bytes)?;
            } else {
                let chunk_subset_in_array_subset =
                    unsafe { chunk_subset_in_array.in_subset_unchecked(array_subset) };
                let chunk_subset_bytes = unsafe {
                    chunk_subset_in_array_subset.extract_bytes_unchecked(
                        &subset_bytes,
                        array_subset.shape(),
                        element_size,
                    )
                };

                // Store the chunk subset
                let array_subset_in_chunk_subset =
                    unsafe { array_subset.in_subset_unchecked(&chunk_subset_in_array) };

                self.store_chunk_subset(
                    chunk_indices,
                    &array_subset_in_chunk_subset,
                    chunk_subset_bytes,
                )?;
            }
        } else {
            let store_chunk = |chunk_indices: Vec<u64>| -> Result<(), ArrayError> {
                let chunk_subset_in_array = unsafe {
                    self.chunk_grid()
                        .subset_unchecked(&chunk_indices, self.shape())
                        .unwrap()
                };
                let chunk_subset_in_array_subset =
                    unsafe { chunk_subset_in_array.in_subset_unchecked(array_subset) };
                let chunk_subset_bytes = unsafe {
                    chunk_subset_in_array_subset.extract_bytes_unchecked(
                        &subset_bytes,
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
                    chunk_subset_bytes,
                )?;

                Ok(())
            };
            if parallel {
                (0..chunks.shape().iter().product())
                    .into_par_iter()
                    .map(|chunk_index| {
                        std::iter::zip(unravel_index(chunk_index, chunks.shape()), chunks.start())
                            .map(|(chunk_indices, chunks_start)| chunk_indices + chunks_start)
                            .collect::<Vec<_>>()
                    })
                    // chunks
                    //     .iter_indices()
                    //     .par_bridge()
                    .try_for_each(store_chunk)?;
            } else {
                for chunk_indices in chunks.iter_indices() {
                    store_chunk(chunk_indices)?;
                }
            }
        }
        Ok(())
    }

    /// Encode `subset_bytes` and store in `array_subset`.
    ///
    /// If `parallel` is true, chunks intersecting the array subset are retrieved in parallel.
    /// Prefer to use [`store_chunk`](Array<WritableStorageTraits>::store_chunk) since this will decode and encode each chunk intersecting `array_subset`.
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if
    ///  - the dimensionality of `array_subset` does not match the chunk grid dimensionality
    ///  - the length of `subset_bytes` does not match the expected length governed by the shape of the array subset and the data type size,
    ///  - there is a codec encoding error, or
    ///  - an underlying store error.
    pub fn store_array_subset(
        &self,
        array_subset: &ArraySubset,
        subset_bytes: Vec<u8>,
    ) -> Result<(), ArrayError> {
        self._store_array_subset(array_subset, subset_bytes, false)
    }

    /// Parallel version of [`Array::store_array_subset`].
    #[allow(clippy::missing_panics_doc, clippy::missing_errors_doc)]
    pub fn par_store_array_subset(
        &self,
        array_subset: &ArraySubset,
        subset_bytes: Vec<u8>,
    ) -> Result<(), ArrayError> {
        self._store_array_subset(array_subset, subset_bytes, true)
    }

    fn _store_array_subset_elements<T: TriviallyTransmutable>(
        &self,
        array_subset: &ArraySubset,
        subset_elements: Vec<T>,
        parallel: bool,
    ) -> Result<(), ArrayError> {
        if self.data_type.size() != std::mem::size_of::<T>() {
            return Err(ArrayError::IncompatibleElementSize(
                self.data_type.size(),
                std::mem::size_of::<T>(),
            ));
        }

        let subset_bytes = safe_transmute_to_bytes_vec(subset_elements);
        self._store_array_subset(array_subset, subset_bytes, parallel)
    }

    /// Encode `subset_elements` and store in `array_subset`.
    ///
    /// Prefer to use [`store_chunk`](Array<WritableStorageTraits>::store_chunk) since this will decode and encode each chunk intersecting `array_subset`.
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if
    ///  - the size of `T` does not match the data type size, or
    ///  - a [`store_array_subset`](Array::store_array_subset) error condition is met.
    pub fn store_array_subset_elements<T: TriviallyTransmutable>(
        &self,
        array_subset: &ArraySubset,
        subset_elements: Vec<T>,
    ) -> Result<(), ArrayError> {
        self._store_array_subset_elements(array_subset, subset_elements, false)
    }

    /// Parallel version of [`Array::store_array_subset_elements`].
    #[allow(clippy::missing_panics_doc, clippy::missing_errors_doc)]
    pub fn par_store_array_subset_elements<T: TriviallyTransmutable>(
        &self,
        array_subset: &ArraySubset,
        subset_elements: Vec<T>,
    ) -> Result<(), ArrayError> {
        self._store_array_subset_elements(array_subset, subset_elements, true)
    }

    #[cfg(feature = "ndarray")]
    fn _store_array_subset_ndarray<T: safe_transmute::TriviallyTransmutable>(
        &self,
        subset_start: &[u64],
        subset_array: &ndarray::ArrayViewD<T>,
        parallel: bool,
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
        let elements = array_standard.into_owned().into_raw_vec();
        self._store_array_subset_elements(&subset, elements, parallel)
    }

    #[cfg(feature = "ndarray")]
    /// Encode `subset_array` and store in the array subset starting at `subset_start`.
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if a [`store_array_subset_elements`](Array::store_array_subset_elements) error condition is met.
    #[allow(clippy::missing_panics_doc)]
    pub fn store_array_subset_ndarray<T: safe_transmute::TriviallyTransmutable>(
        &self,
        subset_start: &[u64],
        subset_array: &ndarray::ArrayViewD<T>,
    ) -> Result<(), ArrayError> {
        self._store_array_subset_ndarray(subset_start, subset_array, false)
    }

    #[cfg(feature = "ndarray")]
    /// Parallel version of [`Array::store_array_subset_ndarray`].
    #[allow(clippy::missing_panics_doc, clippy::missing_errors_doc)]
    pub fn par_store_array_subset_ndarray<T: safe_transmute::TriviallyTransmutable>(
        &self,
        subset_start: &[u64],
        subset_array: &ndarray::ArrayViewD<T>,
    ) -> Result<(), ArrayError> {
        self._store_array_subset_ndarray(subset_start, subset_array, true)
    }

    /// Encode `chunk_subset_bytes` and store in `chunk_subset` of the chunk at `chunk_indices`.
    ///
    /// Prefer to use [`store_chunk`](Array<WritableStorageTraits>::store_chunk) since this function may decode the chunk before updating it and reencoding it.
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if
    ///  - `chunk_subset` is invalid or out of bounds of the chunk,
    ///  - there is a codec encoding error, or
    ///  - an underlying store error.
    ///
    /// # Panics
    /// Panics if attempting to reference a byte beyond `usize::MAX`.
    pub fn store_chunk_subset(
        &self,
        chunk_indices: &[u64],
        chunk_subset: &ArraySubset,
        chunk_subset_bytes: Vec<u8>,
    ) -> Result<(), ArrayError> {
        // Validation
        if let Some(chunk_shape) = self.chunk_grid().chunk_shape(chunk_indices, self.shape())? {
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

            if chunk_subset.shape() == chunk_shape && chunk_subset.start().iter().all(|&x| x == 0) {
                // The subset spans the whole chunk, so store the bytes directly and skip decoding
                self.store_chunk(chunk_indices, chunk_subset_bytes)
            } else {
                // Lock the chunk
                let chunk_mutex = self.chunk_mutex(chunk_indices);
                let _lock = chunk_mutex.lock();

                // Decode the entire chunk
                let mut chunk_bytes = self.retrieve_chunk(chunk_indices)?;

                // Update the intersecting subset of the chunk
                let element_size = self.data_type().size() as u64;
                let mut offset = 0;
                for (chunk_element_index, num_elements) in unsafe {
                    chunk_subset.iter_contiguous_linearised_indices_unchecked(&chunk_shape)
                } {
                    let chunk_offset = usize::try_from(chunk_element_index * element_size).unwrap();
                    let length = usize::try_from(num_elements * element_size).unwrap();
                    debug_assert!(chunk_offset + length <= chunk_bytes.len());
                    debug_assert!(offset + length <= chunk_subset_bytes.len());
                    chunk_bytes[chunk_offset..chunk_offset + length]
                        .copy_from_slice(&chunk_subset_bytes[offset..offset + length]);
                    offset += length;
                }

                // Store the updated chunk
                self.store_chunk(chunk_indices, chunk_bytes.into_vec())
            }
        } else {
            Err(ArrayError::InvalidChunkGridIndicesError(
                chunk_indices.to_vec(),
            ))
        }
    }

    /// Encode `chunk_subset_elements` and store in `chunk_subset` of the chunk at `chunk_indices`.
    ///
    /// Prefer to use [`store_chunk`](Array<WritableStorageTraits>::store_chunk) since this will decode the chunk before updating it and reencoding it.
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if
    ///  - the size of  `T` does not match the data type size, or
    ///  - a [`store_chunk_subset`](Array::store_chunk_subset) error condition is met.
    pub fn store_chunk_subset_elements<T: TriviallyTransmutable>(
        &self,
        chunk_indices: &[u64],
        chunk_subset: &ArraySubset,
        chunk_subset_elements: Vec<T>,
    ) -> Result<(), ArrayError> {
        if self.data_type.size() != std::mem::size_of::<T>() {
            return Err(ArrayError::IncompatibleElementSize(
                self.data_type.size(),
                std::mem::size_of::<T>(),
            ));
        }

        let chunk_subset_bytes = safe_transmute_to_bytes_vec(chunk_subset_elements);
        self.store_chunk_subset(chunk_indices, chunk_subset, chunk_subset_bytes)
    }

    #[cfg(feature = "ndarray")]
    /// Encode `chunk_subset_array` and store in `chunk_subset` of the chunk in the subset starting at `chunk_subset_start`.
    ///
    /// Prefer to use [`store_chunk`](Array<WritableStorageTraits>::store_chunk) since this will decode the chunk before updating it and reencoding it.
    ///
    /// # Errors
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
        let elements = array_standard.to_owned().into_raw_vec();
        self.store_chunk_subset_elements(chunk_indices, &subset, elements)
    }
}

// Safe transmute, avoiding an allocation where possible
// https://github.com/nabijaczleweli/safe-transmute-rs/issues/16#issuecomment-471066699
#[doc(hidden)]
#[must_use]
pub fn safe_transmute_to_bytes_vec<T: TriviallyTransmutable>(mut from: Vec<T>) -> Vec<u8> {
    #[cfg(target_family = "windows")]
    {
        // https://github.com/rust-lang/rust/blob/master/library/std/src/sys/common/alloc.rs
        #[cfg(any(
            target_arch = "x86",
            target_arch = "arm",
            target_arch = "m68k",
            target_arch = "csky",
            target_arch = "mips",
            target_arch = "mips32r6",
            target_arch = "powerpc",
            target_arch = "powerpc64",
            target_arch = "sparc",
            target_arch = "asmjs",
            target_arch = "wasm32",
            target_arch = "hexagon",
            all(target_arch = "riscv32", not(target_os = "espidf")),
            all(target_arch = "xtensa", not(target_os = "espidf")),
        ))]
        pub const MIN_ALIGN: usize = 8;
        #[cfg(any(
            target_arch = "x86_64",
            target_arch = "aarch64",
            target_arch = "loongarch64",
            target_arch = "mips64",
            target_arch = "mips64r6",
            target_arch = "s390x",
            target_arch = "sparc64",
            target_arch = "riscv64",
            target_arch = "wasm64",
        ))]
        pub const MIN_ALIGN: usize = 16;
        // The allocator on the esp-idf platform guarantees 4 byte alignment.
        #[cfg(any(
            all(target_arch = "riscv32", target_os = "espidf"),
            all(target_arch = "xtensa", target_os = "espidf"),
        ))]
        pub const MIN_ALIGN: usize = 4;
        // https://github.com/rust-lang/rust/blob/93b6d9e086c6910118a57e4332c9448ab550931f/src/libstd/sys/windows/alloc.rs#L46-L57
        if core::mem::align_of::<T>() <= MIN_ALIGN {
            unsafe {
                let capacity = from.capacity() * core::mem::size_of::<T>();
                let len = from.len() * core::mem::size_of::<T>();
                let ptr = from.as_mut_ptr();
                core::mem::forget(from);
                Vec::from_raw_parts(ptr.cast::<u8>(), len, capacity)
            }
        } else {
            safe_transmute::transmute_to_bytes(&from).to_vec()
        }
    }

    #[cfg(not(target_family = "windows"))]
    unsafe {
        let capacity = from.capacity() * core::mem::size_of::<T>();
        let len = from.len() * core::mem::size_of::<T>();
        let ptr = from.as_mut_ptr();
        core::mem::forget(from);
        Vec::from_raw_parts(ptr.cast::<u8>(), len, capacity)
    }
}

/// Unravel a linearised index to ND indices.
#[doc(hidden)]
#[must_use]
pub fn unravel_index(mut index: u64, shape: &[u64]) -> ArrayIndices {
    let len = shape.len();
    let mut indices = vec![core::mem::MaybeUninit::uninit(); len];
    for (indices_i, &dim) in std::iter::zip(indices.iter_mut().rev(), shape.iter().rev()) {
        indices_i.write(index % dim);
        index /= dim;
    }
    #[allow(clippy::transmute_undefined_repr)]
    unsafe {
        core::mem::transmute(indices)
    }
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
    use itertools::Itertools;
    use rayon::prelude::{IntoParallelIterator, ParallelIterator};

    use crate::{storage::store::MemoryStore, ZARR_NAN_F32};

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
            FillValue::from(ZARR_NAN_F32),
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
        // .storage_transformers(vec![].into())
        .build(store, array_path)
        .unwrap();

        array
            .store_array_subset_elements::<f32>(
                &ArraySubset::new_with_start_shape(vec![3, 3], vec![3, 3]).unwrap(),
                vec![0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9],
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
            .into()
        );
    }

    #[test]
    fn array_subset_locking() {
        let store = Arc::new(MemoryStore::new());

        let array_path = "/array";
        let array = ArrayBuilder::new(
            vec![100, 4],
            DataType::UInt8,
            vec![10, 2].into(),
            FillValue::from(0u8),
        )
        .build(store, array_path)
        .unwrap();

        for j in 1..10 {
            (0..100).into_par_iter().for_each(|i| {
                let subset = ArraySubset::new_with_start_shape(vec![i, 0], vec![1, 4]).unwrap();
                array.store_array_subset(&subset, vec![j; 4]).unwrap();
            });
            let subset_all =
                ArraySubset::new_with_start_shape(vec![0, 0], array.shape().to_vec()).unwrap();
            let data_all = array.retrieve_array_subset(&subset_all).unwrap();
            assert_eq!(data_all.iter().all_equal_value(), Ok(&j));
        }
    }
}
