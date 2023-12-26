//! Zarr arrays.
//!
//! An array is a node in a Zarr hierarchy used to hold multidimensional array data and associated metadata.
//! See <https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html#array>.
//!
//! Use [`ArrayBuilder`] to setup a new array, or use [`Array::new`] for an existing array.
//! The documentation for [`Array`] details how to interact with arrays.

#[cfg(feature = "async")]
mod array_async;
mod array_builder;
mod array_errors;
mod array_metadata;
mod array_representation;
mod array_sync;
mod bytes_representation;
pub mod chunk_grid;
pub mod chunk_key_encoding;
pub mod codec;
pub mod data_type;
mod dimension_name;
mod fill_value;
mod fill_value_metadata;
mod nan_representations;
mod unsafe_cell_slice;

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
    nan_representations::{ZARR_NAN_BF16, ZARR_NAN_F16, ZARR_NAN_F32, ZARR_NAN_F64},
};

use safe_transmute::TriviallyTransmutable;
use serde::Serialize;

use crate::{
    array_subset::{ArraySubset, IncompatibleDimensionalityError},
    metadata::AdditionalFields,
    node::NodePath,
    storage::storage_transformer::StorageTransformerChain,
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
/// ### Metadata
///
/// An array is defined by the following parameters (which are encoded in its JSON metadata):
///  - **shape**: defines the length of the array dimensions,
///  - **data type**: defines the numerical representation array elements,
///  - **chunk grid**: defines how the array is subdivided into chunks,
///  - **chunk key encoding**: defines how chunk grid cell coordinates are mapped to keys in a store,
///  - **fill value**: an element value to use for uninitialised portions of the array.
///  - **codecs**: used to encode and decode chunks,
///
/// and optional parameters:
///  - **attributes**: user-defined attributes,
///  - **storage transformers**: used to intercept and alter the storage keys and bytes of an array before they reach the underlying physical storage, and
///  - **dimension names**: defines the names of the array dimensions.
///
/// See <https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html#array-metadata> for more information on array metadata.
///
/// ### Initilisation
///
/// A *new* array can be initialised with an [`ArrayBuilder`] or [`Array::new_with_metadata`].
///
/// An *existing* array can be initialised with [`Array::new`], its metadata is read from the store.
///
/// The `shape` and `attributes` of an array are mutable and can be updated after construction.
/// However, array metadata must be written explicitly to the store with [`store_metadata`](Array<WritableStorageTraits>::store_metadata) if an array is newly created or its metadata has been mutated.
///
/// ### Methods
///
/// #### Sync API
/// Array operations are divided into several categories based on the traits implemented for the backing [storage](crate::storage). In summary:
///  - [`ReadableStorageTraits`](crate::storage::ReadableStorageTraits): read array data and metadata
///    - [`retrieve_chunk`](Array::retrieve_chunk)
///    - [`retrieve_chunk_subset`](Array::retrieve_chunk_subset)
///    - [`retrieve_array_subset`](Array::retrieve_array_subset) / [`par_retrieve_array_subset`](Array::par_retrieve_array_subset)
///  - [`WritableStorageTraits`](crate::storage::WritableStorageTraits): write array data and metadata
///    - [`store_chunk`](Array::store_chunk)
///    - [`erase_chunk`](Array::erase_chunk)
///  - [`ReadableWritableStorageTraits`](crate::storage::ReadableWritableStorageTraits): perform operations requiring both reading and writing
///    - [`store_chunk_subset`](Array::store_chunk_subset)
///    - [`store_array_subset`](Array::store_array_subset) / [`par_store_array_subset`](Array::par_store_array_subset)
///
/// These `retrieve` and `store` methods have multiple variants:
///   - The above variants store or retrieve data represented as bytes.
///   - Variants with an `_elements` suffix can read and write array elements with a known type.
///   - With the `ndarray` feature, method variants with an `_ndarray` suffix can be used to store or retrieve [`ndarray::Array`]s.
///
/// #### Async API
/// With the `async` feature and an async store, there are equivalent methods to the sync API with an `async_` prefix.
///
/// ### Parallel Writing
///
/// If a chunk is written more than once, its element values depend on whichever operation wrote to the chunk last.
///
/// The [`store_chunk_subset`](Array::store_chunk_subset) and [`store_array_subset`](Array::store_array_subset) methods and their variants internally retrieve a chunk, update it, then store it.
/// Chunks are locked through this process (with [`StoreKeyMutex`](crate::storage::store_lock::StoreKeyMutex)es) otherwise element updates occuring in other threads could be lost.
/// Chunk locking is not implemented for [`WritableStorageTraits`](crate::storage::WritableStorageTraits) methods, so it is recommended not to intermix these with [`ReadableWritableStorageTraits`](crate::storage::ReadableWritableStorageTraits) methods during parallel write operations.
///
/// #### Default Store Locking ([`DefaultStoreLocks`](crate::storage::store_lock::DefaultStoreLocks))
///
/// By default, stores use [`DefaultStoreLocks`](crate::storage::store_lock::DefaultStoreLocks) internally, but this can be changed with a `new_with_locks` store constructor if implemented.
///
/// With [`DefaultStoreLocks`](crate::storage::store_lock::DefaultStoreLocks), if data is written in overlapping array subsets with the [`store_chunk_subset`](Array::store_chunk_subset) or [`store_array_subset`](Array::store_array_subset) methods, the value of an element in overlapping regions depends on whichever operation wrote to its associated chunk last.
/// Consider the case of parallel writing of the following subsets to a `1x6` array and a `1x3` chunk size (**do not do this, it is just an example**):
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
/// Multiple [`Array`]s can safely point to the same array, provided that they use the same store and [`DefaultStoreLocks`](crate::storage::store_lock::DefaultStoreLocks).
///
/// #### Disabled Store Locking ([`DisabledStoreLocks`](crate::storage::store_lock::DisabledStoreLocks))
///
/// A store with [`DisabledStoreLocks`](crate::storage::store_lock::DisabledStoreLocks) can be used to eliminate locking overhead with the [`store_chunk_subset`](Array::store_chunk_subset) and [`store_array_subset`](Array::store_array_subset) methods.
/// However, written data may be lost if a chunk is written by more than one thread.
/// Thus, it is recommended to only use [`DisabledStoreLocks`](crate::storage::store_lock::DisabledStoreLocks) if each chunk is exclusively written by a single thread during a parallel operation.
///
/// #### Distributed Processes
///
/// The synchronisation guarantees provided by an [`Array`] and its underlying store are not applicable in a distributed context (e.g. a distributed program on a cluster).
/// In such cases, the recommendations outlined in [Disabled Store Locking](#disabled-store-locking-disabledstorelocks) should be followed to ensure written data is not lost.
///
/// ### Best Practices
///
/// #### Writing
///
/// For optimum write performance, an array should be written chunk-by-chunk (which can be done in parallel).
/// Methods such as [`store_chunk_subset`](Array::store_chunk_subset) and [`store_array_subset`](Array::store_array_subset) may decode chunks and incur locking overhead, so they are less preferred.
///
/// #### Reading
///
/// It is fastest to load arrays chunk-by-chunk (which can be done in parallel).
/// In contrast, the [`retrieve_chunk_subset`](Array::retrieve_chunk_subset) and [`retrieve_array_subset`](Array::retrieve_array_subset) may partially decode chunks.
/// This can be useful in many cases (e.g. decoding an inner chunk in a chunk encoded with the [`ShardingCodec`](crate::array::codec::ShardingCodec)).
/// However, it can be quite inefficient with some codecs/stores.
///
/// ### `zarrs` Metadata
/// By default, the `zarrs` version and a link to its source code is written to the `_zarrs` attribute in array metadata.
/// This can be disabled with [`set_include_zarrs_metadata(false)`](Array::set_include_zarrs_metadata).
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
    #[must_use]
    pub fn builder(&self) -> ArrayBuilder {
        ArrayBuilder::from_array(self)
    }

    /// Return the shape of the chunk grid (i.e., the number of chunks).
    #[must_use]
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
}

// Safe transmute, avoiding an allocation where possible
//
// A relevant discussion about this can be found here: https://github.com/nabijaczleweli/safe-transmute-rs/issues/16#issuecomment-471066699
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

    use crate::storage::{
        store::MemoryStore,
        store_lock::{DefaultStoreLocks, StoreLocks},
    };

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
                &ArraySubset::new_with_ranges(&[3..6, 3..6]),
                vec![0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9],
            )
            .unwrap();

        let subset_all = ArraySubset::new_with_shape(array.shape().to_vec());
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

    fn array_subset_locking(locks: StoreLocks, expect_equal: bool) {
        let store = Arc::new(MemoryStore::new_with_locks(locks));

        let array_path = "/array";
        let array = ArrayBuilder::new(
            vec![100, 4],
            DataType::UInt8,
            vec![10, 2].into(),
            FillValue::from(0u8),
        )
        .build(store, array_path)
        .unwrap();

        let mut any_not_equal = false;
        for j in 1..10 {
            (0..100).into_par_iter().for_each(|i| {
                let subset = ArraySubset::new_with_ranges(&[i..i + 1, 0..4]);
                array.store_array_subset(&subset, vec![j; 4]).unwrap();
            });
            let subset_all = ArraySubset::new_with_shape(array.shape().to_vec());
            let data_all = array.retrieve_array_subset(&subset_all).unwrap();
            let all_equal = data_all.iter().all_equal_value() == Ok(&j);
            if expect_equal {
                assert!(all_equal);
            } else {
                any_not_equal |= !all_equal;
            }
        }
        if !expect_equal {
            assert!(any_not_equal);
        }
    }

    #[test]
    fn array_subset_locking_default() {
        array_subset_locking(Arc::new(DefaultStoreLocks::default()), true);
    }

    // // Due to the nature of this test, it can fail sometimes. It was used for development but is now disabled.
    // #[test]
    // fn array_subset_locking_disabled() {
    //     array_subset_locking(
    //         Arc::new(crate::storage::store_lock::DisabledStoreLocks::default()),
    //         false,
    //     );
    // }
}
