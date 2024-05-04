use std::{collections::HashMap, sync::Arc};

use rayon::iter::{IntoParallelIterator, ParallelIterator};
use rayon_iter_concurrent_limit::iter_concurrent_limit;

use super::{
    codec::{CodecError, CodecOptions, ShardingCodecConfiguration},
    concurrency::concurrency_chunks_and_codec,
    Array, ArrayError, ArrayView, ChunkGrid, ChunkShape, UnsafeCellSlice,
};
use crate::storage::ReadableStorageTraits;
use crate::{array::codec::ArrayPartialDecoderTraits, array_subset::ArraySubset};

/// An [`Array`] extension trait to simplify working with arrays using the `sharding_indexed` codec.
pub trait ArrayShardedExt {
    /// Returns true if the array to bytes codec of the array is `sharding_indexed`.
    fn is_sharded(&self) -> bool;

    /// Return the inner chunk shape.
    ///
    /// Returns [`None`] for an unsharded array.
    fn inner_chunk_shape(&self) -> Option<ChunkShape>;

    /// Retrieve the inner chunk grid.
    ///
    /// Returns the normal chunk grid for an unsharded array.
    fn inner_chunk_grid(&self) -> ChunkGrid;
}

impl<TStorage: ?Sized> ArrayShardedExt for Array<TStorage> {
    fn is_sharded(&self) -> bool {
        self.codecs
            .array_to_bytes_codec()
            .create_metadata()
            .expect("the array to bytes codec should have metadata")
            .name() // TODO: Add codec::identifier()?
            == super::codec::array_to_bytes::sharding::IDENTIFIER
    }

    fn inner_chunk_shape(&self) -> Option<ChunkShape> {
        let codec_metadata = self
            .codecs
            .array_to_bytes_codec()
            .create_metadata()
            .expect("the array to bytes codec should have metadata");
        if let Ok(ShardingCodecConfiguration::V1(sharding_configuration)) =
            codec_metadata.to_configuration()
        {
            Some(sharding_configuration.chunk_shape)
        } else {
            None
        }
    }

    fn inner_chunk_grid(&self) -> ChunkGrid {
        if let Some(inner_chunk_shape) = self.inner_chunk_shape() {
            ChunkGrid::new(crate::array::chunk_grid::RegularChunkGrid::new(
                inner_chunk_shape,
            ))
        } else {
            self.chunk_grid().clone()
        }
    }
}

type PartialDecoderHashMap<'a> = HashMap<Vec<u64>, Arc<dyn ArrayPartialDecoderTraits + 'a>>;

/// A cache used for methods in the [`ArrayShardedReadableExt`] trait.
pub struct ArrayShardedReadableExtCache<'a> {
    array_is_sharded: bool,
    inner_chunk_grid: ChunkGrid,
    cache: Arc<parking_lot::Mutex<PartialDecoderHashMap<'a>>>,
}

impl<'a> ArrayShardedReadableExtCache<'a> {
    /// Create a new cache for an array.
    #[must_use]
    pub fn new<TStorage: ?Sized + ReadableStorageTraits + 'static>(
        array: &'a Array<TStorage>,
    ) -> Self {
        let inner_chunk_grid = array.inner_chunk_grid();
        Self {
            array_is_sharded: array.is_sharded(),
            inner_chunk_grid,
            cache: Arc::new(parking_lot::Mutex::new(HashMap::default())),
        }
    }

    /// Returns true if the array is sharded.
    ///
    /// This is cheaper than calling [`ArrayShardedExt::is_sharded`] repeatedly.
    #[must_use]
    pub fn array_is_sharded(&self) -> bool {
        self.array_is_sharded
    }

    fn inner_chunk_grid(&self) -> &ChunkGrid {
        &self.inner_chunk_grid
    }

    /// Return the number of shard indexes cached.
    #[must_use]
    pub fn len(&self) -> usize {
        self.cache.lock().len()
    }

    /// Returns true if the cache contains no cached shard indexes.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.cache.lock().is_empty()
    }

    /// Clear the cache.
    pub fn clear(&self) {
        self.cache.lock().clear();
    }

    fn retrieve<TStorage: ?Sized + ReadableStorageTraits + 'static>(
        &self,
        array: &'a Array<TStorage>,
        chunk_indices: &[u64],
    ) -> Result<Arc<dyn ArrayPartialDecoderTraits + 'a>, ArrayError> {
        let mut cache = self.cache.lock();
        if let Some(partial_decoder) = cache.get(chunk_indices) {
            Ok(partial_decoder.clone())
        } else {
            let partial_decoder: Arc<dyn ArrayPartialDecoderTraits> =
                array.partial_decoder(chunk_indices)?.into();
            cache.insert(chunk_indices.to_vec(), partial_decoder.clone());
            Ok(partial_decoder)
        }
    }
}

/// An [`Array`] extension trait to efficiently read data (e.g. inner chunks) from arrays using the `sharding_indexed` codec.
///
/// Sharding indexes are cached in a [`ArrayShardedReadableExtCache`] enabling faster retrieval in some cases.
// TODO: Add more methods
pub trait ArrayShardedReadableExt<TStorage: ?Sized + ReadableStorageTraits + 'static> {
    /// Read and decode the inner chunk at `chunk_indices` into its bytes.
    ///
    /// See [`Array::retrieve_chunk_opt`].
    #[allow(clippy::missing_errors_doc)]
    fn retrieve_inner_chunk_sharded_opt<'a>(
        &'a self,
        cache: &ArrayShardedReadableExtCache<'a>,
        inner_chunk_indices: &[u64],
        options: &CodecOptions,
    ) -> Result<Vec<u8>, ArrayError>;

    /// Read and decode the `array_subset` of array into its bytes.
    ///
    /// See [`Array::retrieve_array_subset`].
    #[allow(clippy::missing_errors_doc)]
    fn retrieve_array_subset_sharded_opt<'a>(
        &'a self,
        cache: &ArrayShardedReadableExtCache<'a>,
        array_subset: &ArraySubset,
        options: &CodecOptions,
    ) -> Result<Vec<u8>, ArrayError>;

    /// Similar to [`Array::retrieve_array_subset_into_array_view`], except shard indexes are cached.
    #[allow(clippy::missing_errors_doc)]
    fn retrieve_shard_subset_into_array_view_opt<'a>(
        &'a self,
        cache: &ArrayShardedReadableExtCache<'a>,
        shard_indices: &[u64],
        shard_subset: &ArraySubset,
        array_view: &ArrayView,
        options: &CodecOptions,
    ) -> Result<(), ArrayError>;
}

impl<TStorage: ?Sized + ReadableStorageTraits + 'static> ArrayShardedReadableExt<TStorage>
    for Array<TStorage>
{
    fn retrieve_inner_chunk_sharded_opt<'a>(
        &'a self,
        cache: &ArrayShardedReadableExtCache<'a>,
        inner_chunk_indices: &[u64],
        options: &CodecOptions,
    ) -> Result<Vec<u8>, ArrayError> {
        if cache.array_is_sharded() {
            let array_subset = cache
                .inner_chunk_grid()
                .subset(inner_chunk_indices, self.shape())?
                .ok_or_else(|| {
                    ArrayError::InvalidChunkGridIndicesError(inner_chunk_indices.to_vec())
                })?;
            let outer_chunks = self.chunks_in_array_subset(&array_subset)?.ok_or_else(|| {
                ArrayError::InvalidChunkGridIndicesError(inner_chunk_indices.to_vec())
            })?;
            if outer_chunks.num_elements() != 1 {
                // This should not happen, but it is checked just in case.
                return Err(ArrayError::InvalidChunkGridIndicesError(
                    inner_chunk_indices.to_vec(),
                ));
            }
            let shard_indices = outer_chunks.start();
            let shard_origin = self.chunk_origin(shard_indices)?;
            let shard_subset = array_subset.relative_to(&shard_origin)?;

            let partial_decoder = cache.retrieve(self, shard_indices)?;
            Ok(partial_decoder
                .partial_decode_opt(&[shard_subset], options)?
                .pop()
                .expect("partial_decode_opt called with one subset, returned without error"))
        } else {
            self.retrieve_chunk_opt(inner_chunk_indices, options)
        }
    }

    fn retrieve_array_subset_sharded_opt<'a>(
        &'a self,
        cache: &ArrayShardedReadableExtCache<'a>,
        array_subset: &ArraySubset,
        options: &CodecOptions,
    ) -> Result<Vec<u8>, ArrayError> {
        if cache.array_is_sharded() {
            if array_subset.dimensionality() != self.dimensionality() {
                return Err(ArrayError::InvalidArraySubset(
                    array_subset.clone(),
                    self.shape().to_vec(),
                ));
            }

            // Find the shards intersecting this array subset
            let shards = self.chunks_in_array_subset(array_subset)?;
            let Some(shards) = shards else {
                return Err(ArrayError::InvalidArraySubset(
                    array_subset.clone(),
                    self.shape().to_vec(),
                ));
            };

            // Retrieve chunk bytes
            let num_shards = shards.num_elements_usize();
            if num_shards == 0 {
                Ok(self
                    .fill_value()
                    .as_ne_bytes()
                    .repeat(array_subset.num_elements_usize()))
            } else {
                // Allocate the output
                let size_output = array_subset.num_elements_usize() * self.data_type().size();
                let mut output = Vec::with_capacity(size_output);

                // Calculate chunk/codec concurrency
                let chunk_representation =
                    self.chunk_array_representation(&vec![0; self.dimensionality()])?;
                let codec_concurrency =
                    self.recommended_codec_concurrency(&chunk_representation)?;
                let (chunk_concurrent_limit, options) = concurrency_chunks_and_codec(
                    options.concurrent_target(),
                    num_shards,
                    options,
                    &codec_concurrency,
                );

                {
                    let output = UnsafeCellSlice::new_from_vec_with_spare_capacity(&mut output);
                    let retrieve_chunk = |shard_indices: Vec<u64>| {
                        let chunk_subset = self.chunk_subset(&shard_indices)?;
                        let chunk_subset_in_array_subset =
                            unsafe { chunk_subset.overlap_unchecked(array_subset) };
                        let chunk_subset = unsafe {
                            chunk_subset_in_array_subset.relative_to_unchecked(chunk_subset.start())
                        };
                        let array_view_subset = unsafe {
                            chunk_subset_in_array_subset.relative_to_unchecked(array_subset.start())
                        };
                        let array_view = ArrayView::new(
                            unsafe { output.get() },
                            array_subset.shape(),
                            array_view_subset,
                        )
                        .map_err(|err| CodecError::from(err.to_string()))?;
                        self.retrieve_shard_subset_into_array_view_opt(
                            cache,
                            &shard_indices,
                            &chunk_subset,
                            &array_view,
                            &options,
                        )
                    };
                    let indices = shards.indices();
                    iter_concurrent_limit!(
                        chunk_concurrent_limit,
                        indices,
                        try_for_each,
                        retrieve_chunk
                    )?;
                }
                unsafe { output.set_len(size_output) };
                Ok(output)
            }
        } else {
            self.retrieve_array_subset_opt(array_subset, options)
        }
    }

    fn retrieve_shard_subset_into_array_view_opt<'a>(
        &'a self,
        cache: &ArrayShardedReadableExtCache<'a>,
        shard_indices: &[u64],
        shard_subset: &ArraySubset,
        array_view: &ArrayView,
        options: &CodecOptions,
    ) -> Result<(), ArrayError> {
        if shard_subset.shape() != array_view.subset().shape() {
            return Err(ArrayError::InvalidArraySubset(
                shard_subset.clone(),
                array_view.subset().shape().to_vec(),
            ));
        }

        let chunk_representation = self.chunk_array_representation(shard_indices)?;
        if shard_subset.shape() == chunk_representation.shape_u64() {
            self.retrieve_chunk_into_array_view_opt(shard_indices, array_view, options)
        } else {
            cache
                .retrieve(self, shard_indices)?
                .partial_decode_into_array_view_opt(shard_subset, array_view, options)
                .map_err(ArrayError::CodecError)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{
        array::{
            codec::array_to_bytes::sharding::ShardingCodecBuilder, ArrayBuilder, DataType,
            FillValue,
        },
        array_subset::ArraySubset,
        storage::store::MemoryStore,
    };

    use super::*;

    #[test]
    fn array_sharded_ext() -> Result<(), Box<dyn std::error::Error>> {
        let store = Arc::new(MemoryStore::default());
        let array_path = "/array";
        let array = ArrayBuilder::new(
            vec![8, 8], // array shape
            DataType::UInt8,
            vec![4, 4].try_into()?, // regular chunk shape
            FillValue::from(0u8),
        )
        .array_to_bytes_codec(Box::new(
            ShardingCodecBuilder::new(vec![2, 2].try_into()?)
                .bytes_to_bytes_codecs(vec![
                    #[cfg(feature = "gzip")]
                    Box::new(crate::array::codec::GzipCodec::new(5)?),
                ])
                .build(),
        ))
        .build(store, array_path)?;

        let data: Vec<u8> = (0..array.shape().into_iter().product())
            .map(|i| i as u8)
            .collect();

        array.store_array_subset(&ArraySubset::new_with_shape(array.shape().to_vec()), data)?;

        assert!(array.is_sharded());

        assert_eq!(array.inner_chunk_shape(), Some(vec![2, 2].try_into()?));

        let inner_chunk_grid = array.inner_chunk_grid();
        assert_eq!(
            inner_chunk_grid.grid_shape(array.shape())?,
            Some(vec![4, 4])
        );

        let cache = ArrayShardedReadableExtCache::new(&array);
        let compare = array.retrieve_array_subset(&ArraySubset::new_with_ranges(&[4..6, 6..8]))?;
        let test: Vec<u8> =
            array.retrieve_inner_chunk_sharded_opt(&cache, &[2, 3], &CodecOptions::default())?;
        assert_eq!(compare, test);

        assert_eq!(cache.len(), 1);

        let subset = ArraySubset::new_with_ranges(&[3..7, 3..7]);
        let compare2 = array.retrieve_array_subset(&subset)?;
        let test2: Vec<u8> =
            array.retrieve_array_subset_sharded_opt(&cache, &subset, &CodecOptions::default())?;
        assert_eq!(compare2, test2);
        assert_eq!(cache.len(), 4);

        Ok(())
    }

    #[test]
    fn array_sharded_ext_unsharded() -> Result<(), Box<dyn std::error::Error>> {
        let store = Arc::new(MemoryStore::default());
        let array_path = "/array";
        let array = ArrayBuilder::new(
            vec![8, 8], // array shape
            DataType::UInt8,
            vec![4, 4].try_into()?, // regular chunk shape
            FillValue::from(0u8),
        )
        .build(store, array_path)?;

        let data: Vec<u8> = (0..array.shape().into_iter().product())
            .map(|i| i as u8)
            .collect();

        array.store_array_subset(&ArraySubset::new_with_shape(array.shape().to_vec()), data)?;

        assert!(!array.is_sharded());

        assert_eq!(array.inner_chunk_shape(), None);

        let inner_chunk_grid = array.inner_chunk_grid();
        assert_eq!(
            inner_chunk_grid.grid_shape(array.shape())?,
            Some(vec![2, 2])
        );

        let cache = ArrayShardedReadableExtCache::new(&array);
        let compare = array.retrieve_array_subset(&ArraySubset::new_with_ranges(&[4..8, 4..8]))?;
        let test: Vec<u8> =
            array.retrieve_inner_chunk_sharded_opt(&cache, &[1, 1], &CodecOptions::default())?;
        assert_eq!(compare, test);

        let subset = ArraySubset::new_with_ranges(&[3..7, 3..7]);
        let compare2 = array.retrieve_array_subset(&subset)?;
        let test2: Vec<u8> =
            array.retrieve_array_subset_sharded_opt(&cache, &subset, &CodecOptions::default())?;
        assert_eq!(compare2, test2);
        assert!(cache.is_empty());

        Ok(())
    }
}
