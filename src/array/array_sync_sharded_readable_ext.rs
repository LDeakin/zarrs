use std::{collections::HashMap, sync::Arc};

use rayon::iter::{IntoParallelIterator, ParallelIterator};
use rayon_iter_concurrent_limit::iter_concurrent_limit;

use super::{
    codec::{CodecError, CodecOptions},
    concurrency::concurrency_chunks_and_codec,
    transmute_from_bytes_vec, validate_element_size, Array, ArrayError, ArrayShardedExt, ArrayView,
    ChunkGrid, UnsafeCellSlice,
};
use crate::storage::ReadableStorageTraits;
use crate::{array::codec::ArrayPartialDecoderTraits, array_subset::ArraySubset};

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
        shard_indices: &[u64],
    ) -> Result<Arc<dyn ArrayPartialDecoderTraits + 'a>, ArrayError> {
        let mut cache = self.cache.lock();
        if let Some(partial_decoder) = cache.get(shard_indices) {
            Ok(partial_decoder.clone())
        } else {
            let partial_decoder: Arc<dyn ArrayPartialDecoderTraits> =
                array.partial_decoder(shard_indices)?.into();
            cache.insert(shard_indices.to_vec(), partial_decoder.clone());
            Ok(partial_decoder)
        }
    }
}

/// An [`Array`] extension trait to efficiently read data (e.g. inner chunks) from arrays using the `sharding_indexed` codec.
///
/// Sharding indexes are cached in a [`ArrayShardedReadableExtCache`] enabling faster retrieval.
// TODO: Add default methods? Or change to options: Option<&CodecOptions>? Should really do this for array (breaking)...
pub trait ArrayShardedReadableExt<TStorage: ?Sized + ReadableStorageTraits + 'static> {
    /// Read and decode the inner chunk at `chunk_indices` into its bytes.
    ///
    /// See [`Array::retrieve_chunk_opt`].
    #[allow(clippy::missing_errors_doc)]
    fn retrieve_inner_chunk_opt<'a>(
        &'a self,
        cache: &ArrayShardedReadableExtCache<'a>,
        inner_chunk_indices: &[u64],
        options: &CodecOptions,
    ) -> Result<Vec<u8>, ArrayError>;

    /// Read and decode the inner chunk at `chunk_indices` into a vector of its elements.
    ///
    /// See [`Array::retrieve_chunk_elements_opt`].
    #[allow(clippy::missing_errors_doc)]
    fn retrieve_inner_chunk_elements_opt<'a, T: bytemuck::Pod>(
        &'a self,
        cache: &ArrayShardedReadableExtCache<'a>,
        inner_chunk_indices: &[u64],
        options: &CodecOptions,
    ) -> Result<Vec<T>, ArrayError>;

    #[cfg(feature = "ndarray")]
    /// Read and decode the chunk at `chunk_indices` into an [`ndarray::ArrayD`].
    ///
    /// See [`Array::retrieve_chunk_ndarray_opt`].
    #[allow(clippy::missing_errors_doc)]
    fn retrieve_inner_chunk_ndarray_opt<'a, T: bytemuck::Pod>(
        &'a self,
        cache: &ArrayShardedReadableExtCache<'a>,
        inner_chunk_indices: &[u64],
        options: &CodecOptions,
    ) -> Result<ndarray::ArrayD<T>, ArrayError>;

    /// Read and decode the chunks at `chunks` into their bytes.
    ///
    /// See [`Array::retrieve_chunks_opt`].
    #[allow(clippy::missing_errors_doc)]
    fn retrieve_inner_chunks_opt<'a>(
        &'a self,
        cache: &ArrayShardedReadableExtCache<'a>,
        inner_chunks: &ArraySubset,
        options: &CodecOptions,
    ) -> Result<Vec<u8>, ArrayError>;

    /// Read and decode the inner chunks at `inner_chunks` into a vector of their elements.
    ///
    /// See [`Array::retrieve_chunks_elements_opt`].
    #[allow(clippy::missing_errors_doc)]
    fn retrieve_inner_chunks_elements_opt<'a, T: bytemuck::Pod>(
        &'a self,
        cache: &ArrayShardedReadableExtCache<'a>,
        inner_chunks: &ArraySubset,
        options: &CodecOptions,
    ) -> Result<Vec<T>, ArrayError>;

    /// Read and decode the inner chunks at `inner_chunks` into an [`ndarray::ArrayD`].
    ///
    /// See [`Array::retrieve_chunks_ndarray_opt`].
    #[cfg(feature = "ndarray")]
    #[allow(clippy::missing_errors_doc)]
    fn retrieve_inner_chunks_ndarray_opt<'a, T: bytemuck::Pod>(
        &'a self,
        cache: &ArrayShardedReadableExtCache<'a>,
        inner_chunks: &ArraySubset,
        options: &CodecOptions,
    ) -> Result<ndarray::ArrayD<T>, ArrayError>;

    /// Read and decode the `array_subset` of array into its bytes.
    ///
    /// See [`Array::retrieve_array_subset_opt`].
    #[allow(clippy::missing_errors_doc)]
    fn retrieve_array_subset_sharded_opt<'a>(
        &'a self,
        cache: &ArrayShardedReadableExtCache<'a>,
        array_subset: &ArraySubset,
        options: &CodecOptions,
    ) -> Result<Vec<u8>, ArrayError>;

    /// Read and decode the `array_subset` of array into a vector of its elements.
    ///
    /// See [`Array::retrieve_array_subset_elements_opt`].
    #[allow(clippy::missing_errors_doc)]
    fn retrieve_array_subset_elements_sharded_opt<'a, T: bytemuck::Pod>(
        &'a self,
        cache: &ArrayShardedReadableExtCache<'a>,
        array_subset: &ArraySubset,
        options: &CodecOptions,
    ) -> Result<Vec<T>, ArrayError>;

    #[cfg(feature = "ndarray")]
    /// Read and decode the `array_subset` of array into an [`ndarray::ArrayD`].
    ///
    /// See [`Array::retrieve_array_subset_ndarray_opt`].
    #[allow(clippy::missing_errors_doc)]
    fn retrieve_array_subset_ndarray_sharded_opt<'a, T: bytemuck::Pod>(
        &'a self,
        cache: &ArrayShardedReadableExtCache<'a>,
        array_subset: &ArraySubset,
        options: &CodecOptions,
    ) -> Result<ndarray::ArrayD<T>, ArrayError>;

    /// Retrieve a shard subset into an array view.
    ///
    /// For an unsharded array, retrieves
    ///
    /// See [`Array::retrieve_array_subset_into_array_view_opt`].
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
    fn retrieve_inner_chunk_opt<'a>(
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
            let shards = self.chunks_in_array_subset(&array_subset)?.ok_or_else(|| {
                ArrayError::InvalidChunkGridIndicesError(inner_chunk_indices.to_vec())
            })?;
            if shards.num_elements() != 1 {
                // This should not happen, but it is checked just in case.
                return Err(ArrayError::InvalidChunkGridIndicesError(
                    inner_chunk_indices.to_vec(),
                ));
            }
            let shard_indices = shards.start();
            let shard_origin = self.chunk_origin(shard_indices)?;
            let shard_subset = array_subset.relative_to(&shard_origin)?;

            let partial_decoder = cache.retrieve(self, shard_indices)?;
            Ok(partial_decoder
                .partial_decode_opt(&[shard_subset], options)?
                .pop()
                .expect("partial_decode_opt called with one subset, returned without error")
                .into_owned())
        } else {
            self.retrieve_chunk_opt(inner_chunk_indices, options)
        }
    }

    fn retrieve_inner_chunk_elements_opt<'a, T: bytemuck::Pod>(
        &'a self,
        cache: &ArrayShardedReadableExtCache<'a>,
        inner_chunk_indices: &[u64],
        options: &CodecOptions,
    ) -> Result<Vec<T>, ArrayError> {
        validate_element_size::<T>(self.data_type())?;
        let bytes = self.retrieve_inner_chunk_opt(cache, inner_chunk_indices, options)?;
        Ok(transmute_from_bytes_vec::<T>(bytes))
    }

    #[cfg(feature = "ndarray")]
    fn retrieve_inner_chunk_ndarray_opt<'a, T: bytemuck::Pod>(
        &'a self,
        cache: &ArrayShardedReadableExtCache<'a>,
        inner_chunk_indices: &[u64],
        options: &CodecOptions,
    ) -> Result<ndarray::ArrayD<T>, ArrayError> {
        if let Some(inner_chunk_shape) = self.inner_chunk_shape() {
            super::elements_to_ndarray(
                &crate::array::chunk_shape_to_array_shape(&inner_chunk_shape),
                self.retrieve_inner_chunk_elements_opt::<T>(cache, inner_chunk_indices, options)?,
            )
        } else {
            self.retrieve_chunk_ndarray_opt(inner_chunk_indices, options)
        }
    }

    fn retrieve_inner_chunks_opt<'a>(
        &'a self,
        cache: &ArrayShardedReadableExtCache<'a>,
        inner_chunks: &ArraySubset,
        options: &CodecOptions,
    ) -> Result<Vec<u8>, ArrayError> {
        if cache.array_is_sharded() {
            let inner_chunk_grid = cache.inner_chunk_grid();
            let array_subset = inner_chunk_grid
                .chunks_subset(inner_chunks, self.shape())?
                .ok_or_else(|| {
                    ArrayError::InvalidArraySubset(
                        inner_chunks.clone(),
                        inner_chunk_grid
                            .grid_shape(self.shape())
                            .unwrap_or_default()
                            .unwrap_or_default(),
                    )
                })?;
            self.retrieve_array_subset_sharded_opt(cache, &array_subset, options)
        } else {
            self.retrieve_chunks_opt(inner_chunks, options)
        }
    }

    fn retrieve_inner_chunks_elements_opt<'a, T: bytemuck::Pod>(
        &'a self,
        cache: &ArrayShardedReadableExtCache<'a>,
        inner_chunks: &ArraySubset,
        options: &CodecOptions,
    ) -> Result<Vec<T>, ArrayError> {
        validate_element_size::<T>(self.data_type())?;
        let bytes = self.retrieve_inner_chunks_opt(cache, inner_chunks, options)?;
        Ok(transmute_from_bytes_vec::<T>(bytes))
    }

    #[cfg(feature = "ndarray")]
    fn retrieve_inner_chunks_ndarray_opt<'a, T: bytemuck::Pod>(
        &'a self,
        cache: &ArrayShardedReadableExtCache<'a>,
        inner_chunks: &ArraySubset,
        options: &CodecOptions,
    ) -> Result<ndarray::ArrayD<T>, ArrayError> {
        let inner_chunk_grid = cache.inner_chunk_grid();
        let array_subset = inner_chunk_grid
            .chunks_subset(inner_chunks, self.shape())?
            .ok_or_else(|| {
                ArrayError::InvalidArraySubset(
                    inner_chunks.clone(),
                    inner_chunk_grid
                        .grid_shape(self.shape())
                        .unwrap_or_default()
                        .unwrap_or_default(),
                )
            })?;
        let elements =
            self.retrieve_inner_chunks_elements_opt::<T>(cache, inner_chunks, options)?;
        super::elements_to_ndarray(array_subset.shape(), elements)
    }

    fn retrieve_array_subset_sharded_opt<'a>(
        &'a self,
        cache: &ArrayShardedReadableExtCache<'a>,
        array_subset: &ArraySubset,
        options: &CodecOptions,
    ) -> Result<Vec<u8>, ArrayError> {
        if cache.array_is_sharded() {
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
                    let retrieve_shard = |shard_indices: Vec<u64>| {
                        let shard_subset = self.chunk_subset(&shard_indices)?;
                        let shard_subset_in_array_subset =
                            unsafe { shard_subset.overlap_unchecked(array_subset) };
                        let shard_subset = unsafe {
                            shard_subset_in_array_subset.relative_to_unchecked(shard_subset.start())
                        };
                        let array_view_subset = unsafe {
                            shard_subset_in_array_subset.relative_to_unchecked(array_subset.start())
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
                            &shard_subset,
                            &array_view,
                            &options,
                        )
                    };
                    let indices = shards.indices();
                    iter_concurrent_limit!(
                        chunk_concurrent_limit,
                        indices,
                        try_for_each,
                        retrieve_shard
                    )?;
                }
                unsafe { output.set_len(size_output) };
                Ok(output)
            }
        } else {
            self.retrieve_array_subset_opt(array_subset, options)
        }
    }

    fn retrieve_array_subset_elements_sharded_opt<'a, T: bytemuck::Pod>(
        &'a self,
        cache: &ArrayShardedReadableExtCache<'a>,
        array_subset: &ArraySubset,
        options: &CodecOptions,
    ) -> Result<Vec<T>, ArrayError> {
        validate_element_size::<T>(self.data_type())?;
        let bytes = self.retrieve_array_subset_sharded_opt(cache, array_subset, options)?;
        Ok(transmute_from_bytes_vec::<T>(bytes))
    }

    #[cfg(feature = "ndarray")]
    fn retrieve_array_subset_ndarray_sharded_opt<'a, T: bytemuck::Pod>(
        &'a self,
        cache: &ArrayShardedReadableExtCache<'a>,
        array_subset: &ArraySubset,
        options: &CodecOptions,
    ) -> Result<ndarray::ArrayD<T>, ArrayError> {
        let elements =
            self.retrieve_array_subset_elements_sharded_opt::<T>(cache, array_subset, options)?;
        super::elements_to_ndarray(array_subset.shape(), elements)
    }

    fn retrieve_shard_subset_into_array_view_opt<'a>(
        &'a self,
        cache: &ArrayShardedReadableExtCache<'a>,
        shard_indices: &[u64],
        shard_subset: &ArraySubset,
        array_view: &ArrayView,
        options: &CodecOptions,
    ) -> Result<(), ArrayError> {
        cache
            .retrieve(self, shard_indices)?
            .partial_decode_into_array_view_opt(shard_subset, array_view, options)
            .map_err(ArrayError::CodecError)
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

    fn array_sharded_ext_impl(sharded: bool) -> Result<(), Box<dyn std::error::Error>> {
        let store = Arc::new(MemoryStore::default());
        let array_path = "/array";
        let mut builder = ArrayBuilder::new(
            vec![8, 8], // array shape
            DataType::UInt16,
            vec![4, 4].try_into()?, // regular chunk shape
            FillValue::from(0u16),
        );
        if sharded {
            builder.array_to_bytes_codec(Box::new(
                ShardingCodecBuilder::new(vec![2, 2].try_into()?)
                    .bytes_to_bytes_codecs(vec![
                        #[cfg(feature = "gzip")]
                        Box::new(crate::array::codec::GzipCodec::new(5)?),
                    ])
                    .build(),
            ));
        }
        let array = builder.build(store, array_path)?;

        let data: Vec<u16> = (0..array.shape().into_iter().product())
            .map(|i| i as u16)
            .collect();

        array.store_array_subset_elements(
            &ArraySubset::new_with_shape(array.shape().to_vec()),
            &data,
        )?;

        let cache = ArrayShardedReadableExtCache::new(&array);
        assert_eq!(array.is_sharded(), sharded);
        let inner_chunk_grid = array.inner_chunk_grid();
        if sharded {
            assert_eq!(array.inner_chunk_shape(), Some(vec![2, 2].try_into()?));
            assert_eq!(
                inner_chunk_grid.grid_shape(array.shape())?,
                Some(vec![4, 4])
            );

            let compare =
                array.retrieve_array_subset_elements::<u16>(&ArraySubset::new_with_ranges(&[
                    4..6,
                    6..8,
                ]))?;
            let test = array.retrieve_inner_chunk_elements_opt::<u16>(
                &cache,
                &[2, 3],
                &CodecOptions::default(),
            )?;
            assert_eq!(compare, test);
            assert_eq!(cache.len(), 1);

            #[cfg(feature = "ndarray")]
            {
                let compare = array.retrieve_array_subset_ndarray::<u16>(
                    &ArraySubset::new_with_ranges(&[4..6, 6..8]),
                )?;
                let test = array.retrieve_inner_chunk_ndarray_opt::<u16>(
                    &cache,
                    &[2, 3],
                    &CodecOptions::default(),
                )?;
                assert_eq!(compare, test);
            }

            cache.clear();
            assert_eq!(cache.len(), 0);

            let subset = ArraySubset::new_with_ranges(&[3..7, 3..7]);
            let compare = array.retrieve_array_subset_elements::<u16>(&subset)?;
            let test = array.retrieve_array_subset_elements_sharded_opt::<u16>(
                &cache,
                &subset,
                &CodecOptions::default(),
            )?;
            assert_eq!(compare, test);
            assert_eq!(cache.len(), 4);

            #[cfg(feature = "ndarray")]
            {
                let subset = ArraySubset::new_with_ranges(&[3..7, 3..7]);
                let compare = array.retrieve_array_subset_ndarray::<u16>(&subset)?;
                let test = array.retrieve_array_subset_ndarray_sharded_opt::<u16>(
                    &cache,
                    &subset,
                    &CodecOptions::default(),
                )?;
                assert_eq!(compare, test);
            }

            let subset = ArraySubset::new_with_ranges(&[2..6, 2..6]);
            let inner_chunks = ArraySubset::new_with_ranges(&[1..3, 1..3]);
            let compare = array.retrieve_array_subset_elements::<u16>(&subset)?;
            let test = array.retrieve_inner_chunks_elements_opt::<u16>(
                &cache,
                &inner_chunks,
                &CodecOptions::default(),
            )?;
            assert_eq!(compare, test);
            assert_eq!(cache.len(), 4);

            #[cfg(feature = "ndarray")]
            {
                let subset = ArraySubset::new_with_ranges(&[2..6, 2..6]);
                let inner_chunks = ArraySubset::new_with_ranges(&[1..3, 1..3]);
                let compare = array.retrieve_array_subset_ndarray::<u16>(&subset)?;
                let test = array.retrieve_inner_chunks_ndarray_opt::<u16>(
                    &cache,
                    &inner_chunks,
                    &CodecOptions::default(),
                )?;
                assert_eq!(compare, test);
                assert_eq!(cache.len(), 4);
            }
        } else {
            assert_eq!(array.inner_chunk_shape(), None);
            assert_eq!(
                inner_chunk_grid.grid_shape(array.shape())?,
                Some(vec![2, 2])
            );

            let compare =
                array.retrieve_array_subset_elements::<u16>(&ArraySubset::new_with_ranges(&[
                    4..8,
                    4..8,
                ]))?;
            let test = array.retrieve_inner_chunk_elements_opt::<u16>(
                &cache,
                &[1, 1],
                &CodecOptions::default(),
            )?;
            assert_eq!(compare, test);

            let subset = ArraySubset::new_with_ranges(&[3..7, 3..7]);
            let compare = array.retrieve_array_subset_elements::<u16>(&subset)?;
            let test = array.retrieve_array_subset_elements_sharded_opt::<u16>(
                &cache,
                &subset,
                &CodecOptions::default(),
            )?;
            assert_eq!(compare, test);
            assert!(cache.is_empty());
        }

        Ok(())
    }

    #[test]
    fn array_sharded_ext() -> Result<(), Box<dyn std::error::Error>> {
        array_sharded_ext_impl(true)
    }

    #[test]
    fn array_sharded_ext_unsharded() -> Result<(), Box<dyn std::error::Error>> {
        array_sharded_ext_impl(false)
    }
}
