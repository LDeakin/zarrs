use std::sync::Arc;

use rayon::iter::{IntoParallelIterator, ParallelIterator};
use rayon_iter_concurrent_limit::iter_concurrent_limit;

use crate::{
    array::{
        array_bytes::{merge_chunks_vlen, update_bytes_flen},
        codec::CodecOptions,
        concurrency::concurrency_chunks_and_codec,
        Array, ArrayBytes, ArrayError, DataTypeSize, UnsafeCellSlice,
    },
    array_subset::ArraySubset,
    storage::ReadableStorageTraits,
};

use super::ChunkCache;

/// An [`Array`] extension trait to support reading with a chunk cache.
///
/// Note that these methods never perform partial decoding and always fully decode chunks intersected that are not in the cache.
pub trait ArrayChunkCacheExt<TStorage: ?Sized + ReadableStorageTraits + 'static> {
    /// Cached variant of [`retrieve_chunk_opt`](Array::retrieve_chunk_opt).
    #[allow(clippy::missing_errors_doc)]
    fn retrieve_chunk_opt_cached(
        &self,
        cache: &impl ChunkCache,
        chunk_indices: &[u64],
        options: &CodecOptions,
    ) -> Result<Arc<ArrayBytes<'static>>, ArrayError>;

    /// Cached variant of [`retrieve_chunks_opt`](Array::retrieve_chunks_opt).
    #[allow(clippy::missing_errors_doc)]
    fn retrieve_chunks_opt_cached(
        &self,
        cache: &impl ChunkCache,
        chunks: &ArraySubset,
        options: &CodecOptions,
    ) -> Result<ArrayBytes<'_>, ArrayError>;

    /// Cached variant of [`retrieve_array_subset_opt`](Array::retrieve_array_subset_opt).
    ///
    /// Unlike [`Array::retrieve_array_subset_opt`] and variants, this method does not use partial decoding and always decode entire chunks.
    #[allow(clippy::missing_errors_doc)]
    fn retrieve_array_subset_opt_cached(
        &self,
        cache: &impl ChunkCache,
        array_subset: &ArraySubset,
        options: &CodecOptions,
    ) -> Result<ArrayBytes<'_>, ArrayError>;
}

impl<TStorage: ?Sized + ReadableStorageTraits + 'static> ArrayChunkCacheExt<TStorage>
    for Array<TStorage>
{
    fn retrieve_chunk_opt_cached(
        &self,
        cache: &impl ChunkCache,
        chunk_indices: &[u64],
        options: &CodecOptions,
    ) -> Result<Arc<ArrayBytes<'static>>, ArrayError> {
        if let Some(chunk) = cache.retrieve(chunk_indices) {
            Ok(chunk)
        } else {
            let chunk = Arc::new(
                self.retrieve_chunk_opt(chunk_indices, options)?
                    .into_owned(),
            );
            cache.insert(chunk_indices, chunk.clone());
            Ok(chunk)
        }
    }

    fn retrieve_chunks_opt_cached(
        &self,
        cache: &impl ChunkCache,
        chunks: &ArraySubset,
        options: &CodecOptions,
    ) -> Result<ArrayBytes<'_>, ArrayError> {
        if chunks.dimensionality() != self.dimensionality() {
            return Err(ArrayError::InvalidArraySubset(
                chunks.clone(),
                self.shape().to_vec(),
            ));
        }

        let array_subset = self.chunks_subset(chunks)?;
        self.retrieve_array_subset_opt_cached(cache, &array_subset, options)
    }

    fn retrieve_array_subset_opt_cached(
        &self,
        cache: &impl ChunkCache,
        array_subset: &ArraySubset,
        options: &CodecOptions,
    ) -> Result<ArrayBytes<'_>, ArrayError> {
        if array_subset.dimensionality() != self.dimensionality() {
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

        let chunk_representation0 =
            self.chunk_array_representation(&vec![0; self.dimensionality()])?;

        // Calculate chunk/codec concurrency
        let num_chunks = chunks.num_elements_usize();
        let codec_concurrency = self.recommended_codec_concurrency(&chunk_representation0)?;
        let (chunk_concurrent_limit, options) = concurrency_chunks_and_codec(
            options.concurrent_target(),
            num_chunks,
            options,
            &codec_concurrency,
        );

        // Retrieve chunks
        let indices = chunks.indices();
        let chunk_bytes_and_subsets =
            iter_concurrent_limit!(chunk_concurrent_limit, indices, map, |chunk_indices| {
                let chunk_subset = self.chunk_subset(&chunk_indices)?;
                self.retrieve_chunk_opt_cached(cache, &chunk_indices, &options)
                    .map(|bytes| (bytes, chunk_subset))
            })
            .collect::<Result<Vec<_>, ArrayError>>()?;

        // Merge
        match self.data_type().size() {
            DataTypeSize::Variable => {
                // Arc<ArrayBytes> -> ArrayBytes (not copied, but a bit wasteful, change merge_chunks_vlen?)
                let chunk_bytes_and_subsets = chunk_bytes_and_subsets
                    .iter()
                    .map(|(chunk_bytes, chunk_subset)| {
                        (ArrayBytes::clone(chunk_bytes), chunk_subset.clone())
                    })
                    .collect();
                Ok(merge_chunks_vlen(
                    chunk_bytes_and_subsets,
                    array_subset.shape(),
                )?)
            }
            DataTypeSize::Fixed(data_type_size) => {
                // Allocate the output
                let size_output = array_subset.num_elements_usize() * data_type_size;
                let mut output = Vec::with_capacity(size_output);

                {
                    let output = UnsafeCellSlice::new_from_vec_with_spare_capacity(&mut output);
                    let update_output =
                        |(chunk_subset_bytes, chunk_subset): (Arc<ArrayBytes>, ArraySubset)| {
                            // Extract the overlapping bytes
                            let chunk_subset_overlap = chunk_subset.overlap(array_subset)?;
                            let chunk_subset_bytes = chunk_subset_bytes.extract_array_subset(
                                &chunk_subset_overlap.relative_to(chunk_subset.start())?,
                                chunk_subset.shape(),
                                self.data_type(),
                            )?;

                            update_bytes_flen(
                                unsafe { output.get() },
                                array_subset.shape(),
                                &chunk_subset_bytes.into_fixed()?,
                                &chunk_subset_overlap.relative_to(array_subset.start())?,
                                data_type_size,
                            );
                            Ok::<_, ArrayError>(())
                        };
                    iter_concurrent_limit!(
                        chunk_concurrent_limit,
                        chunk_bytes_and_subsets,
                        try_for_each,
                        update_output
                    )?;
                }
                unsafe { output.set_len(size_output) };
                Ok(ArrayBytes::from(output))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::{num::NonZeroUsize, sync::Arc};

    use crate::{
        array::{ArrayBuilder, ChunkCacheLruChunks, ChunkCacheLruSize, DataType, FillValue},
        array_subset::ArraySubset,
        storage::{storage_transformer::PerformanceMetricsStorageTransformer, store::MemoryStore},
    };

    #[test]
    fn array_chunk_cache_chunks() {
        use crate::storage::storage_transformer::StorageTransformerExtension;

        let performance_metrics = Arc::new(PerformanceMetricsStorageTransformer::new());
        let store = Arc::new(MemoryStore::default());
        let store = performance_metrics
            .clone()
            .create_readable_writable_transformer(store);
        let builder = ArrayBuilder::new(
            vec![8, 8], // array shape
            DataType::UInt8,
            vec![4, 4].try_into().unwrap(), // regular chunk shape
            FillValue::from(0u8),
        );
        let array = builder.build(store, "/").unwrap();

        let data: Vec<u8> = (0..array.shape().into_iter().product())
            .map(|i| i as u8)
            .collect();
        array
            .store_array_subset_elements(
                &ArraySubset::new_with_shape(array.shape().to_vec()),
                &data,
            )
            .unwrap();

        let cache = ChunkCacheLruChunks::new(NonZeroUsize::new(2).unwrap());

        assert_eq!(performance_metrics.reads(), 0);
        assert!(cache.is_empty());
        assert_eq!(
            array
                .retrieve_array_subset_opt_cached(
                    &cache,
                    &ArraySubset::new_with_ranges(&[3..5, 0..4]),
                    &CodecOptions::default()
                )
                .unwrap(),
            vec![24, 25, 26, 27, 32, 33, 34, 35,].into()
        );
        assert_eq!(performance_metrics.reads(), 2);
        assert_eq!(cache.len(), 2);

        // Retrieve a chunk in cache
        assert_eq!(
            array
                .retrieve_chunk_opt_cached(&cache, &[0, 0], &CodecOptions::default())
                .unwrap(),
            Arc::new(vec![0, 1, 2, 3, 8, 9, 10, 11, 16, 17, 18, 19, 24, 25, 26, 27].into())
        );
        assert_eq!(performance_metrics.reads(), 2);
        assert_eq!(cache.len(), 2);
        assert!(cache.retrieve(&[0, 0]).is_some());
        assert!(cache.retrieve(&[1, 0]).is_some());

        // Retrieve a chunk not in cache
        assert_eq!(
            array
                .retrieve_chunk_opt_cached(&cache, &[0, 1], &CodecOptions::default())
                .unwrap(),
            Arc::new(vec![4, 5, 6, 7, 12, 13, 14, 15, 20, 21, 22, 23, 28, 29, 30, 31].into())
        );
        assert_eq!(performance_metrics.reads(), 3);
        assert_eq!(cache.len(), 2);
        assert!(cache.retrieve(&[0, 1]).is_some());
        assert!(cache.retrieve(&[0, 0]).is_none() || cache.retrieve(&[1, 0]).is_none());
    }

    #[test]
    fn array_chunk_cache_size() {
        use crate::storage::storage_transformer::StorageTransformerExtension;

        let performance_metrics = Arc::new(PerformanceMetricsStorageTransformer::new());
        let store = Arc::new(MemoryStore::default());
        let store = performance_metrics
            .clone()
            .create_readable_writable_transformer(store);
        let builder = ArrayBuilder::new(
            vec![8, 8], // array shape
            DataType::UInt8,
            vec![4, 4].try_into().unwrap(), // regular chunk shape
            FillValue::from(0u8),
        );
        let array = builder.build(store, "/").unwrap();

        let data: Vec<u8> = (0..array.shape().into_iter().product())
            .map(|i| i as u8)
            .collect();
        array
            .store_array_subset_elements(
                &ArraySubset::new_with_shape(array.shape().to_vec()),
                &data,
            )
            .unwrap();

        // Create a cache with a size limit equivalent to 2 chunks
        let chunk_size = 4 * 4 * size_of::<u8>();
        let cache = ChunkCacheLruSize::new(NonZeroUsize::new(2 * chunk_size).unwrap());

        assert_eq!(performance_metrics.reads(), 0);
        assert!(cache.is_empty());
        assert_eq!(cache.size(), 0);
        assert_eq!(
            array
                .retrieve_array_subset_opt_cached(
                    &cache,
                    &ArraySubset::new_with_ranges(&[3..5, 0..4]),
                    &CodecOptions::default()
                )
                .unwrap(),
            vec![24, 25, 26, 27, 32, 33, 34, 35,].into()
        );
        assert_eq!(performance_metrics.reads(), 2);
        assert_eq!(cache.len(), 2);
        assert_eq!(cache.size(), chunk_size * 2);

        // Retrieve a chunk in cache
        assert_eq!(
            array
                .retrieve_chunk_opt_cached(&cache, &[0, 0], &CodecOptions::default())
                .unwrap(),
            Arc::new(vec![0, 1, 2, 3, 8, 9, 10, 11, 16, 17, 18, 19, 24, 25, 26, 27].into())
        );
        assert_eq!(performance_metrics.reads(), 2);
        assert_eq!(cache.len(), 2);
        assert_eq!(cache.size(), chunk_size * 2);
        assert!(cache.retrieve(&[0, 0]).is_some());
        assert!(cache.retrieve(&[1, 0]).is_some());

        // Retrieve a chunk not in cache
        assert_eq!(
            array
                .retrieve_chunk_opt_cached(&cache, &[0, 1], &CodecOptions::default())
                .unwrap(),
            Arc::new(vec![4, 5, 6, 7, 12, 13, 14, 15, 20, 21, 22, 23, 28, 29, 30, 31].into())
        );
        assert_eq!(performance_metrics.reads(), 3);
        assert_eq!(cache.len(), 2);
        assert_eq!(cache.size(), chunk_size * 2);
        assert!(cache.retrieve(&[0, 1]).is_some());
        assert!(cache.retrieve(&[0, 0]).is_none() || cache.retrieve(&[1, 0]).is_none());
    }
}
