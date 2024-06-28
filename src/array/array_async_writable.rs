use std::{borrow::Cow, sync::Arc};

use futures::{StreamExt, TryStreamExt};

use crate::{
    array_subset::ArraySubset,
    metadata::MetadataEraseVersion,
    storage::{
        meta_key, meta_key_v2_array, meta_key_v2_attributes, AsyncBytes,
        AsyncWritableStorageTraits, StorageError, StorageHandle,
    },
};

use super::{
    codec::{options::CodecOptions, ArrayCodecTraits},
    concurrency::concurrency_chunks_and_codec,
    Array, ArrayError, ArrayMetadata, ArrayMetadataOptions,
};

impl<TStorage: ?Sized + AsyncWritableStorageTraits + 'static> Array<TStorage> {
    /// Async variant of [`store_metadata`](Array::store_metadata).
    #[allow(clippy::missing_errors_doc)]
    pub async fn async_store_metadata(&self) -> Result<(), StorageError> {
        self.async_store_metadata_opt(&ArrayMetadataOptions::default())
            .await
    }

    /// Async variant of [`store_metadata_opt`](Array::store_metadata_opt).
    #[allow(clippy::missing_errors_doc)]
    pub async fn async_store_metadata_opt(
        &self,
        options: &ArrayMetadataOptions,
    ) -> Result<(), StorageError> {
        let storage_handle = Arc::new(StorageHandle::new(self.storage.clone()));
        let storage_transformer = self
            .storage_transformers()
            .create_async_writable_transformer(storage_handle);

        // Get the metadata with options applied and store
        let metadata = self.metadata_opt(options);
        crate::storage::async_create_array(&*storage_transformer, self.path(), &metadata).await
    }

    /// Async variant of [`store_chunk`](Array::store_chunk).
    #[allow(clippy::missing_errors_doc)]
    pub async fn async_store_chunk(
        &self,
        chunk_indices: &[u64],
        chunk_bytes: &[u8],
    ) -> Result<(), ArrayError> {
        self.async_store_chunk_opt(chunk_indices, chunk_bytes, &CodecOptions::default())
            .await
    }

    /// Async variant of [`store_chunk_elements`](Array::store_chunk_elements).
    #[allow(clippy::missing_errors_doc)]
    pub async fn async_store_chunk_elements<T: bytemuck::Pod + Send + Sync>(
        &self,
        chunk_indices: &[u64],
        chunk_elements: &[T],
    ) -> Result<(), ArrayError> {
        self.async_store_chunk_elements_opt(chunk_indices, chunk_elements, &CodecOptions::default())
            .await
    }

    #[cfg(feature = "ndarray")]
    /// Async variant of [`store_chunk_ndarray`](Array::store_chunk_ndarray).
    #[allow(clippy::missing_errors_doc)]
    pub async fn async_store_chunk_ndarray<
        T: bytemuck::Pod + Send + Sync,
        TArray: Into<ndarray::Array<T, D>> + Send,
        D: ndarray::Dimension,
    >(
        &self,
        chunk_indices: &[u64],
        chunk_array: TArray,
    ) -> Result<(), ArrayError> {
        self.async_store_chunk_ndarray_opt(chunk_indices, chunk_array, &CodecOptions::default())
            .await
    }

    /// Async variant of [`store_chunks`](Array::store_chunks).
    #[allow(clippy::missing_errors_doc)]
    #[allow(clippy::similar_names)]
    pub async fn async_store_chunks(
        &self,
        chunks: &ArraySubset,
        chunks_bytes: &[u8],
    ) -> Result<(), ArrayError> {
        self.async_store_chunks_opt(chunks, chunks_bytes, &CodecOptions::default())
            .await
    }

    /// Async variant of [`store_chunks_elements`](Array::store_chunks_elements).
    #[allow(clippy::missing_errors_doc)]
    pub async fn async_store_chunks_elements<T: bytemuck::Pod + Send + Sync>(
        &self,
        chunks: &ArraySubset,
        chunks_elements: &[T],
    ) -> Result<(), ArrayError> {
        self.async_store_chunks_elements_opt(chunks, chunks_elements, &CodecOptions::default())
            .await
    }

    #[cfg(feature = "ndarray")]
    /// Async variant of [`store_chunks_ndarray`](Array::store_chunks_ndarray).
    #[allow(clippy::missing_errors_doc)]
    pub async fn async_store_chunks_ndarray<
        T: bytemuck::Pod + Send + Sync,
        TArray: Into<ndarray::Array<T, D>> + Send,
        D: ndarray::Dimension,
    >(
        &self,
        chunks: &ArraySubset,
        chunks_array: TArray,
    ) -> Result<(), ArrayError> {
        self.async_store_chunks_ndarray_opt(chunks, chunks_array, &CodecOptions::default())
            .await
    }

    /// Async variant of [`erase_metadata`](Array::erase_metadata).
    #[allow(clippy::missing_errors_doc)]
    pub async fn async_erase_metadata(&self) -> Result<(), StorageError> {
        self.async_erase_metadata_opt(&MetadataEraseVersion::default())
            .await
    }

    /// Async variant of [`erase_metadata_opt`](Array::erase_metadata_opt).
    #[allow(clippy::missing_errors_doc)]
    pub async fn async_erase_metadata_opt(
        &self,
        options: &MetadataEraseVersion,
    ) -> Result<(), StorageError> {
        let storage_handle = StorageHandle::new(self.storage.clone());
        match options {
            MetadataEraseVersion::Default => match self.metadata {
                ArrayMetadata::V3(_) => storage_handle.erase(&meta_key(self.path())).await,
                ArrayMetadata::V2(_) => {
                    storage_handle
                        .erase(&meta_key_v2_array(self.path()))
                        .await?;
                    storage_handle
                        .erase(&meta_key_v2_attributes(self.path()))
                        .await
                }
            },
            MetadataEraseVersion::All => {
                storage_handle.erase(&meta_key(self.path())).await?;
                storage_handle
                    .erase(&meta_key_v2_array(self.path()))
                    .await?;
                storage_handle
                    .erase(&meta_key_v2_attributes(self.path()))
                    .await
            }
            MetadataEraseVersion::V3 => storage_handle.erase(&meta_key(self.path())).await,
            MetadataEraseVersion::V2 => {
                storage_handle
                    .erase(&meta_key_v2_array(self.path()))
                    .await?;
                storage_handle
                    .erase(&meta_key_v2_attributes(self.path()))
                    .await
            }
        }
    }

    /// Async variant of [`erase_chunk`](Array::erase_chunk).
    #[allow(clippy::missing_errors_doc)]
    pub async fn async_erase_chunk(&self, chunk_indices: &[u64]) -> Result<(), StorageError> {
        let storage_handle = Arc::new(StorageHandle::new(self.storage.clone()));
        let storage_transformer = self
            .storage_transformers()
            .create_async_writable_transformer(storage_handle);
        crate::storage::async_erase_chunk(
            &*storage_transformer,
            self.path(),
            chunk_indices,
            self.chunk_key_encoding(),
        )
        .await
    }

    /// Async variant of [`erase_chunks`](Array::erase_chunks).
    #[allow(clippy::missing_errors_doc)]
    pub async fn async_erase_chunks(&self, chunks: &ArraySubset) -> Result<(), StorageError> {
        let storage_handle = Arc::new(StorageHandle::new(self.storage.clone()));
        let storage_transformer = self
            .storage_transformers()
            .create_async_writable_transformer(storage_handle);
        let erase_chunk = |chunk_indices: Vec<u64>| {
            let storage_transformer = storage_transformer.clone();
            async move {
                crate::storage::async_erase_chunk(
                    &*storage_transformer,
                    self.path(),
                    &chunk_indices,
                    self.chunk_key_encoding(),
                )
                .await
            }
        };
        futures::stream::iter(chunks.indices().into_iter())
            .map(Ok)
            .try_for_each_concurrent(None, erase_chunk)
            .await
    }

    /////////////////////////////////////////////////////////////////////////////
    // Advanced methods
    /////////////////////////////////////////////////////////////////////////////

    /// Async variant of [`store_chunk_opt`](Array::store_chunk_opt).
    #[allow(clippy::missing_errors_doc)]
    pub async fn async_store_chunk_opt(
        &self,
        chunk_indices: &[u64],
        chunk_bytes: &[u8],
        options: &CodecOptions,
    ) -> Result<(), ArrayError> {
        // Validation
        let chunk_array_representation = self.chunk_array_representation(chunk_indices)?;
        if chunk_bytes.len() as u64 != chunk_array_representation.size() {
            return Err(ArrayError::InvalidBytesInputSize(
                chunk_bytes.len(),
                chunk_array_representation.size(),
            ));
        }

        if !options.store_empty_chunks() && self.fill_value().equals_all(chunk_bytes) {
            self.async_erase_chunk(chunk_indices).await?;
            Ok(())
        } else {
            let storage_handle = Arc::new(StorageHandle::new(self.storage.clone()));
            let storage_transformer = self
                .storage_transformers()
                .create_async_writable_transformer(storage_handle);
            let chunk_encoded = self
                .codecs()
                .encode(
                    Cow::Borrowed(chunk_bytes),
                    &chunk_array_representation,
                    options,
                )
                .map_err(ArrayError::CodecError)?;
            let chunk_encoded = AsyncBytes::from(chunk_encoded.to_vec());
            crate::storage::async_store_chunk(
                &*storage_transformer,
                self.path(),
                chunk_indices,
                self.chunk_key_encoding(),
                chunk_encoded,
            )
            .await
            .map_err(ArrayError::StorageError)
        }
    }

    /// Async variant of [`store_chunk_elements_opt`](Array::store_chunk_elements_opt).
    #[allow(clippy::missing_errors_doc)]
    pub async fn async_store_chunk_elements_opt<T: bytemuck::Pod + Send + Sync>(
        &self,
        chunk_indices: &[u64],
        chunk_elements: &[T],
        options: &CodecOptions,
    ) -> Result<(), ArrayError> {
        array_async_store_elements!(
            self,
            chunk_elements,
            async_store_chunk_opt(chunk_indices, &chunk_elements, options)
        )
    }

    #[cfg(feature = "ndarray")]
    /// Async variant of [`store_chunk_ndarray_opt`](Array::store_chunk_ndarray_opt).
    #[allow(clippy::missing_errors_doc)]
    pub async fn async_store_chunk_ndarray_opt<
        T: bytemuck::Pod + Send + Sync,
        TArray: Into<ndarray::Array<T, D>> + Send,
        D: ndarray::Dimension,
    >(
        &self,
        chunk_indices: &[u64],
        chunk_array: TArray,
        options: &CodecOptions,
    ) -> Result<(), ArrayError> {
        let chunk_array: ndarray::Array<T, D> = chunk_array.into();
        let chunk_shape = self.chunk_shape_usize(chunk_indices)?;
        if chunk_array.shape() == chunk_shape {
            array_async_store_ndarray!(
                self,
                chunk_array,
                async_store_chunk_elements_opt(chunk_indices, &chunk_array, options)
            )
        } else {
            Err(ArrayError::InvalidDataShape(
                chunk_array.shape().to_vec(),
                chunk_shape,
            ))
        }
    }

    /// Async variant of [`store_chunks_opt`](Array::store_chunks_opt).
    #[allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]
    #[allow(clippy::similar_names)]
    pub async fn async_store_chunks_opt(
        &self,
        chunks: &ArraySubset,
        chunks_bytes: &[u8],
        options: &CodecOptions,
    ) -> Result<(), ArrayError> {
        let num_chunks = chunks.num_elements_usize();
        match num_chunks {
            0 => {}
            1 => {
                let chunk_indices = chunks.start();
                self.async_store_chunk_opt(chunk_indices, chunks_bytes, options)
                    .await?;
            }
            _ => {
                let array_subset = self.chunks_subset(chunks)?;
                let element_size = self.data_type().size();
                let expected_size = element_size as u64 * array_subset.num_elements();
                if chunks_bytes.len() as u64 != expected_size {
                    return Err(ArrayError::InvalidBytesInputSize(
                        chunks_bytes.len(),
                        expected_size,
                    ));
                }

                // Calculate chunk/codec concurrency
                let chunk_representation =
                    self.chunk_array_representation(&vec![0; self.dimensionality()])?;
                let codec_concurrency =
                    self.recommended_codec_concurrency(&chunk_representation)?;
                let (chunk_concurrent_limit, options) = concurrency_chunks_and_codec(
                    options.concurrent_target(),
                    num_chunks,
                    options,
                    &codec_concurrency,
                );

                let store_chunk = |chunk_indices: Vec<u64>| {
                    let chunk_subset_in_array = unsafe {
                        self.chunk_grid()
                            .subset_unchecked(&chunk_indices, self.shape())
                            .unwrap() // FIXME: Unwrap
                    };
                    let overlap = unsafe { array_subset.overlap_unchecked(&chunk_subset_in_array) };
                    let chunk_subset_in_array_subset =
                        unsafe { overlap.relative_to_unchecked(array_subset.start()) };
                    let chunk_bytes = unsafe {
                        chunk_subset_in_array_subset.extract_bytes_unchecked(
                            chunks_bytes,
                            array_subset.shape(),
                            element_size,
                        )
                    };

                    debug_assert_eq!(
                        chunk_subset_in_array.num_elements(),
                        chunk_subset_in_array_subset.num_elements()
                    );

                    let options = options.clone();
                    async move {
                        self.async_store_chunk_opt(&chunk_indices, &chunk_bytes, &options)
                            .await
                    }
                };
                futures::stream::iter(&chunks.indices())
                    .map(Ok)
                    .try_for_each_concurrent(Some(chunk_concurrent_limit), store_chunk)
                    .await?;
            }
        }

        Ok(())
    }

    /// Async variant of [`store_chunks_elements_opt`](Array::store_chunks_elements_opt).
    #[allow(clippy::missing_errors_doc)]
    pub async fn async_store_chunks_elements_opt<T: bytemuck::Pod + Send + Sync>(
        &self,
        chunks: &ArraySubset,
        chunks_elements: &[T],
        options: &CodecOptions,
    ) -> Result<(), ArrayError> {
        array_async_store_elements!(
            self,
            chunks_elements,
            async_store_chunks_opt(chunks, &chunks_elements, options)
        )
    }

    #[cfg(feature = "ndarray")]
    /// Async variant of [`store_chunks_ndarray_opt`](Array::store_chunks_ndarray_opt).
    #[allow(clippy::missing_errors_doc)]
    pub async fn async_store_chunks_ndarray_opt<
        T: bytemuck::Pod + Send + Sync,
        TArray: Into<ndarray::Array<T, D>> + Send,
        D: ndarray::Dimension,
    >(
        &self,
        chunks: &ArraySubset,
        chunks_array: TArray,
        options: &CodecOptions,
    ) -> Result<(), ArrayError> {
        let chunks_array: ndarray::Array<T, D> = chunks_array.into();
        let chunks_subset = self.chunks_subset(chunks)?;
        let chunks_shape = chunks_subset.shape_usize();
        if chunks_array.shape() == chunks_shape {
            array_async_store_ndarray!(
                self,
                chunks_array,
                async_store_chunks_elements_opt(chunks, &chunks_array, options)
            )
        } else {
            Err(ArrayError::InvalidDataShape(
                chunks_array.shape().to_vec(),
                chunks_shape,
            ))
        }
    }
}
