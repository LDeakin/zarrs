use futures::StreamExt;

use crate::{
    array_subset::ArraySubset,
    storage::{data_key, AsyncReadableWritableStorageTraits},
};

use super::{
    codec::options::CodecOptions, concurrency::concurrency_chunks_and_codec, Array, ArrayError,
};

impl<TStorage: ?Sized + AsyncReadableWritableStorageTraits + 'static> Array<TStorage> {
    /// Encode `chunk_subset_bytes` and store in `chunk_subset` of the chunk at `chunk_indices` (default options).
    ///
    /// See [`Array::async_store_chunk_subset_opt`].
    #[allow(clippy::missing_panics_doc, clippy::missing_errors_doc)]
    pub async fn async_store_chunk_subset(
        &self,
        chunk_indices: &[u64],
        chunk_subset: &ArraySubset,
        chunk_subset_bytes: Vec<u8>,
    ) -> Result<(), ArrayError> {
        self.async_store_chunk_subset_opt(
            chunk_indices,
            chunk_subset,
            chunk_subset_bytes,
            &CodecOptions::default(),
        )
        .await
    }

    /// Encode `chunk_subset_elements` and store in `chunk_subset` of the chunk at `chunk_indices` (default options).
    ///
    /// See [`Array::async_store_chunk_subset_elements_opt`].
    #[allow(clippy::missing_panics_doc, clippy::missing_errors_doc)]
    pub async fn async_store_chunk_subset_elements<T: bytemuck::Pod + Send + Sync>(
        &self,
        chunk_indices: &[u64],
        chunk_subset: &ArraySubset,
        chunk_subset_elements: Vec<T>,
    ) -> Result<(), ArrayError> {
        self.async_store_chunk_subset_elements_opt(
            chunk_indices,
            chunk_subset,
            chunk_subset_elements,
            &CodecOptions::default(),
        )
        .await
    }

    #[cfg(feature = "ndarray")]
    /// Encode `chunk_subset_array` and store in `chunk_subset` of the chunk in the subset starting at `chunk_subset_start` (default options).
    ///
    /// See [`Array::async_store_chunk_subset_ndarray_opt`].
    #[allow(clippy::missing_panics_doc, clippy::missing_errors_doc)]
    pub async fn async_store_chunk_subset_ndarray<T: bytemuck::Pod + Send + Sync>(
        &self,
        chunk_indices: &[u64],
        chunk_subset_start: &[u64],
        chunk_subset_array: &ndarray::ArrayViewD<'_, T>,
    ) -> Result<(), ArrayError> {
        self.async_store_chunk_subset_ndarray_opt(
            chunk_indices,
            chunk_subset_start,
            chunk_subset_array,
            &CodecOptions::default(),
        )
        .await
    }

    /// Encode `subset_bytes` and store in `array_subset` (default options).
    ///
    /// See [`Array::async_store_array_subset_opt`].
    #[allow(clippy::missing_panics_doc, clippy::missing_errors_doc)]
    pub async fn async_store_array_subset(
        &self,
        array_subset: &ArraySubset,
        subset_bytes: Vec<u8>,
    ) -> Result<(), ArrayError> {
        self.async_store_array_subset_opt(array_subset, subset_bytes, &CodecOptions::default())
            .await
    }

    /// Encode `subset_elements` and store in `array_subset` (default options).
    ///
    /// See [`Array::async_store_array_subset_elements_opt`].
    #[allow(clippy::missing_panics_doc, clippy::missing_errors_doc)]
    pub async fn async_store_array_subset_elements<T: bytemuck::Pod + Send + Sync>(
        &self,
        array_subset: &ArraySubset,
        subset_elements: Vec<T>,
    ) -> Result<(), ArrayError> {
        self.async_store_array_subset_elements_opt(
            array_subset,
            subset_elements,
            &CodecOptions::default(),
        )
        .await
    }

    #[cfg(feature = "ndarray")]
    /// Encode `subset_array` and store in the array subset starting at `subset_start` (default options).
    ///
    /// See [`Array::async_store_array_subset_ndarray_opt`].
    #[allow(clippy::missing_panics_doc, clippy::missing_errors_doc)]
    pub async fn async_store_array_subset_ndarray<T: bytemuck::Pod + Send + Sync>(
        &self,
        subset_start: &[u64],
        subset_array: &ndarray::ArrayViewD<'_, T>,
    ) -> Result<(), ArrayError> {
        self.async_store_array_subset_ndarray_opt(
            subset_start,
            subset_array,
            &CodecOptions::default(),
        )
        .await
    }

    /////////////////////////////////////////////////////////////////////////////
    /// Advanced methods
    /////////////////////////////////////////////////////////////////////////////

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
    #[allow(clippy::missing_panics_doc, clippy::too_many_lines)]
    pub async fn async_store_array_subset_opt(
        &self,
        array_subset: &ArraySubset,
        subset_bytes: Vec<u8>,
        options: &CodecOptions,
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
                self.async_store_chunk_opt(chunk_indices, subset_bytes, options)
                    .await?;
            } else {
                let overlap = unsafe { array_subset.overlap_unchecked(&chunk_subset_in_array) };
                let chunk_subset_in_array_subset =
                    unsafe { overlap.relative_to_unchecked(array_subset.start()) };
                let chunk_subset_bytes = unsafe {
                    chunk_subset_in_array_subset.extract_bytes_unchecked(
                        &subset_bytes,
                        array_subset.shape(),
                        self.data_type().size(),
                    )
                };

                // Store the chunk subset
                let array_subset_in_chunk_subset =
                    unsafe { overlap.relative_to_unchecked(chunk_subset_in_array.start()) };
                self.async_store_chunk_subset_opt(
                    chunk_indices,
                    &array_subset_in_chunk_subset,
                    chunk_subset_bytes,
                    options,
                )
                .await?;
            }
        } else {
            // Calculate chunk/codec concurrency
            let chunk_representation =
                self.chunk_array_representation(&vec![0; self.dimensionality()])?;
            let codec_concurrency = self.recommended_codec_concurrency(&chunk_representation)?;
            let (chunk_concurrent_limit, options) = concurrency_chunks_and_codec(
                options.concurrent_target(),
                num_chunks,
                &codec_concurrency,
            );

            let store_chunk = |chunk_indices: Vec<u64>| {
                let chunk_subset_in_array = unsafe {
                    self.chunk_grid()
                        .subset_unchecked(&chunk_indices, self.shape())
                        .unwrap()
                };
                let overlap = unsafe { array_subset.overlap_unchecked(&chunk_subset_in_array) };
                let chunk_subset_in_array_subset =
                    unsafe { overlap.relative_to_unchecked(array_subset.start()) };
                let array_subset_in_chunk_subset =
                    unsafe { overlap.relative_to_unchecked(chunk_subset_in_array.start()) };
                let chunk_subset_bytes = unsafe {
                    chunk_subset_in_array_subset.extract_bytes_unchecked(
                        &subset_bytes,
                        array_subset.shape(),
                        self.data_type().size(),
                    )
                };
                let options = options.clone();
                async move {
                    self.async_store_chunk_subset_opt(
                        &chunk_indices,
                        &array_subset_in_chunk_subset,
                        chunk_subset_bytes,
                        &options,
                    )
                    .await
                }
            };

            let indices = chunks.indices();
            let futures = indices.into_iter().map(store_chunk);
            let mut stream =
                futures::stream::iter(futures).buffer_unordered(chunk_concurrent_limit);
            while let Some(item) = stream.next().await {
                item?;
            }
        }
        Ok(())
    }

    /// Encode `subset_elements` and store in `array_subset`.
    ///
    /// Prefer to use [`store_chunk`](Array<WritableStorageTraits>::store_chunk) since this will decode and encode each chunk intersecting `array_subset`.
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if
    ///  - the size of `T` does not match the data type size, or
    ///  - a [`store_array_subset`](Array::store_array_subset) error condition is met.
    pub async fn async_store_array_subset_elements_opt<T: bytemuck::Pod + Send + Sync>(
        &self,
        array_subset: &ArraySubset,
        subset_elements: Vec<T>,
        options: &CodecOptions,
    ) -> Result<(), ArrayError> {
        array_async_store_elements!(
            self,
            subset_elements,
            async_store_array_subset_opt(array_subset, subset_elements, options)
        )
    }

    #[cfg(feature = "ndarray")]
    /// Encode `subset_array` and store in the array subset starting at `subset_start`.
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if a [`store_array_subset_elements`](Array::store_array_subset_elements) error condition is met.
    #[allow(clippy::missing_panics_doc)]
    pub async fn async_store_array_subset_ndarray_opt<T: bytemuck::Pod + Send + Sync>(
        &self,
        subset_start: &[u64],
        subset_array: &ndarray::ArrayViewD<'_, T>,
        options: &CodecOptions,
    ) -> Result<(), ArrayError> {
        let subset = ArraySubset::new_with_start_shape(
            subset_start.to_vec(),
            subset_array.shape().iter().map(|u| *u as u64).collect(),
        )?;
        array_async_store_ndarray!(
            self,
            subset_array,
            async_store_array_subset_elements_opt(&subset, subset_array, options)
        )
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
    pub async fn async_store_chunk_subset_opt(
        &self,
        chunk_indices: &[u64],
        chunk_subset: &ArraySubset,
        chunk_subset_bytes: Vec<u8>,
        options: &CodecOptions,
    ) -> Result<(), ArrayError> {
        let chunk_shape = self
            .chunk_grid()
            .chunk_shape_u64(chunk_indices, self.shape())?
            .ok_or_else(|| ArrayError::InvalidChunkGridIndicesError(chunk_indices.to_vec()))?;
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
            self.async_store_chunk_opt(chunk_indices, chunk_subset_bytes, options)
                .await
        } else {
            // Lock the chunk
            let key = data_key(self.path(), chunk_indices, self.chunk_key_encoding());
            let mutex = self.storage.mutex(&key).await?;
            let _lock = mutex.lock();

            // Decode the entire chunk
            let mut chunk_bytes = self
                .async_retrieve_chunk_opt(chunk_indices, options)
                .await?;

            // Update the intersecting subset of the chunk
            let element_size = self.data_type().size() as u64;
            let mut offset = 0;
            let contiguous_indices =
                unsafe { chunk_subset.contiguous_linearised_indices_unchecked(&chunk_shape) };
            let length =
                usize::try_from(contiguous_indices.contiguous_elements() * element_size).unwrap();
            for (chunk_element_index, _num_elements) in &contiguous_indices {
                let chunk_offset = usize::try_from(chunk_element_index * element_size).unwrap();
                debug_assert!(chunk_offset + length <= chunk_bytes.len());
                debug_assert!(offset + length <= chunk_subset_bytes.len());
                chunk_bytes[chunk_offset..chunk_offset + length]
                    .copy_from_slice(&chunk_subset_bytes[offset..offset + length]);
                offset += length;
            }

            // Store the updated chunk
            self.async_store_chunk_opt(chunk_indices, chunk_bytes, options)
                .await
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
    pub async fn async_store_chunk_subset_elements_opt<T: bytemuck::Pod + Send + Sync>(
        &self,
        chunk_indices: &[u64],
        chunk_subset: &ArraySubset,
        chunk_subset_elements: Vec<T>,
        options: &CodecOptions,
    ) -> Result<(), ArrayError> {
        array_async_store_elements!(
            self,
            chunk_subset_elements,
            async_store_chunk_subset_opt(
                chunk_indices,
                chunk_subset,
                chunk_subset_elements,
                options
            )
        )
    }

    #[cfg(feature = "ndarray")]
    /// Encode `chunk_subset_array` and store in `chunk_subset` of the chunk in the subset starting at `chunk_subset_start`.
    ///
    /// Prefer to use [`store_chunk`](Array<WritableStorageTraits>::store_chunk) since this will decode the chunk before updating it and reencoding it.
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if a [`store_chunk_subset_elements`](Array::store_chunk_subset_elements) error condition is met.
    #[allow(clippy::missing_panics_doc)]
    pub async fn async_store_chunk_subset_ndarray_opt<T: bytemuck::Pod + Send + Sync>(
        &self,
        chunk_indices: &[u64],
        chunk_subset_start: &[u64],
        chunk_subset_array: &ndarray::ArrayViewD<'_, T>,
        options: &CodecOptions,
    ) -> Result<(), ArrayError> {
        let subset = ArraySubset::new_with_start_shape(
            chunk_subset_start.to_vec(),
            chunk_subset_array
                .shape()
                .iter()
                .map(|u| *u as u64)
                .collect(),
        )?;
        array_async_store_ndarray!(
            self,
            chunk_subset_array,
            async_store_chunk_subset_elements_opt(
                chunk_indices,
                &subset,
                chunk_subset_array,
                options
            )
        )
    }
}
