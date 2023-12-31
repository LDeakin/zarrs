use super::TriviallyTransmutable;
use futures::{stream::FuturesUnordered, StreamExt};

use crate::{
    array_subset::ArraySubset,
    storage::{data_key, AsyncReadableWritableStorageTraits},
};

use super::{safe_transmute_to_bytes_vec, Array, ArrayError};

impl<TStorage: ?Sized + AsyncReadableWritableStorageTraits> Array<TStorage> {
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
    pub async fn async_store_array_subset(
        &self,
        array_subset: &ArraySubset,
        subset_bytes: Vec<u8>,
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
                self.async_store_chunk(chunk_indices, subset_bytes).await?;
            } else {
                let overlap = unsafe { array_subset.overlap_unchecked(&chunk_subset_in_array) };
                let chunk_subset_in_array_subset =
                    unsafe { overlap.relative_to_unchecked(array_subset.start()) };
                let chunk_subset_bytes = unsafe {
                    chunk_subset_in_array_subset.extract_bytes_unchecked(
                        &subset_bytes,
                        array_subset.shape(),
                        element_size,
                    )
                };

                // Store the chunk subset
                let array_subset_in_chunk_subset =
                    unsafe { overlap.relative_to_unchecked(chunk_subset_in_array.start()) };
                self.async_store_chunk_subset(
                    chunk_indices,
                    &array_subset_in_chunk_subset,
                    chunk_subset_bytes,
                )
                .await?;
            }
        } else {
            let chunks_to_update = chunks
                .iter_indices()
                .map(|chunk_indices| {
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
                    (
                        chunk_indices,
                        chunk_subset_in_array_subset,
                        array_subset_in_chunk_subset,
                    )
                })
                .collect::<Vec<_>>();
            let mut futures = chunks_to_update
                .iter()
                .map(
                    |(
                        chunk_indices,
                        chunk_subset_in_array_subset,
                        array_subset_in_chunk_subset,
                    )| {
                        let chunk_subset_bytes = unsafe {
                            chunk_subset_in_array_subset.extract_bytes_unchecked(
                                &subset_bytes,
                                array_subset.shape(),
                                element_size,
                            )
                        };
                        self.async_store_chunk_subset(
                            chunk_indices,
                            array_subset_in_chunk_subset,
                            chunk_subset_bytes,
                        )
                    },
                )
                .collect::<FuturesUnordered<_>>();
            while let Some(item) = futures.next().await {
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
    pub async fn async_store_array_subset_elements<T: TriviallyTransmutable + Send>(
        &self,
        array_subset: &ArraySubset,
        subset_elements: Vec<T>,
    ) -> Result<(), ArrayError> {
        array_async_store_elements!(
            self,
            subset_elements,
            async_store_array_subset(array_subset, subset_elements)
        )
    }

    #[cfg(feature = "ndarray")]
    /// Encode `subset_array` and store in the array subset starting at `subset_start`.
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if a [`store_array_subset_elements`](Array::store_array_subset_elements) error condition is met.
    #[allow(clippy::missing_panics_doc)]
    pub async fn async_store_array_subset_ndarray<T: TriviallyTransmutable + Send + Sync>(
        &self,
        subset_start: &[u64],
        subset_array: &ndarray::ArrayViewD<'_, T>,
    ) -> Result<(), ArrayError> {
        let subset = ArraySubset::new_with_start_shape(
            subset_start.to_vec(),
            subset_array.shape().iter().map(|u| *u as u64).collect(),
        )?;
        array_async_store_ndarray!(
            self,
            subset_array,
            async_store_array_subset_elements(&subset, subset_array)
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
    pub async fn async_store_chunk_subset(
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
                self.async_store_chunk(chunk_indices, chunk_subset_bytes)
                    .await
            } else {
                // Lock the chunk
                let key = data_key(self.path(), chunk_indices, self.chunk_key_encoding());
                let mutex = self.storage.mutex(&key).await?;
                let _lock = mutex.lock();

                // Decode the entire chunk
                let mut chunk_bytes = self.async_retrieve_chunk(chunk_indices).await?;

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
                self.async_store_chunk(chunk_indices, chunk_bytes.into_vec())
                    .await
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
    pub async fn async_store_chunk_subset_elements<T: TriviallyTransmutable + Send + Sync>(
        &self,
        chunk_indices: &[u64],
        chunk_subset: &ArraySubset,
        chunk_subset_elements: Vec<T>,
    ) -> Result<(), ArrayError> {
        array_async_store_elements!(
            self,
            chunk_subset_elements,
            async_store_chunk_subset(chunk_indices, chunk_subset, chunk_subset_elements)
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
    pub async fn async_store_chunk_subset_ndarray<T: TriviallyTransmutable + Send + Sync>(
        &self,
        chunk_indices: &[u64],
        chunk_subset_start: &[u64],
        chunk_subset_array: &ndarray::ArrayViewD<'_, T>,
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
            async_store_chunk_subset_elements(chunk_indices, &subset, chunk_subset_array)
        )
    }
}
