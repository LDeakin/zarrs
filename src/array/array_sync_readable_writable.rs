use rayon::iter::{IntoParallelIterator, ParallelIterator};

use crate::{
    array_subset::ArraySubset,
    storage::{data_key, ReadableWritableStorageTraits},
};

use super::{unravel_index, Array, ArrayError};

impl<TStorage: ?Sized + ReadableWritableStorageTraits> Array<TStorage> {
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
    #[allow(clippy::missing_panics_doc)]
    pub fn store_array_subset_opt(
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

    /// Serial version of [`Array::store_array_subset_opt`].
    #[allow(clippy::missing_panics_doc, clippy::missing_errors_doc)]
    pub fn store_array_subset(
        &self,
        array_subset: &ArraySubset,
        subset_bytes: Vec<u8>,
    ) -> Result<(), ArrayError> {
        self.store_array_subset_opt(array_subset, subset_bytes, false)
    }

    /// Parallel version of [`Array::store_array_subset_opt`].
    #[allow(clippy::missing_panics_doc, clippy::missing_errors_doc)]
    pub fn par_store_array_subset(
        &self,
        array_subset: &ArraySubset,
        subset_bytes: Vec<u8>,
    ) -> Result<(), ArrayError> {
        self.store_array_subset_opt(array_subset, subset_bytes, true)
    }

    /// Encode `subset_elements` and store in `array_subset`.
    ///
    /// Prefer to use [`store_chunk`](Array<WritableStorageTraits>::store_chunk) since this will decode and encode each chunk intersecting `array_subset`.
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if
    ///  - the size of `T` does not match the data type size, or
    ///  - a [`store_array_subset`](Array::store_array_subset) error condition is met.
    pub fn store_array_subset_elements_opt<T: bytemuck::Pod>(
        &self,
        array_subset: &ArraySubset,
        subset_elements: Vec<T>,
        parallel: bool,
    ) -> Result<(), ArrayError> {
        array_store_elements!(
            self,
            subset_elements,
            store_array_subset_opt(array_subset, subset_elements, parallel)
        )
    }

    /// Serial version of [`Array::store_array_subset_elements_opt`].
    #[allow(clippy::missing_panics_doc, clippy::missing_errors_doc)]
    pub fn store_array_subset_elements<T: bytemuck::Pod>(
        &self,
        array_subset: &ArraySubset,
        subset_elements: Vec<T>,
    ) -> Result<(), ArrayError> {
        self.store_array_subset_elements_opt(array_subset, subset_elements, false)
    }

    /// Parallel version of [`Array::store_array_subset_elements_opt`].
    #[allow(clippy::missing_panics_doc, clippy::missing_errors_doc)]
    pub fn par_store_array_subset_elements<T: bytemuck::Pod>(
        &self,
        array_subset: &ArraySubset,
        subset_elements: Vec<T>,
    ) -> Result<(), ArrayError> {
        self.store_array_subset_elements_opt(array_subset, subset_elements, true)
    }

    #[cfg(feature = "ndarray")]
    /// Encode `subset_array` and store in the array subset starting at `subset_start`.
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if a [`store_array_subset_elements`](Array::store_array_subset_elements) error condition is met.
    #[allow(clippy::missing_panics_doc)]
    pub fn store_array_subset_ndarray_opt<T: bytemuck::Pod>(
        &self,
        subset_start: &[u64],
        subset_array: &ndarray::ArrayViewD<T>,
        parallel: bool,
    ) -> Result<(), ArrayError> {
        let subset = ArraySubset::new_with_start_shape(
            subset_start.to_vec(),
            subset_array.shape().iter().map(|u| *u as u64).collect(),
        )?;
        array_store_ndarray!(
            self,
            subset_array,
            store_array_subset_elements_opt(&subset, subset_array, parallel)
        )
    }

    #[cfg(feature = "ndarray")]
    /// Serial version of [`Array::store_array_subset_ndarray_opt`].
    #[allow(clippy::missing_panics_doc, clippy::missing_errors_doc)]
    pub fn store_array_subset_ndarray<T: bytemuck::Pod>(
        &self,
        subset_start: &[u64],
        subset_array: &ndarray::ArrayViewD<T>,
    ) -> Result<(), ArrayError> {
        self.store_array_subset_ndarray_opt(subset_start, subset_array, false)
    }

    #[cfg(feature = "ndarray")]
    /// Parallel version of [`Array::store_array_subset_ndarray_opt`].
    #[allow(clippy::missing_panics_doc, clippy::missing_errors_doc)]
    pub fn par_store_array_subset_ndarray<T: bytemuck::Pod>(
        &self,
        subset_start: &[u64],
        subset_array: &ndarray::ArrayViewD<T>,
    ) -> Result<(), ArrayError> {
        self.store_array_subset_ndarray_opt(subset_start, subset_array, true)
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
        let chunk_shape = self
            .chunk_grid()
            .chunk_shape_u64(chunk_indices, self.shape())?
            .ok_or_else(|| ArrayError::InvalidChunkGridIndicesError(chunk_indices.to_vec()))?;

        // Validation
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
            let key = data_key(self.path(), chunk_indices, self.chunk_key_encoding());
            let mutex = self.storage.mutex(&key)?;
            let _lock = mutex.lock();

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
            self.store_chunk(chunk_indices, chunk_bytes)
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
    pub fn store_chunk_subset_elements<T: bytemuck::Pod>(
        &self,
        chunk_indices: &[u64],
        chunk_subset: &ArraySubset,
        chunk_subset_elements: Vec<T>,
    ) -> Result<(), ArrayError> {
        array_store_elements!(
            self,
            chunk_subset_elements,
            store_chunk_subset(chunk_indices, chunk_subset, chunk_subset_elements)
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
    pub fn store_chunk_subset_ndarray<T: bytemuck::Pod>(
        &self,
        chunk_indices: &[u64],
        chunk_subset_start: &[u64],
        chunk_subset_array: &ndarray::ArrayViewD<T>,
    ) -> Result<(), ArrayError> {
        let subset = ArraySubset::new_with_start_shape(
            chunk_subset_start.to_vec(),
            chunk_subset_array
                .shape()
                .iter()
                .map(|u| *u as u64)
                .collect(),
        )?;
        array_store_ndarray!(
            self,
            chunk_subset_array,
            store_chunk_subset_elements(chunk_indices, &subset, chunk_subset_array)
        )
    }
}
