use rayon::iter::{IntoParallelIterator, ParallelIterator};

use crate::{
    array::validate_element_size, array_subset::ArraySubset, storage::ReadableWritableStorageTraits,
};

use super::{
    codec::options::CodecOptions, concurrency::concurrency_chunks_and_codec, Array, ArrayError,
};

impl<TStorage: ?Sized + ReadableWritableStorageTraits + 'static> Array<TStorage> {
    /// Encode `chunk_subset_bytes` and store in `chunk_subset` of the chunk at `chunk_indices` with default codec options.
    ///
    /// Use [`store_chunk_subset_opt`](Array::store_chunk_subset_opt) to control codec options.
    /// Prefer to use [`store_chunk`](Array::store_chunk) where possible, since this function may decode the chunk before updating it and reencoding it.
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if
    ///  - `chunk_subset` is invalid or out of bounds of the chunk,
    ///  - there is a codec encoding error, or
    ///  - an underlying store error.
    ///
    /// # Panics
    /// Panics if attempting to reference a byte beyond `usize::MAX`.
    #[allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]
    pub fn store_chunk_subset(
        &self,
        chunk_indices: &[u64],
        chunk_subset: &ArraySubset,
        chunk_subset_bytes: &[u8],
    ) -> Result<(), ArrayError> {
        self.store_chunk_subset_opt(
            chunk_indices,
            chunk_subset,
            chunk_subset_bytes,
            &CodecOptions::default(),
        )
    }

    /// Encode `chunk_subset_elements` and store in `chunk_subset` of the chunk at `chunk_indices` with default codec options.
    ///
    /// Use [`store_chunk_subset_elements_opt`](Array::store_chunk_subset_elements_opt) to control codec options.
    /// Prefer to use [`store_chunk_elements`](Array::store_chunk_elements) where possible, since this will decode the chunk before updating it and reencoding it.
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if
    ///  - the size of  `T` does not match the data type size, or
    ///  - a [`store_chunk_subset`](Array::store_chunk_subset) error condition is met.
    #[allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]
    pub fn store_chunk_subset_elements<T: bytemuck::Pod>(
        &self,
        chunk_indices: &[u64],
        chunk_subset: &ArraySubset,
        chunk_subset_elements: &[T],
    ) -> Result<(), ArrayError> {
        self.store_chunk_subset_elements_opt(
            chunk_indices,
            chunk_subset,
            chunk_subset_elements,
            &CodecOptions::default(),
        )
    }

    #[cfg(feature = "ndarray")]
    /// Encode `chunk_subset_array` and store in `chunk_subset` of the chunk in the subset starting at `chunk_subset_start`.
    ///
    /// Use [`store_chunk_subset_ndarray_opt`](Array::store_chunk_subset_ndarray_opt) to control codec options.
    /// Prefer to use [`store_chunk_ndarray`](Array::store_chunk_ndarray) where possible, since this will decode the chunk before updating it and reencoding it.
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if a [`store_chunk_subset_elements`](Array::store_chunk_subset_elements) error condition is met.
    pub fn store_chunk_subset_ndarray<
        T: bytemuck::Pod,
        TArray: Into<ndarray::Array<T, D>>,
        D: ndarray::Dimension,
    >(
        &self,
        chunk_indices: &[u64],
        chunk_subset_start: &[u64],
        chunk_subset_array: TArray,
    ) -> Result<(), ArrayError> {
        self.store_chunk_subset_ndarray_opt(
            chunk_indices,
            chunk_subset_start,
            chunk_subset_array,
            &CodecOptions::default(),
        )
    }

    /// Encode `subset_bytes` and store in `array_subset`.
    ///
    /// Use [`store_array_subset_opt`](Array::store_array_subset_opt) to control codec options.
    /// Prefer to use [`store_chunk`](Array::store_chunk) or [`store_chunks`](Array::store_chunks) where possible, since this will decode and encode each chunk intersecting `array_subset`.
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if
    ///  - the dimensionality of `array_subset` does not match the chunk grid dimensionality
    ///  - the length of `subset_bytes` does not match the expected length governed by the shape of the array subset and the data type size,
    ///  - there is a codec encoding error, or
    ///  - an underlying store error.
    #[allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]
    pub fn store_array_subset(
        &self,
        array_subset: &ArraySubset,
        subset_bytes: &[u8],
    ) -> Result<(), ArrayError> {
        self.store_array_subset_opt(array_subset, subset_bytes, &CodecOptions::default())
    }

    /// Encode `subset_elements` and store in `array_subset`.
    ///
    /// Use [`store_array_subset_elements_opt`](Array::store_array_subset_elements_opt) to control codec options.
    /// Prefer to use [`store_chunk_elements`](Array::store_chunk_elements) or [`store_chunks_elements`](Array::store_chunks_elements) where possible, since this will decode and encode each chunk intersecting `array_subset`.
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if
    ///  - the size of `T` does not match the data type size, or
    ///  - a [`store_array_subset`](Array::store_array_subset) error condition is met.
    #[allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]
    pub fn store_array_subset_elements<T: bytemuck::Pod>(
        &self,
        array_subset: &ArraySubset,
        subset_elements: &[T],
    ) -> Result<(), ArrayError> {
        self.store_array_subset_elements_opt(
            array_subset,
            subset_elements,
            &CodecOptions::default(),
        )
    }

    #[cfg(feature = "ndarray")]
    /// Encode `subset_array` and store in the array subset starting at `subset_start`.
    ///
    /// Use [`store_array_subset_ndarray_opt`](Array::store_array_subset_ndarray_opt) to control codec options.
    /// Prefer to use [`store_chunk_ndarray`](Array::store_chunk_ndarray) or [`store_chunks_ndarray`](Array::store_chunks_ndarray) where possible, since this will decode and encode each chunk intersecting `array_subset`.
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if a [`store_array_subset_elements`](Array::store_array_subset_elements) error condition is met.
    #[allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]
    pub fn store_array_subset_ndarray<
        T: bytemuck::Pod,
        TArray: Into<ndarray::Array<T, D>>,
        D: ndarray::Dimension,
    >(
        &self,
        subset_start: &[u64],
        subset_array: TArray,
    ) -> Result<(), ArrayError> {
        self.store_array_subset_ndarray_opt(subset_start, subset_array, &CodecOptions::default())
    }

    /////////////////////////////////////////////////////////////////////////////
    // Advanced methods
    /////////////////////////////////////////////////////////////////////////////

    /// Explicit options version of [`store_chunk_subset`](Array::store_chunk_subset).
    #[allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]
    pub fn store_chunk_subset_opt(
        &self,
        chunk_indices: &[u64],
        chunk_subset: &ArraySubset,
        chunk_subset_bytes: &[u8],
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
            self.store_chunk_opt(chunk_indices, chunk_subset_bytes, options)
        } else {
            // Lock the chunk
            // let key = data_key(self.path(), chunk_indices, self.chunk_key_encoding());
            // let mutex = self.storage.mutex(&key)?;
            // let _lock = mutex.lock();

            // Decode the entire chunk
            let mut chunk_bytes = self.retrieve_chunk_opt(chunk_indices, options)?;

            // Update the intersecting subset of the chunk
            let element_size = self.data_type().size();
            let mut offset = 0;
            let contiguous_indices =
                unsafe { chunk_subset.contiguous_linearised_indices_unchecked(&chunk_shape) };
            let length = contiguous_indices.contiguous_elements_usize() * element_size;
            // FIXME: Par iter?
            for (chunk_element_index, _num_elements) in &contiguous_indices {
                let chunk_offset = usize::try_from(chunk_element_index).unwrap() * element_size;
                debug_assert!(chunk_offset + length <= chunk_bytes.len());
                debug_assert!(offset + length <= chunk_subset_bytes.len());
                chunk_bytes[chunk_offset..chunk_offset + length]
                    .copy_from_slice(&chunk_subset_bytes[offset..offset + length]);
                offset += length;
            }

            // Store the updated chunk
            self.store_chunk_opt(chunk_indices, &chunk_bytes, options)
        }
    }

    /// Explicit options version of [`store_chunk_subset_elements`](Array::store_chunk_subset_elements).
    #[allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]
    pub fn store_chunk_subset_elements_opt<T: bytemuck::Pod>(
        &self,
        chunk_indices: &[u64],
        chunk_subset: &ArraySubset,
        chunk_subset_elements: &[T],
        options: &CodecOptions,
    ) -> Result<(), ArrayError> {
        validate_element_size::<T>(self.data_type())?;
        let chunk_subset_elements = crate::array::transmute_to_bytes(chunk_subset_elements);
        self.store_chunk_subset_opt(chunk_indices, chunk_subset, chunk_subset_elements, options)
    }

    #[cfg(feature = "ndarray")]
    /// Explicit options version of [`store_chunk_subset_ndarray`](Array::store_chunk_subset_ndarray).
    #[allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]
    pub fn store_chunk_subset_ndarray_opt<
        T: bytemuck::Pod,
        TArray: Into<ndarray::Array<T, D>>,
        D: ndarray::Dimension,
    >(
        &self,
        chunk_indices: &[u64],
        chunk_subset_start: &[u64],
        chunk_subset_array: TArray,
        options: &CodecOptions,
    ) -> Result<(), ArrayError> {
        validate_element_size::<T>(self.data_type())?;
        let chunk_subset_array: ndarray::Array<T, D> = chunk_subset_array.into();
        let subset = ArraySubset::new_with_start_shape(
            chunk_subset_start.to_vec(),
            chunk_subset_array
                .shape()
                .iter()
                .map(|u| *u as u64)
                .collect(),
        )?;
        let chunk_subset_array = super::ndarray_into_vec(chunk_subset_array);
        self.store_chunk_subset_elements_opt(chunk_indices, &subset, &chunk_subset_array, options)
    }

    /// Explicit options version of [`store_array_subset`](Array::store_array_subset).
    #[allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]
    pub fn store_array_subset_opt(
        &self,
        array_subset: &ArraySubset,
        subset_bytes: &[u8],
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
                self.store_chunk_opt(chunk_indices, subset_bytes, options)?;
            } else {
                let overlap = unsafe { array_subset.overlap_unchecked(&chunk_subset_in_array) };
                let chunk_subset_in_array_subset =
                    unsafe { overlap.relative_to_unchecked(array_subset.start()) };
                let chunk_subset_bytes = unsafe {
                    chunk_subset_in_array_subset.extract_bytes_unchecked(
                        subset_bytes,
                        array_subset.shape(),
                        self.data_type().size(),
                    )
                };

                // Store the chunk subset
                let array_subset_in_chunk_subset =
                    unsafe { overlap.relative_to_unchecked(chunk_subset_in_array.start()) };
                self.store_chunk_subset_opt(
                    chunk_indices,
                    &array_subset_in_chunk_subset,
                    &chunk_subset_bytes,
                    options,
                )?;
            }
        } else {
            // Calculate chunk/codec concurrency
            let chunk_representation =
                self.chunk_array_representation(&vec![0; self.dimensionality()])?;
            let codec_concurrency = self.recommended_codec_concurrency(&chunk_representation)?;
            let (chunk_concurrent_limit, options) = concurrency_chunks_and_codec(
                options.concurrent_target(),
                num_chunks,
                options,
                &codec_concurrency,
            );

            let store_chunk = |chunk_indices: Vec<u64>| -> Result<(), ArrayError> {
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
                        subset_bytes,
                        array_subset.shape(),
                        self.data_type().size(),
                    )
                };
                self.store_chunk_subset_opt(
                    &chunk_indices,
                    &array_subset_in_chunk_subset,
                    &chunk_subset_bytes,
                    &options,
                )
            };

            let indices = chunks.indices();
            rayon_iter_concurrent_limit::iter_concurrent_limit!(
                chunk_concurrent_limit,
                indices,
                try_for_each,
                store_chunk
            )?;
        }
        Ok(())
    }

    /// Explicit options version of [`store_array_subset_elements`](Array::store_array_subset_elements).
    #[allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]
    pub fn store_array_subset_elements_opt<T: bytemuck::Pod>(
        &self,
        array_subset: &ArraySubset,
        subset_elements: &[T],
        options: &CodecOptions,
    ) -> Result<(), ArrayError> {
        validate_element_size::<T>(self.data_type())?;
        let subset_elements = crate::array::transmute_to_bytes(subset_elements);
        self.store_array_subset_opt(array_subset, subset_elements, options)
    }

    #[cfg(feature = "ndarray")]
    /// Explicit options version of [`store_array_subset_ndarray`](Array::store_array_subset_ndarray).
    #[allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]
    pub fn store_array_subset_ndarray_opt<
        T: bytemuck::Pod,
        TArray: Into<ndarray::Array<T, D>>,
        D: ndarray::Dimension,
    >(
        &self,
        subset_start: &[u64],
        subset_array: TArray,
        options: &CodecOptions,
    ) -> Result<(), ArrayError> {
        validate_element_size::<T>(self.data_type())?;
        let subset_array: ndarray::Array<T, D> = subset_array.into();
        let subset = ArraySubset::new_with_start_shape(
            subset_start.to_vec(),
            subset_array.shape().iter().map(|u| *u as u64).collect(),
        )?;
        let subset_array = super::ndarray_into_vec(subset_array);
        self.store_array_subset_elements_opt(&subset, &subset_array, options)
    }
}
