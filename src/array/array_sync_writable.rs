use std::sync::Arc;

use rayon::iter::{IntoParallelIterator, ParallelIterator};

use crate::{
    array_subset::ArraySubset,
    storage::{StorageError, StorageHandle, WritableStorageTraits},
};

use super::{
    codec::{ArrayCodecTraits, EncodeOptions},
    unravel_index, Array, ArrayError,
};

impl<TStorage: ?Sized + WritableStorageTraits + 'static> Array<TStorage> {
    /// Store metadata.
    ///
    /// # Errors
    /// Returns [`StorageError`] if there is an underlying store error.
    pub fn store_metadata(&self) -> Result<(), StorageError> {
        let storage_handle = Arc::new(StorageHandle::new(self.storage.clone()));
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
    pub fn store_chunk_opt(
        &self,
        chunk_indices: &[u64],
        chunk_bytes: Vec<u8>,
        options: &EncodeOptions,
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
            let storage_handle = Arc::new(StorageHandle::new(self.storage.clone()));
            let storage_transformer = self
                .storage_transformers()
                .create_writable_transformer(storage_handle);
            let chunk_encoded: Vec<u8> = self
                .codecs()
                .encode_opt(chunk_bytes, &chunk_array_representation, options)
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

    /// Encode `chunk_bytes` and store at `chunk_indices` (default options).    
    #[allow(clippy::missing_panics_doc, clippy::missing_errors_doc)]
    pub fn store_chunk(
        &self,
        chunk_indices: &[u64],
        chunk_bytes: Vec<u8>,
    ) -> Result<(), ArrayError> {
        self.store_chunk_opt(chunk_indices, chunk_bytes, &EncodeOptions::default())
    }

    /// Encode `chunk_elements` and store at `chunk_indices`.
    ///
    /// A chunk composed entirely of the fill value will not be written to the store.
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if
    ///  - the size of  `T` does not match the data type size, or
    ///  - a [`store_chunk`](Array::store_chunk) error condition is met.
    pub fn store_chunk_elements_opt<T: bytemuck::Pod>(
        &self,
        chunk_indices: &[u64],
        chunk_elements: Vec<T>,
        options: &EncodeOptions,
    ) -> Result<(), ArrayError> {
        array_store_elements!(
            self,
            chunk_elements,
            store_chunk_opt(chunk_indices, chunk_elements, options)
        )
    }

    /// Encode `chunk_elements` and store at `chunk_indices` (default options).
    #[allow(clippy::missing_panics_doc, clippy::missing_errors_doc)]
    pub fn store_chunk_elements<T: bytemuck::Pod>(
        &self,
        chunk_indices: &[u64],
        chunk_elements: Vec<T>,
    ) -> Result<(), ArrayError> {
        self.store_chunk_elements_opt(chunk_indices, chunk_elements, &EncodeOptions::default())
    }

    #[cfg(feature = "ndarray")]
    /// Encode `chunk_array` and store at `chunk_indices`.
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if
    ///  - the size of `T` does not match the size of the data type,
    ///  - a [`store_chunk_elements`](Array::store_chunk_elements) error condition is met.
    #[allow(clippy::missing_panics_doc)]
    pub fn store_chunk_ndarray_opt<T: bytemuck::Pod>(
        &self,
        chunk_indices: &[u64],
        chunk_array: &ndarray::ArrayViewD<T>,
        options: &EncodeOptions,
    ) -> Result<(), ArrayError> {
        array_store_ndarray!(
            self,
            chunk_array,
            store_chunk_elements_opt(chunk_indices, chunk_array, options)
        )
    }

    #[cfg(feature = "ndarray")]
    /// Encode `chunk_array` and store at `chunk_indices` (default options).
    #[allow(clippy::missing_panics_doc, clippy::missing_errors_doc)]
    pub fn store_chunk_ndarray<T: bytemuck::Pod>(
        &self,
        chunk_indices: &[u64],
        chunk_array: &ndarray::ArrayViewD<T>,
    ) -> Result<(), ArrayError> {
        self.store_chunk_ndarray_opt(chunk_indices, chunk_array, &EncodeOptions::default())
    }

    /// Encode `chunks_bytes` and store at the chunks with indices represented by the `chunks` array subset.
    ///
    /// A chunk composed entirely of the fill value will not be written to the store.
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if
    ///  - `chunks` are invalid,
    ///  - the length of `chunk_bytes` is not equal to the expected length (the product of the number of elements in the chunks and the data type size in bytes),
    ///  - there is a codec encoding error, or
    ///  - an underlying store error.
    #[allow(clippy::similar_names)]
    pub fn store_chunks_opt(
        &self,
        chunks: &ArraySubset,
        chunks_bytes: Vec<u8>,
        options: &EncodeOptions,
    ) -> Result<(), ArrayError> {
        let num_chunks = chunks.num_elements_usize();
        if num_chunks == 1 {
            let chunk_indices = chunks.start();
            self.store_chunk_opt(chunk_indices, chunks_bytes, options)?;
        } else {
            let array_subset = self.chunks_subset(chunks)?;
            let element_size = self.data_type().size();
            let expected_size = element_size as u64 * array_subset.num_elements();
            if chunks_bytes.len() as u64 != expected_size {
                return Err(ArrayError::InvalidBytesInputSize(
                    chunks_bytes.len(),
                    expected_size,
                ));
            }

            let store_chunk = |chunk_indices: Vec<u64>| -> Result<(), ArrayError> {
                let chunk_subset_in_array = unsafe {
                    self.chunk_grid()
                        .subset_unchecked(&chunk_indices, self.shape())
                        .ok_or_else(|| {
                            ArrayError::InvalidChunkGridIndicesError(chunk_indices.clone())
                        })?
                };
                let overlap = unsafe { array_subset.overlap_unchecked(&chunk_subset_in_array) };
                let chunk_subset_in_array_subset =
                    unsafe { overlap.relative_to_unchecked(array_subset.start()) };
                #[allow(clippy::similar_names)]
                let chunk_bytes = unsafe {
                    chunk_subset_in_array_subset.extract_bytes_unchecked(
                        &chunks_bytes,
                        array_subset.shape(),
                        element_size,
                    )
                };

                debug_assert_eq!(
                    chunk_subset_in_array.num_elements(),
                    chunk_subset_in_array_subset.num_elements()
                );

                // Store the chunk
                self.store_chunk_opt(&chunk_indices, chunk_bytes, options)?;

                Ok(())
            };
            if options.is_parallel() {
                (0..chunks.shape().iter().product())
                    .into_par_iter()
                    .map(|chunk_index| {
                        std::iter::zip(unravel_index(chunk_index, chunks.shape()), chunks.start())
                            .map(|(chunk_indices, chunks_start)| chunk_indices + chunks_start)
                            .collect::<Vec<_>>()
                    })
                    .try_for_each(store_chunk)?;
            } else {
                for chunk_indices in chunks.iter_indices() {
                    store_chunk(chunk_indices)?;
                }
            }
        }

        Ok(())
    }

    /// Encode `chunks_bytes` and store at the chunks with indices represented by the `chunks` array subset (default options).
    #[allow(clippy::similar_names)]
    #[allow(clippy::missing_panics_doc, clippy::missing_errors_doc)]
    pub fn store_chunks(
        &self,
        chunks: &ArraySubset,
        chunks_bytes: Vec<u8>,
    ) -> Result<(), ArrayError> {
        self.store_chunks_opt(chunks, chunks_bytes, &EncodeOptions::default())
    }

    /// Variation of [`Array::store_chunks_opt`] for elements with a known type.
    ///
    /// # Errors
    /// In addition to [`Array::store_chunks_opt`] errors, returns an [`ArrayError`] if the size of `T` does not match the data type size.
    pub fn store_chunks_elements_opt<T: bytemuck::Pod>(
        &self,
        chunks: &ArraySubset,
        chunks_elements: Vec<T>,
        options: &EncodeOptions,
    ) -> Result<(), ArrayError> {
        array_store_elements!(
            self,
            chunks_elements,
            store_chunks_opt(chunks, chunks_elements, options)
        )
    }

    /// Variation of [`Array::store_chunks_opt`] for elements with a known type (default options).
    #[allow(clippy::missing_panics_doc, clippy::missing_errors_doc)]
    pub fn store_chunks_elements<T: bytemuck::Pod>(
        &self,
        chunks: &ArraySubset,
        chunks_elements: Vec<T>,
    ) -> Result<(), ArrayError> {
        self.store_chunks_elements_opt(chunks, chunks_elements, &EncodeOptions::default())
    }

    #[cfg(feature = "ndarray")]
    /// Variation of [`Array::store_chunks_opt`] for an [`ndarray::ArrayViewD`].
    ///
    /// # Errors
    /// In addition to [`Array::store_chunks_opt`] errors, returns an [`ArrayError`] if the size of `T` does not match the data type size.
    pub fn store_chunks_ndarray_opt<T: bytemuck::Pod>(
        &self,
        chunks: &ArraySubset,
        chunks_array: &ndarray::ArrayViewD<'_, T>,
        options: &EncodeOptions,
    ) -> Result<(), ArrayError> {
        array_store_ndarray!(
            self,
            chunks_array,
            store_chunks_elements_opt(chunks, chunks_array, options)
        )
    }

    #[cfg(feature = "ndarray")]
    /// Variation of [`Array::store_chunks_opt`] for an [`ndarray::ArrayViewD`] (default options).
    #[allow(clippy::missing_panics_doc, clippy::missing_errors_doc)]
    pub fn store_chunks_ndarray<T: bytemuck::Pod>(
        &self,
        chunks: &ArraySubset,
        chunks_array: &ndarray::ArrayViewD<'_, T>,
    ) -> Result<(), ArrayError> {
        self.store_chunks_ndarray_opt(chunks, chunks_array, &EncodeOptions::default())
    }

    /// Erase the chunk at `chunk_indices`.
    ///
    /// Succeeds if the chunk does not exist.
    ///
    /// # Errors
    /// Returns a [`StorageError`] if there is an underlying store error.
    pub fn erase_chunk(&self, chunk_indices: &[u64]) -> Result<(), StorageError> {
        let storage_handle = Arc::new(StorageHandle::new(self.storage.clone()));
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

    /// Erase the chunks in `chunks`.
    ///
    /// # Errors
    /// Returns a [`StorageError`] if there is an underlying store error.
    pub fn erase_chunks(&self, chunks: &ArraySubset) -> Result<(), StorageError> {
        let storage_handle = Arc::new(StorageHandle::new(self.storage.clone()));
        let storage_transformer = self
            .storage_transformers()
            .create_writable_transformer(storage_handle);
        (0..chunks.shape().iter().product())
            .into_par_iter()
            .map(|chunk_index| {
                std::iter::zip(unravel_index(chunk_index, chunks.shape()), chunks.start())
                    .map(|(chunk_indices, chunks_start)| chunk_indices + chunks_start)
                    .collect::<Vec<_>>()
            })
            // chunks
            // .iter_indices()
            // .par_bridge()
            .try_for_each(|chunk_indices| {
                crate::storage::erase_chunk(
                    &*storage_transformer,
                    self.path(),
                    &chunk_indices,
                    self.chunk_key_encoding(),
                )?;
                Ok::<_, StorageError>(())
            })
    }
}
