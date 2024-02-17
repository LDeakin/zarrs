use std::sync::Arc;

use futures::{stream::FuturesUnordered, StreamExt};

use crate::{
    array_subset::ArraySubset,
    node::NodePath,
    storage::{data_key, meta_key, AsyncReadableStorageTraits, StorageHandle},
};

use super::{
    codec::{
        ArrayCodecTraits, ArrayToBytesCodecTraits, AsyncArrayPartialDecoderTraits,
        AsyncStoragePartialDecoder, DecodeOptions, PartialDecoderOptions,
    },
    transmute_from_bytes_vec,
    unsafe_cell_slice::UnsafeCellSlice,
    validate_element_size, Array, ArrayCreateError, ArrayError, ArrayMetadata,
};

#[cfg(feature = "ndarray")]
use super::elements_to_ndarray;

impl<TStorage: ?Sized + AsyncReadableStorageTraits + 'static> Array<TStorage> {
    /// Create an array in `storage` at `path`. The metadata is read from the store.
    ///
    /// # Errors
    /// Returns [`ArrayCreateError`] if there is a storage error or any metadata is invalid.
    pub async fn async_new(storage: Arc<TStorage>, path: &str) -> Result<Self, ArrayCreateError> {
        let node_path = NodePath::new(path)?;
        let key = meta_key(&node_path);
        let metadata: ArrayMetadata = serde_json::from_slice(
            &storage
                .get(&key)
                .await?
                .ok_or(ArrayCreateError::MissingMetadata)?,
        )
        .map_err(|err| crate::storage::StorageError::InvalidMetadata(key, err.to_string()))?;
        Self::new_with_metadata(storage, path, metadata)
    }

    /// Read and decode the chunk at `chunk_indices` into its bytes if it exists.
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if
    ///  - `chunk_indices` are invalid,
    ///  - there is a codec decoding error, or
    ///  - an underlying store error.
    ///
    /// # Panics
    /// Panics if the number of elements in the chunk exceeds `usize::MAX`.
    pub async fn async_retrieve_chunk_if_exists_opt(
        &self,
        chunk_indices: &[u64],
        options: &DecodeOptions,
    ) -> Result<Option<Vec<u8>>, ArrayError> {
        let storage_handle = Arc::new(StorageHandle::new(self.storage.clone()));
        let storage_transformer = self
            .storage_transformers()
            .create_async_readable_transformer(storage_handle);
        let chunk_encoded = crate::storage::async_retrieve_chunk(
            &*storage_transformer,
            self.path(),
            chunk_indices,
            self.chunk_key_encoding(),
        )
        .await
        .map_err(ArrayError::StorageError)?;
        if let Some(chunk_encoded) = chunk_encoded {
            let chunk_representation = self.chunk_array_representation(chunk_indices)?;
            let chunk_decoded = self
                .codecs()
                .async_decode_opt(chunk_encoded, &chunk_representation, options)
                .await
                .map_err(ArrayError::CodecError)?;
            let chunk_decoded_size =
                chunk_representation.num_elements_usize() * chunk_representation.data_type().size();
            if chunk_decoded.len() == chunk_decoded_size {
                Ok(Some(chunk_decoded))
            } else {
                Err(ArrayError::UnexpectedChunkDecodedSize(
                    chunk_decoded.len(),
                    chunk_decoded_size,
                ))
            }
        } else {
            Ok(None)
        }
    }

    /// Read and decode the chunk at `chunk_indices` into its bytes if it exists (default options).
    #[allow(clippy::missing_panics_doc, clippy::missing_errors_doc)]
    pub async fn async_retrieve_chunk_if_exists(
        &self,
        chunk_indices: &[u64],
    ) -> Result<Option<Vec<u8>>, ArrayError> {
        self.async_retrieve_chunk_if_exists_opt(chunk_indices, &DecodeOptions::default())
            .await
    }

    /// Read and decode the chunk at `chunk_indices` into its bytes or the fill value if it does not exist.
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if
    ///  - `chunk_indices` are invalid,
    ///  - there is a codec decoding error, or
    ///  - an underlying store error.
    ///
    /// # Panics
    /// Panics if the number of elements in the chunk exceeds `usize::MAX`.
    pub async fn async_retrieve_chunk_opt(
        &self,
        chunk_indices: &[u64],
        options: &DecodeOptions,
    ) -> Result<Vec<u8>, ArrayError> {
        let chunk = self
            .async_retrieve_chunk_if_exists_opt(chunk_indices, options)
            .await?;
        if let Some(chunk) = chunk {
            Ok(chunk)
        } else {
            let chunk_representation = self.chunk_array_representation(chunk_indices)?;
            let fill_value = chunk_representation.fill_value().as_ne_bytes();
            Ok(fill_value.repeat(chunk_representation.num_elements_usize()))
        }
    }

    /// Read and decode the chunk at `chunk_indices` into its bytes or the fill value if it does not exist (default options).
    #[allow(clippy::missing_panics_doc, clippy::missing_errors_doc)]
    pub async fn async_retrieve_chunk(&self, chunk_indices: &[u64]) -> Result<Vec<u8>, ArrayError> {
        self.async_retrieve_chunk_opt(chunk_indices, &DecodeOptions::default())
            .await
    }

    /// Read and decode the chunk at `chunk_indices` into a vector of its elements if it exists.
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if
    ///  - the size of `T` does not match the data type size,
    ///  - the decoded bytes cannot be transmuted,
    ///  - `chunk_indices` are invalid,
    ///  - there is a codec decoding error, or
    ///  - an underlying store error.
    pub async fn async_retrieve_chunk_elements_if_exists_opt<T: bytemuck::Pod + Send + Sync>(
        &self,
        chunk_indices: &[u64],
        options: &DecodeOptions,
    ) -> Result<Option<Vec<T>>, ArrayError> {
        validate_element_size::<T>(self.data_type())?;
        let bytes = self
            .async_retrieve_chunk_if_exists_opt(chunk_indices, options)
            .await?;
        Ok(bytes.map(|bytes| transmute_from_bytes_vec::<T>(bytes)))
    }

    /// Read and decode the chunk at `chunk_indices` into a vector of its elements if it exists (default options).
    #[allow(clippy::missing_panics_doc, clippy::missing_errors_doc)]
    pub async fn async_retrieve_chunk_elements_if_exists<T: bytemuck::Pod + Send + Sync>(
        &self,
        chunk_indices: &[u64],
    ) -> Result<Option<Vec<T>>, ArrayError> {
        self.async_retrieve_chunk_elements_if_exists_opt(chunk_indices, &DecodeOptions::default())
            .await
    }

    /// Read and decode the chunk at `chunk_indices` into a vector of its elements or the fill value if it does not exist.
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if
    ///  - the size of `T` does not match the data type size,
    ///  - the decoded bytes cannot be transmuted,
    ///  - `chunk_indices` are invalid,
    ///  - there is a codec decoding error, or
    ///  - an underlying store error.
    pub async fn async_retrieve_chunk_elements_opt<T: bytemuck::Pod + Send + Sync>(
        &self,
        chunk_indices: &[u64],
        options: &DecodeOptions,
    ) -> Result<Vec<T>, ArrayError> {
        validate_element_size::<T>(self.data_type())?;
        let bytes = self
            .async_retrieve_chunk_opt(chunk_indices, options)
            .await?;
        Ok(transmute_from_bytes_vec::<T>(bytes))
    }

    /// Read and decode the chunk at `chunk_indices` into a vector of its elements or the fill value if it does not exist (default options).
    #[allow(clippy::missing_panics_doc, clippy::missing_errors_doc)]
    pub async fn async_retrieve_chunk_elements<T: bytemuck::Pod + Send + Sync>(
        &self,
        chunk_indices: &[u64],
    ) -> Result<Vec<T>, ArrayError> {
        self.async_retrieve_chunk_elements_opt(chunk_indices, &DecodeOptions::default())
            .await
    }

    #[cfg(feature = "ndarray")]
    /// Read and decode the chunk at `chunk_indices` into an [`ndarray::ArrayD`] if it exists.
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if:
    ///  - the size of `T` does not match the data type size,
    ///  - the decoded bytes cannot be transmuted,
    ///  - the chunk indices are invalid,
    ///  - there is a codec decoding error, or
    ///  - an underlying store error.
    ///
    /// # Panics
    /// Will panic if a chunk dimension is larger than `usize::MAX`.
    pub async fn async_retrieve_chunk_ndarray_if_exists_opt<T: bytemuck::Pod + Send + Sync>(
        &self,
        chunk_indices: &[u64],
        options: &DecodeOptions,
    ) -> Result<Option<ndarray::ArrayD<T>>, ArrayError> {
        // validate_element_size::<T>(self.data_type())?; in // async_retrieve_chunk_elements_if_exists
        let shape = self
            .chunk_grid()
            .chunk_shape_u64(chunk_indices, self.shape())?
            .ok_or_else(|| ArrayError::InvalidChunkGridIndicesError(chunk_indices.to_vec()))?;
        let elements = self
            .async_retrieve_chunk_elements_if_exists_opt(chunk_indices, options)
            .await?;
        if let Some(elements) = elements {
            Ok(Some(elements_to_ndarray(&shape, elements)?))
        } else {
            Ok(None)
        }
    }

    #[cfg(feature = "ndarray")]
    /// Read and decode the chunk at `chunk_indices` into an [`ndarray::ArrayD`] if it exists (default options).
    #[allow(clippy::missing_panics_doc, clippy::missing_errors_doc)]
    pub async fn async_retrieve_chunk_ndarray_if_exists<T: bytemuck::Pod + Send + Sync>(
        &self,
        chunk_indices: &[u64],
    ) -> Result<Option<ndarray::ArrayD<T>>, ArrayError> {
        self.async_retrieve_chunk_ndarray_if_exists_opt(chunk_indices, &DecodeOptions::default())
            .await
    }

    #[cfg(feature = "ndarray")]
    /// Read and decode the chunk at `chunk_indices` into an [`ndarray::ArrayD`]. It is filled with the fill value if it does not exist.
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if:
    ///  - the size of `T` does not match the data type size,
    ///  - the decoded bytes cannot be transmuted,
    ///  - the chunk indices are invalid,
    ///  - there is a codec decoding error, or
    ///  - an underlying store error.
    ///
    /// # Panics
    /// Will panic if a chunk dimension is larger than `usize::MAX`.
    pub async fn async_retrieve_chunk_ndarray_opt<T: bytemuck::Pod + Send + Sync>(
        &self,
        chunk_indices: &[u64],
        options: &DecodeOptions,
    ) -> Result<ndarray::ArrayD<T>, ArrayError> {
        // validate_element_size::<T>(self.data_type())?; // in async_retrieve_chunk_elements
        let shape = self
            .chunk_grid()
            .chunk_shape_u64(chunk_indices, self.shape())?
            .ok_or_else(|| ArrayError::InvalidChunkGridIndicesError(chunk_indices.to_vec()))?;
        let elements = self
            .async_retrieve_chunk_elements_opt(chunk_indices, options)
            .await?;
        elements_to_ndarray(&shape, elements)
    }

    #[cfg(feature = "ndarray")]
    /// Read and decode the chunk at `chunk_indices` into an [`ndarray::ArrayD`]. It is filled with the fill value if it does not exist (default options).
    #[allow(clippy::missing_panics_doc, clippy::missing_errors_doc)]
    pub async fn async_retrieve_chunk_ndarray<T: bytemuck::Pod + Send + Sync>(
        &self,
        chunk_indices: &[u64],
    ) -> Result<ndarray::ArrayD<T>, ArrayError> {
        self.async_retrieve_chunk_ndarray_opt(chunk_indices, &DecodeOptions::default())
            .await
    }

    /// Read and decode the chunk at `chunk_indices` into its bytes.
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if
    ///  - `chunk_indices` are invalid,
    ///  - there is a codec decoding error, or
    ///  - an underlying store error.
    ///
    /// # Panics
    /// Panics if the number of elements in the chunk exceeds `usize::MAX`.
    pub async fn async_retrieve_chunks_opt(
        &self,
        chunks: &ArraySubset,
        options: &DecodeOptions,
    ) -> Result<Vec<u8>, ArrayError> {
        if chunks.dimensionality() != self.chunk_grid().dimensionality() {
            return Err(ArrayError::InvalidArraySubset(
                chunks.clone(),
                self.shape().to_vec(),
            ));
        }

        let array_subset = Arc::new(self.chunks_subset(chunks)?);

        // Retrieve chunk bytes
        let num_chunks = chunks.num_elements();
        match num_chunks {
            0 => Ok(vec![]),
            1 => {
                let chunk_indices = chunks.start();
                self.async_retrieve_chunk_opt(chunk_indices, options).await
            }
            _ => {
                // Decode chunks and copy to output
                let size_output =
                    usize::try_from(array_subset.num_elements() * self.data_type().size() as u64)
                        .unwrap();
                let mut output = Vec::with_capacity(size_output);
                {
                    let output_slice = UnsafeCellSlice::new(unsafe {
                        crate::vec_spare_capacity_to_mut_slice(&mut output)
                    });
                    let mut futures = chunks
                        .indices()
                        .into_iter()
                        .map(|chunk_indices| {
                            let array_subset = array_subset.clone();
                            async move {
                                self._async_decode_chunk_into_array_subset(
                                    &chunk_indices,
                                    &array_subset,
                                    unsafe { output_slice.get() },
                                    options,
                                )
                                .await
                            }
                        })
                        .collect::<FuturesUnordered<_>>();
                    while let Some(item) = futures.next().await {
                        item?;
                    }
                }
                unsafe { output.set_len(size_output) };
                Ok(output)
            }
        }
    }

    /// Read and decode the chunk at `chunk_indices` into its bytes (default options).
    #[allow(clippy::missing_panics_doc, clippy::missing_errors_doc)]
    pub async fn async_retrieve_chunks(&self, chunks: &ArraySubset) -> Result<Vec<u8>, ArrayError> {
        self.async_retrieve_chunks_opt(chunks, &DecodeOptions::default())
            .await
    }

    /// Read and decode the chunk at `chunk_indices` into a vector of its elements.
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if the size of `T` does not match the data type size or a [`Array::async_retrieve_chunks`] error condition is met.
    pub async fn async_retrieve_chunks_elements_opt<T: bytemuck::Pod + Send + Sync>(
        &self,
        chunks: &ArraySubset,
        options: &DecodeOptions,
    ) -> Result<Vec<T>, ArrayError> {
        validate_element_size::<T>(self.data_type())?;
        let bytes = self.async_retrieve_chunks_opt(chunks, options).await?;
        Ok(transmute_from_bytes_vec::<T>(bytes))
    }

    /// Read and decode the chunk at `chunk_indices` into a vector of its elements (default options).
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if the size of `T` does not match the data type size or a [`Array::async_retrieve_chunks`] error condition is met.
    pub async fn async_retrieve_chunks_elements<T: bytemuck::Pod + Send + Sync>(
        &self,
        chunks: &ArraySubset,
    ) -> Result<Vec<T>, ArrayError> {
        self.async_retrieve_chunks_elements_opt(chunks, &DecodeOptions::default())
            .await
    }

    #[cfg(feature = "ndarray")]
    /// Read and decode the chunk at `chunk_indices` into an [`ndarray::ArrayD`].
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if the size of `T` does not match the data type size or a [`Array::async_retrieve_chunks`] error condition is met.
    pub async fn async_retrieve_chunks_ndarray_opt<T: bytemuck::Pod + Send + Sync>(
        &self,
        chunks: &ArraySubset,
        options: &DecodeOptions,
    ) -> Result<ndarray::ArrayD<T>, ArrayError> {
        validate_element_size::<T>(self.data_type())?;
        let array_subset = self.chunks_subset(chunks)?;
        let elements = self
            .async_retrieve_chunks_elements_opt(chunks, options)
            .await?;
        elements_to_ndarray(array_subset.shape(), elements)
    }

    #[cfg(feature = "ndarray")]
    /// Read and decode the chunk at `chunk_indices` into an [`ndarray::ArrayD`] (default options).
    #[allow(clippy::missing_panics_doc, clippy::missing_errors_doc)]
    pub async fn async_retrieve_chunks_ndarray<T: bytemuck::Pod + Send + Sync>(
        &self,
        chunks: &ArraySubset,
    ) -> Result<ndarray::ArrayD<T>, ArrayError> {
        self.async_retrieve_chunks_ndarray_opt(chunks, &DecodeOptions::default())
            .await
    }

    async fn _async_decode_chunk_into_array_subset(
        &self,
        chunk_indices: &[u64],
        array_subset: &ArraySubset,
        output: &mut [u8],
        options: &DecodeOptions,
    ) -> Result<(), ArrayError> {
        // Get the subset of the array corresponding to the chunk
        let chunk_subset_in_array = unsafe {
            self.chunk_grid()
                .subset_unchecked(chunk_indices, self.shape())
        };
        let Some(chunk_subset_in_array) = chunk_subset_in_array else {
            return Err(ArrayError::InvalidArraySubset(
                array_subset.clone(),
                self.shape().to_vec(),
            ));
        };

        // Decode the subset of the chunk which intersects array_subset
        let overlap = unsafe { array_subset.overlap_unchecked(&chunk_subset_in_array) };
        let array_subset_in_chunk_subset =
            unsafe { overlap.relative_to_unchecked(chunk_subset_in_array.start()) };
        let decoded_bytes = self
            .async_retrieve_chunk_subset_opt(chunk_indices, &array_subset_in_chunk_subset, options)
            .await?;

        // Copy decoded bytes to the output
        let element_size = self.data_type().size() as u64;
        let chunk_subset_in_array_subset =
            unsafe { overlap.relative_to_unchecked(array_subset.start()) };
        let mut decoded_offset = 0;
        let contiguous_indices = unsafe {
            chunk_subset_in_array_subset
                .contiguous_linearised_indices_unchecked(array_subset.shape())
        };
        let length =
            usize::try_from(contiguous_indices.contiguous_elements() * element_size).unwrap();
        for (array_subset_element_index, _num_elements) in &contiguous_indices {
            let output_offset = usize::try_from(array_subset_element_index * element_size).unwrap();
            debug_assert!((output_offset + length) <= output.len());
            debug_assert!((decoded_offset + length) <= decoded_bytes.len());
            output[output_offset..output_offset + length]
                .copy_from_slice(&decoded_bytes[decoded_offset..decoded_offset + length]);
            decoded_offset += length;
        }
        Ok(())
    }

    /// Read and decode the `array_subset` of array into its bytes.
    ///
    /// Out-of-bounds elements will have the fill value.
    /// If `parallel` is true, chunks intersecting the array subset are retrieved in parallel.
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if:
    ///  - the `array_subset` dimensionality does not match the chunk grid dimensionality,
    ///  - there is a codec decoding error, or
    ///  - an underlying store error.
    ///
    /// # Panics
    /// Panics if attempting to reference a byte beyond `usize::MAX`.
    #[allow(clippy::too_many_lines)]
    pub async fn async_retrieve_array_subset_opt(
        &self,
        array_subset: &ArraySubset,
        options: &DecodeOptions,
    ) -> Result<Vec<u8>, ArrayError> {
        if array_subset.dimensionality() != self.chunk_grid().dimensionality() {
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

        // Retrieve chunk bytes
        let num_chunks = chunks.num_elements();
        match num_chunks {
            0 => Ok(vec![]),
            1 => {
                let chunk_indices = chunks.start();
                let chunk_subset = self.chunk_subset(chunk_indices).unwrap();
                if &chunk_subset == array_subset {
                    // Single chunk fast path if the array subset domain matches the chunk domain
                    self.async_retrieve_chunk_opt(chunk_indices, options).await
                } else {
                    let size_output = usize::try_from(
                        array_subset.num_elements() * self.data_type().size() as u64,
                    )
                    .unwrap();
                    let mut output = Vec::with_capacity(size_output);
                    let output_slice =
                        unsafe { crate::vec_spare_capacity_to_mut_slice(&mut output) };
                    self._async_decode_chunk_into_array_subset(
                        chunk_indices,
                        array_subset,
                        output_slice,
                        options,
                    )
                    .await?;
                    unsafe { output.set_len(size_output) };
                    Ok(output)
                }
            }
            _ => {
                // Decode chunks and copy to output
                let size_output =
                    usize::try_from(array_subset.num_elements() * self.data_type().size() as u64)
                        .unwrap();

                // let mut output = vec![0; size_output];
                // let output_slice = output.as_mut_slice();
                let mut output = Vec::with_capacity(size_output);
                {
                    let output_slice = UnsafeCellSlice::new(unsafe {
                        crate::vec_spare_capacity_to_mut_slice(&mut output)
                    });
                    let mut futures = chunks
                        .indices()
                        .into_iter()
                        .map(|chunk_indices| {
                            async move {
                                // Get the subset of the array corresponding to the chunk
                                let chunk_subset_in_array = unsafe {
                                    self.chunk_grid()
                                        .subset_unchecked(&chunk_indices, self.shape())
                                };
                                let Some(chunk_subset_in_array) = chunk_subset_in_array else {
                                    return Err(ArrayError::InvalidArraySubset(
                                        array_subset.clone(),
                                        self.shape().to_vec(),
                                    ));
                                };

                                // Decode the subset of the chunk which intersects array_subset
                                let overlap = unsafe {
                                    array_subset.overlap_unchecked(&chunk_subset_in_array)
                                };
                                let array_subset_in_chunk_subset = unsafe {
                                    overlap.relative_to_unchecked(chunk_subset_in_array.start())
                                };

                                let storage_handle =
                                    Arc::new(StorageHandle::new(self.storage.clone()));
                                let storage_transformer = self
                                    .storage_transformers()
                                    .create_async_readable_transformer(storage_handle);
                                let input_handle = Box::new(AsyncStoragePartialDecoder::new(
                                    storage_transformer,
                                    data_key(
                                        self.path(),
                                        &chunk_indices,
                                        self.chunk_key_encoding(),
                                    ),
                                ));

                                let decoded_bytes = {
                                    let chunk_representation =
                                        self.chunk_array_representation(&chunk_indices)?;
                                    let partial_decoder = self
                                        .codecs()
                                        .async_partial_decoder_opt(
                                            input_handle,
                                            &chunk_representation,
                                            options, // FIXME: Adjust internal decode options
                                        )
                                        .await?;

                                    partial_decoder
                                        .partial_decode_opt(
                                            &[array_subset_in_chunk_subset],
                                            options,
                                        ) // FIXME: Adjust internal decode options
                                        .await?
                                        .remove(0)
                                };

                                // Copy decoded bytes to the output
                                let element_size = self.data_type().size() as u64;
                                let chunk_subset_in_array_subset =
                                    unsafe { overlap.relative_to_unchecked(array_subset.start()) };
                                let mut decoded_offset = 0;
                                let contiguous_indices = unsafe {
                                    chunk_subset_in_array_subset
                                        .contiguous_linearised_indices_unchecked(
                                            array_subset.shape(),
                                        )
                                };
                                let length = usize::try_from(
                                    contiguous_indices.contiguous_elements() * element_size,
                                )
                                .unwrap();
                                for (array_subset_element_index, _num_elements) in
                                    &contiguous_indices
                                {
                                    let output_offset =
                                        usize::try_from(array_subset_element_index * element_size)
                                            .unwrap();
                                    debug_assert!((output_offset + length) <= size_output);
                                    debug_assert!((decoded_offset + length) <= decoded_bytes.len());
                                    unsafe {
                                        let output_slice = output_slice.get();
                                        output_slice[output_offset..output_offset + length]
                                            .copy_from_slice(
                                                &decoded_bytes
                                                    [decoded_offset..decoded_offset + length],
                                            );
                                    }
                                    decoded_offset += length;
                                }
                                Ok(())
                            }
                        })
                        .collect::<FuturesUnordered<_>>();
                    while let Some(item) = futures.next().await {
                        item?;
                    }
                }
                unsafe { output.set_len(size_output) };
                Ok(output)
            }
        }
    }

    /// Read and decode the `array_subset` of array into its bytes (default options).
    #[allow(clippy::missing_panics_doc, clippy::missing_errors_doc)]
    pub async fn async_retrieve_array_subset(
        &self,
        array_subset: &ArraySubset,
    ) -> Result<Vec<u8>, ArrayError> {
        self.async_retrieve_array_subset_opt(array_subset, &DecodeOptions::default())
            .await
    }

    /// Read and decode the `array_subset` of array into a vector of its elements.
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if:
    ///  - the size of `T` does not match the data type size,
    ///  - the decoded bytes cannot be transmuted,
    ///  - an array subset is invalid or out of bounds of the array,
    ///  - there is a codec decoding error, or
    ///  - an underlying store error.
    pub async fn async_retrieve_array_subset_elements_opt<T: bytemuck::Pod + Send + Sync>(
        &self,
        array_subset: &ArraySubset,
        options: &DecodeOptions,
    ) -> Result<Vec<T>, ArrayError> {
        validate_element_size::<T>(self.data_type())?;
        let bytes = self
            .async_retrieve_array_subset_opt(array_subset, options)
            .await?;
        Ok(transmute_from_bytes_vec::<T>(bytes))
    }

    /// Read and decode the `array_subset` of array into a vector of its elements (default options).
    #[allow(clippy::missing_panics_doc, clippy::missing_errors_doc)]
    pub async fn async_retrieve_array_subset_elements<T: bytemuck::Pod + Send + Sync>(
        &self,
        array_subset: &ArraySubset,
    ) -> Result<Vec<T>, ArrayError> {
        self.async_retrieve_array_subset_elements_opt(array_subset, &DecodeOptions::default())
            .await
    }

    #[cfg(feature = "ndarray")]
    /// Read and decode the `array_subset` of array into an [`ndarray::ArrayD`].
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if:
    ///  - an array subset is invalid or out of bounds of the array,
    ///  - there is a codec decoding error, or
    ///  - an underlying store error.
    ///
    /// # Panics
    /// Will panic if any dimension in `chunk_subset` is `usize::MAX` or larger.
    pub async fn async_retrieve_array_subset_ndarray_opt<T: bytemuck::Pod + Send + Sync>(
        &self,
        array_subset: &ArraySubset,
        options: &DecodeOptions,
    ) -> Result<ndarray::ArrayD<T>, ArrayError> {
        // validate_element_size::<T>(self.data_type())?; // in async_retrieve_array_subset_elements
        let elements = self
            .async_retrieve_array_subset_elements_opt(array_subset, options)
            .await?;
        elements_to_ndarray(array_subset.shape(), elements)
    }

    #[cfg(feature = "ndarray")]
    /// Read and decode the `array_subset` of array into an [`ndarray::ArrayD`] (default options).
    #[allow(clippy::missing_panics_doc, clippy::missing_errors_doc)]
    pub async fn async_retrieve_array_subset_ndarray<T: bytemuck::Pod + Send + Sync>(
        &self,
        array_subset: &ArraySubset,
    ) -> Result<ndarray::ArrayD<T>, ArrayError> {
        self.async_retrieve_array_subset_ndarray_opt(array_subset, &DecodeOptions::default())
            .await
    }

    /// Read and decode the `chunk_subset` of the chunk at `chunk_indices` into its bytes.
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if:
    ///  - the chunk indices are invalid,
    ///  - the chunk subset is invalid,
    ///  - there is a codec decoding error, or
    ///  - an underlying store error.
    ///
    /// # Panics
    /// Will panic if the number of elements in `chunk_subset` is `usize::MAX` or larger.
    pub async fn async_retrieve_chunk_subset_opt(
        &self,
        chunk_indices: &[u64],
        chunk_subset: &ArraySubset,
        options: &DecodeOptions,
    ) -> Result<Vec<u8>, ArrayError> {
        let chunk_representation = self.chunk_array_representation(chunk_indices)?;
        if !chunk_subset.inbounds(&chunk_representation.shape_u64()) {
            return Err(ArrayError::InvalidArraySubset(
                chunk_subset.clone(),
                self.shape().to_vec(),
            ));
        }

        let storage_handle = Arc::new(StorageHandle::new(self.storage.clone()));
        let storage_transformer = self
            .storage_transformers()
            .create_async_readable_transformer(storage_handle);
        let input_handle = Box::new(AsyncStoragePartialDecoder::new(
            storage_transformer,
            data_key(self.path(), chunk_indices, self.chunk_key_encoding()),
        ));

        let decoded_bytes = self
            .codecs()
            .async_partial_decoder_opt(input_handle, &chunk_representation, options)
            .await?
            .partial_decode_opt(&[chunk_subset.clone()], options)
            .await?
            .pop()
            .unwrap();

        let expected_size = chunk_subset.num_elements_usize() * self.data_type().size();
        if decoded_bytes.len() == chunk_subset.num_elements_usize() * self.data_type().size() {
            Ok(decoded_bytes)
        } else {
            Err(ArrayError::UnexpectedChunkDecodedSize(
                decoded_bytes.len(),
                expected_size,
            ))
        }
    }

    /// Read and decode the `chunk_subset` of the chunk at `chunk_indices` into its bytes (default options).
    #[allow(clippy::missing_panics_doc, clippy::missing_errors_doc)]
    pub async fn async_retrieve_chunk_subset(
        &self,
        chunk_indices: &[u64],
        chunk_subset: &ArraySubset,
    ) -> Result<Vec<u8>, ArrayError> {
        self.async_retrieve_chunk_subset_opt(chunk_indices, chunk_subset, &DecodeOptions::default())
            .await
    }

    /// Read and decode the `chunk_subset` of the chunk at `chunk_indices` into its elements.
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if:
    ///  - the chunk indices are invalid,
    ///  - the chunk subset is invalid,
    ///  - there is a codec decoding error, or
    ///  - an underlying store error.
    pub async fn async_retrieve_chunk_subset_elements_opt<T: bytemuck::Pod + Send + Sync>(
        &self,
        chunk_indices: &[u64],
        chunk_subset: &ArraySubset,
        options: &DecodeOptions,
    ) -> Result<Vec<T>, ArrayError> {
        validate_element_size::<T>(self.data_type())?;
        let bytes = self
            .async_retrieve_chunk_subset_opt(chunk_indices, chunk_subset, options)
            .await?;
        Ok(transmute_from_bytes_vec::<T>(bytes))
    }

    /// Read and decode the `chunk_subset` of the chunk at `chunk_indices` into its elements (default options).
    #[allow(clippy::missing_panics_doc, clippy::missing_errors_doc)]
    pub async fn async_retrieve_chunk_subset_elements<T: bytemuck::Pod + Send + Sync>(
        &self,
        chunk_indices: &[u64],
        chunk_subset: &ArraySubset,
    ) -> Result<Vec<T>, ArrayError> {
        self.async_retrieve_chunk_subset_elements_opt(
            chunk_indices,
            chunk_subset,
            &DecodeOptions::default(),
        )
        .await
    }

    #[cfg(feature = "ndarray")]
    /// Read and decode the `chunk_subset` of the chunk at `chunk_indices` into an [`ndarray::ArrayD`].
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if:
    ///  - the chunk indices are invalid,
    ///  - the chunk subset is invalid,
    ///  - there is a codec decoding error, or
    ///  - an underlying store error.
    ///
    /// # Panics
    /// Will panic if the number of elements in `chunk_subset` is `usize::MAX` or larger.
    pub async fn async_retrieve_chunk_subset_ndarray_opt<T: bytemuck::Pod + Send + Sync>(
        &self,
        chunk_indices: &[u64],
        chunk_subset: &ArraySubset,
        options: &DecodeOptions,
    ) -> Result<ndarray::ArrayD<T>, ArrayError> {
        // validate_element_size::<T>(self.data_type())?; // in async_retrieve_chunk_subset_elements
        let elements = self
            .async_retrieve_chunk_subset_elements_opt(chunk_indices, chunk_subset, options)
            .await?;
        elements_to_ndarray(chunk_subset.shape(), elements)
    }

    #[cfg(feature = "ndarray")]
    /// Read and decode the `chunk_subset` of the chunk at `chunk_indices` into an [`ndarray::ArrayD`] (default options).
    #[allow(clippy::missing_panics_doc, clippy::missing_errors_doc)]
    pub async fn async_retrieve_chunk_subset_ndarray<T: bytemuck::Pod + Send + Sync>(
        &self,
        chunk_indices: &[u64],
        chunk_subset: &ArraySubset,
    ) -> Result<ndarray::ArrayD<T>, ArrayError> {
        self.async_retrieve_chunk_subset_ndarray_opt(
            chunk_indices,
            chunk_subset,
            &DecodeOptions::default(),
        )
        .await
    }

    /// Initialises a partial decoder for the chunk at `chunk_indices`.
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if initialisation of the partial decoder fails.
    pub async fn async_partial_decoder_opt<'a>(
        &'a self,
        chunk_indices: &[u64],
        options: &PartialDecoderOptions,
    ) -> Result<Box<dyn AsyncArrayPartialDecoderTraits + 'a>, ArrayError> {
        let storage_handle = Arc::new(StorageHandle::new(self.storage.clone()));
        let storage_transformer = self
            .storage_transformers()
            .create_async_readable_transformer(storage_handle);
        let input_handle = Box::new(AsyncStoragePartialDecoder::new(
            storage_transformer,
            data_key(self.path(), chunk_indices, self.chunk_key_encoding()),
        ));
        let chunk_representation = self.chunk_array_representation(chunk_indices)?;
        Ok(self
            .codecs()
            .async_partial_decoder_opt(input_handle, &chunk_representation, options)
            .await?)
    }

    /// Initialises a partial decoder for the chunk at `chunk_indices` (default options).
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if initialisation of the partial decoder fails.
    pub async fn async_partial_decoder<'a>(
        &'a self,
        chunk_indices: &[u64],
    ) -> Result<Box<dyn AsyncArrayPartialDecoderTraits + 'a>, ArrayError> {
        self.async_partial_decoder_opt(chunk_indices, &PartialDecoderOptions::default())
            .await
    }
}
