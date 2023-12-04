use std::sync::Arc;

use futures::{stream::FuturesUnordered, StreamExt};
use safe_transmute::TriviallyTransmutable;

use crate::{
    array_subset::ArraySubset,
    node::NodePath,
    storage::{
        data_key, meta_key, AsyncReadableStorageTraits, AsyncWritableStorageTraits, StorageError,
        StorageHandle,
    },
};

use super::{
    array_errors::TransmuteError,
    codec::{
        ArrayCodecTraits, ArrayToBytesCodecTraits, AsyncArrayPartialDecoderTraits,
        AsyncStoragePartialDecoder,
    },
    safe_transmute_to_bytes_vec,
    unsafe_cell_slice::UnsafeCellSlice,
    Array, ArrayCreateError, ArrayError, ArrayMetadata,
};

impl<TStorage: ?Sized + AsyncReadableStorageTraits> Array<TStorage> {
    /// Create an array in `storage` at `path`. The metadata is read from the store.
    ///
    /// # Errors
    /// Returns [`ArrayCreateError`] if there is a storage error or any metadata is invalid.
    pub async fn async_new(storage: Arc<TStorage>, path: &str) -> Result<Self, ArrayCreateError> {
        let node_path = NodePath::new(path)?;
        let metadata: ArrayMetadata = serde_json::from_slice(
            &storage
                .get(&meta_key(&node_path))
                .await?
                .ok_or(ArrayCreateError::MissingMetadata)?,
        )?;
        Self::new_with_metadata(storage, path, metadata)
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
    pub async fn async_retrieve_chunk(
        &self,
        chunk_indices: &[u64],
    ) -> Result<Box<[u8]>, ArrayError> {
        let storage_handle = Arc::new(StorageHandle::new(&*self.storage));
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
        let chunk_representation = self.chunk_array_representation(chunk_indices)?;
        if let Some(chunk_encoded) = chunk_encoded {
            let chunk_decoded = self
                .codecs()
                .async_decode_opt(chunk_encoded, &chunk_representation, self.parallel_codecs())
                .await
                .map_err(ArrayError::CodecError)?;
            let chunk_decoded_size =
                chunk_representation.num_elements_usize() * chunk_representation.data_type().size();
            if chunk_decoded.len() == chunk_decoded_size {
                Ok(chunk_decoded.into_boxed_slice())
            } else {
                Err(ArrayError::UnexpectedChunkDecodedSize(
                    chunk_decoded.len(),
                    chunk_decoded_size,
                ))
            }
        } else {
            let fill_value = chunk_representation.fill_value().as_ne_bytes();
            Ok(fill_value
                .repeat(chunk_representation.num_elements_usize())
                .into_boxed_slice())
        }
    }

    /// Read and decode the chunk at `chunk_indices` into a vector of its elements.
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if
    ///  - the size of `T` does not match the data type size,
    ///  - the decoded bytes cannot be transmuted,
    ///  - `chunk_indices` are invalid,
    ///  - there is a codec decoding error, or
    ///  - an underlying store error.
    pub async fn async_retrieve_chunk_elements<T: TriviallyTransmutable>(
        &self,
        chunk_indices: &[u64],
    ) -> Result<Box<[T]>, ArrayError> {
        if self.data_type.size() != std::mem::size_of::<T>() {
            return Err(ArrayError::IncompatibleElementSize(
                self.data_type.size(),
                std::mem::size_of::<T>(),
            ));
        }

        let bytes = self.async_retrieve_chunk(chunk_indices).await?;
        if safe_transmute::align::check_alignment::<_, T>(&bytes).is_ok() {
            // no-copy path
            let mut bytes = core::mem::ManuallyDrop::new(bytes);
            Ok(unsafe {
                Vec::from_raw_parts(
                    bytes.as_mut_ptr().cast::<T>(),
                    bytes.len() / core::mem::size_of::<T>(),
                    bytes.len(),
                )
            }
            .into_boxed_slice())
        } else {
            let elements = safe_transmute::transmute_many_permissive::<T>(&bytes)
                .map_err(TransmuteError::from)?
                .to_vec()
                .into_boxed_slice();
            Ok(elements)
        }
    }

    #[cfg(feature = "ndarray")]
    /// Read and decode the chunk at `chunk_indices` into an ndarray.
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
    pub async fn async_retrieve_chunk_ndarray<T: safe_transmute::TriviallyTransmutable>(
        &self,
        chunk_indices: &[u64],
    ) -> Result<ndarray::ArrayD<T>, ArrayError> {
        use super::iter_u64_to_usize;

        if self.data_type.size() != std::mem::size_of::<T>() {
            return Err(ArrayError::IncompatibleElementSize(
                self.data_type.size(),
                std::mem::size_of::<T>(),
            ));
        }

        let shape = self.chunk_grid().chunk_shape(chunk_indices, self.shape())?;
        if let Some(shape) = shape {
            let elements = self.async_retrieve_chunk_elements(chunk_indices).await?;
            let length = elements.len();
            ndarray::ArrayD::<T>::from_shape_vec(
                iter_u64_to_usize(shape.iter()),
                elements.into_vec(),
            )
            .map_err(|_| {
                ArrayError::CodecError(crate::array::codec::CodecError::UnexpectedChunkDecodedSize(
                    length * std::mem::size_of::<T>(),
                    shape.iter().product::<u64>() * std::mem::size_of::<T>() as u64,
                ))
            })
        } else {
            Err(ArrayError::InvalidChunkGridIndicesError(
                chunk_indices.to_vec(),
            ))
        }
    }

    async fn _async_decode_chunk_into_array_subset(
        &self,
        chunk_indices: &[u64],
        array_subset: &ArraySubset,
        output: &mut [u8],
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
        let array_subset_in_chunk_subset =
            unsafe { array_subset.in_subset_unchecked(&chunk_subset_in_array) };
        let decoded_bytes = self
            .async_retrieve_chunk_subset(chunk_indices, &array_subset_in_chunk_subset)
            .await?;

        // Copy decoded bytes to the output
        let element_size = self.data_type().size() as u64;
        let chunk_subset_in_array_subset =
            unsafe { chunk_subset_in_array.in_subset_unchecked(array_subset) };
        let mut decoded_offset = 0;
        for (array_subset_element_index, num_elements) in unsafe {
            chunk_subset_in_array_subset
                .iter_contiguous_linearised_indices_unchecked(array_subset.shape())
        } {
            let output_offset = usize::try_from(array_subset_element_index * element_size).unwrap();
            let length = usize::try_from(num_elements * element_size).unwrap();
            debug_assert!((output_offset + length) <= output.len());
            debug_assert!((decoded_offset + length) <= decoded_bytes.len());
            output[output_offset..output_offset + length]
                .copy_from_slice(&decoded_bytes[decoded_offset..decoded_offset + length]);
            decoded_offset += length;
        }
        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    async fn _async_retrieve_array_subset(
        &self,
        array_subset: &ArraySubset,
        parallel: bool,
    ) -> Result<Box<[u8]>, ArrayError> {
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
            0 => Ok(vec![].into_boxed_slice()),
            1 => {
                let chunk_indices = chunks.start();
                let chunk_subset = self.chunk_subset(chunk_indices).unwrap();
                if &chunk_subset == array_subset {
                    // Single chunk fast path if the array subset domain matches the chunk domain
                    self.async_retrieve_chunk(chunk_indices).await
                } else {
                    let size_output = usize::try_from(
                        array_subset.num_elements() * self.data_type().size() as u64,
                    )
                    .unwrap();
                    let mut output = vec![core::mem::MaybeUninit::<u8>::uninit(); size_output];
                    let output_slice = unsafe {
                        std::slice::from_raw_parts_mut(
                            output.as_mut_ptr().cast::<u8>(),
                            size_output,
                        )
                    };
                    self._async_decode_chunk_into_array_subset(
                        chunk_indices,
                        array_subset,
                        output_slice,
                    )
                    .await?;
                    #[allow(clippy::transmute_undefined_repr)]
                    let output: Vec<u8> = unsafe { core::mem::transmute(output) };
                    Ok(output.into_boxed_slice())
                }
            }
            _ => {
                // Decode chunks and copy to output
                let size_output =
                    usize::try_from(array_subset.num_elements() * self.data_type().size() as u64)
                        .unwrap();

                // let mut output = vec![0; size_output];
                // let output_slice = output.as_mut_slice();
                let mut output = vec![core::mem::MaybeUninit::<u8>::uninit(); size_output];
                let output_slice = unsafe {
                    std::slice::from_raw_parts_mut(output.as_mut_ptr().cast::<u8>(), size_output)
                };
                if parallel {
                    let output_slice = UnsafeCellSlice::new(output_slice);
                    let mut futures = chunks
                        .iter_indices()
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
                                let array_subset_in_chunk_subset = unsafe {
                                    array_subset.in_subset_unchecked(&chunk_subset_in_array)
                                };

                                let storage_handle = Arc::new(StorageHandle::new(&*self.storage));
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
                                        .async_partial_decoder(input_handle, &chunk_representation)
                                        .await?;

                                    partial_decoder
                                        .par_partial_decode(&[array_subset_in_chunk_subset])
                                        .await?
                                        .remove(0)
                                };

                                // Copy decoded bytes to the output
                                let element_size = self.data_type().size() as u64;
                                let chunk_subset_in_array_subset = unsafe {
                                    chunk_subset_in_array.in_subset_unchecked(array_subset)
                                };
                                let mut decoded_offset = 0;
                                for (array_subset_element_index, num_elements) in unsafe {
                                    chunk_subset_in_array_subset
                                        .iter_contiguous_linearised_indices_unchecked(
                                            array_subset.shape(),
                                        )
                                } {
                                    let output_offset =
                                        usize::try_from(array_subset_element_index * element_size)
                                            .unwrap();
                                    let length =
                                        usize::try_from(num_elements * element_size).unwrap();
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
                } else {
                    for chunk_indices in chunks.iter_indices() {
                        self._async_decode_chunk_into_array_subset(
                            &chunk_indices,
                            array_subset,
                            output_slice,
                        )
                        .await?;
                    }
                }
                #[allow(clippy::transmute_undefined_repr)]
                let output: Vec<u8> = unsafe { core::mem::transmute(output) };
                Ok(output.into_boxed_slice())
            }
        }
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
    pub async fn async_retrieve_array_subset(
        &self,
        array_subset: &ArraySubset,
    ) -> Result<Box<[u8]>, ArrayError> {
        self._async_retrieve_array_subset(array_subset, false).await
    }

    /// Parallel version of [`Array::retrieve_array_subset`].
    #[allow(clippy::missing_panics_doc, clippy::missing_errors_doc)]
    pub async fn async_par_retrieve_array_subset(
        &self,
        array_subset: &ArraySubset,
    ) -> Result<Box<[u8]>, ArrayError> {
        self._async_retrieve_array_subset(array_subset, true).await
    }

    async fn _async_retrieve_array_subset_elements<T: TriviallyTransmutable>(
        &self,
        array_subset: &ArraySubset,
        parallel: bool,
    ) -> Result<Box<[T]>, ArrayError> {
        if self.data_type.size() != std::mem::size_of::<T>() {
            return Err(ArrayError::IncompatibleElementSize(
                self.data_type.size(),
                std::mem::size_of::<T>(),
            ));
        }

        let bytes = self
            ._async_retrieve_array_subset(array_subset, parallel)
            .await?;
        if safe_transmute::align::check_alignment::<_, T>(&bytes).is_ok() {
            // no-copy path
            let mut bytes = core::mem::ManuallyDrop::new(bytes);
            Ok(unsafe {
                Vec::from_raw_parts(
                    bytes.as_mut_ptr().cast::<T>(),
                    bytes.len() / core::mem::size_of::<T>(),
                    bytes.len(),
                )
            }
            .into_boxed_slice())
        } else {
            let elements = safe_transmute::transmute_many_permissive::<T>(&bytes)
                .map_err(TransmuteError::from)?
                .to_vec()
                .into_boxed_slice();
            Ok(elements)
        }
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
    pub async fn async_retrieve_array_subset_elements<T: TriviallyTransmutable>(
        &self,
        array_subset: &ArraySubset,
    ) -> Result<Box<[T]>, ArrayError> {
        self._async_retrieve_array_subset_elements(array_subset, false)
            .await
    }

    /// Parallel version of [`Array::retrieve_array_subset_elements`].
    #[allow(clippy::missing_panics_doc, clippy::missing_errors_doc)]
    pub async fn async_par_retrieve_array_subset_elements<T: TriviallyTransmutable>(
        &self,
        array_subset: &ArraySubset,
    ) -> Result<Box<[T]>, ArrayError> {
        self._async_retrieve_array_subset_elements(array_subset, true)
            .await
    }

    #[cfg(feature = "ndarray")]
    async fn _async_retrieve_array_subset_ndarray<T: safe_transmute::TriviallyTransmutable>(
        &self,
        array_subset: &ArraySubset,
        parallel: bool,
    ) -> Result<ndarray::ArrayD<T>, ArrayError> {
        use super::iter_u64_to_usize;

        if self.data_type.size() != std::mem::size_of::<T>() {
            return Err(ArrayError::IncompatibleElementSize(
                self.data_type.size(),
                std::mem::size_of::<T>(),
            ));
        }

        let elements = self
            ._async_retrieve_array_subset_elements(array_subset, parallel)
            .await?;
        let length = elements.len();
        ndarray::ArrayD::<T>::from_shape_vec(
            iter_u64_to_usize(array_subset.shape().iter()),
            elements.to_vec(),
        )
        .map_err(|_| {
            ArrayError::CodecError(crate::array::codec::CodecError::UnexpectedChunkDecodedSize(
                length * self.data_type().size(),
                array_subset.num_elements() * self.data_type().size() as u64,
            ))
        })
    }

    #[cfg(feature = "ndarray")]
    /// Read and decode the `array_subset` of array into an ndarray.
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if:
    ///  - an array subset is invalid or out of bounds of the array,
    ///  - there is a codec decoding error, or
    ///  - an underlying store error.
    ///
    /// # Panics
    /// Will panic if any dimension in `chunk_subset` is `usize::MAX` or larger.
    pub async fn async_retrieve_array_subset_ndarray<T: safe_transmute::TriviallyTransmutable>(
        &self,
        array_subset: &ArraySubset,
    ) -> Result<ndarray::ArrayD<T>, ArrayError> {
        self._async_retrieve_array_subset_ndarray(array_subset, false)
            .await
    }

    #[cfg(feature = "ndarray")]
    /// Parallel version of [`Array::retrieve_array_subset_ndarray`].
    #[allow(clippy::missing_panics_doc, clippy::missing_errors_doc)]
    pub async fn async_par_retrieve_array_subset_ndarray<
        T: safe_transmute::TriviallyTransmutable,
    >(
        &self,
        array_subset: &ArraySubset,
    ) -> Result<ndarray::ArrayD<T>, ArrayError> {
        self._async_retrieve_array_subset_ndarray(array_subset, true)
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
    pub async fn async_retrieve_chunk_subset(
        &self,
        chunk_indices: &[u64],
        chunk_subset: &ArraySubset,
    ) -> Result<Box<[u8]>, ArrayError> {
        let chunk_representation = self.chunk_array_representation(chunk_indices)?;
        if !chunk_subset.inbounds(chunk_representation.shape()) {
            return Err(ArrayError::InvalidArraySubset(
                chunk_subset.clone(),
                self.shape().to_vec(),
            ));
        }

        let storage_handle = Arc::new(StorageHandle::new(&*self.storage));
        let storage_transformer = self
            .storage_transformers()
            .create_async_readable_transformer(storage_handle);
        let input_handle = Box::new(AsyncStoragePartialDecoder::new(
            storage_transformer,
            data_key(self.path(), chunk_indices, self.chunk_key_encoding()),
        ));

        let decoded_bytes = self
            .codecs()
            .async_partial_decoder_opt(input_handle, &chunk_representation, self.parallel_codecs())
            .await?
            .partial_decode_opt(&[chunk_subset.clone()], self.parallel_codecs())
            .await?;

        let total_size = decoded_bytes.iter().map(Vec::len).sum::<usize>();
        let expected_size = chunk_subset.num_elements_usize() * self.data_type().size();
        if total_size == chunk_subset.num_elements_usize() * self.data_type().size() {
            Ok(decoded_bytes.concat().into_boxed_slice())
        } else {
            Err(ArrayError::UnexpectedChunkDecodedSize(
                total_size,
                expected_size,
            ))
        }
    }

    /// Read and decode the `chunk_subset` of the chunk at `chunk_indices` into its elements.
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if:
    ///  - the chunk indices are invalid,
    ///  - the chunk subset is invalid,
    ///  - there is a codec decoding error, or
    ///  - an underlying store error.
    pub async fn async_retrieve_chunk_subset_elements<T: TriviallyTransmutable>(
        &self,
        chunk_indices: &[u64],
        chunk_subset: &ArraySubset,
    ) -> Result<Box<[T]>, ArrayError> {
        if self.data_type.size() != std::mem::size_of::<T>() {
            return Err(ArrayError::IncompatibleElementSize(
                self.data_type.size(),
                std::mem::size_of::<T>(),
            ));
        }

        let bytes = self
            .async_retrieve_chunk_subset(chunk_indices, chunk_subset)
            .await?;
        if safe_transmute::align::check_alignment::<_, T>(&bytes).is_ok() {
            // no-copy path
            let mut bytes = core::mem::ManuallyDrop::new(bytes);
            Ok(unsafe {
                Vec::from_raw_parts(
                    bytes.as_mut_ptr().cast::<T>(),
                    bytes.len() / core::mem::size_of::<T>(),
                    bytes.len(),
                )
            }
            .into_boxed_slice())
        } else {
            let elements = safe_transmute::transmute_many_permissive::<T>(&bytes)
                .map_err(TransmuteError::from)?
                .to_vec()
                .into_boxed_slice();
            Ok(elements)
        }
    }

    #[cfg(feature = "ndarray")]
    /// Read and decode the `chunk_subset` of the chunk at `chunk_indices` into an ndarray.
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
    pub async fn async_retrieve_chunk_subset_ndarray<T: TriviallyTransmutable>(
        &self,
        chunk_indices: &[u64],
        chunk_subset: &ArraySubset,
    ) -> Result<ndarray::ArrayD<T>, ArrayError> {
        use super::iter_u64_to_usize;

        let elements = self
            .async_retrieve_chunk_subset_elements(chunk_indices, chunk_subset)
            .await?;
        let length = elements.len();
        ndarray::ArrayD::from_shape_vec(
            iter_u64_to_usize(chunk_subset.shape().iter()),
            elements.into_vec(),
        )
        .map_err(|_| {
            ArrayError::CodecError(crate::array::codec::CodecError::UnexpectedChunkDecodedSize(
                length * std::mem::size_of::<T>(),
                chunk_subset.shape().iter().product::<u64>() * std::mem::size_of::<T>() as u64,
            ))
        })
    }

    /// Initialises a partial decoder for the chunk at `chunk_indices` with optional parallelism.
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if initialisation of the partial decoder fails.
    pub async fn async_partial_decoder_opt<'a>(
        &'a self,
        chunk_indices: &[u64],
        parallel: bool,
    ) -> Result<Box<dyn AsyncArrayPartialDecoderTraits + 'a>, ArrayError> {
        let storage_handle = Arc::new(StorageHandle::new(&*self.storage));
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
            .async_partial_decoder_opt(input_handle, &chunk_representation, parallel)
            .await?)
    }

    /// Initialises a partial decoder for the chunk at `chunk_indices`.
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if initialisation of the partial decoder fails.
    pub async fn async_partial_decoder<'a>(
        &'a self,
        chunk_indices: &[u64],
    ) -> Result<Box<dyn AsyncArrayPartialDecoderTraits + 'a>, ArrayError> {
        self.async_partial_decoder_opt(chunk_indices, false).await
    }

    /// Initialises a partial decoder for the chunk at `chunk_indices` using multithreading if applicable.
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if initialisation of the partial decoder fails.
    pub async fn async_par_partial_decoder<'a>(
        &'a self,
        chunk_indices: &[u64],
    ) -> Result<Box<dyn AsyncArrayPartialDecoderTraits + 'a>, ArrayError> {
        self.async_partial_decoder_opt(chunk_indices, true).await
    }
}

impl<TStorage: ?Sized + AsyncWritableStorageTraits> Array<TStorage> {
    /// Store metadata.
    ///
    /// # Errors
    /// Returns [`StorageError`] if there is an underlying store error.
    pub async fn async_store_metadata(&self) -> Result<(), StorageError> {
        let storage_handle = Arc::new(StorageHandle::new(&*self.storage));
        let storage_transformer = self
            .storage_transformers()
            .create_async_writable_transformer(storage_handle);
        crate::storage::async_create_array(&*storage_transformer, self.path(), &self.metadata())
            .await
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
    pub async fn async_store_chunk(
        &self,
        chunk_indices: &[u64],
        chunk_bytes: Vec<u8>,
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
            self.async_erase_chunk(chunk_indices).await?;
            Ok(())
        } else {
            let storage_handle = Arc::new(StorageHandle::new(&*self.storage));
            let storage_transformer = self
                .storage_transformers()
                .create_async_writable_transformer(storage_handle);
            let chunk_encoded: Vec<u8> = self
                .codecs()
                .async_encode_opt(
                    chunk_bytes,
                    &chunk_array_representation,
                    self.parallel_codecs(),
                )
                .await
                .map_err(ArrayError::CodecError)?;
            crate::storage::async_store_chunk(
                &*storage_transformer,
                self.path(),
                chunk_indices,
                self.chunk_key_encoding(),
                &chunk_encoded,
            )
            .await
            .map_err(ArrayError::StorageError)
        }
    }

    /// Encode `chunk_elements` and store at `chunk_indices`.
    ///
    /// A chunk composed entirely of the fill value will not be written to the store.
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if
    ///  - the size of  `T` does not match the data type size, or
    ///  - a [`store_chunk`](Array::store_chunk) error condition is met.
    pub async fn async_store_chunk_elements<T: TriviallyTransmutable>(
        &self,
        chunk_indices: &[u64],
        chunk_elements: Vec<T>,
    ) -> Result<(), ArrayError> {
        if self.data_type.size() != std::mem::size_of::<T>() {
            return Err(ArrayError::IncompatibleElementSize(
                self.data_type.size(),
                std::mem::size_of::<T>(),
            ));
        }

        let chunk_bytes = safe_transmute_to_bytes_vec(chunk_elements);
        self.async_store_chunk(chunk_indices, chunk_bytes).await
    }

    #[cfg(feature = "ndarray")]
    /// Encode `chunk_array` and store at `chunk_indices`.
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if
    ///  - the size of `T` does not match the size of the data type,
    ///  - a [`store_chunk_elements`](Array::store_chunk_elements) error condition is met.
    #[allow(clippy::missing_panics_doc)]
    pub async fn async_store_chunk_ndarray<T: safe_transmute::TriviallyTransmutable>(
        &self,
        chunk_indices: &[u64],
        chunk_array: &ndarray::ArrayViewD<'_, T>,
    ) -> Result<(), ArrayError> {
        if self.data_type.size() != std::mem::size_of::<T>() {
            return Err(ArrayError::IncompatibleElementSize(
                self.data_type.size(),
                std::mem::size_of::<T>(),
            ));
        }
        let shape = chunk_array.shape().iter().map(|u| *u as u64).collect();
        if let Some(chunk_shape) = self.chunk_grid().chunk_shape(chunk_indices, self.shape())? {
            if shape != chunk_shape {
                return Err(ArrayError::UnexpectedChunkDecodedShape(shape, chunk_shape));
            }

            let chunk_array = chunk_array.as_standard_layout();
            let chunk_elements = chunk_array.into_owned().into_raw_vec();
            self.async_store_chunk_elements(chunk_indices, chunk_elements)
                .await
        } else {
            Err(ArrayError::InvalidChunkGridIndicesError(
                chunk_indices.to_vec(),
            ))
        }
    }

    /// Erase the chunk at `chunk_indices`.
    ///
    /// Returns true if the chunk was erased, or false if it did not exist.
    ///
    /// # Errors
    /// Returns a [`StorageError`] if there is an underlying store error.
    pub async fn async_erase_chunk(&self, chunk_indices: &[u64]) -> Result<bool, StorageError> {
        let storage_handle = Arc::new(StorageHandle::new(&*self.storage));
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
}

impl<TStorage: ?Sized + AsyncReadableStorageTraits + AsyncWritableStorageTraits> Array<TStorage> {
    #[allow(clippy::too_many_lines)]
    async fn _async_store_array_subset(
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
                self.async_store_chunk(chunk_indices, subset_bytes).await?;
            } else {
                let chunk_subset_in_array_subset =
                    unsafe { chunk_subset_in_array.in_subset_unchecked(array_subset) };
                let chunk_subset_bytes = unsafe {
                    chunk_subset_in_array_subset.extract_bytes_unchecked(
                        &subset_bytes,
                        array_subset.shape(),
                        element_size,
                    )
                };

                // Store the chunk subset
                let array_subset_in_chunk_subset =
                    unsafe { array_subset.in_subset_unchecked(&chunk_subset_in_array) };

                self.async_store_chunk_subset(
                    chunk_indices,
                    &array_subset_in_chunk_subset,
                    chunk_subset_bytes,
                )
                .await?;
            }
        } else if parallel {
            let chunks_to_update = chunks
                .iter_indices()
                .map(|chunk_indices| {
                    let chunk_subset_in_array = unsafe {
                        self.chunk_grid()
                            .subset_unchecked(&chunk_indices, self.shape())
                            .unwrap()
                    };
                    let chunk_subset_in_array_subset =
                        unsafe { chunk_subset_in_array.in_subset_unchecked(array_subset) };
                    let array_subset_in_chunk_subset =
                        unsafe { array_subset.in_subset_unchecked(&chunk_subset_in_array) };
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
        } else {
            for chunk_indices in chunks.iter_indices() {
                let chunk_subset_in_array = unsafe {
                    self.chunk_grid()
                        .subset_unchecked(&chunk_indices, self.shape())
                        .unwrap()
                };
                let chunk_subset_in_array_subset =
                    unsafe { chunk_subset_in_array.in_subset_unchecked(array_subset) };
                let chunk_subset_bytes = unsafe {
                    chunk_subset_in_array_subset.extract_bytes_unchecked(
                        &subset_bytes,
                        array_subset.shape(),
                        element_size,
                    )
                };
                let array_subset_in_chunk_subset =
                    unsafe { array_subset.in_subset_unchecked(&chunk_subset_in_array) };
                self.async_store_chunk_subset(
                    &chunk_indices,
                    &array_subset_in_chunk_subset,
                    chunk_subset_bytes,
                )
                .await?;
            }
        }
        Ok(())
    }

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
    pub async fn async_store_array_subset(
        &self,
        array_subset: &ArraySubset,
        subset_bytes: Vec<u8>,
    ) -> Result<(), ArrayError> {
        self._async_store_array_subset(array_subset, subset_bytes, false)
            .await
    }

    /// Parallel version of [`Array::store_array_subset`].
    #[allow(clippy::missing_panics_doc, clippy::missing_errors_doc)]
    pub async fn async_par_store_array_subset(
        &self,
        array_subset: &ArraySubset,
        subset_bytes: Vec<u8>,
    ) -> Result<(), ArrayError> {
        self._async_store_array_subset(array_subset, subset_bytes, true)
            .await
    }

    async fn _async_store_array_subset_elements<T: TriviallyTransmutable>(
        &self,
        array_subset: &ArraySubset,
        subset_elements: Vec<T>,
        parallel: bool,
    ) -> Result<(), ArrayError> {
        if self.data_type.size() != std::mem::size_of::<T>() {
            return Err(ArrayError::IncompatibleElementSize(
                self.data_type.size(),
                std::mem::size_of::<T>(),
            ));
        }

        let subset_bytes = safe_transmute_to_bytes_vec(subset_elements);
        self._async_store_array_subset(array_subset, subset_bytes, parallel)
            .await
    }

    /// Encode `subset_elements` and store in `array_subset`.
    ///
    /// Prefer to use [`store_chunk`](Array<WritableStorageTraits>::store_chunk) since this will decode and encode each chunk intersecting `array_subset`.
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if
    ///  - the size of `T` does not match the data type size, or
    ///  - a [`store_array_subset`](Array::store_array_subset) error condition is met.
    pub async fn async_store_array_subset_elements<T: TriviallyTransmutable>(
        &self,
        array_subset: &ArraySubset,
        subset_elements: Vec<T>,
    ) -> Result<(), ArrayError> {
        self._async_store_array_subset_elements(array_subset, subset_elements, false)
            .await
    }

    /// Parallel version of [`Array::store_array_subset_elements`].
    #[allow(clippy::missing_panics_doc, clippy::missing_errors_doc)]
    pub async fn async_par_store_array_subset_elements<T: TriviallyTransmutable>(
        &self,
        array_subset: &ArraySubset,
        subset_elements: Vec<T>,
    ) -> Result<(), ArrayError> {
        self._async_store_array_subset_elements(array_subset, subset_elements, true)
            .await
    }

    #[cfg(feature = "ndarray")]
    async fn _async_store_array_subset_ndarray<T: safe_transmute::TriviallyTransmutable>(
        &self,
        subset_start: &[u64],
        subset_array: &ndarray::ArrayViewD<'_, T>,
        parallel: bool,
    ) -> Result<(), ArrayError> {
        if subset_start.len() != self.chunk_grid().dimensionality() {
            return Err(crate::array_subset::IncompatibleDimensionalityError::new(
                subset_start.len(),
                self.chunk_grid().dimensionality(),
            )
            .into());
        } else if subset_array.shape().len() != self.chunk_grid().dimensionality() {
            return Err(crate::array_subset::IncompatibleDimensionalityError::new(
                subset_array.shape().len(),
                self.chunk_grid().dimensionality(),
            )
            .into());
        }

        let subset = unsafe {
            ArraySubset::new_with_start_shape_unchecked(
                subset_start.to_vec(),
                subset_array.shape().iter().map(|u| *u as u64).collect(),
            )
        };
        let array_standard = subset_array.as_standard_layout();
        let elements = array_standard.into_owned().into_raw_vec();
        self._async_store_array_subset_elements(&subset, elements, parallel)
            .await
    }

    #[cfg(feature = "ndarray")]
    /// Encode `subset_array` and store in the array subset starting at `subset_start`.
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if a [`store_array_subset_elements`](Array::store_array_subset_elements) error condition is met.
    #[allow(clippy::missing_panics_doc)]
    pub async fn async_store_array_subset_ndarray<T: safe_transmute::TriviallyTransmutable>(
        &self,
        subset_start: &[u64],
        subset_array: &ndarray::ArrayViewD<'_, T>,
    ) -> Result<(), ArrayError> {
        self._async_store_array_subset_ndarray(subset_start, subset_array, false)
            .await
    }

    #[cfg(feature = "ndarray")]
    /// Parallel version of [`Array::store_array_subset_ndarray`].
    #[allow(clippy::missing_panics_doc, clippy::missing_errors_doc)]
    pub async fn async_par_store_array_subset_ndarray<T: safe_transmute::TriviallyTransmutable>(
        &self,
        subset_start: &[u64],
        subset_array: &ndarray::ArrayViewD<'_, T>,
    ) -> Result<(), ArrayError> {
        self._async_store_array_subset_ndarray(subset_start, subset_array, true)
            .await
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
                let chunk_mutex = self.async_chunk_mutex(chunk_indices).await;
                let _lock = chunk_mutex.lock();

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
    pub async fn async_store_chunk_subset_elements<T: TriviallyTransmutable>(
        &self,
        chunk_indices: &[u64],
        chunk_subset: &ArraySubset,
        chunk_subset_elements: Vec<T>,
    ) -> Result<(), ArrayError> {
        if self.data_type.size() != std::mem::size_of::<T>() {
            return Err(ArrayError::IncompatibleElementSize(
                self.data_type.size(),
                std::mem::size_of::<T>(),
            ));
        }

        let chunk_subset_bytes = safe_transmute_to_bytes_vec(chunk_subset_elements);
        self.async_store_chunk_subset(chunk_indices, chunk_subset, chunk_subset_bytes)
            .await
    }

    #[cfg(feature = "ndarray")]
    /// Encode `chunk_subset_array` and store in `chunk_subset` of the chunk in the subset starting at `chunk_subset_start`.
    ///
    /// Prefer to use [`store_chunk`](Array<WritableStorageTraits>::store_chunk) since this will decode the chunk before updating it and reencoding it.
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if a [`store_chunk_subset_elements`](Array::store_chunk_subset_elements) error condition is met.
    #[allow(clippy::missing_panics_doc)]
    pub async fn async_store_chunk_subset_ndarray<T: TriviallyTransmutable>(
        &self,
        chunk_indices: &[u64],
        chunk_subset_start: &[u64],
        chunk_subset_array: &ndarray::ArrayViewD<'_, T>,
    ) -> Result<(), ArrayError> {
        if chunk_subset_start.len() != self.chunk_grid().dimensionality() {
            return Err(crate::array_subset::IncompatibleDimensionalityError::new(
                chunk_subset_start.len(),
                self.chunk_grid().dimensionality(),
            )
            .into());
        } else if chunk_subset_array.shape().len() != self.chunk_grid().dimensionality() {
            return Err(crate::array_subset::IncompatibleDimensionalityError::new(
                chunk_subset_array.shape().len(),
                self.chunk_grid().dimensionality(),
            )
            .into());
        }

        let subset = unsafe {
            ArraySubset::new_with_start_shape_unchecked(
                chunk_subset_start.to_vec(),
                chunk_subset_array
                    .shape()
                    .iter()
                    .map(|u| *u as u64)
                    .collect(),
            )
        };
        let array_standard = chunk_subset_array.as_standard_layout();
        let elements = array_standard.to_owned().into_raw_vec();
        self.async_store_chunk_subset_elements(chunk_indices, &subset, elements)
            .await
    }
}
