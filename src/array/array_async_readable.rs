use std::sync::Arc;

use futures::{stream::FuturesUnordered, StreamExt};
use itertools::Itertools;

use crate::{
    array_subset::ArraySubset,
    node::NodePath,
    storage::{data_key, meta_key, AsyncReadableStorageTraits, StorageHandle},
};

use super::{
    codec::{
        ArrayCodecTraits, ArrayToBytesCodecTraits, AsyncArrayPartialDecoderTraits,
        AsyncStoragePartialDecoder,
    },
    unsafe_cell_slice::UnsafeCellSlice,
    Array, ArrayCreateError, ArrayError, ArrayMetadata,
};

// FIXME: Matches array_retrieve_elements with await
macro_rules! array_async_retrieve_elements {
    ( $self:expr, $func:ident($($arg:tt)*) ) => {
        if $self.data_type.size() != std::mem::size_of::<T>() {
            Err(ArrayError::IncompatibleElementSize(
                $self.data_type.size(),
                std::mem::size_of::<T>(),
            ))
        } else {
            let bytes = $self.$func($($arg)*).await?;
            let elements = crate::array::transmute_from_bytes_vec::<T>(bytes);
            Ok(elements)
        }
    };
}

// FIXME: Matches array_retrieve_ndarray with await
#[cfg(feature = "ndarray")]
macro_rules! array_async_retrieve_ndarray {
    ( $self:expr, $shape:expr, $func:ident($($arg:tt)*) ) => {
        if $self.data_type.size() != std::mem::size_of::<T>() {
            Err(ArrayError::IncompatibleElementSize(
                $self.data_type.size(),
                std::mem::size_of::<T>(),
            ))
        } else {
            let elements = $self.$func($($arg)*).await?;
            let length = elements.len();
            ndarray::ArrayD::<T>::from_shape_vec(
                super::iter_u64_to_usize($shape.iter()),
                elements,
            )
            .map_err(|_| {
                ArrayError::CodecError(crate::array::codec::CodecError::UnexpectedChunkDecodedSize(
                    length * std::mem::size_of::<T>(),
                    $shape.iter().product::<u64>() * std::mem::size_of::<T>() as u64,
                ))
            })
        }
    };
}

impl<TStorage: ?Sized + AsyncReadableStorageTraits> Array<TStorage> {
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
    pub async fn async_retrieve_chunk(&self, chunk_indices: &[u64]) -> Result<Vec<u8>, ArrayError> {
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
                Ok(chunk_decoded)
            } else {
                Err(ArrayError::UnexpectedChunkDecodedSize(
                    chunk_decoded.len(),
                    chunk_decoded_size,
                ))
            }
        } else {
            let fill_value = chunk_representation.fill_value().as_ne_bytes();
            Ok(fill_value.repeat(chunk_representation.num_elements_usize()))
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
    pub async fn async_retrieve_chunk_elements<T: bytemuck::Pod + Send + Sync>(
        &self,
        chunk_indices: &[u64],
    ) -> Result<Vec<T>, ArrayError> {
        array_async_retrieve_elements!(self, async_retrieve_chunk(chunk_indices))
    }

    #[cfg(feature = "ndarray")]
    /// Read and decode the chunk at `chunk_indices` into an [`ndarray::ArrayD`].
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
    pub async fn async_retrieve_chunk_ndarray<T: bytemuck::Pod + Send + Sync>(
        &self,
        chunk_indices: &[u64],
    ) -> Result<ndarray::ArrayD<T>, ArrayError> {
        let shape = self
            .chunk_grid()
            .chunk_shape(chunk_indices, self.shape())?
            .ok_or_else(|| ArrayError::InvalidChunkGridIndicesError(chunk_indices.to_vec()))?;
        array_async_retrieve_ndarray!(self, shape, async_retrieve_chunk_elements(chunk_indices))
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
    pub async fn async_retrieve_chunks(&self, chunks: &ArraySubset) -> Result<Vec<u8>, ArrayError> {
        if chunks.dimensionality() != self.chunk_grid().dimensionality() {
            return Err(ArrayError::InvalidArraySubset(
                chunks.clone(),
                self.shape().to_vec(),
            ));
        }

        let array_subset = self.chunks_subset(chunks)?;

        // Retrieve chunk bytes
        let num_chunks = chunks.num_elements();
        match num_chunks {
            0 => Ok(vec![]),
            1 => {
                let chunk_indices = chunks.start();
                self.async_retrieve_chunk(chunk_indices).await
            }
            _ => {
                // Decode chunks and copy to output
                let size_output =
                    usize::try_from(array_subset.num_elements() * self.data_type().size() as u64)
                        .unwrap();
                let mut output = vec![core::mem::MaybeUninit::<u8>::uninit(); size_output];
                let output_slice = unsafe {
                    std::slice::from_raw_parts_mut(output.as_mut_ptr().cast::<u8>(), size_output)
                };
                let output_slice = UnsafeCellSlice::new(output_slice);
                let indices = chunks.iter_indices().collect_vec();
                let mut futures = indices
                    .iter()
                    .map(|chunk_indices| {
                        self._async_decode_chunk_into_array_subset(
                            chunk_indices,
                            &array_subset,
                            unsafe { output_slice.get() },
                        )
                    })
                    .collect::<FuturesUnordered<_>>();
                while let Some(item) = futures.next().await {
                    item?;
                }
                #[allow(clippy::transmute_undefined_repr)]
                let output: Vec<u8> = unsafe { core::mem::transmute(output) };
                Ok(output)
            }
        }
    }

    /// Read and decode the chunk at `chunk_indices` into a vector of its elements.
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if the size of `T` does not match the data type size or a [`Array::async_retrieve_chunks`] error condition is met.
    pub async fn async_retrieve_chunks_elements<T: bytemuck::Pod + Send + Sync>(
        &self,
        chunks: &ArraySubset,
    ) -> Result<Vec<T>, ArrayError> {
        array_async_retrieve_elements!(self, async_retrieve_chunks(chunks))
    }

    /// Read and decode the chunk at `chunk_indices` into an [`ndarray::ArrayD`].
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if the size of `T` does not match the data type size or a [`Array::async_retrieve_chunks`] error condition is met.
    pub async fn async_retrieve_chunks_ndarray<T: bytemuck::Pod + Send + Sync>(
        &self,
        chunks: &ArraySubset,
    ) -> Result<ndarray::ArrayD<T>, ArrayError> {
        let array_subset = self.chunks_subset(chunks)?;
        array_async_retrieve_ndarray!(
            self,
            array_subset.shape(),
            async_retrieve_chunks_elements(chunks)
        )
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
        let overlap = unsafe { array_subset.overlap_unchecked(&chunk_subset_in_array) };
        let array_subset_in_chunk_subset =
            unsafe { overlap.relative_to_unchecked(chunk_subset_in_array.start()) };
        let decoded_bytes = self
            .async_retrieve_chunk_subset(chunk_indices, &array_subset_in_chunk_subset)
            .await?;

        // Copy decoded bytes to the output
        let element_size = self.data_type().size() as u64;
        let chunk_subset_in_array_subset =
            unsafe { overlap.relative_to_unchecked(array_subset.start()) };
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
    pub async fn async_retrieve_array_subset(
        &self,
        array_subset: &ArraySubset,
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
                let mut output = vec![core::mem::MaybeUninit::<u8>::uninit(); size_output];
                let output_slice = unsafe {
                    std::slice::from_raw_parts_mut(output.as_mut_ptr().cast::<u8>(), size_output)
                };
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
                            let overlap =
                                unsafe { array_subset.overlap_unchecked(&chunk_subset_in_array) };
                            let array_subset_in_chunk_subset = unsafe {
                                overlap.relative_to_unchecked(chunk_subset_in_array.start())
                            };

                            let storage_handle = Arc::new(StorageHandle::new(&*self.storage));
                            let storage_transformer = self
                                .storage_transformers()
                                .create_async_readable_transformer(storage_handle);
                            let input_handle = Box::new(AsyncStoragePartialDecoder::new(
                                storage_transformer,
                                data_key(self.path(), &chunk_indices, self.chunk_key_encoding()),
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
                            let chunk_subset_in_array_subset =
                                unsafe { overlap.relative_to_unchecked(array_subset.start()) };
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
                                let length = usize::try_from(num_elements * element_size).unwrap();
                                debug_assert!((output_offset + length) <= size_output);
                                debug_assert!((decoded_offset + length) <= decoded_bytes.len());
                                unsafe {
                                    let output_slice = output_slice.get();
                                    output_slice[output_offset..output_offset + length]
                                        .copy_from_slice(
                                            &decoded_bytes[decoded_offset..decoded_offset + length],
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
                #[allow(clippy::transmute_undefined_repr)]
                let output: Vec<u8> = unsafe { core::mem::transmute(output) };
                Ok(output)
            }
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
    pub async fn async_retrieve_array_subset_elements<T: bytemuck::Pod + Send + Sync>(
        &self,
        array_subset: &ArraySubset,
    ) -> Result<Vec<T>, ArrayError> {
        array_async_retrieve_elements!(self, async_retrieve_array_subset(array_subset))
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
    pub async fn async_retrieve_array_subset_ndarray<T: bytemuck::Pod + Send + Sync>(
        &self,
        array_subset: &ArraySubset,
    ) -> Result<ndarray::ArrayD<T>, ArrayError> {
        array_async_retrieve_ndarray!(
            self,
            array_subset.shape(),
            async_retrieve_array_subset_elements(array_subset)
        )
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
    ) -> Result<Vec<u8>, ArrayError> {
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
            Ok(decoded_bytes.concat())
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
    pub async fn async_retrieve_chunk_subset_elements<T: bytemuck::Pod + Send + Sync>(
        &self,
        chunk_indices: &[u64],
        chunk_subset: &ArraySubset,
    ) -> Result<Vec<T>, ArrayError> {
        array_async_retrieve_elements!(
            self,
            async_retrieve_chunk_subset(chunk_indices, chunk_subset)
        )
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
    pub async fn async_retrieve_chunk_subset_ndarray<T: bytemuck::Pod + Send + Sync>(
        &self,
        chunk_indices: &[u64],
        chunk_subset: &ArraySubset,
    ) -> Result<ndarray::ArrayD<T>, ArrayError> {
        array_async_retrieve_ndarray!(
            self,
            chunk_subset.shape(),
            async_retrieve_chunk_subset_elements(chunk_indices, chunk_subset)
        )
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
