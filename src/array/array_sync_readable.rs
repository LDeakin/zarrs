use std::sync::Arc;

use rayon::iter::{IntoParallelIterator, ParallelIterator};
use rayon_iter_concurrent_limit::iter_concurrent_limit;

use crate::{
    array::concurrency::RecommendedConcurrency,
    array_subset::ArraySubset,
    config::global_config,
    node::NodePath,
    storage::{data_key, meta_key, ReadableStorageTraits, StorageError, StorageHandle},
};

use super::{
    codec::{
        ArrayCodecTraits, ArrayPartialDecoderTraits, ArrayToBytesCodecTraits, DecodeOptions,
        PartialDecoderOptions, StoragePartialDecoder,
    },
    concurrency::calc_concurrent_limits,
    transmute_from_bytes_vec,
    unsafe_cell_slice::UnsafeCellSlice,
    validate_element_size, Array, ArrayCreateError, ArrayError, ArrayMetadata,
};

#[cfg(feature = "ndarray")]
use super::elements_to_ndarray;

impl<TStorage: ?Sized + ReadableStorageTraits + 'static> Array<TStorage> {
    /// Create an array in `storage` at `path`. The metadata is read from the store.
    ///
    /// # Errors
    /// Returns [`ArrayCreateError`] if there is a storage error or any metadata is invalid.
    pub fn new(storage: Arc<TStorage>, path: &str) -> Result<Self, ArrayCreateError> {
        let node_path = NodePath::new(path)?;
        let key = meta_key(&node_path);
        let metadata: ArrayMetadata = serde_json::from_slice(
            &storage
                .get(&key)?
                .ok_or(ArrayCreateError::MissingMetadata)?,
        )
        .map_err(|err| StorageError::InvalidMetadata(key, err.to_string()))?;
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
    pub fn retrieve_chunk_if_exists_opt(
        &self,
        chunk_indices: &[u64],
        options: &DecodeOptions,
    ) -> Result<Option<Vec<u8>>, ArrayError> {
        let storage_handle = Arc::new(StorageHandle::new(self.storage.clone()));
        let storage_transformer = self
            .storage_transformers()
            .create_readable_transformer(storage_handle);
        let chunk_encoded = crate::storage::retrieve_chunk(
            &*storage_transformer,
            self.path(),
            chunk_indices,
            self.chunk_key_encoding(),
        )
        .map_err(ArrayError::StorageError)?;
        if let Some(chunk_encoded) = chunk_encoded {
            let chunk_representation = self.chunk_array_representation(chunk_indices)?;
            let chunk_decoded = self
                .codecs()
                .decode_opt(chunk_encoded, &chunk_representation, options)
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
    pub fn retrieve_chunk_if_exists(
        &self,
        chunk_indices: &[u64],
    ) -> Result<Option<Vec<u8>>, ArrayError> {
        self.retrieve_chunk_if_exists_opt(chunk_indices, &DecodeOptions::default())
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
    pub fn retrieve_chunk_opt(
        &self,
        chunk_indices: &[u64],
        options: &DecodeOptions,
    ) -> Result<Vec<u8>, ArrayError> {
        let chunk = self.retrieve_chunk_if_exists_opt(chunk_indices, options)?;
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
    pub fn retrieve_chunk(&self, chunk_indices: &[u64]) -> Result<Vec<u8>, ArrayError> {
        self.retrieve_chunk_opt(chunk_indices, &DecodeOptions::default())
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
    pub fn retrieve_chunk_elements_if_exists_opt<T: bytemuck::Pod>(
        &self,
        chunk_indices: &[u64],
        options: &DecodeOptions,
    ) -> Result<Option<Vec<T>>, ArrayError> {
        validate_element_size::<T>(self.data_type())?;
        let bytes = self.retrieve_chunk_if_exists_opt(chunk_indices, options)?;
        Ok(bytes.map(|bytes| transmute_from_bytes_vec::<T>(bytes)))
    }

    /// Read and decode the chunk at `chunk_indices` into a vector of its elements if it exists (default options).
    #[allow(clippy::missing_panics_doc, clippy::missing_errors_doc)]
    pub fn retrieve_chunk_elements_if_exists<T: bytemuck::Pod>(
        &self,
        chunk_indices: &[u64],
    ) -> Result<Option<Vec<T>>, ArrayError> {
        self.retrieve_chunk_elements_if_exists_opt(chunk_indices, &DecodeOptions::default())
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
    pub fn retrieve_chunk_elements_opt<T: bytemuck::Pod>(
        &self,
        chunk_indices: &[u64],
        options: &DecodeOptions,
    ) -> Result<Vec<T>, ArrayError> {
        validate_element_size::<T>(self.data_type())?;
        let bytes = self.retrieve_chunk_opt(chunk_indices, options)?;
        Ok(transmute_from_bytes_vec::<T>(bytes))
    }

    /// Read and decode the chunk at `chunk_indices` into a vector of its elements or the fill value if it does not exist (default options).
    #[allow(clippy::missing_panics_doc, clippy::missing_errors_doc)]
    pub fn retrieve_chunk_elements<T: bytemuck::Pod>(
        &self,
        chunk_indices: &[u64],
    ) -> Result<Vec<T>, ArrayError> {
        self.retrieve_chunk_elements_opt(chunk_indices, &DecodeOptions::default())
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
    pub fn retrieve_chunk_ndarray_if_exists_opt<T: bytemuck::Pod>(
        &self,
        chunk_indices: &[u64],
        options: &DecodeOptions,
    ) -> Result<Option<ndarray::ArrayD<T>>, ArrayError> {
        // validate_element_size::<T>(self.data_type())?; // in retrieve_chunk_elements_if_exists
        let shape = self
            .chunk_grid()
            .chunk_shape_u64(chunk_indices, self.shape())?
            .ok_or_else(|| ArrayError::InvalidChunkGridIndicesError(chunk_indices.to_vec()))?;
        let elements = self.retrieve_chunk_elements_if_exists_opt::<T>(chunk_indices, options)?;
        if let Some(elements) = elements {
            Ok(Some(elements_to_ndarray(&shape, elements)?))
        } else {
            Ok(None)
        }
    }

    #[cfg(feature = "ndarray")]
    /// Read and decode the chunk at `chunk_indices` into an [`ndarray::ArrayD`] if it exists (default options).
    #[allow(clippy::missing_panics_doc, clippy::missing_errors_doc)]
    pub fn retrieve_chunk_ndarray_if_exists<T: bytemuck::Pod>(
        &self,
        chunk_indices: &[u64],
    ) -> Result<Option<ndarray::ArrayD<T>>, ArrayError> {
        self.retrieve_chunk_ndarray_if_exists_opt(chunk_indices, &DecodeOptions::default())
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
    pub fn retrieve_chunk_ndarray_opt<T: bytemuck::Pod>(
        &self,
        chunk_indices: &[u64],
        options: &DecodeOptions,
    ) -> Result<ndarray::ArrayD<T>, ArrayError> {
        // validate_element_size::<T>(self.data_type())?; // in retrieve_chunk_elements
        let shape = self
            .chunk_grid()
            .chunk_shape_u64(chunk_indices, self.shape())?
            .ok_or_else(|| ArrayError::InvalidChunkGridIndicesError(chunk_indices.to_vec()))?;
        elements_to_ndarray(
            &shape,
            self.retrieve_chunk_elements_opt::<T>(chunk_indices, options)?,
        )
    }

    #[cfg(feature = "ndarray")]
    /// Read and decode the chunk at `chunk_indices` into an [`ndarray::ArrayD`]. It is filled with the fill value if it does not exist (default options).
    #[allow(clippy::missing_panics_doc, clippy::missing_errors_doc)]
    pub fn retrieve_chunk_ndarray<T: bytemuck::Pod>(
        &self,
        chunk_indices: &[u64],
    ) -> Result<ndarray::ArrayD<T>, ArrayError> {
        self.retrieve_chunk_ndarray_opt(chunk_indices, &DecodeOptions::default())
    }

    /// Read and decode the chunks at `chunks` into their bytes.
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if
    ///  - any chunk indices in `chunks` are invalid,
    ///  - there is a codec decoding error, or
    ///  - an underlying store error.
    ///
    /// # Panics
    /// Panics if the number of array elements in the chunk exceeds `usize::MAX`.
    pub fn retrieve_chunks_opt(
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

        let array_subset = self.chunks_subset(chunks)?;

        // Retrieve chunk bytes
        let num_chunks = chunks.num_elements();
        match num_chunks {
            0 => Ok(vec![]),
            1 => {
                let chunk_indices = chunks.start();
                self.retrieve_chunk_opt(chunk_indices, options)
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
                {
                    let output = UnsafeCellSlice::new(output_slice);
                    // FIXME: constrain concurrency based on codec
                    let indices = chunks.indices();
                    rayon_iter_concurrent_limit::iter_concurrent_limit!(
                        options.concurrent_limit(),
                        indices.into_par_iter(),
                        try_for_each,
                        |chunk_indices| {
                            self._decode_chunk_into_array_subset(
                                &chunk_indices,
                                &array_subset,
                                unsafe { output.get() },
                                options,
                            )
                        }
                    )?;
                }
                #[allow(clippy::transmute_undefined_repr)]
                let output: Vec<u8> = unsafe { core::mem::transmute(output) };
                Ok(output)
            }
        }
    }

    /// Read and decode the chunks at `chunks` into their bytes (default options).
    ///
    /// # Errors
    /// See [`Array::retrieve_chunks_opt`].
    /// # Panics
    /// See [`Array::retrieve_chunks_opt`].
    pub fn retrieve_chunks(&self, chunks: &ArraySubset) -> Result<Vec<u8>, ArrayError> {
        self.retrieve_chunks_opt(chunks, &DecodeOptions::default())
    }

    /// Read and decode the chunks at `chunks` into a vector of its elements.
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if any chunk indices in `chunks` are invalid or an error condition in [`Array::retrieve_chunks_opt`].
    ///
    /// # Panics
    /// Panics if the number of array elements in the chunks exceeds `usize::MAX`.
    pub fn retrieve_chunks_elements_opt<T: bytemuck::Pod>(
        &self,
        chunks: &ArraySubset,
        options: &DecodeOptions,
    ) -> Result<Vec<T>, ArrayError> {
        validate_element_size::<T>(self.data_type())?;
        let bytes = self.retrieve_chunks_opt(chunks, options)?;
        Ok(transmute_from_bytes_vec::<T>(bytes))
    }

    /// Read and decode the chunks at `chunks` into a vector of its elements with default options
    ///
    /// # Errors
    /// See [`Array::retrieve_chunks_elements_opt`].
    /// # Panics
    /// See [`Array::retrieve_chunks_elements_opt`].
    pub fn retrieve_chunks_elements<T: bytemuck::Pod>(
        &self,
        chunks: &ArraySubset,
    ) -> Result<Vec<T>, ArrayError> {
        self.retrieve_chunks_elements_opt(chunks, &DecodeOptions::default())
    }

    #[cfg(feature = "ndarray")]
    /// Read and decode the chunks at `chunks` into an [`ndarray::ArrayD`].
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if any chunk indices in `chunks` are invalid or an error condition in [`Array::retrieve_chunks_elements_opt`].
    ///
    /// # Panics
    /// Panics if the number of array elements in the chunks exceeds `usize::MAX`.
    pub fn retrieve_chunks_ndarray_opt<T: bytemuck::Pod>(
        &self,
        chunks: &ArraySubset,
        options: &DecodeOptions,
    ) -> Result<ndarray::ArrayD<T>, ArrayError> {
        // validate_element_size::<T>(self.data_type())?; // in retrieve_chunks_elements_opt
        let array_subset = self.chunks_subset(chunks)?;
        let elements = self.retrieve_chunks_elements_opt::<T>(chunks, options)?;
        elements_to_ndarray(array_subset.shape(), elements)
    }

    #[cfg(feature = "ndarray")]
    /// Read and decode the chunks at `chunks` into an [`ndarray::ArrayD`] (default options).
    ///
    /// # Errors
    /// See [`Array::retrieve_chunks_ndarray_opt`].
    /// # Panics
    /// See [`Array::retrieve_chunks_ndarray_opt`].
    pub fn retrieve_chunks_ndarray<T: bytemuck::Pod>(
        &self,
        chunks: &ArraySubset,
    ) -> Result<ndarray::ArrayD<T>, ArrayError> {
        self.retrieve_chunks_ndarray_opt(chunks, &DecodeOptions::default())
    }

    fn _decode_chunk_into_array_subset(
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
        let decoded_bytes =
            self.retrieve_chunk_subset_opt(chunk_indices, &array_subset_in_chunk_subset, options)?;

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
    pub fn retrieve_array_subset_opt(
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
                    self.retrieve_chunk_opt(chunk_indices, options)
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
                    self._decode_chunk_into_array_subset(
                        chunk_indices,
                        array_subset,
                        output_slice,
                        options,
                    )?;
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

                // Calc self/internal concurrent limits
                let chunk_representation =
                    self.chunk_array_representation(&vec![0; self.chunk_grid().dimensionality()])?;
                let (self_concurrent_limit, codec_concurrent_limit) = calc_concurrent_limits(
                    options.concurrent_limit(),
                    &RecommendedConcurrency::new_minimum(
                        global_config().chunk_concurrent_minimum(),
                    ),
                    &self
                        .codecs()
                        .recommended_concurrency(&chunk_representation)?,
                );
                let mut codec_options = DecodeOptions::default();
                codec_options.set_concurrent_limit(codec_concurrent_limit);
                // println!("self_concurrent_limit {self_concurrent_limit:?} codec_concurrent_limit {codec_concurrent_limit:?}"); // FIXME: log this

                {
                    let output = UnsafeCellSlice::new(output_slice);
                    // FIXME: Constrain concurrency here based on parallelism internally vs externally
                    let indices = chunks.indices();
                    iter_concurrent_limit!(
                        self_concurrent_limit,
                        indices.into_par_iter(),
                        try_for_each,
                        |chunk_indices| {
                            self._decode_chunk_into_array_subset(
                                &chunk_indices,
                                array_subset,
                                unsafe { output.get() },
                                &codec_options,
                            )
                        }
                    )?;
                }
                #[allow(clippy::transmute_undefined_repr)]
                let output: Vec<u8> = unsafe { core::mem::transmute(output) };
                Ok(output)
            }
        }
    }

    /// Read and decode the `array_subset` of array into its bytes.
    ///
    /// # Errors
    /// See [`Array::retrieve_array_subset_opt`].
    /// # Panics
    /// See [`Array::retrieve_array_subset_opt`].
    pub fn retrieve_array_subset(&self, array_subset: &ArraySubset) -> Result<Vec<u8>, ArrayError> {
        self.retrieve_array_subset_opt(array_subset, &DecodeOptions::default())
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
    pub fn retrieve_array_subset_elements_opt<T: bytemuck::Pod>(
        &self,
        array_subset: &ArraySubset,
        options: &DecodeOptions,
    ) -> Result<Vec<T>, ArrayError> {
        validate_element_size::<T>(self.data_type())?;
        let bytes = self.retrieve_array_subset_opt(array_subset, options)?;
        Ok(transmute_from_bytes_vec::<T>(bytes))
    }

    /// Read and decode the `array_subset` of array into a vector of its elements (default options).
    ///
    /// # Errors
    /// See [`Array::retrieve_array_subset_elements_opt`].
    /// # Panics
    /// See [`Array::retrieve_array_subset_elements_opt`].
    pub fn retrieve_array_subset_elements<T: bytemuck::Pod>(
        &self,
        array_subset: &ArraySubset,
    ) -> Result<Vec<T>, ArrayError> {
        self.retrieve_array_subset_elements_opt(array_subset, &DecodeOptions::default())
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
    pub fn retrieve_array_subset_ndarray_opt<T: bytemuck::Pod>(
        &self,
        array_subset: &ArraySubset,
        options: &DecodeOptions,
    ) -> Result<ndarray::ArrayD<T>, ArrayError> {
        // validate_element_size::<T>(self.data_type())?; // in retrieve_array_subset_elements_opt
        let elements = self.retrieve_array_subset_elements_opt::<T>(array_subset, options)?;
        elements_to_ndarray(array_subset.shape(), elements)
    }

    #[cfg(feature = "ndarray")]
    /// Read and decode the `array_subset` of array into an [`ndarray::ArrayD`] (default options).
    ///
    /// # Errors
    /// See [`Array::retrieve_array_subset_ndarray`].
    /// # Panics
    /// See [`Array::retrieve_array_subset_ndarray`].
    pub fn retrieve_array_subset_ndarray<T: bytemuck::Pod>(
        &self,
        array_subset: &ArraySubset,
    ) -> Result<ndarray::ArrayD<T>, ArrayError> {
        self.retrieve_array_subset_ndarray_opt(array_subset, &DecodeOptions::default())
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
    pub fn retrieve_chunk_subset_opt(
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

        let decoded_bytes = if chunk_subset.start().iter().all(|&o| o == 0)
            && chunk_subset.shape() == chunk_representation.shape_u64()
        {
            // Fast path if `chunk_subset` encompasses the whole chunk
            self.retrieve_chunk(chunk_indices)?
        } else {
            let storage_handle = Arc::new(StorageHandle::new(self.storage.clone()));
            let storage_transformer = self
                .storage_transformers()
                .create_readable_transformer(storage_handle);
            let input_handle = Box::new(StoragePartialDecoder::new(
                storage_transformer,
                data_key(self.path(), chunk_indices, self.chunk_key_encoding()),
            ));

            self.codecs()
                .partial_decoder_opt(input_handle, &chunk_representation, options)?
                .partial_decode_opt(&[chunk_subset.clone()], options)?
                .concat()
        };

        let total_size = decoded_bytes.len();
        let expected_size = chunk_subset.num_elements_usize() * self.data_type().size();
        if total_size == chunk_subset.num_elements_usize() * self.data_type().size() {
            Ok(decoded_bytes)
        } else {
            Err(ArrayError::UnexpectedChunkDecodedSize(
                total_size,
                expected_size,
            ))
        }
    }

    /// Read and decode the `chunk_subset` of the chunk at `chunk_indices` into its bytes (default options).
    #[allow(clippy::missing_panics_doc, clippy::missing_errors_doc)]
    pub fn retrieve_chunk_subset(
        &self,
        chunk_indices: &[u64],
        chunk_subset: &ArraySubset,
    ) -> Result<Vec<u8>, ArrayError> {
        self.retrieve_chunk_subset_opt(chunk_indices, chunk_subset, &DecodeOptions::default())
    }

    /// Read and decode the `chunk_subset` of the chunk at `chunk_indices` into its elements.
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if:
    ///  - the chunk indices are invalid,
    ///  - the chunk subset is invalid,
    ///  - there is a codec decoding error, or
    ///  - an underlying store error.
    pub fn retrieve_chunk_subset_elements_opt<T: bytemuck::Pod>(
        &self,
        chunk_indices: &[u64],
        chunk_subset: &ArraySubset,
        options: &DecodeOptions,
    ) -> Result<Vec<T>, ArrayError> {
        validate_element_size::<T>(self.data_type())?;
        let bytes = self.retrieve_chunk_subset_opt(chunk_indices, chunk_subset, options)?;
        Ok(transmute_from_bytes_vec::<T>(bytes))
    }

    /// Read and decode the `chunk_subset` of the chunk at `chunk_indices` into its elements (default options).
    #[allow(clippy::missing_panics_doc, clippy::missing_errors_doc)]
    pub fn retrieve_chunk_subset_elements<T: bytemuck::Pod>(
        &self,
        chunk_indices: &[u64],
        chunk_subset: &ArraySubset,
    ) -> Result<Vec<T>, ArrayError> {
        self.retrieve_chunk_subset_elements_opt(
            chunk_indices,
            chunk_subset,
            &DecodeOptions::default(),
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
    pub fn retrieve_chunk_subset_ndarray_opt<T: bytemuck::Pod>(
        &self,
        chunk_indices: &[u64],
        chunk_subset: &ArraySubset,
        options: &DecodeOptions,
    ) -> Result<ndarray::ArrayD<T>, ArrayError> {
        // validate_element_size::<T>(self.data_type())?; // in retrieve_chunk_subset_elements
        let elements =
            self.retrieve_chunk_subset_elements_opt::<T>(chunk_indices, chunk_subset, options)?;
        elements_to_ndarray(chunk_subset.shape(), elements)
    }

    #[cfg(feature = "ndarray")]
    /// Read and decode the `chunk_subset` of the chunk at `chunk_indices` into an [`ndarray::ArrayD`] (default options).
    #[allow(clippy::missing_panics_doc, clippy::missing_errors_doc)]
    pub fn retrieve_chunk_subset_ndarray<T: bytemuck::Pod>(
        &self,
        chunk_indices: &[u64],
        chunk_subset: &ArraySubset,
    ) -> Result<ndarray::ArrayD<T>, ArrayError> {
        self.retrieve_chunk_subset_ndarray_opt(
            chunk_indices,
            chunk_subset,
            &DecodeOptions::default(),
        )
    }

    /// Initialises a partial decoder for the chunk at `chunk_indices`.
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if initialisation of the partial decoder fails.
    pub fn partial_decoder_opt<'a>(
        &'a self,
        chunk_indices: &[u64],
        options: &PartialDecoderOptions,
    ) -> Result<Box<dyn ArrayPartialDecoderTraits + 'a>, ArrayError> {
        let storage_handle = Arc::new(StorageHandle::new(self.storage.clone()));
        let storage_transformer = self
            .storage_transformers()
            .create_readable_transformer(storage_handle);
        let input_handle = Box::new(StoragePartialDecoder::new(
            storage_transformer,
            data_key(self.path(), chunk_indices, self.chunk_key_encoding()),
        ));
        let chunk_representation = self.chunk_array_representation(chunk_indices)?;
        Ok(self
            .codecs()
            .partial_decoder_opt(input_handle, &chunk_representation, options)?)
    }

    /// Initialises a partial decoder for the chunk at `chunk_indices` (default options).
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if initialisation of the partial decoder fails.
    pub fn partial_decoder<'a>(
        &'a self,
        chunk_indices: &[u64],
    ) -> Result<Box<dyn ArrayPartialDecoderTraits + 'a>, ArrayError> {
        self.partial_decoder_opt(chunk_indices, &PartialDecoderOptions::default())
    }
}
