use std::sync::Arc;

use rayon::iter::{IndexedParallelIterator, IntoParallelIterator, ParallelIterator};

use crate::{
    array_subset::ArraySubset,
    node::NodePath,
    storage::{data_key, meta_key, ReadableStorageTraits, StorageError, StorageHandle},
};

use super::{
    codec::{
        options::CodecOptions, ArrayCodecTraits, ArrayPartialDecoderTraits,
        ArrayToBytesCodecTraits, CodecError, StoragePartialDecoder,
    },
    concurrency::concurrency_chunks_and_codec,
    transmute_from_bytes_vec,
    unsafe_cell_slice::UnsafeCellSlice,
    validate_element_size, Array, ArrayCreateError, ArrayError, ArrayMetadata, ArrayView,
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

    /// Read and decode the chunk at `chunk_indices` into its bytes if it exists with default codec options.
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if
    ///  - `chunk_indices` are invalid,
    ///  - there is a codec decoding error, or
    ///  - an underlying store error.
    ///
    /// # Panics
    /// Panics if the number of elements in the chunk exceeds `usize::MAX`.
    pub fn retrieve_chunk_if_exists(
        &self,
        chunk_indices: &[u64],
    ) -> Result<Option<Vec<u8>>, ArrayError> {
        self.retrieve_chunk_if_exists_opt(chunk_indices, &CodecOptions::default())
    }

    /// Read and decode the chunk at `chunk_indices` into a vector of its elements if it exists with default codec options.
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if
    ///  - the size of `T` does not match the data type size,
    ///  - the decoded bytes cannot be transmuted,
    ///  - `chunk_indices` are invalid,
    ///  - there is a codec decoding error, or
    ///  - an underlying store error.
    pub fn retrieve_chunk_elements_if_exists<T: bytemuck::Pod>(
        &self,
        chunk_indices: &[u64],
    ) -> Result<Option<Vec<T>>, ArrayError> {
        self.retrieve_chunk_elements_if_exists_opt(chunk_indices, &CodecOptions::default())
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
    pub fn retrieve_chunk_ndarray_if_exists<T: bytemuck::Pod>(
        &self,
        chunk_indices: &[u64],
    ) -> Result<Option<ndarray::ArrayD<T>>, ArrayError> {
        self.retrieve_chunk_ndarray_if_exists_opt(chunk_indices, &CodecOptions::default())
    }

    /// Read and decode the chunk at `chunk_indices` into its bytes or the fill value if it does not exist with default codec options.
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if
    ///  - `chunk_indices` are invalid,
    ///  - there is a codec decoding error, or
    ///  - an underlying store error.
    ///
    /// # Panics
    /// Panics if the number of elements in the chunk exceeds `usize::MAX`.
    pub fn retrieve_chunk(&self, chunk_indices: &[u64]) -> Result<Vec<u8>, ArrayError> {
        self.retrieve_chunk_opt(chunk_indices, &CodecOptions::default())
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
    pub fn retrieve_chunk_elements<T: bytemuck::Pod>(
        &self,
        chunk_indices: &[u64],
    ) -> Result<Vec<T>, ArrayError> {
        self.retrieve_chunk_elements_opt(chunk_indices, &CodecOptions::default())
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
    pub fn retrieve_chunk_ndarray<T: bytemuck::Pod>(
        &self,
        chunk_indices: &[u64],
    ) -> Result<ndarray::ArrayD<T>, ArrayError> {
        self.retrieve_chunk_ndarray_opt(chunk_indices, &CodecOptions::default())
    }

    /// Retrieve a chunk and output into an existing array.
    ///
    /// # Errors
    /// See [`Array::retrieve_chunk`].
    /// Can also error if the [`ArraySubset`] in `array_view` does not have the same shape as the chunk at `chunk_indices`.
    ///
    /// # Panics
    /// Panics if an offset is larger than `usize::MAX`.
    pub fn retrieve_chunk_into_array_view(
        &self,
        chunk_indices: &[u64],
        array_view: &ArrayView,
    ) -> Result<(), ArrayError> {
        self.retrieve_chunk_into_array_view_opt(chunk_indices, array_view, &CodecOptions::default())
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
    pub fn retrieve_chunks(&self, chunks: &ArraySubset) -> Result<Vec<u8>, ArrayError> {
        self.retrieve_chunks_opt(chunks, &CodecOptions::default())
    }

    /// Read and decode the chunks at `chunks` into a vector of their elements.
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if any chunk indices in `chunks` are invalid or an error condition in [`Array::retrieve_chunks_opt`].
    ///
    /// # Panics
    /// Panics if the number of array elements in the chunks exceeds `usize::MAX`.
    pub fn retrieve_chunks_elements<T: bytemuck::Pod>(
        &self,
        chunks: &ArraySubset,
    ) -> Result<Vec<T>, ArrayError> {
        self.retrieve_chunks_elements_opt(chunks, &CodecOptions::default())
    }

    #[cfg(feature = "ndarray")]
    /// Read and decode the chunks at `chunks` into an [`ndarray::ArrayD`].
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if any chunk indices in `chunks` are invalid or an error condition in [`Array::retrieve_chunks_elements_opt`].
    ///
    /// # Panics
    /// Panics if the number of array elements in the chunks exceeds `usize::MAX`.
    pub fn retrieve_chunks_ndarray<T: bytemuck::Pod>(
        &self,
        chunks: &ArraySubset,
    ) -> Result<ndarray::ArrayD<T>, ArrayError> {
        self.retrieve_chunks_ndarray_opt(chunks, &CodecOptions::default())
    }

    /// Retrieve chunks into an array view.
    ///
    /// # Errors
    /// See [`Array::retrieve_chunks_opt`].
    /// Can also error if the [`ArraySubset`] in `array_view` does not have the same shape as `array_subset`.
    ///
    /// # Panics
    /// Panics if an offset is larger than `usize::MAX`.
    pub fn retrieve_chunks_into_array_view(
        &self,
        chunks: &ArraySubset,
        array_view: &ArrayView,
    ) -> Result<(), ArrayError> {
        self.retrieve_chunks_into_array_view_opt(chunks, array_view, &CodecOptions::default())
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
    pub fn retrieve_chunk_subset(
        &self,
        chunk_indices: &[u64],
        chunk_subset: &ArraySubset,
    ) -> Result<Vec<u8>, ArrayError> {
        self.retrieve_chunk_subset_opt(chunk_indices, chunk_subset, &CodecOptions::default())
    }

    /// Read and decode the `chunk_subset` of the chunk at `chunk_indices` into its elements.
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if:
    ///  - the chunk indices are invalid,
    ///  - the chunk subset is invalid,
    ///  - there is a codec decoding error, or
    ///  - an underlying store error.
    pub fn retrieve_chunk_subset_elements<T: bytemuck::Pod>(
        &self,
        chunk_indices: &[u64],
        chunk_subset: &ArraySubset,
    ) -> Result<Vec<T>, ArrayError> {
        self.retrieve_chunk_subset_elements_opt(
            chunk_indices,
            chunk_subset,
            &CodecOptions::default(),
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
    pub fn retrieve_chunk_subset_ndarray<T: bytemuck::Pod>(
        &self,
        chunk_indices: &[u64],
        chunk_subset: &ArraySubset,
    ) -> Result<ndarray::ArrayD<T>, ArrayError> {
        self.retrieve_chunk_subset_ndarray_opt(
            chunk_indices,
            chunk_subset,
            &CodecOptions::default(),
        )
    }

    /// Retrieve a subset of a chunk and output into an existing array.
    ///
    /// # Errors
    /// See [`Array::retrieve_chunk_subset`].
    /// Can also error if the [`ArraySubset`] in `array_view` does not have the same shape as `chunk_subset`.
    ///
    /// # Panics
    /// Panics if an offset is larger than `usize::MAX`.
    #[allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]
    pub fn retrieve_chunk_subset_into_array_view(
        &self,
        chunk_indices: &[u64],
        chunk_subset: &ArraySubset,
        array_view: &ArrayView,
    ) -> Result<(), ArrayError> {
        self.retrieve_chunk_subset_into_array_view_opt(
            chunk_indices,
            chunk_subset,
            array_view,
            &CodecOptions::default(),
        )
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
    pub fn retrieve_array_subset(&self, array_subset: &ArraySubset) -> Result<Vec<u8>, ArrayError> {
        self.retrieve_array_subset_opt(array_subset, &CodecOptions::default())
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
    pub fn retrieve_array_subset_elements<T: bytemuck::Pod>(
        &self,
        array_subset: &ArraySubset,
    ) -> Result<Vec<T>, ArrayError> {
        self.retrieve_array_subset_elements_opt(array_subset, &CodecOptions::default())
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
    pub fn retrieve_array_subset_ndarray<T: bytemuck::Pod>(
        &self,
        array_subset: &ArraySubset,
    ) -> Result<ndarray::ArrayD<T>, ArrayError> {
        self.retrieve_array_subset_ndarray_opt(array_subset, &CodecOptions::default())
    }

    /// Retrieve an array subset into an array view.
    ///
    /// # Errors
    /// See [`Array::retrieve_array_subset`].
    /// Can also error if the [`ArraySubset`] in `array_view` does not have the same shape as `array_subset`.
    ///
    /// # Panics
    /// Panics if an offset is larger than `usize::MAX`.
    pub fn retrieve_array_subset_into_array_view(
        &self,
        array_subset: &ArraySubset,
        array_view: &ArrayView,
    ) -> Result<(), ArrayError> {
        self.retrieve_array_subset_into_array_view_opt(
            array_subset,
            array_view,
            &CodecOptions::default(),
        )
    }

    /// Initialises a partial decoder for the chunk at `chunk_indices`.
    ///
    /// # Errors
    /// Returns an [`ArrayError`] if initialisation of the partial decoder fails.
    pub fn partial_decoder<'a>(
        &'a self,
        chunk_indices: &[u64],
    ) -> Result<Box<dyn ArrayPartialDecoderTraits + 'a>, ArrayError> {
        self.partial_decoder_opt(chunk_indices, &CodecOptions::default())
    }

    /////////////////////////////////////////////////////////////////////////////
    // Advanced methods
    /////////////////////////////////////////////////////////////////////////////

    /// Explicit options version of [`retrieve_chunk_if_exists`](Array::retrieve_chunk_if_exists).
    #[allow(clippy::missing_errors_doc)]
    pub fn retrieve_chunk_if_exists_opt(
        &self,
        chunk_indices: &[u64],
        options: &CodecOptions,
    ) -> Result<Option<Vec<u8>>, ArrayError> {
        if chunk_indices.len() != self.dimensionality() {
            return Err(ArrayError::InvalidChunkGridIndicesError(
                chunk_indices.to_vec(),
            ));
        }
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
                .decode(chunk_encoded, &chunk_representation, options)
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

    /// Explicit options version of [`retrieve_chunk`](Array::retrieve_chunk).
    #[allow(clippy::missing_errors_doc)]
    pub fn retrieve_chunk_opt(
        &self,
        chunk_indices: &[u64],
        options: &CodecOptions,
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

    /// Explicit options version of [`retrieve_chunk_elements_if_exists`](Array::retrieve_chunk_elements_if_exists).
    #[allow(clippy::missing_errors_doc)]
    pub fn retrieve_chunk_elements_if_exists_opt<T: bytemuck::Pod>(
        &self,
        chunk_indices: &[u64],
        options: &CodecOptions,
    ) -> Result<Option<Vec<T>>, ArrayError> {
        validate_element_size::<T>(self.data_type())?;
        let bytes = self.retrieve_chunk_if_exists_opt(chunk_indices, options)?;
        Ok(bytes.map(|bytes| transmute_from_bytes_vec::<T>(bytes)))
    }

    /// Explicit options version of [`retrieve_chunk_elements`](Array::retrieve_chunk_elements).
    #[allow(clippy::missing_errors_doc)]
    pub fn retrieve_chunk_elements_opt<T: bytemuck::Pod>(
        &self,
        chunk_indices: &[u64],
        options: &CodecOptions,
    ) -> Result<Vec<T>, ArrayError> {
        validate_element_size::<T>(self.data_type())?;
        let bytes = self.retrieve_chunk_opt(chunk_indices, options)?;
        Ok(transmute_from_bytes_vec::<T>(bytes))
    }

    #[cfg(feature = "ndarray")]
    /// Explicit options version of [`retrieve_chunk_ndarray_if_exists`](Array::retrieve_chunk_ndarray_if_exists).
    #[allow(clippy::missing_errors_doc)]
    pub fn retrieve_chunk_ndarray_if_exists_opt<T: bytemuck::Pod>(
        &self,
        chunk_indices: &[u64],
        options: &CodecOptions,
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
    /// Explicit options version of [`retrieve_chunk_ndarray`](Array::retrieve_chunk_ndarray).
    #[allow(clippy::missing_errors_doc)]
    pub fn retrieve_chunk_ndarray_opt<T: bytemuck::Pod>(
        &self,
        chunk_indices: &[u64],
        options: &CodecOptions,
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

    /// Explicit options version of [`retrieve_chunk_into_array_view`](Array::retrieve_chunk_into_array_view).
    #[allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]
    pub fn retrieve_chunk_into_array_view_opt(
        &self,
        chunk_indices: &[u64],
        array_view: &ArrayView,
        options: &CodecOptions,
    ) -> Result<(), ArrayError> {
        let chunk_representation = self.chunk_array_representation(chunk_indices)?;
        let chunk_shape_u64 = chunk_representation.shape_u64();
        if chunk_shape_u64 != array_view.subset().shape() {
            return Err(ArrayError::InvalidArraySubset(
                array_view.subset().clone(),
                chunk_shape_u64,
            ));
        }

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
            self.codecs()
                .decode_into_array_view(&chunk_encoded, &chunk_representation, array_view, options)
                .map_err(ArrayError::CodecError)
        } else {
            // fill array_view with fill value
            let contiguous_indices = unsafe {
                array_view
                    .subset()
                    .contiguous_linearised_indices_unchecked(array_view.array_shape())
            };
            let element_size = chunk_representation.element_size();
            let length = contiguous_indices.contiguous_elements_usize() * element_size;
            let fill = self
                .fill_value()
                .as_ne_bytes()
                .repeat(contiguous_indices.contiguous_elements_usize());
            // FIXME: Par iteration?
            let output = unsafe { array_view.bytes_mut() };
            for (array_subset_element_index, _num_elements) in &contiguous_indices {
                let output_offset =
                    usize::try_from(array_subset_element_index).unwrap() * element_size;
                debug_assert!((output_offset + length) <= output.len());
                output[output_offset..output_offset + length].copy_from_slice(&fill);
            }
            Ok(())
        }
    }

    /// Explicit options version of [`retrieve_chunk_subset_into_array_view`](Array::retrieve_chunk_subset_into_array_view).
    #[allow(clippy::missing_errors_doc)]
    pub fn retrieve_chunk_subset_into_array_view_opt(
        &self,
        chunk_indices: &[u64],
        chunk_subset: &ArraySubset,
        array_view: &ArrayView,
        options: &CodecOptions,
    ) -> Result<(), ArrayError> {
        if chunk_subset.shape() != array_view.subset().shape() {
            return Err(ArrayError::InvalidArraySubset(
                chunk_subset.clone(),
                array_view.subset().shape().to_vec(),
            ));
        }

        let chunk_representation = self.chunk_array_representation(chunk_indices)?;
        if chunk_subset.shape() == chunk_representation.shape_u64() {
            self.retrieve_chunk_into_array_view_opt(chunk_indices, array_view, options)
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
                .partial_decoder(input_handle, &chunk_representation, options)?
                .partial_decode_into_array_view_opt(chunk_subset, array_view, options)
                .map_err(ArrayError::CodecError)
        }
    }

    /// Explicit options version of [`retrieve_chunks`](Array::retrieve_chunks).
    #[allow(clippy::missing_errors_doc)]
    pub fn retrieve_chunks_opt(
        &self,
        chunks: &ArraySubset,
        options: &CodecOptions,
    ) -> Result<Vec<u8>, ArrayError> {
        if chunks.dimensionality() != self.dimensionality() {
            return Err(ArrayError::InvalidArraySubset(
                chunks.clone(),
                self.shape().to_vec(),
            ));
        }

        let array_subset = self.chunks_subset(chunks)?;

        // Retrieve chunk bytes
        let num_chunks = chunks.num_elements_usize();
        match num_chunks {
            0 => Ok(vec![]),
            1 => {
                let chunk_indices = chunks.start();
                self.retrieve_chunk_opt(chunk_indices, options)
            }
            _ => {
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

                // let mut output = vec![0; size_output];
                // let output_slice = output.as_mut_slice();
                let size_output = array_subset.num_elements_usize() * self.data_type().size();
                let mut output = Vec::with_capacity(size_output);
                {
                    let output_slice =
                        UnsafeCellSlice::new_from_vec_with_spare_capacity(&mut output);
                    let indices = chunks.indices();
                    let chunk0_subset = self.chunk_subset(chunks.start())?;
                    indices
                        .into_par_iter()
                        .by_uniform_blocks(indices.len().div_ceil(chunk_concurrent_limit).max(1))
                        .try_for_each(|chunk_indices: Vec<u64>| {
                            let chunk_subset = self.chunk_subset(&chunk_indices)?;
                            let array_view_subset = unsafe {
                                chunk_subset.relative_to_unchecked(chunk0_subset.start())
                            };
                            self.retrieve_chunk_into_array_view_opt(
                                &chunk_indices,
                                &ArrayView::new(
                                    unsafe { output_slice.get() },
                                    array_subset.shape(),
                                    array_view_subset,
                                )
                                .map_err(|err| CodecError::from(err.to_string()))?,
                                &options,
                            )
                        })?;
                }
                unsafe { output.set_len(size_output) };
                Ok(output)
            }
        }
    }

    /// Explicit options version of [`retrieve_chunks_elements`](Array::retrieve_chunks_elements).
    #[allow(clippy::missing_errors_doc)]
    pub fn retrieve_chunks_elements_opt<T: bytemuck::Pod>(
        &self,
        chunks: &ArraySubset,
        options: &CodecOptions,
    ) -> Result<Vec<T>, ArrayError> {
        validate_element_size::<T>(self.data_type())?;
        let bytes = self.retrieve_chunks_opt(chunks, options)?;
        Ok(transmute_from_bytes_vec::<T>(bytes))
    }

    #[cfg(feature = "ndarray")]
    /// Explicit options version of [`retrieve_chunks_ndarray`](Array::retrieve_chunks_ndarray).
    #[allow(clippy::missing_errors_doc)]
    pub fn retrieve_chunks_ndarray_opt<T: bytemuck::Pod>(
        &self,
        chunks: &ArraySubset,
        options: &CodecOptions,
    ) -> Result<ndarray::ArrayD<T>, ArrayError> {
        // validate_element_size::<T>(self.data_type())?; // in retrieve_chunks_elements_opt
        let array_subset = self.chunks_subset(chunks)?;
        let elements = self.retrieve_chunks_elements_opt::<T>(chunks, options)?;
        elements_to_ndarray(array_subset.shape(), elements)
    }

    /// Explicit options version of [`retrieve_array_subset`](Array::retrieve_array_subset).
    #[allow(clippy::missing_errors_doc)]
    pub fn retrieve_array_subset_opt(
        &self,
        array_subset: &ArraySubset,
        options: &CodecOptions,
    ) -> Result<Vec<u8>, ArrayError> {
        if array_subset.dimensionality() != self.dimensionality() {
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
        let num_chunks = chunks.num_elements_usize();
        match num_chunks {
            0 => Ok(self
                .fill_value()
                .as_ne_bytes()
                .repeat(array_subset.num_elements_usize())),
            1 => {
                let chunk_indices = chunks.start();
                let chunk_subset = self.chunk_subset(chunk_indices)?;
                if &chunk_subset == array_subset {
                    // Single chunk fast path if the array subset domain matches the chunk domain
                    self.retrieve_chunk_opt(chunk_indices, options)
                } else {
                    let array_subset_in_chunk_subset =
                        unsafe { array_subset.relative_to_unchecked(chunk_subset.start()) };
                    self.retrieve_chunk_subset_opt(
                        chunk_indices,
                        &array_subset_in_chunk_subset,
                        options,
                    )
                }
            }
            _ => {
                // Allocate the output
                let size_output = array_subset.num_elements_usize() * self.data_type().size();
                let mut output = Vec::with_capacity(size_output);

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

                {
                    let output = UnsafeCellSlice::new_from_vec_with_spare_capacity(&mut output);
                    let retrieve_chunk = |chunk_indices: Vec<u64>| {
                        let chunk_subset = self.chunk_subset(&chunk_indices)?;
                        let chunk_subset_in_array_subset =
                            unsafe { chunk_subset.overlap_unchecked(array_subset) };
                        let chunk_subset = unsafe {
                            chunk_subset_in_array_subset.relative_to_unchecked(chunk_subset.start())
                        };
                        let array_view_subset = unsafe {
                            chunk_subset_in_array_subset.relative_to_unchecked(array_subset.start())
                        };
                        let array_view = ArrayView::new(
                            unsafe { output.get() },
                            array_subset.shape(),
                            array_view_subset,
                        )
                        .map_err(|err| CodecError::from(err.to_string()))?;
                        self.retrieve_chunk_subset_into_array_view_opt(
                            &chunk_indices,
                            &chunk_subset,
                            &array_view,
                            &options,
                        )
                    };
                    let indices = chunks.indices();
                    indices
                        .into_par_iter()
                        .by_uniform_blocks(indices.len().div_ceil(chunk_concurrent_limit).max(1))
                        .try_for_each(retrieve_chunk)?;
                }
                unsafe { output.set_len(size_output) };
                Ok(output)
            }
        }
    }

    /// Explicit options version of [`retrieve_chunks_into_array_view`](Array::retrieve_chunks_into_array_view).
    #[allow(clippy::missing_errors_doc)]
    pub fn retrieve_chunks_into_array_view_opt(
        &self,
        chunks: &ArraySubset,
        array_view: &ArrayView,
        options: &CodecOptions,
    ) -> Result<(), ArrayError> {
        if chunks.dimensionality() != self.dimensionality() {
            return Err(ArrayError::InvalidArraySubset(
                chunks.clone(),
                self.chunk_grid_shape()
                    .unwrap_or_else(|| vec![0u64; self.dimensionality()]),
            ));
        }
        let num_chunks = chunks.num_elements_usize();
        if num_chunks == 0 {
            return Ok(());
        }

        let array_subset = self.chunks_subset(chunks)?;
        if array_subset.shape() != array_view.subset().shape() {
            return Err(ArrayError::InvalidArraySubset(
                array_subset,
                array_view.subset().shape().to_vec(),
            ));
        }

        if num_chunks == 1 {
            let chunk_indices = chunks.start();
            self.retrieve_chunk_into_array_view_opt(chunk_indices, array_view, options)
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

            {
                let indices = chunks.indices();
                indices
                    .into_par_iter()
                    .by_uniform_blocks(indices.len().div_ceil(chunk_concurrent_limit).max(1))
                    .try_for_each(|chunk_indices: Vec<u64>| {
                        let chunk_subset = self.chunk_subset(&chunk_indices)?;
                        let array_view_subset =
                            unsafe { chunk_subset.relative_to_unchecked(array_subset.start()) };
                        self.retrieve_chunk_into_array_view_opt(
                            &chunk_indices,
                            &unsafe { array_view.subset_view(&array_view_subset) }
                                .map_err(|err| CodecError::from(err.to_string()))?,
                            &options,
                        )
                    })?;
            }
            Ok(())
        }
    }

    /// Explicit options version of [`retrieve_array_subset_into_array_view`](Array::retrieve_array_subset_into_array_view).
    #[allow(clippy::missing_errors_doc)]
    pub fn retrieve_array_subset_into_array_view_opt(
        &self,
        array_subset: &ArraySubset,
        array_view: &ArrayView,
        options: &CodecOptions,
    ) -> Result<(), ArrayError> {
        if array_subset.shape() != array_view.subset().shape() {
            return Err(ArrayError::InvalidArraySubset(
                array_subset.clone(),
                array_view.subset().shape().to_vec(),
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
        let num_chunks = chunks.num_elements_usize();
        match num_chunks {
            0 => Ok(()),
            1 => {
                let chunk_indices = chunks.start();
                let chunk_subset = self.chunk_subset(chunk_indices)?;
                if &chunk_subset == array_subset {
                    // Single chunk fast path if the array subset domain matches the chunk domain
                    let array_view_subset =
                        unsafe { chunk_subset.relative_to_unchecked(array_subset.start()) };
                    self.retrieve_chunk_into_array_view_opt(
                        chunk_indices,
                        &unsafe { array_view.subset_view(&array_view_subset) }
                            .map_err(|err| CodecError::from(err.to_string()))?,
                        options,
                    )
                } else {
                    let chunk_subset_in_array_subset =
                        unsafe { chunk_subset.overlap_unchecked(array_subset) };
                    let chunk_subset = unsafe {
                        chunk_subset_in_array_subset.relative_to_unchecked(chunk_subset.start())
                    };
                    let array_view_subset = unsafe {
                        chunk_subset_in_array_subset.relative_to_unchecked(array_subset.start())
                    };
                    self.retrieve_chunk_subset_into_array_view_opt(
                        chunk_indices,
                        &chunk_subset,
                        &unsafe { array_view.subset_view(&array_view_subset) }
                            .map_err(|err| CodecError::from(err.to_string()))?,
                        options,
                    )
                }
            }
            _ => {
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

                {
                    let indices = chunks.indices();
                    indices
                        .into_par_iter()
                        .by_uniform_blocks(indices.len().div_ceil(chunk_concurrent_limit).max(1))
                        .try_for_each(|chunk_indices: Vec<u64>| {
                            let chunk_subset = self.chunk_subset(&chunk_indices)?;
                            let chunk_subset_in_array_subset =
                                unsafe { chunk_subset.overlap_unchecked(array_subset) };
                            let chunk_subset = unsafe {
                                chunk_subset_in_array_subset
                                    .relative_to_unchecked(chunk_subset.start())
                            };
                            let array_view_subset = unsafe {
                                chunk_subset_in_array_subset
                                    .relative_to_unchecked(array_subset.start())
                            };
                            self.retrieve_chunk_subset_into_array_view_opt(
                                &chunk_indices,
                                &chunk_subset,
                                &unsafe { array_view.subset_view(&array_view_subset) }
                                    .map_err(|err| CodecError::from(err.to_string()))?,
                                &options,
                            )
                        })?;
                }
                Ok(())
            }
        }
    }

    /// Explicit options version of [`retrieve_array_subset_elements`](Array::retrieve_array_subset_elements).
    #[allow(clippy::missing_errors_doc)]
    pub fn retrieve_array_subset_elements_opt<T: bytemuck::Pod>(
        &self,
        array_subset: &ArraySubset,
        options: &CodecOptions,
    ) -> Result<Vec<T>, ArrayError> {
        validate_element_size::<T>(self.data_type())?;
        let bytes = self.retrieve_array_subset_opt(array_subset, options)?;
        Ok(transmute_from_bytes_vec::<T>(bytes))
    }

    #[cfg(feature = "ndarray")]
    /// Explicit options version of [`retrieve_array_subset_ndarray`](Array::retrieve_array_subset_ndarray).
    #[allow(clippy::missing_errors_doc)]
    pub fn retrieve_array_subset_ndarray_opt<T: bytemuck::Pod>(
        &self,
        array_subset: &ArraySubset,
        options: &CodecOptions,
    ) -> Result<ndarray::ArrayD<T>, ArrayError> {
        // validate_element_size::<T>(self.data_type())?; // in retrieve_array_subset_elements_opt
        let elements = self.retrieve_array_subset_elements_opt::<T>(array_subset, options)?;
        elements_to_ndarray(array_subset.shape(), elements)
    }

    /// Explicit options version of [`retrieve_chunk_subset`](Array::retrieve_chunk_subset).
    #[allow(clippy::missing_errors_doc)]
    pub fn retrieve_chunk_subset_opt(
        &self,
        chunk_indices: &[u64],
        chunk_subset: &ArraySubset,
        options: &CodecOptions,
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

            unsafe {
                self.codecs()
                    .partial_decoder(input_handle, &chunk_representation, options)?
                    .partial_decode_opt(&[chunk_subset.clone()], options)?
                    .pop()
                    .unwrap_unchecked()
            }
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

    /// Explicit options version of [`retrieve_chunk_subset_elements`](Array::retrieve_chunk_subset_elements).
    #[allow(clippy::missing_errors_doc)]
    pub fn retrieve_chunk_subset_elements_opt<T: bytemuck::Pod>(
        &self,
        chunk_indices: &[u64],
        chunk_subset: &ArraySubset,
        options: &CodecOptions,
    ) -> Result<Vec<T>, ArrayError> {
        validate_element_size::<T>(self.data_type())?;
        let bytes = self.retrieve_chunk_subset_opt(chunk_indices, chunk_subset, options)?;
        Ok(transmute_from_bytes_vec::<T>(bytes))
    }

    #[cfg(feature = "ndarray")]
    /// Explicit options version of [`retrieve_chunk_subset_ndarray`](Array::retrieve_chunk_subset_ndarray).
    #[allow(clippy::missing_errors_doc)]
    pub fn retrieve_chunk_subset_ndarray_opt<T: bytemuck::Pod>(
        &self,
        chunk_indices: &[u64],
        chunk_subset: &ArraySubset,
        options: &CodecOptions,
    ) -> Result<ndarray::ArrayD<T>, ArrayError> {
        // validate_element_size::<T>(self.data_type())?; // in retrieve_chunk_subset_elements
        let elements =
            self.retrieve_chunk_subset_elements_opt::<T>(chunk_indices, chunk_subset, options)?;
        elements_to_ndarray(chunk_subset.shape(), elements)
    }

    /// Explicit options version of [`partial_decoder`](Array::partial_decoder).
    #[allow(clippy::missing_errors_doc)]
    pub fn partial_decoder_opt<'a>(
        &'a self,
        chunk_indices: &[u64],
        options: &CodecOptions,
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
            .partial_decoder(input_handle, &chunk_representation, options)?)
    }
}
