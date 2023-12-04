use rayon::prelude::*;

use crate::{
    array::{
        chunk_grid::RegularChunkGrid,
        codec::{
            ArrayPartialDecoderTraits, ArraySubset, ArrayToBytesCodecTraits,
            ByteIntervalPartialDecoder, BytesPartialDecoderTraits, CodecChain, CodecError,
        },
        ravel_indices,
        unsafe_cell_slice::UnsafeCellSlice,
        ArrayRepresentation, ArrayShape,
    },
    byte_range::ByteRange,
};

#[cfg(feature = "async")]
use crate::array::codec::{
    byte_interval_partial_decoder::AsyncByteIntervalPartialDecoder, AsyncArrayPartialDecoderTraits,
    AsyncBytesPartialDecoderTraits,
};

use super::{
    calculate_chunks_per_shard, compute_index_encoded_size, decode_shard_index,
    sharding_configuration::ShardingIndexLocation, sharding_index_decoded_representation,
};

#[cfg(feature = "async")]
use super::async_decode_shard_index;

/// Partial decoder for the sharding codec.
pub struct ShardingPartialDecoder<'a> {
    input_handle: Box<dyn BytesPartialDecoderTraits + 'a>,
    decoded_representation: ArrayRepresentation,
    chunk_grid: RegularChunkGrid,
    inner_codecs: &'a CodecChain,
    shard_index: Option<Vec<u64>>,
}

impl<'a> ShardingPartialDecoder<'a> {
    /// Create a new partial decoder for the sharding codec.
    pub fn new(
        input_handle: Box<dyn BytesPartialDecoderTraits + 'a>,
        decoded_representation: ArrayRepresentation,
        chunk_shape: ArrayShape,
        inner_codecs: &'a CodecChain,
        index_codecs: &'a CodecChain,
        index_location: ShardingIndexLocation,
        parallel: bool,
    ) -> Result<Self, CodecError> {
        let shard_index = Self::decode_shard_index(
            &*input_handle,
            index_codecs,
            index_location,
            &chunk_shape,
            &decoded_representation,
            parallel,
        )?;
        Ok(Self {
            input_handle,
            decoded_representation,
            chunk_grid: RegularChunkGrid::new(chunk_shape),
            inner_codecs,
            shard_index,
        })
    }

    /// Returns `None` if there is no shard.
    fn decode_shard_index(
        input_handle: &dyn BytesPartialDecoderTraits,
        index_codecs: &'a CodecChain,
        index_location: ShardingIndexLocation,
        chunk_shape: &[u64],
        decoded_representation: &ArrayRepresentation,
        parallel: bool,
    ) -> Result<Option<Vec<u64>>, CodecError> {
        let shard_shape = decoded_representation.shape();
        let chunk_representation = unsafe {
            ArrayRepresentation::new_unchecked(
                chunk_shape.to_vec(),
                decoded_representation.data_type().clone(),
                decoded_representation.fill_value().clone(),
            )
        };

        // Calculate chunks per shard
        let chunks_per_shard =
            calculate_chunks_per_shard(shard_shape, chunk_representation.shape())
                .map_err(|e| CodecError::Other(e.to_string()))?;

        // Get index array representation and encoded size
        let index_array_representation = sharding_index_decoded_representation(&chunks_per_shard);
        let index_encoded_size =
            compute_index_encoded_size(index_codecs, &index_array_representation)
                .map_err(|e| CodecError::Other(e.to_string()))?;

        // Decode the shard index
        let index_byte_range = match index_location {
            ShardingIndexLocation::Start => ByteRange::FromStart(0, Some(index_encoded_size)),
            ShardingIndexLocation::End => ByteRange::FromEnd(0, Some(index_encoded_size)),
        };

        let encoded_shard_index = input_handle
            .partial_decode_opt(&[index_byte_range], parallel)?
            .map(|mut v| v.remove(0));

        Ok(match encoded_shard_index {
            Some(encoded_shard_index) => Some(decode_shard_index(
                encoded_shard_index,
                &index_array_representation,
                index_codecs,
                parallel,
            )?),
            None => None,
        })
    }
}

impl ArrayPartialDecoderTraits for ShardingPartialDecoder<'_> {
    fn partial_decode_opt(
        &self,
        array_subsets: &[ArraySubset],
        parallel: bool,
    ) -> Result<Vec<Vec<u8>>, CodecError> {
        if parallel {
            self.par_partial_decode(array_subsets)
        } else {
            self.partial_decode(array_subsets)
        }
    }

    fn partial_decode(&self, array_subsets: &[ArraySubset]) -> Result<Vec<Vec<u8>>, CodecError> {
        let Some(shard_index) = &self.shard_index else {
            return Ok(array_subsets
                .iter()
                .map(|array_subset| {
                    self.decoded_representation
                        .fill_value()
                        .as_ne_bytes()
                        .repeat(array_subset.num_elements_usize())
                })
                .collect());
        };

        let chunk_representation = unsafe {
            ArrayRepresentation::new_unchecked(
                self.chunk_grid.chunk_shape().to_vec(),
                self.decoded_representation.data_type().clone(),
                self.decoded_representation.fill_value().clone(),
            )
        };

        let chunks_per_shard = calculate_chunks_per_shard(
            self.decoded_representation.shape(),
            chunk_representation.shape(),
        )
        .map_err(|e| CodecError::Other(e.to_string()))?;

        let element_size = self.decoded_representation.element_size();
        let element_size_u64 = element_size as u64;
        let fill_value = chunk_representation.fill_value().as_ne_bytes();

        let mut out = Vec::with_capacity(array_subsets.len());
        for array_subset in array_subsets {
            let array_subset_size =
                usize::try_from(array_subset.num_elements() * element_size_u64).unwrap();
            let mut out_array_subset = vec![0; array_subset_size];

            // Decode those chunks if required and put in chunk cache
            for (chunk_indices, chunk_subset) in
                unsafe { array_subset.iter_chunks_unchecked(chunk_representation.shape()) }
            {
                let shard_index_index: usize =
                    usize::try_from(ravel_indices(&chunk_indices, &chunks_per_shard) * 2).unwrap();
                let offset = shard_index[shard_index_index];
                let size = shard_index[shard_index_index + 1];

                let decoded_bytes = if offset == u64::MAX && size == u64::MAX {
                    // The chunk is just the fill value
                    fill_value.repeat(chunk_subset.num_elements_usize())
                } else {
                    // The chunk must be decoded
                    let partial_decoder = self.inner_codecs.partial_decoder(
                        Box::new(ByteIntervalPartialDecoder::new(
                            &*self.input_handle,
                            offset,
                            size,
                        )),
                        &chunk_representation,
                    )?;
                    let array_subset_in_chunk_subset =
                        unsafe { array_subset.in_subset_unchecked(&chunk_subset) };

                    // Partial decoding is actually really slow with the blosc codec! Assume sharded chunks are small, and just decode the whole thing and extract bytes
                    // TODO: Make this behaviour optional?
                    // partial_decoder
                    //     .partial_decode(&[array_subset_in_chunk_subset.clone()])?
                    //     .remove(0)
                    let decoded_chunk = partial_decoder
                        .partial_decode(&[ArraySubset::new_with_shape(
                            chunk_subset.shape().to_vec(),
                        )])?
                        .remove(0);
                    array_subset_in_chunk_subset
                        .extract_bytes(&decoded_chunk, chunk_subset.shape(), element_size)
                        .unwrap()
                };

                // Copy decoded bytes to the output
                let chunk_subset_in_array_subset =
                    unsafe { chunk_subset.in_subset_unchecked(array_subset) };
                let mut decoded_offset = 0;
                for (array_subset_element_index, num_elements) in unsafe {
                    chunk_subset_in_array_subset
                        .iter_contiguous_linearised_indices_unchecked(array_subset.shape())
                } {
                    let output_offset =
                        usize::try_from(array_subset_element_index * element_size_u64).unwrap();
                    let length = usize::try_from(num_elements * element_size_u64).unwrap();
                    out_array_subset[output_offset..output_offset + length]
                        .copy_from_slice(&decoded_bytes[decoded_offset..decoded_offset + length]);
                    decoded_offset += length;
                }
            }
            out.push(out_array_subset);
        }
        Ok(out)
    }

    fn par_partial_decode(
        &self,
        array_subsets: &[ArraySubset],
    ) -> Result<Vec<Vec<u8>>, CodecError> {
        let Some(shard_index) = &self.shard_index else {
            return Ok(array_subsets
                .iter()
                .map(|decoded_region| {
                    self.decoded_representation
                        .fill_value()
                        .as_ne_bytes()
                        .repeat(decoded_region.num_elements_usize())
                })
                .collect());
        };

        let chunk_representation = unsafe {
            ArrayRepresentation::new_unchecked(
                self.chunk_grid.chunk_shape().to_vec(),
                self.decoded_representation.data_type().clone(),
                self.decoded_representation.fill_value().clone(),
            )
        };

        let chunks_per_shard = calculate_chunks_per_shard(
            self.decoded_representation.shape(),
            chunk_representation.shape(),
        )
        .map_err(|e| CodecError::Other(e.to_string()))?;

        let element_size = self.decoded_representation.element_size() as u64;
        let fill_value = chunk_representation.fill_value().as_ne_bytes();

        let mut out = Vec::with_capacity(array_subsets.len());
        for array_subset in array_subsets {
            let array_subset_size =
                usize::try_from(array_subset.num_elements() * element_size).unwrap();
            let mut out_array_subset = vec![0; array_subset_size];
            let out_array_subset_slice = UnsafeCellSlice::new(out_array_subset.as_mut_slice());

            // Decode those chunks if required
            unsafe { array_subset.iter_chunks_unchecked(chunk_representation.shape()) }
                .par_bridge()
                .try_for_each(|(chunk_indices, chunk_subset)| {
                    let out_array_subset_slice = unsafe { out_array_subset_slice.get() };

                    let shard_index_idx: usize =
                        usize::try_from(ravel_indices(&chunk_indices, &chunks_per_shard) * 2)
                            .unwrap();
                    let offset = shard_index[shard_index_idx];
                    let size = shard_index[shard_index_idx + 1];

                    // Get the subset of bytes from the chunk which intersect the array
                    let array_subset_in_chunk_subset =
                        unsafe { array_subset.in_subset_unchecked(&chunk_subset) };

                    let decoded_bytes = if offset == u64::MAX && size == u64::MAX {
                        // The chunk is just the fill value
                        fill_value.repeat(array_subset_in_chunk_subset.num_elements_usize())
                    } else {
                        // The chunk must be decoded
                        let partial_decoder = self.inner_codecs.partial_decoder(
                            Box::new(ByteIntervalPartialDecoder::new(
                                &*self.input_handle,
                                offset,
                                size,
                            )),
                            &chunk_representation,
                        )?;
                        // NOTE: Intentionally using single threaded decode, since parallelisation is in the loop
                        partial_decoder
                            .partial_decode(&[array_subset_in_chunk_subset])?
                            .remove(0)
                    };

                    // Copy decoded bytes to the output
                    let chunk_subset_in_array_subset =
                        unsafe { chunk_subset.in_subset_unchecked(array_subset) };
                    let mut decoded_offset = 0;
                    for (array_subset_element_index, num_elements) in unsafe {
                        chunk_subset_in_array_subset
                            .iter_contiguous_linearised_indices_unchecked(array_subset.shape())
                    } {
                        let output_offset =
                            usize::try_from(array_subset_element_index * element_size).unwrap();
                        let length = usize::try_from(num_elements * element_size).unwrap();
                        out_array_subset_slice[output_offset..output_offset + length]
                            .copy_from_slice(
                                &decoded_bytes[decoded_offset..decoded_offset + length],
                            );
                        decoded_offset += length;
                    }
                    Ok::<_, CodecError>(())
                })?;
            out.push(out_array_subset);
        }
        Ok(out)
    }
}

#[cfg(feature = "async")]
/// Asynchronous partial decoder for the sharding codec.
pub struct AsyncShardingPartialDecoder<'a> {
    input_handle: Box<dyn AsyncBytesPartialDecoderTraits + 'a>,
    decoded_representation: ArrayRepresentation,
    chunk_grid: RegularChunkGrid,
    inner_codecs: &'a CodecChain,
    shard_index: Option<Vec<u64>>,
}

#[cfg(feature = "async")]
impl<'a> AsyncShardingPartialDecoder<'a> {
    /// Create a new partial decoder for the sharding codec.
    pub async fn new(
        input_handle: Box<dyn AsyncBytesPartialDecoderTraits + 'a>,
        decoded_representation: ArrayRepresentation,
        chunk_shape: ArrayShape,
        inner_codecs: &'a CodecChain,
        index_codecs: &'a CodecChain,
        index_location: ShardingIndexLocation,
        parallel: bool,
    ) -> Result<AsyncShardingPartialDecoder<'a>, CodecError> {
        let shard_index = Self::decode_shard_index(
            &*input_handle,
            index_codecs,
            index_location,
            &chunk_shape,
            &decoded_representation,
            parallel,
        )
        .await?;
        Ok(Self {
            input_handle,
            decoded_representation,
            chunk_grid: RegularChunkGrid::new(chunk_shape),
            inner_codecs,
            shard_index,
        })
    }

    /// Returns `None` if there is no shard.
    async fn decode_shard_index(
        input_handle: &dyn AsyncBytesPartialDecoderTraits,
        index_codecs: &'a CodecChain,
        index_location: ShardingIndexLocation,
        chunk_shape: &[u64],
        decoded_representation: &ArrayRepresentation,
        parallel: bool,
    ) -> Result<Option<Vec<u64>>, CodecError> {
        let shard_shape = decoded_representation.shape();
        let chunk_representation = unsafe {
            ArrayRepresentation::new_unchecked(
                chunk_shape.to_vec(),
                decoded_representation.data_type().clone(),
                decoded_representation.fill_value().clone(),
            )
        };

        // Calculate chunks per shard
        let chunks_per_shard =
            calculate_chunks_per_shard(shard_shape, chunk_representation.shape())
                .map_err(|e| CodecError::Other(e.to_string()))?;

        // Get index array representation and encoded size
        let index_array_representation = sharding_index_decoded_representation(&chunks_per_shard);
        let index_encoded_size =
            compute_index_encoded_size(index_codecs, &index_array_representation)
                .map_err(|e| CodecError::Other(e.to_string()))?;

        // Decode the shard index
        let index_byte_range = match index_location {
            ShardingIndexLocation::Start => ByteRange::FromStart(0, Some(index_encoded_size)),
            ShardingIndexLocation::End => ByteRange::FromEnd(0, Some(index_encoded_size)),
        };

        let encoded_shard_index = input_handle
            .partial_decode_opt(&[index_byte_range], parallel)
            .await?
            .map(|mut v| v.remove(0));

        Ok(match encoded_shard_index {
            Some(encoded_shard_index) => Some(
                async_decode_shard_index(
                    encoded_shard_index,
                    &index_array_representation,
                    index_codecs,
                    parallel,
                )
                .await?,
            ),
            None => None,
        })
    }
}

#[cfg(feature = "async")]
#[async_trait::async_trait]
impl AsyncArrayPartialDecoderTraits for AsyncShardingPartialDecoder<'_> {
    async fn partial_decode_opt(
        &self,
        array_subsets: &[ArraySubset],
        parallel: bool,
    ) -> Result<Vec<Vec<u8>>, CodecError> {
        if parallel {
            self.par_partial_decode(array_subsets).await
        } else {
            self.partial_decode(array_subsets).await
        }
    }

    async fn partial_decode(
        &self,
        array_subsets: &[ArraySubset],
    ) -> Result<Vec<Vec<u8>>, CodecError> {
        let Some(shard_index) = &self.shard_index else {
            return Ok(array_subsets
                .iter()
                .map(|array_subset| {
                    self.decoded_representation
                        .fill_value()
                        .as_ne_bytes()
                        .repeat(array_subset.num_elements_usize())
                })
                .collect());
        };

        let chunk_representation = unsafe {
            ArrayRepresentation::new_unchecked(
                self.chunk_grid.chunk_shape().to_vec(),
                self.decoded_representation.data_type().clone(),
                self.decoded_representation.fill_value().clone(),
            )
        };

        let chunks_per_shard = calculate_chunks_per_shard(
            self.decoded_representation.shape(),
            chunk_representation.shape(),
        )
        .map_err(|e| CodecError::Other(e.to_string()))?;

        let element_size = self.decoded_representation.element_size() as u64;
        let fill_value = chunk_representation.fill_value().as_ne_bytes();

        let mut out = Vec::with_capacity(array_subsets.len());
        for array_subset in array_subsets {
            let array_subset_size =
                usize::try_from(array_subset.num_elements() * element_size).unwrap();
            let mut out_array_subset = vec![0; array_subset_size];

            // Decode those chunks if required and put in chunk cache
            for (chunk_indices, chunk_subset) in
                unsafe { array_subset.iter_chunks_unchecked(chunk_representation.shape()) }
            {
                let shard_index_index: usize =
                    usize::try_from(ravel_indices(&chunk_indices, &chunks_per_shard) * 2).unwrap();
                let offset = shard_index[shard_index_index];
                let size = shard_index[shard_index_index + 1];

                let decoded_bytes = if offset == u64::MAX && size == u64::MAX {
                    // The chunk is just the fill value
                    fill_value.repeat(chunk_subset.num_elements_usize())
                } else {
                    // The chunk must be decoded
                    let partial_decoder = self
                        .inner_codecs
                        .async_partial_decoder(
                            Box::new(AsyncByteIntervalPartialDecoder::new(
                                &*self.input_handle,
                                offset,
                                size,
                            )),
                            &chunk_representation,
                        )
                        .await?;
                    let array_subset_in_chunk_subset =
                        unsafe { array_subset.in_subset_unchecked(&chunk_subset) };
                    partial_decoder
                        .partial_decode(&[array_subset_in_chunk_subset.clone()])
                        .await?
                        .remove(0)
                };

                // Copy decoded bytes to the output
                let chunk_subset_in_array_subset =
                    unsafe { chunk_subset.in_subset_unchecked(array_subset) };
                let mut decoded_offset = 0;
                for (array_subset_element_index, num_elements) in unsafe {
                    chunk_subset_in_array_subset
                        .iter_contiguous_linearised_indices_unchecked(array_subset.shape())
                } {
                    let output_offset =
                        usize::try_from(array_subset_element_index * element_size).unwrap();
                    let length = usize::try_from(num_elements * element_size).unwrap();
                    out_array_subset[output_offset..output_offset + length]
                        .copy_from_slice(&decoded_bytes[decoded_offset..decoded_offset + length]);
                    decoded_offset += length;
                }
            }
            out.push(out_array_subset);
        }
        Ok(out)
    }

    #[allow(clippy::too_many_lines)]
    async fn par_partial_decode(
        &self,
        array_subsets: &[ArraySubset],
    ) -> Result<Vec<Vec<u8>>, CodecError> {
        let Some(shard_index) = &self.shard_index else {
            return Ok(array_subsets
                .iter()
                .map(|decoded_region| {
                    self.decoded_representation
                        .fill_value()
                        .as_ne_bytes()
                        .repeat(decoded_region.num_elements_usize())
                })
                .collect());
        };

        let chunks_per_shard = calculate_chunks_per_shard(
            self.decoded_representation.shape(),
            self.chunk_grid.chunk_shape(),
        )
        .map_err(|e| CodecError::Other(e.to_string()))?;

        let element_size = self.decoded_representation.element_size();
        let mut out = Vec::with_capacity(array_subsets.len());
        for array_subset in array_subsets {
            // shard (subset)
            let mut shard = vec![
                std::mem::MaybeUninit::<u8>::uninit();
                array_subset.num_elements_usize() * element_size
            ];
            let shard_slice = unsafe {
                std::slice::from_raw_parts_mut(shard.as_mut_ptr().cast::<u8>(), shard.len())
            };
            let shard_slice = UnsafeCellSlice::new(shard_slice);

            // Find filled / non filled chunks
            let chunk_info =
                unsafe { array_subset.iter_chunks_unchecked(self.chunk_grid.chunk_shape()) }
                    .map(|(chunk_indices, chunk_subset)| {
                        let chunk_index = ravel_indices(&chunk_indices, &chunks_per_shard);
                        let chunk_index = usize::try_from(chunk_index).unwrap();

                        // Read the offset/size
                        let offset = shard_index[chunk_index * 2];
                        let size = shard_index[chunk_index * 2 + 1];
                        if offset == u64::MAX && size == u64::MAX {
                            (chunk_subset, None)
                        } else {
                            let offset: usize = offset.try_into().unwrap();
                            let size: usize = size.try_into().unwrap();
                            (chunk_subset, Some((offset, size)))
                        }
                    })
                    .collect::<Vec<_>>();

            // Decode unfilled chunks
            futures::future::join_all(
                chunk_info
                    .iter()
                    .filter_map(|(chunk_subset, offset_size)| {
                        offset_size
                            .as_ref()
                            .map(|offset_size| (chunk_subset, offset_size))
                    })
                    .map(|(chunk_subset, (offset, size))| async move {
                        let chunk_representation = unsafe {
                            ArrayRepresentation::new_unchecked(
                                self.chunk_grid.chunk_shape().to_vec(),
                                self.decoded_representation.data_type().clone(),
                                self.decoded_representation.fill_value().clone(),
                            )
                        };
                        let partial_decoder = self
                            .inner_codecs
                            .async_partial_decoder(
                                Box::new(AsyncByteIntervalPartialDecoder::new(
                                    &*self.input_handle,
                                    u64::try_from(*offset).unwrap(),
                                    u64::try_from(*size).unwrap(),
                                )),
                                &chunk_representation,
                            )
                            .await?;
                        let array_subset_in_chunk_subset =
                            unsafe { array_subset.in_subset_unchecked(chunk_subset) };
                        // Partial decoding is actually really slow with the blosc codec! Assume sharded chunks are small, and just decode the whole thing and extract bytes
                        // TODO: Investigate further
                        // let decoded_chunk = partial_decoder
                        //     .partial_decode(&[array_subset_in_chunk_subset])
                        //     .await?
                        //     .remove(0);
                        let decoded_chunk = partial_decoder
                            .partial_decode(&[ArraySubset::new_with_shape(
                                chunk_subset.shape().to_vec(),
                            )])
                            .await?
                            .remove(0);
                        let decoded_chunk = array_subset_in_chunk_subset
                            .extract_bytes(&decoded_chunk, chunk_subset.shape(), element_size)
                            .unwrap();
                        let chunk_subset_in_array_subset =
                            unsafe { chunk_subset.in_subset_unchecked(array_subset) };
                        Ok::<_, CodecError>((chunk_subset_in_array_subset, decoded_chunk))
                    }),
            )
            .await
            .into_par_iter()
            .try_for_each(|subset_and_decoded_chunk| {
                let (chunk_subset_in_array_subset, decoded_chunk) = subset_and_decoded_chunk?;
                let mut data_idx = 0;
                let element_size = element_size as u64;
                let shard_slice = unsafe { shard_slice.get() };
                for (index, num_elements) in unsafe {
                    chunk_subset_in_array_subset
                        .iter_contiguous_linearised_indices_unchecked(array_subset.shape())
                } {
                    let shard_offset = usize::try_from(index * element_size).unwrap();
                    let length = usize::try_from(num_elements * element_size).unwrap();
                    shard_slice[shard_offset..shard_offset + length]
                        .copy_from_slice(&decoded_chunk[data_idx..data_idx + length]);
                    data_idx += length;
                }
                Ok::<_, CodecError>(())
            })?;

            // Write filled chunks
            let filled_chunks = chunk_info
                .iter()
                .filter_map(|(chunk_subset, offset_size)| {
                    if offset_size.is_none() {
                        Some(chunk_subset)
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();
            if !filled_chunks.is_empty() {
                let chunk_array_ss =
                    ArraySubset::new_with_shape(self.chunk_grid.chunk_shape().to_vec());
                let filled_chunk = self
                    .decoded_representation
                    .fill_value()
                    .as_ne_bytes()
                    .repeat(chunk_array_ss.num_elements_usize());

                // Write filled chunks
                filled_chunks.par_iter().for_each(|chunk_subset| {
                    let chunk_subset_in_array_subset =
                        unsafe { chunk_subset.in_subset_unchecked(array_subset) };
                    let mut data_idx = 0;
                    let element_size = self.decoded_representation.element_size() as u64;
                    let shard_slice = unsafe { shard_slice.get() };
                    for (index, num_elements) in unsafe {
                        chunk_subset_in_array_subset
                            .iter_contiguous_linearised_indices_unchecked(array_subset.shape())
                    } {
                        let shard_offset = usize::try_from(index * element_size).unwrap();
                        let length = usize::try_from(num_elements * element_size).unwrap();
                        shard_slice[shard_offset..shard_offset + length]
                            .copy_from_slice(&filled_chunk[data_idx..data_idx + length]);
                        data_idx += length;
                    }
                });
            };

            #[allow(clippy::transmute_undefined_repr)]
            let shard = unsafe { core::mem::transmute(shard) };

            out.push(shard);
        }
        Ok(out)
    }
}
