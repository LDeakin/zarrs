use std::num::NonZeroU64;

use rayon::prelude::*;

use crate::{
    array::{
        chunk_grid::RegularChunkGrid,
        chunk_shape_to_array_shape,
        codec::{
            ArrayPartialDecoderTraits, ArraySubset, ArrayToBytesCodecTraits,
            ByteIntervalPartialDecoder, BytesPartialDecoderTraits, CodecChain, CodecError,
            PartialDecodeOptions, PartialDecoderOptions,
        },
        ravel_indices,
        unsafe_cell_slice::UnsafeCellSlice,
        ChunkRepresentation, ChunkShape,
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
    decoded_representation: ChunkRepresentation,
    chunk_grid: RegularChunkGrid,
    inner_codecs: &'a CodecChain,
    shard_index: Option<Vec<u64>>,
}

impl<'a> ShardingPartialDecoder<'a> {
    /// Create a new partial decoder for the sharding codec.
    pub fn new(
        input_handle: Box<dyn BytesPartialDecoderTraits + 'a>,
        decoded_representation: ChunkRepresentation,
        chunk_shape: ChunkShape,
        inner_codecs: &'a CodecChain,
        index_codecs: &'a CodecChain,
        index_location: ShardingIndexLocation,
        options: &PartialDecoderOptions,
    ) -> Result<Self, CodecError> {
        let shard_index = Self::decode_shard_index(
            &*input_handle,
            index_codecs,
            index_location,
            chunk_shape.as_slice(),
            &decoded_representation,
            options,
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
        chunk_shape: &[NonZeroU64],
        decoded_representation: &ChunkRepresentation,
        options: &PartialDecoderOptions,
    ) -> Result<Option<Vec<u64>>, CodecError> {
        let shard_shape = decoded_representation.shape();
        let chunk_representation = unsafe {
            ChunkRepresentation::new_unchecked(
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
        let index_array_representation =
            sharding_index_decoded_representation(chunks_per_shard.as_slice());
        let index_encoded_size =
            compute_index_encoded_size(index_codecs, &index_array_representation)
                .map_err(|e| CodecError::Other(e.to_string()))?;

        // Decode the shard index
        let index_byte_range = match index_location {
            ShardingIndexLocation::Start => ByteRange::FromStart(0, Some(index_encoded_size)),
            ShardingIndexLocation::End => ByteRange::FromEnd(0, Some(index_encoded_size)),
        };

        let encoded_shard_index = input_handle
            .partial_decode_opt(&[index_byte_range], options)?
            .map(|mut v| v.remove(0));

        Ok(match encoded_shard_index {
            Some(encoded_shard_index) => Some(decode_shard_index(
                encoded_shard_index,
                &index_array_representation,
                index_codecs,
                options,
            )?),
            None => None,
        })
    }
}

impl ArrayPartialDecoderTraits for ShardingPartialDecoder<'_> {
    fn element_size(&self) -> usize {
        self.decoded_representation.element_size()
    }

    fn partial_decode_opt(
        &self,
        array_subsets: &[ArraySubset],
        options: &PartialDecodeOptions,
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
            ChunkRepresentation::new_unchecked(
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
        let chunks_per_shard = chunk_shape_to_array_shape(chunks_per_shard.as_slice());

        let element_size = self.decoded_representation.element_size() as u64;
        let fill_value = chunk_representation.fill_value().as_ne_bytes();

        let mut out = Vec::with_capacity(array_subsets.len());
        for array_subset in array_subsets {
            let array_subset_size =
                usize::try_from(array_subset.num_elements() * element_size).unwrap();
            let mut out_array_subset = vec![0; array_subset_size];
            let out_array_subset_slice = UnsafeCellSlice::new(out_array_subset.as_mut_slice());

            // Decode those chunks if required
            let chunks = unsafe { array_subset.chunks_unchecked(chunk_representation.shape()) };

            rayon_iter_concurrent_limit::iter_concurrent_limit!(
                options.concurrent_limit(),
                chunks.into_par_iter(),
                try_for_each,
                |(chunk_indices, chunk_subset)| {
                    let out_array_subset_slice = unsafe { out_array_subset_slice.get() };

                    let shard_index_idx: usize =
                        usize::try_from(ravel_indices(&chunk_indices, &chunks_per_shard) * 2)
                            .unwrap();
                    let offset = shard_index[shard_index_idx];
                    let size = shard_index[shard_index_idx + 1];

                    // Get the subset of bytes from the chunk which intersect the array
                    let overlap = unsafe { array_subset.overlap_unchecked(&chunk_subset) };
                    let array_subset_in_chunk_subset =
                        unsafe { overlap.relative_to_unchecked(chunk_subset.start()) };

                    let decoded_bytes = if offset == u64::MAX && size == u64::MAX {
                        // The chunk is just the fill value
                        fill_value.repeat(array_subset_in_chunk_subset.num_elements_usize())
                    } else {
                        // The chunk must be decoded
                        let partial_decoder = self.inner_codecs.partial_decoder_opt(
                            Box::new(ByteIntervalPartialDecoder::new(
                                &*self.input_handle,
                                offset,
                                size,
                            )),
                            &chunk_representation,
                            options, // FIXME: Adjust options for partial decoding
                        )?;
                        // NOTE: Intentionally using single threaded decode, since parallelisation is in the loop
                        partial_decoder
                            .partial_decode_opt(&[array_subset_in_chunk_subset], options)? // FIXME: Adjust options for partial decoding
                            .remove(0)
                    };

                    // Copy decoded bytes to the output
                    let chunk_subset_in_array_subset =
                        unsafe { overlap.relative_to_unchecked(array_subset.start()) };
                    let mut decoded_offset = 0;
                    let contiguous_iterator = unsafe {
                        chunk_subset_in_array_subset
                            .contiguous_linearised_indices_unchecked(array_subset.shape())
                    };
                    let length =
                        usize::try_from(contiguous_iterator.contiguous_elements() * element_size)
                            .unwrap();
                    for (array_subset_element_index, _num_elements) in &contiguous_iterator {
                        let output_offset =
                            usize::try_from(array_subset_element_index * element_size).unwrap();
                        out_array_subset_slice[output_offset..output_offset + length]
                            .copy_from_slice(
                                &decoded_bytes[decoded_offset..decoded_offset + length],
                            );
                        decoded_offset += length;
                    }
                    Ok::<_, CodecError>(())
                }
            )?;
            out.push(out_array_subset);
        }
        Ok(out)
    }

    // fn partial_decode(&self, array_subsets: &[ArraySubset]) -> Result<Vec<Vec<u8>>, CodecError> {
    //     let Some(shard_index) = &self.shard_index else {
    //         return Ok(array_subsets
    //             .iter()
    //             .map(|array_subset| {
    //                 self.decoded_representation
    //                     .fill_value()
    //                     .as_ne_bytes()
    //                     .repeat(array_subset.num_elements_usize())
    //             })
    //             .collect());
    //     };

    //     let chunk_representation = unsafe {
    //         ChunkRepresentation::new_unchecked(
    //             self.chunk_grid.chunk_shape().to_vec(),
    //             self.decoded_representation.data_type().clone(),
    //             self.decoded_representation.fill_value().clone(),
    //         )
    //     };

    //     let chunks_per_shard = calculate_chunks_per_shard(
    //         self.decoded_representation.shape(),
    //         chunk_representation.shape(),
    //     )
    //     .map_err(|e| CodecError::Other(e.to_string()))?;
    //     let chunks_per_shard = chunk_shape_to_array_shape(chunks_per_shard.as_slice());

    //     let element_size = self.decoded_representation.element_size();
    //     let element_size_u64 = element_size as u64;
    //     let fill_value = chunk_representation.fill_value().as_ne_bytes();

    //     let mut out = Vec::with_capacity(array_subsets.len());
    //     for array_subset in array_subsets {
    //         let array_subset_size =
    //             usize::try_from(array_subset.num_elements() * element_size_u64).unwrap();
    //         let mut out_array_subset = vec![0; array_subset_size];

    //         // Decode those chunks if required and put in chunk cache
    //         for (chunk_indices, chunk_subset) in
    //             unsafe { array_subset.iter_chunks_unchecked(chunk_representation.shape()) }
    //         {
    //             let shard_index_index: usize =
    //                 usize::try_from(ravel_indices(&chunk_indices, &chunks_per_shard) * 2).unwrap();
    //             let offset = shard_index[shard_index_index];
    //             let size = shard_index[shard_index_index + 1];

    //             let overlap = unsafe { array_subset.overlap_unchecked(&chunk_subset) };
    //             let decoded_bytes = if offset == u64::MAX && size == u64::MAX {
    //                 // The chunk is just the fill value
    //                 fill_value.repeat(chunk_subset.num_elements_usize())
    //             } else {
    //                 // The chunk must be decoded
    //                 let partial_decoder = self.inner_codecs.partial_decoder(
    //                     Box::new(ByteIntervalPartialDecoder::new(
    //                         &*self.input_handle,
    //                         offset,
    //                         size,
    //                     )),
    //                     &chunk_representation,
    //                 )?;
    //                 let array_subset_in_chunk_subset =
    //                     unsafe { overlap.relative_to_unchecked(chunk_subset.start()) };

    //                 // Partial decoding is actually really slow with the blosc codec! Assume sharded chunks are small, and just decode the whole thing and extract bytes
    //                 // TODO: Make this behaviour optional?
    //                 // partial_decoder
    //                 //     .partial_decode(&[array_subset_in_chunk_subset.clone()])?
    //                 //     .remove(0)
    //                 let decoded_chunk = partial_decoder
    //                     .partial_decode(&[ArraySubset::new_with_shape(
    //                         chunk_subset.shape().to_vec(),
    //                     )])?
    //                     .remove(0);
    //                 array_subset_in_chunk_subset
    //                     .extract_bytes(&decoded_chunk, chunk_subset.shape(), element_size)
    //                     .unwrap()
    //             };

    //             // Copy decoded bytes to the output
    //             let chunk_subset_in_array_subset =
    //                 unsafe { overlap.relative_to_unchecked(array_subset.start()) };
    //             let mut decoded_offset = 0;
    //             for (array_subset_element_index, num_elements) in unsafe {
    //                 chunk_subset_in_array_subset
    //                     .iter_contiguous_linearised_indices_unchecked(array_subset.shape())
    //             } {
    //                 let output_offset =
    //                     usize::try_from(array_subset_element_index * element_size_u64).unwrap();
    //                 let length = usize::try_from(num_elements * element_size_u64).unwrap();
    //                 out_array_subset[output_offset..output_offset + length]
    //                     .copy_from_slice(&decoded_bytes[decoded_offset..decoded_offset + length]);
    //                 decoded_offset += length;
    //             }
    //         }
    //         out.push(out_array_subset);
    //     }
    //     Ok(out)
    // }
}

#[cfg(feature = "async")]
/// Asynchronous partial decoder for the sharding codec.
pub struct AsyncShardingPartialDecoder<'a> {
    input_handle: Box<dyn AsyncBytesPartialDecoderTraits + 'a>,
    decoded_representation: ChunkRepresentation,
    chunk_grid: RegularChunkGrid,
    inner_codecs: &'a CodecChain,
    shard_index: Option<Vec<u64>>,
}

#[cfg(feature = "async")]
impl<'a> AsyncShardingPartialDecoder<'a> {
    /// Create a new partial decoder for the sharding codec.
    pub async fn new(
        input_handle: Box<dyn AsyncBytesPartialDecoderTraits + 'a>,
        decoded_representation: ChunkRepresentation,
        chunk_shape: ChunkShape,
        inner_codecs: &'a CodecChain,
        index_codecs: &'a CodecChain,
        index_location: ShardingIndexLocation,
        options: &PartialDecodeOptions,
    ) -> Result<AsyncShardingPartialDecoder<'a>, CodecError> {
        let shard_index = Self::decode_shard_index(
            &*input_handle,
            index_codecs,
            index_location,
            chunk_shape.as_slice(),
            &decoded_representation,
            options,
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
        chunk_shape: &[NonZeroU64],
        decoded_representation: &ChunkRepresentation,
        options: &PartialDecodeOptions,
    ) -> Result<Option<Vec<u64>>, CodecError> {
        let shard_shape = decoded_representation.shape();
        let chunk_representation = unsafe {
            ChunkRepresentation::new_unchecked(
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
        let index_array_representation =
            sharding_index_decoded_representation(chunks_per_shard.as_slice());
        let index_encoded_size =
            compute_index_encoded_size(index_codecs, &index_array_representation)
                .map_err(|e| CodecError::Other(e.to_string()))?;

        // Decode the shard index
        let index_byte_range = match index_location {
            ShardingIndexLocation::Start => ByteRange::FromStart(0, Some(index_encoded_size)),
            ShardingIndexLocation::End => ByteRange::FromEnd(0, Some(index_encoded_size)),
        };

        let encoded_shard_index = input_handle
            .partial_decode_opt(&[index_byte_range], options)
            .await?
            .map(|mut v| v.remove(0));

        Ok(match encoded_shard_index {
            Some(encoded_shard_index) => Some(
                async_decode_shard_index(
                    encoded_shard_index,
                    &index_array_representation,
                    index_codecs,
                    options,
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
    #[allow(clippy::too_many_lines)]
    async fn partial_decode_opt(
        &self,
        array_subsets: &[ArraySubset],
        options: &PartialDecodeOptions,
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
        let chunks_per_shard = chunk_shape_to_array_shape(chunks_per_shard.as_slice());

        let element_size = self.decoded_representation.element_size();
        let mut out = Vec::with_capacity(array_subsets.len());
        // FIXME: Could go parallel here
        for array_subset in array_subsets {
            // shard (subset)
            let shard_size = array_subset.num_elements_usize() * element_size;
            let mut shard = Vec::with_capacity(shard_size);
            let shard_slice = UnsafeCellSlice::new_from_vec_with_spare_capacity(&mut shard);

            // Find filled / non filled chunks
            let chunk_info =
                unsafe { array_subset.chunks_unchecked(self.chunk_grid.chunk_shape()) }
                    .into_iter()
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
            let results = futures::future::join_all(
                chunk_info
                    .iter()
                    .filter_map(|(chunk_subset, offset_size)| {
                        offset_size
                            .as_ref()
                            .map(|offset_size| (chunk_subset, offset_size))
                    })
                    .map(|(chunk_subset, (offset, size))| async move {
                        let chunk_representation = unsafe {
                            ChunkRepresentation::new_unchecked(
                                self.chunk_grid.chunk_shape().to_vec(),
                                self.decoded_representation.data_type().clone(),
                                self.decoded_representation.fill_value().clone(),
                            )
                        };
                        let partial_decoder = self
                            .inner_codecs
                            .async_partial_decoder_opt(
                                Box::new(AsyncByteIntervalPartialDecoder::new(
                                    &*self.input_handle,
                                    u64::try_from(*offset).unwrap(),
                                    u64::try_from(*size).unwrap(),
                                )),
                                &chunk_representation,
                                options, // FIXME: Adjust options for partial decoding
                            )
                            .await?;
                        let overlap = unsafe { array_subset.overlap_unchecked(chunk_subset) };
                        let array_subset_in_chunk_subset =
                            unsafe { overlap.relative_to_unchecked(chunk_subset.start()) };
                        // Partial decoding is actually really slow with the blosc codec! Assume sharded chunks are small, and just decode the whole thing and extract bytes
                        // TODO: Investigate further
                        // let decoded_chunk = partial_decoder
                        //     .partial_decode(&[array_subset_in_chunk_subset])
                        //     .await?
                        //     .remove(0);
                        let decoded_chunk = partial_decoder
                            .partial_decode_opt(
                                &[ArraySubset::new_with_shape(chunk_subset.shape().to_vec())],
                                options,
                            ) // FIXME: Adjust options for partial decoding
                            .await?
                            .remove(0);
                        let decoded_chunk = array_subset_in_chunk_subset
                            .extract_bytes(&decoded_chunk, chunk_subset.shape(), element_size)
                            .unwrap();
                        let chunk_subset_in_array_subset =
                            unsafe { overlap.relative_to_unchecked(array_subset.start()) };
                        Ok::<_, CodecError>((chunk_subset_in_array_subset, decoded_chunk))
                    }),
            )
            .await;
            // FIXME: Concurrency limit for futures

            if !results.is_empty() {
                rayon_iter_concurrent_limit::iter_concurrent_limit!(
                    options.concurrent_limit(),
                    results.into_par_iter(),
                    try_for_each,
                    |subset_and_decoded_chunk| {
                        let (chunk_subset_in_array_subset, decoded_chunk) =
                            subset_and_decoded_chunk?;
                        let mut data_idx = 0;
                        let element_size = element_size as u64;
                        let shard_slice = unsafe { shard_slice.get() };
                        let contiguous_iterator = unsafe {
                            chunk_subset_in_array_subset
                                .contiguous_linearised_indices_unchecked(array_subset.shape())
                        };
                        let length = usize::try_from(
                            contiguous_iterator.contiguous_elements() * element_size,
                        )
                        .unwrap();
                        for (index, _num_elements) in &contiguous_iterator {
                            let shard_offset = usize::try_from(index * element_size).unwrap();
                            shard_slice[shard_offset..shard_offset + length]
                                .copy_from_slice(&decoded_chunk[data_idx..data_idx + length]);
                            data_idx += length;
                        }
                        Ok::<_, CodecError>(())
                    }
                )?;
            }

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
                let chunk_array_ss = ArraySubset::new_with_shape(self.chunk_grid.chunk_shape_u64());
                let filled_chunk = self
                    .decoded_representation
                    .fill_value()
                    .as_ne_bytes()
                    .repeat(chunk_array_ss.num_elements_usize());

                // Write filled chunks
                rayon_iter_concurrent_limit::iter_concurrent_limit!(
                    options.concurrent_limit(),
                    filled_chunks.into_par_iter(),
                    for_each,
                    |chunk_subset| {
                        let overlap = unsafe { array_subset.overlap_unchecked(chunk_subset) };
                        let chunk_subset_in_array_subset =
                            unsafe { overlap.relative_to_unchecked(array_subset.start()) };
                        let mut data_idx = 0;
                        let element_size = self.decoded_representation.element_size() as u64;
                        let shard_slice = unsafe { shard_slice.get() };
                        let contiguous_iterator = unsafe {
                            chunk_subset_in_array_subset
                                .contiguous_linearised_indices_unchecked(array_subset.shape())
                        };
                        let length = usize::try_from(
                            contiguous_iterator.contiguous_elements() * element_size,
                        )
                        .unwrap();
                        for (index, _num_elements) in &contiguous_iterator {
                            let shard_offset = usize::try_from(index * element_size).unwrap();
                            shard_slice[shard_offset..shard_offset + length]
                                .copy_from_slice(&filled_chunk[data_idx..data_idx + length]);
                            data_idx += length;
                        }
                    }
                );
            };
            unsafe { shard.set_len(shard_size) };
            out.push(shard);
        }
        Ok(out)
    }
}
