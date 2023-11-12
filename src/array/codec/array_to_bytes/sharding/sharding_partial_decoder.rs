use rayon::prelude::*;

use crate::{
    array::{
        chunk_grid::RegularChunkGrid,
        codec::{
            ArrayPartialDecoderTraits, ArraySubset, ArrayToBytesCodecTraits,
            ByteIntervalPartialDecoder, BytesPartialDecoderTraits, CodecChain, CodecError,
        },
        ravel_indices, ArrayRepresentation, ArrayShape, UnsafeCellSlice,
    },
    byte_range::ByteRange,
};

use super::{
    calculate_chunks_per_shard, compute_index_encoded_size, decode_shard_index,
    sharding_configuration::ShardingIndexLocation, sharding_index_decoded_representation,
};

/// The partial decoder for the sharding codec.
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
                    partial_decoder
                        .partial_decode(&[array_subset_in_chunk_subset.clone()])?
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
