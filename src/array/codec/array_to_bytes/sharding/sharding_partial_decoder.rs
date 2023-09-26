use parking_lot::RwLock;
use rayon::prelude::*;

use crate::{
    array::{
        chunk_grid::RegularChunkGrid,
        codec::{
            ArrayPartialDecoderTraits, ArraySubset, ArrayToBytesCodecTraits,
            ByteIntervalPartialDecoder, BytesPartialDecoderTraits, CodecChain, CodecError,
        },
        ravel_indices, ArrayRepresentation, ArrayShape, BytesRepresentation,
    },
    byte_range::ByteRange,
};

use super::{
    calculate_chunks_per_shard, compute_index_encoded_size, decode_shard_index,
    sharding_index_decoded_representation, UnsafeCellSlice,
};

/// The partial decoder for the sharding codec.
pub struct ShardingPartialDecoder<'a> {
    input_handle: Box<dyn BytesPartialDecoderTraits + 'a>,
    chunk_grid: RegularChunkGrid,
    inner_codecs: &'a CodecChain,
    index_codecs: &'a CodecChain,
    shard_index: RwLock<Option<Vec<u64>>>,
}

impl<'a> ShardingPartialDecoder<'a> {
    /// Create a new partial decoder for the sharding codec.
    pub fn new(
        input_handle: Box<dyn BytesPartialDecoderTraits + 'a>,
        chunk_shape: ArrayShape,
        inner_codecs: &'a CodecChain,
        index_codecs: &'a CodecChain,
    ) -> Self {
        Self {
            input_handle,
            chunk_grid: RegularChunkGrid::new(chunk_shape),
            inner_codecs,
            index_codecs,
            shard_index: RwLock::new(None),
        }
    }

    fn decode_shard_index(
        &self,
        decoded_representation: &ArrayRepresentation,
        parallel: bool,
    ) -> Result<Vec<u64>, CodecError> {
        let shard_shape = decoded_representation.shape();
        let chunk_representation = unsafe {
            ArrayRepresentation::new_unchecked(
                self.chunk_grid.chunk_shape().to_vec(),
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
            compute_index_encoded_size(self.index_codecs, &index_array_representation)
                .map_err(|e| CodecError::Other(e.to_string()))?;

        // Decode the shard index
        let encoded_shard_index = if parallel {
            self.input_handle.par_partial_decode(
                &BytesRepresentation::VariableSize,
                &[ByteRange::FromEnd(0, Some(index_encoded_size))],
            )
        } else {
            self.input_handle.partial_decode(
                &BytesRepresentation::VariableSize,
                &[ByteRange::FromEnd(0, Some(index_encoded_size))],
            )
        }?
        .remove(0);

        decode_shard_index(
            &encoded_shard_index,
            &index_array_representation,
            self.index_codecs,
            parallel,
        )
    }
}

impl ArrayPartialDecoderTraits for ShardingPartialDecoder<'_> {
    fn partial_decode(
        &self,
        decoded_representation: &ArrayRepresentation,
        decoded_regions: &[ArraySubset],
    ) -> Result<Vec<Vec<u8>>, CodecError> {
        let mut read_shard_index = self.shard_index.read();
        if read_shard_index.is_none() {
            drop(read_shard_index);
            let mut write_shard_index = self.shard_index.write();
            if write_shard_index.is_none() {
                *write_shard_index = Some(self.decode_shard_index(decoded_representation, false)?);
            }
            drop(write_shard_index);
            read_shard_index = self.shard_index.read();
        }
        let shard_index = read_shard_index.as_ref().unwrap();

        let chunk_representation = unsafe {
            ArrayRepresentation::new_unchecked(
                self.chunk_grid.chunk_shape().to_vec(),
                decoded_representation.data_type().clone(),
                decoded_representation.fill_value().clone(),
            )
        };

        let chunks_per_shard = calculate_chunks_per_shard(
            decoded_representation.shape(),
            chunk_representation.shape(),
        )
        .map_err(|e| CodecError::Other(e.to_string()))?;

        let element_size = decoded_representation.element_size();
        let fill_value = chunk_representation.fill_value().as_ne_bytes();

        let mut out = Vec::with_capacity(decoded_regions.len());
        for array_subset in decoded_regions {
            let array_subset_size = array_subset.num_elements() * element_size;
            let mut out_array_subset = vec![0; array_subset_size];

            // Decode those chunks if required and put in chunk cache
            for (chunk_indices, chunk_subset) in
                unsafe { array_subset.iter_chunks_unchecked(chunk_representation.shape()) }
            {
                let shard_index_index: usize = ravel_indices(&chunk_indices, &chunks_per_shard) * 2;
                let offset = shard_index[shard_index_index];
                let size = shard_index[shard_index_index + 1];

                let decoded_bytes = if offset == u64::MAX && size == u64::MAX {
                    // The chunk is just the fill value
                    fill_value.repeat(chunk_subset.num_elements())
                } else {
                    // The chunk must be decoded
                    let partial_decoder = self.inner_codecs.partial_decoder(Box::new(
                        ByteIntervalPartialDecoder::new(
                            &*self.input_handle,
                            offset.try_into().unwrap(),
                            size.try_into().unwrap(),
                        ),
                    ));
                    let array_subset_in_chunk_subset =
                        unsafe { array_subset.in_subset_unchecked(&chunk_subset) };
                    partial_decoder
                        .partial_decode(
                            &chunk_representation,
                            &[array_subset_in_chunk_subset.clone()],
                        )?
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
                    let output_offset = array_subset_element_index * element_size;
                    let length = num_elements * element_size;
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
        decoded_representation: &ArrayRepresentation,
        decoded_regions: &[ArraySubset],
    ) -> Result<Vec<Vec<u8>>, CodecError> {
        let mut read_shard_index = self.shard_index.read();
        if read_shard_index.is_none() {
            drop(read_shard_index);
            let mut write_shard_index = self.shard_index.write();
            if write_shard_index.is_none() {
                *write_shard_index = Some(self.decode_shard_index(decoded_representation, false)?);
            }
            drop(write_shard_index);
            read_shard_index = self.shard_index.read();
        }
        let shard_index = read_shard_index.as_ref().unwrap();

        let chunk_representation = unsafe {
            ArrayRepresentation::new_unchecked(
                self.chunk_grid.chunk_shape().to_vec(),
                decoded_representation.data_type().clone(),
                decoded_representation.fill_value().clone(),
            )
        };

        let chunks_per_shard = calculate_chunks_per_shard(
            decoded_representation.shape(),
            chunk_representation.shape(),
        )
        .map_err(|e| CodecError::Other(e.to_string()))?;

        let element_size = decoded_representation.element_size();
        let fill_value = chunk_representation.fill_value().as_ne_bytes();

        let mut out = Vec::with_capacity(decoded_regions.len());
        for array_subset in decoded_regions {
            let array_subset_size = array_subset.num_elements() * element_size;
            let mut out_array_subset = vec![0; array_subset_size];
            let out_array_subset_slice = UnsafeCellSlice::new(out_array_subset.as_mut_slice());

            // Decode those chunks if required and put in chunk cache
            unsafe { array_subset.iter_chunks_unchecked(chunk_representation.shape()) }
                .par_bridge()
                .map(|(chunk_indices, chunk_subset)| {
                    let shard_index_index: usize =
                        ravel_indices(&chunk_indices, &chunks_per_shard) * 2;
                    let offset = shard_index[shard_index_index];
                    let size = shard_index[shard_index_index + 1];

                    // Get the subset of bytes from the chunk which intersect the array
                    let array_subset_in_chunk_subset =
                        unsafe { array_subset.in_subset_unchecked(&chunk_subset) };

                    let decoded_bytes = if offset == u64::MAX && size == u64::MAX {
                        // The chunk is just the fill value
                        fill_value.repeat(array_subset_in_chunk_subset.num_elements())
                    } else {
                        // The chunk must be decoded
                        let partial_decoder = self.inner_codecs.partial_decoder(Box::new(
                            ByteIntervalPartialDecoder::new(
                                &*self.input_handle,
                                offset.try_into().unwrap(),
                                size.try_into().unwrap(),
                            ),
                        ));
                        // NOTE: Intentionally using single threaded decode, since parallelisation is in the loop
                        partial_decoder
                            .partial_decode(&chunk_representation, &[array_subset_in_chunk_subset])?
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
                        let output_offset = array_subset_element_index * element_size;
                        let length = num_elements * element_size;
                        unsafe {
                            out_array_subset_slice.copy_from_slice(
                                output_offset,
                                &decoded_bytes[decoded_offset..decoded_offset + length],
                            );
                        }
                        decoded_offset += length;
                    }
                    Ok::<_, CodecError>(())
                })
                .collect::<Result<Vec<_>, CodecError>>()?;
            out.push(out_array_subset);
        }
        Ok(out)
    }
}
