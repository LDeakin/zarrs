use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};

use itertools::Itertools;
use rayon::iter::{
    IndexedParallelIterator, IntoParallelIterator, IntoParallelRefIterator, ParallelIterator,
};

use crate::{
    array::{
        array_bytes::update_array_bytes,
        chunk_grid::{ChunkGridTraits, RegularChunkGrid},
        codec::{
            array_to_bytes::sharding::{calculate_chunks_per_shard, compute_index_encoded_size},
            ArrayPartialEncoderTraits, ArrayToBytesCodecTraits, BytesPartialDecoderTraits,
            BytesPartialEncoderTraits, CodecError, CodecOptions,
        },
        ravel_indices, transmute_to_bytes, ArrayBytes, ArraySize, ChunkRepresentation, ChunkShape,
        CodecChain, RawBytes,
    },
    array_subset::{ArraySubset, IncompatibleArraySubsetAndShapeError},
    byte_range::ByteRange,
};

use super::{sharding_index_decoded_representation, ShardingIndexLocation};

pub(crate) struct ShardingPartialEncoder {
    input_handle: Arc<dyn BytesPartialDecoderTraits>,
    output_handle: Arc<dyn BytesPartialEncoderTraits>,
    decoded_representation: ChunkRepresentation,
    chunk_grid: RegularChunkGrid,
    inner_codecs: Arc<CodecChain>,
    index_codecs: Arc<CodecChain>,
    index_location: ShardingIndexLocation,
    index_decoded_representation: ChunkRepresentation,
    inner_chunk_representation: ChunkRepresentation,
    shard_index: Arc<Mutex<Vec<u64>>>,
}

impl ShardingPartialEncoder {
    /// Create a new partial encoder for the sharding codec.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        input_handle: Arc<dyn BytesPartialDecoderTraits>,
        output_handle: Arc<dyn BytesPartialEncoderTraits>,
        decoded_representation: ChunkRepresentation,
        chunk_shape: ChunkShape,
        inner_codecs: Arc<CodecChain>,
        index_codecs: Arc<CodecChain>,
        index_location: ShardingIndexLocation,
        options: &CodecOptions,
    ) -> Result<Self, CodecError> {
        let chunks_per_shard =
            &calculate_chunks_per_shard(decoded_representation.shape(), &chunk_shape)?;
        let index_decoded_representation =
            sharding_index_decoded_representation(chunks_per_shard.as_slice());
        let inner_chunk_representation = ChunkRepresentation::new(
            chunk_shape.to_vec(),
            decoded_representation.data_type().clone(),
            decoded_representation.fill_value().clone(),
        )
        .map_err(|_| CodecError::Other("Fill value and data type are incompatible?".to_string()))?;

        // Decode the index
        let shard_index = super::decode_shard_index_partial_decoder(
            &*input_handle,
            &index_codecs,
            index_location,
            inner_chunk_representation.shape(),
            &decoded_representation,
            options,
        )?
        .unwrap_or_else(|| {
            let num_chunks =
                usize::try_from(chunks_per_shard.iter().map(|x| x.get()).product::<u64>()).unwrap();
            vec![u64::MAX; num_chunks * 2]
        });

        Ok(Self {
            input_handle,
            output_handle,
            decoded_representation,
            chunk_grid: RegularChunkGrid::new(chunk_shape),
            inner_codecs,
            index_codecs,
            index_location,
            index_decoded_representation,
            inner_chunk_representation,
            shard_index: Arc::new(Mutex::new(shard_index)),
        })
    }
}

impl ArrayPartialEncoderTraits for ShardingPartialEncoder {
    fn erase(&self) -> Result<(), super::CodecError> {
        self.output_handle.erase()
    }

    #[allow(clippy::too_many_lines)]
    #[allow(clippy::similar_names)]
    fn partial_encode(
        &self,
        subsets_and_bytes: &[(&ArraySubset, ArrayBytes<'_>)],
        options: &super::CodecOptions,
    ) -> Result<(), super::CodecError> {
        let mut shard_index = self.shard_index.lock().unwrap();

        let chunks_per_shard = calculate_chunks_per_shard(
            self.decoded_representation.shape(),
            self.inner_chunk_representation.shape(),
        )?;
        let chunks_per_shard = chunks_per_shard.to_array_shape();

        // Get the maximum offset of existing encoded chunks
        let max_data_offset = shard_index
            .iter()
            .tuples()
            .map(|(&offset, &size)| {
                if offset == u64::MAX && size == u64::MAX {
                    0
                } else {
                    offset + size
                }
            })
            .max()
            .expect("shards cannot be empty");

        let get_inner_chunks = |chunk_subset| {
            self.chunk_grid
                .chunks_in_array_subset(chunk_subset, &chunks_per_shard)
                .map_err(|_| {
                    CodecError::InvalidArraySubsetError(IncompatibleArraySubsetAndShapeError::new(
                        (*chunk_subset).clone(),
                        chunks_per_shard.clone(),
                    ))
                })?
                .ok_or_else(|| {
                    CodecError::Other(
                        "Cannot determine the inner chunk of a chunk subset".to_string(),
                    )
                })
        };
        let inner_chunk_fill_value = || {
            let array_size = ArraySize::new(
                self.inner_chunk_representation.data_type().size(),
                self.inner_chunk_representation.num_elements(),
            );
            ArrayBytes::new_fill_value(array_size, self.inner_chunk_representation.fill_value())
        };

        // Get all the inner chunks that need to be retrieved
        //   This only includes chunks that straddle chunk subsets.
        //   Chunks that are entirely within a chunk subset are entirely replaced and are not read.
        let shard_shape_u64 = self.decoded_representation.shape_u64();

        let mut inner_chunks_intersected = HashSet::<u64>::new();
        let mut inner_chunks_indices = HashSet::<u64>::new();

        for (chunk_subset, _chunk_subset_bytes) in subsets_and_bytes {
            // Check the subset is within the chunk shape
            if chunk_subset
                .end_exc()
                .iter()
                .zip(self.decoded_representation.shape())
                .any(|(a, b)| *a > b.get())
            {
                return Err(CodecError::InvalidArraySubsetError(
                    IncompatibleArraySubsetAndShapeError::new(
                        (*chunk_subset).clone(),
                        self.decoded_representation.shape_u64(),
                    ),
                ));
            }

            // Get the iterator over the inner chunks
            let inner_chunks = get_inner_chunks(chunk_subset)?;
            let inner_chunks = inner_chunks.indices();

            // Get all the inner chunks intersected
            inner_chunks_intersected.extend(
                inner_chunks.into_iter().map(|inner_chunk_indices| {
                    ravel_indices(&inner_chunk_indices, &chunks_per_shard)
                }),
            );

            // Get all the inner chunks that need to be updated
            inner_chunks_indices.extend(inner_chunks.into_iter().filter_map(
                |inner_chunk_indices| {
                    let inner_chunk_subset = self
                        .chunk_grid
                        .subset(&inner_chunk_indices, &shard_shape_u64)
                        .expect("already validated")
                        .expect("regular grid");

                    // Check if the inner chunk straddles the chunk subset
                    if inner_chunk_subset
                        .start()
                        .iter()
                        .zip(chunk_subset.start())
                        .any(|(a, b)| a < b)
                        || inner_chunk_subset
                            .end_exc()
                            .iter()
                            .zip(chunk_subset.end_exc())
                            .any(|(a, b)| *a > b)
                    {
                        let inner_chunk_index =
                            ravel_indices(&inner_chunk_indices, &chunks_per_shard);
                        Some(inner_chunk_index)
                    } else {
                        None
                    }
                },
            ));
        }

        // Get the byte ranges of the straddling inner chunk indices
        //   Sorting byte ranges may improves store retrieve efficiency in some cases
        let (inner_chunks_indices, byte_ranges): (Vec<_>, Vec<_>) = inner_chunks_indices
            .into_par_iter()
            .filter_map(|inner_chunk_index| {
                let offset = shard_index[usize::try_from(inner_chunk_index * 2).unwrap()];
                let size = shard_index[usize::try_from(inner_chunk_index * 2 + 1).unwrap()];
                if offset == u64::MAX && size == u64::MAX {
                    None
                } else {
                    Some((inner_chunk_index, ByteRange::FromStart(offset, Some(size))))
                }
            })
            .collect::<Vec<_>>()
            .into_iter()
            .sorted_by_key(|(_, byte_range)| *byte_range)
            .unzip();

        // Read the straddling inner chunks
        let inner_chunks_encoded = self
            .input_handle
            .partial_decode(&byte_ranges, options)?
            .map(|bytes| bytes.into_iter().map(Cow::into_owned).collect::<Vec<_>>());

        // Decode the straddling inner chunks
        let inner_chunks_decoded: HashMap<_, _> =
            if let Some(inner_chunks_encoded) = inner_chunks_encoded {
                let inner_chunks_encoded = inner_chunks_indices
                    .into_par_iter()
                    .zip(inner_chunks_encoded)
                    .map(|(inner_chunk_index, inner_chunk_encoded)| {
                        Ok((
                            inner_chunk_index,
                            self.inner_codecs.decode(
                                Cow::Owned(inner_chunk_encoded),
                                &self.inner_chunk_representation,
                                options,
                            )?,
                        ))
                    })
                    .collect::<Result<Vec<_>, CodecError>>()?;
                HashMap::from_iter(inner_chunks_encoded)
            } else {
                HashMap::new()
            };

        // Update all of the intersecting inner chunks
        //   This loop is intentionally not run in parallel so that overapping subset updates are applied incrementally rather than having a non deterministic output.
        let inner_chunks_decoded = Arc::new(Mutex::new(inner_chunks_decoded));
        for (chunk_subset, chunk_subset_bytes) in subsets_and_bytes {
            let inner_chunks = get_inner_chunks(chunk_subset)?;

            inner_chunks
                .indices()
                .into_par_iter()
                .try_for_each(|inner_chunk_indices| {
                    // Extract the inner chunk bytes that overlap with the chunk subset
                    let inner_chunk_index = ravel_indices(&inner_chunk_indices, &chunks_per_shard);
                    let inner_chunk_subset = self
                        .chunk_grid
                        .subset(&inner_chunk_indices, &chunks_per_shard)
                        .expect("already validated")
                        .expect("regular grid");
                    let inner_chunk_subset_overlap =
                        chunk_subset.overlap(&inner_chunk_subset).unwrap();
                    let inner_chunk_bytes = chunk_subset_bytes.extract_array_subset(
                        &inner_chunk_subset_overlap
                            .relative_to(chunk_subset.start())
                            .unwrap(),
                        chunk_subset.shape(),
                        self.inner_chunk_representation.data_type(),
                    )?;

                    // Decode the inner chunk
                    let inner_chunk_decoded = if let Some(inner_chunk_decoded) =
                        inner_chunks_decoded
                            .lock()
                            .unwrap()
                            .remove(&inner_chunk_index)
                    {
                        inner_chunk_decoded.into_owned()
                    } else {
                        inner_chunk_fill_value()
                    };

                    // Update the inner chunk
                    let inner_chunk_updated = update_array_bytes(
                        inner_chunk_decoded,
                        &self.inner_chunk_representation.shape_u64(),
                        &inner_chunk_subset_overlap
                            .relative_to(inner_chunk_subset.start())
                            .unwrap(),
                        &inner_chunk_bytes,
                        self.inner_chunk_representation.data_type().size(),
                    )?;
                    inner_chunks_decoded
                        .lock()
                        .unwrap()
                        .insert(inner_chunk_index, inner_chunk_updated);

                    Ok::<_, CodecError>(())
                })?;
        }
        let inner_chunks_decoded = Arc::try_unwrap(inner_chunks_decoded)
            .expect("inner_chunks_decoded should have one strong reference")
            .into_inner()
            .expect("inner_chunks_decoded should not be poisoned");

        // Encode the updated inner chunks
        let updated_inner_chunks = inner_chunks_decoded
            .into_par_iter()
            .map(|(inner_chunk_index, inner_chunk_decoded)| {
                if inner_chunk_decoded.is_fill_value(self.inner_chunk_representation.fill_value()) {
                    Ok((inner_chunk_index, None))
                } else {
                    let inner_chunk_encoded = self
                        .inner_codecs
                        .encode(
                            inner_chunk_decoded,
                            &self.inner_chunk_representation,
                            options,
                        )?
                        .into_owned();
                    Ok((inner_chunk_index, Some(inner_chunk_encoded)))
                }
            })
            .collect::<Result<Vec<_>, CodecError>>()?;

        // Check if the shard can be entirely rewritten instead of appended
        //  This occurs if the shard index is empty if all of the intersected inner chunks are removed
        for inner_chunk_index in &inner_chunks_intersected {
            shard_index[usize::try_from(inner_chunk_index * 2).unwrap()] = u64::MAX;
            shard_index[usize::try_from(inner_chunk_index * 2 + 1).unwrap()] = u64::MAX;
        }
        let max_data_offset = if shard_index.par_iter().all(|&x| x == u64::MAX) {
            self.output_handle.erase()?;
            0
        } else {
            max_data_offset
        };

        // Get the offset for new data
        let index_encoded_size = compute_index_encoded_size(
            self.index_codecs.as_ref(),
            &self.index_decoded_representation,
        )?;
        let offset_new_chunks = match self.index_location {
            ShardingIndexLocation::Start => max_data_offset.max(index_encoded_size),
            ShardingIndexLocation::End => max_data_offset,
        };

        // Update the shard index
        {
            let mut offset_append = offset_new_chunks;
            for (inner_chunk_index, inner_chunk_encoded) in &updated_inner_chunks {
                if let Some(inner_chunk_encoded) = inner_chunk_encoded {
                    let len = inner_chunk_encoded.len() as u64;
                    shard_index[usize::try_from(inner_chunk_index * 2).unwrap()] = offset_append;
                    shard_index[usize::try_from(inner_chunk_index * 2 + 1).unwrap()] = len;
                    offset_append += len;
                } else {
                    shard_index[usize::try_from(inner_chunk_index * 2).unwrap()] = u64::MAX;
                    shard_index[usize::try_from(inner_chunk_index * 2 + 1).unwrap()] = u64::MAX;
                }
            }
        }

        if shard_index.par_iter().all(|&x| x == u64::MAX) {
            // Erase the shard if all chunks are empty
            self.output_handle.erase()?;
        } else {
            // Encode the updated shard index
            let shard_index_bytes: RawBytes = transmute_to_bytes(shard_index.as_slice()).into();
            let encoded_array_index = self
                .index_codecs
                .encode(
                    shard_index_bytes.into(),
                    &self.index_decoded_representation,
                    options,
                )?
                .into_owned();

            // Get the total size of the encoded inner chunks
            let encoded_inner_chunks_size = updated_inner_chunks
                .iter()
                .filter_map(|(_, inner_chunk_encoded)| inner_chunk_encoded.as_ref().map(Vec::len))
                .sum::<usize>();

            // Get the suffix write size
            let suffix_write_size = match self.index_location {
                ShardingIndexLocation::Start => encoded_inner_chunks_size,
                ShardingIndexLocation::End => encoded_inner_chunks_size + encoded_array_index.len(),
            };

            // Concatenate the updated inner chunks
            let mut encoded_output = Vec::with_capacity(suffix_write_size);
            for (_, inner_chunk_encoded) in updated_inner_chunks {
                if let Some(inner_chunk_encoded) = inner_chunk_encoded {
                    encoded_output.extend(inner_chunk_encoded);
                }
            }

            // Write the encoded index and updated inner chunks
            match self.index_location {
                ShardingIndexLocation::Start => {
                    self.output_handle.partial_encode(
                        &[
                            (0, Cow::Owned(encoded_array_index)),
                            (offset_new_chunks, Cow::Owned(encoded_output)),
                        ],
                        options,
                    )?;
                }
                ShardingIndexLocation::End => {
                    encoded_output.extend(encoded_array_index);
                    self.output_handle.partial_encode(
                        &[(offset_new_chunks, Cow::Owned(encoded_output))],
                        options,
                    )?;
                }
            }
        }
        Ok(())
    }
}
