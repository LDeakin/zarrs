use std::{
    collections::{hash_map::Entry, HashMap},
    sync::Arc,
};

use itertools::Itertools;

use crate::{
    array::{
        array_bytes::update_array_bytes,
        chunk_grid::{ChunkGridTraits, RegularChunkGrid},
        codec::{
            array_to_bytes::sharding::{calculate_chunks_per_shard, compute_index_encoded_size},
            ArrayPartialEncoderTraits, ArrayToBytesCodecTraits, BytesPartialDecoderTraits,
            BytesPartialEncoderTraits, CodecError, CodecOptions,
        },
        ravel_indices, transmute_to_bytes_vec, ArrayBytes, ArraySize, ChunkRepresentation,
        ChunkShape, CodecChain, RawBytes,
    },
    array_subset::{ArraySubset, IncompatibleArraySubsetAndShapeError},
    byte_range::ByteRange,
};

use super::{sharding_index_decoded_representation, ShardingIndexLocation};

pub struct ShardingPartialEncoder {
    input_handle: Arc<dyn BytesPartialDecoderTraits>,
    output_handle: Arc<dyn BytesPartialEncoderTraits>,
    decoded_representation: ChunkRepresentation,
    chunk_grid: RegularChunkGrid,
    inner_codecs: Arc<CodecChain>,
    index_codecs: Arc<CodecChain>,
    index_location: ShardingIndexLocation,
    index_decoded_representation: ChunkRepresentation,
    inner_chunk_representation: ChunkRepresentation,
}

impl ShardingPartialEncoder {
    /// Create a new partial encoder for the sharding codec.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        input_handle: Arc<dyn BytesPartialDecoderTraits>,
        output_handle: Arc<dyn BytesPartialEncoderTraits>,
        decoded_representation: ChunkRepresentation,
        chunk_shape: ChunkShape,
        inner_codecs: Arc<CodecChain>,
        index_codecs: Arc<CodecChain>,
        index_location: ShardingIndexLocation,
        _options: &CodecOptions,
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
        })
    }
}

impl ArrayPartialEncoderTraits for ShardingPartialEncoder {
    fn erase(&self) -> Result<(), CodecError> {
        self.output_handle.erase()
    }

    #[allow(clippy::too_many_lines)]
    fn partial_encode_opt(
        &self,
        chunk_subsets: &[&ArraySubset],
        chunk_subsets_bytes: Vec<crate::array::ArrayBytes<'_>>,
        options: &super::CodecOptions,
    ) -> Result<(), super::CodecError> {
        // Decode the index
        let mut chunks_per_shard = self.index_decoded_representation.shape_u64();
        chunks_per_shard.truncate(chunks_per_shard.len() - 1); // has "2" on last axis for offset/size
        let mut shard_index = super::decode_shard_index_partial_decoder(
            &*self.input_handle,
            &self.index_codecs,
            self.index_location,
            self.inner_chunk_representation.shape(),
            &self.decoded_representation,
            options,
        )?
        .unwrap_or_else(|| {
            let num_chunks = usize::try_from(chunks_per_shard.iter().product::<u64>()).unwrap();
            vec![u64::MAX; num_chunks * 2]
        });

        let mut updated_inner_chunks = HashMap::<u64, ArrayBytes>::new();
        for (subset, subset_bytes) in std::iter::zip(chunk_subsets, chunk_subsets_bytes) {
            let inner_chunks = self
                .chunk_grid
                .chunks_in_array_subset(subset, &chunks_per_shard)
                .map_err(|_| {
                    CodecError::InvalidArraySubsetError(IncompatibleArraySubsetAndShapeError::new(
                        (*subset).clone(),
                        chunks_per_shard.clone(),
                    ))
                })?
                .ok_or_else(|| {
                    CodecError::Other(
                        "Cannot determine the inner chunk of a chunk subset".to_string(),
                    )
                })?;

            let inner_chunk_fill_value = || {
                let array_size = ArraySize::new(
                    self.inner_chunk_representation.data_type().size(),
                    self.inner_chunk_representation.num_elements(),
                );
                ArrayBytes::new_fill_value(array_size, self.inner_chunk_representation.fill_value())
            };

            for inner_chunk_indices in &inner_chunks.indices() {
                let inner_chunk_index = ravel_indices(&inner_chunk_indices, &chunks_per_shard);
                // Decode the inner chunk (if needed)
                if let Entry::Vacant(entry) = updated_inner_chunks.entry(inner_chunk_index) {
                    let inner_chunk_index_usize = usize::try_from(inner_chunk_index).unwrap();

                    // Get the offset/size of the chunk and temporarily remove it from the shard index
                    let offset = shard_index[inner_chunk_index_usize * 2];
                    let size = shard_index[inner_chunk_index_usize * 2 + 1];
                    shard_index[inner_chunk_index_usize * 2] = u64::MAX;
                    shard_index[inner_chunk_index_usize * 2 + 1] = u64::MAX;

                    let inner_chunk_decoded = if offset == u64::MAX && size == u64::MAX {
                        inner_chunk_fill_value()
                    } else {
                        let inner_chunk_encoded = self
                            .input_handle
                            .partial_decode(&[ByteRange::FromStart(offset, Some(size))], options)?
                            .map(|mut bytes| bytes.pop().unwrap());
                        if let Some(inner_chunk_encoded) = inner_chunk_encoded {
                            self.inner_codecs.decode(
                                inner_chunk_encoded,
                                &self.inner_chunk_representation,
                                options,
                            )?
                        } else {
                            inner_chunk_fill_value()
                        }
                    };
                    entry.insert(inner_chunk_decoded);
                }

                let inner_chunk_decoded = updated_inner_chunks.get_mut(&inner_chunk_index).unwrap();

                // Update the inner chunk
                // FIXME: Does this work if subset is not a subset of the shape?
                let inner_chunk_subset = self
                    .chunk_grid
                    .subset(&inner_chunk_indices, &chunks_per_shard)
                    .expect("already validated")
                    .expect("regular grid");
                let inner_chunk_subset_overlap = subset.overlap(&inner_chunk_subset).unwrap();

                let inner_chunk_bytes = subset_bytes.extract_array_subset(
                    &inner_chunk_subset_overlap
                        .relative_to(subset.start())
                        .unwrap(),
                    subset.shape(),
                    self.inner_chunk_representation.data_type(),
                )?;

                *inner_chunk_decoded = update_array_bytes(
                    inner_chunk_decoded.clone(),
                    self.inner_chunk_representation.shape_u64(),
                    inner_chunk_bytes,
                    &inner_chunk_subset_overlap
                        .relative_to(inner_chunk_subset.start())
                        .unwrap(),
                    self.inner_chunk_representation.data_type().size(),
                );
            }
        }

        // Get the maximum offset of existing encoded chunks
        // TODO: This is append only! Can do much better and slot chunks in the middle
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

        // Get the offset for new data
        let index_encoded_size = compute_index_encoded_size(
            self.index_codecs.as_ref(),
            &self.index_decoded_representation,
        )?;
        let mut offset_append = match self.index_location {
            ShardingIndexLocation::Start => max_data_offset.max(index_encoded_size),
            ShardingIndexLocation::End => max_data_offset,
        };

        // Write the updated chunks
        for (inner_chunk_index, inner_chunk_decoded) in updated_inner_chunks {
            let inner_chunk_encoded = self.inner_codecs.encode(
                inner_chunk_decoded,
                &self.inner_chunk_representation,
                options,
            )?;
            let len = inner_chunk_encoded.len() as u64;
            self.output_handle.partial_encode(
                &[ByteRange::FromStart(offset_append, Some(len))],
                vec![inner_chunk_encoded],
            )?;

            shard_index[usize::try_from(inner_chunk_index * 2).unwrap()] = offset_append;
            shard_index[usize::try_from(inner_chunk_index * 2 + 1).unwrap()] = len;
            offset_append += len;
        }

        // Write the updated shard index
        let shard_index_bytes: RawBytes = transmute_to_bytes_vec(shard_index).into();
        let encoded_array_index = self.index_codecs.encode(
            shard_index_bytes.into(),
            &self.index_decoded_representation,
            options,
        )?;
        {
            match self.index_location {
                ShardingIndexLocation::Start => {
                    self.output_handle.partial_encode(
                        &[ByteRange::FromStart(
                            0,
                            Some(encoded_array_index.len() as u64),
                        )],
                        vec![encoded_array_index],
                    )?;
                }
                ShardingIndexLocation::End => {
                    self.output_handle.partial_encode(
                        &[ByteRange::FromStart(
                            offset_append,
                            Some(encoded_array_index.len() as u64),
                        )],
                        vec![encoded_array_index],
                    )?;
                }
            }
        }
        Ok(())
    }
}
