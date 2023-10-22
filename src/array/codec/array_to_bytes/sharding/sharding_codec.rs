use crate::{
    array::{
        codec::{
            ArrayCodecTraits, ArrayPartialDecoderTraits, ArrayToBytesCodecTraits,
            BytesPartialDecoderTraits, Codec, CodecChain, CodecError, CodecPlugin, CodecTraits,
        },
        ArrayRepresentation, BytesRepresentation, UnsafeCellSlice,
    },
    array_subset::ArraySubset,
    metadata::Metadata,
    plugin::PluginCreateError,
};

use super::{
    calculate_chunks_per_shard, compute_index_encoded_size, decode_shard_index,
    sharding_index_decoded_representation, sharding_partial_decoder, ShardingCodecConfiguration,
    ShardingCodecConfigurationV1,
};

use rayon::prelude::*;

const IDENTIFIER: &str = "sharding_indexed";

// Register the codec.
inventory::submit! {
    CodecPlugin::new(IDENTIFIER, is_name_sharding, create_codec_sharding)
}

fn is_name_sharding(name: &str) -> bool {
    name.eq(IDENTIFIER)
}

fn create_codec_sharding(metadata: &Metadata) -> Result<Codec, PluginCreateError> {
    let configuration: ShardingCodecConfiguration = metadata.to_configuration()?;
    let codec = ShardingCodec::new_with_configuration(&configuration)?;
    Ok(Codec::ArrayToBytes(Box::new(codec)))
}

/// A Sharding codec implementation.
#[derive(Clone, Debug)]
pub struct ShardingCodec {
    /// An array of integers specifying the shape of the inner chunks in a shard along each dimension of the outer array.
    chunk_shape: Vec<u64>,
    /// The codecs used to encode and decode inner chunks.
    inner_codecs: CodecChain,
    /// The codecs used to encode and decode the shard index.
    index_codecs: CodecChain,
}

impl ShardingCodec {
    /// Create a new `sharding` codec.
    #[must_use]
    pub fn new(chunk_shape: Vec<u64>, inner_codecs: CodecChain, index_codecs: CodecChain) -> Self {
        Self {
            chunk_shape,
            inner_codecs,
            index_codecs,
        }
    }

    /// Create a new `sharding` codec from configuration.
    ///
    /// # Errors
    ///
    /// Returns [`PluginCreateError`] if there is a configuration issue.
    pub fn new_with_configuration(
        configuration: &ShardingCodecConfiguration,
    ) -> Result<Self, PluginCreateError> {
        let ShardingCodecConfiguration::V1(configuration) = configuration;
        let inner_codecs = CodecChain::from_metadata(&configuration.codecs)?;
        let index_codecs = CodecChain::from_metadata(&configuration.index_codecs)?;
        Ok(Self::new(
            configuration.chunk_shape.clone(),
            inner_codecs,
            index_codecs,
        ))
    }
}

impl CodecTraits for ShardingCodec {
    fn create_metadata(&self) -> Option<Metadata> {
        let configuration = ShardingCodecConfigurationV1 {
            chunk_shape: self.chunk_shape.clone(),
            codecs: self.inner_codecs.create_metadatas(),
            index_codecs: self.index_codecs.create_metadatas(),
        };
        Some(Metadata::new_with_serializable_configuration(IDENTIFIER, &configuration).unwrap())
    }

    fn partial_decoder_should_cache_input(&self) -> bool {
        false
    }

    fn partial_decoder_decodes_all(&self) -> bool {
        false
    }
}

impl ArrayCodecTraits for ShardingCodec {
    fn encode(
        &self,
        decoded_value: Vec<u8>,
        shard_representation: &ArrayRepresentation,
    ) -> Result<Vec<u8>, CodecError> {
        if decoded_value.len() as u64 != shard_representation.size() {
            return Err(CodecError::UnexpectedChunkDecodedSize(
                decoded_value.len(),
                shard_representation.size(),
            ));
        }

        let chunk_representation = unsafe {
            ArrayRepresentation::new_unchecked(
                self.chunk_shape.clone(),
                shard_representation.data_type().clone(),
                shard_representation.fill_value().clone(),
            )
        };
        let chunks_per_shard =
            calculate_chunks_per_shard(shard_representation.shape(), chunk_representation.shape())
                .map_err(|e| CodecError::Other(e.to_string()))?;

        // Create array index
        let index_decoded_representation = sharding_index_decoded_representation(&chunks_per_shard);
        let mut shard_index = vec![u64::MAX; index_decoded_representation.num_elements_usize()];

        // Iterate over chunk indices
        let mut shard_inner_chunks = Vec::new();
        let mut encoded_shard_offset: usize = 0;
        for (chunk_index, (_chunk_indices, chunk_subset)) in unsafe {
            ArraySubset::new_with_shape(shard_representation.shape().to_vec())
                .iter_chunks_unchecked(&self.chunk_shape)
        }
        .enumerate()
        {
            let bytes = unsafe {
                chunk_subset.extract_bytes_unchecked(
                    &decoded_value,
                    shard_representation.shape(),
                    shard_representation.element_size(),
                )
            };
            let all_fill_value = chunk_representation.fill_value().equals_all(&bytes);
            if !all_fill_value {
                // Encode chunk
                let chunk_encoded = self.inner_codecs.encode(bytes, &chunk_representation)?;

                // Append chunk, update array index and offset
                shard_index[chunk_index * 2] = encoded_shard_offset.try_into().unwrap();
                shard_index[chunk_index * 2 + 1] = chunk_encoded.len().try_into().unwrap();
                encoded_shard_offset += chunk_encoded.len();
                shard_inner_chunks.push(chunk_encoded);
            }
        }

        // Encode array index
        let shard_index = safe_transmute::transmute_to_bytes(&shard_index);
        let encoded_array_index = self
            .index_codecs
            .encode(shard_index.to_vec(), &index_decoded_representation)?;

        // Encode the shard
        let shard_size =
            shard_inner_chunks.iter().map(Vec::len).sum::<usize>() + encoded_array_index.len();
        let mut shard = Vec::with_capacity(shard_size);
        for chunk in shard_inner_chunks {
            shard.extend(chunk);
        }
        shard.extend(encoded_array_index);
        Ok(shard)
    }

    fn par_encode(
        &self,
        decoded_value: Vec<u8>,
        shard_representation: &ArrayRepresentation,
    ) -> Result<Vec<u8>, CodecError> {
        if decoded_value.len() as u64 != shard_representation.size() {
            return Err(CodecError::UnexpectedChunkDecodedSize(
                decoded_value.len(),
                shard_representation.size(),
            ));
        }

        let chunk_representation = unsafe {
            ArrayRepresentation::new_unchecked(
                self.chunk_shape.clone(),
                shard_representation.data_type().clone(),
                shard_representation.fill_value().clone(),
            )
        };
        let chunks_per_shard =
            calculate_chunks_per_shard(shard_representation.shape(), chunk_representation.shape())
                .map_err(|e| CodecError::Other(e.to_string()))?;

        // Iterate over chunk indices
        let shard_inner_chunks = unsafe {
            ArraySubset::new_with_shape(shard_representation.shape().to_vec())
                .iter_chunks_unchecked(&self.chunk_shape)
        }
        .enumerate()
        .par_bridge()
        .map(|(chunk_index, (_chunk_indices, chunk_subset))| {
            let bytes = unsafe {
                chunk_subset.extract_bytes_unchecked(
                    &decoded_value,
                    shard_representation.shape(),
                    shard_representation.element_size(),
                )
            };
            let all_fill_value = chunk_representation.fill_value().equals_all(&bytes);
            if all_fill_value {
                Ok((chunk_index, None))
            } else {
                // let chunk_encoded = self.inner_codecs.par_encode(bytes, &chunk_representation)?;
                let chunk_encoded = self.inner_codecs.encode(bytes, &chunk_representation)?;
                Ok((chunk_index, Some(chunk_encoded)))
            }
        })
        .collect::<Result<Vec<_>, CodecError>>()?;

        // Sort into chunk order
        // shard_inner_chunks.sort_by(|a, b| Ord::cmp(&a.0, &b.0));

        // Write the shard index
        let index_decoded_representation = sharding_index_decoded_representation(&chunks_per_shard);
        let mut shard_index = vec![u64::MAX; index_decoded_representation.num_elements_usize()];
        let mut offset = 0;
        for (chunk_index, chunk) in &shard_inner_chunks {
            if let Some(chunk) = chunk {
                shard_index[chunk_index * 2] = offset.try_into().unwrap();
                shard_index[chunk_index * 2 + 1] = chunk.len().try_into().unwrap();
                offset += chunk.len();
            }
        }

        // Encode array index
        let shard_index = safe_transmute::transmute_to_bytes(&shard_index);
        let encoded_array_index = self
            .index_codecs
            .par_encode(shard_index.to_vec(), &index_decoded_representation)?;

        // Encode the shard
        let shard_size = shard_inner_chunks
            .iter()
            .map(|(_, chunk)| chunk.as_ref().map_or(0, Vec::len))
            .sum::<usize>()
            + encoded_array_index.len();
        let mut shard = Vec::with_capacity(shard_size);
        for chunk in shard_inner_chunks
            .into_iter()
            .filter_map(|(_, chunk)| chunk)
        {
            shard.extend(chunk);
        }
        shard.extend(encoded_array_index);
        Ok(shard)
    }

    fn decode(
        &self,
        encoded_value: Vec<u8>,
        decoded_representation: &ArrayRepresentation,
    ) -> Result<Vec<u8>, CodecError> {
        let chunks_per_shard =
            calculate_chunks_per_shard(decoded_representation.shape(), &self.chunk_shape)
                .map_err(|e| CodecError::Other(e.to_string()))?;
        let shard_index = self.decode_index(&encoded_value, &chunks_per_shard, false)?;
        let chunks = self.decode_chunks(&encoded_value, &shard_index, decoded_representation)?;
        Ok(chunks)
    }

    fn par_decode(
        &self,
        encoded_value: Vec<u8>,
        decoded_representation: &ArrayRepresentation,
    ) -> Result<Vec<u8>, CodecError> {
        let chunks_per_shard =
            calculate_chunks_per_shard(decoded_representation.shape(), &self.chunk_shape)
                .map_err(|e| CodecError::Other(e.to_string()))?;
        let shard_index = self.decode_index(&encoded_value, &chunks_per_shard, true)?;
        let chunks =
            self.par_decode_chunks(&encoded_value, &shard_index, decoded_representation)?;
        Ok(chunks)
    }
}

impl ArrayToBytesCodecTraits for ShardingCodec {
    fn partial_decoder<'a>(
        &'a self,
        input_handle: Box<dyn BytesPartialDecoderTraits + 'a>,
    ) -> Box<dyn ArrayPartialDecoderTraits + 'a> {
        Box::new(sharding_partial_decoder::ShardingPartialDecoder::new(
            input_handle,
            self.chunk_shape.clone(),
            &self.inner_codecs,
            &self.index_codecs,
        ))
    }

    fn compute_encoded_size(
        &self,
        _decoded_representation: &ArrayRepresentation,
    ) -> Result<BytesRepresentation, CodecError> {
        Ok(BytesRepresentation::VariableSize)
    }
}

impl ShardingCodec {
    fn decode_index(
        &self,
        encoded_shard: &[u8],
        chunks_per_shard: &[u64],
        parallel: bool,
    ) -> Result<Vec<u64>, CodecError> {
        // Get index array representation and encoded size
        let index_array_representation = sharding_index_decoded_representation(chunks_per_shard);
        let index_encoded_size =
            compute_index_encoded_size(&self.index_codecs, &index_array_representation)?;

        // Get encoded shard index
        if (encoded_shard.len() as u64) < index_encoded_size {
            return Err(CodecError::Other(
                "The encoded shard is smaller than the expected size of its index.".to_string(),
            ));
        }
        let encoded_shard_offset =
            usize::try_from(encoded_shard.len() as u64 - index_encoded_size).unwrap();
        let encoded_shard_index = &encoded_shard[encoded_shard_offset..];

        // Decode the shard index
        decode_shard_index(
            encoded_shard_index,
            &index_array_representation,
            &self.index_codecs,
            parallel,
        )
    }

    fn decode_chunks(
        &self,
        encoded_shard: &[u8],
        shard_index: &[u64],
        shard_representation: &ArrayRepresentation,
    ) -> Result<Vec<u8>, CodecError> {
        // Allocate an array for the output
        let mut shard = shard_representation
            .fill_value()
            .as_ne_bytes()
            .repeat(shard_representation.num_elements_usize());

        // Decode chunks
        let chunk_representation = unsafe {
            ArrayRepresentation::new_unchecked(
                self.chunk_shape.clone(),
                shard_representation.data_type().clone(),
                shard_representation.fill_value().clone(),
            )
        };
        let element_size = chunk_representation.element_size() as u64;
        for (chunk_index, (_chunk_indices, chunk_subset)) in unsafe {
            ArraySubset::new_with_shape(shard_representation.shape().to_vec())
                .iter_chunks_unchecked(&self.chunk_shape)
        }
        .enumerate()
        {
            // Read the offset/size
            let offset = shard_index[chunk_index * 2];
            let size = shard_index[chunk_index * 2 + 1];
            if offset != u64::MAX || size != u64::MAX {
                let offset: usize = offset.try_into().unwrap(); // safe
                let size: usize = size.try_into().unwrap(); // safe
                let encoded_chunk_slice = encoded_shard[offset..offset + size].to_vec();
                let decoded_chunk = self
                    .inner_codecs
                    .decode(encoded_chunk_slice, &chunk_representation)?;

                // Copy to subset of shard
                let mut data_idx = 0;
                for (index, num_elements) in unsafe {
                    chunk_subset
                        .iter_contiguous_linearised_indices_unchecked(shard_representation.shape())
                } {
                    let shard_offset = usize::try_from(index * element_size).unwrap();
                    let length = usize::try_from(num_elements * element_size).unwrap();
                    shard[shard_offset..shard_offset + length]
                        .copy_from_slice(&decoded_chunk[data_idx..data_idx + length]);
                    data_idx += length;
                }
            }
        }
        Ok(shard)
    }

    fn par_decode_chunks(
        &self,
        encoded_shard: &[u8],
        shard_index: &[u64],
        shard_representation: &ArrayRepresentation,
    ) -> Result<Vec<u8>, CodecError> {
        // Allocate an array for the output
        let mut shard = shard_representation
            .fill_value()
            .as_ne_bytes()
            .repeat(shard_representation.num_elements_usize());
        let shard_slice = UnsafeCellSlice::new(shard.as_mut_slice());

        // Decode chunks
        let chunk_repr = unsafe {
            ArrayRepresentation::new_unchecked(
                self.chunk_shape.clone(),
                shard_representation.data_type().clone(),
                shard_representation.fill_value().clone(),
            )
        };
        unsafe {
            ArraySubset::new_with_shape(shard_representation.shape().to_vec())
                .iter_chunks_unchecked(&self.chunk_shape)
        }
        .enumerate()
        .par_bridge()
        .map(|(chunk_index, (_chunk_indices, chunk_subset))| {
            let shard_slice = unsafe { shard_slice.get() };

            // Read the offset/size
            let offset = shard_index[chunk_index * 2];
            let size = shard_index[chunk_index * 2 + 1];
            if offset != u64::MAX || size != u64::MAX {
                let offset: usize = offset.try_into().unwrap(); // safe
                let size: usize = size.try_into().unwrap(); // safe
                let encoded_chunk_slice = encoded_shard[offset..offset + size].to_vec();
                // NOTE: Intentionally using single threaded decode, since parallelisation is in the loop
                let decoded_chunk = self.inner_codecs.decode(encoded_chunk_slice, &chunk_repr)?;

                // Copy to subset of shard
                let mut data_idx = 0;
                let element_size = chunk_repr.element_size() as u64;
                for (index, num_elements) in unsafe {
                    chunk_subset
                        .iter_contiguous_linearised_indices_unchecked(shard_representation.shape())
                } {
                    let shard_offset = usize::try_from(index * element_size).unwrap();
                    let length = usize::try_from(num_elements * element_size).unwrap();
                    shard_slice[shard_offset..shard_offset + length]
                        .copy_from_slice(&decoded_chunk[data_idx..data_idx + length]);
                    data_idx += length;
                }
            }
            Ok::<_, CodecError>(())
        })
        .collect::<Result<Vec<_>, CodecError>>()?;

        Ok(shard)
    }
}
