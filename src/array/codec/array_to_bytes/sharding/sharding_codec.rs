use std::{mem::MaybeUninit, sync::atomic::AtomicUsize};

use crate::{
    array::{
        codec::{
            ArrayCodecTraits, ArrayPartialDecoderTraits, ArrayToBytesCodecTraits,
            BytesPartialDecoderTraits, Codec, CodecChain, CodecError, CodecPlugin, CodecTraits,
        },
        unravel_index, ArrayRepresentation, BytesRepresentation, UnsafeCellSlice,
    },
    array_subset::ArraySubset,
    metadata::Metadata,
    plugin::PluginCreateError,
};

use super::{
    calculate_chunks_per_shard, compute_index_encoded_size, decode_shard_index,
    sharding_configuration::ShardingIndexLocation, sharding_index_decoded_representation,
    sharding_partial_decoder, ShardingCodecConfiguration, ShardingCodecConfigurationV1,
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
    /// Specifies whether the shard index is located at the beginning or end of the file.
    index_location: ShardingIndexLocation,
}

impl ShardingCodec {
    /// Create a new `sharding` codec.
    #[must_use]
    pub fn new(
        chunk_shape: Vec<u64>,
        inner_codecs: CodecChain,
        index_codecs: CodecChain,
        index_location: ShardingIndexLocation,
    ) -> Self {
        Self {
            chunk_shape,
            inner_codecs,
            index_codecs,
            index_location,
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
            configuration.index_location,
        ))
    }
}

impl CodecTraits for ShardingCodec {
    fn create_metadata(&self) -> Option<Metadata> {
        let configuration = ShardingCodecConfigurationV1 {
            chunk_shape: self.chunk_shape.clone(),
            codecs: self.inner_codecs.create_metadatas(),
            index_codecs: self.index_codecs.create_metadatas(),
            index_location: self.index_location,
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
        // TODO: Preallocate, like in par_encode

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
        let index_encoded_size =
            compute_index_encoded_size(&self.index_codecs, &index_decoded_representation)?;
        let mut encoded_shard_offset = match self.index_location {
            ShardingIndexLocation::Start => index_encoded_size,
            ShardingIndexLocation::End => 0,
        };
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
                shard_index[chunk_index * 2] = encoded_shard_offset;
                shard_index[chunk_index * 2 + 1] = chunk_encoded.len().try_into().unwrap();
                encoded_shard_offset += chunk_encoded.len() as u64;
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
        match self.index_location {
            ShardingIndexLocation::Start => {
                shard.extend(encoded_array_index);
                for chunk in shard_inner_chunks {
                    shard.extend(chunk);
                }
            }
            ShardingIndexLocation::End => {
                for chunk in shard_inner_chunks {
                    shard.extend(chunk);
                }
                shard.extend(encoded_array_index);
            }
        }
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

        // Chunk parallel iterator
        let chunk_iterator = (0..chunks_per_shard.iter().product::<u64>())
            .into_par_iter()
            .map(|chunk_index| self.chunk_index_to_subset_tuple(chunk_index, &chunks_per_shard));

        // Find chunks that are not entirely the fill value and collect their decoded bytes
        let shard_chunks_nofill = chunk_iterator
            .filter_map(|(chunk_index, chunk_subset)| {
                let bytes = unsafe {
                    chunk_subset.extract_bytes_unchecked(
                        &decoded_value,
                        shard_representation.shape(),
                        shard_representation.element_size(),
                    )
                };
                (!chunk_representation.fill_value().equals_all(&bytes))
                    .then(|| Ok((chunk_index, bytes)))
            })
            .collect::<Result<Vec<_>, CodecError>>()?;

        // Sort into chunk order
        // shard_inner_chunks.sort_by(|a, b| Ord::cmp(&a.0, &b.0));

        // Allocate an array for the shard
        let index_decoded_representation = sharding_index_decoded_representation(&chunks_per_shard);
        let index_encoded_size = usize::try_from(compute_index_encoded_size(
            &self.index_codecs,
            &index_decoded_representation,
        )?)
        .unwrap();
        let max_chunk_size = self
            .inner_codecs
            .compute_encoded_size(&chunk_representation)?;
        let Some(max_chunk_size) = max_chunk_size.size() else {
            todo!("Add an alternative path for unbounded encoding");
        };
        let max_chunk_size = usize::try_from(max_chunk_size).unwrap();
        let max_shard_size = max_chunk_size * shard_chunks_nofill.len() + index_encoded_size;
        let mut shard = vec![0; max_shard_size]; // FIXME Maybeuninit

        // Allocate the decoded shard index
        let mut shard_index = vec![u64::MAX; index_decoded_representation.num_elements_usize()];
        let encoded_shard_offset: AtomicUsize = match self.index_location {
            ShardingIndexLocation::Start => index_encoded_size.into(),
            ShardingIndexLocation::End => 0.into(),
        };

        // Encode the chunks
        let shard_slice = UnsafeCellSlice::new(&mut shard);
        let shard_index_slice = UnsafeCellSlice::new(&mut shard_index);
        shard_chunks_nofill
            .into_par_iter()
            .try_for_each(|(chunk_index, bytes)| {
                let chunk_index = usize::try_from(chunk_index).unwrap();
                let chunk_encoded = self.inner_codecs.encode(bytes, &chunk_representation)?;

                let chunk_offset = encoded_shard_offset
                    .fetch_add(chunk_encoded.len(), std::sync::atomic::Ordering::Relaxed);
                if chunk_offset + chunk_encoded.len() > max_shard_size {
                    // This is a dev error, indicates the codec bounded size is not correct
                    return Err(CodecError::from(
                        "Sharding did not allocate a large enough buffer",
                    ));
                }

                unsafe {
                    let shard_index_unsafe = shard_index_slice.get();
                    shard_index_unsafe[chunk_index * 2] = u64::try_from(chunk_offset).unwrap();
                    shard_index_unsafe[chunk_index * 2 + 1] =
                        u64::try_from(chunk_encoded.len()).unwrap();

                    let shard_unsafe = shard_slice.get();
                    shard_unsafe[chunk_offset..chunk_offset + chunk_encoded.len()]
                        .copy_from_slice(&chunk_encoded);
                }

                Ok::<_, CodecError>(())
            })?;
        let shard_length = encoded_shard_offset.load(std::sync::atomic::Ordering::Relaxed)
            + match self.index_location {
                ShardingIndexLocation::Start => 0,
                ShardingIndexLocation::End => index_encoded_size,
            };

        // Encode and write array index
        let shard_index = safe_transmute::transmute_to_bytes(&shard_index);
        let encoded_array_index = self
            .index_codecs
            .par_encode(shard_index.to_vec(), &index_decoded_representation)?;
        shard.truncate(shard_length);
        match self.index_location {
            ShardingIndexLocation::Start => {
                shard[..encoded_array_index.len()].copy_from_slice(&encoded_array_index);
            }
            ShardingIndexLocation::End => {
                shard[shard_length - encoded_array_index.len()..]
                    .copy_from_slice(&encoded_array_index);
            }
        }
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
            self.index_location,
        ))
    }

    fn compute_encoded_size(
        &self,
        decoded_representation: &ArrayRepresentation,
    ) -> Result<BytesRepresentation, CodecError> {
        // Get the maximum size of encoded chunks
        let chunk_representation = unsafe {
            ArrayRepresentation::new_unchecked(
                self.chunk_shape.clone(),
                decoded_representation.data_type().clone(),
                decoded_representation.fill_value().clone(),
            )
        };
        let chunk_bytes_representation = self
            .inner_codecs
            .compute_encoded_size(&chunk_representation)?;

        let chunks_per_shard =
            calculate_chunks_per_shard(decoded_representation.shape(), &self.chunk_shape)
                .map_err(|e| CodecError::Other(e.to_string()))?;

        self.encoded_shard_bounded_size(&chunk_bytes_representation, &chunks_per_shard)
    }
}

impl ShardingCodec {
    fn chunk_index_to_subset_tuple(
        &self,
        chunk_index: u64,
        chunks_per_shard: &[u64],
    ) -> (u64, ArraySubset) {
        let chunk_indices = unravel_index(chunk_index, chunks_per_shard);
        let chunk_start = std::iter::zip(&chunk_indices, &self.chunk_shape)
            .map(|(i, c)| i * c)
            .collect();
        let shape = self.chunk_shape.clone();
        let chunk_subset =
            unsafe { ArraySubset::new_with_start_shape_unchecked(chunk_start, shape) };
        (chunk_index, chunk_subset)
    }

    /// Computed the bounded size of an encoded shard from
    ///  - the chunk bytes representation, and
    ///  - the number of chunks per shard.
    /// Equal to the num chunks * max chunk size + index size
    fn encoded_shard_bounded_size(
        &self,
        chunk_bytes_representation: &BytesRepresentation,
        chunks_per_shard: &[u64],
    ) -> Result<BytesRepresentation, CodecError> {
        match chunk_bytes_representation.size() {
            Some(chunk_encoded_size) => {
                let num_chunks = chunks_per_shard.iter().product::<u64>();
                let index_decoded_representation =
                    sharding_index_decoded_representation(chunks_per_shard);
                let index_encoded_size = usize::try_from(compute_index_encoded_size(
                    &self.index_codecs,
                    &index_decoded_representation,
                )?)
                .unwrap();

                Ok(BytesRepresentation::BoundedSize(
                    num_chunks * chunk_encoded_size + index_encoded_size as u64,
                ))
            }
            None => Ok(BytesRepresentation::UnboundedSize),
        }
    }

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

        let encoded_shard_index = match self.index_location {
            ShardingIndexLocation::Start => {
                &encoded_shard[..index_encoded_size.try_into().unwrap()]
            }
            ShardingIndexLocation::End => {
                let encoded_shard_offset =
                    usize::try_from(encoded_shard.len() as u64 - index_encoded_size).unwrap();
                &encoded_shard[encoded_shard_offset..]
            }
        };

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
        let mut shard = vec![MaybeUninit::<u8>::uninit(); shard_representation.size_usize()];
        let shard_slice =
            unsafe { std::slice::from_raw_parts_mut(shard.as_mut_ptr().cast::<u8>(), shard.len()) };

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
            let decoded_chunk = if offset == u64::MAX && size == u64::MAX {
                chunk_representation
                    .fill_value()
                    .as_ne_bytes()
                    .repeat(chunk_representation.num_elements_usize())
            } else {
                let offset: usize = offset.try_into().unwrap(); // safe
                let size: usize = size.try_into().unwrap(); // safe
                let encoded_chunk_slice = encoded_shard[offset..offset + size].to_vec();
                self.inner_codecs
                    .decode(encoded_chunk_slice, &chunk_representation)?
            };

            // Copy to subset of shard
            let mut data_idx = 0;
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

        let shard = unsafe { core::mem::transmute(shard) };
        Ok(shard)
    }

    fn par_decode_chunks(
        &self,
        encoded_shard: &[u8],
        shard_index: &[u64],
        shard_representation: &ArrayRepresentation,
    ) -> Result<Vec<u8>, CodecError> {
        // Allocate an array for the output
        let mut shard = vec![MaybeUninit::<u8>::uninit(); shard_representation.size_usize()];
        let shard_slice =
            unsafe { std::slice::from_raw_parts_mut(shard.as_mut_ptr().cast::<u8>(), shard.len()) };
        let shard_slice = UnsafeCellSlice::new(shard_slice);

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

        // Decode chunks
        (0..chunks_per_shard.iter().product::<u64>())
            .into_par_iter()
            .map(|chunk_index| self.chunk_index_to_subset_tuple(chunk_index, &chunks_per_shard))
            .try_for_each(|(chunk_index, chunk_subset)| {
                let chunk_index = usize::try_from(chunk_index).unwrap();
                // unsafe {
                //     ArraySubset::new_with_shape(shard_representation.shape().to_vec())
                //         .iter_chunks_unchecked(&self.chunk_shape)
                // }
                // .enumerate()
                // .par_bridge()
                // .map(|(chunk_index, (_chunk_indices, chunk_subset))| {
                let shard_slice = unsafe { shard_slice.get() };

                // Read the offset/size
                let offset = shard_index[chunk_index * 2];
                let size = shard_index[chunk_index * 2 + 1];
                let decoded_chunk = if offset == u64::MAX && size == u64::MAX {
                    // Can fill values be populated faster than repeat?
                    chunk_representation
                        .fill_value()
                        .as_ne_bytes()
                        .repeat(chunk_representation.num_elements_usize())
                } else {
                    let offset: usize = offset.try_into().unwrap(); // safe
                    let size: usize = size.try_into().unwrap(); // safe
                    let encoded_chunk_slice = encoded_shard[offset..offset + size].to_vec();
                    // NOTE: Intentionally using single threaded decode, since parallelisation is in the loop
                    self.inner_codecs
                        .decode(encoded_chunk_slice, &chunk_representation)?
                };

                // Copy to subset of shard
                let mut data_idx = 0;
                let element_size = chunk_representation.element_size() as u64;
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

                Ok::<_, CodecError>(())
            })?;

        let shard = unsafe { core::mem::transmute(shard) };
        Ok(shard)
    }
}
