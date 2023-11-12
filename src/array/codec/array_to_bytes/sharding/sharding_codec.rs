use std::{mem::MaybeUninit, sync::atomic::AtomicUsize};

use crate::{
    array::{
        codec::{
            ArrayCodecTraits, ArrayPartialDecoderTraits, ArrayToBytesCodecTraits,
            BytesPartialDecoderTraits, Codec, CodecChain, CodecError, CodecPlugin, CodecTraits,
        },
        safe_transmute_to_bytes_vec, unravel_index, ArrayRepresentation, BytesRepresentation,
        UnsafeCellSlice,
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
    fn encode_opt(
        &self,
        decoded_value: Vec<u8>,
        shard_rep: &ArrayRepresentation,
        parallel: bool,
    ) -> Result<Vec<u8>, CodecError> {
        if decoded_value.len() as u64 != shard_rep.size() {
            return Err(CodecError::UnexpectedChunkDecodedSize(
                decoded_value.len(),
                shard_rep.size(),
            ));
        }

        // Get chunk bytes representation, and choose implementation based on whether the size is unbounded or not
        let chunk_rep = unsafe {
            ArrayRepresentation::new_unchecked(
                self.chunk_shape.clone(),
                shard_rep.data_type().clone(),
                shard_rep.fill_value().clone(),
            )
        };
        let chunk_bytes_representation = self.inner_codecs.compute_encoded_size(&chunk_rep)?;
        match chunk_bytes_representation {
            BytesRepresentation::BoundedSize(size) | BytesRepresentation::FixedSize(size) => {
                if parallel {
                    self.par_encode_bounded(&decoded_value, shard_rep, &chunk_rep, size)
                } else {
                    self.encode_bounded(&decoded_value, shard_rep, &chunk_rep, size)
                }
            }
            BytesRepresentation::UnboundedSize => {
                if parallel {
                    self.par_encode_unbounded(&decoded_value, shard_rep, &chunk_rep)
                } else {
                    self.encode_unbounded(&decoded_value, shard_rep, &chunk_rep)
                }
            }
        }
    }

    fn decode_opt(
        &self,
        encoded_value: Vec<u8>,
        decoded_representation: &ArrayRepresentation,
        parallel: bool,
    ) -> Result<Vec<u8>, CodecError> {
        let chunks_per_shard =
            calculate_chunks_per_shard(decoded_representation.shape(), &self.chunk_shape)
                .map_err(|e| CodecError::Other(e.to_string()))?;
        let shard_index = self.decode_index(&encoded_value, &chunks_per_shard, false)?; // FIXME: par decode index?
        let chunks = if parallel {
            self.par_decode_chunks(&encoded_value, &shard_index, decoded_representation)?
        } else {
            self.decode_chunks(&encoded_value, &shard_index, decoded_representation)?
        };
        Ok(chunks)
    }
}

impl ArrayToBytesCodecTraits for ShardingCodec {
    fn partial_decoder_opt<'a>(
        &'a self,
        input_handle: Box<dyn BytesPartialDecoderTraits + 'a>,
        decoded_representation: &ArrayRepresentation,
        parallel: bool,
    ) -> Result<Box<dyn ArrayPartialDecoderTraits + 'a>, CodecError> {
        Ok(Box::new(
            sharding_partial_decoder::ShardingPartialDecoder::new(
                input_handle,
                decoded_representation.clone(),
                self.chunk_shape.clone(),
                &self.inner_codecs,
                &self.index_codecs,
                self.index_location,
                parallel,
            )?,
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

        match chunk_bytes_representation {
            BytesRepresentation::BoundedSize(size) | BytesRepresentation::FixedSize(size) => {
                let chunks_per_shard =
                    calculate_chunks_per_shard(decoded_representation.shape(), &self.chunk_shape)
                        .map_err(|e| CodecError::Other(e.to_string()))?;
                let index_decoded_representation =
                    sharding_index_decoded_representation(&chunks_per_shard);
                let index_encoded_size =
                    compute_index_encoded_size(&self.index_codecs, &index_decoded_representation)?;
                let shard_size =
                    Self::encoded_shard_bounded_size(index_encoded_size, size, &chunks_per_shard);
                Ok(BytesRepresentation::BoundedSize(shard_size))
            }
            BytesRepresentation::UnboundedSize => Ok(BytesRepresentation::UnboundedSize),
        }
    }
}

impl ShardingCodec {
    fn chunk_index_to_subset(&self, chunk_index: u64, chunks_per_shard: &[u64]) -> ArraySubset {
        let chunk_indices = unravel_index(chunk_index, chunks_per_shard);
        let chunk_start = std::iter::zip(&chunk_indices, &self.chunk_shape)
            .map(|(i, c)| i * c)
            .collect();
        let shape = self.chunk_shape.clone();
        unsafe { ArraySubset::new_with_start_shape_unchecked(chunk_start, shape) }
    }

    /// Computed the bounded size of an encoded shard from
    ///  - the chunk bytes representation, and
    ///  - the number of chunks per shard.
    /// Equal to the num chunks * max chunk size + index size
    fn encoded_shard_bounded_size(
        index_encoded_size: u64,
        chunk_encoded_size: u64,
        chunks_per_shard: &[u64],
    ) -> u64 {
        let num_chunks = chunks_per_shard.iter().product::<u64>();
        num_chunks * chunk_encoded_size + index_encoded_size
    }

    /// Preallocate shard, encode and write chunks, then truncate shard
    fn encode_bounded(
        &self,
        decoded_value: &[u8],
        shard_representation: &ArrayRepresentation,
        chunk_representation: &ArrayRepresentation,
        chunk_size_bounded: u64,
    ) -> Result<Vec<u8>, CodecError> {
        debug_assert_eq!(decoded_value.len() as u64, shard_representation.size());

        // Allocate an array for the shard
        let chunks_per_shard =
            calculate_chunks_per_shard(shard_representation.shape(), chunk_representation.shape())
                .map_err(|e| CodecError::Other(e.to_string()))?;
        let index_decoded_representation = sharding_index_decoded_representation(&chunks_per_shard);
        let index_encoded_size =
            compute_index_encoded_size(&self.index_codecs, &index_decoded_representation)?;
        let shard_size_bounded = Self::encoded_shard_bounded_size(
            index_encoded_size,
            chunk_size_bounded,
            &chunks_per_shard,
        );
        let shard_size_bounded = usize::try_from(shard_size_bounded).unwrap();
        let mut shard = vec![core::mem::MaybeUninit::<u8>::uninit(); shard_size_bounded];

        // Create array index
        let chunks_per_shard =
            calculate_chunks_per_shard(shard_representation.shape(), chunk_representation.shape())
                .map_err(|e| CodecError::Other(e.to_string()))?;
        let index_decoded_representation = sharding_index_decoded_representation(&chunks_per_shard);
        let mut shard_index = vec![u64::MAX; index_decoded_representation.num_elements_usize()];

        // Iterate over chunk indices
        let index_encoded_size = usize::try_from(index_encoded_size).unwrap();
        let mut encoded_shard_offset: usize = match self.index_location {
            ShardingIndexLocation::Start => index_encoded_size,
            ShardingIndexLocation::End => 0,
        };
        {
            let shard_slice = unsafe {
                std::slice::from_raw_parts_mut(shard.as_mut_ptr().cast::<u8>(), shard.len())
            };
            for (chunk_index, (_chunk_indices, chunk_subset)) in unsafe {
                ArraySubset::new_with_shape(shard_representation.shape().to_vec())
                    .iter_chunks_unchecked(&self.chunk_shape)
            }
            .enumerate()
            {
                let bytes = unsafe {
                    chunk_subset.extract_bytes_unchecked(
                        decoded_value,
                        shard_representation.shape(),
                        shard_representation.element_size(),
                    )
                };
                if !chunk_representation.fill_value().equals_all(&bytes) {
                    let chunk_encoded = self.inner_codecs.encode(bytes, chunk_representation)?;
                    shard_index[chunk_index * 2] = u64::try_from(encoded_shard_offset).unwrap();
                    shard_index[chunk_index * 2 + 1] = u64::try_from(chunk_encoded.len()).unwrap();
                    shard_slice[encoded_shard_offset..encoded_shard_offset + chunk_encoded.len()]
                        .copy_from_slice(&chunk_encoded);
                    encoded_shard_offset += chunk_encoded.len();
                }
            }
        }

        // Truncate the shard
        let shard_length = encoded_shard_offset
            + match self.index_location {
                ShardingIndexLocation::Start => 0,
                ShardingIndexLocation::End => index_encoded_size,
            };
        shard.truncate(shard_length);

        // Encode array index
        let encoded_array_index = self.index_codecs.encode(
            safe_transmute_to_bytes_vec(shard_index),
            &index_decoded_representation,
        )?;

        // Add the index
        {
            let shard_slice = unsafe {
                std::slice::from_raw_parts_mut(shard.as_mut_ptr().cast::<u8>(), shard.len())
            };
            match self.index_location {
                ShardingIndexLocation::Start => {
                    shard_slice[..index_encoded_size].copy_from_slice(&encoded_array_index);
                }
                ShardingIndexLocation::End => {
                    shard_slice[shard_length - index_encoded_size..]
                        .copy_from_slice(&encoded_array_index);
                }
            }
        }
        #[allow(clippy::transmute_undefined_repr)]
        let shard = unsafe { core::mem::transmute(shard) };
        Ok(shard)
    }

    /// Encode inner chunks, then allocate shard, then write to shard
    fn encode_unbounded(
        &self,
        decoded_value: &[u8],
        shard_representation: &ArrayRepresentation,
        chunk_representation: &ArrayRepresentation,
    ) -> Result<Vec<u8>, CodecError> {
        debug_assert_eq!(decoded_value.len() as u64, shard_representation.size());

        // Create array index
        let chunks_per_shard =
            calculate_chunks_per_shard(shard_representation.shape(), chunk_representation.shape())
                .map_err(|e| CodecError::Other(e.to_string()))?;
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
                    decoded_value,
                    shard_representation.shape(),
                    shard_representation.element_size(),
                )
            };
            if !chunk_representation.fill_value().equals_all(&bytes) {
                let chunk_encoded = self.inner_codecs.encode(bytes, chunk_representation)?;
                shard_index[chunk_index * 2] = encoded_shard_offset;
                shard_index[chunk_index * 2 + 1] = chunk_encoded.len().try_into().unwrap();
                encoded_shard_offset += chunk_encoded.len() as u64;
                shard_inner_chunks.push(chunk_encoded);
            }
        }

        // Encode array index
        let encoded_array_index = self.index_codecs.encode(
            safe_transmute_to_bytes_vec(shard_index),
            &index_decoded_representation,
        )?;

        // Encode the shard
        let shard_inner_chunks_length = shard_inner_chunks.iter().map(Vec::len).sum::<usize>();
        let shard_size = shard_inner_chunks_length + encoded_array_index.len();
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

    /// Preallocate shard, encode and write chunks (in parallel), then truncate shard
    fn par_encode_bounded(
        &self,
        decoded_value: &[u8],
        shard_representation: &ArrayRepresentation,
        chunk_representation: &ArrayRepresentation,
        chunk_size_bounded: u64,
    ) -> Result<Vec<u8>, CodecError> {
        debug_assert_eq!(decoded_value.len() as u64, shard_representation.size()); // already validated in par_encode

        // Calculate maximum possible shard size
        let chunks_per_shard =
            calculate_chunks_per_shard(shard_representation.shape(), chunk_representation.shape())
                .map_err(|e| CodecError::Other(e.to_string()))?;
        let index_decoded_representation = sharding_index_decoded_representation(&chunks_per_shard);
        let index_encoded_size =
            compute_index_encoded_size(&self.index_codecs, &index_decoded_representation)?;
        let shard_size_bounded = Self::encoded_shard_bounded_size(
            index_encoded_size,
            chunk_size_bounded,
            &chunks_per_shard,
        );

        let shard_size_bounded = usize::try_from(shard_size_bounded).unwrap();
        let index_encoded_size = usize::try_from(index_encoded_size).unwrap();

        // Allocate an array for the shard
        let mut shard = vec![core::mem::MaybeUninit::<u8>::uninit(); shard_size_bounded];

        // Allocate the decoded shard index
        let mut shard_index = vec![u64::MAX; index_decoded_representation.num_elements_usize()];
        let encoded_shard_offset: AtomicUsize = match self.index_location {
            ShardingIndexLocation::Start => index_encoded_size.into(),
            ShardingIndexLocation::End => 0.into(),
        };

        // Encode the shards and update the shard index
        {
            let shard_slice = unsafe {
                std::slice::from_raw_parts_mut(shard.as_mut_ptr().cast::<u8>(), shard.len())
            };
            let shard_slice = UnsafeCellSlice::new(shard_slice);
            let shard_index_slice = UnsafeCellSlice::new(&mut shard_index);
            (0..chunks_per_shard.iter().product::<u64>())
                .into_par_iter()
                .try_for_each(|chunk_index| {
                    let chunk_subset = self.chunk_index_to_subset(chunk_index, &chunks_per_shard);
                    let chunk_index = usize::try_from(chunk_index).unwrap();
                    let bytes = unsafe {
                        chunk_subset.extract_bytes_unchecked(
                            decoded_value,
                            shard_representation.shape(),
                            shard_representation.element_size(),
                        )
                    };
                    if !chunk_representation.fill_value().equals_all(&bytes) {
                        let chunk_encoded =
                            self.inner_codecs.encode(bytes, chunk_representation)?;

                        let chunk_offset = encoded_shard_offset
                            .fetch_add(chunk_encoded.len(), std::sync::atomic::Ordering::Relaxed);
                        if chunk_offset + chunk_encoded.len() > shard_size_bounded {
                            // This is a dev error, indicates the codec bounded size is not correct
                            return Err(CodecError::from(
                                "Sharding did not allocate a large enough buffer",
                            ));
                        }

                        unsafe {
                            let shard_index_unsafe = shard_index_slice.get();
                            shard_index_unsafe[chunk_index * 2] =
                                u64::try_from(chunk_offset).unwrap();
                            shard_index_unsafe[chunk_index * 2 + 1] =
                                u64::try_from(chunk_encoded.len()).unwrap();

                            let shard_unsafe = shard_slice.get();
                            shard_unsafe[chunk_offset..chunk_offset + chunk_encoded.len()]
                                .copy_from_slice(&chunk_encoded);
                        }
                    }
                    Ok(())
                })?;
        }

        // Truncate shard
        let shard_length = encoded_shard_offset.load(std::sync::atomic::Ordering::Relaxed)
            + match self.index_location {
                ShardingIndexLocation::Start => 0,
                ShardingIndexLocation::End => index_encoded_size,
            };
        shard.truncate(shard_length);

        // Encode and write array index
        let encoded_array_index = self.index_codecs.par_encode(
            safe_transmute_to_bytes_vec(shard_index),
            &index_decoded_representation,
        )?;
        {
            let shard_slice = unsafe {
                std::slice::from_raw_parts_mut(shard.as_mut_ptr().cast::<u8>(), shard.len())
            };
            match self.index_location {
                ShardingIndexLocation::Start => {
                    shard_slice[..encoded_array_index.len()].copy_from_slice(&encoded_array_index);
                }
                ShardingIndexLocation::End => {
                    shard_slice[shard_length - encoded_array_index.len()..]
                        .copy_from_slice(&encoded_array_index);
                }
            }
        }
        #[allow(clippy::transmute_undefined_repr)]
        let shard = unsafe { core::mem::transmute(shard) };
        Ok(shard)
    }

    /// Encode inner chunks (in parallel), then allocate shard, then write to shard (in parallel)
    fn par_encode_unbounded(
        &self,
        decoded_value: &[u8],
        shard_representation: &ArrayRepresentation,
        chunk_representation: &ArrayRepresentation,
    ) -> Result<Vec<u8>, CodecError> {
        debug_assert_eq!(decoded_value.len() as u64, shard_representation.size()); // already validated in par_encode

        let chunks_per_shard =
            calculate_chunks_per_shard(shard_representation.shape(), chunk_representation.shape())
                .map_err(|e| CodecError::Other(e.to_string()))?;
        let index_decoded_representation = sharding_index_decoded_representation(&chunks_per_shard);
        let index_encoded_size =
            compute_index_encoded_size(&self.index_codecs, &index_decoded_representation)?;
        let index_encoded_size = usize::try_from(index_encoded_size).unwrap();

        // Find chunks that are not entirely the fill value and collect their decoded bytes
        let encoded_chunks: Vec<(u64, Vec<u8>)> = (0..chunks_per_shard.iter().product::<u64>())
            .into_par_iter()
            .filter_map(|chunk_index| {
                let chunk_subset = self.chunk_index_to_subset(chunk_index, &chunks_per_shard);
                let bytes = unsafe {
                    chunk_subset.extract_bytes_unchecked(
                        decoded_value,
                        shard_representation.shape(),
                        shard_representation.element_size(),
                    )
                };
                if chunk_representation.fill_value().equals_all(&bytes) {
                    None
                } else {
                    let encoded_chunk = self.inner_codecs.encode(bytes, chunk_representation);
                    match encoded_chunk {
                        Ok(encoded_chunk) => Some(Ok((chunk_index, encoded_chunk))),
                        Err(err) => Some(Err(err)),
                    }
                }
            })
            .collect::<Result<Vec<_>, _>>()?;

        // Allocate the shard
        let encoded_chunk_length = encoded_chunks
            .iter()
            .map(|(_, bytes)| bytes.len())
            .sum::<usize>();
        let shard_length = encoded_chunk_length + index_encoded_size;
        let mut shard = vec![core::mem::MaybeUninit::<u8>::uninit(); shard_length];

        // Allocate the decoded shard index
        let mut shard_index = vec![u64::MAX; index_decoded_representation.num_elements_usize()];
        let encoded_shard_offset: AtomicUsize = match self.index_location {
            ShardingIndexLocation::Start => index_encoded_size.into(),
            ShardingIndexLocation::End => 0.into(),
        };

        // Write shard and update shard index
        {
            let shard_slice = unsafe {
                std::slice::from_raw_parts_mut(shard.as_mut_ptr().cast::<u8>(), shard.len())
            };
            let shard_slice = UnsafeCellSlice::new(shard_slice);
            let shard_index_slice = UnsafeCellSlice::new(&mut shard_index);
            encoded_chunks
                .into_par_iter()
                .for_each(|(chunk_index, chunk_encoded)| {
                    let chunk_index = usize::try_from(chunk_index).unwrap();
                    let chunk_offset = encoded_shard_offset
                        .fetch_add(chunk_encoded.len(), std::sync::atomic::Ordering::Relaxed);
                    unsafe {
                        let shard_index_unsafe = shard_index_slice.get();
                        shard_index_unsafe[chunk_index * 2] = u64::try_from(chunk_offset).unwrap();
                        shard_index_unsafe[chunk_index * 2 + 1] =
                            u64::try_from(chunk_encoded.len()).unwrap();

                        let shard_unsafe = shard_slice.get();
                        shard_unsafe[chunk_offset..chunk_offset + chunk_encoded.len()]
                            .copy_from_slice(&chunk_encoded);
                    }
                });
        }

        // Write shard index
        let encoded_array_index = self.index_codecs.par_encode(
            safe_transmute_to_bytes_vec(shard_index),
            &index_decoded_representation,
        )?;
        {
            let shard_slice = unsafe {
                std::slice::from_raw_parts_mut(shard.as_mut_ptr().cast::<u8>(), shard.len())
            };
            match self.index_location {
                ShardingIndexLocation::Start => {
                    shard_slice[..encoded_array_index.len()].copy_from_slice(&encoded_array_index);
                }
                ShardingIndexLocation::End => {
                    shard_slice[shard_length - encoded_array_index.len()..]
                        .copy_from_slice(&encoded_array_index);
                }
            }
        }
        #[allow(clippy::transmute_undefined_repr)]
        let shard = unsafe { core::mem::transmute(shard) };
        Ok(shard)
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

        #[allow(clippy::transmute_undefined_repr)]
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
            .try_for_each(|chunk_index| {
                let chunk_subset = self.chunk_index_to_subset(chunk_index, &chunks_per_shard);
                let chunk_index = usize::try_from(chunk_index).unwrap();
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

        #[allow(clippy::transmute_undefined_repr)]
        let shard = unsafe { core::mem::transmute(shard) };
        Ok(shard)
    }
}
