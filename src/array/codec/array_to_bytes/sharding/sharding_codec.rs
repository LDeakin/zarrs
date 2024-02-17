use std::{num::NonZeroU64, sync::atomic::AtomicUsize};

use crate::{
    array::{
        chunk_shape_to_array_shape,
        codec::{
            ArrayCodecTraits, ArrayPartialDecoderTraits, ArrayToBytesCodecTraits,
            BytesPartialDecoderTraits, CodecChain, CodecError, CodecTraits, DecodeOptions,
            EncodeOptions, PartialDecoderOptions, RecommendedConcurrency,
        },
        concurrency::calc_concurrent_limits,
        transmute_to_bytes_vec, unravel_index,
        unsafe_cell_slice::UnsafeCellSlice,
        ArrayView, BytesRepresentation, ChunkRepresentation, ChunkShape,
    },
    array_subset::ArraySubset,
    metadata::Metadata,
    plugin::PluginCreateError,
};

#[cfg(feature = "async")]
use crate::array::codec::{AsyncArrayPartialDecoderTraits, AsyncBytesPartialDecoderTraits};

use super::{
    calculate_chunks_per_shard, compute_index_encoded_size, decode_shard_index,
    sharding_configuration::ShardingIndexLocation, sharding_index_decoded_representation,
    sharding_partial_decoder, ShardingCodecConfiguration, ShardingCodecConfigurationV1, IDENTIFIER,
};

#[cfg(feature = "async")]
use super::async_decode_shard_index;

use rayon::prelude::*;

/// A `sharding` codec implementation.
#[derive(Clone, Debug)]
pub struct ShardingCodec {
    /// An array of integers specifying the shape of the inner chunks in a shard along each dimension of the outer array.
    chunk_shape: ChunkShape,
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
        chunk_shape: ChunkShape,
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

#[cfg_attr(feature = "async", async_trait::async_trait)]
impl ArrayCodecTraits for ShardingCodec {
    fn recommended_concurrency(
        &self,
        decoded_representation: &ChunkRepresentation,
    ) -> Result<RecommendedConcurrency, CodecError> {
        let chunks_per_shard =
            calculate_chunks_per_shard(decoded_representation.shape(), self.chunk_shape.as_slice())
                .map_err(|e| CodecError::Other(e.to_string()))?;
        let num_elements = chunks_per_shard.num_elements_nonzero_usize();
        Ok(RecommendedConcurrency::new_maximum(num_elements.into()))
    }

    fn encode_opt(
        &self,
        decoded_value: Vec<u8>,
        shard_rep: &ChunkRepresentation,
        options: &EncodeOptions,
    ) -> Result<Vec<u8>, CodecError> {
        if decoded_value.len() as u64 != shard_rep.size() {
            return Err(CodecError::UnexpectedChunkDecodedSize(
                decoded_value.len(),
                shard_rep.size(),
            ));
        }

        // Get chunk bytes representation, and choose implementation based on whether the size is unbounded or not
        let chunk_rep = unsafe {
            ChunkRepresentation::new_unchecked(
                self.chunk_shape.as_slice().to_vec(),
                shard_rep.data_type().clone(),
                shard_rep.fill_value().clone(),
            )
        };
        let chunk_bytes_representation = self.inner_codecs.compute_encoded_size(&chunk_rep)?;
        match chunk_bytes_representation {
            BytesRepresentation::BoundedSize(size) | BytesRepresentation::FixedSize(size) => {
                self.encode_bounded(&decoded_value, shard_rep, &chunk_rep, size, options)
            }
            BytesRepresentation::UnboundedSize => {
                self.encode_unbounded(&decoded_value, shard_rep, &chunk_rep, options)
            }
        }
    }

    fn decode_opt(
        &self,
        encoded_value: Vec<u8>,
        decoded_representation: &ChunkRepresentation,
        options: &DecodeOptions,
    ) -> Result<Vec<u8>, CodecError> {
        // Allocate an array for the output
        let len = decoded_representation.size_usize();
        let mut decoded_shard = Vec::<u8>::with_capacity(len);

        // Decode the shard into the output
        let decoded_shard_slice =
            unsafe { crate::vec_spare_capacity_to_mut_slice(&mut decoded_shard) };
        self.decode_into_array_view_opt(
            &encoded_value,
            decoded_representation,
            ArrayView::new(
                decoded_shard_slice,
                &decoded_representation.shape_u64(),
                &ArraySubset::new_with_shape(decoded_representation.shape_u64()),
            ),
            options,
        )?;
        unsafe {
            decoded_shard.set_len(len);
        }

        Ok(decoded_shard)
    }

    #[cfg(feature = "async")]
    async fn async_encode_opt(
        &self,
        decoded_value: Vec<u8>,
        shard_rep: &ChunkRepresentation,
        options: &EncodeOptions,
    ) -> Result<Vec<u8>, CodecError> {
        if decoded_value.len() as u64 != shard_rep.size() {
            return Err(CodecError::UnexpectedChunkDecodedSize(
                decoded_value.len(),
                shard_rep.size(),
            ));
        }

        // Get chunk bytes representation, and choose implementation based on whether the size is unbounded or not
        let chunk_rep = unsafe {
            ChunkRepresentation::new_unchecked(
                self.chunk_shape.as_slice().to_vec(),
                shard_rep.data_type().clone(),
                shard_rep.fill_value().clone(),
            )
        };
        let chunk_bytes_representation = self.inner_codecs.compute_encoded_size(&chunk_rep)?;
        match chunk_bytes_representation {
            BytesRepresentation::BoundedSize(size) | BytesRepresentation::FixedSize(size) => {
                self.async_encode_bounded(&decoded_value, shard_rep, &chunk_rep, size, options)
                    .await
            }
            BytesRepresentation::UnboundedSize => {
                self.async_encode_unbounded(&decoded_value, shard_rep, &chunk_rep, options)
                    .await
            }
        }
    }

    #[cfg(feature = "async")]
    async fn async_decode_opt(
        &self,
        encoded_value: Vec<u8>,
        decoded_representation: &ChunkRepresentation,
        options: &DecodeOptions,
    ) -> Result<Vec<u8>, CodecError> {
        let chunks_per_shard =
            calculate_chunks_per_shard(decoded_representation.shape(), self.chunk_shape.as_slice())
                .map_err(|e| CodecError::Other(e.to_string()))?;
        let shard_index = self
            .async_decode_index(&encoded_value, chunks_per_shard.as_slice(), options)
            .await?;
        let chunks = self
            .async_decode_chunks(
                &encoded_value,
                &shard_index,
                decoded_representation,
                options,
            )
            .await?;
        Ok(chunks)
    }

    fn decode_into_array_view_opt(
        &self,
        encoded_shard: &[u8],
        shard_representation: &ChunkRepresentation,
        array_view: ArrayView,
        options: &DecodeOptions,
    ) -> Result<(), CodecError> {
        let chunks_per_shard =
            calculate_chunks_per_shard(shard_representation.shape(), self.chunk_shape.as_slice())
                .map_err(|e| CodecError::Other(e.to_string()))?;
        let shard_index = self.decode_index(encoded_shard, chunks_per_shard.as_slice(), options)?;

        // Decode chunks
        let chunk_representation = unsafe {
            ChunkRepresentation::new_unchecked(
                self.chunk_shape.as_slice().to_vec(),
                shard_representation.data_type().clone(),
                shard_representation.fill_value().clone(),
            )
        };

        let any_empty = shard_index
            .par_iter()
            .any(|offset_or_size| *offset_or_size == u64::MAX);
        let fill_value_chunk = if any_empty {
            Some(
                chunk_representation
                    .fill_value()
                    .as_ne_bytes()
                    .repeat(chunk_representation.num_elements_usize()),
            )
        } else {
            None
        };

        // Calc self/internal concurrent limits
        let (shard_concurrent_limit, concurrency_limit_inner_chunks) = calc_concurrent_limits(
            options.concurrent_limit(),
            &self.recommended_concurrency(shard_representation)?,
            &self
                .inner_codecs
                .recommended_concurrency(&chunk_representation)?,
        );
        let mut options_inner = DecodeOptions::default();
        options_inner.set_concurrent_limit(concurrency_limit_inner_chunks);
        // println!("{shard_concurrent_limit} {concurrency_limit_inner_chunks:?}"); // FIXME: log debug?

        let chunks_per_shard =
            calculate_chunks_per_shard(shard_representation.shape(), chunk_representation.shape())
                .map_err(|e| CodecError::Other(e.to_string()))?;
        let num_chunks = chunks_per_shard
            .as_slice()
            .iter()
            .map(|i| usize::try_from(i.get()).unwrap())
            .product::<usize>();
        let element_size = chunk_representation.element_size() as u64;

        rayon_iter_concurrent_limit::iter_concurrent_limit!(
            shard_concurrent_limit,
            (0..num_chunks).into_par_iter(),
            try_for_each,
            |chunk_index| {
                let chunk_subset = self.chunk_index_to_subset(
                    chunk_index as u64,
                    chunks_per_shard.as_slice(),
                    Some(array_view.subset.start()),
                );
                let array_slice = unsafe { array_view.bytes_mut() };

                let mut copy_to_subset = |decoded_chunk: &[u8]| {
                    // Copy to subset of shard
                    let mut data_idx = 0;
                    let contiguous_iterator = unsafe {
                        chunk_subset.contiguous_linearised_indices_unchecked(array_view.shape)
                    };
                    let length =
                        usize::try_from(contiguous_iterator.contiguous_elements() * element_size)
                            .unwrap();
                    for (index, _) in &contiguous_iterator {
                        let shard_offset = usize::try_from(index * element_size).unwrap();
                        array_slice[shard_offset..shard_offset + length]
                            .copy_from_slice(&decoded_chunk[data_idx..data_idx + length]);
                        data_idx += length;
                    }
                };

                // Read the offset/size
                let offset = shard_index[chunk_index * 2];
                let size = shard_index[chunk_index * 2 + 1];
                if offset == u64::MAX && size == u64::MAX {
                    if let Some(fill_value_chunk) = &fill_value_chunk {
                        copy_to_subset(fill_value_chunk.as_slice());
                    } else {
                        unreachable!();
                    }
                } else {
                    let offset: usize = offset.try_into().unwrap(); // safe
                    let size: usize = size.try_into().unwrap(); // safe
                    let encoded_chunk_slice = &encoded_shard[offset..offset + size];
                    let array_view_chunk = array_view.subset_view(&chunk_subset);
                    self.inner_codecs.decode_into_array_view_opt(
                        encoded_chunk_slice,
                        &chunk_representation,
                        array_view_chunk,
                        &options_inner,
                    )?;
                };

                Ok::<_, CodecError>(())
            }
        )?;
        Ok(())
    }
}

#[cfg_attr(feature = "async", async_trait::async_trait)]
impl ArrayToBytesCodecTraits for ShardingCodec {
    fn partial_decoder_opt<'a>(
        &'a self,
        input_handle: Box<dyn BytesPartialDecoderTraits + 'a>,
        decoded_representation: &ChunkRepresentation,
        options: &PartialDecoderOptions,
    ) -> Result<Box<dyn ArrayPartialDecoderTraits + 'a>, CodecError> {
        Ok(Box::new(
            sharding_partial_decoder::ShardingPartialDecoder::new(
                input_handle,
                decoded_representation.clone(),
                self.chunk_shape.clone(),
                &self.inner_codecs,
                &self.index_codecs,
                self.index_location,
                options,
            )?,
        ))
    }

    #[cfg(feature = "async")]
    async fn async_partial_decoder_opt<'a>(
        &'a self,
        input_handle: Box<dyn AsyncBytesPartialDecoderTraits + 'a>,
        decoded_representation: &ChunkRepresentation,
        options: &PartialDecoderOptions,
    ) -> Result<Box<dyn AsyncArrayPartialDecoderTraits + 'a>, CodecError> {
        Ok(Box::new(
            sharding_partial_decoder::AsyncShardingPartialDecoder::new(
                input_handle,
                decoded_representation.clone(),
                self.chunk_shape.clone(),
                &self.inner_codecs,
                &self.index_codecs,
                self.index_location,
                options,
            )
            .await?,
        ))
    }

    fn compute_encoded_size(
        &self,
        decoded_representation: &ChunkRepresentation,
    ) -> Result<BytesRepresentation, CodecError> {
        // Get the maximum size of encoded chunks
        let chunk_representation = unsafe {
            ChunkRepresentation::new_unchecked(
                self.chunk_shape.as_slice().to_vec(),
                decoded_representation.data_type().clone(),
                decoded_representation.fill_value().clone(),
            )
        };
        let chunk_bytes_representation = self
            .inner_codecs
            .compute_encoded_size(&chunk_representation)?;

        match chunk_bytes_representation {
            BytesRepresentation::BoundedSize(size) | BytesRepresentation::FixedSize(size) => {
                let chunks_per_shard = calculate_chunks_per_shard(
                    decoded_representation.shape(),
                    self.chunk_shape.as_slice(),
                )
                .map_err(|e| CodecError::Other(e.to_string()))?;
                let index_decoded_representation =
                    sharding_index_decoded_representation(chunks_per_shard.as_slice());
                let index_encoded_size =
                    compute_index_encoded_size(&self.index_codecs, &index_decoded_representation)?;
                let shard_size = Self::encoded_shard_bounded_size(
                    index_encoded_size,
                    size,
                    chunks_per_shard.as_slice(),
                );
                Ok(BytesRepresentation::BoundedSize(shard_size))
            }
            BytesRepresentation::UnboundedSize => Ok(BytesRepresentation::UnboundedSize),
        }
    }
}

impl ShardingCodec {
    fn chunk_index_to_subset(
        &self,
        chunk_index: u64,
        chunks_per_shard: &[NonZeroU64],
        shard_offset: Option<&[u64]>,
    ) -> ArraySubset {
        let chunks_per_shard = chunk_shape_to_array_shape(chunks_per_shard);
        let chunk_indices = unravel_index(chunk_index, chunks_per_shard.as_slice());
        let chunk_start = if let Some(shard_offset) = shard_offset {
            itertools::izip!(&chunk_indices, self.chunk_shape.as_slice(), shard_offset)
                .map(|(i, c, o)| i * c.get() + o)
                .collect()
        } else {
            std::iter::zip(&chunk_indices, self.chunk_shape.as_slice())
                .map(|(i, c)| i * c.get())
                .collect()
        };
        let shape = chunk_shape_to_array_shape(self.chunk_shape.as_slice());
        unsafe { ArraySubset::new_with_start_shape_unchecked(chunk_start, shape) }
    }

    /// Computed the bounded size of an encoded shard from
    ///  - the chunk bytes representation, and
    ///  - the number of chunks per shard.
    /// Equal to `num chunks * max chunk size + index size`
    fn encoded_shard_bounded_size(
        index_encoded_size: u64,
        chunk_encoded_size: u64,
        chunks_per_shard: &[NonZeroU64],
    ) -> u64 {
        let num_chunks = chunks_per_shard.iter().map(|i| i.get()).product::<u64>();
        num_chunks * chunk_encoded_size + index_encoded_size
    }

    /// Preallocate shard, encode and write chunks (in parallel), then truncate shard
    #[allow(clippy::too_many_lines)]
    fn encode_bounded(
        &self,
        decoded_value: &[u8],
        shard_representation: &ChunkRepresentation,
        chunk_representation: &ChunkRepresentation,
        chunk_size_bounded: u64,
        options: &EncodeOptions,
    ) -> Result<Vec<u8>, CodecError> {
        debug_assert_eq!(decoded_value.len() as u64, shard_representation.size()); // already validated in par_encode

        // Calculate maximum possible shard size
        let chunks_per_shard =
            calculate_chunks_per_shard(shard_representation.shape(), chunk_representation.shape())
                .map_err(|e| CodecError::Other(e.to_string()))?;
        let index_decoded_representation =
            sharding_index_decoded_representation(chunks_per_shard.as_slice());
        let index_encoded_size =
            compute_index_encoded_size(&self.index_codecs, &index_decoded_representation)?;
        let shard_size_bounded = Self::encoded_shard_bounded_size(
            index_encoded_size,
            chunk_size_bounded,
            chunks_per_shard.as_slice(),
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

        // Calc self/internal concurrent limits
        let (shard_concurrent_limit, concurrency_limit_inner_chunks) = calc_concurrent_limits(
            options.concurrent_limit(),
            &self.recommended_concurrency(shard_representation)?,
            &self
                .inner_codecs
                .recommended_concurrency(chunk_representation)?,
        );
        let mut options_inner = EncodeOptions::default();
        options_inner.set_concurrent_limit(concurrency_limit_inner_chunks);
        // println!("{shard_concurrent_limit} {concurrency_limit_inner_chunks:?}"); // FIXME: log debug?

        // Encode the shards and update the shard index
        {
            let shard_slice = unsafe {
                std::slice::from_raw_parts_mut(shard.as_mut_ptr().cast::<u8>(), shard.len())
            };
            let shard_slice = UnsafeCellSlice::new(shard_slice);
            let shard_index_slice = UnsafeCellSlice::new(&mut shard_index);
            let shard_shape = shard_representation.shape_u64();
            let n_chunks = chunks_per_shard
                .as_slice()
                .iter()
                .map(|i| usize::try_from(i.get()).unwrap())
                .product::<usize>();
            rayon_iter_concurrent_limit::iter_concurrent_limit!(
                shard_concurrent_limit,
                (0..n_chunks).into_par_iter(),
                try_for_each,
                |chunk_index| {
                    let chunk_subset = self.chunk_index_to_subset(
                        chunk_index as u64,
                        chunks_per_shard.as_slice(),
                        None,
                    );
                    let bytes = unsafe {
                        chunk_subset.extract_bytes_unchecked(
                            decoded_value,
                            &shard_shape,
                            shard_representation.element_size(),
                        )
                    };
                    if !chunk_representation.fill_value().equals_all(&bytes) {
                        let chunk_encoded = self.inner_codecs.encode_opt(
                            bytes,
                            chunk_representation,
                            &options_inner,
                        )?;

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
                }
            )?;
        }

        // Truncate shard
        let shard_length = encoded_shard_offset.load(std::sync::atomic::Ordering::Relaxed)
            + match self.index_location {
                ShardingIndexLocation::Start => 0,
                ShardingIndexLocation::End => index_encoded_size,
            };
        shard.truncate(shard_length);

        // Encode and write array index
        let encoded_array_index = self.index_codecs.encode_opt(
            transmute_to_bytes_vec(shard_index),
            &index_decoded_representation,
            options,
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
    // TODO: Collecting chunks then allocating shard can use a lot of memory, have a low memory variant
    // TODO: Also benchmark performance with just performing an alloc like 1x decoded size and writing directly into it, growing if needed
    #[allow(clippy::too_many_lines)]
    fn encode_unbounded(
        &self,
        decoded_value: &[u8],
        shard_representation: &ChunkRepresentation,
        chunk_representation: &ChunkRepresentation,
        options: &EncodeOptions,
    ) -> Result<Vec<u8>, CodecError> {
        debug_assert_eq!(decoded_value.len() as u64, shard_representation.size()); // already validated in par_encode

        let chunks_per_shard =
            calculate_chunks_per_shard(shard_representation.shape(), chunk_representation.shape())
                .map_err(|e| CodecError::Other(e.to_string()))?;
        let index_decoded_representation =
            sharding_index_decoded_representation(chunks_per_shard.as_slice());
        let index_encoded_size =
            compute_index_encoded_size(&self.index_codecs, &index_decoded_representation)?;
        let index_encoded_size = usize::try_from(index_encoded_size).unwrap();

        // Find chunks that are not entirely the fill value and collect their decoded bytes
        let shard_shape = shard_representation.shape_u64();
        let n_chunks = chunks_per_shard
            .as_slice()
            .iter()
            .map(|i| usize::try_from(i.get()).unwrap())
            .product::<usize>();

        // Calc self/internal concurrent limits
        let (shard_concurrent_limit, concurrency_limit_inner_chunks) = calc_concurrent_limits(
            options.concurrent_limit(),
            &self.recommended_concurrency(shard_representation)?,
            &self
                .inner_codecs
                .recommended_concurrency(chunk_representation)?,
        );
        let mut options_inner = EncodeOptions::default();
        options_inner.set_concurrent_limit(concurrency_limit_inner_chunks);
        // println!("{shard_concurrent_limit} {concurrency_limit_inner_chunks:?}"); // FIXME: log debug?

        let encoded_chunks: Vec<(usize, Vec<u8>)> =
            rayon_iter_concurrent_limit::iter_concurrent_limit!(
                shard_concurrent_limit,
                (0..n_chunks).into_par_iter(),
                filter_map,
                |chunk_index| {
                    let chunk_subset = self.chunk_index_to_subset(
                        chunk_index as u64,
                        chunks_per_shard.as_slice(),
                        None,
                    );
                    let bytes = unsafe {
                        chunk_subset.extract_bytes_unchecked(
                            decoded_value,
                            &shard_shape,
                            shard_representation.element_size(),
                        )
                    };
                    if chunk_representation.fill_value().equals_all(&bytes) {
                        None
                    } else {
                        let encoded_chunk = self.inner_codecs.encode_opt(
                            bytes,
                            chunk_representation,
                            &options_inner,
                        );
                        match encoded_chunk {
                            Ok(encoded_chunk) => Some(Ok((chunk_index, encoded_chunk))),
                            Err(err) => Some(Err(err)),
                        }
                    }
                }
            )
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
        if !encoded_chunks.is_empty() {
            let shard_slice = unsafe {
                std::slice::from_raw_parts_mut(shard.as_mut_ptr().cast::<u8>(), shard.len())
            };
            let shard_slice = UnsafeCellSlice::new(shard_slice);
            let shard_index_slice = UnsafeCellSlice::new(&mut shard_index);
            rayon_iter_concurrent_limit::iter_concurrent_limit!(
                options.concurrent_limit(),
                encoded_chunks.into_par_iter(),
                for_each,
                |(chunk_index, chunk_encoded)| {
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
                }
            );
        }

        // Write shard index
        let encoded_array_index = self.index_codecs.encode_opt(
            transmute_to_bytes_vec(shard_index),
            &index_decoded_representation,
            options,
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

    #[cfg(feature = "async")]
    /// Preallocate shard, encode and write chunks (in parallel), then truncate shard
    #[allow(clippy::too_many_lines)]
    async fn async_encode_bounded(
        &self,
        decoded_value: &[u8],
        shard_representation: &ChunkRepresentation,
        chunk_representation: &ChunkRepresentation,
        chunk_size_bounded: u64,
        options: &EncodeOptions,
    ) -> Result<Vec<u8>, CodecError> {
        debug_assert_eq!(decoded_value.len() as u64, shard_representation.size()); // already validated in par_encode

        // Calculate maximum possible shard size
        let chunks_per_shard =
            calculate_chunks_per_shard(shard_representation.shape(), chunk_representation.shape())
                .map_err(|e| CodecError::Other(e.to_string()))?;
        let index_decoded_representation =
            sharding_index_decoded_representation(chunks_per_shard.as_slice());
        let index_encoded_size =
            compute_index_encoded_size(&self.index_codecs, &index_decoded_representation)?;
        let shard_size_bounded = Self::encoded_shard_bounded_size(
            index_encoded_size,
            chunk_size_bounded,
            chunks_per_shard.as_slice(),
        );

        // Allocate an array for the shard
        let mut shard = vec![
            core::mem::MaybeUninit::<u8>::uninit();
            usize::try_from(shard_size_bounded).unwrap()
        ];

        // Allocate the decoded shard index
        let mut shard_index = vec![u64::MAX; index_decoded_representation.num_elements_usize()];
        let encoded_shard_offset =
            std::sync::Arc::<std::sync::atomic::AtomicU64>::new(match self.index_location {
                ShardingIndexLocation::Start => index_encoded_size.into(),
                ShardingIndexLocation::End => 0.into(),
            });

        // Encode the shards and update the shard index
        {
            let shard_slice = unsafe {
                std::slice::from_raw_parts_mut(shard.as_mut_ptr().cast::<u8>(), shard.len())
            };
            let shard_slice = UnsafeCellSlice::new(shard_slice);
            let shard_index_slice = UnsafeCellSlice::new(&mut shard_index);
            let chunks_per_shard = &chunks_per_shard;
            let shard_shape = shard_representation.shape_u64();
            let futures = (0..chunks_per_shard
                .as_slice()
                .iter()
                .map(|i| i.get())
                .product::<u64>())
                .into_par_iter()
                .filter_map(|chunk_index| {
                    let chunk_subset =
                        self.chunk_index_to_subset(chunk_index, chunks_per_shard.as_slice(), None);
                    let bytes = unsafe {
                        chunk_subset.extract_bytes_unchecked(
                            decoded_value,
                            &shard_shape,
                            shard_representation.element_size(),
                        )
                    };
                    if chunk_representation.fill_value().equals_all(&bytes) {
                        None
                    } else {
                        Some(async move {
                            let chunk_encoded = self
                                .inner_codecs
                                .async_encode_opt(bytes, chunk_representation, options)
                                .await?;
                            Ok::<_, CodecError>((chunk_index, chunk_encoded))
                        })
                    }
                })
                .collect::<Vec<_>>();
            // FIXME: Limit concurrency and write to output internally

            let chunks_encoded = futures::future::join_all(futures)
                .await
                .into_iter()
                .collect::<Result<Vec<_>, _>>()?;
            chunks_encoded
                .into_par_iter()
                .try_for_each(|(chunk_index, chunk_encoded)| {
                    let chunk_index = usize::try_from(chunk_index).unwrap();
                    let chunk_len = u64::try_from(chunk_encoded.len()).unwrap();
                    let chunk_offset = encoded_shard_offset
                        .fetch_add(chunk_len, std::sync::atomic::Ordering::Relaxed);
                    if chunk_offset + chunk_len > shard_size_bounded {
                        // This is a dev error, indicates the codec bounded size is not correct
                        return Err(CodecError::from(
                            "Sharding did not allocate a large enough buffer",
                        ));
                    }
                    unsafe {
                        let shard_index_unsafe = shard_index_slice.get();
                        shard_index_unsafe[chunk_index * 2] = chunk_offset;
                        shard_index_unsafe[chunk_index * 2 + 1] = chunk_len;
                        let chunk_offset = usize::try_from(chunk_offset).unwrap();
                        let chunk_len = usize::try_from(chunk_len).unwrap();

                        let shard_unsafe = shard_slice.get();
                        shard_unsafe[chunk_offset..chunk_offset + chunk_len]
                            .copy_from_slice(&chunk_encoded);
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
        shard.truncate(usize::try_from(shard_length).unwrap());

        // Encode and write array index
        let encoded_array_index = self
            .index_codecs
            .async_encode_opt(
                transmute_to_bytes_vec(shard_index),
                &index_decoded_representation,
                options,
            )
            .await?;
        {
            let shard_slice = unsafe {
                std::slice::from_raw_parts_mut(shard.as_mut_ptr().cast::<u8>(), shard.len())
            };
            match self.index_location {
                ShardingIndexLocation::Start => {
                    shard_slice[..encoded_array_index.len()].copy_from_slice(&encoded_array_index);
                }
                ShardingIndexLocation::End => {
                    let shard_length = usize::try_from(shard_length).unwrap();
                    shard_slice[shard_length - encoded_array_index.len()..]
                        .copy_from_slice(&encoded_array_index);
                }
            }
        }
        #[allow(clippy::transmute_undefined_repr)]
        let shard = unsafe { core::mem::transmute(shard) };
        Ok(shard)
    }

    #[cfg(feature = "async")]
    /// Preallocate shard, encode and write chunks (in parallel), then truncate shard
    #[allow(clippy::too_many_lines)]
    async fn async_encode_unbounded(
        &self,
        decoded_value: &[u8],
        shard_representation: &ChunkRepresentation,
        chunk_representation: &ChunkRepresentation,
        options: &EncodeOptions,
    ) -> Result<Vec<u8>, CodecError> {
        debug_assert_eq!(decoded_value.len() as u64, shard_representation.size()); // already validated in par_encode

        // Calculate maximum possible shard size
        let chunks_per_shard =
            calculate_chunks_per_shard(shard_representation.shape(), chunk_representation.shape())
                .map_err(|e| CodecError::Other(e.to_string()))?;
        let index_decoded_representation =
            sharding_index_decoded_representation(chunks_per_shard.as_slice());
        let index_encoded_size =
            compute_index_encoded_size(&self.index_codecs, &index_decoded_representation)?;
        let index_encoded_size = usize::try_from(index_encoded_size).unwrap();

        // Encode the chunks
        let shard_shape = shard_representation.shape_u64();
        let encoded_chunks = futures::future::join_all(
            (0..chunks_per_shard
                .as_slice()
                .iter()
                .map(|i| i.get())
                .product::<u64>())
                .filter_map(|chunk_index| {
                    let chunk_subset =
                        self.chunk_index_to_subset(chunk_index, chunks_per_shard.as_slice(), None);
                    let bytes = unsafe {
                        chunk_subset.extract_bytes_unchecked(
                            decoded_value,
                            &shard_shape,
                            shard_representation.element_size(),
                        )
                    };
                    if chunk_representation.fill_value().equals_all(&bytes) {
                        None
                    } else {
                        Some((chunk_index, bytes))
                    }
                })
                .map(|(chunk_index, bytes)| async move {
                    let encoded_chunk = self
                        .inner_codecs
                        .async_encode_opt(bytes, chunk_representation, options)
                        .await?;
                    Ok::<_, CodecError>((chunk_index, encoded_chunk))
                }),
        )
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;

        // Get the shard size
        let encoded_chunks_size = encoded_chunks
            .iter()
            .map(|(_, encoded_chunk)| encoded_chunk.len())
            .sum::<usize>();
        let shard_size = encoded_chunks_size + index_encoded_size;

        // Allocate an array for the shard
        let mut shard = vec![core::mem::MaybeUninit::<u8>::uninit(); shard_size];

        // Allocate the decoded shard index
        let mut shard_index = vec![u64::MAX; index_decoded_representation.num_elements_usize()];
        let encoded_shard_offset: AtomicUsize = match self.index_location {
            ShardingIndexLocation::Start => index_encoded_size.into(),
            ShardingIndexLocation::End => 0.into(),
        };

        // Write the encoded shards into the shard and update the shard index
        {
            let shard_slice = unsafe {
                std::slice::from_raw_parts_mut(shard.as_mut_ptr().cast::<u8>(), shard.len())
            };
            let shard_slice = UnsafeCellSlice::new(shard_slice);
            let shard_index_slice = UnsafeCellSlice::new(&mut shard_index);
            encoded_chunks
                .into_par_iter()
                .try_for_each(|(chunk_index, encoded_chunk)| {
                    let chunk_index = usize::try_from(chunk_index).unwrap();
                    let chunk_offset = encoded_shard_offset
                        .fetch_add(encoded_chunk.len(), std::sync::atomic::Ordering::Relaxed);
                    if chunk_offset + encoded_chunk.len() > shard_size {
                        // This is a dev error
                        return Err(CodecError::from(
                            "Sharding did not allocate a large enough buffer",
                        ));
                    }

                    unsafe {
                        let shard_index_unsafe = shard_index_slice.get();
                        shard_index_unsafe[chunk_index * 2] = u64::try_from(chunk_offset).unwrap();
                        shard_index_unsafe[chunk_index * 2 + 1] =
                            u64::try_from(encoded_chunk.len()).unwrap();

                        let shard_unsafe = shard_slice.get();
                        shard_unsafe[chunk_offset..chunk_offset + encoded_chunk.len()]
                            .copy_from_slice(&encoded_chunk);
                    }
                    Ok::<_, CodecError>(())
                })?;
        }

        // Encode and write array index
        let encoded_array_index = self
            .index_codecs
            .async_encode_opt(
                transmute_to_bytes_vec(shard_index),
                &index_decoded_representation,
                options,
            )
            .await?;
        {
            let shard_slice = unsafe {
                std::slice::from_raw_parts_mut(shard.as_mut_ptr().cast::<u8>(), shard.len())
            };
            match self.index_location {
                ShardingIndexLocation::Start => {
                    shard_slice[..encoded_array_index.len()].copy_from_slice(&encoded_array_index);
                }
                ShardingIndexLocation::End => {
                    shard_slice[shard_size - encoded_array_index.len()..]
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
        chunks_per_shard: &[NonZeroU64],
        options: &DecodeOptions,
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
            encoded_shard_index.to_vec(),
            &index_array_representation,
            &self.index_codecs,
            options,
        )
    }

    #[cfg(feature = "async")]
    async fn async_decode_index(
        &self,
        encoded_shard: &[u8],
        chunks_per_shard: &[NonZeroU64],
        options: &DecodeOptions,
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
        async_decode_shard_index(
            encoded_shard_index.to_vec(),
            &index_array_representation,
            &self.index_codecs,
            options,
        )
        .await
    }

    #[cfg(feature = "async")]
    #[allow(clippy::too_many_lines)]
    async fn async_decode_chunks(
        &self,
        encoded_shard: &[u8],
        shard_index: &[u64],
        shard_representation: &ChunkRepresentation,
        options: &DecodeOptions,
    ) -> Result<Vec<u8>, CodecError> {
        // Allocate an array for the output
        let mut shard =
            vec![core::mem::MaybeUninit::<u8>::uninit(); shard_representation.size_usize()];
        let shard_slice =
            unsafe { std::slice::from_raw_parts_mut(shard.as_mut_ptr().cast::<u8>(), shard.len()) };
        let shard_slice = UnsafeCellSlice::new(shard_slice);

        let chunk_representation = unsafe {
            ChunkRepresentation::new_unchecked(
                self.chunk_shape.as_slice().to_vec(),
                shard_representation.data_type().clone(),
                shard_representation.fill_value().clone(),
            )
        };

        // Decode chunks
        let shard_shape = shard_representation.shape_u64();
        let chunks_per_shard =
            calculate_chunks_per_shard(shard_representation.shape(), chunk_representation.shape())
                .map_err(|e| CodecError::Other(e.to_string()))?;

        // Find filled / non filled chunks
        let n_chunks = chunks_per_shard
            .as_slice()
            .iter()
            .map(|i| i.get())
            .product::<u64>();
        let chunk_info = (0..n_chunks)
            .map(|chunk_index| {
                let chunk_subset =
                    self.chunk_index_to_subset(chunk_index, chunks_per_shard.as_slice(), None); // TODO
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
        // FIXME: Limit concurrency
        futures::future::join_all(
            chunk_info
                .iter()
                .filter_map(|(chunk_subset, offset_size)| {
                    offset_size
                        .as_ref()
                        .map(|offset_size| (chunk_subset, offset_size))
                })
                .map(|(chunk_subset, (offset, size))| {
                    let chunk_representation = chunk_representation.clone();
                    async move {
                        let encoded_chunk_slice = encoded_shard[*offset..*offset + *size].to_vec();
                        self.inner_codecs
                            .async_decode_opt(encoded_chunk_slice, &chunk_representation, options) // FIXME
                            .await
                            .map(move |decoded_chunk| (chunk_subset, decoded_chunk))
                    }
                }),
        )
        .await
        .into_par_iter()
        .try_for_each(|subset_and_decoded_chunk| {
            let (chunk_subset, decoded_chunk) = subset_and_decoded_chunk?;
            let mut data_idx = 0;
            let element_size = chunk_representation.element_size() as u64;
            let shard_slice = unsafe { shard_slice.get() };
            let contiguous_iterator =
                unsafe { chunk_subset.contiguous_linearised_indices_unchecked(&shard_shape) };
            let length =
                usize::try_from(contiguous_iterator.contiguous_elements() * element_size).unwrap();
            for (index, _num_elements) in &contiguous_iterator {
                let shard_offset = usize::try_from(index * element_size).unwrap();
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
            let filled_chunk = chunk_representation
                .fill_value()
                .as_ne_bytes()
                .repeat(chunk_representation.num_elements_usize());

            // Write filled chunks
            filled_chunks.par_iter().for_each(|chunk_subset| {
                let mut data_idx = 0;
                let element_size = chunk_representation.element_size() as u64;
                let shard_slice = unsafe { shard_slice.get() };
                let contiguous_iterator =
                    unsafe { chunk_subset.contiguous_linearised_indices_unchecked(&shard_shape) };
                let length =
                    usize::try_from(contiguous_iterator.contiguous_elements() * element_size)
                        .unwrap();
                for (index, _num_elements) in &contiguous_iterator {
                    let shard_offset = usize::try_from(index * element_size).unwrap();
                    shard_slice[shard_offset..shard_offset + length]
                        .copy_from_slice(&filled_chunk[data_idx..data_idx + length]);
                    data_idx += length;
                }
            });
        };

        #[allow(clippy::transmute_undefined_repr)]
        let shard = unsafe { core::mem::transmute(shard) };
        Ok(shard)
    }
}
