use std::{num::NonZeroU64, sync::Arc};

use rayon::prelude::*;

use crate::{
    array::{
        array_bytes::{merge_chunks_vlen, update_bytes_flen},
        chunk_grid::RegularChunkGrid,
        chunk_shape_to_array_shape,
        codec::{
            ArrayCodecTraits, ArrayPartialDecoderTraits, ArraySubset, ArrayToBytesCodecTraits,
            ByteIntervalPartialDecoder, BytesPartialDecoderTraits, CodecChain, CodecError,
            CodecOptions,
        },
        concurrency::{calc_concurrency_outer_inner, RecommendedConcurrency},
        ravel_indices,
        unsafe_cell_slice::UnsafeCellSlice,
        ArrayBytes, ArraySize, ChunkRepresentation, ChunkShape, DataType, DataTypeSize,
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
    sharding_index_decoded_representation, ShardingIndexLocation,
};

/// Partial decoder for the sharding codec.
pub struct ShardingPartialDecoder<'a> {
    input_handle: Arc<dyn BytesPartialDecoderTraits + 'a>,
    decoded_representation: ChunkRepresentation,
    chunk_grid: RegularChunkGrid,
    inner_codecs: &'a CodecChain,
    shard_index: Option<Vec<u64>>,
}

impl<'a> ShardingPartialDecoder<'a> {
    /// Create a new partial decoder for the sharding codec.
    pub fn new(
        input_handle: Arc<dyn BytesPartialDecoderTraits + 'a>,
        decoded_representation: ChunkRepresentation,
        chunk_shape: ChunkShape,
        inner_codecs: &'a CodecChain,
        index_codecs: &'a CodecChain,
        index_location: ShardingIndexLocation,
        options: &CodecOptions,
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
        options: &CodecOptions,
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
            calculate_chunks_per_shard(shard_shape, chunk_representation.shape())?;

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
            .partial_decode(&[index_byte_range], options)?
            .map(|mut v| v.remove(0));

        Ok(match encoded_shard_index {
            Some(encoded_shard_index) => Some(decode_shard_index(
                &encoded_shard_index,
                &index_array_representation,
                index_codecs,
                options,
            )?),
            None => None,
        })
    }
}

impl ArrayPartialDecoderTraits for ShardingPartialDecoder<'_> {
    fn data_type(&self) -> &DataType {
        self.decoded_representation.data_type()
    }

    #[allow(clippy::too_many_lines)]
    fn partial_decode_opt(
        &self,
        array_subsets: &[ArraySubset],
        options: &CodecOptions,
    ) -> Result<Vec<ArrayBytes<'_>>, CodecError> {
        for array_subset in array_subsets {
            if array_subset.dimensionality() != self.decoded_representation.dimensionality() {
                return Err(CodecError::InvalidArraySubsetDimensionalityError(
                    array_subset.clone(),
                    self.decoded_representation.dimensionality(),
                ));
            }
        }

        let Some(shard_index) = &self.shard_index else {
            return Ok(array_subsets
                .iter()
                .map(|decoded_region| {
                    let array_size = ArraySize::new(
                        self.decoded_representation.data_type().size(),
                        decoded_region.num_elements(),
                    );
                    ArrayBytes::new_fill_value(array_size, self.decoded_representation.fill_value())
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
        )?;
        let chunks_per_shard = chunk_shape_to_array_shape(chunks_per_shard.as_slice());
        let num_chunks = usize::try_from(chunks_per_shard.iter().product::<u64>()).unwrap();

        // Calculate inner chunk/codec concurrency
        let (inner_chunk_concurrent_limit, concurrency_limit_codec) = calc_concurrency_outer_inner(
            options.concurrent_target(),
            &RecommendedConcurrency::new_maximum(std::cmp::min(
                options.concurrent_target(),
                num_chunks,
            )),
            &self
                .inner_codecs
                .recommended_concurrency(&chunk_representation)?,
        );
        let options = options
            .into_builder()
            .concurrent_target(concurrency_limit_codec)
            .build();

        let mut out = Vec::with_capacity(array_subsets.len());
        for array_subset in array_subsets {
            let chunks = unsafe { array_subset.chunks_unchecked(chunk_representation.shape()) };

            match self.decoded_representation.element_size() {
                DataTypeSize::Variable => {
                    let decode_inner_chunk_subset = |(chunk_indices, chunk_subset): (
                        Vec<u64>,
                        _,
                    )| {
                        let shard_index_idx: usize =
                            usize::try_from(ravel_indices(&chunk_indices, &chunks_per_shard) * 2)
                                .unwrap();
                        let offset = shard_index[shard_index_idx];
                        let size = shard_index[shard_index_idx + 1];

                        // Get the subset of bytes from the chunk which intersect the array
                        let chunk_subset_overlap =
                            unsafe { array_subset.overlap_unchecked(&chunk_subset) };

                        let chunk_subset_bytes = if offset == u64::MAX && size == u64::MAX {
                            let array_size = ArraySize::new(
                                chunk_representation.data_type().size(),
                                chunk_subset_overlap.num_elements(),
                            );
                            ArrayBytes::new_fill_value(
                                array_size,
                                chunk_representation.fill_value(),
                            )
                        } else {
                            // Partially decode the inner chunk
                            let partial_decoder = self.inner_codecs.partial_decoder(
                                Arc::new(ByteIntervalPartialDecoder::new(
                                    &*self.input_handle,
                                    offset,
                                    size,
                                )),
                                &chunk_representation,
                                &options,
                            )
                            .map_err(|err| if let CodecError::InvalidByteRangeError(_) = err {
                                CodecError::Other(
                                    "The shard index references out-of-bounds bytes. The chunk may be corrupted."
                                        .to_string(),
                                )
                            } else {
                                err
                            })?;
                            partial_decoder
                                .partial_decode_opt(
                                    &[chunk_subset_overlap
                                        .relative_to(chunk_subset.start())
                                        .unwrap()],
                                    &options,
                                )?
                                .remove(0)
                                .into_owned()
                        };
                        Ok::<_, CodecError>((
                            chunk_subset_bytes,
                            chunk_subset_overlap
                                .relative_to(array_subset.start())
                                .unwrap(),
                        ))
                    };

                    // Decode the inner chunk subsets
                    let chunk_bytes_and_subsets =
                        rayon_iter_concurrent_limit::iter_concurrent_limit!(
                            inner_chunk_concurrent_limit,
                            chunks,
                            map,
                            decode_inner_chunk_subset
                        )
                        .collect::<Result<Vec<_>, _>>()?;

                    // Convert into an array
                    let out_array_subset =
                        merge_chunks_vlen(chunk_bytes_and_subsets, array_subset.shape())?;
                    out.push(out_array_subset);
                }
                DataTypeSize::Fixed(data_type_size) => {
                    let array_subset_size = array_subset.num_elements_usize() * data_type_size;
                    let mut out_array_subset = vec![0; array_subset_size];
                    let out_array_subset_slice =
                        UnsafeCellSlice::new(out_array_subset.as_mut_slice());

                    let decode_inner_chunk_subset_into_slice = |(chunk_indices, chunk_subset): (
                        Vec<u64>,
                        _,
                    )| {
                        let shard_index_idx: usize =
                            usize::try_from(ravel_indices(&chunk_indices, &chunks_per_shard) * 2)
                                .unwrap();
                        let offset = shard_index[shard_index_idx];
                        let size = shard_index[shard_index_idx + 1];

                        // Get the subset of bytes from the chunk which intersect the array
                        let chunk_subset_overlap =
                            unsafe { array_subset.overlap_unchecked(&chunk_subset) };

                        let decoded_bytes = if offset == u64::MAX && size == u64::MAX {
                            let array_size = ArraySize::new(
                                chunk_representation.data_type().size(),
                                chunk_subset_overlap.num_elements(),
                            );
                            ArrayBytes::new_fill_value(
                                array_size,
                                chunk_representation.fill_value(),
                            )
                        } else {
                            // Partially decode the inner chunk
                            let partial_decoder = self.inner_codecs.partial_decoder(
                                Arc::new(ByteIntervalPartialDecoder::new(
                                    &*self.input_handle,
                                    offset,
                                    size,
                                )),
                                &chunk_representation,
                                &options,
                            )
                            .map_err(|err| if let CodecError::InvalidByteRangeError(_) = err {
                                CodecError::Other(
                                    "The shard index references out-of-bounds bytes. The chunk may be corrupted."
                                        .to_string(),
                                )
                            } else {
                                err
                            })?;
                            partial_decoder
                                .partial_decode_opt(
                                    &[chunk_subset_overlap
                                        .relative_to(chunk_subset.start())
                                        .unwrap()],
                                    &options,
                                )?
                                .remove(0)
                                .into_owned()
                        };
                        let decoded_bytes = decoded_bytes.into_fixed()?;
                        update_bytes_flen(
                            unsafe { out_array_subset_slice.get() },
                            array_subset.shape(),
                            &decoded_bytes,
                            &chunk_subset_overlap
                                .relative_to(array_subset.start())
                                .unwrap(),
                            data_type_size,
                        );
                        Ok::<_, CodecError>(())
                    };

                    rayon_iter_concurrent_limit::iter_concurrent_limit!(
                        inner_chunk_concurrent_limit,
                        chunks,
                        try_for_each,
                        decode_inner_chunk_subset_into_slice
                    )?;
                    out.push(ArrayBytes::from(out_array_subset));
                }
            }
        }
        Ok(out)
    }
}

#[cfg(feature = "async")]
/// Asynchronous partial decoder for the sharding codec.
pub struct AsyncShardingPartialDecoder<'a> {
    input_handle: Arc<dyn AsyncBytesPartialDecoderTraits + 'a>,
    decoded_representation: ChunkRepresentation,
    chunk_grid: RegularChunkGrid,
    inner_codecs: &'a CodecChain,
    shard_index: Option<Vec<u64>>,
}

#[cfg(feature = "async")]
impl<'a> AsyncShardingPartialDecoder<'a> {
    /// Create a new partial decoder for the sharding codec.
    pub async fn new(
        input_handle: Arc<dyn AsyncBytesPartialDecoderTraits + 'a>,
        decoded_representation: ChunkRepresentation,
        chunk_shape: ChunkShape,
        inner_codecs: &'a CodecChain,
        index_codecs: &'a CodecChain,
        index_location: ShardingIndexLocation,
        options: &CodecOptions,
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
        options: &CodecOptions,
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
            calculate_chunks_per_shard(shard_shape, chunk_representation.shape())?;

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
            .partial_decode(&[index_byte_range], options)
            .await?
            .map(|mut v| v.remove(0));

        Ok(match encoded_shard_index {
            Some(encoded_shard_index) => Some(decode_shard_index(
                &encoded_shard_index,
                &index_array_representation,
                index_codecs,
                options,
            )?),
            None => None,
        })
    }
}

#[cfg(feature = "async")]
#[async_trait::async_trait]
impl AsyncArrayPartialDecoderTraits for AsyncShardingPartialDecoder<'_> {
    fn data_type(&self) -> &DataType {
        self.decoded_representation.data_type()
    }

    #[allow(clippy::too_many_lines)]
    async fn partial_decode_opt(
        &self,
        array_subsets: &[ArraySubset],
        options: &CodecOptions,
    ) -> Result<Vec<ArrayBytes<'_>>, CodecError> {
        for array_subset in array_subsets {
            if array_subset.dimensionality() != self.decoded_representation.dimensionality() {
                return Err(CodecError::InvalidArraySubsetDimensionalityError(
                    array_subset.clone(),
                    self.decoded_representation.dimensionality(),
                ));
            }
        }

        let Some(shard_index) = &self.shard_index else {
            return Ok(array_subsets
                .iter()
                .map(|decoded_region| {
                    let array_size = ArraySize::new(
                        self.decoded_representation.data_type().size(),
                        decoded_region.num_elements(),
                    );
                    ArrayBytes::new_fill_value(array_size, self.decoded_representation.fill_value())
                })
                .collect());
        };

        let chunks_per_shard = calculate_chunks_per_shard(
            self.decoded_representation.shape(),
            self.chunk_grid.chunk_shape(),
        )?;
        let chunks_per_shard = chunk_shape_to_array_shape(chunks_per_shard.as_slice());

        let chunk_representation = unsafe {
            ChunkRepresentation::new_unchecked(
                self.chunk_grid.chunk_shape().to_vec(),
                self.decoded_representation.data_type().clone(),
                self.decoded_representation.fill_value().clone(),
            )
        };

        let mut out = Vec::with_capacity(array_subsets.len());
        // TODO: Could go parallel here?
        for array_subset in array_subsets {
            match self.decoded_representation.element_size() {
                DataTypeSize::Variable => {
                    let chunks =
                        unsafe { array_subset.chunks_unchecked(chunk_representation.shape()) };

                    let decode_inner_chunk_subset = |(chunk_indices, chunk_subset): (
                        Vec<u64>,
                        _,
                    )| {
                        let shard_index_idx: usize =
                            usize::try_from(ravel_indices(&chunk_indices, &chunks_per_shard) * 2)
                                .unwrap();
                        let chunk_representation = chunk_representation.clone();
                        async move {
                            let offset = shard_index[shard_index_idx];
                            let size = shard_index[shard_index_idx + 1];

                            // Get the subset of bytes from the chunk which intersect the array
                            let chunk_subset_overlap =
                                unsafe { array_subset.overlap_unchecked(&chunk_subset) };

                            let chunk_subset_bytes = if offset == u64::MAX && size == u64::MAX {
                                let array_size = ArraySize::new(
                                    self.data_type().size(),
                                    chunk_subset_overlap.num_elements(),
                                );
                                ArrayBytes::new_fill_value(
                                    array_size,
                                    chunk_representation.fill_value(),
                                )
                            } else {
                                // Partially decode the inner chunk
                                let partial_decoder = self.inner_codecs.async_partial_decoder(
                                    Arc::new(AsyncByteIntervalPartialDecoder::new(
                                        &*self.input_handle,
                                        offset,
                                        size,
                                    )),
                                    &chunk_representation,
                                    options,
                                ).await
                                .map_err(|err| if let CodecError::InvalidByteRangeError(_) = err {
                                    CodecError::Other(
                                        "The shard index references out-of-bounds bytes. The chunk may be corrupted."
                                            .to_string(),
                                    )
                                } else {
                                    err
                                })?;
                                partial_decoder
                                    .partial_decode_opt(
                                        &[chunk_subset_overlap
                                            .relative_to(chunk_subset.start())
                                            .unwrap()],
                                        options,
                                    )
                                    .await?
                                    .remove(0)
                                    .into_owned()
                            };
                            Ok::<_, CodecError>((
                                chunk_subset_bytes,
                                chunk_subset_overlap
                                    .relative_to(array_subset.start())
                                    .unwrap(),
                            ))
                        }
                    };

                    // Decode the inner chunk subsets
                    let futures = chunks.iter().map(decode_inner_chunk_subset);
                    let chunk_bytes_and_subsets = futures::future::try_join_all(futures).await?;

                    // Convert into an array
                    let out_array_subset =
                        merge_chunks_vlen(chunk_bytes_and_subsets, array_subset.shape())?;
                    out.push(out_array_subset);
                }
                DataTypeSize::Fixed(data_type_size) => {
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

                    let shard_size = array_subset.num_elements_usize() * data_type_size;
                    let mut shard = Vec::with_capacity(shard_size);
                    let shard_slice = UnsafeCellSlice::new_from_vec_with_spare_capacity(&mut shard);

                    // Decode unfilled chunks
                    let results = futures::future::join_all(
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
                                let partial_decoder = self
                                    .inner_codecs
                                    .async_partial_decoder(
                                        Arc::new(AsyncByteIntervalPartialDecoder::new(
                                            &*self.input_handle,
                                            u64::try_from(*offset).unwrap(),
                                            u64::try_from(*size).unwrap(),
                                        )),
                                        &chunk_representation,
                                        options, // TODO: Adjust options for partial decoding?
                                    )
                                    .await
                                    .map_err(|err| if let CodecError::InvalidByteRangeError(_) = err {
                                        CodecError::Other(
                                            "The shard index references out-of-bounds bytes. The chunk may be corrupted."
                                                .to_string(),
                                        )
                                    } else {
                                        err
                                    })?;
                                let chunk_subset_overlap = unsafe { array_subset.overlap_unchecked(chunk_subset) };
                                // Partial decoding is actually really slow with the blosc codec! Assume sharded chunks are small, and just decode the whole thing and extract bytes
                                // TODO: Investigate further
                                // let decoded_chunk = partial_decoder
                                //     .partial_decode(&[chunk_subset_overlap.relative_to(chunk_subset.start())?])
                                //     .await?
                                //     .remove(0);
                                let decoded_chunk = partial_decoder
                                    .partial_decode_opt(
                                        &[ArraySubset::new_with_shape(chunk_subset.shape().to_vec())],
                                        options,
                                    ) // TODO: Adjust options for partial decoding
                                    .await?
                                    .remove(0).into_owned();
                                let decoded_chunk = decoded_chunk
                                    .extract_array_subset(
                                        &chunk_subset_overlap.relative_to(chunk_subset.start()).unwrap(),
                                        chunk_subset.shape(),
                                        self.decoded_representation.data_type()
                                    )?
                                    .into_fixed()?
                                    .into_owned();
                                Ok::<_, CodecError>((decoded_chunk, chunk_subset_overlap))
                            }}),
                        )
                        .await;
                    // FIXME: Concurrency limit for futures

                    if !results.is_empty() {
                        rayon_iter_concurrent_limit::iter_concurrent_limit!(
                            options.concurrent_target(),
                            results,
                            try_for_each,
                            |subset_and_decoded_chunk| {
                                let (chunk_subset_bytes, chunk_subset_overlap): (
                                    Vec<u8>,
                                    ArraySubset,
                                ) = subset_and_decoded_chunk?;
                                update_bytes_flen(
                                    unsafe { shard_slice.get() },
                                    array_subset.shape(),
                                    &chunk_subset_bytes.into(),
                                    &chunk_subset_overlap
                                        .relative_to(array_subset.start())
                                        .unwrap(),
                                    data_type_size,
                                );
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
                        // Write filled chunks
                        rayon_iter_concurrent_limit::iter_concurrent_limit!(
                            options.concurrent_target(),
                            filled_chunks,
                            for_each,
                            |chunk_subset: &ArraySubset| {
                                let chunk_subset_overlap =
                                    unsafe { array_subset.overlap_unchecked(chunk_subset) };
                                let filled_chunk = self
                                    .decoded_representation
                                    .fill_value()
                                    .as_ne_bytes()
                                    .repeat(chunk_subset_overlap.num_elements_usize());
                                update_bytes_flen(
                                    unsafe { shard_slice.get() },
                                    array_subset.shape(),
                                    &filled_chunk.into(),
                                    &chunk_subset_overlap
                                        .relative_to(array_subset.start())
                                        .unwrap(),
                                    data_type_size,
                                );
                            }
                        );
                    };
                    unsafe { shard.set_len(shard_size) };
                    out.push(ArrayBytes::from(shard));
                }
            }
        }
        Ok(out)
    }
}
