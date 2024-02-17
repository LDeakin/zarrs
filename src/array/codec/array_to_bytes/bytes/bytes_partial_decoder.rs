use crate::{
    array::{
        codec::{
            ArrayPartialDecoderTraits, ArraySubset, BytesPartialDecoderTraits, CodecError,
            PartialDecodeOptions,
        },
        ChunkRepresentation,
    },
    array_subset::IncompatibleArraySubsetAndShapeError,
};

#[cfg(feature = "async")]
use crate::array::codec::{AsyncArrayPartialDecoderTraits, AsyncBytesPartialDecoderTraits};

use super::{reverse_endianness, Endianness};

/// Partial decoder for the `bytes` codec.
pub struct BytesPartialDecoder<'a> {
    input_handle: Box<dyn BytesPartialDecoderTraits + 'a>,
    decoded_representation: ChunkRepresentation,
    endian: Option<Endianness>,
}

impl<'a> BytesPartialDecoder<'a> {
    /// Create a new partial decoder for the `bytes` codec.
    pub fn new(
        input_handle: Box<dyn BytesPartialDecoderTraits + 'a>,
        decoded_representation: ChunkRepresentation,
        endian: Option<Endianness>,
    ) -> Self {
        Self {
            input_handle,
            decoded_representation,
            endian,
        }
    }
}

impl ArrayPartialDecoderTraits for BytesPartialDecoder<'_> {
    fn element_size(&self) -> usize {
        self.decoded_representation.element_size()
    }

    fn partial_decode_opt(
        &self,
        decoded_regions: &[ArraySubset],
        options: &PartialDecodeOptions,
    ) -> Result<Vec<Vec<u8>>, CodecError> {
        let mut bytes = Vec::with_capacity(decoded_regions.len());
        let chunk_shape = self.decoded_representation.shape_u64();
        for array_subset in decoded_regions {
            // Get byte ranges
            let byte_ranges = array_subset
                .byte_ranges(&chunk_shape, self.decoded_representation.element_size())
                .map_err(|_| {
                    IncompatibleArraySubsetAndShapeError::from((
                        array_subset.clone(),
                        self.decoded_representation.shape_u64(),
                    ))
                })?;

            // Decode
            let decoded = self
                .input_handle
                .partial_decode_opt(&byte_ranges, options)?;

            let bytes_subset = decoded.map_or_else(
                || {
                    self.decoded_representation
                        .fill_value()
                        .as_ne_bytes()
                        .repeat(array_subset.num_elements_usize())
                },
                |decoded| {
                    // FIXME: Avoid this concat, prealloc and write to that
                    let mut bytes_subset = decoded.concat();
                    if let Some(endian) = &self.endian {
                        if !endian.is_native() {
                            reverse_endianness(
                                &mut bytes_subset,
                                self.decoded_representation.data_type(),
                            );
                        }
                    }
                    bytes_subset
                },
            );

            bytes.push(bytes_subset);
        }
        Ok(bytes)
    }
}

#[cfg(feature = "async")]
/// Asynchronous partial decoder for the `bytes` codec.
pub struct AsyncBytesPartialDecoder<'a> {
    input_handle: Box<dyn AsyncBytesPartialDecoderTraits + 'a>,
    decoded_representation: ChunkRepresentation,
    endian: Option<Endianness>,
}

#[cfg(feature = "async")]
impl<'a> AsyncBytesPartialDecoder<'a> {
    /// Create a new partial decoder for the `bytes` codec.
    pub fn new(
        input_handle: Box<dyn AsyncBytesPartialDecoderTraits + 'a>,
        decoded_representation: ChunkRepresentation,
        endian: Option<Endianness>,
    ) -> Self {
        Self {
            input_handle,
            decoded_representation,
            endian,
        }
    }
}

#[cfg(feature = "async")]
#[async_trait::async_trait]
impl AsyncArrayPartialDecoderTraits for AsyncBytesPartialDecoder<'_> {
    async fn partial_decode_opt(
        &self,
        decoded_regions: &[ArraySubset],
        options: &PartialDecodeOptions,
    ) -> Result<Vec<Vec<u8>>, CodecError> {
        let mut bytes = Vec::with_capacity(decoded_regions.len());
        let chunk_shape = self.decoded_representation.shape_u64();
        for array_subset in decoded_regions {
            // Get byte ranges
            let byte_ranges = array_subset
                .byte_ranges(&chunk_shape, self.decoded_representation.element_size())
                .map_err(|_| {
                    IncompatibleArraySubsetAndShapeError::from((
                        array_subset.clone(),
                        self.decoded_representation.shape_u64(),
                    ))
                })?;

            // Decode
            let decoded = self
                .input_handle
                .partial_decode_opt(&byte_ranges, options)
                .await?;

            let bytes_subset = decoded.map_or_else(
                || {
                    self.decoded_representation
                        .fill_value()
                        .as_ne_bytes()
                        .repeat(array_subset.num_elements_usize())
                },
                |decoded| {
                    // FIXME: Avoid this concat, prealloc and write to that
                    let mut bytes_subset = decoded.concat();
                    if let Some(endian) = &self.endian {
                        if !endian.is_native() {
                            reverse_endianness(
                                &mut bytes_subset,
                                self.decoded_representation.data_type(),
                            );
                        }
                    }
                    bytes_subset
                },
            );

            bytes.push(bytes_subset);
        }
        Ok(bytes)
    }
}
