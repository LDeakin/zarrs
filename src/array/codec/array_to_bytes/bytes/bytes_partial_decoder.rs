use crate::{
    array::{
        chunk_shape_to_array_shape,
        codec::{ArrayPartialDecoderTraits, ArraySubset, BytesPartialDecoderTraits, CodecError},
        ChunkRepresentation,
    },
    array_subset::InvalidArraySubsetError,
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
    fn partial_decode_opt(
        &self,
        decoded_regions: &[ArraySubset],
        parallel: bool,
    ) -> Result<Vec<Vec<u8>>, CodecError> {
        let mut bytes = Vec::with_capacity(decoded_regions.len());
        let chunk_shape = chunk_shape_to_array_shape(self.decoded_representation.shape());
        for array_subset in decoded_regions {
            // Get byte ranges
            let byte_ranges = array_subset
                .byte_ranges(&chunk_shape, self.decoded_representation.element_size())
                .map_err(|_| InvalidArraySubsetError)?;

            // Decode
            let decoded = self
                .input_handle
                .partial_decode_opt(&byte_ranges, parallel)?;

            let bytes_subset = decoded.map_or_else(
                || {
                    self.decoded_representation
                        .fill_value()
                        .as_ne_bytes()
                        .repeat(array_subset.num_elements_usize())
                },
                |decoded| {
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
#[cfg_attr(feature = "async", async_trait::async_trait)]
impl AsyncArrayPartialDecoderTraits for AsyncBytesPartialDecoder<'_> {
    async fn partial_decode_opt(
        &self,
        decoded_regions: &[ArraySubset],
        parallel: bool,
    ) -> Result<Vec<Vec<u8>>, CodecError> {
        let mut bytes = Vec::with_capacity(decoded_regions.len());
        let chunk_shape = chunk_shape_to_array_shape(self.decoded_representation.shape());
        for array_subset in decoded_regions {
            // Get byte ranges
            let byte_ranges = array_subset
                .byte_ranges(&chunk_shape, self.decoded_representation.element_size())
                .map_err(|_| InvalidArraySubsetError)?;

            // Decode
            let decoded = self
                .input_handle
                .partial_decode_opt(&byte_ranges, parallel)
                .await?;

            let bytes_subset = decoded.map_or_else(
                || {
                    self.decoded_representation
                        .fill_value()
                        .as_ne_bytes()
                        .repeat(array_subset.num_elements_usize())
                },
                |decoded| {
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
