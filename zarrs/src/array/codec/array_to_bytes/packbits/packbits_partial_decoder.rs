use std::{ops::Div, sync::Arc};

use num::Integer;

use zarrs_metadata::codec::packbits::PackBitsPaddingEncoding;
use zarrs_storage::byte_range::ByteRange;

use crate::{
    array::{
        codec::{
            array_to_bytes::packbits::element_size_bits, ArrayPartialDecoderTraits, ArraySubset,
            BytesPartialDecoderTraits, CodecError, CodecOptions,
        },
        ArrayBytes, ArraySize, ChunkRepresentation, DataType,
    },
    array_subset::IncompatibleArraySubsetAndShapeError,
};

#[cfg(feature = "async")]
use crate::array::codec::{AsyncArrayPartialDecoderTraits, AsyncBytesPartialDecoderTraits};

#[cfg(feature = "async")]
use async_generic::async_generic;

// https://github.com/scouten/async-generic/pull/17
#[cfg_attr(feature = "async", async_generic(
    async_signature(
    input_handle: &Arc<dyn AsyncBytesPartialDecoderTraits>,
    decoded_representation: &ChunkRepresentation,
    padding_encoding: PackBitsPaddingEncoding,
    decoded_regions: &[ArraySubset],
    options: &CodecOptions,
)))]
fn partial_decode<'a>(
    input_handle: &Arc<dyn BytesPartialDecoderTraits>,
    decoded_representation: &ChunkRepresentation,
    padding_encoding: PackBitsPaddingEncoding,
    decoded_regions: &[ArraySubset],
    options: &CodecOptions,
) -> Result<Vec<ArrayBytes<'a>>, CodecError> {
    let element_size_bits = element_size_bits(decoded_representation.data_type())?;
    let element_size_bits_usize = usize::from(element_size_bits);
    let element_size_bits = u64::from(element_size_bits);
    let encoded_length_bits = decoded_representation.num_elements() * element_size_bits;

    let offset = match padding_encoding {
        PackBitsPaddingEncoding::StartByte => 1,
        PackBitsPaddingEncoding::None | PackBitsPaddingEncoding::EndByte => 0,
    };

    let chunk_shape = decoded_representation.shape_u64();
    let mut output = Vec::with_capacity(decoded_regions.len());
    for array_subset in decoded_regions {
        // Get the bit ranges that map to the elements
        let bit_ranges = array_subset
            .byte_ranges(&chunk_shape, element_size_bits_usize)
            .map_err(|_| {
                IncompatibleArraySubsetAndShapeError::from((
                    array_subset.clone(),
                    chunk_shape.clone(),
                ))
            })?;

        // Convert to byte ranges, skipping the padding encoding byte
        let byte_ranges: Vec<ByteRange> = bit_ranges
            .iter()
            .map(|bit_range| {
                let byte_start = offset + bit_range.start(encoded_length_bits).div(8);
                let byte_end = offset + bit_range.end(encoded_length_bits).div_ceil(8);
                ByteRange::new(byte_start..=byte_end)
            })
            .collect();

        // Retrieve those bytes
        #[cfg(feature = "async")]
        let encoded_bytes = if _async {
            input_handle.partial_decode(&byte_ranges, options).await
        } else {
            input_handle.partial_decode(&byte_ranges, options)
        }?;
        #[cfg(not(feature = "async"))]
        let encoded_bytes = input_handle.partial_decode(&byte_ranges, options)?;

        // Convert to elements
        let decoded_bytes = if let Some(encoded_bytes) = encoded_bytes {
            let mut elements: Vec<u8> = Vec::with_capacity(array_subset.num_elements_usize());
            dbg!(&array_subset);
            for (bytes, bit_range) in encoded_bytes.into_iter().zip(bit_ranges) {
                dbg!(&bytes);
                dbg!(&bit_range);

                // Get the bit range within the entire chunk
                let bit_start = usize::try_from(bit_range.start(encoded_length_bits)).unwrap();
                let bit_end = usize::try_from(bit_range.end(encoded_length_bits)).unwrap();

                // Get the "local bit range" within the requsted contiguous bytes
                let bit_offset_from_contiguous_byte_range = 8 * bit_start.div(8);
                let bit_start = bit_start - bit_offset_from_contiguous_byte_range;
                let bit_end = bit_end - bit_offset_from_contiguous_byte_range;
                debug_assert_eq!((bit_end - bit_start) % element_size_bits_usize, 0);

                // Determine the number of elements in this bit range
                let num_elements = (bit_end - bit_start) / element_size_bits_usize;

                for element_idx in 0..num_elements {
                    let bit_offset = element_idx * element_size_bits_usize;
                    let mut element = 0;
                    for bit in 0..element_size_bits_usize {
                        let (byte_encoded, bit_encoded) = (bit_offset + bit).div_rem(&8);
                        element |= ((bytes[byte_encoded] >> bit_encoded) & 0b1) << bit;
                    }
                    elements.push(element);
                }
            }
            dbg!(&elements);
            ArrayBytes::new_flen(elements)
        } else {
            ArrayBytes::new_fill_value(
                ArraySize::new(
                    decoded_representation.data_type().size(),
                    array_subset.num_elements(),
                ),
                decoded_representation.fill_value(),
            )
        };
        output.push(decoded_bytes);
    }

    Ok(output)
}

/// Partial decoder for the `packbits` codec.
pub(crate) struct PackBitsPartialDecoder {
    input_handle: Arc<dyn BytesPartialDecoderTraits>,
    decoded_representation: ChunkRepresentation,
    padding_encoding: PackBitsPaddingEncoding,
}

impl PackBitsPartialDecoder {
    /// Create a new partial decoder for the `packbits` codec.
    pub(crate) fn new(
        input_handle: Arc<dyn BytesPartialDecoderTraits>,
        decoded_representation: ChunkRepresentation,
        padding_encoding: PackBitsPaddingEncoding,
    ) -> Self {
        Self {
            input_handle,
            decoded_representation,
            padding_encoding,
        }
    }
}

impl ArrayPartialDecoderTraits for PackBitsPartialDecoder {
    fn data_type(&self) -> &DataType {
        self.decoded_representation.data_type()
    }

    fn partial_decode(
        &self,
        decoded_regions: &[ArraySubset],
        options: &CodecOptions,
    ) -> Result<Vec<ArrayBytes<'_>>, CodecError> {
        partial_decode(
            &self.input_handle,
            &self.decoded_representation,
            self.padding_encoding,
            decoded_regions,
            options,
        )
    }
}

#[cfg(feature = "async")]
/// Asynchronous partial decoder for the `packbits` codec.
pub(crate) struct AsyncPackBitsPartialDecoder {
    input_handle: Arc<dyn AsyncBytesPartialDecoderTraits>,
    decoded_representation: ChunkRepresentation,
    padding_encoding: PackBitsPaddingEncoding,
}

#[cfg(feature = "async")]
impl AsyncPackBitsPartialDecoder {
    /// Create a new partial decoder for the `packbits` codec.
    pub(crate) fn new(
        input_handle: Arc<dyn AsyncBytesPartialDecoderTraits>,
        decoded_representation: ChunkRepresentation,
        padding_encoding: PackBitsPaddingEncoding,
    ) -> Self {
        Self {
            input_handle,
            decoded_representation,
            padding_encoding,
        }
    }
}

#[cfg(feature = "async")]
#[async_trait::async_trait]
impl AsyncArrayPartialDecoderTraits for AsyncPackBitsPartialDecoder {
    fn data_type(&self) -> &DataType {
        self.decoded_representation.data_type()
    }

    async fn partial_decode(
        &self,
        decoded_regions: &[ArraySubset],
        options: &CodecOptions,
    ) -> Result<Vec<ArrayBytes<'_>>, CodecError> {
        partial_decode_async(
            &self.input_handle,
            &self.decoded_representation,
            self.padding_encoding,
            decoded_regions,
            options,
        )
        .await
    }
}
