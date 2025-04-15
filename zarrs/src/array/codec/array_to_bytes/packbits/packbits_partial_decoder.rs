#![allow(clippy::similar_names)]

use std::{ops::Div, sync::Arc};

use num::Integer;

use zarrs_metadata::codec::packbits::PackBitsPaddingEncoding;
use zarrs_storage::byte_range::ByteRange;

use crate::{
    array::{
        codec::{
            array_to_bytes::packbits::{div_rem_8bit, pack_bits_components},
            ArrayPartialDecoderTraits, ArraySubset, BytesPartialDecoderTraits, CodecError,
            CodecOptions,
        },
        ArrayBytes, ArraySize, ChunkRepresentation, DataType,
    },
    array_subset::IncompatibleArraySubsetAndShapeError,
};

#[cfg(feature = "async")]
use crate::array::codec::{AsyncArrayPartialDecoderTraits, AsyncBytesPartialDecoderTraits};

#[cfg(feature = "async")]
use async_generic::async_generic;

use super::DataTypeExtensionPackBitsCodecComponents;

// https://github.com/scouten/async-generic/pull/17
#[allow(clippy::too_many_lines)]
#[cfg_attr(feature = "async", async_generic(
    async_signature(
    input_handle: &Arc<dyn AsyncBytesPartialDecoderTraits>,
    decoded_representation: &ChunkRepresentation,
    padding_encoding: PackBitsPaddingEncoding,
    first_bit: Option<u64>,
    last_bit: Option<u64>,
    decoded_regions: &[ArraySubset],
    options: &CodecOptions,
)))]
fn partial_decode<'a>(
    input_handle: &Arc<dyn BytesPartialDecoderTraits>,
    decoded_representation: &ChunkRepresentation,
    padding_encoding: PackBitsPaddingEncoding,
    first_bit: Option<u64>,
    last_bit: Option<u64>,
    decoded_regions: &[ArraySubset],
    options: &CodecOptions,
) -> Result<Vec<ArrayBytes<'a>>, CodecError> {
    let DataTypeExtensionPackBitsCodecComponents {
        component_size_bits,
        num_components,
        sign_extension,
    } = pack_bits_components(decoded_representation.data_type())?;
    let first_bit = first_bit.unwrap_or(0);
    let last_bit = last_bit.unwrap_or(component_size_bits - 1);

    // Get the component and element size in bits
    let num_elements = decoded_representation.num_elements();
    let component_size_bits_extracted = last_bit - first_bit + 1;
    let element_size_bits = component_size_bits_extracted * num_components;
    let elements_size_bytes = (num_elements * element_size_bits).div_ceil(8);

    let data_type_size_dec = decoded_representation
        .data_type()
        .fixed_size()
        .ok_or_else(|| {
            CodecError::Other("data type must have a fixed size for packbits codec".to_string())
        })?;

    let element_size_bits_usize = usize::try_from(element_size_bits).unwrap();
    let encoded_length_bits = elements_size_bytes * 8;

    let offset = match padding_encoding {
        PackBitsPaddingEncoding::FirstByte => 1,
        PackBitsPaddingEncoding::None | PackBitsPaddingEncoding::LastByte => 0,
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
                ByteRange::new(byte_start..byte_end)
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
            let mut bytes_dec: Vec<u8> =
                vec![0; array_subset.num_elements_usize() * data_type_size_dec];
            let mut component_idx_outer = 0;
            for (packed_elements, bit_range) in encoded_bytes.into_iter().zip(bit_ranges) {
                // Get the bit range within the entire chunk
                let bit_start = bit_range.start(encoded_length_bits);
                let bit_end = bit_range.end(encoded_length_bits);
                let num_elements = (bit_end - bit_start) / element_size_bits;

                // Get the offset from the start of the byte range encapsulating the bit range
                let bit_offset_from_contiguous_byte_range = bit_start - 8 * bit_start.div(8);

                // Decode the components
                for component_idx in 0..num_elements * num_components {
                    let bit_dec0 = (component_idx_outer + component_idx) * component_size_bits;
                    let bit_enc0 = component_idx * component_size_bits_extracted;
                    for bit in 0..component_size_bits_extracted {
                        let bit_in = bit_enc0 + bit + bit_offset_from_contiguous_byte_range;
                        let bit_out = bit_dec0 + bit;
                        let (byte_enc, bit_enc) = bit_in.div_rem(&8);
                        let (byte_dec, bit_dec) = div_rem_8bit(bit_out, component_size_bits);
                        bytes_dec[usize::try_from(byte_dec).unwrap()] |=
                            ((packed_elements[usize::try_from(byte_enc).unwrap()] >> bit_enc)
                                & 0b1)
                                << bit_dec;
                    }
                    if sign_extension {
                        let signed: bool = {
                            let (byte_dec, bit_dec) = div_rem_8bit(
                                bit_dec0 + component_size_bits_extracted.saturating_sub(1),
                                component_size_bits,
                            );
                            bytes_dec[usize::try_from(byte_dec).unwrap()] >> bit_dec & 0x1 == 1
                        };
                        if signed {
                            for bit in component_size_bits_extracted..component_size_bits {
                                let (byte_dec, bit_dec) =
                                    div_rem_8bit(bit_dec0 + bit, component_size_bits);
                                bytes_dec[usize::try_from(byte_dec).unwrap()] |= 1 << bit_dec;
                            }
                        }
                    }
                }
                component_idx_outer += num_elements * num_components;
            }
            ArrayBytes::new_flen(bytes_dec)
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
    first_bit: Option<u64>,
    last_bit: Option<u64>,
}

impl PackBitsPartialDecoder {
    /// Create a new partial decoder for the `packbits` codec.
    pub(crate) fn new(
        input_handle: Arc<dyn BytesPartialDecoderTraits>,
        decoded_representation: ChunkRepresentation,
        padding_encoding: PackBitsPaddingEncoding,
        first_bit: Option<u64>,
        last_bit: Option<u64>,
    ) -> Self {
        Self {
            input_handle,
            decoded_representation,
            padding_encoding,
            first_bit,
            last_bit,
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
            self.first_bit,
            self.last_bit,
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
    first_bit: Option<u64>,
    last_bit: Option<u64>,
}

#[cfg(feature = "async")]
impl AsyncPackBitsPartialDecoder {
    /// Create a new partial decoder for the `packbits` codec.
    pub(crate) fn new(
        input_handle: Arc<dyn AsyncBytesPartialDecoderTraits>,
        decoded_representation: ChunkRepresentation,
        padding_encoding: PackBitsPaddingEncoding,
        first_bit: Option<u64>,
        last_bit: Option<u64>,
    ) -> Self {
        Self {
            input_handle,
            decoded_representation,
            padding_encoding,
            first_bit,
            last_bit,
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
            self.first_bit,
            self.last_bit,
            decoded_regions,
            options,
        )
        .await
    }
}
