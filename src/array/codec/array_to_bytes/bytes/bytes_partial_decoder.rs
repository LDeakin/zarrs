use crate::{
    array::{
        codec::{ArrayPartialDecoderTraits, ArraySubset, BytesPartialDecoderTraits, CodecError},
        ArrayRepresentation, BytesRepresentation,
    },
    array_subset::InvalidArraySubsetError,
};

use super::{reverse_endianness, Endianness};

/// The partial decoder for the `bytes` codec.
pub struct BytesPartialDecoder<'a> {
    input_handle: Box<dyn BytesPartialDecoderTraits + 'a>,
    endian: Option<Endianness>,
}

impl<'a> BytesPartialDecoder<'a> {
    /// Create a new partial decoder for the `bytes` codec.
    pub fn new(
        input_handle: Box<dyn BytesPartialDecoderTraits + 'a>,
        endian: Option<Endianness>,
    ) -> Self {
        Self {
            input_handle,
            endian,
        }
    }
}

impl ArrayPartialDecoderTraits for BytesPartialDecoder<'_> {
    fn partial_decode(
        &self,
        decoded_representation: &ArrayRepresentation,
        decoded_regions: &[ArraySubset],
    ) -> Result<Vec<Vec<u8>>, CodecError> {
        let mut bytes = Vec::with_capacity(decoded_regions.len());
        for array_subset in decoded_regions {
            // Get byte ranges
            let byte_ranges = array_subset
                .byte_ranges(
                    decoded_representation.shape(),
                    decoded_representation.element_size(),
                )
                .map_err(|_| InvalidArraySubsetError)?;

            // Decode
            let decoded = self.input_handle.partial_decode(
                &BytesRepresentation::KnownSize(
                    decoded_representation.num_elements()
                        * decoded_representation.element_size() as u64,
                ),
                &byte_ranges,
            )?;

            let mut bytes_subset: Vec<u8> = decoded.concat();

            // Reverse endianness
            if let Some(endian) = &self.endian {
                if !endian.is_native() {
                    reverse_endianness(&mut bytes_subset, decoded_representation.element_size());
                }
            }
            bytes.push(bytes_subset);
        }
        Ok(bytes)
    }
}
