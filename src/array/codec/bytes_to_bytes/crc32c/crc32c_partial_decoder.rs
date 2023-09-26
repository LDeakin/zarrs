use crate::{
    array::{
        codec::{BytesPartialDecoderTraits, CodecError},
        BytesRepresentation,
    },
    byte_range::ByteRange,
};

use super::CHECKSUM_SIZE;

/// The partial decoder for the Crc32c codec.
pub struct Crc32cPartialDecoder<'a> {
    input_handle: Box<dyn BytesPartialDecoderTraits + 'a>,
}

impl<'a> Crc32cPartialDecoder<'a> {
    /// Create a new partial decoder for the Crc32c codec.
    pub fn new(input_handle: Box<dyn BytesPartialDecoderTraits + 'a>) -> Self {
        Self { input_handle }
    }
}

impl BytesPartialDecoderTraits for Crc32cPartialDecoder<'_> {
    fn partial_decode(
        &self,
        decoded_representation: &BytesRepresentation,
        decoded_regions: &[ByteRange],
    ) -> Result<Vec<Vec<u8>>, CodecError> {
        let mut bytes = self
            .input_handle
            .partial_decode(decoded_representation, decoded_regions)?;

        // Drop trailing checksum
        for (bytes, byte_range) in bytes.iter_mut().zip(decoded_regions) {
            match byte_range {
                ByteRange::FromStart(_, Some(_)) => {}
                ByteRange::FromStart(_, None) => {
                    bytes.resize(bytes.len() - CHECKSUM_SIZE, 0);
                }
                ByteRange::FromEnd(offset, _) => {
                    if *offset < CHECKSUM_SIZE {
                        bytes.resize(bytes.len() - (CHECKSUM_SIZE - offset), 0);
                    }
                }
            };
        }

        Ok(bytes)
    }
}
