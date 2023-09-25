use crate::{
    array::{
        codec::{BytesPartialDecoderTraits, CodecError},
        BytesRepresentation,
    },
    byte_range::{extract_byte_ranges, ByteRange},
};

/// The partial decoder for the Zstd codec.
pub struct ZstdPartialDecoder<'a> {
    input_handle: Box<dyn BytesPartialDecoderTraits + 'a>,
}

impl<'a> ZstdPartialDecoder<'a> {
    /// Create a new partial decoder for the Zstd codec.
    pub fn new(input_handle: Box<dyn BytesPartialDecoderTraits + 'a>) -> Self {
        Self { input_handle }
    }
}

impl BytesPartialDecoderTraits for ZstdPartialDecoder<'_> {
    fn partial_decode(
        &self,
        decoded_representation: &BytesRepresentation,
        decoded_regions: &[ByteRange],
    ) -> Result<Vec<Vec<u8>>, CodecError> {
        let compressed = self.input_handle.decode(decoded_representation)?;
        let decompressed = zstd::decode_all(compressed.as_slice()).map_err(CodecError::IOError)?;
        extract_byte_ranges(&decompressed, decoded_regions)
            .map_err(CodecError::InvalidByteRangeError)
    }
}
