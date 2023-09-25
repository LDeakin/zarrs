use crate::{
    array::{
        codec::{BytesPartialDecoderTraits, CodecError},
        BytesRepresentation,
    },
    byte_range::{extract_byte_ranges, ByteRange},
};

use super::decompress_bytes;

pub struct BloscPartialDecoder<'a> {
    input_handle: Box<dyn BytesPartialDecoderTraits + 'a>,
}

impl<'a> BloscPartialDecoder<'a> {
    pub fn new(input_handle: Box<dyn BytesPartialDecoderTraits + 'a>) -> Self {
        Self { input_handle }
    }
}

impl BytesPartialDecoderTraits for BloscPartialDecoder<'_> {
    fn partial_decode(
        &self,
        decoded_representation: &BytesRepresentation,
        decoded_regions: &[ByteRange],
    ) -> Result<Vec<Vec<u8>>, CodecError> {
        let encoded_value = self.input_handle.decode(decoded_representation)?;
        let decoded_value =
            decompress_bytes(&encoded_value).map_err(|e| CodecError::Other(e.to_string()))?;

        extract_byte_ranges(&decoded_value, decoded_regions)
            .map_err(CodecError::InvalidByteRangeError)
    }
}
