use crate::{
    array::codec::{BytesPartialDecoderTraits, CodecError},
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
    fn partial_decode_opt(
        &self,
        decoded_regions: &[ByteRange],
        parallel: bool,
    ) -> Result<Option<Vec<Vec<u8>>>, CodecError> {
        let encoded_value = self.input_handle.decode_opt(parallel)?;
        let Some(encoded_value) = encoded_value else {
            return Ok(None);
        };

        let decompressed =
            zstd::decode_all(encoded_value.as_slice()).map_err(CodecError::IOError)?;

        Ok(Some(
            extract_byte_ranges(&decompressed, decoded_regions)
                .map_err(CodecError::InvalidByteRangeError)?,
        ))
    }
}
