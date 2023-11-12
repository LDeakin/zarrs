use std::io::{Cursor, Read};

use flate2::bufread::GzDecoder;

use crate::{
    array::codec::{BytesPartialDecoderTraits, CodecError},
    byte_range::{extract_byte_ranges, ByteRange},
};

/// The partial decoder for the gzip codec.
pub struct GzipPartialDecoder<'a> {
    input_handle: Box<dyn BytesPartialDecoderTraits + 'a>,
}

impl<'a> GzipPartialDecoder<'a> {
    /// Create a new partial decoder for the gzip codec.
    pub fn new(input_handle: Box<dyn BytesPartialDecoderTraits + 'a>) -> Self {
        Self { input_handle }
    }
}

impl BytesPartialDecoderTraits for GzipPartialDecoder<'_> {
    fn partial_decode_opt(
        &self,
        decoded_regions: &[ByteRange],
        parallel: bool,
    ) -> Result<Option<Vec<Vec<u8>>>, CodecError> {
        let encoded_value = if parallel {
            self.input_handle
                .par_partial_decode(&[ByteRange::FromStart(0, None)])?
        } else {
            self.input_handle
                .partial_decode(&[ByteRange::FromStart(0, None)])?
        }
        .map(|mut bytes| bytes.remove(0));
        let Some(encoded_value) = encoded_value else {
            return Ok(None);
        };

        let mut decoder = GzDecoder::new(Cursor::new(&encoded_value));
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed)?;

        Ok(Some(
            extract_byte_ranges(&decompressed, decoded_regions)
                .map_err(CodecError::InvalidByteRangeError)?,
        ))
    }
}
