use std::io::{Cursor, Read};

use flate2::bufread::GzDecoder;

use crate::{
    array::{
        codec::{BytesPartialDecoderTraits, CodecError},
        BytesRepresentation,
    },
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
    fn partial_decode(
        &self,
        decoded_representation: &BytesRepresentation,
        decoded_regions: &[ByteRange],
    ) -> Result<Vec<Vec<u8>>, CodecError> {
        let compressed = self.input_handle.decode(decoded_representation)?;
        let mut decoder = GzDecoder::new(Cursor::new(&compressed));
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed)?;
        extract_byte_ranges(&decompressed, decoded_regions)
            .map_err(CodecError::InvalidByteRangeError)
    }
}
