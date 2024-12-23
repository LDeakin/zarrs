use std::{
    borrow::Cow,
    io::{Cursor, Read},
    sync::Arc,
};

use flate2::bufread::GzDecoder;

use crate::{
    array::{
        codec::{BytesPartialDecoderTraits, CodecError, CodecOptions},
        RawBytes,
    },
    byte_range::{extract_byte_ranges, ByteRange},
};

#[cfg(feature = "async")]
use crate::array::codec::AsyncBytesPartialDecoderTraits;

/// Partial decoder for the `gzip` codec.
pub(crate) struct GzipPartialDecoder {
    input_handle: Arc<dyn BytesPartialDecoderTraits>,
}

impl GzipPartialDecoder {
    /// Create a new partial decoder for the `gzip` codec.
    pub(crate) fn new(input_handle: Arc<dyn BytesPartialDecoderTraits>) -> Self {
        Self { input_handle }
    }
}

impl BytesPartialDecoderTraits for GzipPartialDecoder {
    fn partial_decode(
        &self,
        decoded_regions: &[ByteRange],
        options: &CodecOptions,
    ) -> Result<Option<Vec<RawBytes<'_>>>, CodecError> {
        let encoded_value = self.input_handle.decode(options)?;
        let Some(encoded_value) = encoded_value else {
            return Ok(None);
        };

        let mut decoder = GzDecoder::new(Cursor::new(&encoded_value));
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed)?;

        Ok(Some(
            extract_byte_ranges(&decompressed, decoded_regions)
                .map_err(CodecError::InvalidByteRangeError)?
                .into_iter()
                .map(Cow::Owned)
                .collect(),
        ))
    }
}

#[cfg(feature = "async")]
/// Asynchronous partial decoder for the `gzip` codec.
pub(crate) struct AsyncGzipPartialDecoder {
    input_handle: Arc<dyn AsyncBytesPartialDecoderTraits>,
}

#[cfg(feature = "async")]
impl AsyncGzipPartialDecoder {
    /// Create a new partial decoder for the `gzip` codec.
    pub(crate) fn new(input_handle: Arc<dyn AsyncBytesPartialDecoderTraits>) -> Self {
        Self { input_handle }
    }
}

#[cfg(feature = "async")]
#[async_trait::async_trait]
impl AsyncBytesPartialDecoderTraits for AsyncGzipPartialDecoder {
    async fn partial_decode(
        &self,
        decoded_regions: &[ByteRange],
        options: &CodecOptions,
    ) -> Result<Option<Vec<RawBytes<'_>>>, CodecError> {
        let encoded_value = self.input_handle.decode(options).await?;
        let Some(encoded_value) = encoded_value else {
            return Ok(None);
        };

        let mut decoder = GzDecoder::new(Cursor::new(&encoded_value));
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed)?;

        Ok(Some(
            extract_byte_ranges(&decompressed, decoded_regions)
                .map_err(CodecError::InvalidByteRangeError)?
                .into_iter()
                .map(Cow::Owned)
                .collect(),
        ))
    }
}
