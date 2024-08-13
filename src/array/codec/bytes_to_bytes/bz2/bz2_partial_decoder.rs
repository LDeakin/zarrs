use std::{borrow::Cow, io::Read, sync::Arc};

use bytes::Buf;

use crate::{
    array::{
        codec::{BytesPartialDecoderTraits, CodecError, CodecOptions},
        RawBytes,
    },
    byte_range::{extract_byte_ranges, ByteRange},
};

#[cfg(feature = "async")]
use crate::array::codec::AsyncBytesPartialDecoderTraits;

/// Partial decoder for the `bz2` codec.
pub struct Bz2PartialDecoder<'a> {
    input_handle: Arc<dyn BytesPartialDecoderTraits + 'a>,
}

impl<'a> Bz2PartialDecoder<'a> {
    pub fn new(input_handle: Arc<dyn BytesPartialDecoderTraits + 'a>) -> Self {
        Self { input_handle }
    }
}

impl BytesPartialDecoderTraits for Bz2PartialDecoder<'_> {
    fn partial_decode(
        &self,
        decoded_regions: &[ByteRange],
        options: &CodecOptions,
    ) -> Result<Option<Vec<RawBytes<'_>>>, CodecError> {
        let encoded_value = self.input_handle.decode(options)?;
        let Some(encoded_value) = encoded_value else {
            return Ok(None);
        };

        let mut decoder = bzip2::read::BzDecoder::new(encoded_value.reader());
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
/// Asynchronous partial decoder for the `bz2` codec.
pub struct AsyncBz2PartialDecoder<'a> {
    input_handle: Arc<dyn AsyncBytesPartialDecoderTraits + 'a>,
}

#[cfg(feature = "async")]
impl<'a> AsyncBz2PartialDecoder<'a> {
    pub fn new(input_handle: Arc<dyn AsyncBytesPartialDecoderTraits + 'a>) -> Self {
        Self { input_handle }
    }
}

#[cfg(feature = "async")]
#[async_trait::async_trait]
impl AsyncBytesPartialDecoderTraits for AsyncBz2PartialDecoder<'_> {
    async fn partial_decode(
        &self,
        decoded_regions: &[ByteRange],
        options: &CodecOptions,
    ) -> Result<Option<Vec<RawBytes<'_>>>, CodecError> {
        let encoded_value = self.input_handle.decode(options).await?;
        let Some(encoded_value) = encoded_value else {
            return Ok(None);
        };

        let mut decoder = bzip2::read::BzDecoder::new(encoded_value.reader());
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
