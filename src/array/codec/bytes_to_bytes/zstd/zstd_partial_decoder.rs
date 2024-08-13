use std::{borrow::Cow, sync::Arc};

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

/// Partial decoder for the `zstd` codec.
pub struct ZstdPartialDecoder<'a> {
    input_handle: Arc<dyn BytesPartialDecoderTraits + 'a>,
}

impl<'a> ZstdPartialDecoder<'a> {
    /// Create a new partial decoder for the `zstd` codec.
    pub fn new(input_handle: Arc<dyn BytesPartialDecoderTraits + 'a>) -> Self {
        Self { input_handle }
    }
}

impl BytesPartialDecoderTraits for ZstdPartialDecoder<'_> {
    fn partial_decode(
        &self,
        decoded_regions: &[ByteRange],
        options: &CodecOptions,
    ) -> Result<Option<Vec<RawBytes<'_>>>, CodecError> {
        let encoded_value = self.input_handle.decode(options)?;
        let Some(encoded_value) = encoded_value else {
            return Ok(None);
        };

        let decompressed = zstd::decode_all(encoded_value.reader()).map_err(CodecError::IOError)?;

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
/// Asynchronous partial decoder for the `zstd` codec.
pub struct AsyncZstdPartialDecoder<'a> {
    input_handle: Arc<dyn AsyncBytesPartialDecoderTraits + 'a>,
}

#[cfg(feature = "async")]
impl<'a> AsyncZstdPartialDecoder<'a> {
    /// Create a new partial decoder for the `zstd` codec.
    pub fn new(input_handle: Arc<dyn AsyncBytesPartialDecoderTraits + 'a>) -> Self {
        Self { input_handle }
    }
}

#[cfg(feature = "async")]
#[async_trait::async_trait]
impl AsyncBytesPartialDecoderTraits for AsyncZstdPartialDecoder<'_> {
    async fn partial_decode(
        &self,
        decoded_regions: &[ByteRange],
        options: &CodecOptions,
    ) -> Result<Option<Vec<RawBytes<'_>>>, CodecError> {
        let encoded_value = self.input_handle.decode(options).await?;
        let Some(encoded_value) = encoded_value else {
            return Ok(None);
        };

        let decompressed = zstd::decode_all(encoded_value.reader()).map_err(CodecError::IOError)?;

        Ok(Some(
            extract_byte_ranges(&decompressed, decoded_regions)
                .map_err(CodecError::InvalidByteRangeError)?
                .into_iter()
                .map(Cow::Owned)
                .collect(),
        ))
    }
}
