use crate::{
    array::codec::{BytesPartialDecoderTraits, CodecError},
    byte_range::{extract_byte_ranges, ByteRange},
};

#[cfg(feature = "async")]
use crate::array::codec::AsyncBytesPartialDecoderTraits;

/// Partial decoder for the `bz2` codec.
pub struct LzmaPartialDecoder<'a> {
    input_handle: Box<dyn BytesPartialDecoderTraits + 'a>,
}

impl<'a> LzmaPartialDecoder<'a> {
    pub fn new(input_handle: Box<dyn BytesPartialDecoderTraits + 'a>) -> Self {
        Self { input_handle }
    }
}

impl BytesPartialDecoderTraits for LzmaPartialDecoder<'_> {
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
            lzma::decompress(&encoded_value).map_err(|err| CodecError::Other(err.to_string()))?;

        Ok(Some(
            extract_byte_ranges(&decompressed, decoded_regions)
                .map_err(CodecError::InvalidByteRangeError)?,
        ))
    }
}

#[cfg(feature = "async")]
/// Asynchronous partial decoder for the `bz2` codec.
pub struct AsyncLzmaPartialDecoder<'a> {
    input_handle: Box<dyn AsyncBytesPartialDecoderTraits + 'a>,
}

#[cfg(feature = "async")]
impl<'a> AsyncLzmaPartialDecoder<'a> {
    pub fn new(input_handle: Box<dyn AsyncBytesPartialDecoderTraits + 'a>) -> Self {
        Self { input_handle }
    }
}

#[cfg(feature = "async")]
#[async_trait::async_trait]
impl AsyncBytesPartialDecoderTraits for AsyncLzmaPartialDecoder<'_> {
    async fn partial_decode_opt(
        &self,
        decoded_regions: &[ByteRange],
        parallel: bool,
    ) -> Result<Option<Vec<Vec<u8>>>, CodecError> {
        let encoded_value = self.input_handle.decode_opt(parallel).await?;
        let Some(encoded_value) = encoded_value else {
            return Ok(None);
        };

        let decompressed =
            lzma::decompress(&encoded_value).map_err(|err| CodecError::Other(err.to_string()))?;

        Ok(Some(
            extract_byte_ranges(&decompressed, decoded_regions)
                .map_err(CodecError::InvalidByteRangeError)?,
        ))
    }
}
