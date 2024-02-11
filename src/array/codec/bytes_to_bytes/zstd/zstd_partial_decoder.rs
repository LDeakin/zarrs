use crate::{
    array::codec::{BytesPartialDecoderTraits, CodecError, PartialDecodeOptions},
    byte_range::{extract_byte_ranges, ByteRange},
};

#[cfg(feature = "async")]
use crate::array::codec::AsyncBytesPartialDecoderTraits;

/// Partial decoder for the `zstd` codec.
pub struct ZstdPartialDecoder<'a> {
    input_handle: Box<dyn BytesPartialDecoderTraits + 'a>,
}

impl<'a> ZstdPartialDecoder<'a> {
    /// Create a new partial decoder for the `zstd` codec.
    pub fn new(input_handle: Box<dyn BytesPartialDecoderTraits + 'a>) -> Self {
        Self { input_handle }
    }
}

impl BytesPartialDecoderTraits for ZstdPartialDecoder<'_> {
    fn partial_decode_opt(
        &self,
        decoded_regions: &[ByteRange],
        options: &PartialDecodeOptions,
    ) -> Result<Option<Vec<Vec<u8>>>, CodecError> {
        let encoded_value = self.input_handle.decode_opt(options)?;
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

#[cfg(feature = "async")]
/// Asynchronous partial decoder for the `zstd` codec.
pub struct AsyncZstdPartialDecoder<'a> {
    input_handle: Box<dyn AsyncBytesPartialDecoderTraits + 'a>,
}

#[cfg(feature = "async")]
impl<'a> AsyncZstdPartialDecoder<'a> {
    /// Create a new partial decoder for the `zstd` codec.
    pub fn new(input_handle: Box<dyn AsyncBytesPartialDecoderTraits + 'a>) -> Self {
        Self { input_handle }
    }
}

#[cfg(feature = "async")]
#[async_trait::async_trait]
impl AsyncBytesPartialDecoderTraits for AsyncZstdPartialDecoder<'_> {
    async fn partial_decode_opt(
        &self,
        decoded_regions: &[ByteRange],
        options: &PartialDecodeOptions,
    ) -> Result<Option<Vec<Vec<u8>>>, CodecError> {
        let encoded_value = self.input_handle.decode_opt(options).await?;
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
