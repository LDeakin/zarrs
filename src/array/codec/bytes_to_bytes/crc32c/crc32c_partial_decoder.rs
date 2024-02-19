use crate::{
    array::codec::{BytesPartialDecoderTraits, CodecError, CodecOptions},
    byte_range::ByteRange,
};

#[cfg(feature = "async")]
use crate::array::codec::AsyncBytesPartialDecoderTraits;

use super::CHECKSUM_SIZE;

/// Partial decoder for the `CRC32C checksum` codec.
pub struct Crc32cPartialDecoder<'a> {
    input_handle: Box<dyn BytesPartialDecoderTraits + 'a>,
}

impl<'a> Crc32cPartialDecoder<'a> {
    /// Create a new partial decoder for the `CRC32C checksum` codec.
    pub fn new(input_handle: Box<dyn BytesPartialDecoderTraits + 'a>) -> Self {
        Self { input_handle }
    }
}

impl BytesPartialDecoderTraits for Crc32cPartialDecoder<'_> {
    fn partial_decode(
        &self,
        decoded_regions: &[ByteRange],
        options: &CodecOptions,
    ) -> Result<Option<Vec<Vec<u8>>>, CodecError> {
        let bytes = self.input_handle.partial_decode(decoded_regions, options)?;
        let Some(mut bytes) = bytes else {
            return Ok(None);
        };

        // Drop trailing checksum
        for (bytes, byte_range) in bytes.iter_mut().zip(decoded_regions) {
            match byte_range {
                ByteRange::FromStart(_, Some(_)) => {}
                ByteRange::FromStart(_, None) => {
                    bytes.resize(bytes.len() - CHECKSUM_SIZE, 0);
                }
                ByteRange::FromEnd(offset, _) => {
                    if *offset < CHECKSUM_SIZE as u64 {
                        let length = bytes.len() as u64 - (CHECKSUM_SIZE as u64 - offset);
                        bytes.resize(usize::try_from(length).unwrap(), 0);
                    }
                }
            };
        }

        Ok(Some(bytes))
    }
}

#[cfg(feature = "async")]
/// Asynchronous partial decoder for the `CRC32C checksum` codec.
pub struct AsyncCrc32cPartialDecoder<'a> {
    input_handle: Box<dyn AsyncBytesPartialDecoderTraits + 'a>,
}

#[cfg(feature = "async")]
impl<'a> AsyncCrc32cPartialDecoder<'a> {
    /// Create a new partial decoder for the `CRC32C checksum` codec.
    pub fn new(input_handle: Box<dyn AsyncBytesPartialDecoderTraits + 'a>) -> Self {
        Self { input_handle }
    }
}

#[cfg(feature = "async")]
#[async_trait::async_trait]
impl AsyncBytesPartialDecoderTraits for AsyncCrc32cPartialDecoder<'_> {
    async fn partial_decode(
        &self,
        decoded_regions: &[ByteRange],
        options: &CodecOptions,
    ) -> Result<Option<Vec<Vec<u8>>>, CodecError> {
        let bytes = self
            .input_handle
            .partial_decode(decoded_regions, options)
            .await?;
        let Some(mut bytes) = bytes else {
            return Ok(None);
        };

        // Drop trailing checksum
        for (bytes, byte_range) in bytes.iter_mut().zip(decoded_regions) {
            match byte_range {
                ByteRange::FromStart(_, Some(_)) => {}
                ByteRange::FromStart(_, None) => {
                    bytes.resize(bytes.len() - CHECKSUM_SIZE, 0);
                }
                ByteRange::FromEnd(offset, _) => {
                    if *offset < CHECKSUM_SIZE as u64 {
                        let length = bytes.len() as u64 - (CHECKSUM_SIZE as u64 - offset);
                        bytes.resize(usize::try_from(length).unwrap(), 0);
                    }
                }
            };
        }

        Ok(Some(bytes))
    }
}
