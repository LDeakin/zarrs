use async_trait::async_trait;

use crate::{
    array::codec::{AsyncBytesPartialDecoderTraits, BytesPartialDecoderTraits, CodecError},
    byte_range::ByteRange,
};

use super::CHECKSUM_SIZE;

/// Partial decoder for the Crc32c codec.
pub struct Crc32cPartialDecoder<'a> {
    input_handle: Box<dyn BytesPartialDecoderTraits + 'a>,
}

impl<'a> Crc32cPartialDecoder<'a> {
    /// Create a new partial decoder for the Crc32c codec.
    pub fn new(input_handle: Box<dyn BytesPartialDecoderTraits + 'a>) -> Self {
        Self { input_handle }
    }
}

impl BytesPartialDecoderTraits for Crc32cPartialDecoder<'_> {
    fn partial_decode_opt(
        &self,
        decoded_regions: &[ByteRange],
        parallel: bool,
    ) -> Result<Option<Vec<Vec<u8>>>, CodecError> {
        let bytes = self
            .input_handle
            .partial_decode_opt(decoded_regions, parallel)?;
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

/// Asynchronous partial decoder for the Crc32c codec.
pub struct AsyncCrc32cPartialDecoder<'a> {
    input_handle: Box<dyn AsyncBytesPartialDecoderTraits + 'a>,
}

impl<'a> AsyncCrc32cPartialDecoder<'a> {
    /// Create a new partial decoder for the Crc32c codec.
    pub fn new(input_handle: Box<dyn AsyncBytesPartialDecoderTraits + 'a>) -> Self {
        Self { input_handle }
    }
}

#[async_trait]
impl AsyncBytesPartialDecoderTraits for AsyncCrc32cPartialDecoder<'_> {
    async fn partial_decode_opt(
        &self,
        decoded_regions: &[ByteRange],
        parallel: bool,
    ) -> Result<Option<Vec<Vec<u8>>>, CodecError> {
        let bytes = self
            .input_handle
            .partial_decode_opt(decoded_regions, parallel)
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
