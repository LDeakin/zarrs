use std::{borrow::Cow, sync::Arc};

use crate::{
    array::{
        codec::{BytesPartialDecoderTraits, CodecError, CodecOptions},
        RawBytes,
    },
    byte_range::ByteRange,
};

#[cfg(feature = "async")]
use crate::array::codec::AsyncBytesPartialDecoderTraits;

use super::CHECKSUM_SIZE;

/// Partial decoder for the `crc32c` (CRC32C checksum) codec.
pub struct Crc32cPartialDecoder<'a> {
    input_handle: Arc<dyn BytesPartialDecoderTraits + 'a>,
}

impl<'a> Crc32cPartialDecoder<'a> {
    /// Create a new partial decoder for the `crc32c` codec.
    pub fn new(input_handle: Arc<dyn BytesPartialDecoderTraits + 'a>) -> Self {
        Self { input_handle }
    }
}

impl BytesPartialDecoderTraits for Crc32cPartialDecoder<'_> {
    fn partial_decode(
        &self,
        decoded_regions: &[ByteRange],
        options: &CodecOptions,
    ) -> Result<Option<Vec<RawBytes<'_>>>, CodecError> {
        let bytes = self.input_handle.partial_decode(decoded_regions, options)?;
        let Some(bytes) = bytes else {
            return Ok(None);
        };

        // Drop trailing checksum
        let mut output = Vec::with_capacity(bytes.len());
        for (bytes, byte_range) in bytes.into_iter().zip(decoded_regions) {
            let bytes = match byte_range {
                ByteRange::FromStart(_, Some(_)) => bytes,
                ByteRange::FromStart(_, None) => {
                    let length = bytes.len() - CHECKSUM_SIZE;
                    Cow::Owned(bytes[..length].to_vec())
                }
                ByteRange::FromEnd(offset, _) => {
                    if *offset < CHECKSUM_SIZE as u64 {
                        let length = bytes.len() as u64 - (CHECKSUM_SIZE as u64 - offset);
                        let length = usize::try_from(length).unwrap();
                        Cow::Owned(bytes[..length].to_vec())
                    } else {
                        bytes
                    }
                }
            };
            output.push(bytes);
        }

        Ok(Some(output))
    }
}

#[cfg(feature = "async")]
/// Asynchronous partial decoder for the `crc32c` (CRC32C checksum) codec.
pub struct AsyncCrc32cPartialDecoder<'a> {
    input_handle: Arc<dyn AsyncBytesPartialDecoderTraits + 'a>,
}

#[cfg(feature = "async")]
impl<'a> AsyncCrc32cPartialDecoder<'a> {
    /// Create a new partial decoder for the `crc32c` codec.
    pub fn new(input_handle: Arc<dyn AsyncBytesPartialDecoderTraits + 'a>) -> Self {
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
    ) -> Result<Option<Vec<RawBytes<'_>>>, CodecError> {
        let bytes = self
            .input_handle
            .partial_decode(decoded_regions, options)
            .await?;
        let Some(bytes) = bytes else {
            return Ok(None);
        };

        // Drop trailing checksum
        let mut output = Vec::with_capacity(bytes.len());
        for (bytes, byte_range) in bytes.into_iter().zip(decoded_regions) {
            let bytes = match byte_range {
                ByteRange::FromStart(_, Some(_)) => bytes,
                ByteRange::FromStart(_, None) => {
                    let length = bytes.len() - CHECKSUM_SIZE;
                    Cow::Owned(bytes[..length].to_vec())
                }
                ByteRange::FromEnd(offset, _) => {
                    if *offset < CHECKSUM_SIZE as u64 {
                        let length = bytes.len() as u64 - (CHECKSUM_SIZE as u64 - offset);
                        let length = usize::try_from(length).unwrap();
                        Cow::Owned(bytes[..length].to_vec())
                    } else {
                        bytes
                    }
                }
            };
            output.push(bytes);
        }

        Ok(Some(output))
    }
}
