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
pub(crate) struct Crc32cPartialDecoder {
    input_handle: Arc<dyn BytesPartialDecoderTraits>,
}

impl Crc32cPartialDecoder {
    /// Create a new partial decoder for the `crc32c` codec.
    pub(crate) fn new(input_handle: Arc<dyn BytesPartialDecoderTraits>) -> Self {
        Self { input_handle }
    }
}

impl BytesPartialDecoderTraits for Crc32cPartialDecoder {
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
                ByteRange::Suffix(_) => {
                    let length = bytes.len() as u64 - (CHECKSUM_SIZE as u64);
                    let length = usize::try_from(length).unwrap();
                    Cow::Owned(bytes[..length].to_vec())
                }
            };
            output.push(bytes);
        }

        Ok(Some(output))
    }
}

#[cfg(feature = "async")]
/// Asynchronous partial decoder for the `crc32c` (CRC32C checksum) codec.
pub(crate) struct AsyncCrc32cPartialDecoder {
    input_handle: Arc<dyn AsyncBytesPartialDecoderTraits>,
}

#[cfg(feature = "async")]
impl AsyncCrc32cPartialDecoder {
    /// Create a new partial decoder for the `crc32c` codec.
    pub(crate) fn new(input_handle: Arc<dyn AsyncBytesPartialDecoderTraits>) -> Self {
        Self { input_handle }
    }
}

#[cfg(feature = "async")]
#[async_trait::async_trait]
impl AsyncBytesPartialDecoderTraits for AsyncCrc32cPartialDecoder {
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
                ByteRange::Suffix(_) => {
                    let length = bytes.len() as u64 - (CHECKSUM_SIZE as u64);
                    let length = usize::try_from(length).unwrap();
                    Cow::Owned(bytes[..length].to_vec())
                }
            };
            output.push(bytes);
        }

        Ok(Some(output))
    }
}
