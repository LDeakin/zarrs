use crate::byte_range::{ByteLength, ByteOffset, ByteRange};

use super::{BytesPartialDecoderTraits, CodecError};

#[cfg(feature = "async")]
use super::AsyncBytesPartialDecoderTraits;

/// A byte interval partial decoder.
///
/// Modifies byte range requests to a specific byte interval in an inner bytes partial decoder.
pub struct ByteIntervalPartialDecoder<'a> {
    inner: &'a dyn BytesPartialDecoderTraits,
    byte_offset: ByteOffset,
    byte_length: ByteLength,
}

impl<'a> ByteIntervalPartialDecoder<'a> {
    /// Create a new byte interval partial decoder.
    pub fn new(
        inner: &'a dyn BytesPartialDecoderTraits,
        byte_offset: ByteOffset,
        byte_length: ByteLength,
    ) -> Self {
        Self {
            inner,
            byte_offset,
            byte_length,
        }
    }
}

impl<'a> BytesPartialDecoderTraits for ByteIntervalPartialDecoder<'a> {
    fn partial_decode_opt(
        &self,
        byte_ranges: &[ByteRange],
        parallel: bool,
    ) -> Result<Option<Vec<Vec<u8>>>, CodecError> {
        let byte_ranges: Vec<ByteRange> = byte_ranges
            .iter()
            .map(|byte_range| match byte_range {
                ByteRange::FromStart(offset, None) => {
                    ByteRange::FromStart(self.byte_offset + offset, Some(self.byte_length))
                }
                ByteRange::FromStart(offset, Some(length)) => {
                    ByteRange::FromStart(self.byte_offset + offset, Some(*length))
                }
                ByteRange::FromEnd(offset, None) => {
                    ByteRange::FromStart(self.byte_offset, Some(self.byte_length - *offset))
                }
                ByteRange::FromEnd(offset, Some(length)) => ByteRange::FromEnd(
                    self.byte_offset + self.byte_length - offset - *length,
                    Some(*length),
                ),
            })
            .collect();
        self.inner.partial_decode_opt(&byte_ranges, parallel)
    }
}

#[cfg(feature = "async")]
/// An asynchronous byte interval partial decoder.
///
/// Modifies byte range requests to a specific byte interval in an inner bytes partial decoder.
pub struct AsyncByteIntervalPartialDecoder<'a> {
    inner: &'a dyn AsyncBytesPartialDecoderTraits,
    byte_offset: ByteOffset,
    byte_length: ByteLength,
}

#[cfg(feature = "async")]
impl<'a> AsyncByteIntervalPartialDecoder<'a> {
    /// Create a new byte interval partial decoder.
    pub fn new(
        inner: &'a dyn AsyncBytesPartialDecoderTraits,
        byte_offset: ByteOffset,
        byte_length: ByteLength,
    ) -> Self {
        Self {
            inner,
            byte_offset,
            byte_length,
        }
    }
}

#[cfg(feature = "async")]
#[async_trait::async_trait]
impl<'a> AsyncBytesPartialDecoderTraits for AsyncByteIntervalPartialDecoder<'a> {
    async fn partial_decode_opt(
        &self,
        byte_ranges: &[ByteRange],
        parallel: bool,
    ) -> Result<Option<Vec<Vec<u8>>>, CodecError> {
        let byte_ranges: Vec<ByteRange> = byte_ranges
            .iter()
            .map(|byte_range| match byte_range {
                ByteRange::FromStart(offset, None) => {
                    ByteRange::FromStart(self.byte_offset + offset, Some(self.byte_length))
                }
                ByteRange::FromStart(offset, Some(length)) => {
                    ByteRange::FromStart(self.byte_offset + offset, Some(*length))
                }
                ByteRange::FromEnd(offset, None) => {
                    ByteRange::FromStart(self.byte_offset, Some(self.byte_length - *offset))
                }
                ByteRange::FromEnd(offset, Some(length)) => ByteRange::FromEnd(
                    self.byte_offset + self.byte_length - offset - *length,
                    Some(*length),
                ),
            })
            .collect();
        self.inner.partial_decode_opt(&byte_ranges, parallel).await
    }
}
