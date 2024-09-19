use std::sync::Arc;

use crate::{
    array::RawBytes,
    byte_range::{ByteLength, ByteOffset, ByteRange},
};

use super::{BytesPartialDecoderTraits, CodecError, CodecOptions};

#[cfg(feature = "async")]
use super::AsyncBytesPartialDecoderTraits;

/// A partial decoder for a byte interval of a [`BytesPartialDecoderTraits`] partial decoder.
///
/// Modifies byte range requests to a specific byte interval in an inner bytes partial decoder.
pub struct ByteIntervalPartialDecoder {
    inner: Arc<dyn BytesPartialDecoderTraits>,
    byte_offset: ByteOffset,
    byte_length: ByteLength,
}

impl ByteIntervalPartialDecoder {
    /// Create a new byte interval partial decoder.
    pub fn new(
        inner: Arc<dyn BytesPartialDecoderTraits>,
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

impl BytesPartialDecoderTraits for ByteIntervalPartialDecoder {
    fn partial_decode(
        &self,
        byte_ranges: &[ByteRange],
        options: &CodecOptions,
    ) -> Result<Option<Vec<RawBytes<'_>>>, CodecError> {
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
        self.inner.partial_decode(&byte_ranges, options)
    }
}

#[cfg(feature = "async")]
/// A partial decoder for a byte interval of a [`AsyncBytesPartialDecoderTraits`] partial decoder.
///
/// Modifies byte range requests to a specific byte interval in an inner bytes partial decoder.
pub struct AsyncByteIntervalPartialDecoder {
    inner: Arc<dyn AsyncBytesPartialDecoderTraits>,
    byte_offset: ByteOffset,
    byte_length: ByteLength,
}

#[cfg(feature = "async")]
impl AsyncByteIntervalPartialDecoder {
    /// Create a new byte interval partial decoder.
    pub fn new(
        inner: Arc<dyn AsyncBytesPartialDecoderTraits>,
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
impl AsyncBytesPartialDecoderTraits for AsyncByteIntervalPartialDecoder {
    async fn partial_decode(
        &self,
        byte_ranges: &[ByteRange],
        options: &CodecOptions,
    ) -> Result<Option<Vec<RawBytes<'_>>>, CodecError> {
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
        self.inner.partial_decode(&byte_ranges, options).await
    }
}
