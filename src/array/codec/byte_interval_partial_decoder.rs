use crate::{
    array::BytesRepresentation,
    byte_range::{ByteLength, ByteOffset, ByteRange},
};

use super::{BytesPartialDecoderTraits, CodecError};

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
    fn partial_decode(
        &self,
        decoded_representation: &BytesRepresentation,
        byte_ranges: &[ByteRange],
    ) -> Result<Vec<Vec<u8>>, CodecError> {
        let byte_ranges: Vec<ByteRange> = byte_ranges
            .iter()
            .map(|byte_range| match byte_range {
                ByteRange::All => ByteRange::Interval(self.byte_offset, self.byte_length),
                ByteRange::FromStart(length) => ByteRange::Interval(self.byte_offset, *length),
                ByteRange::FromEnd(length) => {
                    ByteRange::Interval(self.byte_offset + self.byte_length - *length, *length)
                }
                ByteRange::Interval(start, length) => {
                    ByteRange::Interval(self.byte_offset + start, *length)
                }
            })
            .collect();
        self.inner
            .partial_decode(decoded_representation, &byte_ranges)
    }

    fn decode(&self, decoded_representation: &BytesRepresentation) -> Result<Vec<u8>, CodecError> {
        Ok(self
            .partial_decode(decoded_representation, &[ByteRange::All])?
            .remove(0))
    }
}
