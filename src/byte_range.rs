//! Byte ranges.
//!
//! A [`ByteRange`] represents a byte range relative to the start or end of a byte sequence.
//! A byte range has an offset and optional length, which if omitted means to read all remaining bytes.
//!
//! A [codec](crate::array::codec) partially decoding from bytes will retrieve byte ranges from an input handle implementing [`BytesPartialDecoderTraits`](crate::array::codec::BytesPartialDecoderTraits) which can be either:
//! - a [store](crate::storage::store) or [storage transformer](crate::storage::storage_transformer) wrapped by [`StoragePartialDecoder`](crate::array::codec::StoragePartialDecoder), or
//! - the bytes partial decoder of the next codec in the codec chain.
//!
//! This module provides the [`extract_byte_ranges`] convenience function for extracting byte ranges from a slice of bytes.
//!

use std::ops::Range;

use thiserror::Error;

/// A byte offset.
pub type ByteOffset = u64;

/// A byte length.
pub type ByteLength = u64;

/// A byte range.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ByteRange {
    /// A byte range from the start.
    ///
    /// If the byte length is [`None`], reads to the end of the value.
    FromStart(ByteOffset, Option<ByteLength>),
    /// A byte range from the end.
    ///
    /// If the byte length is [`None`], reads to the start of the value.
    FromEnd(ByteOffset, Option<ByteLength>),
}

impl ByteRange {
    /// Return the start of a byte range. `size` is the size of the entire bytes.
    #[must_use]
    pub fn start(&self, size: u64) -> u64 {
        match self {
            ByteRange::FromStart(offset, _) => *offset,
            ByteRange::FromEnd(offset, length) => match length {
                Some(length) => size - *offset - *length,
                None => 0,
            },
        }
    }

    /// Return the exclusive end of a byte range. `size` is the size of the entire bytes.
    #[must_use]
    pub fn end(&self, size: u64) -> u64 {
        match self {
            ByteRange::FromStart(offset, length) => match length {
                Some(length) => offset + length,
                None => size,
            },
            ByteRange::FromEnd(offset, _) => size - offset,
        }
    }

    /// Return the length of a byte range. `size` is the size of the entire bytes.
    #[must_use]
    pub fn length(&self, size: u64) -> u64 {
        match self {
            ByteRange::FromStart(offset, None) | ByteRange::FromEnd(offset, None) => size - offset,
            ByteRange::FromStart(_, Some(length)) | ByteRange::FromEnd(_, Some(length)) => *length,
        }
    }

    /// Convert the byte range to a [`Range<u64>`].
    #[must_use]
    pub fn to_range(&self, size: u64) -> Range<u64> {
        self.start(size)..self.end(size)
    }

    /// Convert the byte range to a [`Range<usize>`].
    ///
    /// # Panics
    ///
    /// Panics if the byte range exceeds [`usize::MAX`].
    #[must_use]
    pub fn to_range_usize(&self, size: u64) -> core::ops::Range<usize> {
        self.start(size).try_into().unwrap()..self.end(size).try_into().unwrap()
    }
}

/// An invalid byte range error.
#[derive(Copy, Clone, Debug, Error)]
#[error("invalid byte range")]
pub struct InvalidByteRangeError;

fn validate_byte_ranges(byte_ranges: &[ByteRange], bytes_len: u64) -> bool {
    for byte_range in byte_ranges {
        let valid = match byte_range {
            ByteRange::FromStart(offset, length) | ByteRange::FromEnd(offset, length) => {
                offset + length.unwrap_or(0) <= bytes_len
            }
        };
        if !valid {
            return false;
        }
    }
    true
}

/// Extract byte ranges from bytes.
///
/// # Errors
///
/// Returns [`InvalidByteRangeError`] if any bytes are requested beyond the end of `bytes`.
pub fn extract_byte_ranges(
    bytes: &[u8],
    byte_ranges: &[ByteRange],
) -> Result<Vec<Vec<u8>>, InvalidByteRangeError> {
    if !validate_byte_ranges(byte_ranges, bytes.len() as u64) {
        return Err(InvalidByteRangeError);
    }
    Ok(unsafe { extract_byte_ranges_unchecked(bytes, byte_ranges) })
}

/// Extract byte ranges from bytes.
///
/// # Safety
///
/// All byte ranges in `byte_ranges` must specify a range within `bytes`.
///
#[doc(hidden)]
#[must_use]
pub unsafe fn extract_byte_ranges_unchecked(
    bytes: &[u8],
    byte_ranges: &[ByteRange],
) -> Vec<Vec<u8>> {
    let mut out = Vec::with_capacity(byte_ranges.len());
    for byte_range in byte_ranges {
        out.push({
            let start = usize::try_from(byte_range.start(bytes.len() as u64)).unwrap();
            let end = usize::try_from(byte_range.end(bytes.len() as u64)).unwrap();
            bytes[start..end].to_vec()
        });
    }
    out
}
