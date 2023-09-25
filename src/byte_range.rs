//! Byte ranges.
//!
//! A [`ByteRange`] can be all, an interval, or a length from the start of end of a byte sequence.
//!
//! This module provides the [`extract_byte_ranges`] convenience function for extracting byte ranges from bytes.
//!

use thiserror::Error;

/// A byte offset.
pub type ByteOffset = usize;

/// A byte length.
pub type ByteLength = usize;

/// A byte range.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ByteRange {
    /// All bytes.
    All,
    /// A byte interval.
    Interval(ByteOffset, ByteLength),
    /// A length of bytes from the start.
    FromStart(ByteLength),
    /// A length of bytes from the end.
    FromEnd(ByteLength),
}

impl ByteRange {
    /// Return the start of a byte range. `size` is the size of the entire bytes.
    #[must_use]
    pub fn start(&self, size: usize) -> usize {
        match self {
            ByteRange::All => 0,
            ByteRange::FromStart(_offset) => 0,
            ByteRange::FromEnd(length) => size - *length,
            ByteRange::Interval(start, _length) => *start,
        }
    }

    /// Return the exclusive end of a byte range. `size` is the size of the entire bytes.
    #[must_use]
    pub fn end(&self, size: usize) -> usize {
        match self {
            ByteRange::All => size,
            ByteRange::FromStart(offset) => *offset,
            ByteRange::FromEnd(_length) => size,
            ByteRange::Interval(start, length) => start + length,
        }
    }

    /// Return the length of a byte range. `size` is the size of the entire bytes.
    #[must_use]
    pub fn length(&self, size: usize) -> usize {
        match self {
            ByteRange::All => size,
            ByteRange::FromStart(length) | ByteRange::FromEnd(length) => *length,
            ByteRange::Interval(_start, length) => *length,
        }
    }
}

/// An invalid byte range error.
#[derive(Copy, Clone, Debug, Error)]
#[error("invalid byte range")]
pub struct InvalidByteRangeError;

fn validate_byte_ranges(byte_ranges: &[ByteRange], bytes_len: usize) -> bool {
    for byte_range in byte_ranges {
        let valid = match byte_range {
            ByteRange::All => true,
            ByteRange::FromStart(length) | ByteRange::FromEnd(length) => *length <= bytes_len,
            ByteRange::Interval(offset, length) => offset + length <= bytes_len,
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
/// Returns [`InvalidByteRangeError`] if any byte range is invalid.
pub fn extract_byte_ranges(
    bytes: &[u8],
    byte_ranges: &[ByteRange],
) -> Result<Vec<Vec<u8>>, InvalidByteRangeError> {
    if !validate_byte_ranges(byte_ranges, bytes.len()) {
        return Err(InvalidByteRangeError);
    }
    Ok(unsafe { extract_byte_ranges_unchecked(bytes, byte_ranges) })
}

/// Extract byte ranges from bytes.
///
/// # Safety
///
/// All byte ranges in `byte_ranges` must specify a range within `bytes`.
#[doc(hidden)]
#[must_use]
pub unsafe fn extract_byte_ranges_unchecked(
    bytes: &[u8],
    byte_ranges: &[ByteRange],
) -> Vec<Vec<u8>> {
    let mut out = Vec::with_capacity(byte_ranges.len());
    for byte_range in byte_ranges {
        out.push(
            match byte_range {
                ByteRange::All => bytes,
                ByteRange::FromStart(length) => &bytes[0..*length],
                ByteRange::FromEnd(length) => &bytes[bytes.len() - length..],
                ByteRange::Interval(offset, length) => &bytes[*offset..offset + length],
            }
            .to_vec(),
        );
    }
    out
}
