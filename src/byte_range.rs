//! Byte ranges.
//!
//! A [`ByteRange`] represents a byte range relative to the start or end of a byte sequence.
//! A byte range has an offset and optional length, which if omitted means to read all remaining bytes.
//!
//! A [codec](crate::array::codec) partially decoding from bytes will retrieve byte ranges from an input handle implementing [`BytesPartialDecoderTraits`](crate::array::codec::BytesPartialDecoderTraits) which can be either:
//! - a [store](crate::storage::store) or [storage transformer](crate::storage::storage_transformer) wrapped by [`StoragePartialDecoder`](crate::array::codec::StoragePartialDecoder), or
//! - the bytes partial decoder of the next codec in the [`CodecChain`](crate::array::codec::CodecChain).
//!
//! [`extract_byte_ranges`] is a convenience function for extracting byte ranges from a slice of bytes.

use std::ops::Range;

use thiserror::Error;

use crate::array::UnsafeCellSlice;

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
            Self::FromStart(offset, _) => *offset,
            Self::FromEnd(offset, length) => {
                length.as_ref().map_or(0, |length| size - *offset - *length)
            }
        }
    }

    /// Return the exclusive end of a byte range. `size` is the size of the entire bytes.
    #[must_use]
    pub fn end(&self, size: u64) -> u64 {
        match self {
            Self::FromStart(offset, length) => {
                length.as_ref().map_or(size, |length| offset + length)
            }
            Self::FromEnd(offset, _) => size - offset,
        }
    }

    /// Return the internal offset of the byte range (which can be at its start or end).
    #[must_use]
    pub const fn offset(&self) -> u64 {
        let (Self::FromStart(offset, _) | Self::FromEnd(offset, _)) = self;
        *offset
    }

    /// Return the length of a byte range. `size` is the size of the entire bytes.
    #[must_use]
    pub fn length(&self, size: u64) -> u64 {
        match self {
            Self::FromStart(offset, None) | Self::FromEnd(offset, None) => size - offset,
            Self::FromStart(_, Some(length)) | Self::FromEnd(_, Some(length)) => *length,
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

impl std::fmt::Display for ByteRange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        match self {
            Self::FromStart(offset, length) => write!(
                f,
                "{}..{}",
                if offset == &0 {
                    String::new()
                } else {
                    offset.to_string()
                },
                length.map_or(String::new(), |length| (offset + length).to_string())
            ),
            Self::FromEnd(offset, length) => write!(
                f,
                "{}..{}",
                length.map_or(String::new(), |length| format!("-{}", offset + length)),
                if offset == &0 {
                    String::new()
                } else {
                    format!("-{offset}")
                }
            ),
        }
    }
}

/// An invalid byte range error.
#[derive(Copy, Clone, Debug, Error)]
#[error("invalid byte range {0} for bytes of length {1}")]
pub struct InvalidByteRangeError(ByteRange, u64);

impl InvalidByteRangeError {
    /// Create a new [`InvalidByteRangeError`].
    #[must_use]
    pub fn new(byte_range: ByteRange, bytes_len: u64) -> Self {
        Self(byte_range, bytes_len)
    }
}

fn validate_byte_ranges(
    byte_ranges: &[ByteRange],
    bytes_len: u64,
) -> Result<(), InvalidByteRangeError> {
    for byte_range in byte_ranges {
        let valid = match byte_range {
            ByteRange::FromStart(offset, length) | ByteRange::FromEnd(offset, length) => {
                offset + length.unwrap_or(0) <= bytes_len
            }
        };
        if !valid {
            return Err(InvalidByteRangeError(*byte_range, bytes_len));
        }
    }
    Ok(())
}

/// Extract byte ranges from bytes.
///
/// # Errors
/// Returns [`InvalidByteRangeError`] if any bytes are requested beyond the end of `bytes`.
pub fn extract_byte_ranges(
    bytes: &[u8],
    byte_ranges: &[ByteRange],
) -> Result<Vec<Vec<u8>>, InvalidByteRangeError> {
    validate_byte_ranges(byte_ranges, bytes.len() as u64)?;
    Ok(unsafe { extract_byte_ranges_unchecked(bytes, byte_ranges) })
}

/// Extract byte ranges from bytes.
///
/// # Safety
/// All byte ranges in `byte_ranges` must specify a range within `bytes`.
///
/// # Panics
/// Panics if attempting to reference a byte beyond `usize::MAX`.
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

/// Extract byte ranges from bytes and concatenate.
///
/// # Errors
/// Returns [`InvalidByteRangeError`] if any bytes are requested beyond the end of `bytes`.
pub fn extract_byte_ranges_concat(
    bytes: &[u8],
    byte_ranges: &[ByteRange],
) -> Result<Vec<u8>, InvalidByteRangeError> {
    validate_byte_ranges(byte_ranges, bytes.len() as u64)?;
    Ok(unsafe { extract_byte_ranges_concat_unchecked(bytes, byte_ranges) })
}

/// Extract byte ranges from bytes and concatenate.
///
/// # Safety
/// All byte ranges in `byte_ranges` must specify a range within `bytes`.
///
/// # Panics
/// Panics if attempting to reference a byte beyond `usize::MAX`.
#[must_use]
pub unsafe fn extract_byte_ranges_concat_unchecked(
    bytes: &[u8],
    byte_ranges: &[ByteRange],
) -> Vec<u8> {
    let out_size = usize::try_from(
        byte_ranges
            .iter()
            .map(|byte_range| byte_range.length(bytes.len() as u64))
            .sum::<u64>(),
    )
    .unwrap();
    let mut out = Vec::with_capacity(out_size);
    let out_slice = UnsafeCellSlice::new_from_vec_with_spare_capacity(&mut out);
    let out_slice = out_slice.get();
    let mut offset: usize = 0;
    for byte_range in byte_ranges {
        let start = usize::try_from(byte_range.start(bytes.len() as u64)).unwrap();
        let byte_range_len = usize::try_from(byte_range.length(bytes.len() as u64)).unwrap();
        out_slice[offset..offset + byte_range_len]
            .copy_from_slice(&bytes[start..start + byte_range_len]);
        offset += byte_range_len;
    }
    out.set_len(out_size);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn byte_ranges() {
        let byte_range = ByteRange::FromStart(1, None);
        assert_eq!(byte_range.to_range(10), 1..10);
        assert_eq!(byte_range.length(10), 9);
        assert_eq!(byte_range.offset(), 1);

        let byte_range = ByteRange::FromEnd(1, None);
        assert_eq!(byte_range.to_range(10), 0..9);
        assert_eq!(byte_range.length(10), 9);
        assert_eq!(byte_range.offset(), 1);

        let byte_range = ByteRange::FromStart(1, Some(5));
        assert_eq!(byte_range.to_range(10), 1..6);
        assert_eq!(byte_range.to_range_usize(10), 1..6);
        assert_eq!(byte_range.length(10), 5);

        assert!(validate_byte_ranges(&[ByteRange::FromStart(1, Some(5))], 6).is_ok());
        assert!(validate_byte_ranges(&[ByteRange::FromStart(1, Some(5))], 2).is_err());

        assert!(validate_byte_ranges(&[ByteRange::FromEnd(1, Some(5))], 6).is_ok());
        assert!(validate_byte_ranges(&[ByteRange::FromEnd(1, Some(5))], 2).is_err());

        assert!(extract_byte_ranges(&[1, 2, 3], &[ByteRange::FromStart(1, Some(2))]).is_ok());
        let bytes = extract_byte_ranges(&[1, 2, 3], &[ByteRange::FromStart(1, Some(4))]);
        assert!(bytes.is_err());
        assert_eq!(
            bytes.unwrap_err().to_string(),
            "invalid byte range 1..5 for bytes of length 3"
        );
    }

    #[test]
    fn byte_range_display() {
        assert_eq!(format!("{}", ByteRange::FromStart(0, None)), "..");
        assert_eq!(format!("{}", ByteRange::FromStart(5, None)), "5..");
        assert_eq!(format!("{}", ByteRange::FromStart(5, Some(2))), "5..7");
        assert_eq!(format!("{}", ByteRange::FromEnd(5, None)), "..-5");
        assert_eq!(format!("{}", ByteRange::FromEnd(0, Some(2))), "-2..");
        assert_eq!(format!("{}", ByteRange::FromEnd(5, Some(2))), "-7..-5");
    }
}
