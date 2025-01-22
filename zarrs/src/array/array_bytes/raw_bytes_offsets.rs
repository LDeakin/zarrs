use std::{borrow::Cow, ops::Deref};

use derive_more::derive::Display;
use thiserror::Error;

/// Array element byte offsets.
///
/// These must be monotonically increasing. See [`ArrayBytes::Variable`](crate::array::ArrayBytes::Variable).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RawBytesOffsets<'a>(Cow<'a, [usize]>);

impl Deref for RawBytesOffsets<'_> {
    type Target = [usize];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// An error creating [`RawBytesOffsets`].
///
/// This error occurs when the offsets are not monotonically increasing.
#[derive(Debug, Error, Display)]
pub struct RawBytesOffsetsCreateError;

impl<'a> RawBytesOffsets<'a> {
    /// Creates a new `RawBytesOffsets`.
    ///
    /// # Errors
    /// Returns an error if the offsets are not monotonically increasing.
    pub fn new(offsets: impl Into<Cow<'a, [usize]>>) -> Result<Self, RawBytesOffsetsCreateError> {
        let offsets = offsets.into();
        if offsets.windows(2).all(|w| w[1] >= w[0]) {
            Ok(Self(offsets))
        } else {
            Err(RawBytesOffsetsCreateError)
        }
    }

    /// Creates a new `RawBytesOffsets` without checking the offsets.
    ///
    /// # Safety
    /// The offsets must be monotonically increasing.
    #[must_use]
    pub unsafe fn new_unchecked(offsets: impl Into<Cow<'a, [usize]>>) -> Self {
        let offsets = offsets.into();
        debug_assert!(offsets.windows(2).all(|w| w[1] >= w[0]));
        Self(offsets)
    }

    /// Clones the offsets if not already owned.
    #[must_use]
    pub fn into_owned(self) -> RawBytesOffsets<'static> {
        RawBytesOffsets(self.0.into_owned().into())
    }
}

impl<'a> TryFrom<Cow<'a, [usize]>> for RawBytesOffsets<'a> {
    type Error = RawBytesOffsetsCreateError;

    fn try_from(value: Cow<'a, [usize]>) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl<'a> TryFrom<&'a [usize]> for RawBytesOffsets<'a> {
    type Error = RawBytesOffsetsCreateError;

    fn try_from(value: &'a [usize]) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl<'a, const N: usize> TryFrom<&'a [usize; N]> for RawBytesOffsets<'a> {
    type Error = RawBytesOffsetsCreateError;

    fn try_from(value: &'a [usize; N]) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<Vec<usize>> for RawBytesOffsets<'_> {
    type Error = RawBytesOffsetsCreateError;

    fn try_from(value: Vec<usize>) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raw_bytes_offsets() {
        let offsets = RawBytesOffsets::new(vec![0, 1, 2, 3]).unwrap();
        assert_eq!(&*offsets, &[0, 1, 2, 3]);
        assert!(RawBytesOffsets::new(vec![0, 1, 1]).is_ok());
        assert!(RawBytesOffsets::new(vec![0, 1, 0]).is_err());
        assert!(RawBytesOffsets::try_from(vec![0, 1, 2]).is_ok());
        assert!(RawBytesOffsets::try_from(vec![0, 1, 0]).is_err());
        assert!(RawBytesOffsets::try_from([0, 1, 2].as_slice()).is_ok());
        assert!(RawBytesOffsets::try_from([0, 1, 0].as_slice()).is_err());
        assert!(RawBytesOffsets::try_from(&[0, 1, 2]).is_ok());
        assert!(RawBytesOffsets::try_from(&[0, 1, 0]).is_err());
        assert!(RawBytesOffsets::try_from(Cow::Owned(vec![0, 1, 0])).is_err());
    }
}
