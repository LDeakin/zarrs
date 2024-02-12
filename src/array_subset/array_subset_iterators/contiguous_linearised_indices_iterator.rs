use std::iter::FusedIterator;

use crate::{
    array::ravel_indices,
    array_subset::{ArraySubset, IncompatibleArraySubsetAndShapeError},
};

use super::{contiguous_indices_iterator::ContiguousIndices, ContiguousIndicesIterator};

/// TODO
pub struct ContiguousLinearisedIndices {
    inner: ContiguousIndices,
    array_shape: Vec<u64>,
}

impl ContiguousLinearisedIndices {
    /// Return a new contiguous linearised indices iterator.
    ///
    /// # Errors
    ///
    /// Returns [`IncompatibleArraySubsetAndShapeError`] if `array_shape` does not encapsulate `subset`.
    pub fn new(
        subset: &ArraySubset,
        array_shape: Vec<u64>,
    ) -> Result<Self, IncompatibleArraySubsetAndShapeError> {
        let inner = subset.contiguous_indices(&array_shape)?;
        Ok(Self { inner, array_shape })
    }

    /// Return a new contiguous linearised indices iterator.
    ///
    /// # Safety
    ///
    /// `array_shape` must encapsulate `subset`.
    #[must_use]
    pub unsafe fn new_unchecked(subset: &ArraySubset, array_shape: Vec<u64>) -> Self {
        let inner = subset.contiguous_indices_unchecked(&array_shape);
        Self { inner, array_shape }
    }

    /// Return the number of contiguous elements (fixed on each iteration).
    #[must_use]
    pub fn contiguous_elements(&self) -> u64 {
        self.inner.contiguous_elements()
    }

    /// Create a new serial iterator.
    #[must_use]
    pub fn iter(&self) -> ContiguousLinearisedIndicesIterator<'_> {
        <&Self as IntoIterator>::into_iter(self)
    }
}

impl<'a> IntoIterator for &'a ContiguousLinearisedIndices {
    type Item = (u64, u64);
    type IntoIter = ContiguousLinearisedIndicesIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        ContiguousLinearisedIndicesIterator {
            inner: self.inner.into_iter(),
            array_shape: &self.array_shape,
        }
    }
}

/// Iterates over contiguous linearised element indices in an array subset.
///
/// The iterator item is a tuple: (linearised index, # contiguous elements).
pub struct ContiguousLinearisedIndicesIterator<'a> {
    inner: ContiguousIndicesIterator<'a>,
    array_shape: &'a [u64],
}

impl Iterator for ContiguousLinearisedIndicesIterator<'_> {
    type Item = (u64, u64);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .next()
            .map(|(indices, elements)| (ravel_indices(&indices, self.array_shape), elements))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl DoubleEndedIterator for ContiguousLinearisedIndicesIterator<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner
            .next_back()
            .map(|(indices, elements)| (ravel_indices(&indices, self.array_shape), elements))
    }
}

impl ExactSizeIterator for ContiguousLinearisedIndicesIterator<'_> {}

impl FusedIterator for ContiguousLinearisedIndicesIterator<'_> {}
