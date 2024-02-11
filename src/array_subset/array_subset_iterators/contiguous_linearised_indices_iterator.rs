use std::iter::FusedIterator;

use crate::{
    array::ravel_indices,
    array_subset::{ArraySubset, IncompatibleArraySubsetAndShapeError},
};

use super::ContiguousIndicesIterator;

/// Iterates over contiguous linearised element indices in an array subset.
///
/// The iterator item is a tuple: (linearised index, # contiguous elements).
pub struct ContiguousLinearisedIndicesIterator<'a> {
    inner: ContiguousIndicesIterator,
    array_shape: &'a [u64],
}

impl<'a> ContiguousLinearisedIndicesIterator<'a> {
    /// Return a new contiguous linearised indices iterator.
    ///
    /// # Errors
    ///
    /// Returns [`IncompatibleArraySubsetAndShapeError`] if `array_shape` does not encapsulate `subset`.
    pub fn new(
        subset: &ArraySubset,
        array_shape: &'a [u64],
    ) -> Result<Self, IncompatibleArraySubsetAndShapeError> {
        let inner = subset.iter_contiguous_indices(array_shape)?;
        Ok(Self { inner, array_shape })
    }

    /// Return a new contiguous linearised indices iterator.
    ///
    /// # Safety
    ///
    /// `array_shape` must encapsulate `subset`.
    #[must_use]
    pub unsafe fn new_unchecked(subset: &ArraySubset, array_shape: &'a [u64]) -> Self {
        let inner = subset.iter_contiguous_indices_unchecked(array_shape);
        Self { inner, array_shape }
    }

    /// Return the number of contiguous elements (fixed on each iteration).
    #[must_use]
    pub fn contiguous_elements(&self) -> u64 {
        self.inner.contiguous_elements()
    }
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

impl ExactSizeIterator for ContiguousLinearisedIndicesIterator<'_> {}

impl FusedIterator for ContiguousLinearisedIndicesIterator<'_> {}
