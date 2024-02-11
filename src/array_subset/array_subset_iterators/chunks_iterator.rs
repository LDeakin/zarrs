use std::{iter::FusedIterator, num::NonZeroU64};

use crate::{
    array::{chunk_shape_to_array_shape, ArrayIndices},
    array_subset::{ArraySubset, IncompatibleDimensionalityError},
};

use super::IndicesIterator;

/// Iterates over the regular sized chunks overlapping this array subset.
/// All chunks have the same size, and may extend over the bounds of the array subset.
///
/// The iterator item is a ([`ArrayIndices`], [`ArraySubset`]) tuple corresponding to the chunk indices and array subset.
pub struct ChunksIterator {
    inner: IndicesIterator,
    chunk_shape: Vec<u64>,
}

impl ChunksIterator {
    /// Create a new chunks iterator.
    ///
    /// # Errors
    ///
    /// Returns [`IncompatibleDimensionalityError`] if `chunk_shape` does not match the dimensionality of `subset`.
    pub fn new(
        subset: &ArraySubset,
        chunk_shape: &[NonZeroU64],
    ) -> Result<Self, IncompatibleDimensionalityError> {
        if subset.dimensionality() == chunk_shape.len() {
            Ok(unsafe { Self::new_unchecked(subset, chunk_shape) })
        } else {
            Err(IncompatibleDimensionalityError(
                chunk_shape.len(),
                subset.dimensionality(),
            ))
        }
    }

    /// Create a new chunks iterator.
    ///
    /// # Safety
    ///
    /// The dimensionality of `chunk_shape` must match the dimensionality of `subset`.
    #[must_use]
    pub unsafe fn new_unchecked(subset: &ArraySubset, chunk_shape: &[NonZeroU64]) -> Self {
        debug_assert_eq!(subset.dimensionality(), chunk_shape.len());
        let chunk_shape = chunk_shape_to_array_shape(chunk_shape);
        let chunk_start: ArrayIndices = std::iter::zip(subset.start(), &chunk_shape)
            .map(|(s, c)| s / c)
            .collect();
        let chunk_end_inc: ArrayIndices = std::iter::zip(subset.end_inc(), &chunk_shape)
            .map(|(e, c)| e / c)
            .collect();
        let subset_chunks =
            unsafe { ArraySubset::new_with_start_end_inc_unchecked(chunk_start, chunk_end_inc) };
        let inner = IndicesIterator::new(subset_chunks);
        Self { inner, chunk_shape }
    }
}

impl Iterator for ChunksIterator {
    type Item = (ArrayIndices, ArraySubset);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|chunk_indices| {
            let start = std::iter::zip(&chunk_indices, &self.chunk_shape)
                .map(|(i, c)| i * c)
                .collect();
            let chunk_subset = unsafe {
                ArraySubset::new_with_start_shape_unchecked(start, self.chunk_shape.clone())
            };
            (chunk_indices, chunk_subset)
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl ExactSizeIterator for ChunksIterator {}

impl FusedIterator for ChunksIterator {}
