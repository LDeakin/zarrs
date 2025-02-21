use std::iter::FusedIterator;

use crate::{
    array::{ravel_indices, ArrayShape},
    indexer::{IncompatibleIndexerAndShapeError, Indexer},
};

use super::IndicesIterator;

/// An iterator over the linearised indices in an array subset.
///
/// Iterates over the last dimension fastest (i.e. C-contiguous order).
/// For example, consider a 4x3 array with linearised element indices
/// ```text
/// 0   1   2
/// 3   4   5
/// 6   7   8
/// 9  10  11
/// ```
/// An iterator with an array subset corresponding to the lower right 2x2 region will produce `[7, 8, 10, 11]`.
pub struct LinearisedIndices {
    indexer: Indexer,
    array_shape: ArrayShape,
}

impl LinearisedIndices {
    /// Create a new linearised indices iterator.
    ///
    /// # Errors
    /// Returns [`IncompatibleIndexerAndShapeError`] if `array_shape` does not encapsulate `indexer`.
    pub fn new(
        indexer: impl Into<Indexer>,
        array_shape: ArrayShape,
    ) -> Result<Self, IncompatibleIndexerAndShapeError> {
        let indexer = indexer.into();
        indexer.is_compatible(&array_shape)?;
        Ok(Self {
            indexer,
            array_shape,
        })
    }

    /// Create a new linearised indices iterator.
    ///
    /// # Safety
    /// `array_shape` must encapsulate `subset`.
    #[must_use]
    pub unsafe fn new_unchecked(indexer: impl Into<Indexer>, array_shape: ArrayShape) -> Self {
        let indexer = indexer.into();
        debug_assert!(indexer.is_compatible(&array_shape).is_ok());
        Self {
            indexer,
            array_shape,
        }
    }

    /// Return the number of indices.
    #[must_use]
    pub fn len(&self) -> usize {
        self.indexer.num_elements_usize()
    }

    /// Returns true if the number of indices is zero.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Create a new serial iterator.
    #[must_use]
    pub fn iter(&self) -> LinearisedIndicesIterator<'_> {
        <&Self as IntoIterator>::into_iter(self)
    }
}

impl<'a> IntoIterator for &'a LinearisedIndices {
    type Item = u64;
    type IntoIter = LinearisedIndicesIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        LinearisedIndicesIterator {
            inner: IndicesIterator::new(&self.indexer),
            array_shape: &self.array_shape,
        }
    }
}

/// Serial linearised indices iterator.
///
/// See [`LinearisedIndices`].
pub struct LinearisedIndicesIterator<'a> {
    inner: IndicesIterator<'a>,
    array_shape: &'a [u64],
}

impl Iterator for LinearisedIndicesIterator<'_> {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .next()
            .map(|indices| ravel_indices(&indices, self.array_shape))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl DoubleEndedIterator for LinearisedIndicesIterator<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner
            .next_back()
            .map(|indices| ravel_indices(&indices, self.array_shape))
    }
}

impl ExactSizeIterator for LinearisedIndicesIterator<'_> {}

impl FusedIterator for LinearisedIndicesIterator<'_> {}

#[cfg(test)]
mod tests {
    use crate::array_subset::ArraySubset;

    use super::*;

    #[test]
    fn linearised_indices_iterator_partial() {
        let indices =
            LinearisedIndices::new(ArraySubset::new_with_ranges(&[1..3, 5..7]), vec![8, 8])
                .unwrap();
        assert_eq!(indices.len(), 4);
        let mut iter = indices.iter();
        assert_eq!(iter.next(), Some(13)); // [1,5]
        assert_eq!(iter.next(), Some(14)); // [1,6]
        assert_eq!(iter.next_back(), Some(22)); // [2,6]
        assert_eq!(iter.next(), Some(21)); // [2,5]
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn linearised_indices_iterator_oob() {
        assert!(
            LinearisedIndices::new(ArraySubset::new_with_ranges(&[1..3, 5..7]), vec![1, 1])
                .is_err()
        );
    }

    #[test]
    fn linearised_indices_iterator_empty() {
        let indices =
            LinearisedIndices::new(ArraySubset::new_with_ranges(&[1..1, 5..5]), vec![5, 5])
                .unwrap();
        assert_eq!(indices.len(), 0);
        assert!(indices.is_empty());
    }
}
