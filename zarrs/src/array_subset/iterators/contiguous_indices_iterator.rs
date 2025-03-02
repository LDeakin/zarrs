use std::iter::FusedIterator;

use itertools::izip;

use crate::{
    array::ArrayIndices,
    array_subset::{ArraySubset, IncompatibleIndexerAndShapeError},
    indexer::Indexer,
};

use super::IndicesIterator;

/// Iterates over contiguous element indices in an array subset.
///
/// The iterator item is a tuple: (indices, # contiguous elements).
///
/// Iterates over the last dimension fastest (i.e. C-contiguous order).
/// For example, consider a 4x3 array with element indices
/// ```text
/// (0, 0)  (0, 1)  (0, 2)
/// (1, 0)  (1, 1)  (1, 2)
/// (2, 0)  (2, 1)  (2, 2)
/// (3, 0)  (3, 1)  (3, 2)
/// ```
/// An iterator with an array subset covering the entire array will produce
/// ```rust,ignore
/// [((0, 0), 9)]
/// ```
/// An iterator with an array subset corresponding to the lower right 2x2 region will produce
/// ```rust,ignore
/// [((2, 1), 2), ((3, 1), 2)]
/// ```
pub struct ContiguousIndices {
    indexer_contiguous_start: Indexer,
    contiguous_elements: u64,
}

impl ContiguousIndices {
    /// Create a new contiguous indices iterator.
    ///
    /// # Errors
    /// Returns [`IncompatibleIndexerAndShapeError`] if `array_shape` does not encapsulate `subset`.
    pub fn new(
        indexer: impl Into<Indexer>,
        array_shape: &[u64],
    ) -> Result<Self, IncompatibleIndexerAndShapeError> {
        let indexer = indexer.into();
        indexer.is_compatible(array_shape)?;
        Ok(unsafe { Self::new_unchecked(indexer, array_shape) })
    }

    /// Create a new contiguous indices iterator.
    ///
    /// # Safety
    /// `array_shape` must encapsulate `subset`.
    #[must_use]
    #[allow(clippy::missing_panics_doc)]
    pub unsafe fn new_unchecked(indexer: impl Into<Indexer>, array_shape: &[u64]) -> Self {
        let indexer = indexer.into();
        debug_assert!(indexer.is_compatible(array_shape).is_ok());

        match indexer {
            Indexer::Subset(subset) => {
                let mut contiguous = true;
                let mut contiguous_elements = 1;
                let mut shape_out: Vec<u64> = Vec::with_capacity(array_shape.len());
                for (&subset_start, &subset_size, &array_size, shape_out_i) in izip!(
                    subset.start().iter().rev(),
                    subset.shape().iter().rev(),
                    array_shape.iter().rev(),
                    shape_out.spare_capacity_mut().iter_mut().rev(),
                ) {
                    if contiguous {
                        contiguous_elements *= subset_size;
                        shape_out_i.write(1);
                        contiguous = subset_start == 0 && subset_size == array_size;
                    } else {
                        shape_out_i.write(subset_size);
                    }
                }
                // SAFETY: each element is initialised
                unsafe { shape_out.set_len(array_shape.len()) };
                // SAFETY: The length of shape_out matches the subset dimensionality
                let subset_contiguous_start = unsafe {
                    ArraySubset::new_with_start_shape_unchecked(subset.start().to_vec(), shape_out)
                };
                Self {
                    indexer_contiguous_start: Indexer::Subset(subset_contiguous_start),
                    contiguous_elements,
                }
            }
            Indexer::VIndex(vindices) => {
                // TODO: integer indexing vindices could have contiguous elements, worth checking?
                Self {
                    indexer_contiguous_start: Indexer::VIndex(vindices),
                    contiguous_elements: 1,
                }
            }
            Indexer::OIndex(oindices) => {
                // TODO: integer indexing oindices could have contiguous elements, worth checking?
                Self {
                    indexer_contiguous_start: Indexer::OIndex(oindices),
                    contiguous_elements: 1,
                }
            }
            Indexer::Mixed(mindices) => {
                // TODO: integer indexing mindices could have contiguous elements, worth checking?
                Self {
                    indexer_contiguous_start: Indexer::Mixed(mindices),
                    contiguous_elements: 1,
                }
            }
        }
    }

    /// Return the number of starting indices (i.e. the length of the iterator).
    #[must_use]
    pub fn len(&self) -> usize {
        self.indexer_contiguous_start.num_elements_usize()
    }

    /// Returns true if the number of starting indices is zero.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Return the number of contiguous elements (fixed on each iteration).
    #[must_use]
    pub fn contiguous_elements(&self) -> u64 {
        self.contiguous_elements
    }

    /// Return the number of contiguous elements (fixed on each iteration).
    ///
    /// # Panics
    /// Panics if the number of contiguous elements exceeds [`usize::MAX`].
    #[must_use]
    pub fn contiguous_elements_usize(&self) -> usize {
        usize::try_from(self.contiguous_elements).unwrap()
    }

    /// Create a new serial iterator.
    #[must_use]
    pub fn iter(&self) -> ContiguousIndicesIterator<'_> {
        <&Self as IntoIterator>::into_iter(self)
    }
}

impl<'a> IntoIterator for &'a ContiguousIndices {
    type Item = ArrayIndices;
    type IntoIter = ContiguousIndicesIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        ContiguousIndicesIterator {
            inner: IndicesIterator::new(&self.indexer_contiguous_start),
            contiguous_elements: self.contiguous_elements,
        }
    }
}

/// Serial contiguous indices iterator.
///
/// See [`ContiguousIndices`].
pub struct ContiguousIndicesIterator<'a> {
    inner: IndicesIterator<'a>,
    contiguous_elements: u64,
}

impl ContiguousIndicesIterator<'_> {
    /// Return the number of contiguous elements (fixed on each iteration).
    #[must_use]
    pub fn contiguous_elements(&self) -> u64 {
        self.contiguous_elements
    }

    /// Return the number of contiguous elements (fixed on each iteration).
    ///
    /// # Panics
    /// Panics if the number of contiguous elements exceeds [`usize::MAX`].
    #[must_use]
    pub fn contiguous_elements_usize(&self) -> usize {
        usize::try_from(self.contiguous_elements).unwrap()
    }
}

impl Iterator for ContiguousIndicesIterator<'_> {
    type Item = ArrayIndices;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl DoubleEndedIterator for ContiguousIndicesIterator<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner.next_back()
    }
}

impl ExactSizeIterator for ContiguousIndicesIterator<'_> {}

impl FusedIterator for ContiguousIndicesIterator<'_> {}
