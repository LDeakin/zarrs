use std::iter::FusedIterator;

use itertools::izip;

use crate::{
    array::ArrayIndices,
    array_subset::{ArraySubset, IncompatibleArraySubsetAndShapeError},
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
    subset_contiguous_start: ArraySubset,
    contiguous_elements: u64,
}

impl ContiguousIndices {
    /// Create a new contiguous indices iterator.
    ///
    /// # Errors
    /// Returns [`IncompatibleArraySubsetAndShapeError`] if `array_shape` does not encapsulate `subset`.
    pub fn new(
        subset: &ArraySubset,
        array_shape: &[u64],
    ) -> Result<Self, IncompatibleArraySubsetAndShapeError> {
        if subset.dimensionality() == array_shape.len()
            && std::iter::zip(subset.end_exc(), array_shape).all(|(end, shape)| end <= *shape)
        {
            Ok(unsafe { Self::new_unchecked(subset, array_shape) })
        } else {
            Err(IncompatibleArraySubsetAndShapeError(
                subset.clone(),
                array_shape.to_vec(),
            ))
        }
    }

    /// Create a new contiguous indices iterator.
    ///
    /// # Safety
    /// `array_shape` must encapsulate `subset`.
    #[must_use]
    #[allow(clippy::missing_panics_doc)]
    pub unsafe fn new_unchecked(subset: &ArraySubset, array_shape: &[u64]) -> Self {
        debug_assert_eq!(subset.dimensionality(), array_shape.len());
        debug_assert!(
            std::iter::zip(subset.end_exc(), array_shape).all(|(end, shape)| end <= *shape)
        );

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
        unsafe { shape_out.set_len(array_shape.len()) };
        let subset_contiguous_start =
            ArraySubset::new_with_start_shape_unchecked(subset.start().to_vec(), shape_out);
        // let inner = subset_contiguous_start.iter_indices();
        Self {
            subset_contiguous_start,
            contiguous_elements,
        }
    }

    /// Return the number of starting indices (i.e. the length of the iterator).
    #[must_use]
    pub fn len(&self) -> usize {
        self.subset_contiguous_start.num_elements_usize()
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
    type Item = (ArrayIndices, u64);
    type IntoIter = ContiguousIndicesIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        ContiguousIndicesIterator {
            inner: IndicesIterator::new(&self.subset_contiguous_start),
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

impl Iterator for ContiguousIndicesIterator<'_> {
    type Item = (ArrayIndices, u64);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .next()
            .map(|indices| (indices, self.contiguous_elements))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl DoubleEndedIterator for ContiguousIndicesIterator<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner
            .next_back()
            .map(|indices| (indices, self.contiguous_elements))
    }
}

impl ExactSizeIterator for ContiguousIndicesIterator<'_> {}

impl FusedIterator for ContiguousIndicesIterator<'_> {}
