use std::iter::FusedIterator;

use itertools::izip;

use crate::array_subset::{ArraySubset, IncompatibleArraySubsetAndShapeError};

/// Iterates over linearised element indices of an array subset in an array.
pub struct LinearisedIndicesIterator<'a> {
    subset: ArraySubset,
    index: u64,
    array_shape: &'a [u64],
}

impl<'a> LinearisedIndicesIterator<'a> {
    /// Create a new linearised indices iterator.
    ///
    /// # Errors
    ///
    /// Returns [`IncompatibleArraySubsetAndShapeError`] if `array_shape` does not encapsulate `subset`.
    pub fn new(
        subset: ArraySubset,
        array_shape: &'a [u64],
    ) -> Result<Self, IncompatibleArraySubsetAndShapeError> {
        if subset.dimensionality() == array_shape.len()
            && std::iter::zip(subset.end_exc(), array_shape).all(|(end, shape)| end <= *shape)
        {
            Ok(Self {
                subset,
                index: 0,
                array_shape,
            })
        } else {
            Err(IncompatibleArraySubsetAndShapeError(
                subset,
                array_shape.to_vec(),
            ))
        }
    }

    /// Create a new linearised indices iterator.
    ///
    /// # Safety
    ///
    /// `array_shape` must encapsulate `subset`.
    #[must_use]
    pub unsafe fn new_unchecked(subset: ArraySubset, array_shape: &'a [u64]) -> Self {
        debug_assert_eq!(subset.dimensionality(), array_shape.len());
        debug_assert!(
            std::iter::zip(subset.end_exc(), array_shape).all(|(end, shape)| end <= *shape)
        );
        Self {
            subset,
            index: 0,
            array_shape,
        }
    }
}

impl Iterator for LinearisedIndicesIterator<'_> {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        let mut current = self.index;
        let mut out = 0;
        let mut mult = 1;
        for (&subset_start, &subset_size, &array_size) in izip!(
            self.subset.start.iter().rev(),
            self.subset.shape.iter().rev(),
            self.array_shape.iter().rev()
        ) {
            let index = current % subset_size + subset_start;
            current /= subset_size;
            out += index * mult;
            mult *= array_size;
        }
        if current == 0 {
            self.index += 1;
            Some(out)
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let num_elements = self.subset.num_elements_usize();
        (num_elements, Some(num_elements))
    }
}

impl ExactSizeIterator for LinearisedIndicesIterator<'_> {}

impl FusedIterator for LinearisedIndicesIterator<'_> {}
