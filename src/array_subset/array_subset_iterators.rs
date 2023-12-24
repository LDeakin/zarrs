use std::iter::FusedIterator;

use itertools::izip;

use crate::array::{ravel_indices, ArrayIndices};

use super::{ArraySubset, IncompatibleArrayShapeError, IncompatibleDimensionalityError};

/// Iterates over element indices in an array subset.
pub struct IndicesIterator {
    subset_rev: ArraySubset,
    index: u64,
}

impl IndicesIterator {
    /// Create a new indices iterator.
    #[must_use]
    pub fn new(mut subset: ArraySubset) -> Self {
        subset.start.reverse();
        subset.shape.reverse();
        Self {
            subset_rev: subset,
            index: 0,
        }
    }
}

impl Iterator for IndicesIterator {
    type Item = ArrayIndices;

    fn next(&mut self) -> Option<Self::Item> {
        let mut current = self.index;
        // let mut indices = vec![0u64; self.subset_rev.dimensionality()];
        let mut indices = vec![core::mem::MaybeUninit::uninit(); self.subset_rev.dimensionality()];
        for (out, &subset_start, &subset_size) in izip!(
            indices.iter_mut().rev(),
            self.subset_rev.start.iter(),
            self.subset_rev.shape.iter(),
        ) {
            out.write(current % subset_size + subset_start);
            current /= subset_size;
        }
        if current == 0 {
            self.index += 1;
            #[allow(clippy::transmute_undefined_repr)]
            Some(unsafe { std::mem::transmute(indices) })
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let num_elements = self.subset_rev.num_elements_usize();
        (num_elements, Some(num_elements))
    }
}

impl ExactSizeIterator for IndicesIterator {}

impl FusedIterator for IndicesIterator {}

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
    /// Returns [`IncompatibleArrayShapeError`] if `array_shape` does not encapsulate `subset`.
    pub fn new(
        subset: ArraySubset,
        array_shape: &'a [u64],
    ) -> Result<Self, IncompatibleArrayShapeError> {
        if subset.dimensionality() == array_shape.len()
            && std::iter::zip(subset.end_exc(), array_shape).all(|(end, shape)| end <= *shape)
        {
            Ok(Self {
                subset,
                index: 0,
                array_shape,
            })
        } else {
            Err(IncompatibleArrayShapeError(array_shape.to_vec(), subset))
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

/// Iterates over contiguous element indices in an array subset.
///
/// The iterator item is a tuple: (indices, # contiguous elements).
pub struct ContiguousIndicesIterator {
    inner: IndicesIterator,
    contiguous_elements: u64,
}

impl ContiguousIndicesIterator {
    /// Create a new contiguous indices iterator.
    ///
    /// # Errors
    ///
    /// Returns [`IncompatibleArrayShapeError`] if `array_shape` does not encapsulate `subset`.
    pub fn new(
        subset: &ArraySubset,
        array_shape: &[u64],
    ) -> Result<Self, IncompatibleArrayShapeError> {
        if subset.dimensionality() == array_shape.len()
            && std::iter::zip(subset.end_exc(), array_shape).all(|(end, shape)| end <= *shape)
        {
            Ok(unsafe { Self::new_unchecked(subset, array_shape) })
        } else {
            Err(IncompatibleArrayShapeError(
                array_shape.to_vec(),
                subset.clone(),
            ))
        }
    }

    /// Create a new contiguous indices iterator.
    ///
    /// # Safety
    ///
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
        let mut shape_out = vec![core::mem::MaybeUninit::uninit(); array_shape.len()];
        for (&subset_start, &subset_size, &array_size, shape_out_i) in izip!(
            subset.start().iter().rev(),
            subset.shape().iter().rev(),
            array_shape.iter().rev(),
            shape_out.iter_mut().rev(),
        ) {
            if contiguous {
                contiguous_elements *= subset_size;
                shape_out_i.write(1);
                contiguous = subset_start == 0 && subset_size == array_size;
            } else {
                shape_out_i.write(subset_size);
            }
        }
        #[allow(clippy::transmute_undefined_repr)]
        let shape_out: Vec<u64> = unsafe { core::mem::transmute(shape_out) };
        let subset_contiguous_start =
            ArraySubset::new_with_start_shape_unchecked(subset.start().to_vec(), shape_out);
        let inner = subset_contiguous_start.iter_indices();
        Self {
            inner,
            contiguous_elements,
        }
    }
}

impl Iterator for ContiguousIndicesIterator {
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

impl ExactSizeIterator for ContiguousIndicesIterator {}

impl FusedIterator for ContiguousIndicesIterator {}

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
    /// Returns [`IncompatibleArrayShapeError`] if `array_shape` does not encapsulate `subset`.
    pub fn new(
        subset: &ArraySubset,
        array_shape: &'a [u64],
    ) -> Result<Self, IncompatibleArrayShapeError> {
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

/// Iterates over the regular sized chunks overlapping this array subset.
/// All chunks have the same size, and may extend over the bounds of the array subset.
///
/// The iterator item is a ([`ArrayIndices`], [`ArraySubset`]) tuple corresponding to the chunk indices and array subset.
pub struct ChunksIterator<'a> {
    inner: IndicesIterator,
    chunk_shape: &'a [u64],
}

impl<'a> ChunksIterator<'a> {
    /// Create a new chunks iterator.
    ///
    /// # Errors
    ///
    /// Returns [`IncompatibleDimensionalityError`] if `chunk_shape` does not match the dimensionality of `subset`.
    pub fn new(
        subset: &ArraySubset,
        chunk_shape: &'a [u64],
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
    pub unsafe fn new_unchecked(subset: &ArraySubset, chunk_shape: &'a [u64]) -> Self {
        debug_assert_eq!(subset.dimensionality(), chunk_shape.len());
        let chunk_start: ArrayIndices = std::iter::zip(subset.start(), chunk_shape)
            .map(|(s, c)| s / c)
            .collect();
        let chunk_end_inc: ArrayIndices = std::iter::zip(subset.end_inc(), chunk_shape)
            .map(|(e, c)| e / c)
            .collect();
        let subset_chunks =
            unsafe { ArraySubset::new_with_start_end_inc_unchecked(chunk_start, chunk_end_inc) };
        let inner = IndicesIterator::new(subset_chunks);
        Self { inner, chunk_shape }
    }
}

impl Iterator for ChunksIterator<'_> {
    type Item = (ArrayIndices, ArraySubset);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|chunk_indices| {
            let start = std::iter::zip(&chunk_indices, self.chunk_shape)
                .map(|(i, c)| i * c)
                .collect();
            let shape = self.chunk_shape.to_vec();
            let chunk_subset = unsafe { ArraySubset::new_with_start_shape_unchecked(start, shape) };
            (chunk_indices, chunk_subset)
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl ExactSizeIterator for ChunksIterator<'_> {}

impl FusedIterator for ChunksIterator<'_> {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn array_subset_iter_indices() {
        let subset = ArraySubset::new_with_ranges(&[1..3, 1..3]);
        let mut iter = subset.iter_indices();
        assert_eq!(iter.next(), Some(vec![1, 1]));
        assert_eq!(iter.next(), Some(vec![1, 2]));
        assert_eq!(iter.next(), Some(vec![2, 1]));
        assert_eq!(iter.next(), Some(vec![2, 2]));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn array_subset_iter_linearised_indices() {
        let subset = ArraySubset::new_with_ranges(&[1..3, 1..3]);
        let mut iter = subset.iter_linearised_indices(&[4, 4]).unwrap();
        //  0  1  2  3
        //  4  5  6  7
        //  8  9 10 11
        // 12 13 14 15
        assert_eq!(iter.next(), Some(5));
        assert_eq!(iter.next(), Some(6));
        assert_eq!(iter.next(), Some(9));
        assert_eq!(iter.next(), Some(10));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn array_subset_iter_contiguous_indices1() {
        let subset = ArraySubset::new_with_shape(vec![2, 2]);
        let mut iter = subset.iter_contiguous_indices(&[2, 2]).unwrap();
        assert_eq!(iter.next(), Some((vec![0, 0], 4)));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn array_subset_iter_contiguous_indices2() {
        let subset = ArraySubset::new_with_ranges(&[1..3, 1..3]);
        let mut iter = subset.iter_contiguous_indices(&[4, 4]).unwrap();
        assert_eq!(iter.next(), Some((vec![1, 1], 2)));
        assert_eq!(iter.next(), Some((vec![2, 1], 2)));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn array_subset_iter_contiguous_indices3() {
        let subset = ArraySubset::new_with_ranges(&[1..3, 0..1, 0..2, 0..2]);
        let mut iter = subset.iter_contiguous_indices(&[3, 1, 2, 2]).unwrap();
        assert_eq!(iter.next(), Some((vec![1, 0, 0, 0], 8)));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn array_subset_iter_continuous_linearised_indices() {
        let subset = ArraySubset::new_with_ranges(&[1..3, 1..3]);
        let mut iter = subset.iter_contiguous_linearised_indices(&[4, 4]).unwrap();
        //  0  1  2  3
        //  4  5  6  7
        //  8  9 10 11
        // 12 13 14 15
        assert_eq!(iter.next(), Some((5, 2)));
        assert_eq!(iter.next(), Some((9, 2)));
        assert_eq!(iter.next(), None);
    }

    #[test]
    #[rustfmt::skip]
    fn array_subset_iter_chunks1() {
        let subset = ArraySubset::new_with_ranges(&[1..5, 1..5]);
        let mut iter = subset.iter_chunks(&[2, 2]).unwrap();
        assert_eq!(iter.next(), Some((vec![0, 0], ArraySubset::new_with_ranges(&[0..2, 0..2]))));
        assert_eq!(iter.next(), Some((vec![0, 1], ArraySubset::new_with_ranges(&[0..2, 2..4]))));
        assert_eq!(iter.next(), Some((vec![0, 2], ArraySubset::new_with_ranges(&[0..2, 4..6]))));
        assert_eq!(iter.next(), Some((vec![1, 0], ArraySubset::new_with_ranges(&[2..4, 0..2]))));
        assert_eq!(iter.next(), Some((vec![1, 1], ArraySubset::new_with_ranges(&[2..4, 2..4]))));
        assert_eq!(iter.next(), Some((vec![1, 2], ArraySubset::new_with_ranges(&[2..4, 4..6]))));
        assert_eq!(iter.next(), Some((vec![2, 0], ArraySubset::new_with_ranges(&[4..6, 0..2]))));
        assert_eq!(iter.next(), Some((vec![2, 1], ArraySubset::new_with_ranges(&[4..6, 2..4]))));
        assert_eq!(iter.next(), Some((vec![2, 2], ArraySubset::new_with_ranges(&[4..6, 4..6]))));
        assert_eq!(iter.next(), None);
    }

    #[test]
    #[rustfmt::skip]
    fn array_subset_iter_chunks2() {
        let subset = ArraySubset::new_with_ranges(&[2..5, 2..6]);
        let mut iter = subset.iter_chunks(&[2, 3]).unwrap();
        assert_eq!(iter.next(), Some((vec![1, 0], ArraySubset::new_with_ranges(&[2..4, 0..3]))));
        assert_eq!(iter.next(), Some((vec![1, 1], ArraySubset::new_with_ranges(&[2..4, 3..6]))));
        assert_eq!(iter.next(), Some((vec![2, 0], ArraySubset::new_with_ranges(&[4..6, 0..3]))));
        assert_eq!(iter.next(), Some((vec![2, 1], ArraySubset::new_with_ranges(&[4..6, 3..6]))));
        assert_eq!(iter.next(), None);
    }
}
