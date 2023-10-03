use itertools::izip;

use crate::array::{ravel_indices, ArrayIndices};

use super::ArraySubset;

/// Iterates over element indices in an array subset.
pub struct IndicesIterator {
    subset: ArraySubset,
    next: Option<ArrayIndices>,
}

impl IndicesIterator {
    /// Create a new indices iterator.
    #[must_use]
    pub fn new(subset: ArraySubset, next: Option<ArrayIndices>) -> Self {
        Self { subset, next }
    }
}

impl Iterator for IndicesIterator {
    type Item = ArrayIndices;

    fn next(&mut self) -> Option<Self::Item> {
        let current = self.next.clone();
        if let Some(next) = self.next.as_mut() {
            let mut carry = true;
            for (next, start, size) in izip!(
                next.iter_mut().rev(),
                self.subset.start.iter().rev(),
                self.subset.shape.iter().rev()
            ) {
                if carry {
                    *next += 1;
                }
                if *next == start + size {
                    *next = *start;
                    carry = true;
                } else {
                    carry = false;
                    break;
                }
            }
            if carry {
                self.next = None;
            }
        }
        current
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let num_elements = self.subset.num_elements_usize();
        (num_elements, Some(num_elements))
    }
}

/// Iterates over linearised element indices of an array subset in an array.
pub struct LinearisedIndicesIterator<'a> {
    inner: IndicesIterator,
    array_shape: &'a [u64],
}

impl<'a> LinearisedIndicesIterator<'a> {
    /// Create a new linearised indices iterator.
    #[must_use]
    pub fn new(inner: IndicesIterator, array_shape: &'a [u64]) -> Self {
        Self { inner, array_shape }
    }
}

impl Iterator for LinearisedIndicesIterator<'_> {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        let indices = self.inner.next();
        match indices {
            Some(indices) => Some(ravel_indices(&indices, self.array_shape)),
            None => None,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

/// Iterates over contiguous element indices in an array subset.
///
/// The iterator item is a tuple: (indices, # contigous elements).
pub struct ContiguousIndicesIterator<'a> {
    subset: &'a ArraySubset,
    array_shape: &'a [u64],
    next: Option<ArrayIndices>,
}

impl<'a> ContiguousIndicesIterator<'a> {
    /// Create a new contiguous indices iterator.
    #[must_use]
    pub fn new(
        subset: &'a ArraySubset,
        array_shape: &'a [u64],
        next: Option<ArrayIndices>,
    ) -> Self {
        Self {
            subset,
            array_shape,
            next,
        }
    }
}

impl Iterator for ContiguousIndicesIterator<'_> {
    type Item = (ArrayIndices, u64);

    fn next(&mut self) -> Option<Self::Item> {
        let current: Option<ArrayIndices> = self.next.clone();
        let mut contiguous_elements: u64 = 1;
        let mut last_dim_span = true;
        if let Some(next) = self.next.as_mut() {
            let mut carry = true;
            for (next, start, size, shape) in izip!(
                next.iter_mut().rev(),
                self.subset.start.iter().rev(),
                self.subset.shape.iter().rev(),
                self.array_shape.iter().rev(),
            ) {
                if carry {
                    if last_dim_span {
                        let contiguous_elements_dim = start + size - *next;
                        *next += contiguous_elements_dim;
                        contiguous_elements *= contiguous_elements_dim;
                        last_dim_span = size == shape; // && start == 0
                    } else {
                        *next += 1;
                    }
                }
                if *next == start + size {
                    *next = *start;
                    carry = true;
                } else {
                    carry = false;
                    break;
                }
            }
            if carry {
                self.next = None;
            }
        }
        current.map(|index| (index, contiguous_elements))
    }
}

/// Iterates over contiguous linearised element indices in an array subset.
///
/// The iterator item is a tuple: (linearised index, # contigous elements).
pub struct ContiguousLinearisedIndicesIterator<'a> {
    inner: ContiguousIndicesIterator<'a>,
}

impl<'a> ContiguousLinearisedIndicesIterator<'a> {
    /// Return a new contiguous linearised indices iterator.
    #[must_use]
    pub fn new(inner: ContiguousIndicesIterator<'a>) -> Self {
        Self { inner }
    }
}

impl Iterator for ContiguousLinearisedIndicesIterator<'_> {
    type Item = (u64, u64);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .next()
            .map(|(indices, elements)| (ravel_indices(&indices, self.inner.array_shape), elements))
    }
}

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
    #[must_use]
    pub fn new(
        subset: &ArraySubset,
        chunk_shape: &'a [u64],
        first_chunk: Option<ArrayIndices>,
    ) -> Self {
        let chunk_start: ArrayIndices = std::iter::zip(subset.start(), chunk_shape)
            .map(|(s, c)| s / c)
            .collect();
        let chunk_end_inc: ArrayIndices = std::iter::zip(subset.end_inc(), chunk_shape)
            .map(|(e, c)| e / c)
            .collect();
        let subset_chunks =
            unsafe { ArraySubset::new_with_start_end_inc_unchecked(chunk_start, &chunk_end_inc) };
        let inner = IndicesIterator::new(subset_chunks, first_chunk);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn array_subset_iter_indices() {
        let subset = ArraySubset::new_with_start_shape(vec![1, 1], vec![2, 2]).unwrap();
        let mut iter = subset.iter_indices();
        assert_eq!(iter.next(), Some(vec![1, 1]));
        assert_eq!(iter.next(), Some(vec![1, 2]));
        assert_eq!(iter.next(), Some(vec![2, 1]));
        assert_eq!(iter.next(), Some(vec![2, 2]));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn array_subset_iter_linearised_indices() {
        let subset = ArraySubset::new_with_start_shape(vec![1, 1], vec![2, 2]).unwrap();
        let mut iter = subset.iter_linearised_indices(&[4, 4]);
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
        let subset = ArraySubset::new_with_start_shape(vec![0, 0], vec![2, 2]).unwrap();
        let mut iter = subset.iter_contiguous_indices(&[2, 2]).unwrap();
        assert_eq!(iter.next(), Some((vec![0, 0], 4)));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn array_subset_iter_contiguous_indices2() {
        let subset = ArraySubset::new_with_start_shape(vec![1, 1], vec![2, 2]).unwrap();
        let mut iter = subset.iter_contiguous_indices(&[4, 4]).unwrap();
        assert_eq!(iter.next(), Some((vec![1, 1], 2)));
        assert_eq!(iter.next(), Some((vec![2, 1], 2)));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn array_subset_iter_contiguous_indices3() {
        let subset = ArraySubset::new_with_start_shape(vec![1, 0, 0, 0], vec![2, 1, 2, 2]).unwrap();
        let mut iter = subset.iter_contiguous_indices(&[2, 2, 2, 2]).unwrap();
        assert_eq!(iter.next(), Some((vec![1, 0, 0, 0], 4)));
        assert_eq!(iter.next(), Some((vec![2, 0, 0, 0], 4)));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn array_subset_iter_continguous_linearised_indices() {
        let subset = ArraySubset::new_with_start_shape(vec![1, 1], vec![2, 2]).unwrap();
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
        let subset = ArraySubset::new_with_start_shape(vec![1, 1], vec![4, 4]).unwrap();
        let mut iter = subset.iter_chunks(&[2, 2]).unwrap();
        assert_eq!(iter.next(), Some((vec![0, 0], ArraySubset::new_with_start_shape(vec![0, 0], vec![2, 2]).unwrap())));
        assert_eq!(iter.next(), Some((vec![0, 1], ArraySubset::new_with_start_shape(vec![0, 2], vec![2, 2]).unwrap())));
        assert_eq!(iter.next(), Some((vec![0, 2], ArraySubset::new_with_start_shape(vec![0, 4], vec![2, 2]).unwrap())));
        assert_eq!(iter.next(), Some((vec![1, 0], ArraySubset::new_with_start_shape(vec![2, 0], vec![2, 2]).unwrap())));
        assert_eq!(iter.next(), Some((vec![1, 1], ArraySubset::new_with_start_shape(vec![2, 2], vec![2, 2]).unwrap())));
        assert_eq!(iter.next(), Some((vec![1, 2], ArraySubset::new_with_start_shape(vec![2, 4], vec![2, 2]).unwrap())));
        assert_eq!(iter.next(), Some((vec![2, 0], ArraySubset::new_with_start_shape(vec![4, 0], vec![2, 2]).unwrap())));
        assert_eq!(iter.next(), Some((vec![2, 1], ArraySubset::new_with_start_shape(vec![4, 2], vec![2, 2]).unwrap())));
        assert_eq!(iter.next(), Some((vec![2, 2], ArraySubset::new_with_start_shape(vec![4, 4], vec![2, 2]).unwrap())));
        assert_eq!(iter.next(), None);
    }

    #[test]
    #[rustfmt::skip]
    fn array_subset_iter_chunks2() {
        let subset = ArraySubset::new_with_start_shape(vec![2, 2], vec![3, 4]).unwrap();
        let mut iter = subset.iter_chunks(&[2, 3]).unwrap();
        assert_eq!(iter.next(), Some((vec![1, 0], ArraySubset::new_with_start_shape(vec![2, 0], vec![2, 3]).unwrap())));
        assert_eq!(iter.next(), Some((vec![1, 1], ArraySubset::new_with_start_shape(vec![2, 3], vec![2, 3]).unwrap())));
        assert_eq!(iter.next(), Some((vec![2, 0], ArraySubset::new_with_start_shape(vec![4, 0], vec![2, 3]).unwrap())));
        assert_eq!(iter.next(), Some((vec![2, 1], ArraySubset::new_with_start_shape(vec![4, 3], vec![2, 3]).unwrap())));
        assert_eq!(iter.next(), None);
    }
}
