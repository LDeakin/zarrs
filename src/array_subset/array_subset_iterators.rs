use itertools::izip;
use thiserror::Error;

use crate::array::{ravel_indices, ArrayIndices, ArrayShape};

use super::ArraySubset;

/// Iterates over element indices in an array subset.
pub struct IndicesIterator {
    subset: ArraySubset,
    next: Option<Vec<usize>>,
}

impl IndicesIterator {
    /// Create a new indices iterator.
    #[must_use]
    pub fn new(subset: ArraySubset, next: Option<Vec<usize>>) -> Self {
        Self { subset, next }
    }
}

impl Iterator for IndicesIterator {
    type Item = Vec<usize>;

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
}

/// Iterates over linearised element indices in an array subset.
pub struct LinearisedIndicesIterator {
    inner: IndicesIterator,
}

impl LinearisedIndicesIterator {
    /// Create a new linearised indices iterator.
    #[must_use]
    pub fn new(inner: IndicesIterator) -> Self {
        Self { inner }
    }
}

impl Iterator for LinearisedIndicesIterator {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        let indices = self.inner.next();
        match indices {
            Some(indices) => Some(ravel_indices(&indices, &self.inner.subset.shape)),
            None => None,
        }
    }
}

/// Iterates over contiguous element indices in an array subset.
///
/// The iterator item is a tuple: (indices, # contigous elements).
pub struct ContiguousIndicesIterator<'a> {
    subset: &'a ArraySubset,
    array_shape: &'a [usize],
    next: Option<Vec<usize>>,
}

impl<'a> ContiguousIndicesIterator<'a> {
    /// Create a new contiguous indices iterator.
    #[must_use]
    pub fn new(
        subset: &'a ArraySubset,
        array_shape: &'a [usize],
        next: Option<Vec<usize>>,
    ) -> Self {
        Self {
            subset,
            array_shape,
            next,
        }
    }
}

impl Iterator for ContiguousIndicesIterator<'_> {
    type Item = (Vec<usize>, usize);

    fn next(&mut self) -> Option<Self::Item> {
        let current: Option<Vec<usize>> = self.next.clone();
        let mut contiguous_elements: usize = 1;
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
    type Item = (usize, usize);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .next()
            .map(|(indices, elements)| (ravel_indices(&indices, self.inner.array_shape), elements))
    }
}

/// Iterates over the regular sized chunks overlapping this array subset.
///
/// The iterator item is a ([`ArrayIndices`], [`ArraySubset`]) tuple corresponding to the chunk indices and array subset.
pub struct ChunksIterator<'a> {
    inner: IndicesIterator,
    chunk_shape: &'a [usize],
}

impl<'a> ChunksIterator<'a> {
    /// Create a new chunks iterator.
    #[must_use]
    pub fn new(
        subset: &ArraySubset,
        chunk_shape: &'a [usize],
        first_chunk: Option<Vec<usize>>,
    ) -> Self {
        let chunk_start = std::iter::zip(subset.start(), chunk_shape)
            .map(|(s, c)| s / c)
            .collect();
        let chunk_end_inc: Vec<usize> = std::iter::zip(subset.end_inc(), chunk_shape)
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
}

/// A chunks iterator error.
#[derive(Error, Debug)]
#[error(
    "array shape {_1:?} is not a multiple of chunk shape {_0:?} or it differs in dimensionality"
)]
pub struct ChunksIteratorError(ArrayShape, ArrayShape);

impl ChunksIteratorError {
    /// Create a new [`ChunksIteratorError`]
    #[must_use]
    pub fn new(chunk_shape: Vec<usize>, array_shape: Vec<usize>) -> Self {
        Self(chunk_shape, array_shape)
    }
}
