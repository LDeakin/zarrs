use std::{iter::FusedIterator, num::NonZeroU64};

use rayon::iter::{
    plumbing::{bridge, Consumer, Producer, ProducerCallback, UnindexedConsumer},
    IndexedParallelIterator, IntoParallelIterator, ParallelIterator,
};

use crate::{
    array::{chunk_shape_to_array_shape, ArrayIndices},
    array_subset::{ArraySubset, IncompatibleDimensionalityError},
};

use super::{
    indices_iterator::ParIndicesIteratorProducer, Indices, IndicesIterator, ParIndicesIterator,
};

/// TODO
pub struct Chunks {
    indices: Indices,
    chunk_shape: Vec<u64>,
}

impl Chunks {
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
        Self {
            indices: subset_chunks.indices(),
            chunk_shape,
        }
    }

    /// Create a new serial iterator.
    #[must_use]
    pub fn iter(&self) -> ChunksIterator<'_> {
        <&Self as IntoIterator>::into_iter(self)
    }
}

impl<'a> IntoIterator for &'a Chunks {
    type Item = (ArrayIndices, ArraySubset);
    type IntoIter = ChunksIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        ChunksIterator {
            inner: self.indices.into_iter(),
            chunk_shape: &self.chunk_shape,
        }
    }
}

impl<'a> IntoParallelIterator for &'a Chunks {
    type Item = (ArrayIndices, ArraySubset);
    type Iter = ParChunksIterator<'a>;

    fn into_par_iter(self) -> Self::Iter {
        ParChunksIterator {
            inner: self.indices.into_par_iter(),
            chunk_shape: &self.chunk_shape,
        }
    }
}

/// Iterates over the regular sized chunks overlapping this array subset.
/// All chunks have the same size, and may extend over the bounds of the array subset.
///
/// The iterator item is a ([`ArrayIndices`], [`ArraySubset`]) tuple corresponding to the chunk indices and array subset.
pub struct ChunksIterator<'a> {
    inner: IndicesIterator<'a>,
    chunk_shape: &'a [u64],
}

impl ChunksIterator<'_> {
    fn chunk_indices_with_subset(&self, chunk_indices: Vec<u64>) -> (Vec<u64>, ArraySubset) {
        let start = std::iter::zip(&chunk_indices, self.chunk_shape)
            .map(|(i, c)| i * c)
            .collect();
        let chunk_subset = unsafe {
            ArraySubset::new_with_start_shape_unchecked(start, self.chunk_shape.to_vec())
        };
        (chunk_indices, chunk_subset)
    }
}

impl Iterator for ChunksIterator<'_> {
    type Item = (ArrayIndices, ArraySubset);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .next()
            .map(|chunk_indices| self.chunk_indices_with_subset(chunk_indices))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl DoubleEndedIterator for ChunksIterator<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner
            .next_back()
            .map(|chunk_indices| self.chunk_indices_with_subset(chunk_indices))
    }
}

impl ExactSizeIterator for ChunksIterator<'_> {}

impl FusedIterator for ChunksIterator<'_> {}

/// Parallel indices iterator.
///
/// See [`Indices`].
pub struct ParChunksIterator<'a> {
    inner: ParIndicesIterator<'a>,
    chunk_shape: &'a [u64],
}

impl ParallelIterator for ParChunksIterator<'_> {
    type Item = (Vec<u64>, ArraySubset);

    fn drive_unindexed<C>(self, consumer: C) -> C::Result
    where
        C: UnindexedConsumer<Self::Item>,
    {
        bridge(self, consumer)
    }

    fn opt_len(&self) -> Option<usize> {
        Some(self.len())
    }
}

impl IndexedParallelIterator for ParChunksIterator<'_> {
    fn with_producer<CB: ProducerCallback<Self::Item>>(self, callback: CB) -> CB::Output {
        let producer = ParChunksIteratorProducer::from(&self);
        callback.callback(producer)
    }

    fn drive<C: Consumer<Self::Item>>(self, consumer: C) -> C::Result {
        bridge(self, consumer)
    }

    fn len(&self) -> usize {
        self.inner.len()
    }
}

#[derive(Debug)]
struct ParChunksIteratorProducer<'a> {
    inner: ParIndicesIteratorProducer<'a>,
    chunk_shape: &'a [u64],
}

impl<'a> Producer for ParChunksIteratorProducer<'a> {
    type Item = (Vec<u64>, ArraySubset);
    type IntoIter = ChunksIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        ChunksIterator {
            inner: IndicesIterator::new_with_start_end(
                self.inner.subset,
                self.inner.index_front,
                self.inner.index_back,
            ),
            chunk_shape: self.chunk_shape,
        }
    }

    fn split_at(self, index: usize) -> (Self, Self) {
        let (left, right) = self.inner.split_at(index);
        (
            ParChunksIteratorProducer {
                inner: left,
                chunk_shape: self.chunk_shape,
            },
            ParChunksIteratorProducer {
                inner: right,
                chunk_shape: self.chunk_shape,
            },
        )
    }
}

impl<'a> From<&'a ParChunksIterator<'_>> for ParChunksIteratorProducer<'a> {
    fn from(iterator: &'a ParChunksIterator<'_>) -> Self {
        Self {
            inner: ParIndicesIteratorProducer::from(&iterator.inner),
            chunk_shape: iterator.chunk_shape,
        }
    }
}
