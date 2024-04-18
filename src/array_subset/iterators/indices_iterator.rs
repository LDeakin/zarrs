use std::iter::FusedIterator;

use crate::{
    array::{unravel_index, ArrayIndices},
    array_subset::ArraySubset,
};

use rayon::iter::{
    plumbing::{bridge, Consumer, Producer, ProducerCallback, UnindexedConsumer},
    IndexedParallelIterator, IntoParallelIterator, ParallelIterator,
};

/// An iterator over the indices in an array subset.
///
/// Iterates over the last dimension fastest (i.e. C-contiguous order).
/// For example, consider a 4x3 array with element indices
/// ```text
/// (0, 0)  (0, 1)  (0, 2)
/// (1, 0)  (1, 1)  (1, 2)
/// (2, 0)  (2, 1)  (2, 2)
/// (3, 0)  (3, 1)  (3, 2)
/// ```
/// An iterator with an array subset corresponding to the lower right 2x2 region will produce `[(2, 1), (2, 2), (3, 1), (3, 2)]`.
pub struct Indices {
    subset: ArraySubset,
    index_front: u64,
    index_back: u64,
    length: usize,
}

impl Indices {
    /// Create a new indices struct.
    #[must_use]
    pub fn new(subset: ArraySubset) -> Self {
        let length = subset.num_elements_usize();
        let index_front = 0;
        let index_back = length as u64;
        Self {
            subset,
            index_front,
            index_back,
            length,
        }
    }

    /// Create a new indices struct spanning an explicit index range.
    ///
    /// # Panics
    /// Panics if `index_back` < `index_front`
    #[must_use]
    pub fn new_with_start_end(subset: ArraySubset, index_front: u64, index_back: u64) -> Self {
        let length = usize::try_from(index_back - index_front).unwrap();
        Self {
            subset,
            index_front,
            index_back,
            length,
        }
    }

    /// Return the number of indices.
    #[must_use]
    pub fn len(&self) -> usize {
        self.length
    }

    /// Returns true if the number of indices is zero.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Create a new serial iterator.
    #[must_use]
    pub fn iter(&self) -> IndicesIterator<'_> {
        <&Self as IntoIterator>::into_iter(self)
    }
}

impl<'a> IntoIterator for &'a Indices {
    type Item = ArrayIndices;
    type IntoIter = IndicesIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        IndicesIterator {
            subset: &self.subset,
            index_front: self.index_front,
            index_back: self.index_back,
            length: self.length,
        }
    }
}

impl<'a> IntoParallelIterator for &'a Indices {
    type Item = ArrayIndices;
    type Iter = ParIndicesIterator<'a>;

    fn into_par_iter(self) -> Self::Iter {
        ParIndicesIterator {
            subset: &self.subset,
            index_front: self.index_front,
            index_back: self.index_back,
            length: self.length,
        }
    }
}

/// Serial indices iterator.
///
/// See [`Indices`].
pub struct IndicesIterator<'a> {
    subset: &'a ArraySubset,
    index_front: u64,
    index_back: u64,
    length: usize,
}

impl<'a> IndicesIterator<'a> {
    /// Create a new indices iterator.
    #[must_use]
    pub(super) fn new(subset: &'a ArraySubset) -> Self {
        let length = subset.num_elements_usize();
        let index_front = 0;
        let index_back = length as u64;
        Self {
            subset,
            index_front,
            index_back,
            length,
        }
    }

    /// Create a new indices iterator spanning an explicit index range.
    ///
    /// # Panics
    /// Panics if `index_back` < `index_front`
    #[must_use]
    pub(super) fn new_with_start_end(
        subset: &'a ArraySubset,
        index_front: u64,
        index_back: u64,
    ) -> Self {
        let length = usize::try_from(index_back - index_front).unwrap();
        Self {
            subset,
            index_front,
            index_back,
            length,
        }
    }
}

impl Iterator for IndicesIterator<'_> {
    type Item = ArrayIndices;

    fn next(&mut self) -> Option<Self::Item> {
        let mut indices = unravel_index(self.index_front, self.subset.shape());
        std::iter::zip(indices.iter_mut(), self.subset.start())
            .for_each(|(index, start)| *index += start);

        if self.index_front < self.index_back {
            self.index_front += 1;
            Some(indices)
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.length, Some(self.length))
    }
}

impl DoubleEndedIterator for IndicesIterator<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.index_back > self.index_front {
            self.index_back -= 1;
            let mut indices = unravel_index(self.index_back, self.subset.shape());
            std::iter::zip(indices.iter_mut(), self.subset.start())
                .for_each(|(index, start)| *index += start);
            Some(indices)
        } else {
            None
        }
    }
}

impl ExactSizeIterator for IndicesIterator<'_> {}

impl FusedIterator for IndicesIterator<'_> {}

/// Parallel indices iterator.
///
/// See [`Indices`].
pub struct ParIndicesIterator<'a> {
    subset: &'a ArraySubset,
    index_front: u64,
    index_back: u64,
    length: usize,
}

impl ParallelIterator for ParIndicesIterator<'_> {
    type Item = ArrayIndices;

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

impl IndexedParallelIterator for ParIndicesIterator<'_> {
    fn with_producer<CB: ProducerCallback<Self::Item>>(self, callback: CB) -> CB::Output {
        let producer = ParIndicesIteratorProducer::from(&self);
        callback.callback(producer)
    }

    fn drive<C: Consumer<Self::Item>>(self, consumer: C) -> C::Result {
        bridge(self, consumer)
    }

    fn len(&self) -> usize {
        self.length
    }
}

#[derive(Debug)]
pub(super) struct ParIndicesIteratorProducer<'a> {
    pub subset: &'a ArraySubset,
    pub index_front: u64,
    pub index_back: u64,
}

impl<'a> Producer for ParIndicesIteratorProducer<'a> {
    type Item = ArrayIndices;
    type IntoIter = IndicesIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        IndicesIterator::new_with_start_end(self.subset, self.index_front, self.index_back)
    }

    fn split_at(self, index: usize) -> (Self, Self) {
        let left = ParIndicesIteratorProducer {
            subset: self.subset,
            index_front: self.index_front,
            index_back: self.index_front + index as u64,
        };
        let right = ParIndicesIteratorProducer {
            subset: self.subset,
            index_front: self.index_front + index as u64,
            index_back: self.index_back,
        };
        (left, right)
    }
}

impl<'a> From<&'a ParIndicesIterator<'_>> for ParIndicesIteratorProducer<'a> {
    fn from(iterator: &'a ParIndicesIterator<'_>) -> Self {
        Self {
            subset: iterator.subset,
            index_front: iterator.index_front,
            index_back: iterator.index_back,
        }
    }
}
