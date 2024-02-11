use std::iter::FusedIterator;

use itertools::izip;

use crate::{array::ArrayIndices, array_subset::ArraySubset};

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
