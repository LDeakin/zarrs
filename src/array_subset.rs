//! Array subsets.
//!
//! An [`ArraySubset`] represents a subset of an array or chunk.
//!
//! Many [`Array`](crate::array::Array) store and retrieve methods have an [`ArraySubset`] parameter.
//! [`iterators`] includes various types of [`ArraySubset`] iterators.
//!
//! This module also provides convenience functions for:
//!  - computing the byte ranges of array subsets within an array with a fixed element size.

pub mod iterators;

use std::{num::NonZeroU64, ops::Range};

use iterators::{
    Chunks, ContiguousIndices, ContiguousLinearisedIndices, Indices, LinearisedIndices,
};

use derive_more::{Display, From};
use itertools::izip;
use thiserror::Error;

use crate::{
    array::{ArrayIndices, ArrayShape},
    byte_range::ByteRange,
};

/// An array subset.
///
/// The unsafe `_unchecked methods` are mostly intended for internal use to avoid redundant input validation.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Display, Default)]
#[display("start {start:?} shape {shape:?}")]
pub struct ArraySubset {
    /// The start of the array subset.
    start: ArrayIndices,
    /// The shape of the array subset.
    shape: ArrayShape,
}

impl ArraySubset {
    /// Create a new empty array subset.
    #[must_use]
    pub fn new_empty(dimensionality: usize) -> Self {
        Self {
            start: vec![0; dimensionality],
            shape: vec![0; dimensionality],
        }
    }

    /// Create a new array subset from a list of [`Range`]s.
    #[must_use]
    pub fn new_with_ranges(ranges: &[Range<u64>]) -> Self {
        let start = ranges.iter().map(|range| range.start).collect();
        let shape = ranges.iter().map(|range| range.end - range.start).collect();
        Self { start, shape }
    }

    /// Create a new array subset with `size` starting at the origin.
    #[must_use]
    pub fn new_with_shape(shape: ArrayShape) -> Self {
        Self {
            start: vec![0; shape.len()],
            shape,
        }
    }

    /// Create a new array subset.
    ///
    /// # Errors
    ///
    /// Returns [`IncompatibleDimensionalityError`] if the size of `start` and `size` do not match.
    pub fn new_with_start_shape(
        start: ArrayIndices,
        shape: ArrayShape,
    ) -> Result<Self, IncompatibleDimensionalityError> {
        if start.len() == shape.len() {
            Ok(Self { start, shape })
        } else {
            Err(IncompatibleDimensionalityError::new(
                start.len(),
                shape.len(),
            ))
        }
    }

    /// Create a new array subset.
    ///
    /// # Safety
    /// The length of `start` and `size` must match.
    #[must_use]
    pub unsafe fn new_with_start_shape_unchecked(start: ArrayIndices, shape: ArrayShape) -> Self {
        debug_assert_eq!(start.len(), shape.len());
        Self { start, shape }
    }

    /// Create a new array subset from a start and end (inclusive).
    ///
    /// # Errors
    /// Returns [`IncompatibleStartEndIndicesError`] if `start` and `end` are incompatible, such as if any element of `end` is less than `start` or they differ in length.
    pub fn new_with_start_end_inc(
        start: ArrayIndices,
        end: ArrayIndices,
    ) -> Result<Self, IncompatibleStartEndIndicesError> {
        if start.len() != end.len() || std::iter::zip(&start, &end).any(|(start, end)| end < start)
        {
            Err(IncompatibleStartEndIndicesError::from((start, end)))
        } else {
            Ok(unsafe { Self::new_with_start_end_inc_unchecked(start, end) })
        }
    }

    /// Create a new array subset from a start and end (inclusive).
    ///
    /// # Safety
    /// The length of `start` and `end` must match.
    #[must_use]
    pub unsafe fn new_with_start_end_inc_unchecked(start: ArrayIndices, end: ArrayIndices) -> Self {
        debug_assert_eq!(start.len(), end.len());
        let shape = std::iter::zip(&start, end)
            .map(|(&start, end)| {
                debug_assert!(end >= start);
                end.saturating_sub(start) + 1
            })
            .collect();
        Self { start, shape }
    }

    /// Create a new array subset from a start and end (exclusive).
    ///
    /// # Errors
    /// Returns [`IncompatibleStartEndIndicesError`] if `start` and `end` are incompatible, such as if any element of `end` is less than `start` or they differ in length.
    pub fn new_with_start_end_exc(
        start: ArrayIndices,
        end: ArrayIndices,
    ) -> Result<Self, IncompatibleStartEndIndicesError> {
        if start.len() != end.len() || std::iter::zip(&start, &end).any(|(start, end)| end < start)
        {
            Err(IncompatibleStartEndIndicesError::from((start, end)))
        } else {
            Ok(unsafe { Self::new_with_start_end_exc_unchecked(start, end) })
        }
    }

    /// Create a new array subset from a start and end (exclusive).
    ///
    /// # Safety
    /// The length of `start` and `end` must match.
    #[must_use]
    pub unsafe fn new_with_start_end_exc_unchecked(start: ArrayIndices, end: ArrayIndices) -> Self {
        debug_assert_eq!(start.len(), end.len());
        let shape = std::iter::zip(&start, end)
            .map(|(&start, end)| {
                debug_assert!(end >= start);
                end.saturating_sub(start)
            })
            .collect();
        Self { start, shape }
    }

    /// Bound the array subset to the domain within `end` (exclusive).
    ///
    /// # Errors
    /// Returns an error if `end` does not match the array subset dimensionality.
    pub fn bound(&self, end: &[u64]) -> Result<Self, IncompatibleDimensionalityError> {
        if end.len() == self.dimensionality() {
            Ok(unsafe { self.bound_unchecked(end) })
        } else {
            Err(IncompatibleDimensionalityError(
                end.len(),
                self.dimensionality(),
            ))
        }
    }

    /// Bound the array subset to the domain within `end` (exclusive).
    ///
    /// # Safety
    /// The length of `end` must match the array subset dimensionality.
    #[must_use]
    pub unsafe fn bound_unchecked(&self, end: &[u64]) -> Self {
        debug_assert_eq!(end.len(), self.dimensionality());
        let start = std::iter::zip(self.start(), end)
            .map(|(&a, &b)| std::cmp::min(a, b))
            .collect();
        let end = std::iter::zip(self.end_exc(), end)
            .map(|(a, &b)| std::cmp::min(a, b))
            .collect();
        unsafe { Self::new_with_start_end_exc_unchecked(start, end) }
    }

    /// Return the start of the array subset.
    #[must_use]
    pub fn start(&self) -> &[u64] {
        &self.start
    }

    /// Return the shape of the array subset.
    #[must_use]
    pub fn shape(&self) -> &[u64] {
        &self.shape
    }

    /// Return the shape of the array subset.
    ///
    /// # Panics
    /// Panics if a dimension exceeds [`usize::MAX`].
    #[must_use]
    pub fn shape_usize(&self) -> Vec<usize> {
        self.shape
            .iter()
            .map(|d| usize::try_from(*d).unwrap())
            .collect()
    }

    /// Returns if the array subset is empty (i.e. has a zero element in its shape).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.shape.iter().any(|i| i == &0)
    }

    /// Return the dimensionality of the array subset.
    #[must_use]
    pub fn dimensionality(&self) -> usize {
        self.start.len()
    }

    /// Return the end (inclusive) of the array subset.
    ///
    /// Returns [`None`] if the array subset is empty.
    #[must_use]
    pub fn end_inc(&self) -> Option<ArrayIndices> {
        if self.is_empty() {
            None
        } else {
            Some(
                std::iter::zip(&self.start, &self.shape)
                    .map(|(start, size)| start + size - 1)
                    .collect(),
            )
        }
    }

    /// Return the end (exclusive) of the array subset.
    #[must_use]
    pub fn end_exc(&self) -> ArrayIndices {
        std::iter::zip(&self.start, &self.shape)
            .map(|(start, size)| start + size)
            .collect()
    }

    /// Return the number of elements of the array subset.
    ///
    /// Equal to the product of the components of its shape.
    #[must_use]
    pub fn num_elements(&self) -> u64 {
        self.shape.iter().product()
    }

    /// Return the number of elements of the array subset as a `usize`.
    ///
    /// # Panics
    ///
    /// Panics if [`num_elements()`](Self::num_elements()) is greater than [`usize::MAX`].
    #[must_use]
    pub fn num_elements_usize(&self) -> usize {
        usize::try_from(self.num_elements()).unwrap()
    }

    /// Returns [`true`] if the array subset contains `indices`.
    #[must_use]
    pub fn contains(&self, indices: &[u64]) -> bool {
        izip!(indices, &self.start, &self.shape).all(|(&i, &o, &s)| i >= o && i < o + s)
    }

    /// Return the byte ranges of an array subset in an array with `array_shape` and `element_size`.
    ///
    /// # Errors
    ///
    /// Returns [`IncompatibleArraySubsetAndShapeError`] if the `array_shape` does not encapsulate this array subset.
    pub fn byte_ranges(
        &self,
        array_shape: &[u64],
        element_size: usize,
    ) -> Result<Vec<ByteRange>, IncompatibleArraySubsetAndShapeError> {
        let mut byte_ranges: Vec<ByteRange> = Vec::new();
        let contiguous_indices = self.contiguous_linearised_indices(array_shape)?;
        let byte_length = contiguous_indices.contiguous_elements_usize() * element_size;
        for (array_index, _contiguous_elements) in &contiguous_indices {
            let byte_index = array_index * element_size as u64;
            byte_ranges.push(ByteRange::FromStart(byte_index, Some(byte_length as u64)));
        }
        Ok(byte_ranges)
    }

    /// Return the byte ranges of an array subset in an array with `array_shape` and `element_size`.
    ///
    /// # Safety
    /// The length of `array_shape` must match the dimensionality of `array_subset`.
    #[must_use]
    pub unsafe fn byte_ranges_unchecked(
        &self,
        array_shape: &[u64],
        element_size: usize,
    ) -> Vec<ByteRange> {
        let mut byte_ranges: Vec<ByteRange> = Vec::new();
        let contiguous_indices = self.contiguous_linearised_indices_unchecked(array_shape);
        let byte_length = contiguous_indices.contiguous_elements_usize() * element_size;
        for (array_index, _contiguous_elements) in &contiguous_indices {
            let byte_index = array_index * element_size as u64;
            byte_ranges.push(ByteRange::FromStart(byte_index, Some(byte_length as u64)));
        }
        byte_ranges
    }

    /// Return the elements in this array subset from an array with shape `array_shape`.
    ///
    /// # Errors
    ///
    /// Returns [`IncompatibleArraySubsetAndShapeError`] if the length of `array_shape` does not match the array subset dimensionality or the array subset is outside of the bounds of `array_shape`.
    ///
    /// # Panics
    /// Panics if attempting to access a byte index beyond [`usize::MAX`].
    pub fn extract_elements<T: std::marker::Copy>(
        &self,
        elements: &[T],
        array_shape: &[u64],
    ) -> Result<Vec<T>, IncompatibleArraySubsetAndShapeError> {
        if elements.len() as u64 == array_shape.iter().product::<u64>()
            && array_shape.len() == self.dimensionality()
            && self
                .end_exc()
                .iter()
                .zip(array_shape)
                .all(|(end, shape)| end <= shape)
        {
            Ok(unsafe { self.extract_elements_unchecked(elements, array_shape) })
        } else {
            Err(IncompatibleArraySubsetAndShapeError(
                self.clone(),
                array_shape.to_vec(),
            ))
        }
    }

    /// Return the elements in this array subset from an array with shape `array_shape`.
    ///
    /// # Safety
    /// The length of `array_shape` must match the array subset dimensionality and the array subset must be within the bounds of `array_shape`.
    ///
    /// # Panics
    /// Panics if attempting to reference a byte beyond `usize::MAX`.
    #[must_use]
    pub unsafe fn extract_elements_unchecked<T: std::marker::Copy>(
        &self,
        elements: &[T],
        array_shape: &[u64],
    ) -> Vec<T> {
        debug_assert_eq!(elements.len() as u64, array_shape.iter().product::<u64>());
        let num_elements = usize::try_from(self.num_elements()).unwrap();
        let mut bytes_subset = Vec::with_capacity(num_elements);
        let bytes_subset_slice = crate::vec_spare_capacity_to_mut_slice(&mut bytes_subset);
        let mut subset_offset = 0;
        for (array_index, contiguous_elements) in
            &self.contiguous_linearised_indices_unchecked(array_shape)
        {
            let element_offset = usize::try_from(array_index).unwrap();
            let element_length = usize::try_from(contiguous_elements).unwrap();
            debug_assert!(element_offset + element_length <= elements.len());
            debug_assert!(subset_offset + element_length <= num_elements);
            bytes_subset_slice[subset_offset..subset_offset + element_length]
                .copy_from_slice(&elements[element_offset..element_offset + element_length]);
            subset_offset += element_length;
        }
        unsafe { bytes_subset.set_len(num_elements) };
        bytes_subset
    }

    /// Returns an iterator over the indices of elements within the subset.
    #[must_use]
    pub fn indices(&self) -> Indices {
        Indices::new(self.clone())
    }

    /// Returns an iterator over the linearised indices of elements within the subset.
    ///
    /// # Errors
    ///
    /// Returns [`IncompatibleArraySubsetAndShapeError`] if the `array_shape` does not encapsulate this array subset.
    pub fn linearised_indices(
        &self,
        array_shape: &[u64],
    ) -> Result<LinearisedIndices, IncompatibleArraySubsetAndShapeError> {
        LinearisedIndices::new(self.clone(), array_shape.to_vec())
    }

    /// Returns an iterator over the indices of elements within the subset.
    ///
    /// # Safety
    /// `array_shape` must match the dimensionality and encapsulate this array subset.
    #[must_use]
    pub unsafe fn linearised_indices_unchecked(&self, array_shape: &[u64]) -> LinearisedIndices {
        LinearisedIndices::new_unchecked(self.clone(), array_shape.to_vec())
    }

    /// Returns an iterator over the indices of contiguous elements within the subset.
    ///
    /// # Errors
    ///
    /// Returns [`IncompatibleArraySubsetAndShapeError`] if the `array_shape` does not encapsulate this array subset.
    pub fn contiguous_indices(
        &self,
        array_shape: &[u64],
    ) -> Result<ContiguousIndices, IncompatibleArraySubsetAndShapeError> {
        ContiguousIndices::new(self, array_shape)
    }

    /// Returns an iterator over the indices of contiguous elements within the subset.
    ///
    /// # Safety
    /// The length of `array_shape` must match the array subset dimensionality.
    #[must_use]
    pub unsafe fn contiguous_indices_unchecked(&self, array_shape: &[u64]) -> ContiguousIndices {
        ContiguousIndices::new_unchecked(self, array_shape)
    }

    /// Returns an iterator over the linearised indices of contiguous elements within the subset.
    ///
    /// # Errors
    ///
    /// Returns [`IncompatibleArraySubsetAndShapeError`] if the `array_shape` does not encapsulate this array subset.
    pub fn contiguous_linearised_indices(
        &self,
        array_shape: &[u64],
    ) -> Result<ContiguousLinearisedIndices, IncompatibleArraySubsetAndShapeError> {
        ContiguousLinearisedIndices::new(self, array_shape.to_vec())
    }

    /// Returns an iterator over the linearised indices of contiguous elements within the subset.
    ///
    /// # Safety
    /// The length of `array_shape` must match the array subset dimensionality.
    #[must_use]
    pub unsafe fn contiguous_linearised_indices_unchecked(
        &self,
        array_shape: &[u64],
    ) -> ContiguousLinearisedIndices {
        ContiguousLinearisedIndices::new_unchecked(self, array_shape.to_vec())
    }

    /// Returns the [`Chunks`] with `chunk_shape` in the array subset which can be iterated over.
    ///
    /// All chunks overlapping the array subset are returned, and they all have the same shape `chunk_shape`.
    /// Thus, the subsets of the chunks may extend out over the subset.
    ///
    /// # Errors
    /// Returns an error if `chunk_shape` does not match the array subset dimensionality.
    pub fn chunks(
        &self,
        chunk_shape: &[NonZeroU64],
    ) -> Result<Chunks, IncompatibleDimensionalityError> {
        Chunks::new(self, chunk_shape)
    }

    /// Returns the [`Chunks`] with `chunk_shape` in the array subset which can be iterated over.
    ///
    /// All chunks overlapping the array subset are returned, and they all have the same shape `chunk_shape`.
    /// Thus, the subsets of the chunks may extend out over the subset.
    ///
    /// # Safety
    /// The length of `chunk_shape` must match the array subset dimensionality.
    #[must_use]
    pub unsafe fn chunks_unchecked(&self, chunk_shape: &[NonZeroU64]) -> Chunks {
        Chunks::new_unchecked(self, chunk_shape)
    }

    /// Return the overlapping subset between this array subset and `subset_other`.
    ///
    /// # Errors
    ///
    /// Returns [`IncompatibleDimensionalityError`] if the dimensionality of `subset_other` does not match the dimensionality of this array subset.
    pub fn overlap(&self, subset_other: &Self) -> Result<Self, IncompatibleDimensionalityError> {
        if subset_other.dimensionality() == self.dimensionality() {
            Ok(unsafe { self.overlap_unchecked(subset_other) })
        } else {
            Err(IncompatibleDimensionalityError::new(
                subset_other.dimensionality(),
                self.dimensionality(),
            ))
        }
    }

    /// Return the overlapping subset between this array subset and `subset_other`.
    ///
    /// # Safety
    /// Panics if the dimensionality of `subset_other` does not match the dimensionality of this array subset.
    #[must_use]
    pub unsafe fn overlap_unchecked(&self, subset_other: &Self) -> Self {
        debug_assert_eq!(subset_other.dimensionality(), self.dimensionality());
        let mut ranges = Vec::with_capacity(self.dimensionality());
        for (start, size, other_start, other_size) in izip!(
            &self.start,
            &self.shape,
            subset_other.start(),
            subset_other.shape(),
        ) {
            let overlap_start = *std::cmp::max(start, other_start);
            let overlap_end = std::cmp::min(start + size, other_start + other_size);
            ranges.push(overlap_start..overlap_end);
        }
        Self::new_with_ranges(&ranges)
    }

    /// Return the subset relative to `start`.
    ///
    /// Creates an array subset starting at [`ArraySubset::start()`] - `start`.
    ///
    /// # Errors
    /// Returns [`IncompatibleDimensionalityError`] if the length of `start` does not match the dimensionality of this array subset.
    pub fn relative_to(&self, start: &[u64]) -> Result<Self, IncompatibleDimensionalityError> {
        if start.len() == self.dimensionality() {
            Ok(unsafe { self.relative_to_unchecked(start) })
        } else {
            Err(IncompatibleDimensionalityError::new(
                start.len(),
                self.dimensionality(),
            ))
        }
    }

    /// Return the subset relative to `start`.
    ///
    /// Creates an array subset starting at [`ArraySubset::start()`] - `start`.
    ///
    /// # Safety
    /// Panics if the length of `start` does not match the dimensionality of this array subset.
    #[must_use]
    pub unsafe fn relative_to_unchecked(&self, start: &[u64]) -> Self {
        debug_assert_eq!(start.len(), self.dimensionality());
        Self {
            start: std::iter::zip(self.start(), start)
                .map(|(a, b)| a - b)
                .collect::<Vec<_>>(),
            shape: self.shape().to_vec(),
        }
    }

    /// Returns true if the array subset is within the bounds of `array_shape`.
    #[must_use]
    pub fn inbounds(&self, array_shape: &[u64]) -> bool {
        if self.dimensionality() != array_shape.len() {
            return false;
        }

        for (subset_start, subset_shape, shape) in izip!(self.start(), self.shape(), array_shape) {
            if subset_start + subset_shape > *shape {
                return false;
            }
        }
        true
    }
}

/// An incompatible dimensionality error.
#[derive(Copy, Clone, Debug, Error)]
#[error("incompatible dimensionality {0}, expected {1}")]
pub struct IncompatibleDimensionalityError(usize, usize);

impl IncompatibleDimensionalityError {
    /// Create a new incompatible dimensionality error.
    #[must_use]
    pub const fn new(got: usize, expected: usize) -> Self {
        Self(got, expected)
    }
}

/// An incompatible array and array shape error.
#[derive(Clone, Debug, Error, From)]
#[error("incompatible array subset {0} with array shape {1:?}")]
pub struct IncompatibleArraySubsetAndShapeError(ArraySubset, ArrayShape);

impl IncompatibleArraySubsetAndShapeError {
    /// Create a new incompatible array subset and shape error.
    #[must_use]
    pub fn new(array_subset: ArraySubset, array_shape: ArrayShape) -> Self {
        Self(array_subset, array_shape)
    }
}

/// An incompatible start/end indices error.
#[derive(Clone, Debug, Error, From)]
#[error("incompatible start {0:?} with end {1:?}")]
pub struct IncompatibleStartEndIndicesError(ArrayIndices, ArrayIndices);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn array_subset() {
        assert!(ArraySubset::new_with_start_shape(vec![0, 0], vec![10, 10]).is_ok());
        assert!(ArraySubset::new_with_start_shape(vec![0, 0], vec![10]).is_err());
        assert!(ArraySubset::new_with_start_end_inc(vec![0, 0], vec![10, 10]).is_ok());
        assert!(ArraySubset::new_with_start_end_inc(vec![0, 0], vec![10]).is_err());
        assert!(ArraySubset::new_with_start_end_inc(vec![5, 5], vec![0, 0]).is_err());
        assert!(ArraySubset::new_with_start_end_exc(vec![0, 0], vec![10, 10]).is_ok());
        assert!(ArraySubset::new_with_start_end_exc(vec![0, 0], vec![10]).is_err());
        assert!(ArraySubset::new_with_start_end_exc(vec![5, 5], vec![0, 0]).is_err());
        let array_subset = ArraySubset::new_with_start_shape(vec![0, 0], vec![10, 10])
            .unwrap()
            .bound(&[5, 5])
            .unwrap();
        assert_eq!(array_subset.shape(), &[5, 5]);
        assert!(ArraySubset::new_with_start_shape(vec![0, 0], vec![10, 10])
            .unwrap()
            .bound(&[5, 5, 5])
            .is_err());

        let array_subset0 = ArraySubset::new_with_ranges(&[1..5, 2..6]);
        let array_subset1 = ArraySubset::new_with_ranges(&[3..6, 4..7]);
        assert_eq!(
            array_subset0.overlap(&array_subset1).unwrap(),
            ArraySubset::new_with_ranges(&[3..5, 4..6])
        );
        assert_eq!(
            array_subset0.relative_to(&[1, 1]).unwrap(),
            ArraySubset::new_with_ranges(&[0..4, 1..5])
        );
        assert!(array_subset0.relative_to(&[1, 1, 1]).is_err());
        assert!(array_subset0.inbounds(&[10, 10]));
        assert!(!array_subset0.inbounds(&[2, 2]));
        assert!(!array_subset0.inbounds(&[10, 10, 10]));

        let array_subset2 = ArraySubset::new_with_ranges(&[3..6, 4..7, 0..1]);
        assert!(array_subset0.overlap(&array_subset2).is_err());
        assert_eq!(
            unsafe { array_subset2.linearised_indices_unchecked(&[6, 7, 1]) }
                .into_iter()
                .next(),
            Some(4 * 1 + 3 * 7 * 1)
        )
    }

    #[test]
    fn array_subset_bytes() {
        let array_subset = ArraySubset::new_with_ranges(&[1..3, 1..3]);

        assert!(array_subset.byte_ranges(&[1, 1], 1).is_err());

        assert_eq!(
            array_subset.byte_ranges(&[4, 4], 1).unwrap(),
            vec![
                ByteRange::FromStart(5, Some(2)),
                ByteRange::FromStart(9, Some(2))
            ]
        );

        assert_eq!(
            unsafe { array_subset.byte_ranges_unchecked(&[4, 4], 1) },
            vec![
                ByteRange::FromStart(5, Some(2)),
                ByteRange::FromStart(9, Some(2))
            ]
        );
    }
}
