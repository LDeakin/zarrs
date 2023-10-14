//! Array subsets.
//!
//! An [`ArraySubset`] is widely used throughout this library when extracting a subset of data from an array.
//! It can produce convenient iterators over the indices or linearised indices of an array subset.
//!
//! This module provides convenience functions for:
//!  - computing the byte ranges of array subsets within an array, and
//!  - extracting the bytes within subsets of an array.

mod array_subset_iterators;

pub use array_subset_iterators::{
    ChunksIterator, ContiguousIndicesIterator, ContiguousLinearisedIndicesIterator,
    IndicesIterator, LinearisedIndicesIterator,
};

use derive_more::{Display, From};
use itertools::izip;
use thiserror::Error;

use crate::{
    array::{ArrayIndices, ArrayShape},
    byte_range::ByteRange,
};

/// An array subset.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Display, Default)]
#[display(fmt = "start {start:?} shape {shape:?}")]
pub struct ArraySubset {
    /// The start of the array subset.
    start: ArrayIndices,
    /// The shape of the array subset.
    shape: ArrayShape,
}

/// An array extract bytes error.
#[derive(Debug, Error)]
#[error("array subset {_0} is incompatible with array of shape {_1:?} and element size {_2}")]
pub struct ArrayExtractBytesError(ArraySubset, ArrayShape, usize);

/// An array extract bytes error.
#[derive(Debug, Error)]
pub enum ArrayStoreBytesError {
    /// Invalid array shape.
    #[error("array shape {_1:?} is incompatible with array subset {_0:?}")]
    InvalidArrayShape(ArraySubset, ArrayShape),
    /// Invalid subset bytes.
    #[error("expected subset bytes to have length {_1}, got {_0}")]
    InvalidSubsetBytes(usize, usize),
    /// Invalid array bytes.
    #[error("expected array bytes to have length {_1}, got {_0}")]
    InvalidArrayBytes(usize, usize),
}

impl ArraySubset {
    /// Create a new array subset with `size` starting at the origin.
    #[must_use]
    pub fn new_with_shape(shape: ArrayShape) -> Self {
        ArraySubset {
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
            Ok(ArraySubset { start, shape })
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
    ///
    /// The length of `start` and `size` must match.
    #[doc(hidden)]
    #[must_use]
    pub unsafe fn new_with_start_shape_unchecked(start: ArrayIndices, shape: ArrayShape) -> Self {
        debug_assert_eq!(start.len(), shape.len());
        ArraySubset { start, shape }
    }

    /// Create a new array subset from a start and end (inclusive).
    ///
    /// # Errors
    ///
    /// Returns [`IncompatibleDimensionalityError`] if the size of `start` and `size` do not match.
    pub fn new_with_start_end_inc(
        start: ArrayIndices,
        end: &[u64],
    ) -> Result<Self, IncompatibleDimensionalityError> {
        if start.len() == end.len() {
            Ok(unsafe { Self::new_with_start_end_inc_unchecked(start, end) })
        } else {
            Err(IncompatibleDimensionalityError::new(start.len(), end.len()))
        }
    }

    /// Create a new array subset from a start and end (inclusive).
    ///
    /// # Safety
    ///
    /// The length of `start` and `end` must match.
    #[doc(hidden)]
    #[must_use]
    pub unsafe fn new_with_start_end_inc_unchecked(start: ArrayIndices, end: &[u64]) -> Self {
        debug_assert_eq!(start.len(), end.len());
        let shape = std::iter::zip(&start, end)
            .map(|(start, end)| {
                debug_assert!(end >= start);
                end.saturating_sub(*start) + 1
            })
            .collect();
        ArraySubset { start, shape }
    }

    /// Create a new array subset from a start and end (exclusive).
    ///
    /// # Errors
    ///
    /// Returns [`IncompatibleDimensionalityError`] if the size of `start` and `size` do not match.
    pub fn new_with_start_end_exc(
        start: ArrayIndices,
        end: &[u64],
    ) -> Result<Self, IncompatibleDimensionalityError> {
        if start.len() == end.len() {
            Ok(unsafe { Self::new_with_start_end_exc_unchecked(start, end) })
        } else {
            Err(IncompatibleDimensionalityError::new(start.len(), end.len()))
        }
    }

    /// Create a new array subset from a start and end (exclusive).
    ///
    /// # Safety
    ///
    /// The length of `start` and `end` must match.
    #[doc(hidden)]
    #[must_use]
    pub unsafe fn new_with_start_end_exc_unchecked(start: ArrayIndices, end: &[u64]) -> Self {
        debug_assert_eq!(start.len(), end.len());
        let shape = std::iter::zip(&start, end)
            .map(|(start, end)| {
                debug_assert!(end >= start);
                end.saturating_sub(*start)
            })
            .collect();
        ArraySubset { start, shape }
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

    /// Return the dimensionality of the array subset.
    #[must_use]
    pub fn dimensionality(&self) -> usize {
        self.start.len()
    }
    /// Return the end (inclusive) of the array subset.
    #[must_use]
    pub fn end_inc(&self) -> ArrayIndices {
        std::iter::zip(&self.start, &self.shape)
            .map(|(start, size)| start + size - 1)
            .collect()
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

    /// Return the byte ranges of an array subset in an array with `array_shape` and `element_size`.
    ///
    /// # Errors
    ///
    /// Returns [`IncompatibleArrayShapeError`] if the `array_shape` does not encapsulate this array subset.
    pub fn byte_ranges(
        &self,
        array_shape: &[u64],
        element_size: usize,
    ) -> Result<Vec<ByteRange>, IncompatibleArrayShapeError> {
        let mut byte_ranges: Vec<ByteRange> = Vec::new();
        for (array_index, contiguous_elements) in
            self.iter_contiguous_linearised_indices(array_shape)?
        {
            let byte_index = array_index * element_size as u64;
            let byte_length = contiguous_elements * element_size as u64;
            byte_ranges.push(ByteRange::FromStart(byte_index, Some(byte_length)));
        }
        Ok(byte_ranges)
    }

    /// Return the byte ranges of an array subset in an array with `array_shape` and `element_size`.
    ///
    /// # Safety
    ///
    /// The length of `array_shape` must match the dimensionality of `array_subset`.
    #[doc(hidden)]
    #[must_use]
    pub unsafe fn byte_ranges_unchecked(
        &self,
        array_shape: &[u64],
        element_size: usize,
    ) -> Vec<ByteRange> {
        let mut byte_ranges: Vec<ByteRange> = Vec::new();
        for (array_index, contiguous_elements) in
            self.iter_contiguous_linearised_indices_unchecked(array_shape)
        {
            let byte_index = array_index * element_size as u64;
            let byte_length = contiguous_elements * element_size as u64;
            byte_ranges.push(ByteRange::FromStart(byte_index, Some(byte_length)));
        }
        byte_ranges
    }

    /// Return the bytes in this array subset from an array with shape `array_shape` and `element_size`.
    ///
    /// # Errors
    ///
    /// Returns [`ArrayExtractBytesError`] if the length of `array_shape` does not match the array subset dimensionality or the array subset is outside of the bounds of `array_shape`.
    ///
    /// # Panics
    ///
    /// Panics if attempting to access a byte index beyond [`usize::MAX`].
    pub fn extract_bytes(
        &self,
        bytes: &[u8],
        array_shape: &[u64],
        element_size: usize,
    ) -> Result<Vec<u8>, ArrayExtractBytesError> {
        let element_size_u64 = element_size as u64;
        if bytes.len() as u64 == array_shape.iter().product::<u64>() * element_size_u64 {
            let mut bytes_subset: Vec<u8> = Vec::with_capacity(
                usize::try_from(self.num_elements() * element_size_u64).unwrap(),
            );
            for (array_index, contiguous_elements) in self
                .iter_contiguous_linearised_indices(array_shape)
                .map_err(|err| ArrayExtractBytesError(err.1, err.0, element_size))?
            {
                let byte_index = usize::try_from(array_index * element_size_u64).unwrap();
                let byte_length = usize::try_from(contiguous_elements * element_size_u64).unwrap();
                debug_assert!(byte_index + byte_length <= bytes.len());
                bytes_subset.extend(&bytes[byte_index..byte_index + byte_length]);
            }
            Ok(bytes_subset)
        } else {
            Err(ArrayExtractBytesError(
                self.clone(),
                array_shape.to_vec(),
                element_size,
            ))
        }
    }

    /// Return the bytes in this array subset from an array with shape `array_shape` and `element_size`.
    ///
    /// # Safety
    ///
    /// The length of `array_shape` must match the array subset dimensionality and the array subset must be within the bounds of `array_shape`.
    ///
    /// # Panics
    ///
    /// Panics if attempting to reference a byte beyond `usize::MAX`.
    #[doc(hidden)]
    #[must_use]
    pub unsafe fn extract_bytes_unchecked(
        &self,
        bytes: &[u8],
        array_shape: &[u64],
        element_size: usize,
    ) -> Vec<u8> {
        let element_size = element_size as u64;
        debug_assert_eq!(
            bytes.len() as u64,
            array_shape.iter().product::<u64>() * element_size
        );
        let mut bytes_subset: Vec<u8> =
            Vec::with_capacity(usize::try_from(self.num_elements() * element_size).unwrap());
        for (array_index, contiguous_elements) in
            self.iter_contiguous_linearised_indices_unchecked(array_shape)
        {
            let byte_index = usize::try_from(array_index * element_size).unwrap();
            let byte_length = usize::try_from(contiguous_elements * element_size).unwrap();
            debug_assert!(byte_index + byte_length <= bytes.len());
            bytes_subset.extend(&bytes[byte_index..byte_index + byte_length]);
        }
        bytes_subset
    }

    /// Store `bytes_subset` corresponding to the bytes of an array (`array_bytes`) with shape `array_shape` and `element_size`.
    ///
    /// # Errors
    ///
    /// Returns [`ArrayStoreBytesError`] if:
    ///  - the length of `array_shape` does not match the array subset dimensionality or the array subset is outside of the bounds of `array_shape`.
    ///  - the length of `bytes_array` is not compatible with the `array_shape` and `element size`, or
    ///  - the length of `bytes_subset` is not compatible with the shape of this subset and `element_size`.
    ///
    /// # Panics
    ///
    /// Panics if attempting to reference a byte beyond `usize::MAX`.
    pub fn store_bytes(
        &self,
        bytes_subset: &[u8],
        bytes_array: &mut [u8],
        array_shape: &[u64],
        element_size: usize,
    ) -> Result<(), ArrayStoreBytesError> {
        let element_size_u64 = element_size as u64;
        let expected_subset_size = self.num_elements() * element_size_u64;
        let expected_array_size = array_shape.iter().product::<u64>() * element_size_u64;
        if bytes_subset.len() as u64 != expected_subset_size {
            Err(ArrayStoreBytesError::InvalidSubsetBytes(
                bytes_subset.len(),
                usize::try_from(expected_subset_size).unwrap(),
            ))
        } else if bytes_array.len() as u64 != expected_array_size {
            Err(ArrayStoreBytesError::InvalidSubsetBytes(
                bytes_array.len(),
                usize::try_from(expected_array_size).unwrap(),
            ))
        } else {
            let mut offset = 0;
            for (array_index, contiguous_elements) in self
                .iter_contiguous_linearised_indices(array_shape)
                .map_err(|err| ArrayStoreBytesError::InvalidArrayShape(err.1, err.0))?
            {
                let byte_index = usize::try_from(array_index * element_size_u64).unwrap();
                let byte_length = usize::try_from(contiguous_elements * element_size_u64).unwrap();
                debug_assert!(byte_index + byte_length <= bytes_array.len());
                debug_assert!(offset + byte_length <= bytes_subset.len());
                bytes_array[byte_index..byte_index + byte_length]
                    .copy_from_slice(&bytes_subset[offset..offset + byte_length]);
                offset += byte_length;
            }
            Ok(())
        }
    }

    /// Store `bytes_subset` corresponding to the bytes of an array (`array_bytes`) with shape `array_shape` and `element_size`.
    ///
    /// # Safety
    ///
    /// The length of `array_shape` must match the array subset dimensionality and the array subset must be within the bounds of `array_shape`.
    /// The length of `bytes_array` must match the product of the `array_shape` components and `element_size`.
    /// The length of `bytes_subset` must match the product of the array subset shape components and `element_size`.
    ///
    /// # Panics
    ///
    /// Panics if attempting to reference a byte beyond `usize::MAX`.
    pub unsafe fn store_bytes_unchecked(
        &self,
        bytes_subset: &[u8],
        bytes_array: &mut [u8],
        array_shape: &[u64],
        element_size: usize,
    ) {
        let element_size_u64 = element_size as u64;
        debug_assert_eq!(
            bytes_subset.len() as u64,
            self.num_elements() * element_size_u64
        );
        debug_assert_eq!(
            bytes_array.len() as u64,
            array_shape.iter().product::<u64>() * element_size_u64
        );
        let mut offset = 0;
        for (array_index, contiguous_elements) in
            self.iter_contiguous_linearised_indices_unchecked(array_shape)
        {
            let byte_index = usize::try_from(array_index * element_size_u64).unwrap();
            let byte_length = usize::try_from(contiguous_elements * element_size_u64).unwrap();
            debug_assert!(byte_index + byte_length <= bytes_array.len());
            debug_assert!(offset + byte_length <= bytes_subset.len());
            bytes_array[byte_index..byte_index + byte_length]
                .copy_from_slice(&bytes_subset[offset..offset + byte_length]);
            offset += byte_length;
        }
    }

    /// Returns an iterator over the indices of elements within the subset.
    #[must_use]
    pub fn iter_indices(&self) -> IndicesIterator {
        IndicesIterator::new(self.clone(), Some(self.start.clone()))
    }

    /// Returns an iterator over the linearised indices of elements within the subset.
    ///
    /// # Errors
    ///
    /// Returns [`IncompatibleArrayShapeError`] if the `array_shape` does not encapsulate this array subset.
    pub fn iter_linearised_indices<'a>(
        &self,
        array_shape: &'a [u64],
    ) -> Result<LinearisedIndicesIterator<'a>, IncompatibleArrayShapeError> {
        LinearisedIndicesIterator::new(self.iter_indices(), array_shape)
    }

    /// Returns an iterator over the indices of elements within the subset.
    ///
    /// # Safety
    ///
    /// `array_shape` must match the dimensionality and encapsulate this array subset.
    #[doc(hidden)]
    #[must_use]
    pub unsafe fn iter_linearised_indices_unchecked<'a>(
        &'a self,
        array_shape: &'a [u64],
    ) -> LinearisedIndicesIterator<'a> {
        LinearisedIndicesIterator::new_unchecked(self.iter_indices(), array_shape)
    }

    /// Returns an iterator over the indices of contiguous elements within the subset.
    ///
    /// # Errors
    ///
    /// Returns [`IncompatibleArrayShapeError`] if the `array_shape` does not encapsulate this array subset.
    pub fn iter_contiguous_indices<'a>(
        &'a self,
        array_shape: &'a [u64],
    ) -> Result<ContiguousIndicesIterator, IncompatibleArrayShapeError> {
        ContiguousIndicesIterator::new(self, array_shape, Some(self.start.clone()))
    }

    /// Returns an iterator over the indices of contiguous elements within the subset.
    ///
    /// # Safety
    ///
    /// The length of `array_shape` must match the array subset dimensionality.
    #[doc(hidden)]
    #[must_use]
    pub unsafe fn iter_contiguous_indices_unchecked<'a>(
        &'a self,
        array_shape: &'a [u64],
    ) -> ContiguousIndicesIterator {
        ContiguousIndicesIterator::new_unchecked(self, array_shape, Some(self.start.clone()))
    }

    /// Returns an iterator over the linearised indices of contiguous elements within the subset.
    ///
    /// # Errors
    ///
    /// Returns [`IncompatibleArrayShapeError`] if the `array_shape` does not encapsulate this array subset.
    pub fn iter_contiguous_linearised_indices<'a>(
        &'a self,
        array_shape: &'a [u64],
    ) -> Result<ContiguousLinearisedIndicesIterator, IncompatibleArrayShapeError> {
        Ok(ContiguousLinearisedIndicesIterator::new(
            self.iter_contiguous_indices(array_shape)?,
        ))
    }

    /// Returns an iterator over the linearised indices of contiguous elements within the subset.
    ///
    /// # Safety
    ///
    /// The length of `array_shape` must match the array subset dimensionality.
    #[doc(hidden)]
    #[must_use]
    pub unsafe fn iter_contiguous_linearised_indices_unchecked<'a>(
        &'a self,
        array_shape: &'a [u64],
    ) -> ContiguousLinearisedIndicesIterator {
        ContiguousLinearisedIndicesIterator::new(unsafe {
            self.iter_contiguous_indices_unchecked(array_shape)
        })
    }

    /// Returns an iterator over chunks with shape `chunk_shape` in the array subset.
    ///
    /// All chunks overlapping the array subset are returned, and they all have the same shape `chunk_shape`.
    /// Thus, the subsets of the chunks may extend out over the subset.
    ///
    /// # Errors
    ///
    /// Returns an error if `chunk_shape` does not match the array subset dimensionality.
    pub fn iter_chunks<'a>(
        &'a self,
        chunk_shape: &'a [u64],
    ) -> Result<ChunksIterator, IncompatibleDimensionalityError> {
        let first_chunk = std::iter::zip(self.start(), chunk_shape)
            .map(|(i, s)| i / s)
            .collect();
        ChunksIterator::new(self, chunk_shape, Some(first_chunk))
    }

    /// Returns an iterator over chunks with shape `chunk_shape` in the array subset.
    ///
    /// All chunks overlapping the array subset are returned, and they all have the same shape `chunk_shape`.
    /// Thus, the subsets of the chunks may extend out over the subset.
    ///
    /// # Safety
    ///
    /// The length of `chunk_shape` must match the array subset dimensionality.
    #[doc(hidden)]
    #[must_use]
    pub unsafe fn iter_chunks_unchecked<'a>(&'a self, chunk_shape: &'a [u64]) -> ChunksIterator {
        let first_chunk = std::iter::zip(self.start(), chunk_shape)
            .map(|(i, s)| i / s)
            .collect();
        ChunksIterator::new_unchecked(self, chunk_shape, Some(first_chunk))
    }

    /// Return the subset of this array subset in `subset_other`.
    /// The start of the returned array subset is from the start of this array subset.
    ///
    /// # Errors
    ///
    /// Returns [`IncompatibleDimensionalityError`] if the dimensionality of `subset_other` does not match the dimensionality of this array subset.
    pub fn in_subset(
        &self,
        subset_other: &ArraySubset,
    ) -> Result<ArraySubset, IncompatibleDimensionalityError> {
        if subset_other.dimensionality() == self.dimensionality() {
            Ok(unsafe { self.in_subset_unchecked(subset_other) })
        } else {
            Err(IncompatibleDimensionalityError::new(
                subset_other.dimensionality(),
                self.dimensionality(),
            ))
        }
    }

    /// Return the subset of this array subset in `subset_other`.
    /// The start of the returned array subset is from the start of this array subset.
    ///
    /// # Safety
    ///
    /// Panics if the dimensionality of `subset_other` does not match the dimensionality of this array subset.
    #[doc(hidden)]
    #[must_use]
    pub unsafe fn in_subset_unchecked(&self, subset_other: &ArraySubset) -> ArraySubset {
        debug_assert_eq!(subset_other.dimensionality(), self.dimensionality());
        let mut starts = Vec::with_capacity(self.start.len());
        let mut shapes = Vec::with_capacity(self.start.len());
        for (start, size, other_start, other_size) in izip!(
            &self.start,
            &self.shape,
            subset_other.start(),
            subset_other.shape(),
        ) {
            let output_start = start.saturating_sub(*other_start);
            let output_end =
                std::cmp::min((start + size).saturating_sub(*other_start), *other_size);
            let output_size = output_end - output_start;
            starts.push(output_start);
            shapes.push(output_size);
        }
        unsafe { ArraySubset::new_with_start_shape_unchecked(starts, shapes) }
    }
}

/// An incompatible dimensionality error.
#[derive(Copy, Clone, Debug, Error)]
#[error("incompatible dimensionality {0}, expected {1}")]
pub struct IncompatibleDimensionalityError(usize, usize);

/// An incompatible array shape error.
#[derive(Clone, Debug, Error, From)]
#[error("incompatible array shape {0:?} with array subset {1}")]
pub struct IncompatibleArrayShapeError(ArrayShape, ArraySubset);

impl IncompatibleDimensionalityError {
    /// Create a new incompatible dimensionality error.
    #[must_use]
    pub fn new(got: usize, expected: usize) -> Self {
        IncompatibleDimensionalityError(got, expected)
    }
}

/// Returns true if `array_subset` is within the bounds of `array_shape`.
#[must_use]
pub fn validate_array_subset(array_subset: &ArraySubset, array_shape: &[u64]) -> bool {
    if array_subset.dimensionality() != array_shape.len() {
        return false;
    }

    for (subset_start, subset_shape, shape) in
        izip!(array_subset.start(), array_subset.shape(), array_shape)
    {
        if subset_start + subset_shape > *shape {
            return false;
        }
    }
    true
}

/// An invalid array subset error.
#[derive(Copy, Clone, Debug, Error)]
#[error("invalid array subset")]
pub struct InvalidArraySubsetError;
