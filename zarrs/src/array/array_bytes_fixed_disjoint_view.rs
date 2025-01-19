use derive_more::derive::Display;
use thiserror::Error;
use unsafe_cell_slice::UnsafeCellSlice;

use crate::array_subset::{
    iterators::{ContiguousIndices, ContiguousLinearisedIndices},
    ArraySubset,
};

use super::codec::{CodecError, InvalidBytesLengthError, SubsetOutOfBoundsError};

/// A disjoint view of the bytes in an array with a fixed-length data type.
///
/// The `subset` represented by this view must not overlap with the `subset` of any other created views that reference the same array bytes.
pub struct ArrayBytesFixedDisjointView<'a> {
    bytes: UnsafeCellSlice<'a, u8>,
    data_type_size: usize,
    shape: &'a [u64],
    subset: ArraySubset,
    bytes_in_subset_len: usize,
}

/// Errors that can occur when creating a [`ArrayBytesFixedDisjointView`].
#[derive(Debug, Error, Display)]
pub enum ArrayBytesFixedDisjointViewCreateError {
    /// The subset is out-of-bounds of the array shape.
    SubsetOutOfBounds(#[from] SubsetOutOfBoundsError),
    /// The length of the bytes is not the correct length.
    InvalidBytesLength(#[from] InvalidBytesLengthError),
}

impl From<ArrayBytesFixedDisjointViewCreateError> for CodecError {
    fn from(value: ArrayBytesFixedDisjointViewCreateError) -> Self {
        match value {
            ArrayBytesFixedDisjointViewCreateError::SubsetOutOfBounds(e) => e.into(),
            ArrayBytesFixedDisjointViewCreateError::InvalidBytesLength(e) => e.into(),
        }
    }
}

impl<'a> ArrayBytesFixedDisjointView<'a> {
    /// Create a new non-overlapping view of the bytes in an array.
    ///
    /// # Errors
    /// Returns [`ArrayBytesFixedDisjointViewCreateError`] if
    /// - `subset` is out-of-bounds of `shape`, or
    /// - the length of `bytes` is not the product of the elements in `shape` multiplied by `data_type_size`.
    ///
    /// # Safety
    /// The `subset` represented by this view must not overlap with the `subset` of any other created views that reference the same array bytes.
    ///
    /// # Panics
    /// Panics if the product of the elements in `shape` multiplied by `data_type_size` exceeds [`usize::MAX`].
    pub unsafe fn new(
        bytes: UnsafeCellSlice<'a, u8>,
        data_type_size: usize,
        shape: &'a [u64],
        subset: ArraySubset,
    ) -> Result<Self, ArrayBytesFixedDisjointViewCreateError> {
        if !subset.inbounds_shape(shape) {
            let bounding_subset = ArraySubset::new_with_shape(shape.to_vec());
            return Err(SubsetOutOfBoundsError::new(subset, bounding_subset).into());
        }
        let bytes_in_array_len =
            usize::try_from(shape.iter().product::<u64>()).unwrap() * data_type_size;
        if bytes.len() != bytes_in_array_len {
            return Err(InvalidBytesLengthError::new(bytes.len(), bytes_in_array_len).into());
        }

        let bytes_in_subset_len = subset.num_elements_usize() * data_type_size;
        Ok(Self {
            bytes,
            data_type_size,
            shape,
            subset,
            bytes_in_subset_len,
        })
    }

    /// Create a new non-overlapping view of the bytes in an array.
    ///
    /// # Safety
    /// - `subset` must be inbounds of `shape`,
    /// - the length of `bytes` must be the product of the elements in `shape` multiplied by `data_type_size`, and
    /// - the `subset` represented by this view must not overlap with the `subset` of any other created views that reference the same array bytes.
    ///
    /// # Panics
    /// Panics if the product of the elements in `shape` multiplied by `data_type_size` exceeds [`usize::MAX`].
    #[must_use]
    pub unsafe fn new_unchecked(
        bytes: UnsafeCellSlice<'a, u8>,
        data_type_size: usize,
        shape: &'a [u64],
        subset: ArraySubset,
    ) -> Self {
        debug_assert!(subset.inbounds_shape(shape));
        debug_assert_eq!(
            bytes.len(),
            usize::try_from(shape.iter().product::<u64>()).unwrap() * data_type_size
        );

        let bytes_in_subset_len = subset.num_elements_usize() * data_type_size;
        Self {
            bytes,
            data_type_size,
            shape,
            subset,
            bytes_in_subset_len,
        }
    }

    /// Create a new non-overlapping view of the bytes in an array that is a subset of the current view.
    ///
    /// # Errors
    /// Returns [`SubsetOutOfBoundsError`] if `subset` is out-of-bounds of the parent subset.
    ///
    /// # Safety
    /// The `subset` represented by this view must not overlap with the `subset` of any other created views that reference the same array bytes.
    pub unsafe fn subdivide(
        &self,
        subset: ArraySubset,
    ) -> Result<ArrayBytesFixedDisjointView<'a>, SubsetOutOfBoundsError> {
        if !subset.inbounds(&self.subset) {
            return Err(SubsetOutOfBoundsError::new(subset, self.subset.clone()));
        }

        Ok(unsafe {
            // SAFETY: all inputs have been validated
            Self::new_unchecked(self.bytes, self.data_type_size, self.shape, subset)
        })
    }

    /// Create a new non-overlapping view of the bytes in an array that is a subset of the current view.
    ///
    /// # Safety
    /// - `subset` must be inbounds of the parent subset, and
    /// - the `subset` represented by this view must not overlap with the `subset` of any other created views that reference the same array bytes.
    #[must_use]
    pub unsafe fn subdivide_unchecked(
        &self,
        subset: ArraySubset,
    ) -> ArrayBytesFixedDisjointView<'a> {
        debug_assert!(subset.inbounds(&self.subset));

        unsafe { Self::new_unchecked(self.bytes, self.data_type_size, self.shape, subset) }
    }

    /// Return the shape of the bytes this view is created from.
    #[must_use]
    pub fn shape(&self) -> &[u64] {
        self.shape
    }

    /// Return the subset of the bytes this view is created from.
    #[must_use]
    pub fn subset(&self) -> &ArraySubset {
        &self.subset
    }

    /// Return the number of elements in the view.
    #[must_use]
    pub fn num_elements(&self) -> u64 {
        self.subset.num_elements()
    }

    fn contiguous_indices(&self) -> ContiguousIndices {
        unsafe {
            // SAFETY: the output shape encapsulates the output subset, checked in constructor
            self.subset.contiguous_indices_unchecked(self.shape)
        }
    }

    fn contiguous_linearised_indices(&self) -> ContiguousLinearisedIndices {
        unsafe {
            // SAFETY: the output shape encapsulates the output subset, checked in constructor
            self.subset
                .contiguous_linearised_indices_unchecked(self.shape)
        }
    }

    /// Return the contiguous element length of the view.
    ///
    /// This is the number of elements that are accessed in a single contiguous block.
    #[must_use]
    pub fn num_contiguous_elements(&self) -> usize {
        self.contiguous_indices().contiguous_elements_usize()
    }

    /// Return the size in bytes of contiguous elements in the view.
    ///
    /// This is the number of elements that are accessed in a single contiguous block.
    #[must_use]
    pub fn contiguous_bytes_len(&self) -> usize {
        self.contiguous_indices().contiguous_elements_usize() * self.data_type_size
    }

    /// Fill the view with the fill value.
    ///
    /// # Errors
    /// Returns [`InvalidBytesLengthError`] if the length of the `fill_value` does not match the data type size.
    ///
    /// # Panics
    /// Panics if an offset into the internal bytes reference exceeds [`usize::MAX`].
    pub fn fill(&mut self, fill_value: &[u8]) -> Result<(), InvalidBytesLengthError> {
        if fill_value.len() != self.data_type_size {
            return Err(InvalidBytesLengthError::new(
                fill_value.len(),
                self.data_type_size,
            ));
        }

        let fill_value_contiguous = fill_value.repeat(self.num_contiguous_elements());
        let length = self.contiguous_bytes_len();
        debug_assert_eq!(fill_value_contiguous.len(), length);
        let contiguous_indices = self.contiguous_linearised_indices();
        contiguous_indices.into_iter().for_each(|index| {
            let offset = usize::try_from(index * self.data_type_size as u64).unwrap();
            unsafe {
                self.bytes
                    .index_mut(offset..offset + length)
                    .copy_from_slice(&fill_value_contiguous);
            }
        });
        Ok(())
    }

    /// Copy bytes into the view.
    ///
    /// The `subset_bytes` must be the same length as the byte length of the elements in the view.
    ///
    /// # Errors
    /// Returns an [`InvalidBytesLengthError`] if the length of `subset_bytes` is not the same as the byte length of the elements in the view.
    ///
    /// # Panics
    /// Panics if an offset into the internal bytes reference exceeds [`usize::MAX`].
    pub fn copy_from_slice(&mut self, subset_bytes: &[u8]) -> Result<(), InvalidBytesLengthError> {
        if subset_bytes.len() != self.bytes_in_subset_len {
            return Err(InvalidBytesLengthError::new(
                self.bytes.len(),
                self.bytes_in_subset_len,
            ));
        }

        let contiguous_indices = self.contiguous_linearised_indices();
        let length = contiguous_indices.contiguous_elements_usize() * self.data_type_size;

        let bytes_copied = contiguous_indices.into_iter().fold(
            0,
            |subset_offset: usize, array_subset_element_index: u64| {
                let output_offset =
                    usize::try_from(array_subset_element_index).unwrap() * self.data_type_size;
                debug_assert!((output_offset + length) <= self.bytes.len());
                debug_assert!((subset_offset + length) <= subset_bytes.len());
                let subset_offset_end = subset_offset + length;
                unsafe {
                    self.bytes
                        .index_mut(output_offset..output_offset + length)
                        .copy_from_slice(&subset_bytes[subset_offset..subset_offset_end]);
                }
                subset_offset_end
            },
        );
        debug_assert_eq!(bytes_copied, subset_bytes.len());

        Ok(())
    }
}
