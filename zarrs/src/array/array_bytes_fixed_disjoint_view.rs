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

#[derive(Debug, Error, Display)]
pub enum ArrayBytesFixedDisjointViewCreateError {
    SubsetOutOfBounds(#[from] SubsetOutOfBoundsError),
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
    /// The bytes must not overlap with any other views.
    ///
    /// # Panics
    /// Panics if the product of the elements in `shape` multiplied by `data_type_size` exceeds [`usize::MAX`].
    pub unsafe fn new(
        bytes: UnsafeCellSlice<'a, u8>,
        data_type_size: usize,
        shape: &'a [u64],
        subset: ArraySubset,
    ) -> Result<Self, ArrayBytesFixedDisjointViewCreateError> {
        let bounding_subset = ArraySubset::new_with_shape(shape.to_vec());
        if !subset.inbounds(&bounding_subset) {
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
    /// - `subset` must be inbounds of `shape`.
    /// - The length of `bytes` must be the product of the elements in `shape` multiplied by `data_type_size`.
    /// - The bytes must not overlap with any other views.
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
    /// The bytes must not overlap with any other views.
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
    /// - `subset` must be inbounds of the parent subset.
    /// - The bytes must not overlap with any other views.
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
    pub fn contiguous_elements_size(&self) -> usize {
        self.contiguous_indices().contiguous_elements_usize() * self.data_type_size
    }

    /// Fill the view with a constant value.
    ///
    /// The constant value must be the same length as the byte length of contiguous elements in the view.
    ///
    /// # Errors
    /// Returns [`InvalidBytesLengthError`] if the fill value is not the same length [`Self::contiguous_elements_size`].
    ///
    /// # Panics
    /// Panics if an offset into the internal bytes reference exceeds [`usize::MAX`].
    pub fn fill_from(&mut self, fv: &[u8]) -> Result<(), InvalidBytesLengthError> {
        let contiguous_indices = self.contiguous_linearised_indices();
        let length = self.contiguous_elements_size();
        if fv.len() != length {
            return Err(InvalidBytesLengthError::new(fv.len(), length));
        }
        contiguous_indices.into_iter().for_each(|index| {
            let offset = usize::try_from(index * self.data_type_size as u64).unwrap();
            unsafe {
                self.bytes
                    .index_mut(offset..offset + fv.len())
                    .copy_from_slice(fv);
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
