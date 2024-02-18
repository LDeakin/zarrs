use thiserror::Error;

use crate::array_subset::{ArraySubset, IncompatibleDimensionalityError};

use super::{unsafe_cell_slice::UnsafeCellSlice, ArrayShape};

/// A view of a subset of an array.
///
/// This class has various *hidden* unsafe functions which are used internally.
// TODO: Element size as well for bytes/shape validation? But how to handle variable sized elements in the future?
#[derive(Clone)]
pub struct ArrayView<'a> {
    bytes: UnsafeCellSlice<'a, u8>,
    shape: &'a [u64],
    subset: ArraySubset,
}

/// An array view create error.
#[derive(Debug, Error)]
pub enum ArrayViewCreateError {
    // BytesNotMultipleOfElements???
    /// Array subset is out of bounds of the array shape.
    #[error("the array subset {_0} is out of bounds for array shape {_1:?}")]
    ArraySubsetOutOfBounds(ArraySubset, ArrayShape),
    /// The subset has an incompatible dimensionality to the array shape.
    #[error(transparent)]
    SubsetIncompatibleDimensionality(#[from] IncompatibleDimensionalityError),
}

impl<'a> ArrayView<'a> {
    /// Create a new [`ArrayView`].
    ///
    /// # Errors
    /// Returns an error if the subset is out-of-bounds of the array or the dimensionality of `shape` and `subset` does not match.
    pub fn new(
        bytes: &'a mut [u8],
        shape: &'a [u64],
        subset: ArraySubset,
    ) -> Result<Self, ArrayViewCreateError> {
        if shape.len() != subset.dimensionality() {
            Err(IncompatibleDimensionalityError::new(subset.dimensionality(), shape.len()).into())
        } else if std::iter::zip(subset.end_exc(), shape).any(|(end, &shape)| end > shape) {
            Err(ArrayViewCreateError::ArraySubsetOutOfBounds(
                subset.clone(),
                shape.to_vec(),
            ))
        } else {
            Ok(Self {
                bytes: UnsafeCellSlice::new(bytes),
                shape,
                subset,
            })
        }
    }

    /// Return the subset of the array view.
    #[must_use]
    pub fn subset(&self) -> &ArraySubset {
        &self.subset
    }

    /// Return the array shape of the array view.
    #[must_use]
    pub fn array_shape(&self) -> &[u64] {
        self.shape
    }

    /// **For internal use**. Return a mutable reference to the underlying bytes of the array referenced by the array view.
    ///
    /// # Safety
    /// This returns a mutable slice of the array data despite `self` being a non-mutable reference.
    /// This is unsafe because it can be called multiple times, thus creating multiple mutable references to the same data.
    /// It is the responsibility of the caller not to write to the same slice element from than one thread.
    #[doc(hidden)]
    #[allow(clippy::mut_from_ref)]
    #[must_use]
    pub unsafe fn bytes_mut(&self) -> &mut [u8] {
        self.bytes.get()
    }

    /// **For internal use**. Return a new [`ArrayView`] referencing the same array as `self` but with a new subset relative to the existing view.
    ///
    /// # Safety
    /// This returns a subset of an array view referencing the same data as the parent array view.
    /// This function is considered unsafe because the array view it returns references the same underlying data as `self`.
    /// The safety concerns of [`ArrayView::bytes_mut`] apply between `self` and any views created through this method.
    ///
    /// # Errors
    /// Returns [`ArrayViewCreateError`] if `subset` dimensionality does not match the dimensionality of the view or `subset` extends beyond the bounds of the existing view subset.
    #[doc(hidden)]
    pub unsafe fn subset_view(
        &'a self,
        subset: &ArraySubset,
    ) -> Result<ArrayView<'a>, ArrayViewCreateError> {
        let subset_start = std::iter::zip(self.subset.start(), subset.start())
            .map(|(s0, s1)| s0 + s1)
            .collect::<Vec<_>>();
        let subset_inner =
            ArraySubset::new_with_start_shape(subset_start, subset.shape().to_vec())?;
        if std::iter::zip(subset_inner.end_exc(), self.subset.end_exc())
            .all(|(inner, outer)| inner <= outer)
        {
            Self::new(unsafe { self.bytes_mut() }, self.shape, subset_inner)
        } else {
            Err(ArrayViewCreateError::ArraySubsetOutOfBounds(
                subset.clone(),
                self.subset.shape().to_vec(),
            ))
        }
    }
}
