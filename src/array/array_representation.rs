use super::{data_type::IncompatibleFillValueError, ArrayShape, DataType, FillValue};
use derive_more::Display;

/// The shape, data type, and fill value of an `array`.
#[derive(Clone, Debug, Display)]
#[display(fmt = "{array_shape:?} {data_type} {fill_value}")]
pub struct ArrayRepresentation {
    /// The shape of the array.
    array_shape: ArrayShape,
    /// The data type of the array.
    data_type: DataType,
    /// The fill value of the array.
    fill_value: FillValue,
}

impl ArrayRepresentation {
    /// Create a new [`ArrayRepresentation`].
    ///
    /// # Errors
    ///
    /// Returns [`IncompatibleFillValueError`] if the `data_type` and `fill_value` are incompatible.
    pub fn new(
        array_shape: ArrayShape,
        data_type: DataType,
        fill_value: FillValue,
    ) -> Result<Self, IncompatibleFillValueError> {
        if data_type.size() == fill_value.size() {
            Ok(Self {
                array_shape,
                data_type,
                fill_value,
            })
        } else {
            Err(IncompatibleFillValueError::new(
                data_type.name(),
                fill_value,
            ))
        }
    }

    /// Create a new [`ArrayRepresentation`].
    ///
    /// # Safety
    ///
    /// `data_type` and `fill_value` must be compatible.
    #[doc(hidden)]
    #[must_use]
    pub unsafe fn new_unchecked(
        array_shape: ArrayShape,
        data_type: DataType,
        fill_value: FillValue,
    ) -> Self {
        debug_assert_eq!(data_type.size(), fill_value.size());
        Self {
            array_shape,
            data_type,
            fill_value,
        }
    }

    /// Return the shape of the array.
    #[must_use]
    pub fn shape(&self) -> &[u64] {
        &self.array_shape
    }

    /// Return the data type of the array.
    #[must_use]
    pub const fn data_type(&self) -> &DataType {
        &self.data_type
    }

    /// Return the fill value of the array.
    #[must_use]
    pub const fn fill_value(&self) -> &FillValue {
        &self.fill_value
    }

    /// Return the number of elements in the array.
    ///
    /// Equal to the product of its shape.
    #[must_use]
    pub fn num_elements(&self) -> u64 {
        self.array_shape.iter().product()
    }

    /// Return the number of elements of the array as a `usize`.
    ///
    /// # Panics
    ///
    /// Panics if [`num_elements()`](Self::num_elements()) is greater than [`usize::MAX`].
    #[must_use]
    pub fn num_elements_usize(&self) -> usize {
        usize::try_from(self.num_elements()).unwrap()
    }

    /// Return the element size.
    #[must_use]
    pub fn element_size(&self) -> usize {
        self.fill_value.size()
    }

    /// Return the total size in bytes.
    ///
    /// Equal to the product of each element of its shape and the element size.
    #[must_use]
    pub fn size(&self) -> u64 {
        self.num_elements() * self.element_size() as u64
    }

    /// Return the total size in bytes as a [`usize`].
    ///
    /// Equal to the product of each element of its shape and the element size.
    #[must_use]
    pub fn size_usize(&self) -> usize {
        self.num_elements_usize() * self.element_size()
    }
}
