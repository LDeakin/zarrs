use std::borrow::Cow;

use itertools::Itertools;
use thiserror::Error;

use crate::{
    array_subset::{ArraySubset, IncompatibleArraySubsetAndShapeError},
    byte_range::extract_byte_ranges_concat_unchecked,
};

use super::{
    codec::CodecError, ravel_indices, ArrayShape, ArraySize, DataType, DataTypeSize, FillValue,
};

/// Array element bytes.
pub type RawBytes<'a> = Cow<'a, [u8]>;

/// Array element byte offsets.
pub type RawBytesOffsets<'a> = Cow<'a, [usize]>;

/// Fixed or variable length array bytes.
///
/// Offsets are [`None`] if bytes are composed of fixed size data types.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ArrayBytes<'a> {
    /// Bytes for a fixed length array.
    Fixed(RawBytes<'a>),
    /// Bytes and element byte offsets for a variable length array.
    Variable(RawBytes<'a>, RawBytesOffsets<'a>),
}

/// Errors related to [`ArrayBytes<'_>`] and [`ArrayBytes`].
#[derive(Debug, Error)]
pub enum ArrayBytesError {
    /// Invalid use of a fixed length method.
    #[error("Used a fixed length (flen) method on a variable length (vlen) array")]
    UsedFixedLengthMethodOnVariableLengthArray,
}

impl<'a> ArrayBytes<'a> {
    /// Create a new fixed length array bytes from `bytes`.
    pub fn new_flen(bytes: impl Into<RawBytes<'a>>) -> Self {
        Self::Fixed(bytes.into())
    }

    /// Create a new variable length array bytes from `bytes` and `offsets`.
    pub fn new_vlen(
        bytes: impl Into<RawBytes<'a>>,
        offsets: impl Into<RawBytesOffsets<'a>>,
    ) -> Self {
        Self::Variable(bytes.into(), offsets.into())
    }

    /// Create a new [`ArrayBytes`] with `num_elements` composed entirely of the `fill_value`.
    ///
    /// # Panics
    /// Panics if the number of elements in `array_size` exceeds [`usize::MAX`].
    #[must_use]
    pub fn new_fill_value(array_size: ArraySize, fill_value: &FillValue) -> Self {
        match array_size {
            ArraySize::Fixed {
                num_elements,
                data_type_size: _,
            } => {
                let num_elements = usize::try_from(num_elements).unwrap();
                Self::new_flen(fill_value.as_ne_bytes().repeat(num_elements))
            }
            ArraySize::Variable { num_elements } => {
                let num_elements = usize::try_from(num_elements).unwrap();
                Self::new_vlen(
                    fill_value.as_ne_bytes().repeat(num_elements),
                    (0..=num_elements)
                        .map(|i| i * fill_value.size())
                        .collect::<Vec<_>>(),
                )
            }
        }
    }

    /// Convert the array bytes into fixed size bytes.
    ///
    /// # Errors
    /// Returns a [`CodecError::ExpectedFixedLengthBytes`] if the bytes are variable length.
    pub fn into_fixed(self) -> Result<RawBytes<'a>, CodecError> {
        match self {
            Self::Fixed(bytes) => Ok(bytes),
            Self::Variable(_, _) => Err(CodecError::ExpectedFixedLengthBytes),
        }
    }

    /// Convert the array bytes into variable sized bytes and element byte offsets.
    ///
    /// # Errors
    /// Returns a [`CodecError::ExpectedVariableLengthBytes`] if the bytes are fixed length.
    pub fn into_variable(self) -> Result<(RawBytes<'a>, RawBytesOffsets<'a>), CodecError> {
        match self {
            Self::Fixed(_) => Err(CodecError::ExpectedVariableLengthBytes),
            Self::Variable(bytes, offsets) => Ok((bytes, offsets)),
        }
    }

    /// Returns the size (in bytes) of the underlying element bytes.
    ///
    /// This only considers the size of the element bytes, and does not include the element offsets for a variable sized array.
    #[must_use]
    pub fn size(&self) -> usize {
        match self {
            Self::Fixed(bytes) | Self::Variable(bytes, _) => bytes.len(),
        }
    }

    /// Return the byte offsets for variable sized bytes. Returns [`None`] for fixed size bytes.
    #[must_use]
    pub fn offsets(&self) -> Option<&RawBytesOffsets<'a>> {
        match self {
            Self::Fixed(_) => None,
            Self::Variable(_, offsets) => Some(offsets),
        }
    }

    /// Convert into owned [`ArrayBytes<'_>`].
    #[must_use]
    pub fn into_owned<'b>(self) -> ArrayBytes<'b> {
        match self {
            Self::Fixed(bytes) => ArrayBytes::<'b>::new_flen(bytes.into_owned()),
            Self::Variable(bytes, offsets) => {
                ArrayBytes::<'b>::new_vlen(bytes.into_owned(), offsets.into_owned())
            }
        }
    }

    /// Validate that the array has a valid encoding.
    ///
    /// For a fixed-length array, check it matches the expected size.
    /// For a variable-length array, check that the offsets are monotonically increasing and the largest offset is equal to the array length.
    /// Always returns without error for an array with fixed-length data.
    ///
    /// # Errors
    /// Returns an error if the array is not valid.
    pub fn validate(
        &self,
        num_elements: u64,
        data_type_size: DataTypeSize,
    ) -> Result<(), CodecError> {
        validate_bytes(self, num_elements, data_type_size)
    }

    /// Returns [`true`] if the array is empty for the given fill value.
    #[must_use]
    pub fn is_fill_value(&self, fill_value: &FillValue) -> bool {
        match self {
            Self::Fixed(bytes) => fill_value.equals_all(bytes),
            Self::Variable(bytes, _offsets) => fill_value.equals_all(bytes),
        }
    }

    /// Extract a subset of the array bytes.
    ///
    /// # Errors
    /// Returns a [`CodecError::InvalidArraySubsetError`] if the `array_shape` is incompatible with `subset`.
    ///
    /// # Panics
    /// Panics if indices in the subset exceed [`usize::MAX`].
    pub fn extract_array_subset(
        &self,
        subset: &ArraySubset,
        array_shape: &[u64],
        data_type: &DataType,
    ) -> Result<ArrayBytes<'_>, CodecError> {
        match self {
            ArrayBytes::Variable(bytes, offsets) => {
                let indices = subset.linearised_indices(array_shape).map_err(|_| {
                    IncompatibleArraySubsetAndShapeError::new(subset.clone(), array_shape.to_vec())
                })?;
                let mut bytes_length = 0;
                for index in &indices {
                    let index = usize::try_from(index).unwrap();
                    let curr = offsets[index];
                    let next = offsets[index + 1];
                    debug_assert!(next >= curr);
                    bytes_length += next - curr;
                }
                let mut ss_bytes = Vec::with_capacity(bytes_length);
                let mut ss_offsets = Vec::with_capacity(1 + indices.len());
                for index in &indices {
                    let index = usize::try_from(index).unwrap();
                    let curr = offsets[index];
                    let next = offsets[index + 1];
                    ss_offsets.push(ss_bytes.len());
                    ss_bytes.extend_from_slice(&bytes[curr..next]);
                }
                ss_offsets.push(ss_bytes.len());
                Ok(ArrayBytes::new_vlen(ss_bytes, ss_offsets))
            }
            ArrayBytes::Fixed(bytes) => {
                let byte_ranges =
                    subset.byte_ranges(array_shape, data_type.fixed_size().unwrap())?;
                let bytes = unsafe { extract_byte_ranges_concat_unchecked(bytes, &byte_ranges) };
                Ok(ArrayBytes::new_flen(bytes))
            }
        }
    }
}

/// Validate fixed length array bytes for a given array size.
fn validate_bytes_flen(bytes: &RawBytes, array_size: u64) -> Result<(), CodecError> {
    if bytes.len() as u64 == array_size {
        Ok(())
    } else {
        Err(CodecError::UnexpectedChunkDecodedSize(
            bytes.len(),
            array_size,
        ))
    }
}

/// Validate variable length array bytes for an array with `num_elements`.
fn validate_bytes_vlen(
    bytes: &RawBytes,
    offsets: &RawBytesOffsets,
    num_elements: u64,
) -> Result<(), CodecError> {
    if offsets.len() as u64 != num_elements + 1 {
        return Err(CodecError::InvalidVariableSizedArrayOffsets);
    }
    let len = bytes.len();
    let mut offset_last = 0;
    for offset in offsets.iter() {
        if *offset < offset_last || *offset > len {
            return Err(CodecError::InvalidVariableSizedArrayOffsets);
        }
        offset_last = *offset;
    }
    if offset_last == len {
        Ok(())
    } else {
        Err(CodecError::InvalidVariableSizedArrayOffsets)
    }
}

/// Validate array bytes.
fn validate_bytes(
    bytes: &ArrayBytes<'_>,
    num_elements: u64,
    data_type_size: DataTypeSize,
) -> Result<(), CodecError> {
    match (bytes, data_type_size) {
        (ArrayBytes::Fixed(bytes), DataTypeSize::Fixed(data_type_size)) => {
            validate_bytes_flen(bytes, num_elements * data_type_size as u64)
        }
        (ArrayBytes::Variable(bytes, offsets), DataTypeSize::Variable) => {
            validate_bytes_vlen(bytes, offsets, num_elements)
        }
        (ArrayBytes::Fixed(_), DataTypeSize::Variable) => Err(CodecError::Other(
            "Used fixed length array bytes with a variable sized data type.".to_string(),
        )),
        (ArrayBytes::Variable(_, _), DataTypeSize::Fixed(_)) => Err(CodecError::Other(
            "Used variable length array bytes with a fixed length data type.".to_string(),
        )),
    }
}

/// This function is used internally by various array/codec methods to write the bytes of a chunk subset into an output with an associated array subset.
/// This approach only works for fixed length data types.
pub fn update_bytes_flen(
    output_bytes: &mut [u8],
    output_shape: &[u64],
    subset_bytes: &RawBytes,
    subset: &ArraySubset,
    data_type_size: usize,
) {
    debug_assert_eq!(
        output_bytes.len(),
        usize::try_from(output_shape.iter().product::<u64>()).unwrap() * data_type_size
    );
    debug_assert_eq!(
        subset_bytes.len(),
        subset.num_elements_usize() * data_type_size,
    );

    let contiguous_indices =
        unsafe { subset.contiguous_linearised_indices_unchecked(output_shape) };
    let length = contiguous_indices.contiguous_elements_usize() * data_type_size;
    let mut decoded_offset = 0;
    // TODO: Par iteration?
    for (array_subset_element_index, _num_elements) in &contiguous_indices {
        let output_offset = usize::try_from(array_subset_element_index).unwrap() * data_type_size;
        debug_assert!((output_offset + length) <= output_bytes.len());
        debug_assert!((decoded_offset + length) <= subset_bytes.len());
        output_bytes[output_offset..output_offset + length]
            .copy_from_slice(&subset_bytes[decoded_offset..decoded_offset + length]);
        decoded_offset += length;
    }
}

pub fn update_bytes_vlen<'a>(
    output_bytes: &RawBytes,
    output_offsets: &RawBytesOffsets,
    output_shape: ArrayShape,
    subset_bytes: &RawBytes,
    subset_offsets: &RawBytesOffsets,
    subset: &ArraySubset,
) -> ArrayBytes<'a> {
    // Get the current and new length of the bytes in the chunk subset
    let size_subset_new = {
        let chunk_subset_indices = ArraySubset::new_with_shape(subset.shape().to_vec())
            .linearised_indices(subset.shape())
            .unwrap();
        chunk_subset_indices
            .iter()
            .map(|index| {
                let index = usize::try_from(index).unwrap();
                subset_offsets[index + 1] - subset_offsets[index]
            })
            .sum::<usize>()
    };
    let size_subset_old = {
        let chunk_indices = subset.linearised_indices(&output_shape).unwrap();
        chunk_indices
            .iter()
            .map(|index| {
                let index = usize::try_from(index).unwrap();
                output_offsets[index + 1] - output_offsets[index]
            })
            .sum::<usize>()
    };

    // Populate new offsets and bytes
    let mut offsets_new = Vec::with_capacity(output_offsets.len());
    let bytes_new_len = (output_bytes.len() + size_subset_new)
        .checked_sub(size_subset_old)
        .unwrap();
    let mut bytes_new = Vec::with_capacity(bytes_new_len);
    let indices = ArraySubset::new_with_shape(output_shape).indices();
    for (chunk_index, indices) in indices.iter().enumerate() {
        offsets_new.push(bytes_new.len());
        if subset.contains(&indices) {
            let subset_indices = indices
                .iter()
                .zip(subset.start())
                .map(|(i, s)| i - s)
                .collect::<Vec<_>>();
            let subset_index =
                usize::try_from(ravel_indices(&subset_indices, subset.shape())).unwrap();
            let start = subset_offsets[subset_index];
            let end = subset_offsets[subset_index + 1];
            bytes_new.extend_from_slice(&subset_bytes[start..end]);
        } else {
            let start = output_offsets[chunk_index];
            let end = output_offsets[chunk_index + 1];
            bytes_new.extend_from_slice(&output_bytes[start..end]);
        }
    }
    offsets_new.push(bytes_new.len());

    ArrayBytes::new_vlen(bytes_new, offsets_new)
}

/// Update the intersecting subset of the chunk
/// This function is used internally by [`store_chunk_subset_opt`] and [`async_store_chunk_subset_opt`]
pub fn update_array_bytes<'a>(
    output_bytes: ArrayBytes,
    output_shape: ArrayShape,
    subset_bytes: ArrayBytes,
    subset: &ArraySubset,
    data_type_size: DataTypeSize,
) -> ArrayBytes<'a> {
    match (output_bytes, subset_bytes, data_type_size) {
        (
            ArrayBytes::Variable(chunk_bytes, chunk_offsets),
            ArrayBytes::Variable(chunk_subset_bytes, chunk_subset_offsets),
            DataTypeSize::Variable,
        ) => update_bytes_vlen(
            &chunk_bytes,
            &chunk_offsets,
            output_shape,
            &chunk_subset_bytes,
            &chunk_subset_offsets,
            subset,
        ),
        (
            ArrayBytes::Fixed(chunk_bytes),
            ArrayBytes::Fixed(chunk_subset_bytes),
            DataTypeSize::Fixed(data_type_size),
        ) => {
            let mut chunk_bytes = chunk_bytes.into_owned();
            update_bytes_flen(
                &mut chunk_bytes,
                &output_shape,
                &chunk_subset_bytes,
                subset,
                data_type_size,
            );
            ArrayBytes::new_flen(chunk_bytes)
        }
        (_, _, _) => {
            unreachable!("Validation should occur outside of this function")
        }
    }
}

/// Merge a set of chunks into an array subset.
///
/// This function is used internally by [`retrieve_array_subset_opt`] and [`async_retrieve_array_subset_opt`].
pub fn merge_chunks_vlen<'a>(
    chunk_bytes_and_subsets: Vec<(ArrayBytes<'_>, ArraySubset)>,
    array_shape: &[u64],
) -> Result<ArrayBytes<'a>, CodecError> {
    let num_elements = usize::try_from(array_shape.iter().product::<u64>()).unwrap();

    #[cfg(debug_assertions)]
    {
        // Validate the input
        let mut element_in_input = vec![0; num_elements];
        for (_, chunk_subset) in &chunk_bytes_and_subsets {
            // println!("{chunk_subset:?}");
            let indices = chunk_subset.linearised_indices(array_shape).unwrap();
            for idx in &indices {
                let idx = usize::try_from(idx).unwrap();
                element_in_input[idx] += 1;
            }
        }
        assert!(element_in_input.iter().all(|v| *v == 1));
    }

    // Get the size of each element
    // TODO: Go parallel
    let mut element_sizes = vec![0; num_elements];
    for (chunk_bytes, chunk_subset) in &chunk_bytes_and_subsets {
        let chunk_offsets = chunk_bytes.offsets().unwrap();
        debug_assert_eq!(chunk_offsets.len() as u64, chunk_subset.num_elements() + 1);
        let indices = chunk_subset.linearised_indices(array_shape).unwrap();
        debug_assert_eq!(chunk_offsets.len(), indices.len() + 1);
        for (subset_idx, (curr, next)) in indices.iter().zip(chunk_offsets.iter().tuple_windows()) {
            debug_assert!(next >= curr);
            let subset_idx = usize::try_from(subset_idx).unwrap();
            element_sizes[subset_idx] = next - curr;
        }
    }

    // Convert to offsets with a cumulative sum
    // TODO: Parallel cum sum
    let mut offsets = Vec::with_capacity(element_sizes.len() + 1);
    offsets.push(0); // first offset is always zero
    offsets.extend(element_sizes.iter().scan(0, |acc, &sz| {
        *acc += sz;
        Some(*acc)
    }));

    // Write bytes
    // TODO: Go parallel
    let mut bytes = vec![0; *offsets.last().unwrap()];
    for (chunk_bytes, chunk_subset) in chunk_bytes_and_subsets {
        let (chunk_bytes, chunk_offsets) = chunk_bytes.into_variable()?;
        let indices = chunk_subset.linearised_indices(array_shape).unwrap();
        for (subset_idx, (&chunk_curr, &chunk_next)) in
            indices.iter().zip(chunk_offsets.iter().tuple_windows())
        {
            let subset_idx = usize::try_from(subset_idx).unwrap();
            let subset_curr = offsets[subset_idx];
            let subset_next = offsets[subset_idx + 1];
            bytes[subset_curr..subset_next].copy_from_slice(&chunk_bytes[chunk_curr..chunk_next]);
        }
    }

    Ok(ArrayBytes::new_vlen(bytes, offsets))
}

pub fn extract_decoded_regions_vlen<'a>(
    bytes: &[u8],
    offsets: &[usize],
    decoded_regions: &[ArraySubset],
    array_shape: &[u64],
) -> Result<Vec<ArrayBytes<'a>>, CodecError> {
    let mut out = Vec::with_capacity(decoded_regions.len());
    for decoded_region in decoded_regions {
        let indices = decoded_region.linearised_indices(array_shape)?;
        let mut region_bytes_len = 0;
        for index in &indices {
            let index = usize::try_from(index).unwrap();
            let curr = offsets[index];
            let next = offsets[index + 1];
            debug_assert!(next >= curr);
            region_bytes_len += next - curr;
        }
        let mut region_offsets = Vec::with_capacity(decoded_region.num_elements_usize() + 1);
        let mut region_bytes = Vec::with_capacity(region_bytes_len);
        for index in &indices {
            region_offsets.push(region_bytes.len());
            let index = usize::try_from(index).unwrap();
            let curr = offsets[index];
            let next = offsets[index + 1];
            region_bytes.extend_from_slice(&bytes[curr..next]);
        }
        region_offsets.push(region_bytes.len());
        out.push(ArrayBytes::new_vlen(region_bytes, region_offsets));
    }
    Ok(out)
}

impl<'a> From<RawBytes<'a>> for ArrayBytes<'a> {
    fn from(bytes: RawBytes<'a>) -> Self {
        Self::new_flen(bytes)
    }
}

// impl<'a, 'b> From<&ArrayBytes<'a>> for ArrayBytes<'b> {
//     fn from(bytes: &ArrayBytes<'a>) -> Self {
//         match bytes {
//             Self::Fixed(bytes) => {
//                 let bytes = bytes.to_vec();
//                 ArrayBytes::<'b>::new_flen(bytes)
//             },
//             Self::Variable(bytes, offsets) => {
//                 let bytes: RawBytes<'b> = bytes.to_vec().into();
//                 let offsets: RawBytesOffsets<'b> = offsets.to_vec().into();
//                 ArrayBytes::new_vlen(bytes, offsets)
//             }
//         }
//     }
// }

// impl<'a> From<ArrayBytes<'_>> for ArrayBytes<'a> {
//     fn from(bytes: ArrayBytes<'_>) -> Self {
//         match bytes {
//             ArrayBytes::Fixed(bytes) => ArrayBytes::new_flen(bytes)
//             ArrayBytes::Variable(bytes, offsets) => ArrayBytes::new_vlen(bytes, offsets)
//         }
//     }
// }

impl<'a> From<&'a [u8]> for ArrayBytes<'a> {
    fn from(bytes: &'a [u8]) -> Self {
        ArrayBytes::new_flen(bytes)
    }
}

impl From<Vec<u8>> for ArrayBytes<'_> {
    fn from(bytes: Vec<u8>) -> Self {
        ArrayBytes::new_flen(bytes)
    }
}

impl<'a, const N: usize> From<&'a [u8; N]> for ArrayBytes<'a> {
    fn from(bytes: &'a [u8; N]) -> Self {
        // NOTE: as_slice() is needed for rust <1.77
        ArrayBytes::new_flen(bytes.as_slice())
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::mem::size_of;

    use crate::array::Element;

    use super::*;

    #[test]
    fn array_bytes_flen() -> Result<(), Box<dyn Error>> {
        let data = [0u32, 1, 2, 3, 4];
        let bytes = Element::into_array_bytes(&DataType::UInt32, &data)?;
        let ArrayBytes::Fixed(bytes) = bytes else {
            panic!()
        };
        assert_eq!(bytes.len(), size_of::<u32>() * data.len());

        Ok(())
    }

    #[test]
    fn array_bytes_str() -> Result<(), Box<dyn Error>> {
        let data = ["a", "bb", "ccc"];
        let bytes = Element::into_array_bytes(&DataType::String, &data)?;
        let ArrayBytes::Variable(bytes, offsets) = bytes else {
            panic!()
        };
        assert_eq!(bytes, "abbccc".as_bytes());
        assert_eq!(*offsets, [0, 1, 3, 6]);

        Ok(())
    }

    #[test]
    fn test_flen_update_subset() {
        let mut bytes_array = vec![0u8; 4 * 4];
        update_bytes_flen(
            &mut bytes_array,
            &vec![4, 4],
            &vec![1u8, 2].into(),
            &ArraySubset::new_with_ranges(&[1..2, 1..3]),
            1,
        );

        update_bytes_flen(
            &mut bytes_array,
            &vec![4, 4],
            &vec![3u8, 4].into(),
            &ArraySubset::new_with_ranges(&[3..4, 0..2]),
            1,
        );

        debug_assert_eq!(
            bytes_array,
            vec![0, 0, 0, 0, 0, 1, 2, 0, 0, 0, 0, 0, 3, 4, 0, 0]
        );
    }
}
