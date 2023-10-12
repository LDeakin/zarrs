//! The transpose `array->array` codec.
//!
//! Permutes the dimensions of arrays.
//!
//! See <https://zarr-specs.readthedocs.io/en/latest/v3/codecs/transpose/v1.0.html>.

mod transpose_codec;
mod transpose_configuration;
mod transpose_partial_decoder;

pub use transpose_codec::{InvalidPermutationError, TransposeCodec};
pub use transpose_configuration::{
    TransposeCodecConfiguration, TransposeCodecConfigurationV1, TransposeOrder,
};

/// A transpose order.
#[derive(Clone, Eq, PartialEq, Debug)]
pub enum TransposeOrderImpl {
    /// An identity transpose (no-op).
    Identity,
    /// The axis permutation for a transpose.
    Permutation(Vec<usize>),
}

/// Returns the permutation order for an "F" transpose without transposing the last dimension.
fn calculate_permutation_f(array_dimensions: usize) -> Vec<usize> {
    // + 1 for the "bytes" dimension
    let mut permutation: Vec<usize> = vec![0; array_dimensions + 1];
    permutation
        .iter_mut()
        .rev()
        .skip(1)
        .enumerate()
        .for_each(|(i, p)| *p = i);
    *permutation.last_mut().unwrap() = array_dimensions;
    permutation
}

fn calculate_order_encode(order: &TransposeOrder, array_dimensions: usize) -> TransposeOrderImpl {
    match order {
        TransposeOrder::C => TransposeOrderImpl::Identity,
        TransposeOrder::F => {
            TransposeOrderImpl::Permutation(calculate_permutation_f(array_dimensions))
        }
        TransposeOrder::Permutation(array) => {
            assert_eq!(array.len(), array_dimensions);
            let mut permutation_encode = Vec::<usize>::with_capacity(array_dimensions + 1);
            permutation_encode.extend(array);
            permutation_encode.push(array_dimensions);
            TransposeOrderImpl::Permutation(permutation_encode)
        }
    }
}

fn calculate_order_decode(order: &TransposeOrder, array_dimensions: usize) -> TransposeOrderImpl {
    match order {
        TransposeOrder::C => TransposeOrderImpl::Identity,
        TransposeOrder::F => {
            TransposeOrderImpl::Permutation(calculate_permutation_f(array_dimensions))
        }
        TransposeOrder::Permutation(array) => {
            assert_eq!(array.len(), array_dimensions);
            let mut permutation_decode = vec![0; array_dimensions + 1];
            for (i, val) in array.iter().enumerate() {
                permutation_decode[*val] = i;
            }
            permutation_decode[array_dimensions] = array_dimensions;
            TransposeOrderImpl::Permutation(permutation_decode)
        }
    }
}

fn transpose_array(
    transpose_order: &TransposeOrderImpl,
    untransposed_shape: &[u64],
    bytes_per_element: usize,
    data: &[u8],
) -> Result<Vec<u8>, ndarray::ShapeError> {
    match transpose_order {
        TransposeOrderImpl::Identity => Ok(data.to_vec()),
        TransposeOrderImpl::Permutation(permutation) => {
            // Create an array view of the data
            let mut shape_n = Vec::with_capacity(untransposed_shape.len() + 1);
            for size in untransposed_shape {
                shape_n.push(usize::try_from(*size).unwrap());
            }
            shape_n.push(bytes_per_element);
            let array: ndarray::ArrayViewD<u8> = ndarray::ArrayView::from_shape(shape_n, data)?;

            // Transpose the data
            let array_transposed = array.to_owned().permuted_axes(permutation.clone());
            let array_transposed = array_transposed.as_standard_layout();
            Ok(array_transposed.into_owned().into_raw_vec())
        }
    }
}

fn permute(v: &[u64], order: &TransposeOrder) -> Vec<u64> {
    match order {
        TransposeOrder::C => v.to_owned(),
        TransposeOrder::F => {
            let mut shape_encoded = v.to_owned();
            shape_encoded.reverse();
            shape_encoded
        }
        TransposeOrder::Permutation(order) => {
            let mut shape_encoded: crate::array::ArrayShape = Vec::with_capacity(v.len());
            for axis in order {
                shape_encoded.push(v[*axis]);
            }
            shape_encoded
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::array::{codec::ArrayCodecTraits, ArrayRepresentation, DataType, FillValue};

    use super::*;

    const JSON_C: &'static str = r#"{
    "order": "C"
}"#;

    const JSON_F: &'static str = r#"{
    "order": "F"
}"#;

    const JSON_ARRAY: &'static str = r#"{
    "order": [0, 2, 1]
}"#;

    fn codec_transpose_round_trip_impl(json: &str, data_type: DataType, fill_value: FillValue) {
        let array_representation =
            ArrayRepresentation::new(vec![2, 2, 3], data_type.clone(), fill_value.clone()).unwrap();
        let bytes: Vec<u8> = (0..array_representation.size()).map(|s| s as u8).collect();

        let configuration: TransposeCodecConfiguration = serde_json::from_str(json).unwrap();
        let codec = TransposeCodec::new_with_configuration(&configuration).unwrap();

        let encoded = codec.encode(bytes.clone(), &array_representation).unwrap();
        let decoded = codec
            .decode(encoded.clone(), &array_representation)
            .unwrap();
        assert_eq!(bytes, decoded);

        // let array = ndarray::ArrayViewD::from_shape(array_representation.shape(), &bytes).unwrap();
        // let array_representation_transpose =
        //     ArrayRepresentation::new(vec![2, 3, 2], data_type.clone(), fill_value.clone()).unwrap();
        // let encoded_array = ndarray::ArrayViewD::from_shape(
        //     array_representation_transpose.shape().to_vec(),
        //     &encoded,
        // )
        // .unwrap();
        // let decoded_array =
        //     ndarray::ArrayViewD::from_shape(array_representation.shape(), &decoded).unwrap();
    }

    #[test]
    fn codec_transpose_round_trip_c() {
        codec_transpose_round_trip_impl(JSON_C, DataType::UInt8, FillValue::from(0u8));
    }

    #[test]
    fn codec_transpose_round_trip_f() {
        codec_transpose_round_trip_impl(JSON_F, DataType::UInt8, FillValue::from(0u8));
    }

    #[test]
    fn codec_transpose_round_trip_array1() {
        codec_transpose_round_trip_impl(JSON_ARRAY, DataType::UInt8, FillValue::from(0u8));
    }

    #[test]
    fn codec_transpose_round_trip_array2() {
        codec_transpose_round_trip_impl(JSON_ARRAY, DataType::UInt16, FillValue::from(0u16));
    }
}
