//! The transpose array to array codec.
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

use crate::{
    array::codec::{Codec, CodecPlugin},
    metadata::Metadata,
    plugin::{PluginCreateError, PluginMetadataInvalidError},
};

/// The identifier for the `transpose` codec.
pub const IDENTIFIER: &str = "transpose";

// Register the codec.
inventory::submit! {
    CodecPlugin::new(IDENTIFIER, is_name_transpose, create_codec_transpose)
}

fn is_name_transpose(name: &str) -> bool {
    name.eq(IDENTIFIER)
}

pub(crate) fn create_codec_transpose(metadata: &Metadata) -> Result<Codec, PluginCreateError> {
    let configuration: TransposeCodecConfiguration = metadata
        .to_configuration()
        .map_err(|_| PluginMetadataInvalidError::new(IDENTIFIER, "codec", metadata.clone()))?;
    let codec = Box::new(TransposeCodec::new_with_configuration(&configuration)?);
    Ok(Codec::ArrayToArray(codec))
}

fn to_vec_unique(v: &[usize]) -> Vec<usize> {
    let mut v = v.to_vec();
    v.sort_unstable();
    v.dedup();
    v
}

fn validate_permutation(permutation: &[usize]) -> bool {
    let permutation_unique = to_vec_unique(permutation);
    !permutation.is_empty()
        && permutation_unique.len() == permutation.len()
        && *permutation_unique.iter().max().unwrap() == permutation.len() - 1
}

fn calculate_order_encode(order: &TransposeOrder, array_dimensions: usize) -> Vec<usize> {
    assert_eq!(order.0.len(), array_dimensions);
    let mut permutation_encode = Vec::<usize>::with_capacity(array_dimensions + 1);
    permutation_encode.extend(&order.0);
    permutation_encode.push(array_dimensions);
    permutation_encode
}

fn calculate_order_decode(order: &TransposeOrder, array_dimensions: usize) -> Vec<usize> {
    assert_eq!(order.0.len(), array_dimensions);
    let mut permutation_decode = vec![0; array_dimensions + 1];
    for (i, val) in order.0.iter().enumerate() {
        permutation_decode[*val] = i;
    }
    permutation_decode[array_dimensions] = array_dimensions;
    permutation_decode
}

fn transpose_array(
    transpose_order: &[usize],
    untransposed_shape: &[u64],
    bytes_per_element: usize,
    data: Vec<u8>,
) -> Result<Vec<u8>, ndarray::ShapeError> {
    // Create an array view of the data
    let mut shape_n = Vec::with_capacity(untransposed_shape.len() + 1);
    for size in untransposed_shape {
        shape_n.push(usize::try_from(*size).unwrap());
    }
    shape_n.push(bytes_per_element);
    let array = ndarray::ArrayD::<u8>::from_shape_vec(shape_n, data)?;

    // Transpose the data
    let array_transposed = array.permuted_axes(transpose_order);
    if array_transposed.is_standard_layout() {
        Ok(array_transposed.into_raw_vec())
    } else {
        Ok(array_transposed
            .as_standard_layout()
            .into_owned()
            .into_raw_vec())
    }
}

fn permute<T: Copy>(v: &[T], order: &TransposeOrder) -> Vec<T> {
    let mut vec = Vec::<T>::with_capacity(v.len());
    for axis in &order.0 {
        vec.push(v[*axis]);
    }
    vec
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use crate::{
        array::{
            codec::{
                ArrayCodecTraits, ArrayToArrayCodecTraits, ArrayToBytesCodecTraits, BytesCodec,
                CodecOptions,
            },
            ChunkRepresentation, DataType, FillValue,
        },
        array_subset::ArraySubset,
    };

    use super::*;

    fn codec_transpose_round_trip_impl(json: &str, data_type: DataType, fill_value: FillValue) {
        let chunk_representation = ChunkRepresentation::new(
            vec![
                NonZeroU64::new(2).unwrap(),
                NonZeroU64::new(2).unwrap(),
                NonZeroU64::new(3).unwrap(),
            ],
            data_type,
            fill_value,
        )
        .unwrap();
        let bytes: Vec<u8> = (0..chunk_representation.size()).map(|s| s as u8).collect();

        let configuration: TransposeCodecConfiguration = serde_json::from_str(json).unwrap();
        let codec = TransposeCodec::new_with_configuration(&configuration).unwrap();

        let encoded = codec
            .encode(
                bytes.clone(),
                &chunk_representation,
                &CodecOptions::default(),
            )
            .unwrap();
        let decoded = codec
            .decode(encoded, &chunk_representation, &CodecOptions::default())
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
    fn codec_transpose_round_trip_array1() {
        const JSON: &str = r#"{
            "order": [0, 2, 1]
        }"#;
        codec_transpose_round_trip_impl(JSON, DataType::UInt8, FillValue::from(0u8));
    }

    #[test]
    fn codec_transpose_round_trip_array2() {
        const JSON: &str = r#"{
            "order": [2, 1, 0]
        }"#;
        codec_transpose_round_trip_impl(JSON, DataType::UInt16, FillValue::from(0u16));
    }

    #[test]
    fn codec_transpose_partial_decode() {
        let codec = TransposeCodec::new(TransposeOrder::new(&[1, 0]).unwrap());

        let elements: Vec<f32> = (0..16).map(|i| i as f32).collect();
        let chunk_representation = ChunkRepresentation::new(
            vec![NonZeroU64::new(4).unwrap(), NonZeroU64::new(4).unwrap()],
            DataType::Float32,
            0.0f32.into(),
        )
        .unwrap();
        let bytes = crate::array::transmute_to_bytes_vec(elements);

        let encoded = codec
            .encode(
                bytes.clone(),
                &chunk_representation,
                &CodecOptions::default(),
            )
            .unwrap();
        let decoded_regions = [
            ArraySubset::new_with_ranges(&[0..4, 0..4]),
            ArraySubset::new_with_ranges(&[1..3, 1..4]),
            ArraySubset::new_with_ranges(&[2..4, 0..2]),
        ];
        let input_handle = Box::new(std::io::Cursor::new(encoded));
        let bytes_codec = BytesCodec::default();
        let input_handle = bytes_codec
            .partial_decoder(
                input_handle,
                &chunk_representation,
                &CodecOptions::default(),
            )
            .unwrap();
        let partial_decoder = codec
            .partial_decoder(
                input_handle,
                &chunk_representation,
                &CodecOptions::default(),
            )
            .unwrap();
        let decoded_partial_chunk = partial_decoder
            .partial_decode_opt(&decoded_regions, &CodecOptions::default())
            .unwrap();
        let decoded_partial_chunk = decoded_partial_chunk
            .into_iter()
            .map(|bytes| crate::array::transmute_from_bytes_vec::<f32>(bytes))
            .collect::<Vec<_>>();
        let answer: &[Vec<f32>] = &[
            vec![
                0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0,
                15.0,
            ],
            vec![5.0, 6.0, 7.0, 9.0, 10.0, 11.0],
            vec![8.0, 9.0, 12.0, 13.0],
        ];
        assert_eq!(answer, decoded_partial_chunk);
    }

    #[cfg(feature = "async")]
    #[tokio::test]
    async fn codec_transpose_async_partial_decode() {
        let codec = TransposeCodec::new(TransposeOrder::new(&[1, 0]).unwrap());

        let elements: Vec<f32> = (0..16).map(|i| i as f32).collect();
        let chunk_representation = ChunkRepresentation::new(
            vec![NonZeroU64::new(4).unwrap(), NonZeroU64::new(4).unwrap()],
            DataType::Float32,
            0.0f32.into(),
        )
        .unwrap();
        let bytes = crate::array::transmute_to_bytes_vec(elements);

        let encoded = codec
            .encode(
                bytes.clone(),
                &chunk_representation,
                &CodecOptions::default(),
            )
            .unwrap();
        let decoded_regions = [
            ArraySubset::new_with_ranges(&[0..4, 0..4]),
            ArraySubset::new_with_ranges(&[1..3, 1..4]),
            ArraySubset::new_with_ranges(&[2..4, 0..2]),
        ];
        let input_handle = Box::new(std::io::Cursor::new(encoded));
        let bytes_codec = BytesCodec::default();
        let input_handle = bytes_codec
            .async_partial_decoder(
                input_handle,
                &chunk_representation,
                &CodecOptions::default(),
            )
            .await
            .unwrap();
        let partial_decoder = codec
            .async_partial_decoder(
                input_handle,
                &chunk_representation,
                &CodecOptions::default(),
            )
            .await
            .unwrap();
        let decoded_partial_chunk = partial_decoder
            .partial_decode_opt(&decoded_regions, &CodecOptions::default())
            .await
            .unwrap();
        let decoded_partial_chunk = decoded_partial_chunk
            .into_iter()
            .map(|bytes| crate::array::transmute_from_bytes_vec::<f32>(bytes))
            .collect::<Vec<_>>();
        let answer: &[Vec<f32>] = &[
            vec![
                0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0,
                15.0,
            ],
            vec![5.0, 6.0, 7.0, 9.0, 10.0, 11.0],
            vec![8.0, 9.0, 12.0, 13.0],
        ];
        assert_eq!(answer, decoded_partial_chunk);
    }
}
