//! The sharding `array->bytes` codec.
//!
//! See <https://zarr-specs.readthedocs.io/en/latest/v3/codecs/sharding-indexed/v1.0.html>.

mod sharding_codec;
mod sharding_configuration;
mod sharding_partial_decoder;

pub use sharding_configuration::{ShardingCodecConfiguration, ShardingCodecConfigurationV1};

pub use sharding_codec::ShardingCodec;
use thiserror::Error;

use crate::array::{
    codec::{ArrayToBytesCodecTraits, CodecError},
    ArrayRepresentation, ArrayShape, BytesRepresentation, DataType, FillValue,
};

#[derive(Debug, Error)]
#[error("invalid inner chunk shape {chunk_shape:?}, it must evenly divide {shard_shape:?}")]
struct ChunksPerShardError {
    chunk_shape: Vec<u64>,
    shard_shape: Vec<u64>,
}

fn calculate_chunks_per_shard(
    shard_shape: &[u64],
    chunk_shape: &[u64],
) -> Result<ArrayShape, ChunksPerShardError> {
    use num::Integer;

    std::iter::zip(shard_shape, chunk_shape)
        .map(|(s, c)| {
            if s.is_multiple_of(c) {
                Ok(s / c)
            } else {
                Err(ChunksPerShardError {
                    chunk_shape: chunk_shape.to_vec(),
                    shard_shape: shard_shape.to_vec(),
                })
            }
        })
        .collect()
}

fn sharding_index_decoded_representation(chunks_per_shard: &[u64]) -> ArrayRepresentation {
    let mut index_shape = Vec::with_capacity(chunks_per_shard.len() + 1);
    index_shape.extend(chunks_per_shard);
    index_shape.push(2);
    ArrayRepresentation::new(index_shape, DataType::UInt64, FillValue::from(u64::MAX)).unwrap()
}

fn compute_index_encoded_size(
    index_codecs: &dyn ArrayToBytesCodecTraits,
    index_array_representation: &ArrayRepresentation,
) -> Result<u64, CodecError> {
    let bytes_representation = index_codecs.compute_encoded_size(index_array_representation);
    match bytes_representation {
        BytesRepresentation::KnownSize(size) => Ok(size),
        BytesRepresentation::VariableSize => Err(CodecError::Other(
            "the array index cannot include a variable size output codec".to_string(),
        )),
    }
}

fn decode_shard_index(
    encoded_shard_index: &[u8],
    index_array_representation: &ArrayRepresentation,
    index_codecs: &dyn ArrayToBytesCodecTraits,
    parallel: bool,
) -> Result<Vec<u64>, CodecError> {
    // Decode the shard index
    let decoded_shard_index = if parallel {
        index_codecs.par_decode(encoded_shard_index.to_vec(), index_array_representation)
    } else {
        index_codecs.decode(encoded_shard_index.to_vec(), index_array_representation)
    }?;
    Ok(decoded_shard_index
        .chunks_exact(core::mem::size_of::<u64>())
        .map(|v| u64::from_ne_bytes(v.try_into().unwrap() /* safe */))
        .collect())
}

#[cfg(test)]
mod tests {
    use crate::{array::codec::ArrayCodecTraits, array_subset::ArraySubset};

    use super::*;

    const JSON_VALID1: &'static str = r#"{
    "chunk_shape": [2, 2],
    "codecs": [
        {
            "name": "bytes",
            "configuration": {
                "endian": "little"
            }
        }
    ],
    "index_codecs": [
        {
            "name": "bytes",
            "configuration": {
                "endian": "little"
            }
        }
    ]
}"#;

    const JSON_VALID2: &'static str = r#"{
    "chunk_shape": [1, 2, 2],
    "codecs": [
        {
            "name": "bytes",
            "configuration": {
                "endian": "little"
            }
        },
        {
            "name": "gzip",
            "configuration": {
                "level": 1
            }
        }
    ],
    "index_codecs": [
        {
            "name": "bytes",
            "configuration": {
                "endian": "little"
            }
        },
        { "name": "crc32c" }
    ]
}"#;

    fn codec_sharding_round_trip_impl(json: &str, chunk_shape: Vec<u64>) {
        let array_representation =
            ArrayRepresentation::new(chunk_shape, DataType::UInt16, FillValue::from(0u16)).unwrap();
        let elements: Vec<u16> = (0..array_representation.num_elements() as u16).collect();
        let bytes = safe_transmute::transmute_to_bytes(&elements).to_vec();

        let codec_configuration: ShardingCodecConfiguration = serde_json::from_str(json).unwrap();
        let codec = ShardingCodec::new_with_configuration(&codec_configuration).unwrap();

        let encoded = codec.encode(bytes.clone(), &array_representation).unwrap();
        let decoded = codec
            .decode(encoded.clone(), &array_representation)
            .unwrap();
        assert_ne!(encoded, decoded);
        assert_eq!(bytes, decoded);

        // println!("bytes {bytes:?}");
        let encoded = codec
            .par_encode(bytes.clone(), &array_representation)
            .unwrap();
        // println!("encoded {encoded:?}");
        let decoded = codec
            .par_decode(encoded.clone(), &array_representation)
            .unwrap();
        // println!("decoded {decoded:?}");
        assert_ne!(encoded, decoded);
        assert_eq!(bytes, decoded);
    }

    #[test]
    fn codec_sharding_round_trip1() {
        let chunk_shape = vec![4, 4];
        codec_sharding_round_trip_impl(JSON_VALID1, chunk_shape);
    }

    #[cfg(feature = "gzip")]
    #[cfg(feature = "crc32c")]
    #[test]
    fn codec_sharding_round_trip2() {
        let chunk_shape = vec![2, 4, 4];
        codec_sharding_round_trip_impl(JSON_VALID2, chunk_shape);
    }

    #[test]
    fn codec_sharding_fill_value() {
        let chunk_shape = vec![4, 4];
        let array_representation =
            ArrayRepresentation::new(chunk_shape, DataType::UInt16, FillValue::from(1u16)).unwrap();
        let bytes = array_representation
            .fill_value()
            .as_ne_bytes()
            .repeat(array_representation.num_elements() as usize);

        let codec_configuration: ShardingCodecConfiguration =
            serde_json::from_str(JSON_VALID1).unwrap();
        let codec = ShardingCodec::new_with_configuration(&codec_configuration).unwrap();

        let encoded = codec.encode(bytes.clone(), &array_representation).unwrap();
        let decoded = codec
            .decode(encoded.clone(), &array_representation)
            .unwrap();
        assert_ne!(encoded, decoded);
        assert_eq!(bytes, decoded);

        let encoded_u64: Vec<u64> = encoded
            .chunks_exact(8)
            .map(|b| u64::from_ne_bytes(b.try_into().unwrap()))
            .collect();
        assert_eq!(encoded_u64, vec![u64::MAX; 2 * 2 * 2]); // 2 * chunk_shape / sharding.chunk_shape
    }

    #[test]
    fn codec_sharding_partial_decode1() {
        let array_representation =
            ArrayRepresentation::new(vec![4, 4], DataType::UInt8, FillValue::from(0u8)).unwrap();
        let elements: Vec<u8> = (0..array_representation.num_elements() as u8).collect();
        let bytes = elements;

        let codec_configuration: ShardingCodecConfiguration =
            serde_json::from_str(JSON_VALID1).unwrap();
        let codec = ShardingCodec::new_with_configuration(&codec_configuration).unwrap();

        let encoded = codec.encode(bytes.clone(), &array_representation).unwrap();
        let decoded_regions = [ArraySubset::new_with_start_shape(vec![1, 0], vec![2, 1]).unwrap()];
        let input_handle = Box::new(std::io::Cursor::new(encoded));
        let partial_decoder = codec.partial_decoder(input_handle);
        let decoded_partial_chunk = partial_decoder
            .partial_decode(&array_representation, &decoded_regions)
            .unwrap();

        let decoded_partial_chunk: Vec<u8> = decoded_partial_chunk
            .into_iter()
            .flatten()
            .collect::<Vec<_>>()
            .chunks(std::mem::size_of::<u8>())
            .map(|b| u8::from_ne_bytes(b.try_into().unwrap()))
            .collect();
        let answer: Vec<u8> = vec![4, 8];
        assert_eq!(answer, decoded_partial_chunk);
    }

    #[cfg(feature = "gzip")]
    #[cfg(feature = "crc32c")]
    #[test]
    fn codec_sharding_partial_decode2() {
        use crate::array::codec::ArrayCodecTraits;

        let array_representation =
            ArrayRepresentation::new(vec![2, 4, 4], DataType::UInt16, FillValue::from(0u16))
                .unwrap();
        let elements: Vec<u16> = (0..array_representation.num_elements() as u16).collect();
        let bytes = safe_transmute::transmute_to_bytes(&elements).to_vec();

        let codec_configuration: ShardingCodecConfiguration =
            serde_json::from_str(JSON_VALID2).unwrap();
        let codec = ShardingCodec::new_with_configuration(&codec_configuration).unwrap();

        let encoded = codec.encode(bytes.clone(), &array_representation).unwrap();
        let decoded_regions =
            [ArraySubset::new_with_start_shape(vec![1, 0, 0], vec![1, 2, 3]).unwrap()];
        let input_handle = Box::new(std::io::Cursor::new(encoded));
        let partial_decoder = codec.partial_decoder(input_handle);
        let decoded_partial_chunk = partial_decoder
            .partial_decode(&array_representation, &decoded_regions)
            .unwrap();
        println!("decoded_partial_chunk {:?}", decoded_partial_chunk);
        let decoded_partial_chunk: Vec<u16> = decoded_partial_chunk
            .into_iter()
            .flatten()
            .collect::<Vec<_>>()
            .chunks(std::mem::size_of::<u16>())
            .map(|b| u16::from_ne_bytes(b.try_into().unwrap()))
            .collect();

        let answer: Vec<u16> = vec![16, 17, 18, 20, 21, 22];
        assert_eq!(answer, decoded_partial_chunk);
    }
}
