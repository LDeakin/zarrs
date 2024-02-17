//! The `sharding` array to bytes codec.
//!
//! Sharding logically splits chunks (shards) into sub-chunks (inner chunks) that can be individually compressed and accessed.
//! This allows to colocate multiple chunks within one storage object, bundling them in shards.
//!
//! See <https://zarr-specs.readthedocs.io/en/latest/v3/codecs/sharding-indexed/v1.0.html>.
//!
//! This codec requires the `sharding` feature, which is enabled by default.
//!
//! See [`ShardingCodecConfigurationV1`] for example `JSON` metadata.
//! The [`ShardingCodecBuilder`] can help with creating a [`ShardingCodec`].

mod sharding_codec;
mod sharding_codec_builder;
mod sharding_configuration;
mod sharding_partial_decoder;

use std::num::NonZeroU64;

pub use sharding_configuration::{
    ShardingCodecConfiguration, ShardingCodecConfigurationV1, ShardingIndexLocation,
};

pub use sharding_codec::ShardingCodec;
pub use sharding_codec_builder::ShardingCodecBuilder;
use thiserror::Error;

use crate::{
    array::{
        codec::{ArrayToBytesCodecTraits, Codec, CodecError, CodecPlugin, DecodeOptions},
        BytesRepresentation, ChunkRepresentation, ChunkShape, DataType, FillValue,
    },
    metadata::Metadata,
    plugin::{PluginCreateError, PluginMetadataInvalidError},
};

/// The identifier for the `sharding_indexed` codec.
pub const IDENTIFIER: &str = "sharding_indexed";

// Register the codec.
inventory::submit! {
    CodecPlugin::new(IDENTIFIER, is_name_sharding, create_codec_sharding)
}

fn is_name_sharding(name: &str) -> bool {
    name.eq(IDENTIFIER)
}

pub(crate) fn create_codec_sharding(metadata: &Metadata) -> Result<Codec, PluginCreateError> {
    let configuration: ShardingCodecConfiguration = metadata
        .to_configuration()
        .map_err(|_| PluginMetadataInvalidError::new(IDENTIFIER, "codec", metadata.clone()))?;
    let codec = ShardingCodec::new_with_configuration(&configuration)?;
    Ok(Codec::ArrayToBytes(Box::new(codec)))
}

#[derive(Debug, Error)]
#[error("invalid inner chunk shape {chunk_shape:?}, it must evenly divide {shard_shape:?}")]
struct ChunksPerShardError {
    chunk_shape: ChunkShape,
    shard_shape: ChunkShape,
}

fn calculate_chunks_per_shard(
    shard_shape: &[NonZeroU64],
    chunk_shape: &[NonZeroU64],
) -> Result<ChunkShape, ChunksPerShardError> {
    use num::Integer;

    Ok(std::iter::zip(shard_shape, chunk_shape)
        .map(|(s, c)| {
            let s = s.get();
            let c = c.get();
            if s.is_multiple_of(&c) {
                Ok(unsafe { NonZeroU64::new_unchecked(s / c) })
            } else {
                Err(ChunksPerShardError {
                    chunk_shape: chunk_shape.into(),
                    shard_shape: shard_shape.into(),
                })
            }
        })
        .collect::<Result<Vec<_>, _>>()?
        .into())
}

fn sharding_index_decoded_representation(chunks_per_shard: &[NonZeroU64]) -> ChunkRepresentation {
    let mut index_shape = Vec::with_capacity(chunks_per_shard.len() + 1);
    index_shape.extend(chunks_per_shard);
    index_shape.push(unsafe { NonZeroU64::new_unchecked(2) });
    ChunkRepresentation::new(index_shape, DataType::UInt64, FillValue::from(u64::MAX)).unwrap()
}

fn compute_index_encoded_size(
    index_codecs: &dyn ArrayToBytesCodecTraits,
    index_array_representation: &ChunkRepresentation,
) -> Result<u64, CodecError> {
    let bytes_representation = index_codecs.compute_encoded_size(index_array_representation)?;
    match bytes_representation {
        BytesRepresentation::FixedSize(size) => Ok(size),
        _ => Err(CodecError::Other(
            "the array index cannot include a variable size output codec".to_string(),
        )),
    }
}

fn decode_shard_index(
    encoded_shard_index: Vec<u8>,
    index_array_representation: &ChunkRepresentation,
    index_codecs: &dyn ArrayToBytesCodecTraits,
    options: &DecodeOptions,
) -> Result<Vec<u64>, CodecError> {
    // Decode the shard index
    let decoded_shard_index =
        index_codecs.decode_opt(encoded_shard_index, index_array_representation, options)?;
    Ok(decoded_shard_index
        .chunks_exact(core::mem::size_of::<u64>())
        .map(|v| u64::from_ne_bytes(v.try_into().unwrap() /* safe */))
        .collect())
}

#[cfg(feature = "async")]
async fn async_decode_shard_index(
    encoded_shard_index: Vec<u8>,
    index_array_representation: &ChunkRepresentation,
    index_codecs: &dyn ArrayToBytesCodecTraits,
    options: &DecodeOptions,
) -> Result<Vec<u64>, CodecError> {
    // Decode the shard index
    let decoded_shard_index = index_codecs
        .async_decode_opt(encoded_shard_index, index_array_representation, options)
        .await?;
    Ok(decoded_shard_index
        .chunks_exact(core::mem::size_of::<u64>())
        .map(|v| u64::from_ne_bytes(v.try_into().unwrap() /* safe */))
        .collect())
}

#[cfg(test)]
mod tests {
    use crate::{
        array::codec::{
            bytes_to_bytes::test_unbounded::TestUnboundedCodec, ArrayCodecTraits,
            BytesToBytesCodecTraits, EncodeOptions, PartialDecodeOptions,
        },
        array_subset::ArraySubset,
    };

    use super::*;

    const JSON_VALID2: &str = r#"{
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

    const JSON_VALID3: &str = r#"{
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
    ],
    "index_location": "start"
}"#;

    fn codec_sharding_round_trip_impl(
        encode_options: &EncodeOptions,
        decode_options: &DecodeOptions,
        unbounded: bool,
        index_at_end: bool,
        all_fill_value: bool,
        mut bytes_to_bytes_codecs: Vec<Box<dyn BytesToBytesCodecTraits>>,
    ) {
        let chunk_representation = ChunkRepresentation::new(
            ChunkShape::try_from(vec![4, 4]).unwrap().into(),
            DataType::UInt16,
            FillValue::from(0u16),
        )
        .unwrap();
        let elements: Vec<u16> = if all_fill_value {
            vec![0; chunk_representation.num_elements() as usize]
        } else {
            (0..chunk_representation.num_elements() as u16).collect()
        };
        let bytes = crate::array::transmute_to_bytes_vec(elements);

        if unbounded {
            bytes_to_bytes_codecs.push(Box::new(TestUnboundedCodec::new()))
        }
        let codec = ShardingCodecBuilder::new(vec![2, 2].try_into().unwrap())
            .index_location(if index_at_end {
                ShardingIndexLocation::End
            } else {
                ShardingIndexLocation::Start
            })
            .bytes_to_bytes_codecs(bytes_to_bytes_codecs)
            .build();

        let encoded = codec
            .encode_opt(bytes.clone(), &chunk_representation, encode_options)
            .unwrap();
        let decoded = codec
            .decode_opt(encoded.clone(), &chunk_representation, decode_options)
            .unwrap();
        assert_ne!(encoded, decoded);
        assert_eq!(bytes, decoded);
    }

    // FIXME: Investigate miri error for this test
    #[test]
    #[cfg_attr(miri, ignore)]
    fn codec_sharding_round_trip1() {
        for index_at_end in [true, false] {
            for all_fill_value in [true, false] {
                for unbounded in [true, false] {
                    for parallel in [true, false] {
                        let mut encode_options = EncodeOptions::default();
                        let mut decode_options = DecodeOptions::default();
                        if !parallel {
                            encode_options.set_concurrent_limit(1);
                            decode_options.set_concurrent_limit(1);
                        }
                        codec_sharding_round_trip_impl(
                            &encode_options,
                            &decode_options,
                            unbounded,
                            all_fill_value,
                            index_at_end,
                            vec![],
                        );
                    }
                }
            }
        }
    }

    // FIXME: Investigate miri error for this test
    #[cfg(feature = "gzip")]
    #[cfg(feature = "crc32c")]
    #[test]
    #[cfg_attr(miri, ignore)]
    fn codec_sharding_round_trip2() {
        use crate::array::codec::{Crc32cCodec, GzipCodec};

        for index_at_end in [true, false] {
            for all_fill_value in [true, false] {
                for unbounded in [true, false] {
                    for parallel in [true, false] {
                        let mut encode_options = EncodeOptions::default();
                        let mut decode_options = DecodeOptions::default();
                        if !parallel {
                            encode_options.set_concurrent_limit(1);
                            decode_options.set_concurrent_limit(1);
                        }
                        codec_sharding_round_trip_impl(
                            &encode_options,
                            &decode_options,
                            unbounded,
                            all_fill_value,
                            index_at_end,
                            vec![
                                Box::new(GzipCodec::new(5).unwrap()),
                                Box::new(Crc32cCodec::new()),
                            ],
                        );
                    }
                }
            }
        }
    }

    #[cfg(feature = "async")]
    async fn codec_sharding_async_round_trip_impl(
        encode_options: &EncodeOptions,
        decode_options: &DecodeOptions,
        unbounded: bool,
        index_at_end: bool,
        all_fill_value: bool,
        mut bytes_to_bytes_codecs: Vec<Box<dyn BytesToBytesCodecTraits>>,
    ) {
        let chunk_representation = ChunkRepresentation::new(
            ChunkShape::try_from(vec![4, 4]).unwrap().into(),
            DataType::UInt16,
            FillValue::from(0u16),
        )
        .unwrap();
        let elements: Vec<u16> = if all_fill_value {
            vec![0; chunk_representation.num_elements() as usize]
        } else {
            (0..chunk_representation.num_elements() as u16).collect()
        };
        let bytes = crate::array::transmute_to_bytes_vec(elements);

        if unbounded {
            bytes_to_bytes_codecs.push(Box::new(TestUnboundedCodec::new()))
        }
        let codec = ShardingCodecBuilder::new(vec![2, 2].try_into().unwrap())
            .index_location(if index_at_end {
                ShardingIndexLocation::End
            } else {
                ShardingIndexLocation::Start
            })
            .bytes_to_bytes_codecs(bytes_to_bytes_codecs)
            .build();

        let encoded = codec
            .async_encode_opt(bytes.clone(), &chunk_representation, encode_options)
            .await
            .unwrap();
        let decoded = codec
            .async_decode_opt(encoded.clone(), &chunk_representation, decode_options)
            .await
            .unwrap();
        assert_ne!(encoded, decoded);
        assert_eq!(bytes, decoded);
    }

    // FIXME: Investigate miri error for this test
    #[cfg(feature = "async")]
    #[tokio::test]
    #[cfg_attr(miri, ignore)]
    async fn codec_sharding_async_round_trip() {
        for index_at_end in [true, false] {
            for all_fill_value in [true, false] {
                for unbounded in [true, false] {
                    for parallel in [true, false] {
                        let mut encode_options = EncodeOptions::default();
                        let mut decode_options = DecodeOptions::default();
                        if !parallel {
                            encode_options.set_concurrent_limit(1);
                            decode_options.set_concurrent_limit(1);
                        }
                        codec_sharding_async_round_trip_impl(
                            &encode_options,
                            &decode_options,
                            unbounded,
                            all_fill_value,
                            index_at_end,
                            vec![],
                        )
                        .await;
                    }
                }
            }
        }
    }

    fn codec_sharding_partial_decode(
        options: &PartialDecodeOptions,
        unbounded: bool,
        index_at_end: bool,
        all_fill_value: bool,
    ) {
        let chunk_shape: ChunkShape = vec![4, 4].try_into().unwrap();
        let chunk_representation =
            ChunkRepresentation::new(chunk_shape.to_vec(), DataType::UInt8, FillValue::from(0u8))
                .unwrap();
        let elements: Vec<u8> = if all_fill_value {
            vec![0; chunk_representation.num_elements() as usize]
        } else {
            (0..chunk_representation.num_elements() as u8).collect()
        };
        let answer: Vec<u8> = if all_fill_value {
            vec![0, 0]
        } else {
            vec![4, 8]
        };

        let bytes = elements;

        let bytes_to_bytes_codecs: Vec<Box<dyn BytesToBytesCodecTraits>> = if unbounded {
            vec![Box::new(TestUnboundedCodec::new())]
        } else {
            vec![]
        };
        let codec = ShardingCodecBuilder::new(vec![2, 2].try_into().unwrap())
            .index_location(if index_at_end {
                ShardingIndexLocation::End
            } else {
                ShardingIndexLocation::Start
            })
            .bytes_to_bytes_codecs(bytes_to_bytes_codecs)
            .build();

        let encoded = codec.encode(bytes.clone(), &chunk_representation).unwrap();
        let decoded_regions = [ArraySubset::new_with_ranges(&[1..3, 0..1])];
        let input_handle = Box::new(std::io::Cursor::new(encoded));
        let partial_decoder = codec
            .partial_decoder_opt(input_handle, &chunk_representation, options)
            .unwrap();
        let decoded_partial_chunk = partial_decoder
            .partial_decode_opt(&decoded_regions, options)
            .unwrap();

        let decoded_partial_chunk: Vec<u8> = decoded_partial_chunk
            .into_iter()
            .flatten()
            .collect::<Vec<_>>()
            .chunks(std::mem::size_of::<u8>())
            .map(|b| u8::from_ne_bytes(b.try_into().unwrap()))
            .collect();
        assert_eq!(answer, decoded_partial_chunk);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn codec_sharding_partial_decode_all() {
        for index_at_end in [true, false] {
            for all_fill_value in [true, false] {
                for unbounded in [true, false] {
                    for parallel in [true, false] {
                        let mut encode_options = EncodeOptions::default();
                        let mut decode_options = DecodeOptions::default();
                        if !parallel {
                            encode_options.set_concurrent_limit(1);
                            decode_options.set_concurrent_limit(1);
                        }
                        codec_sharding_partial_decode(
                            &decode_options,
                            unbounded,
                            all_fill_value,
                            index_at_end,
                        );
                    }
                }
            }
        }
    }

    #[cfg(feature = "async")]
    async fn codec_sharding_async_partial_decode(
        options: &PartialDecodeOptions,
        unbounded: bool,
        index_at_end: bool,
        all_fill_value: bool,
    ) {
        let chunk_shape: ChunkShape = vec![4, 4].try_into().unwrap();
        let chunk_representation =
            ChunkRepresentation::new(chunk_shape.to_vec(), DataType::UInt8, FillValue::from(0u8))
                .unwrap();
        let elements: Vec<u8> = if all_fill_value {
            vec![0; chunk_representation.num_elements() as usize]
        } else {
            (0..chunk_representation.num_elements() as u8).collect()
        };
        let answer: Vec<u8> = if all_fill_value {
            vec![0, 0]
        } else {
            vec![4, 8]
        };

        let bytes = elements;

        let bytes_to_bytes_codecs: Vec<Box<dyn BytesToBytesCodecTraits>> = if unbounded {
            vec![Box::new(TestUnboundedCodec::new())]
        } else {
            vec![]
        };
        let codec = ShardingCodecBuilder::new(vec![2, 2].try_into().unwrap())
            .index_location(if index_at_end {
                ShardingIndexLocation::End
            } else {
                ShardingIndexLocation::Start
            })
            .bytes_to_bytes_codecs(bytes_to_bytes_codecs)
            .build();

        let encoded = codec.encode(bytes.clone(), &chunk_representation).unwrap();
        let decoded_regions = [ArraySubset::new_with_ranges(&[1..3, 0..1])];
        let input_handle = Box::new(std::io::Cursor::new(encoded));
        let partial_decoder = codec
            .async_partial_decoder_opt(input_handle, &chunk_representation, options)
            .await
            .unwrap();
        let decoded_partial_chunk = partial_decoder
            .partial_decode_opt(&decoded_regions, options)
            .await
            .unwrap();

        let decoded_partial_chunk: Vec<u8> = decoded_partial_chunk
            .into_iter()
            .flatten()
            .collect::<Vec<_>>()
            .chunks(std::mem::size_of::<u8>())
            .map(|b| u8::from_ne_bytes(b.try_into().unwrap()))
            .collect();
        assert_eq!(answer, decoded_partial_chunk);
    }

    #[cfg(feature = "async")]
    #[tokio::test]
    #[cfg_attr(miri, ignore)]
    async fn codec_sharding_async_partial_decode_all() {
        for index_at_end in [true, false] {
            for all_fill_value in [true, false] {
                for unbounded in [true, false] {
                    for parallel in [true, false] {
                        let mut encode_options = EncodeOptions::default();
                        let mut decode_options = DecodeOptions::default();
                        if !parallel {
                            encode_options.set_concurrent_limit(1);
                            decode_options.set_concurrent_limit(1);
                        }
                        codec_sharding_async_partial_decode(
                            &decode_options,
                            unbounded,
                            all_fill_value,
                            index_at_end,
                        )
                        .await;
                    }
                }
            }
        }
    }

    #[cfg(feature = "gzip")]
    #[cfg(feature = "crc32c")]
    #[test]
    #[cfg_attr(miri, ignore)]
    fn codec_sharding_partial_decode2() {
        use crate::array::codec::ArrayCodecTraits;

        let chunk_shape: ChunkShape = vec![2, 4, 4].try_into().unwrap();
        let chunk_representation = ChunkRepresentation::new(
            chunk_shape.to_vec(),
            DataType::UInt16,
            FillValue::from(0u16),
        )
        .unwrap();
        let elements: Vec<u16> = (0..chunk_representation.num_elements() as u16).collect();
        let bytes = crate::array::transmute_to_bytes_vec(elements);

        let codec_configuration: ShardingCodecConfiguration =
            serde_json::from_str(JSON_VALID2).unwrap();
        let codec = ShardingCodec::new_with_configuration(&codec_configuration).unwrap();

        let encoded = codec.encode(bytes, &chunk_representation).unwrap();
        let decoded_regions = [ArraySubset::new_with_ranges(&[1..2, 0..2, 0..3])];
        let input_handle = Box::new(std::io::Cursor::new(encoded));
        let partial_decoder = codec
            .partial_decoder(input_handle, &chunk_representation)
            .unwrap();
        let decoded_partial_chunk = partial_decoder.partial_decode(&decoded_regions).unwrap();
        println!("decoded_partial_chunk {decoded_partial_chunk:?}");
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

    #[test]
    #[cfg_attr(miri, ignore)]
    fn codec_sharding_partial_decode3() {
        let chunk_shape: ChunkShape = vec![4, 4].try_into().unwrap();
        let chunk_representation =
            ChunkRepresentation::new(chunk_shape.to_vec(), DataType::UInt8, FillValue::from(0u8))
                .unwrap();
        let elements: Vec<u8> = (0..chunk_representation.num_elements() as u8).collect();
        let bytes = elements;

        let codec_configuration: ShardingCodecConfiguration =
            serde_json::from_str(JSON_VALID3).unwrap();
        let codec = ShardingCodec::new_with_configuration(&codec_configuration).unwrap();

        let encoded = codec.encode(bytes, &chunk_representation).unwrap();
        let decoded_regions = [ArraySubset::new_with_ranges(&[1..3, 0..1])];
        let input_handle = Box::new(std::io::Cursor::new(encoded));
        let partial_decoder = codec
            .partial_decoder(input_handle, &chunk_representation)
            .unwrap();
        let decoded_partial_chunk = partial_decoder.partial_decode(&decoded_regions).unwrap();

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
}
