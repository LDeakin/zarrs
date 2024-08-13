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
mod sharding_partial_decoder;

use std::{borrow::Cow, num::NonZeroU64};

pub use crate::metadata::v3::codec::sharding::{
    ShardingCodecConfiguration, ShardingCodecConfigurationV1, ShardingIndexLocation,
};

pub use sharding_codec::ShardingCodec;
pub use sharding_codec_builder::ShardingCodecBuilder;

use crate::{
    array::{
        codec::{ArrayToBytesCodecTraits, Codec, CodecError, CodecOptions, CodecPlugin},
        BytesRepresentation, ChunkRepresentation, ChunkShape, DataType, FillValue,
    },
    metadata::v3::{codec::sharding, MetadataV3},
    plugin::{PluginCreateError, PluginMetadataInvalidError},
};

pub use sharding::IDENTIFIER;

// Register the codec.
inventory::submit! {
    CodecPlugin::new(IDENTIFIER, is_name_sharding, create_codec_sharding)
}

fn is_name_sharding(name: &str) -> bool {
    name.eq(IDENTIFIER)
}

pub(crate) fn create_codec_sharding(metadata: &MetadataV3) -> Result<Codec, PluginCreateError> {
    let configuration: ShardingCodecConfiguration = metadata
        .to_configuration()
        .map_err(|_| PluginMetadataInvalidError::new(IDENTIFIER, "codec", metadata.clone()))?;
    let codec = ShardingCodec::new_with_configuration(&configuration)?;
    Ok(Codec::ArrayToBytes(Box::new(codec)))
}

fn calculate_chunks_per_shard(
    shard_shape: &[NonZeroU64],
    chunk_shape: &[NonZeroU64],
) -> Result<ChunkShape, CodecError> {
    Ok(std::iter::zip(shard_shape, chunk_shape)
        .map(|(s, c)| {
            let s = s.get();
            let c = c.get();
            if num::Integer::is_multiple_of(&s, &c) {
                Ok(unsafe { NonZeroU64::new_unchecked(s / c) })
            } else {
                Err(CodecError::Other(
                    format!("invalid inner chunk shape {chunk_shape:?}, it must evenly divide {shard_shape:?}")
                ))
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
    encoded_shard_index: &[u8],
    index_array_representation: &ChunkRepresentation,
    index_codecs: &dyn ArrayToBytesCodecTraits,
    options: &CodecOptions,
) -> Result<Vec<u64>, CodecError> {
    // Decode the shard index
    let decoded_shard_index = index_codecs.decode(
        Cow::Borrowed(encoded_shard_index),
        index_array_representation,
        options,
    )?;
    let decoded_shard_index = decoded_shard_index.into_fixed()?;
    Ok(decoded_shard_index
        .chunks_exact(core::mem::size_of::<u64>())
        .map(|v| u64::from_ne_bytes(v.try_into().unwrap() /* safe */))
        .collect())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{
        array::{
            codec::{
                bytes_to_bytes::test_unbounded::TestUnboundedCodec, BytesToBytesCodecTraits,
                CodecOptionsBuilder,
            },
            ArrayBytes,
        },
        array_subset::ArraySubset,
        config::global_config,
    };

    use super::*;

    fn get_concurrent_target(parallel: bool) -> usize {
        if parallel {
            global_config().codec_concurrent_target()
        } else {
            1
        }
    }

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
        options: &CodecOptions,
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
        let bytes: ArrayBytes = bytes.into();

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
            .encode(bytes.clone(), &chunk_representation, options)
            .unwrap();
        let decoded = codec
            .decode(encoded.clone(), &chunk_representation, options)
            .unwrap();
        assert_eq!(bytes, decoded);
        assert_ne!(encoded, decoded.into_fixed().unwrap());
    }

    #[test]
    fn codec_sharding_round_trip1() {
        for index_at_end in [true, false] {
            for all_fill_value in [true, false] {
                for unbounded in [true, false] {
                    for parallel in [true, false] {
                        let concurrent_target = get_concurrent_target(parallel);
                        let options =
                            CodecOptionsBuilder::new().concurrent_target(concurrent_target);
                        codec_sharding_round_trip_impl(
                            &options.build(),
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

    #[cfg(feature = "gzip")]
    #[cfg(feature = "crc32c")]
    #[test]
    fn codec_sharding_round_trip2() {
        use crate::array::codec::{Crc32cCodec, GzipCodec};

        for index_at_end in [true, false] {
            for all_fill_value in [true, false] {
                for unbounded in [true, false] {
                    for parallel in [true, false] {
                        let concurrent_target = get_concurrent_target(parallel);
                        let options =
                            CodecOptionsBuilder::new().concurrent_target(concurrent_target);
                        codec_sharding_round_trip_impl(
                            &options.build(),
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
        options: &CodecOptions,
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
        let bytes: ArrayBytes = bytes.into();

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
            .encode(bytes.clone(), &chunk_representation, options)
            .unwrap();
        let decoded = codec
            .decode(encoded.clone(), &chunk_representation, options)
            .unwrap();
        assert_eq!(bytes, decoded);
        assert_ne!(encoded, decoded.into_fixed().unwrap());
    }

    #[cfg(feature = "async")]
    #[tokio::test]
    async fn codec_sharding_async_round_trip() {
        for index_at_end in [true, false] {
            for all_fill_value in [true, false] {
                for unbounded in [true, false] {
                    for parallel in [true, false] {
                        let concurrent_target = get_concurrent_target(parallel);
                        let options =
                            CodecOptionsBuilder::new().concurrent_target(concurrent_target);
                        codec_sharding_async_round_trip_impl(
                            &options.build(),
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
        options: &CodecOptions,
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

        let bytes: ArrayBytes = elements.into();

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

        let encoded = codec
            .encode(bytes.clone(), &chunk_representation, options)
            .unwrap();
        let decoded_regions = [ArraySubset::new_with_ranges(&[1..3, 0..1])];
        let input_handle = Arc::new(std::io::Cursor::new(encoded));
        let partial_decoder = codec
            .partial_decoder(input_handle, &chunk_representation, options)
            .unwrap();
        let decoded_partial_chunk = partial_decoder
            .partial_decode_opt(&decoded_regions, options)
            .unwrap();

        let decoded_partial_chunk: Vec<u8> = decoded_partial_chunk
            .into_iter()
            .map(|bytes| bytes.into_fixed().unwrap().to_vec())
            .flatten()
            .collect::<Vec<_>>()
            .chunks(std::mem::size_of::<u8>())
            .map(|b| u8::from_ne_bytes(b.try_into().unwrap()))
            .collect();
        assert_eq!(answer, decoded_partial_chunk);
    }

    #[test]
    fn codec_sharding_partial_decode_all() {
        for index_at_end in [true, false] {
            for all_fill_value in [true, false] {
                for unbounded in [true, false] {
                    for parallel in [true, false] {
                        let concurrent_target = get_concurrent_target(parallel);
                        let options =
                            CodecOptionsBuilder::new().concurrent_target(concurrent_target);
                        codec_sharding_partial_decode(
                            &options.build(),
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
        options: &CodecOptions,
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
        let bytes: ArrayBytes = elements.into();

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

        let encoded = codec
            .encode(bytes.clone(), &chunk_representation, options)
            .unwrap();
        let decoded_regions = [ArraySubset::new_with_ranges(&[1..3, 0..1])];
        let input_handle = Arc::new(std::io::Cursor::new(encoded));
        let partial_decoder = codec
            .async_partial_decoder(input_handle, &chunk_representation, options)
            .await
            .unwrap();
        let decoded_partial_chunk = partial_decoder
            .partial_decode_opt(&decoded_regions, options)
            .await
            .unwrap();

        let decoded_partial_chunk: Vec<u8> = decoded_partial_chunk
            .into_iter()
            .map(|bytes| bytes.into_fixed().unwrap().to_vec())
            .flatten()
            .collect::<Vec<_>>()
            .chunks(std::mem::size_of::<u8>())
            .map(|b| u8::from_ne_bytes(b.try_into().unwrap()))
            .collect();
        assert_eq!(answer, decoded_partial_chunk);
    }

    #[cfg(feature = "async")]
    #[tokio::test]
    async fn codec_sharding_async_partial_decode_all() {
        for index_at_end in [true, false] {
            for all_fill_value in [true, false] {
                for unbounded in [true, false] {
                    for parallel in [true, false] {
                        let concurrent_target = get_concurrent_target(parallel);
                        let options =
                            CodecOptionsBuilder::new().concurrent_target(concurrent_target);
                        codec_sharding_async_partial_decode(
                            &options.build(),
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
    fn codec_sharding_partial_decode2() {
        let chunk_shape: ChunkShape = vec![2, 4, 4].try_into().unwrap();
        let chunk_representation = ChunkRepresentation::new(
            chunk_shape.to_vec(),
            DataType::UInt16,
            FillValue::from(0u16),
        )
        .unwrap();
        let elements: Vec<u16> = (0..chunk_representation.num_elements() as u16).collect();
        let bytes = crate::array::transmute_to_bytes_vec(elements);
        let bytes: ArrayBytes = bytes.into();

        let codec_configuration: ShardingCodecConfiguration =
            serde_json::from_str(JSON_VALID2).unwrap();
        let codec = ShardingCodec::new_with_configuration(&codec_configuration).unwrap();

        let encoded = codec
            .encode(bytes, &chunk_representation, &CodecOptions::default())
            .unwrap();
        let decoded_regions = [ArraySubset::new_with_ranges(&[1..2, 0..2, 0..3])];
        let input_handle = Arc::new(std::io::Cursor::new(encoded));
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
        println!("decoded_partial_chunk {decoded_partial_chunk:?}");
        let decoded_partial_chunk: Vec<u16> = decoded_partial_chunk
            .into_iter()
            .map(|bytes| bytes.into_fixed().unwrap().to_vec())
            .flatten()
            .collect::<Vec<_>>()
            .chunks(std::mem::size_of::<u16>())
            .map(|b| u16::from_ne_bytes(b.try_into().unwrap()))
            .collect();

        let answer: Vec<u16> = vec![16, 17, 18, 20, 21, 22];
        assert_eq!(answer, decoded_partial_chunk);
    }

    #[test]
    fn codec_sharding_partial_decode3() {
        let chunk_shape: ChunkShape = vec![4, 4].try_into().unwrap();
        let chunk_representation =
            ChunkRepresentation::new(chunk_shape.to_vec(), DataType::UInt8, FillValue::from(0u8))
                .unwrap();
        let elements: Vec<u8> = (0..chunk_representation.num_elements() as u8).collect();
        let bytes: ArrayBytes = elements.into();

        let codec_configuration: ShardingCodecConfiguration =
            serde_json::from_str(JSON_VALID3).unwrap();
        let codec = ShardingCodec::new_with_configuration(&codec_configuration).unwrap();

        let encoded = codec
            .encode(bytes, &chunk_representation, &CodecOptions::default())
            .unwrap();
        let decoded_regions = [ArraySubset::new_with_ranges(&[1..3, 0..1])];
        let input_handle = Arc::new(std::io::Cursor::new(encoded));
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

        let decoded_partial_chunk: Vec<u8> = decoded_partial_chunk
            .into_iter()
            .map(|bytes| bytes.into_fixed().unwrap().to_vec())
            .flatten()
            .collect::<Vec<_>>()
            .chunks(std::mem::size_of::<u8>())
            .map(|b| u8::from_ne_bytes(b.try_into().unwrap()))
            .collect();
        let answer: Vec<u8> = vec![4, 8];
        assert_eq!(answer, decoded_partial_chunk);
    }
}
