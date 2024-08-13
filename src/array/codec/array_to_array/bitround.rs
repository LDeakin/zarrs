//! The `bitround` array to array codec.
//!
//! Rounds the mantissa of floating point data types to the specified number of bits.
//! Rounds integers to the specified number of bits from the most significant set bit.
//!
//! <div class="warning">
//! This codec is experimental and is incompatible with other Zarr V3 implementations.
//! </div>
//!
//! This codec requires the `bitround` feature, which is disabled by default.
//!
//! See [`BitroundCodecConfigurationV1`] for example `JSON` metadata.

mod bitround_codec;
mod bitround_partial_decoder;

pub use crate::metadata::v3::codec::bitround::{
    BitroundCodecConfiguration, BitroundCodecConfigurationV1,
};
pub use bitround_codec::BitroundCodec;

use crate::{
    array::{
        codec::{Codec, CodecError, CodecPlugin},
        DataType,
    },
    config::global_config,
    metadata::v3::{codec::bitround, MetadataV3},
    plugin::{PluginCreateError, PluginMetadataInvalidError},
};

pub use bitround::IDENTIFIER;

// Register the codec.
inventory::submit! {
    CodecPlugin::new(IDENTIFIER, is_name_bitround, create_codec_bitround)
}

fn is_name_bitround(name: &str) -> bool {
    name.eq(IDENTIFIER)
        || name
            == global_config()
                .experimental_codec_names()
                .get(IDENTIFIER)
                .expect("experimental codec identifier in global map")
}

pub(crate) fn create_codec_bitround(metadata: &MetadataV3) -> Result<Codec, PluginCreateError> {
    let configuration: BitroundCodecConfiguration = metadata
        .to_configuration()
        .map_err(|_| PluginMetadataInvalidError::new(IDENTIFIER, "codec", metadata.clone()))?;
    let codec = Box::new(BitroundCodec::new_with_configuration(&configuration));
    Ok(Codec::ArrayToArray(codec))
}

fn round_bits8(mut input: u8, keepbits: u32, maxbits: u32) -> u8 {
    if keepbits < maxbits {
        let maskbits = maxbits - keepbits;
        let all_set = u8::MAX;
        let mask = (all_set >> maskbits) << maskbits;
        let half_quantum1 = (1 << (maskbits - 1)) - 1;
        input = input.saturating_add(((input >> maskbits) & 1) + half_quantum1) & mask;
    }
    input
}

const fn round_bits16(mut input: u16, keepbits: u32, maxbits: u32) -> u16 {
    if keepbits < maxbits {
        let maskbits = maxbits - keepbits;
        let all_set = u16::MAX;
        let mask = (all_set >> maskbits) << maskbits;
        let half_quantum1 = (1 << (maskbits - 1)) - 1;
        input = input.saturating_add(((input >> maskbits) & 1) + half_quantum1) & mask;
    }
    input
}

const fn round_bits32(mut input: u32, keepbits: u32, maxbits: u32) -> u32 {
    if keepbits < maxbits {
        let maskbits = maxbits - keepbits;
        let all_set = u32::MAX;
        let mask = (all_set >> maskbits) << maskbits;
        let half_quantum1 = (1 << (maskbits - 1)) - 1;
        input = input.saturating_add(((input >> maskbits) & 1) + half_quantum1) & mask;
    }
    input
}

const fn round_bits64(mut input: u64, keepbits: u32, maxbits: u32) -> u64 {
    if keepbits < maxbits {
        let maskbits = maxbits - keepbits;
        let all_set = u64::MAX;
        let mask = (all_set >> maskbits) << maskbits;
        let half_quantum1 = (1 << (maskbits - 1)) - 1;
        input = input.saturating_add(((input >> maskbits) & 1) + half_quantum1) & mask;
    }
    input
}

fn round_bytes(bytes: &mut [u8], data_type: &DataType, keepbits: u32) -> Result<(), CodecError> {
    match data_type {
        DataType::UInt8 | DataType::Int8 => {
            let round = |element: &mut u8| {
                *element = round_bits8(*element, keepbits, 8 - element.leading_zeros());
            };
            bytes.iter_mut().for_each(round);
            Ok(())
        }
        DataType::Float16 | DataType::BFloat16 => {
            let round = |chunk: &mut [u8]| {
                let element = u16::from_ne_bytes(chunk.try_into().unwrap());
                let element = u16::to_ne_bytes(round_bits16(element, keepbits, 10));
                chunk.copy_from_slice(&element);
            };
            bytes.chunks_exact_mut(2).for_each(round);
            Ok(())
        }
        DataType::UInt16 | DataType::Int16 => {
            let round = |chunk: &mut [u8]| {
                let element = u16::from_ne_bytes(chunk.try_into().unwrap());
                let element = u16::to_ne_bytes(round_bits16(
                    element,
                    keepbits,
                    16 - element.leading_zeros(),
                ));
                chunk.copy_from_slice(&element);
            };
            bytes.chunks_exact_mut(2).for_each(round);
            Ok(())
        }
        DataType::Float32 | DataType::Complex64 => {
            let round = |chunk: &mut [u8]| {
                let element = u32::from_ne_bytes(chunk.try_into().unwrap());
                let element = u32::to_ne_bytes(round_bits32(element, keepbits, 23));
                chunk.copy_from_slice(&element);
            };
            bytes.chunks_exact_mut(4).for_each(round);
            Ok(())
        }
        DataType::UInt32 | DataType::Int32 => {
            let round = |chunk: &mut [u8]| {
                let element = u32::from_ne_bytes(chunk.try_into().unwrap());
                let element = u32::to_ne_bytes(round_bits32(
                    element,
                    keepbits,
                    32 - element.leading_zeros(),
                ));
                chunk.copy_from_slice(&element);
            };
            bytes.chunks_exact_mut(4).for_each(round);
            Ok(())
        }
        DataType::Float64 | DataType::Complex128 => {
            let round = |chunk: &mut [u8]| {
                let element = u64::from_ne_bytes(chunk.try_into().unwrap());
                let element = u64::to_ne_bytes(round_bits64(element, keepbits, 52));
                chunk.copy_from_slice(&element);
            };
            bytes.chunks_exact_mut(8).for_each(round);
            Ok(())
        }
        DataType::UInt64 | DataType::Int64 => {
            let round = |chunk: &mut [u8]| {
                let element = u64::from_ne_bytes(chunk.try_into().unwrap());
                let element = u64::to_ne_bytes(round_bits64(
                    element,
                    keepbits,
                    64 - element.leading_zeros(),
                ));
                chunk.copy_from_slice(&element);
            };
            bytes.chunks_exact_mut(8).for_each(round);
            Ok(())
        }
        _ => Err(CodecError::UnsupportedDataType(
            data_type.clone(),
            IDENTIFIER.to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::{num::NonZeroU64, sync::Arc};

    use array_representation::ChunkRepresentation;
    use itertools::Itertools;

    use crate::{
        array::{
            array_representation,
            codec::{ArrayToArrayCodecTraits, ArrayToBytesCodecTraits, BytesCodec, CodecOptions},
            ArrayBytes,
        },
        array_subset::ArraySubset,
    };

    use super::*;

    #[test]
    fn codec_bitround_float() {
        // 1 sign bit, 8 exponent, 3 mantissa
        const JSON: &'static str = r#"{ "keepbits": 3 }"#;
        let chunk_representation = ChunkRepresentation::new(
            vec![NonZeroU64::new(4).unwrap()],
            DataType::Float32,
            0.0f32.into(),
        )
        .unwrap();
        let elements: Vec<f32> = vec![
            //                         |
            0.0,
            // 1.23456789 -> 001111111001|11100000011001010010
            // 1.25       -> 001111111010
            1.23456789,
            // -8.3587192 -> 110000010000|01011011110101010000
            // -8.0       -> 110000010000
            -8.3587192834,
            // 98765.43210-> 010001111100|00001110011010110111
            // 98304.0    -> 010001111100
            98765.43210,
        ];
        let bytes = crate::array::transmute_to_bytes_vec(elements);
        let bytes = ArrayBytes::from(bytes);

        let codec_configuration: BitroundCodecConfiguration = serde_json::from_str(JSON).unwrap();
        let codec = BitroundCodec::new_with_configuration(&codec_configuration);

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
        let decoded_elements = crate::array::transmute_from_bytes_vec::<f32>(
            decoded.into_fixed().unwrap().into_owned(),
        );
        assert_eq!(decoded_elements, &[0.0f32, 1.25f32, -8.0f32, 98304.0f32]);
    }

    #[test]
    fn codec_bitround_uint() {
        const JSON: &'static str = r#"{ "keepbits": 3 }"#;
        let chunk_representation = ChunkRepresentation::new(
            vec![NonZeroU64::new(4).unwrap()],
            DataType::UInt32,
            0u32.into(),
        )
        .unwrap();
        let elements: Vec<u32> = vec![0, 1024, 1280, 1664, 1685, 123145182, 4294967295];
        let bytes = crate::array::transmute_to_bytes_vec(elements);
        let bytes = ArrayBytes::from(bytes);

        let codec_configuration: BitroundCodecConfiguration = serde_json::from_str(JSON).unwrap();
        let codec = BitroundCodec::new_with_configuration(&codec_configuration);

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
        let decoded_elements = crate::array::transmute_from_bytes_vec::<u32>(
            decoded.into_fixed().unwrap().into_owned(),
        );
        for element in &decoded_elements {
            println!("{element} -> {element:#b}");
        }
        assert_eq!(
            decoded_elements,
            &[0, 1024, 1280, 1536, 1792, 117440512, 3758096384]
        );
    }

    #[test]
    fn codec_bitround_uint8() {
        const JSON: &'static str = r#"{ "keepbits": 3 }"#;
        let chunk_representation = ChunkRepresentation::new(
            vec![NonZeroU64::new(4).unwrap()],
            DataType::UInt8,
            0u8.into(),
        )
        .unwrap();
        let elements: Vec<u32> = vec![0, 3, 7, 15, 17, 54, 89, 128, 255];
        let bytes = crate::array::transmute_to_bytes_vec(elements);
        let bytes = ArrayBytes::from(bytes);

        let codec_configuration: BitroundCodecConfiguration = serde_json::from_str(JSON).unwrap();
        let codec = BitroundCodec::new_with_configuration(&codec_configuration);

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
        let decoded_elements = crate::array::transmute_from_bytes_vec::<u32>(
            decoded.into_fixed().unwrap().into_owned(),
        );
        for element in &decoded_elements {
            println!("{element} -> {element:#b}");
        }
        assert_eq!(decoded_elements, &[0, 3, 7, 16, 16, 56, 96, 128, 224]);
    }

    #[test]
    fn codec_bitround_partial_decode() {
        const JSON: &'static str = r#"{ "keepbits": 2 }"#;
        let codec_configuration: BitroundCodecConfiguration = serde_json::from_str(JSON).unwrap();
        let codec = BitroundCodec::new_with_configuration(&codec_configuration);

        let elements: Vec<f32> = (0..32).map(|i| i as f32).collect();
        let chunk_representation = ChunkRepresentation::new(
            vec![(elements.len() as u64).try_into().unwrap()],
            DataType::Float32,
            0.0f32.into(),
        )
        .unwrap();
        let bytes: ArrayBytes = crate::array::transmute_to_bytes_vec(elements).into();

        let encoded = codec
            .encode(
                bytes.clone(),
                &chunk_representation,
                &CodecOptions::default(),
            )
            .unwrap()
            .into_owned();
        let decoded_regions = [
            ArraySubset::new_with_ranges(&[3..5]),
            ArraySubset::new_with_ranges(&[17..21]),
        ];
        let input_handle = Arc::new(std::io::Cursor::new(encoded.into_fixed().unwrap()));
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
            .map(|bytes| {
                crate::array::transmute_from_bytes_vec::<f32>(
                    bytes.into_fixed().unwrap().into_owned(),
                )
            })
            .collect_vec();
        let answer: &[Vec<f32>] = &[vec![3.0, 4.0], vec![16.0, 16.0, 20.0, 20.0]];
        assert_eq!(answer, decoded_partial_chunk);
    }

    #[cfg(feature = "async")]
    #[tokio::test]
    async fn codec_bitround_async_partial_decode() {
        const JSON: &'static str = r#"{ "keepbits": 2 }"#;
        let codec_configuration: BitroundCodecConfiguration = serde_json::from_str(JSON).unwrap();
        let codec = BitroundCodec::new_with_configuration(&codec_configuration);

        let elements: Vec<f32> = (0..32).map(|i| i as f32).collect();
        let chunk_representation = ChunkRepresentation::new(
            vec![(elements.len() as u64).try_into().unwrap()],
            DataType::Float32,
            0.0f32.into(),
        )
        .unwrap();
        let bytes = crate::array::transmute_to_bytes_vec(elements);
        let bytes = ArrayBytes::from(bytes);

        let encoded = codec
            .encode(
                bytes.clone(),
                &chunk_representation,
                &CodecOptions::default(),
            )
            .unwrap();
        let decoded_regions = [
            ArraySubset::new_with_ranges(&[3..5]),
            ArraySubset::new_with_ranges(&[17..21]),
        ];
        let input_handle = Arc::new(std::io::Cursor::new(encoded.into_fixed().unwrap()));
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
            .map(|bytes| {
                crate::array::transmute_from_bytes_vec::<f32>(
                    bytes.into_fixed().unwrap().into_owned(),
                )
            })
            .collect_vec();
        let answer: &[Vec<f32>] = &[vec![3.0, 4.0], vec![16.0, 16.0, 20.0, 20.0]];
        assert_eq!(answer, decoded_partial_chunk);
    }
}
