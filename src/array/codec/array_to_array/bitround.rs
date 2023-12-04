//! The `bitround` array to array codec.
//!
//! Rounds the mantissa of floating point data types to the specified number of bits.
//! Rounds integers to the specified number of bits from the most significant set bit.
//!
//! This codec requires the `bitround` feature, which is disabled by default.
//!
//! The current implementation does not write its metadata to the array metadata, so the array can be imported by tools which do not presently support this codec.
//! This functionality will be changed when the `bitround` codec is in the zarr specification and supported by multiple implementations.
//!
//! See [`BitroundCodecConfigurationV1`] for example `JSON` metadata.
//!

mod bitround_codec;
mod bitround_configuration;
mod bitround_partial_decoder;

pub use bitround_codec::BitroundCodec;
pub use bitround_configuration::{BitroundCodecConfiguration, BitroundCodecConfigurationV1};

use crate::{
    array::{
        codec::{Codec, CodecError, CodecPlugin},
        DataType,
    },
    metadata::Metadata,
    plugin::PluginCreateError,
};

const IDENTIFIER: &str = "bitround";

// Register the codec.
inventory::submit! {
    CodecPlugin::new(IDENTIFIER, is_name_bitround, create_codec_bitround)
}

fn is_name_bitround(name: &str) -> bool {
    name.eq(IDENTIFIER)
}

fn create_codec_bitround(metadata: &Metadata) -> Result<Codec, PluginCreateError> {
    let configuration: BitroundCodecConfiguration = metadata.to_configuration()?;
    let codec = Box::new(BitroundCodec::new_with_configuration(&configuration));
    Ok(Codec::ArrayToArray(codec))
}

fn round_bits16(mut input: u16, keepbits: u32, maxbits: u32) -> u16 {
    if keepbits >= maxbits {
        input
    } else {
        let maskbits = maxbits - keepbits;
        let all_set = u16::MAX;
        let mask = (all_set >> maskbits) << maskbits;
        let half_quantum1 = (1 << (maskbits - 1)) - 1;
        input += ((input >> maskbits) & 1) + half_quantum1;
        input &= mask;
        input
    }
}

fn round_bits32(mut input: u32, keepbits: u32, maxbits: u32) -> u32 {
    if keepbits >= maxbits {
        input
    } else {
        let maskbits = maxbits - keepbits;
        let all_set = u32::MAX;
        let mask = (all_set >> maskbits) << maskbits;
        let half_quantum1 = (1 << (maskbits - 1)) - 1;
        input += ((input >> maskbits) & 1) + half_quantum1;
        input &= mask;
        input
    }
}

fn round_bits64(mut input: u64, keepbits: u32, maxbits: u32) -> u64 {
    if keepbits >= maxbits {
        input
    } else {
        let maskbits = maxbits - keepbits;
        let all_set = u64::MAX;
        let mask = (all_set >> maskbits) << maskbits;
        let half_quantum1 = (1 << (maskbits - 1)) - 1;
        input += ((input >> maskbits) & 1) + half_quantum1;
        input &= mask;
        input
    }
}

fn round_bytes(bytes: &mut [u8], data_type: &DataType, keepbits: u32) -> Result<(), CodecError> {
    match data_type {
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
    use array_representation::ArrayRepresentation;
    use itertools::Itertools;

    use crate::{
        array::{
            array_representation,
            codec::{
                ArrayCodecTraits, ArrayToArrayCodecTraits, ArrayToBytesCodecTraits, BytesCodec,
            },
            DataType,
        },
        array_subset::ArraySubset,
    };

    use super::*;

    #[test]
    fn codec_bitround_float() {
        // 1 sign bit, 8 exponent, 3 mantissa
        const JSON: &'static str = r#"{ "keepbits": 3 }"#;
        let array_representation =
            ArrayRepresentation::new(vec![4], DataType::Float32, 0.0f32.into()).unwrap();
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
        let bytes = safe_transmute::transmute_to_bytes(&elements).to_vec();

        let codec_configuration: BitroundCodecConfiguration = serde_json::from_str(JSON).unwrap();
        let codec = BitroundCodec::new_with_configuration(&codec_configuration);

        let encoded = codec.encode(bytes.clone(), &array_representation).unwrap();
        let decoded = codec
            .decode(encoded.clone(), &array_representation)
            .unwrap();
        let decoded_elements = safe_transmute::transmute_many_permissive::<f32>(&decoded)
            .unwrap()
            .to_vec();
        assert_eq!(decoded_elements, &[0.0f32, 1.25f32, -8.0f32, 98304.0f32]);
    }

    #[test]
    fn codec_bitround_uint() {
        const JSON: &'static str = r#"{ "keepbits": 3 }"#;
        let array_representation =
            ArrayRepresentation::new(vec![4], DataType::UInt32, 0u32.into()).unwrap();
        let elements: Vec<u32> = vec![0, 1024, 1280, 1664, 1685, 123145182];
        let bytes = safe_transmute::transmute_to_bytes(&elements).to_vec();

        let codec_configuration: BitroundCodecConfiguration = serde_json::from_str(JSON).unwrap();
        let codec = BitroundCodec::new_with_configuration(&codec_configuration);

        let encoded = codec.encode(bytes.clone(), &array_representation).unwrap();
        let decoded = codec
            .decode(encoded.clone(), &array_representation)
            .unwrap();
        let decoded_elements = safe_transmute::transmute_many_permissive::<u32>(&decoded)
            .unwrap()
            .to_vec();
        for element in &decoded_elements {
            println!("{element} -> {element:#b}");
        }
        assert_eq!(decoded_elements, &[0, 1024, 1280, 1536, 1792, 117440512]);
    }

    #[test]
    fn codec_bitround_partial_decode() {
        const JSON: &'static str = r#"{ "keepbits": 2 }"#;
        let codec_configuration: BitroundCodecConfiguration = serde_json::from_str(JSON).unwrap();
        let codec = BitroundCodec::new_with_configuration(&codec_configuration);

        let elements: Vec<f32> = (0..32).map(|i| i as f32).collect();
        let bytes = safe_transmute::transmute_to_bytes(&elements).to_vec();
        let array_representation = ArrayRepresentation::new(
            vec![elements.len().try_into().unwrap()],
            DataType::Float32,
            0.0f32.into(),
        )
        .unwrap();

        let encoded = codec.encode(bytes.clone(), &array_representation).unwrap();
        let decoded_regions = [
            ArraySubset::new_with_start_shape(vec![3], vec![2]).unwrap(),
            ArraySubset::new_with_start_shape(vec![17], vec![4]).unwrap(),
        ];
        let input_handle = Box::new(std::io::Cursor::new(encoded));
        let bytes_codec = BytesCodec::default();
        let input_handle = bytes_codec
            .partial_decoder(input_handle, &array_representation)
            .unwrap();
        let partial_decoder = codec
            .partial_decoder(input_handle, &array_representation)
            .unwrap();
        let decoded_partial_chunk = partial_decoder.partial_decode(&decoded_regions).unwrap();
        let decoded_partial_chunk = decoded_partial_chunk
            .iter()
            .map(|bytes| {
                safe_transmute::transmute_many_permissive::<f32>(&bytes)
                    .unwrap()
                    .to_vec()
            })
            .collect_vec();
        let answer: &[Vec<f32>] = &[vec![3.0, 4.0], vec![16.0, 16.0, 20.0, 20.0]];
        assert_eq!(answer, decoded_partial_chunk);
    }
}
