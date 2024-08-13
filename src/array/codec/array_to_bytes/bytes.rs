//! The `bytes` array to bytes codec.
//!
//! Encodes arrays of fixed-size numeric data types as little endian or big endian in lexicographical order.
//!
//! See <https://zarr-specs.readthedocs.io/en/latest/v3/codecs/bytes/v1.0.html>.

mod bytes_codec;
mod bytes_partial_decoder;

pub use crate::array::endianness::{Endianness, NATIVE_ENDIAN};
use crate::metadata::v3::codec::bytes;
pub use crate::metadata::v3::codec::bytes::{BytesCodecConfiguration, BytesCodecConfigurationV1};

pub use bytes_codec::BytesCodec;

use crate::{
    array::{
        codec::{Codec, CodecPlugin},
        DataType,
    },
    metadata::v3::MetadataV3,
    plugin::{PluginCreateError, PluginMetadataInvalidError},
};

pub use bytes::IDENTIFIER;

// Register the codec.
inventory::submit! {
    CodecPlugin::new(IDENTIFIER, is_name_bytes, create_codec_bytes)
}

fn is_name_bytes(name: &str) -> bool {
    name.eq(IDENTIFIER)
}

pub(crate) fn create_codec_bytes(metadata: &MetadataV3) -> Result<Codec, PluginCreateError> {
    let configuration: BytesCodecConfiguration = metadata
        .to_configuration()
        .map_err(|_| PluginMetadataInvalidError::new(IDENTIFIER, "codec", metadata.clone()))?;
    let codec = Box::new(BytesCodec::new_with_configuration(&configuration));
    Ok(Codec::ArrayToBytes(codec))
}

/// Reverse the endianness of bytes for a given data type.
pub fn reverse_endianness(v: &mut [u8], data_type: &DataType) {
    match data_type {
        DataType::Bool | DataType::Int8 | DataType::UInt8 | DataType::RawBits(_) => {}
        DataType::Int16 | DataType::UInt16 | DataType::Float16 | DataType::BFloat16 => {
            let swap = |chunk: &mut [u8]| {
                let bytes = u16::from_ne_bytes(unsafe { chunk.try_into().unwrap_unchecked() });
                chunk.copy_from_slice(bytes.swap_bytes().to_ne_bytes().as_slice());
            };
            v.chunks_exact_mut(2).for_each(swap);
        }
        DataType::Int32 | DataType::UInt32 | DataType::Float32 | DataType::Complex64 => {
            let swap = |chunk: &mut [u8]| {
                let bytes = u32::from_ne_bytes(unsafe { chunk.try_into().unwrap_unchecked() });
                chunk.copy_from_slice(bytes.swap_bytes().to_ne_bytes().as_slice());
            };
            v.chunks_exact_mut(4).for_each(swap);
        }
        DataType::Int64 | DataType::UInt64 | DataType::Float64 | DataType::Complex128 => {
            let swap = |chunk: &mut [u8]| {
                let bytes = u64::from_ne_bytes(unsafe { chunk.try_into().unwrap_unchecked() });
                chunk.copy_from_slice(bytes.swap_bytes().to_ne_bytes().as_slice());
            };
            v.chunks_exact_mut(8).for_each(swap);
        }
        // Variable-sized data types are not supported and are rejected outside of this function
        DataType::String | DataType::Binary => unreachable!(),
    }
}

#[cfg(test)]
mod tests {
    use std::{num::NonZeroU64, sync::Arc};

    use crate::{
        array::{
            codec::{ArrayToBytesCodecTraits, CodecOptions, CodecTraits},
            ArrayBytes, ChunkRepresentation, ChunkShape, FillValue,
        },
        array_subset::ArraySubset,
    };

    use super::*;

    #[test]
    fn codec_bytes_configuration_big() {
        let codec_configuration: BytesCodecConfiguration =
            serde_json::from_str(r#"{"endian":"big"}"#).unwrap();
        let codec = BytesCodec::new_with_configuration(&codec_configuration);
        let metadata = codec.create_metadata().unwrap();
        assert_eq!(
            serde_json::to_string(&metadata).unwrap(),
            r#"{"name":"bytes","configuration":{"endian":"big"}}"#
        );
    }

    #[test]
    fn codec_bytes_configuration_little() {
        let codec_configuration: BytesCodecConfiguration =
            serde_json::from_str(r#"{"endian":"little"}"#).unwrap();
        let codec = BytesCodec::new_with_configuration(&codec_configuration);
        let metadata = codec.create_metadata().unwrap();
        assert_eq!(
            serde_json::to_string(&metadata).unwrap(),
            r#"{"name":"bytes","configuration":{"endian":"little"}}"#
        );
    }

    #[test]
    fn codec_bytes_configuration_none() {
        let codec_configuration: BytesCodecConfiguration = serde_json::from_str(r#"{}"#).unwrap();
        let codec = BytesCodec::new_with_configuration(&codec_configuration);
        let metadata = codec.create_metadata().unwrap();
        assert_eq!(
            serde_json::to_string(&metadata).unwrap(),
            r#"{"name":"bytes"}"#
        );
    }

    fn codec_bytes_round_trip_impl(
        endianness: Option<Endianness>,
        data_type: DataType,
        fill_value: FillValue,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let chunk_shape = vec![NonZeroU64::new(10).unwrap(), NonZeroU64::new(10).unwrap()];
        let chunk_representation =
            ChunkRepresentation::new(chunk_shape, data_type, fill_value).unwrap();
        let size = chunk_representation.num_elements_usize()
            * chunk_representation.data_type().fixed_size().unwrap();
        let bytes: ArrayBytes = (0..size).map(|s| s as u8).collect::<Vec<_>>().into();

        let codec = BytesCodec::new(endianness);

        let encoded = codec.encode(
            bytes.clone(),
            &chunk_representation,
            &CodecOptions::default(),
        )?;
        let decoded = codec
            .decode(encoded, &chunk_representation, &CodecOptions::default())
            .unwrap();
        assert_eq!(bytes, decoded);
        Ok(())
    }

    #[test]
    fn codec_bytes_round_trip_f32() {
        codec_bytes_round_trip_impl(
            Some(Endianness::Big),
            DataType::Float32,
            FillValue::from(0.0f32),
        )
        .unwrap();
        codec_bytes_round_trip_impl(
            Some(Endianness::Little),
            DataType::Float32,
            FillValue::from(0.0f32),
        )
        .unwrap();
    }

    #[test]
    fn codec_bytes_round_trip_u32() {
        codec_bytes_round_trip_impl(
            Some(Endianness::Big),
            DataType::UInt32,
            FillValue::from(0u32),
        )
        .unwrap();
        codec_bytes_round_trip_impl(
            Some(Endianness::Little),
            DataType::UInt32,
            FillValue::from(0u32),
        )
        .unwrap();
    }

    #[test]
    fn codec_bytes_round_trip_u16() {
        codec_bytes_round_trip_impl(
            Some(Endianness::Big),
            DataType::UInt16,
            FillValue::from(0u16),
        )
        .unwrap();
        codec_bytes_round_trip_impl(
            Some(Endianness::Little),
            DataType::UInt16,
            FillValue::from(0u16),
        )
        .unwrap();
    }

    #[test]
    fn codec_bytes_round_trip_u8() {
        codec_bytes_round_trip_impl(Some(Endianness::Big), DataType::UInt8, FillValue::from(0u8))
            .unwrap();
        codec_bytes_round_trip_impl(
            Some(Endianness::Little),
            DataType::UInt8,
            FillValue::from(0u8),
        )
        .unwrap();
        codec_bytes_round_trip_impl(None, DataType::UInt8, FillValue::from(0u8)).unwrap();
    }

    #[test]
    fn codec_bytes_round_trip_i32() {
        codec_bytes_round_trip_impl(Some(Endianness::Big), DataType::Int32, FillValue::from(0))
            .unwrap();
        codec_bytes_round_trip_impl(
            Some(Endianness::Little),
            DataType::Int32,
            FillValue::from(0),
        )
        .unwrap();
    }

    #[test]
    fn codec_bytes_round_trip_i32_endianness_none() {
        assert!(codec_bytes_round_trip_impl(None, DataType::Int32, FillValue::from(0)).is_err());
    }

    #[test]
    fn codec_bytes_round_trip_complex64() {
        codec_bytes_round_trip_impl(
            Some(Endianness::Big),
            DataType::Complex64,
            FillValue::from(num::complex::Complex32::new(0.0, 0.0)),
        )
        .unwrap();
        codec_bytes_round_trip_impl(
            Some(Endianness::Little),
            DataType::Complex64,
            FillValue::from(num::complex::Complex32::new(0.0, 0.0)),
        )
        .unwrap();
    }

    #[test]
    fn codec_bytes_round_trip_complex128() {
        codec_bytes_round_trip_impl(
            Some(Endianness::Big),
            DataType::Complex128,
            FillValue::from(num::complex::Complex64::new(0.0, 0.0)),
        )
        .unwrap();
        codec_bytes_round_trip_impl(
            Some(Endianness::Little),
            DataType::Complex128,
            FillValue::from(num::complex::Complex64::new(0.0, 0.0)),
        )
        .unwrap();
    }

    #[test]
    fn codec_bytes_partial_decode() {
        let chunk_shape: ChunkShape = vec![4, 4].try_into().unwrap();
        let chunk_representation =
            ChunkRepresentation::new(chunk_shape.to_vec(), DataType::UInt8, FillValue::from(0u8))
                .unwrap();
        let elements: Vec<u8> = (0..chunk_representation.num_elements() as u8).collect();
        let bytes: ArrayBytes = elements.into();

        let codec = BytesCodec::new(None);

        let encoded = codec
            .encode(
                bytes.clone(),
                &chunk_representation,
                &CodecOptions::default(),
            )
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

    #[cfg(feature = "async")]
    #[tokio::test]
    async fn codec_bytes_async_partial_decode() {
        let chunk_shape: ChunkShape = vec![4, 4].try_into().unwrap();
        let chunk_representation =
            ChunkRepresentation::new(chunk_shape.to_vec(), DataType::UInt8, FillValue::from(0u8))
                .unwrap();
        let elements: Vec<u8> = (0..chunk_representation.num_elements() as u8).collect();
        let bytes: ArrayBytes = elements.into();

        let codec = BytesCodec::new(None);

        let encoded = codec
            .encode(
                bytes.clone(),
                &chunk_representation,
                &CodecOptions::default(),
            )
            .unwrap();
        let decoded_regions = [ArraySubset::new_with_ranges(&[1..3, 0..1])];
        let input_handle = Arc::new(std::io::Cursor::new(encoded));
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
