//! The `bytes` array to bytes codec.
//!
//! Encodes arrays of fixed-size numeric data types as little endian or big endian in lexicographical order.
//!
//! See <https://zarr-specs.readthedocs.io/en/latest/v3/codecs/bytes/v1.0.html>.

mod bytes_codec;
mod bytes_configuration;
mod bytes_partial_decoder;

pub use bytes_configuration::{BytesCodecConfiguration, BytesCodecConfigurationV1};

pub use bytes_codec::BytesCodec;

use derive_more::Display;

use crate::{
    array::{
        codec::{Codec, CodecPlugin},
        DataType,
    },
    metadata::Metadata,
    plugin::{PluginCreateError, PluginMetadataInvalidError},
};

/// The identifier for the `bytes` codec.
pub const IDENTIFIER: &str = "bytes";

// Register the codec.
inventory::submit! {
    CodecPlugin::new(IDENTIFIER, is_name_bytes, create_codec_bytes)
}

fn is_name_bytes(name: &str) -> bool {
    name.eq(IDENTIFIER)
}

pub(crate) fn create_codec_bytes(metadata: &Metadata) -> Result<Codec, PluginCreateError> {
    let configuration: BytesCodecConfiguration = metadata
        .to_configuration()
        .map_err(|_| PluginMetadataInvalidError::new(IDENTIFIER, "codec", metadata.clone()))?;
    let codec = Box::new(BytesCodec::new_with_configuration(&configuration));
    Ok(Codec::ArrayToBytes(codec))
}

/// The endianness of each element in an array, either `big` or `little`.
#[derive(Copy, Clone, Eq, PartialEq, Debug, Display)]
pub enum Endianness {
    /// Little endian.
    Little,

    /// Big endian.
    Big,
}

impl Endianness {
    /// Return true if the endianness matches the endianness of the CPU.
    #[must_use]
    pub fn is_native(self) -> bool {
        self == NATIVE_ENDIAN
    }
}

impl serde::Serialize for Endianness {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Little => s.serialize_str("little"),
            Self::Big => s.serialize_str("big"),
        }
    }
}

impl<'de> serde::Deserialize<'de> for Endianness {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let value = serde_json::Value::deserialize(d)?;
        if let serde_json::Value::String(string) = value {
            if string == "little" {
                return Ok(Self::Little);
            } else if string == "big" {
                return Ok(Self::Big);
            }
        }
        Err(serde::de::Error::custom(
            "endian: A string equal to either \"big\" or \"little\"",
        ))
    }
}

/// The endianness of the CPU.
pub const NATIVE_ENDIAN: Endianness = if cfg!(target_endian = "big") {
    Endianness::Big
} else {
    Endianness::Little
};

fn reverse_endianness(v: &mut [u8], data_type: &DataType) {
    match data_type {
        DataType::Bool | DataType::Int8 | DataType::UInt8 | DataType::RawBits(_) => {}
        DataType::Int16 | DataType::UInt16 | DataType::Float16 | DataType::BFloat16 => {
            let swap = |chunk: &mut [u8]| {
                let bytes = u16::from_ne_bytes(chunk.try_into().unwrap());
                chunk.copy_from_slice(bytes.swap_bytes().to_ne_bytes().as_slice());
            };
            v.chunks_exact_mut(2).for_each(swap);
        }
        DataType::Int32 | DataType::UInt32 | DataType::Float32 | DataType::Complex64 => {
            let swap = |chunk: &mut [u8]| {
                let bytes = u32::from_ne_bytes(chunk.try_into().unwrap());
                chunk.copy_from_slice(bytes.swap_bytes().to_ne_bytes().as_slice());
            };
            v.chunks_exact_mut(4).for_each(swap);
        }
        DataType::Int64 | DataType::UInt64 | DataType::Float64 | DataType::Complex128 => {
            let swap = |chunk: &mut [u8]| {
                let bytes = u64::from_ne_bytes(chunk.try_into().unwrap());
                chunk.copy_from_slice(bytes.swap_bytes().to_ne_bytes().as_slice());
            };
            v.chunks_exact_mut(8).for_each(swap);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use crate::{
        array::{
            codec::{ArrayCodecTraits, ArrayToBytesCodecTraits, CodecOptions, CodecTraits},
            ChunkRepresentation, ChunkShape, FillValue,
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
        let bytes: Vec<u8> = (0..chunk_representation.size()).map(|s| s as u8).collect();

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
        let bytes = elements;

        let codec = BytesCodec::new(None);

        let encoded = codec
            .encode(bytes, &chunk_representation, &CodecOptions::default())
            .unwrap();
        let decoded_regions = [ArraySubset::new_with_ranges(&[1..3, 0..1])];
        let input_handle = Box::new(std::io::Cursor::new(encoded));
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
        let bytes = elements;

        let codec = BytesCodec::new(None);

        let encoded = codec
            .encode(bytes, &chunk_representation, &CodecOptions::default())
            .unwrap();
        let decoded_regions = [ArraySubset::new_with_ranges(&[1..3, 0..1])];
        let input_handle = Box::new(std::io::Cursor::new(encoded));
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
            .flatten()
            .collect::<Vec<_>>()
            .chunks(std::mem::size_of::<u8>())
            .map(|b| u8::from_ne_bytes(b.try_into().unwrap()))
            .collect();
        let answer: Vec<u8> = vec![4, 8];
        assert_eq!(answer, decoded_partial_chunk);
    }
}
