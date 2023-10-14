//! The bytes `array->bytes` codec.
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
use rayon::{prelude::ParallelIterator, slice::ParallelSliceMut};

use crate::array::DataType;

/// The endianness of each element in an array, either `big` or `little`.
#[derive(Copy, Clone, Eq, PartialEq, Debug, Display)]
pub enum Endianness {
    /// Little endian.
    Little,

    /// Big endian.
    Big,
}

impl Endianness {
    fn is_native(self) -> bool {
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

#[cfg(target_endian = "big")]
const NATIVE_ENDIAN: Endianness = Endianness::Big;
#[cfg(target_endian = "little")]
const NATIVE_ENDIAN: Endianness = Endianness::Little;

/// The input length needed to invoke rayon for parallel endianness conversion, if `parallel` is true in `reverse_endianness`.
const MIN_PARALLEL_LENGTH: usize = 4_000_000;

fn reverse_endianness(v: &mut [u8], data_type: &DataType, parallel: bool) {
    match data_type {
        DataType::Bool | DataType::Int8 | DataType::UInt8 | DataType::RawBits(_) => {}
        DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::BFloat16 => {
            if parallel && v.len() >= MIN_PARALLEL_LENGTH {
                v.par_chunks_exact_mut(data_type.size())
                    .for_each(<[u8]>::reverse);
            } else {
                v.chunks_exact_mut(data_type.size())
                    .for_each(<[u8]>::reverse);
            }
        }
        DataType::Complex64 | DataType::Complex128 => {
            if parallel && v.len() >= MIN_PARALLEL_LENGTH {
                v.par_chunks_exact_mut(data_type.size() / 2)
                    .for_each(<[u8]>::reverse);
            } else {
                v.chunks_exact_mut(data_type.size() / 2)
                    .for_each(<[u8]>::reverse);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::array::{codec::ArrayCodecTraits, ArrayRepresentation, DataType, FillValue};

    use super::*;

    #[test]
    fn codec_bytes_configuration_big() {
        const JSON_BIG: &'static str = r#"{
        "endian": "big"
    }"#;
        let codec_configuration: BytesCodecConfiguration = serde_json::from_str(JSON_BIG).unwrap();
        let _ = BytesCodec::new_with_configuration(&codec_configuration);
    }

    #[test]
    fn codec_bytes_configuration_little() {
        const JSON_LITTLE: &'static str = r#"{
        "endian": "little"
    }"#;
        let codec_configuration: BytesCodecConfiguration =
            serde_json::from_str(JSON_LITTLE).unwrap();
        let _ = BytesCodec::new_with_configuration(&codec_configuration);
    }

    fn codec_bytes_round_trip_impl(
        endianness: Option<Endianness>,
        data_type: DataType,
        fill_value: FillValue,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let array_representation =
            ArrayRepresentation::new(vec![10, 10], data_type, fill_value).unwrap();
        let bytes: Vec<u8> = (0..array_representation.size()).map(|s| s as u8).collect();

        let codec = BytesCodec::new(endianness);

        let encoded = codec.encode(bytes.clone(), &array_representation)?;
        let decoded = codec
            .decode(encoded.clone(), &array_representation)
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
}
