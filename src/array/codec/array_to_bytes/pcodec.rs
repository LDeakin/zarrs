//! The `pcodec` array to bytes codec.

mod pcodec_codec;
mod pcodec_configuration;
mod pcodec_partial_decoder;

pub use pcodec_configuration::{PcodecCodecConfiguration, PcodecCodecConfigurationV1};

pub use pcodec_codec::PcodecCodec;

use serde::{Deserialize, Deserializer, Serialize};

use crate::{
    array::codec::{Codec, CodecPlugin},
    metadata::Metadata,
    plugin::{PluginCreateError, PluginMetadataInvalidError},
};

/// The identifier for the `pcodec` codec.
pub const IDENTIFIER: &str = "pcodec";

// Register the codec.
inventory::submit! {
    CodecPlugin::new(IDENTIFIER, is_name_pcodec, create_codec_pcodec)
}

fn is_name_pcodec(name: &str) -> bool {
    name.eq(IDENTIFIER)
}

pub(crate) fn create_codec_pcodec(metadata: &Metadata) -> Result<Codec, PluginCreateError> {
    let configuration = if metadata.configuration_is_none_or_empty() {
        PcodecCodecConfiguration::default()
    } else {
        metadata
            .to_configuration()
            .map_err(|_| PluginMetadataInvalidError::new(IDENTIFIER, "codec", metadata.clone()))?
    };
    let codec = Box::new(PcodecCodec::new_with_configuration(&configuration));
    Ok(Codec::ArrayToBytes(codec))
}

/// An integer from 0 to 12 controlling the compression level.
///
/// See <https://docs.rs/pco/latest/pco/struct.ChunkConfig.html#structfield.compression_level>.
///
/// - Level 0 achieves only a small amount of compression.
/// - Level 8 achieves very good compression and runs only slightly slower.
/// - Level 12 achieves marginally better compression than 8 and may run several times slower.
#[derive(Serialize, Copy, Clone, Debug, Eq, PartialEq)]
pub struct PcodecCompressionLevel(u8);

impl Default for PcodecCompressionLevel {
    fn default() -> Self {
        Self(8)
    }
}

macro_rules! pcodec_compression_level_try_from {
    ( $t:ty ) => {
        impl TryFrom<$t> for PcodecCompressionLevel {
            type Error = $t;
            fn try_from(level: $t) -> Result<Self, Self::Error> {
                if level <= 12 {
                    Ok(Self(unsafe { u8::try_from(level).unwrap_unchecked() }))
                } else {
                    Err(level)
                }
            }
        }
    };
}

pcodec_compression_level_try_from!(u8);
pcodec_compression_level_try_from!(u16);
pcodec_compression_level_try_from!(u32);
pcodec_compression_level_try_from!(u64);
pcodec_compression_level_try_from!(usize);

impl<'de> Deserialize<'de> for PcodecCompressionLevel {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let level = u8::deserialize(d)?;
        if level <= 12 {
            Ok(Self(level))
        } else {
            Err(serde::de::Error::custom(
                "pcodec compression level must be between 0 and 12",
            ))
        }
    }
}

impl PcodecCompressionLevel {
    /// Create a new compression level.
    ///
    /// # Errors
    /// Errors if `compression_level` is not between 0-12.
    pub fn new<N: num::Unsigned + std::cmp::PartialOrd<u8>>(compression_level: N) -> Result<Self, N>
    where
        u8: TryFrom<N>,
    {
        if compression_level <= 12 {
            Ok(Self(unsafe {
                u8::try_from(compression_level).unwrap_unchecked()
            }))
        } else {
            Err(compression_level)
        }
    }

    /// The underlying integer compression level.
    #[must_use]
    pub const fn as_usize(&self) -> usize {
        self.0 as usize
    }
}

/// An integer from 0 to 7 controlling the delta encoding order.
///
/// It is the number of times to apply delta encoding before compressing.
/// See <https://docs.rs/pco/latest/pco/struct.ChunkConfig.html#structfield.delta_encoding_order>.
///
/// - 0th order takes numbers as-is. This is perfect for columnar data were the order is essentially random.
/// - 1st order takes consecutive differences, leaving `[0, 2, 0, 2, 0, 2, 0]`. This is best for continuous but noisy time series data, like stock prices or most time series data.
/// - 2nd order takes consecutive differences again, leaving `[2, -2, 2, -2, 2, -2]`. This is best for piecewise-linear or somewhat quadratic data.
/// - Even higher-order is best for time series that are very smooth, like temperature or light sensor readings.
#[derive(Serialize, Copy, Clone, Debug, Eq, PartialEq)]
pub struct PcodecDeltaEncodingOrder(u8);

macro_rules! pcodec_delta_encoding_order_level_try_from {
    ( $t:ty ) => {
        impl TryFrom<$t> for PcodecDeltaEncodingOrder {
            type Error = $t;
            fn try_from(level: $t) -> Result<Self, Self::Error> {
                if level <= 7 {
                    Ok(Self(unsafe { u8::try_from(level).unwrap_unchecked() }))
                } else {
                    Err(level)
                }
            }
        }
    };
}

pcodec_delta_encoding_order_level_try_from!(u8);
pcodec_delta_encoding_order_level_try_from!(u16);
pcodec_delta_encoding_order_level_try_from!(u32);
pcodec_delta_encoding_order_level_try_from!(u64);
pcodec_delta_encoding_order_level_try_from!(usize);

impl<'de> Deserialize<'de> for PcodecDeltaEncodingOrder {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let level = u8::deserialize(d)?;
        if level <= 7 {
            Ok(Self(level))
        } else {
            Err(serde::de::Error::custom(
                "pcodec compression level must be between 0 and 7",
            ))
        }
    }
}

impl PcodecDeltaEncodingOrder {
    /// Create a new delta encoding order.
    ///
    /// # Errors
    /// Errors if `delta_encoding_order` is not between 0-7.
    pub fn new<N: num::Unsigned + std::cmp::PartialOrd<u8>>(
        delta_encoding_order: N,
    ) -> Result<Self, N>
    where
        u8: TryFrom<N>,
    {
        if delta_encoding_order <= 7 {
            Ok(Self(unsafe {
                u8::try_from(delta_encoding_order).unwrap_unchecked()
            }))
        } else {
            Err(delta_encoding_order)
        }
    }

    /// The underlying delta encoding order.
    #[must_use]
    pub const fn as_usize(&self) -> usize {
        self.0 as usize
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use crate::{
        array::{
            codec::{ArrayCodecTraits, ArrayToBytesCodecTraits},
            transmute_to_bytes_vec, ChunkRepresentation, ChunkShape, DataType, FillValue,
        },
        array_subset::ArraySubset,
    };

    use super::*;

    const JSON_VALID: &str = r#"{
        "level": 8,
        "delta_encoding_order": 2,
        "int_mult_spec": true,
        "float_mult_spec": true,
        "max_page_n": 262144
    }"#;

    #[test]
    fn codec_pcodec_configuration() {
        let codec_configuration: PcodecCodecConfiguration =
            serde_json::from_str(JSON_VALID).unwrap();
        let _ = PcodecCodec::new_with_configuration(&codec_configuration);
    }

    fn codec_pcodec_round_trip_impl(
        codec: &PcodecCodec,
        data_type: DataType,
        fill_value: FillValue,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let chunk_shape = vec![NonZeroU64::new(10).unwrap(), NonZeroU64::new(10).unwrap()];
        let chunk_representation =
            ChunkRepresentation::new(chunk_shape, data_type, fill_value).unwrap();
        let bytes: Vec<u8> = (0..chunk_representation.size()).map(|s| s as u8).collect();

        let encoded = codec.encode(bytes.clone(), &chunk_representation)?;
        let decoded = codec.decode(encoded, &chunk_representation).unwrap();
        assert_eq!(bytes, decoded);
        Ok(())
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn codec_pcodec_round_trip_u32() {
        codec_pcodec_round_trip_impl(
            &PcodecCodec::new_with_configuration(&serde_json::from_str(JSON_VALID).unwrap()),
            DataType::UInt32,
            FillValue::from(0u32),
        )
        .unwrap();
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn codec_pcodec_round_trip_u64() {
        codec_pcodec_round_trip_impl(
            &PcodecCodec::new_with_configuration(&serde_json::from_str(JSON_VALID).unwrap()),
            DataType::UInt64,
            FillValue::from(0u64),
        )
        .unwrap();
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn codec_pcodec_round_trip_i32() {
        codec_pcodec_round_trip_impl(
            &PcodecCodec::new_with_configuration(&serde_json::from_str(JSON_VALID).unwrap()),
            DataType::Int32,
            FillValue::from(0i32),
        )
        .unwrap();
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn codec_pcodec_round_trip_i64() {
        codec_pcodec_round_trip_impl(
            &PcodecCodec::new_with_configuration(&serde_json::from_str(JSON_VALID).unwrap()),
            DataType::Int64,
            FillValue::from(0i64),
        )
        .unwrap();
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn codec_pcodec_round_trip_f32() {
        codec_pcodec_round_trip_impl(
            &PcodecCodec::new_with_configuration(&serde_json::from_str(JSON_VALID).unwrap()),
            DataType::Float32,
            FillValue::from(0f32),
        )
        .unwrap();
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn codec_pcodec_round_trip_f64() {
        codec_pcodec_round_trip_impl(
            &PcodecCodec::new_with_configuration(&serde_json::from_str(JSON_VALID).unwrap()),
            DataType::Float64,
            FillValue::from(0f64),
        )
        .unwrap();
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn codec_pcodec_round_trip_complex64() {
        codec_pcodec_round_trip_impl(
            &PcodecCodec::new_with_configuration(&serde_json::from_str(JSON_VALID).unwrap()),
            DataType::Complex64,
            FillValue::from(num::complex::Complex32::new(0f32, 0f32)),
        )
        .unwrap();
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn codec_pcodec_round_trip_complex128() {
        codec_pcodec_round_trip_impl(
            &PcodecCodec::new_with_configuration(&serde_json::from_str(JSON_VALID).unwrap()),
            DataType::Complex128,
            FillValue::from(num::complex::Complex64::new(0f64, 0f64)),
        )
        .unwrap();
    }

    #[test]
    fn codec_pcodec_round_trip_u8() {
        assert!(codec_pcodec_round_trip_impl(
            &PcodecCodec::new_with_configuration(&serde_json::from_str(JSON_VALID).unwrap()),
            DataType::UInt8,
            FillValue::from(0u8),
        )
        .is_err());
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn codec_pcodec_partial_decode() {
        let chunk_shape: ChunkShape = vec![4, 4].try_into().unwrap();
        let chunk_representation = ChunkRepresentation::new(
            chunk_shape.to_vec(),
            DataType::UInt32,
            FillValue::from(0u32),
        )
        .unwrap();
        let elements: Vec<u32> = (0..chunk_representation.num_elements() as u32).collect();
        let bytes = transmute_to_bytes_vec(elements);

        let codec = PcodecCodec::new_with_configuration(&serde_json::from_str(JSON_VALID).unwrap());

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
        let answer: Vec<u32> = vec![4, 8];
        assert_eq!(transmute_to_bytes_vec(answer), decoded_partial_chunk);
    }

    #[cfg(feature = "async")]
    #[tokio::test]
    #[cfg_attr(miri, ignore)]
    async fn codec_pcodec_async_partial_decode() {
        let chunk_shape: ChunkShape = vec![4, 4].try_into().unwrap();
        let chunk_representation = ChunkRepresentation::new(
            chunk_shape.to_vec(),
            DataType::UInt32,
            FillValue::from(0u32),
        )
        .unwrap();
        let elements: Vec<u32> = (0..chunk_representation.num_elements() as u32).collect();
        let bytes = transmute_to_bytes_vec(elements);

        let codec = PcodecCodec::new_with_configuration(&serde_json::from_str(JSON_VALID).unwrap());

        let encoded = codec.encode(bytes, &chunk_representation).unwrap();
        let decoded_regions = [ArraySubset::new_with_ranges(&[1..3, 0..1])];
        let input_handle = Box::new(std::io::Cursor::new(encoded));
        let partial_decoder = codec
            .async_partial_decoder(input_handle, &chunk_representation)
            .await
            .unwrap();
        let decoded_partial_chunk = partial_decoder
            .partial_decode(&decoded_regions)
            .await
            .unwrap();

        let decoded_partial_chunk: Vec<u8> = decoded_partial_chunk
            .into_iter()
            .flatten()
            .collect::<Vec<_>>()
            .chunks(std::mem::size_of::<u8>())
            .map(|b| u8::from_ne_bytes(b.try_into().unwrap()))
            .collect();
        let answer: Vec<u32> = vec![4, 8];
        assert_eq!(transmute_to_bytes_vec(answer), decoded_partial_chunk);
    }
}
