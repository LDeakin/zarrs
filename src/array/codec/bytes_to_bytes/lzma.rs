//! The `lzma` bytes to bytes codec.

mod lzma_codec;
mod lzma_configuration;
mod lzma_partial_decoder;

use derive_more::From;
use serde::{Deserialize, Deserializer, Serialize};
use thiserror::Error;

#[derive(Debug, Error, From)]
#[error("{0}")]
struct LzmaError(String);

impl From<&str> for LzmaError {
    fn from(err: &str) -> Self {
        Self(err.to_string())
    }
}

/// An integer from 0 to 9 controlling the compression level.
#[derive(Serialize, Copy, Clone, Debug, Eq, PartialEq)]
pub struct LzmaCompressionLevel(u32);

macro_rules! lzma_compression_level_try_from {
    ( $t:ty ) => {
        impl TryFrom<$t> for LzmaCompressionLevel {
            type Error = $t;
            fn try_from(level: $t) -> Result<Self, Self::Error> {
                if level <= 9 {
                    Ok(Self(u32::from(level)))
                } else {
                    Err(level)
                }
            }
        }
    };
}

lzma_compression_level_try_from!(u8);
lzma_compression_level_try_from!(u16);
lzma_compression_level_try_from!(u32);

impl<'de> Deserialize<'de> for LzmaCompressionLevel {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let level = u32::deserialize(d)?;
        if level <= 9 {
            Ok(Self(level))
        } else {
            Err(serde::de::Error::custom(
                "lzma compression level must be between 0 and 9",
            ))
        }
    }
}

impl LzmaCompressionLevel {
    /// Create a new compression level.
    ///
    /// # Errors
    /// Errors if `compression_level` is not between 0-9.
    pub fn new<N: num::Unsigned + std::cmp::PartialOrd<u32>>(
        compression_level: N,
    ) -> Result<Self, N>
    where
        u32: From<N>,
    {
        if compression_level < 10 {
            Ok(Self(u32::from(compression_level)))
        } else {
            Err(compression_level)
        }
    }

    /// The underlying integer compression level.
    #[must_use]
    pub const fn as_u32(&self) -> u32 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use tests::{lzma_codec::LzmaCodec, lzma_configuration::LzmaCodecConfiguration};

    use crate::{
        array::{
            codec::BytesToBytesCodecTraits, ArrayRepresentation, BytesRepresentation, DataType,
            FillValue,
        },
        array_subset::ArraySubset,
        byte_range::ByteRange,
    };

    use super::*;

    const JSON_VALID1: &str = r#"{"level":5}"#;

    #[test]
    fn codec_lzma_round_trip1() {
        let elements: Vec<u16> = (0..32).collect();
        let bytes = crate::array::transmute_to_bytes_vec(elements);
        let bytes_representation = BytesRepresentation::FixedSize(bytes.len() as u64);

        let codec_configuration: LzmaCodecConfiguration =
            serde_json::from_str(JSON_VALID1).unwrap();
        let codec = LzmaCodec::new_with_configuration(&codec_configuration);

        let encoded = codec.encode(bytes.clone()).unwrap();
        let decoded = codec.decode(encoded, &bytes_representation).unwrap();
        assert_eq!(bytes, decoded);
    }

    #[test]
    fn codec_lzma_partial_decode() {
        let array_representation =
            ArrayRepresentation::new(vec![2, 2, 2], DataType::UInt16, FillValue::from(0u16))
                .unwrap();
        let bytes_representation = BytesRepresentation::FixedSize(array_representation.size());

        let elements: Vec<u16> = (0..array_representation.num_elements() as u16).collect();
        let bytes = crate::array::transmute_to_bytes_vec(elements);

        let codec_configuration: LzmaCodecConfiguration =
            serde_json::from_str(JSON_VALID1).unwrap();
        let codec = LzmaCodec::new_with_configuration(&codec_configuration);

        let encoded = codec.encode(bytes).unwrap();
        let decoded_regions: Vec<ByteRange> = ArraySubset::new_with_ranges(&[0..2, 1..2, 0..1])
            .byte_ranges(
                array_representation.shape(),
                array_representation.element_size(),
            )
            .unwrap();
        let input_handle = Box::new(std::io::Cursor::new(encoded));
        let partial_decoder = codec
            .partial_decoder(input_handle, &bytes_representation)
            .unwrap();
        let decoded = partial_decoder
            .partial_decode(&decoded_regions)
            .unwrap()
            .unwrap();

        let decoded: Vec<u16> = decoded
            .into_iter()
            .flatten()
            .collect::<Vec<_>>()
            .chunks(std::mem::size_of::<u16>())
            .map(|b| u16::from_ne_bytes(b.try_into().unwrap()))
            .collect();

        let answer: Vec<u16> = vec![2, 6];
        assert_eq!(answer, decoded);
    }
}
