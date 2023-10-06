//! The gzip `bytes->bytes` codec.
//!
//! Applies gzip compression.
//!
//! See <https://zarr-specs.readthedocs.io/en/latest/v3/codecs/gzip/v1.0.html>.

mod gzip_codec;
mod gzip_compression_level;
mod gzip_configuration;
mod gzip_partial_decoder;

pub use gzip_codec::GzipCodec;
pub use gzip_compression_level::{GzipCompressionLevel, GzipCompressionLevelError};
pub use gzip_configuration::{GzipCodecConfiguration, GzipCodecConfigurationV1};

#[cfg(test)]
mod tests {
    use crate::{
        array::{codec::BytesToBytesCodecTraits, BytesRepresentation},
        byte_range::ByteRange,
    };

    use super::*;

    const JSON_VALID: &'static str = r#"{
        "level": 1
    }"#;

    #[test]
    fn codec_gzip_configuration_valid() {
        assert!(serde_json::from_str::<GzipCodecConfiguration>(JSON_VALID).is_ok());
    }

    #[test]
    fn codec_gzip_configuration_invalid1() {
        const JSON_INVALID1: &'static str = r#"{
        "level": -1
    }"#;
        assert!(serde_json::from_str::<GzipCodecConfiguration>(JSON_INVALID1).is_err());
    }

    #[test]
    fn codec_gzip_configuration_invalid2() {
        const JSON_INVALID2: &'static str = r#"{
        "level": 10
    }"#;
        assert!(serde_json::from_str::<GzipCodecConfiguration>(JSON_INVALID2).is_err());
    }

    #[test]
    fn codec_gzip_round_trip1() {
        let elements: Vec<u16> = (0..32).collect();
        let bytes = safe_transmute::transmute_to_bytes(&elements).to_vec();
        let bytes_representation = BytesRepresentation::KnownSize(bytes.len() as u64);

        let configuration: GzipCodecConfiguration = serde_json::from_str(JSON_VALID).unwrap();
        let codec = GzipCodec::new_with_configuration(&configuration);

        let encoded = codec.encode(bytes.clone()).unwrap();
        let decoded = codec
            .decode(encoded.clone(), &bytes_representation)
            .unwrap();
        assert_eq!(bytes, decoded);
    }

    #[test]
    fn codec_gzip_partial_decode() {
        let elements: Vec<u16> = (0..8).collect();
        let bytes = safe_transmute::transmute_to_bytes(&elements).to_vec();
        let bytes_representation = BytesRepresentation::KnownSize(bytes.len() as u64);

        let configuration: GzipCodecConfiguration = serde_json::from_str(JSON_VALID).unwrap();
        let codec = GzipCodec::new_with_configuration(&configuration);

        let encoded = codec.encode(bytes.clone()).unwrap();
        let decoded_regions = [
            ByteRange::FromStart(4, Some(4)),
            ByteRange::FromStart(10, Some(2)),
        ];

        let input_handle = Box::new(std::io::Cursor::new(encoded));
        let partial_decoder = codec.partial_decoder(input_handle);
        let decoded_partial_chunk = partial_decoder
            .partial_decode(&bytes_representation, &decoded_regions)
            .unwrap()
            .unwrap();

        let decoded_partial_chunk: Vec<u16> = decoded_partial_chunk
            .into_iter()
            .flatten()
            .collect::<Vec<_>>()
            .chunks(std::mem::size_of::<u16>())
            .map(|b| u16::from_ne_bytes(b.try_into().unwrap()))
            .collect();
        let answer: Vec<u16> = vec![2, 3, 5];
        assert_eq!(answer, decoded_partial_chunk);
    }
}
