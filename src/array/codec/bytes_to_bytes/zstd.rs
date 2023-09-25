//! The zstd `bytes->bytes` codec.
//!
//! Applies zstd compression.
//!
//! See <https://zarr-specs.readthedocs.io/en/latest/v3/codecs/zstd/v1.0.html>.

mod zstd_codec;
mod zstd_configuration;
mod zstd_partial_decoder;

pub use zstd_codec::ZstdCodec;
pub use zstd_configuration::{
    ZstdCodecConfiguration, ZstdCodecConfigurationV1, ZstdCompressionLevel,
};

#[cfg(test)]
mod tests {
    use crate::{
        array::{codec::BytesToBytesCodecTraits, BytesRepresentation},
        byte_range::ByteRange,
    };

    use super::*;

    const JSON_VALID: &'static str = r#"{
    "level": 22,
    "checksum": false
}"#;

    #[test]
    fn codec_zstd_round_trip1() {
        let elements: Vec<u16> = (0..32).collect();
        let bytes = safe_transmute::transmute_to_bytes(&elements).to_vec();
        let bytes_representation = BytesRepresentation::KnownSize(bytes.len());

        let configuration: ZstdCodecConfiguration = serde_json::from_str(JSON_VALID).unwrap();
        let codec = ZstdCodec::new_with_configuration(&configuration);

        let encoded = codec.encode(bytes.clone()).unwrap();
        let decoded = codec
            .decode(encoded.clone(), &bytes_representation)
            .unwrap();
        assert_eq!(bytes, decoded);
    }

    #[test]
    fn codec_zstd_partial_decode() {
        let elements: Vec<u16> = (0..8).collect();
        let bytes = safe_transmute::transmute_to_bytes(&elements).to_vec();
        let bytes_representation = BytesRepresentation::KnownSize(bytes.len());

        let configuration: ZstdCodecConfiguration = serde_json::from_str(JSON_VALID).unwrap();
        let codec = ZstdCodec::new_with_configuration(&configuration);

        let encoded = codec.encode(bytes.clone()).unwrap();
        let decoded_regions = [ByteRange::Interval(4, 4), ByteRange::Interval(10, 2)];

        let input_handle = Box::new(std::io::Cursor::new(encoded));
        let partial_decoder = codec.partial_decoder(input_handle);
        let decoded_partial_chunk = partial_decoder
            .partial_decode(&bytes_representation, &decoded_regions)
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
