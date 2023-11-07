//! The CRC32C checksum `bytes->bytes` codec.
//!
//! Appends a CRC32C checksum of the input bytestream.
//!
//! See <https://zarr-specs.readthedocs.io/en/latest/v3/codecs/crc32c/v1.0.html>.

mod crc32c_codec;
mod crc32c_configuration;
mod crc32c_partial_decoder;

pub use crc32c_codec::Crc32cCodec;
pub use crc32c_configuration::{Crc32cCodecConfiguration, Crc32cCodecConfigurationV1};

const CHECKSUM_SIZE: usize = core::mem::size_of::<u32>();

#[cfg(test)]
mod tests {
    use crate::{
        array::{codec::BytesToBytesCodecTraits, BytesRepresentation},
        byte_range::ByteRange,
    };

    use super::*;

    const JSON1: &str = r#"{}"#;

    #[test]
    fn codec_crc32c() {
        let elements: Vec<u8> = (0..6).collect();
        let bytes = elements;
        let bytes_representation = BytesRepresentation::FixedSize(bytes.len() as u64);

        let codec_configuration: Crc32cCodecConfiguration = serde_json::from_str(JSON1).unwrap();
        let codec = Crc32cCodec::new_with_configuration(&codec_configuration);

        let encoded = codec.encode(bytes.clone()).unwrap();
        let decoded = codec
            .decode(encoded.clone(), &bytes_representation)
            .unwrap();
        assert_eq!(bytes, decoded);

        // Check that the checksum is correct
        let checksum: &[u8; 4] = &encoded
            [encoded.len() - core::mem::size_of::<u32>()..encoded.len()]
            .try_into()
            .unwrap();
        println!("checksum {checksum:?}");
        assert_eq!(checksum, &[74, 207, 235, 48]);
        // println!("checksum {:?}", checksum);
    }

    #[test]
    fn codec_crc32c_partial_decode() {
        let elements: Vec<u8> = (0..32).collect();
        let bytes = elements;
        let bytes_representation = BytesRepresentation::FixedSize(bytes.len() as u64);

        let codec_configuration: Crc32cCodecConfiguration = serde_json::from_str(JSON1).unwrap();
        let codec = Crc32cCodec::new_with_configuration(&codec_configuration);

        let encoded = codec.encode(bytes).unwrap();
        let decoded_regions = [ByteRange::FromStart(3, Some(2))];
        let input_handle = Box::new(std::io::Cursor::new(encoded));
        let partial_decoder = codec.partial_decoder(input_handle);
        let decoded_partial_chunk = partial_decoder
            .partial_decode(&bytes_representation, &decoded_regions)
            .unwrap()
            .unwrap();
        let answer: &[Vec<u8>] = &[vec![3, 4]];
        assert_eq!(answer, decoded_partial_chunk);
    }
}
