//! The `crc32c` (CRC32C checksum) bytes to bytes codec.
//!
//! Appends a CRC32C checksum of the input bytestream.
//!
//! See <https://zarr-specs.readthedocs.io/en/latest/v3/codecs/crc32c/v1.0.html>.

mod crc32c_codec;
mod crc32c_partial_decoder;

pub use crate::metadata::v3::codec::crc32c::{
    Crc32cCodecConfiguration, Crc32cCodecConfigurationV1,
};
pub use crc32c_codec::Crc32cCodec;

use crate::{
    array::codec::{Codec, CodecPlugin},
    metadata::v3::{codec::crc32c, MetadataV3},
    plugin::{PluginCreateError, PluginMetadataInvalidError},
};

pub use crc32c::IDENTIFIER;

// Register the codec.
inventory::submit! {
    CodecPlugin::new(IDENTIFIER, is_name_crc32c, create_codec_crc32c)
}

fn is_name_crc32c(name: &str) -> bool {
    name.eq(IDENTIFIER)
}

pub(crate) fn create_codec_crc32c(metadata: &MetadataV3) -> Result<Codec, PluginCreateError> {
    let configuration = metadata
        .to_configuration()
        .map_err(|_| PluginMetadataInvalidError::new(IDENTIFIER, "codec", metadata.clone()))?;
    let codec = Box::new(Crc32cCodec::new_with_configuration(&configuration));
    Ok(Codec::BytesToBytes(codec))
}

const CHECKSUM_SIZE: usize = core::mem::size_of::<u32>();

#[cfg(test)]
mod tests {
    use std::{borrow::Cow, sync::Arc};

    use crate::{
        array::{
            codec::{BytesToBytesCodecTraits, CodecOptions, CodecTraits},
            BytesRepresentation,
        },
        byte_range::ByteRange,
    };

    use super::*;

    const JSON1: &str = r#"{}"#;

    #[test]
    fn codec_crc32c_configuration_none() {
        let codec_configuration: Crc32cCodecConfiguration = serde_json::from_str(r#"{}"#).unwrap();
        let codec = Crc32cCodec::new_with_configuration(&codec_configuration);
        let metadata = codec.create_metadata().unwrap();
        assert_eq!(
            serde_json::to_string(&metadata).unwrap(),
            r#"{"name":"crc32c"}"#
        );
    }

    #[test]
    fn codec_crc32c() {
        let elements: Vec<u8> = (0..6).collect();
        let bytes = elements;
        let bytes_representation = BytesRepresentation::FixedSize(bytes.len() as u64);

        let codec_configuration: Crc32cCodecConfiguration = serde_json::from_str(JSON1).unwrap();
        let codec = Crc32cCodec::new_with_configuration(&codec_configuration);

        let encoded = codec
            .encode(Cow::Borrowed(&bytes), &CodecOptions::default())
            .unwrap();
        let decoded = codec
            .decode(
                encoded.clone(),
                &bytes_representation,
                &CodecOptions::default(),
            )
            .unwrap();
        assert_eq!(bytes, decoded.to_vec());

        // Check that the checksum is correct
        let checksum: &[u8; 4] = &encoded
            [encoded.len() - core::mem::size_of::<u32>()..encoded.len()]
            .try_into()
            .unwrap();
        println!("checksum {checksum:?}");
        assert_eq!(checksum, &[20, 133, 9, 65]);
    }

    #[test]
    fn codec_crc32c_partial_decode() {
        let elements: Vec<u8> = (0..32).collect();
        let bytes = elements;
        let bytes_representation = BytesRepresentation::FixedSize(bytes.len() as u64);

        let codec_configuration: Crc32cCodecConfiguration = serde_json::from_str(JSON1).unwrap();
        let codec = Crc32cCodec::new_with_configuration(&codec_configuration);

        let encoded = codec
            .encode(Cow::Borrowed(&bytes), &CodecOptions::default())
            .unwrap();
        let decoded_regions = [ByteRange::FromStart(3, Some(2))];
        let input_handle = Arc::new(std::io::Cursor::new(encoded));
        let partial_decoder = codec
            .partial_decoder(
                input_handle,
                &bytes_representation,
                &CodecOptions::default(),
            )
            .unwrap();
        let decoded_partial_chunk = partial_decoder
            .partial_decode(&decoded_regions, &CodecOptions::default())
            .unwrap()
            .unwrap();
        let answer: &[Vec<u8>] = &[vec![3, 4]];
        assert_eq!(
            answer,
            decoded_partial_chunk
                .into_iter()
                .map(|v| v.to_vec())
                .collect::<Vec<_>>()
        );
    }

    #[cfg(feature = "async")]
    #[tokio::test]
    async fn codec_crc32c_async_partial_decode() {
        let elements: Vec<u8> = (0..32).collect();
        let bytes = elements;
        let bytes_representation = BytesRepresentation::FixedSize(bytes.len() as u64);

        let codec_configuration: Crc32cCodecConfiguration = serde_json::from_str(JSON1).unwrap();
        let codec = Crc32cCodec::new_with_configuration(&codec_configuration);

        let encoded = codec
            .encode(Cow::Borrowed(&bytes), &CodecOptions::default())
            .unwrap();
        let decoded_regions = [ByteRange::FromStart(3, Some(2))];
        let input_handle = Arc::new(std::io::Cursor::new(encoded));
        let partial_decoder = codec
            .async_partial_decoder(
                input_handle,
                &bytes_representation,
                &CodecOptions::default(),
            )
            .await
            .unwrap();
        let decoded_partial_chunk = partial_decoder
            .partial_decode(&decoded_regions, &CodecOptions::default())
            .await
            .unwrap()
            .unwrap();
        let answer: &[Vec<u8>] = &[vec![3, 4]];
        assert_eq!(
            answer,
            decoded_partial_chunk
                .into_iter()
                .map(|v| v.to_vec())
                .collect::<Vec<_>>()
        );
    }
}
