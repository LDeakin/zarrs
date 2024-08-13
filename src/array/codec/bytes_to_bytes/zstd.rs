//! The `zstd` bytes to bytes codec.
//!
//! Applies [Zstd](https://tools.ietf.org/html/rfc8878) compression.
//!
//! See <https://zarr-specs.readthedocs.io/en/latest/v3/codecs/zstd/v1.0.html>.

mod zstd_codec;
mod zstd_partial_decoder;

pub use crate::metadata::v3::codec::zstd::{
    ZstdCodecConfiguration, ZstdCodecConfigurationV1, ZstdCompressionLevel,
};
pub use zstd_codec::ZstdCodec;

use crate::{
    array::codec::{Codec, CodecPlugin},
    metadata::v3::{codec::zstd, MetadataV3},
    plugin::{PluginCreateError, PluginMetadataInvalidError},
};

pub use zstd::IDENTIFIER;

// Register the codec.
inventory::submit! {
    CodecPlugin::new(IDENTIFIER, is_name_zstd, create_codec_zstd)
}

fn is_name_zstd(name: &str) -> bool {
    name.eq(IDENTIFIER)
}

pub(crate) fn create_codec_zstd(metadata: &MetadataV3) -> Result<Codec, PluginCreateError> {
    let configuration: ZstdCodecConfiguration = metadata
        .to_configuration()
        .map_err(|_| PluginMetadataInvalidError::new(IDENTIFIER, "codec", metadata.clone()))?;
    let codec = Box::new(ZstdCodec::new_with_configuration(&configuration));
    Ok(Codec::BytesToBytes(codec))
}

#[cfg(test)]
mod tests {
    use std::{borrow::Cow, sync::Arc};

    use crate::{
        array::{
            codec::{BytesToBytesCodecTraits, CodecOptions},
            BytesRepresentation,
        },
        byte_range::ByteRange,
    };

    use super::*;

    const JSON_VALID: &str = r#"{
    "level": 22,
    "checksum": false
}"#;

    #[test]
    #[cfg_attr(miri, ignore)]
    fn codec_zstd_round_trip1() {
        let elements: Vec<u16> = (0..32).collect();
        let bytes = crate::array::transmute_to_bytes_vec(elements);
        let bytes_representation = BytesRepresentation::FixedSize(bytes.len() as u64);

        let configuration: ZstdCodecConfiguration = serde_json::from_str(JSON_VALID).unwrap();
        let codec = ZstdCodec::new_with_configuration(&configuration);

        let encoded = codec
            .encode(Cow::Borrowed(&bytes), &CodecOptions::default())
            .unwrap();
        let decoded = codec
            .decode(encoded, &bytes_representation, &CodecOptions::default())
            .unwrap();
        assert_eq!(bytes, decoded.to_vec());
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn codec_zstd_partial_decode() {
        let elements: Vec<u16> = (0..8).collect();
        let bytes = crate::array::transmute_to_bytes_vec(elements);
        let bytes_representation = BytesRepresentation::FixedSize(bytes.len() as u64);

        let configuration: ZstdCodecConfiguration = serde_json::from_str(JSON_VALID).unwrap();
        let codec = ZstdCodec::new_with_configuration(&configuration);

        let encoded = codec
            .encode(Cow::Borrowed(&bytes), &CodecOptions::default())
            .unwrap();
        let decoded_regions = [
            ByteRange::FromStart(4, Some(4)),
            ByteRange::FromStart(10, Some(2)),
        ];

        let input_handle = Arc::new(std::io::Cursor::new(encoded));
        let partial_decoder = codec
            .partial_decoder(
                input_handle,
                &bytes_representation,
                &CodecOptions::default(),
            )
            .unwrap();
        let decoded_partial_chunk = partial_decoder
            .partial_decode_concat(&decoded_regions, &CodecOptions::default())
            .unwrap()
            .unwrap();

        let decoded_partial_chunk: Vec<u16> = decoded_partial_chunk
            .to_vec()
            .chunks_exact(std::mem::size_of::<u16>())
            .map(|b| u16::from_ne_bytes(b.try_into().unwrap()))
            .collect();
        let answer: Vec<u16> = vec![2, 3, 5];
        assert_eq!(answer, decoded_partial_chunk);
    }

    #[cfg(feature = "async")]
    #[tokio::test]
    #[cfg_attr(miri, ignore)]
    async fn codec_zstd_async_partial_decode() {
        let elements: Vec<u16> = (0..8).collect();
        let bytes = crate::array::transmute_to_bytes_vec(elements);
        let bytes_representation = BytesRepresentation::FixedSize(bytes.len() as u64);

        let configuration: ZstdCodecConfiguration = serde_json::from_str(JSON_VALID).unwrap();
        let codec = ZstdCodec::new_with_configuration(&configuration);

        let encoded = codec
            .encode(Cow::Borrowed(&bytes), &CodecOptions::default())
            .unwrap();
        let decoded_regions = [
            ByteRange::FromStart(4, Some(4)),
            ByteRange::FromStart(10, Some(2)),
        ];

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
            .partial_decode_concat(&decoded_regions, &CodecOptions::default())
            .await
            .unwrap()
            .unwrap();

        let decoded_partial_chunk: Vec<u16> = decoded_partial_chunk
            .to_vec()
            .chunks_exact(std::mem::size_of::<u16>())
            .map(|b| u16::from_ne_bytes(b.try_into().unwrap()))
            .collect();
        let answer: Vec<u16> = vec![2, 3, 5];
        assert_eq!(answer, decoded_partial_chunk);
    }
}
