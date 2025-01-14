//! The `fletcher32` bytes to bytes codec.
//!
//! Appends a fletcher32 checksum of the input bytestream.
//!
//! This is based on the `numcodecs` implementation.
//! See <https://numcodecs.readthedocs.io/en/latest/checksum32.html#fletcher32>.
//!
//! <div class="warning">
//! This codec is experimental and may be incompatible with other Zarr V3 implementations.
//! </div>
//!
//! This codec requires the `fletcher32` feature, which is disabled by default.
//!
//! See [`Fletcher32CodecConfigurationV1`] for example `JSON` metadata.

mod fletcher32_codec;

use std::sync::Arc;

pub use crate::metadata::v3::array::codec::fletcher32::{
    Fletcher32CodecConfiguration, Fletcher32CodecConfigurationV1,
};
pub use fletcher32_codec::Fletcher32Codec;

use crate::{
    array::codec::{Codec, CodecPlugin},
    config::global_config,
    metadata::v3::{array::codec::fletcher32, MetadataV3},
    plugin::{PluginCreateError, PluginMetadataInvalidError},
};

pub use fletcher32::IDENTIFIER;

// Register the codec.
inventory::submit! {
    CodecPlugin::new(IDENTIFIER, is_name_fletcher32, create_codec_fletcher32)
}

fn is_name_fletcher32(name: &str) -> bool {
    name.eq(IDENTIFIER)
        || name
            == global_config()
                .experimental_codec_names()
                .get(IDENTIFIER)
                .expect("experimental codec identifier in global map")
}

pub(crate) fn create_codec_fletcher32(metadata: &MetadataV3) -> Result<Codec, PluginCreateError> {
    let configuration = metadata
        .to_configuration()
        .map_err(|_| PluginMetadataInvalidError::new(IDENTIFIER, "codec", metadata.clone()))?;
    let codec = Arc::new(Fletcher32Codec::new_with_configuration(&configuration));
    Ok(Codec::BytesToBytes(codec))
}

const CHECKSUM_SIZE: usize = size_of::<u32>();

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
    fn codec_fletcher32_configuration_none() {
        let codec_configuration: Fletcher32CodecConfiguration =
            serde_json::from_str(r#"{}"#).unwrap();
        let codec = Fletcher32Codec::new_with_configuration(&codec_configuration);
        let metadata = codec.create_metadata().unwrap();
        assert_eq!(
            serde_json::to_string(&metadata).unwrap(),
            r#"{"name":"fletcher32"}"#
        );
    }

    #[test]
    fn codec_fletcher32() {
        let elements: Vec<u8> = (0..6).collect();
        let bytes = elements;
        let bytes_representation = BytesRepresentation::FixedSize(bytes.len() as u64);

        let codec_configuration: Fletcher32CodecConfiguration =
            serde_json::from_str(JSON1).unwrap();
        let codec = Fletcher32Codec::new_with_configuration(&codec_configuration);

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
        let checksum: &[u8; 4] = &encoded[encoded.len() - size_of::<u32>()..encoded.len()]
            .try_into()
            .unwrap();
        println!("checksum {checksum:?}");
        assert_eq!(checksum, &[9, 6, 14, 8]); // TODO: CHECK
    }

    #[test]
    fn codec_fletcher32_partial_decode() {
        let elements: Vec<u8> = (0..32).collect();
        let bytes = elements;
        let bytes_representation = BytesRepresentation::FixedSize(bytes.len() as u64);

        let codec_configuration: Fletcher32CodecConfiguration =
            serde_json::from_str(JSON1).unwrap();
        let codec = Arc::new(Fletcher32Codec::new_with_configuration(
            &codec_configuration,
        ));

        let encoded = codec
            .encode(Cow::Owned(bytes), &CodecOptions::default())
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
    async fn codec_fletcher32_async_partial_decode() {
        let elements: Vec<u8> = (0..32).collect();
        let bytes = elements;
        let bytes_representation = BytesRepresentation::FixedSize(bytes.len() as u64);

        let codec_configuration: Fletcher32CodecConfiguration =
            serde_json::from_str(JSON1).unwrap();
        let codec = Arc::new(Fletcher32Codec::new_with_configuration(
            &codec_configuration,
        ));

        let encoded = codec
            .encode(Cow::Owned(bytes), &CodecOptions::default())
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
