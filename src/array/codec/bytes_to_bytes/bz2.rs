//! The `bz2` (bzip2) bytes to bytes codec.
//!
//! <div class="warning">
//! This codec is experimental and is incompatible with other Zarr V3 implementations.
//! </div>
//!
//! This codec requires the `bz2` feature, which is disabled by default.
//!
//! See [`Bz2CodecConfigurationV1`] for example `JSON` metadata.

mod bz2_codec;
mod bz2_partial_decoder;

use crate::{
    array::codec::{Codec, CodecPlugin},
    config::global_config,
    metadata::v3::{codec::bz2, MetadataV3},
    plugin::{PluginCreateError, PluginMetadataInvalidError},
};

pub use crate::metadata::v3::codec::bz2::{
    Bz2CodecConfiguration, Bz2CodecConfigurationV1, Bz2CompressionLevel,
};

pub use self::bz2_codec::Bz2Codec;

pub use bz2::IDENTIFIER;

// Register the codec.
inventory::submit! {
    CodecPlugin::new(IDENTIFIER, is_name_bz2, create_codec_bz2)
}

fn is_name_bz2(name: &str) -> bool {
    name.eq(IDENTIFIER)
        || name
            == global_config()
                .experimental_codec_names()
                .get(IDENTIFIER)
                .expect("experimental codec identifier in global map")
}

pub(crate) fn create_codec_bz2(metadata: &MetadataV3) -> Result<Codec, PluginCreateError> {
    let configuration: Bz2CodecConfiguration = metadata
        .to_configuration()
        .map_err(|_| PluginMetadataInvalidError::new(IDENTIFIER, "codec", metadata.clone()))?;
    let codec = Box::new(Bz2Codec::new_with_configuration(&configuration));
    Ok(Codec::BytesToBytes(codec))
}

#[cfg(test)]
mod tests {
    use std::{borrow::Cow, sync::Arc};

    use crate::{
        array::{
            codec::{BytesToBytesCodecTraits, CodecOptions},
            ArrayRepresentation, BytesRepresentation, DataType, FillValue,
        },
        array_subset::ArraySubset,
        byte_range::ByteRange,
    };

    use super::*;

    const JSON_VALID1: &str = r#"
{
    "level": 5
}"#;

    #[test]
    #[cfg_attr(miri, ignore)]
    fn codec_bz2_round_trip1() {
        let elements: Vec<u16> = (0..32).collect();
        let bytes = crate::array::transmute_to_bytes_vec(elements);
        let bytes_representation = BytesRepresentation::FixedSize(bytes.len() as u64);

        let codec_configuration: Bz2CodecConfiguration = serde_json::from_str(JSON_VALID1).unwrap();
        let codec = Bz2Codec::new_with_configuration(&codec_configuration);

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
    fn codec_bz2_partial_decode() {
        let array_representation =
            ArrayRepresentation::new(vec![2, 2, 2], DataType::UInt16, FillValue::from(0u16))
                .unwrap();
        let data_type_size = array_representation.data_type().fixed_size().unwrap();
        let array_size = array_representation.num_elements_usize() * data_type_size;
        let bytes_representation = BytesRepresentation::FixedSize(array_size as u64);

        let elements: Vec<u16> = (0..array_representation.num_elements() as u16).collect();
        let bytes = crate::array::transmute_to_bytes_vec(elements);

        let codec_configuration: Bz2CodecConfiguration = serde_json::from_str(JSON_VALID1).unwrap();
        let codec = Bz2Codec::new_with_configuration(&codec_configuration);

        let encoded = codec
            .encode(Cow::Borrowed(&bytes), &CodecOptions::default())
            .unwrap();
        let decoded_regions: Vec<ByteRange> = ArraySubset::new_with_ranges(&[0..2, 1..2, 0..1])
            .byte_ranges(array_representation.shape(), data_type_size)
            .unwrap();
        let input_handle = Arc::new(std::io::Cursor::new(encoded));
        let partial_decoder = codec
            .partial_decoder(
                input_handle,
                &bytes_representation,
                &CodecOptions::default(),
            )
            .unwrap();
        let decoded = partial_decoder
            .partial_decode_concat(&decoded_regions, &CodecOptions::default())
            .unwrap()
            .unwrap();

        let decoded: Vec<u16> = decoded
            .to_vec()
            .chunks_exact(std::mem::size_of::<u16>())
            .map(|b| u16::from_ne_bytes(b.try_into().unwrap()))
            .collect();

        let answer: Vec<u16> = vec![2, 6];
        assert_eq!(answer, decoded);
    }

    #[cfg(feature = "async")]
    #[tokio::test]
    #[cfg_attr(miri, ignore)]
    async fn codec_bz2_async_partial_decode() {
        let array_representation =
            ArrayRepresentation::new(vec![2, 2, 2], DataType::UInt16, FillValue::from(0u16))
                .unwrap();
        let data_type_size = array_representation.data_type().fixed_size().unwrap();
        let array_size = array_representation.num_elements_usize() * data_type_size;
        let bytes_representation = BytesRepresentation::FixedSize(array_size as u64);

        let elements: Vec<u16> = (0..array_representation.num_elements() as u16).collect();
        let bytes = crate::array::transmute_to_bytes_vec(elements);

        let codec_configuration: Bz2CodecConfiguration = serde_json::from_str(JSON_VALID1).unwrap();
        let codec = Bz2Codec::new_with_configuration(&codec_configuration);

        let encoded = codec
            .encode(Cow::Borrowed(&bytes), &CodecOptions::default())
            .unwrap();
        let decoded_regions: Vec<ByteRange> = ArraySubset::new_with_ranges(&[0..2, 1..2, 0..1])
            .byte_ranges(array_representation.shape(), data_type_size)
            .unwrap();
        let input_handle = Arc::new(std::io::Cursor::new(encoded));
        let partial_decoder = codec
            .async_partial_decoder(
                input_handle,
                &bytes_representation,
                &CodecOptions::default(),
            )
            .await
            .unwrap();
        let decoded = partial_decoder
            .partial_decode_concat(&decoded_regions, &CodecOptions::default())
            .await
            .unwrap()
            .unwrap();

        let decoded: Vec<u16> = decoded
            .to_vec()
            .chunks_exact(std::mem::size_of::<u16>())
            .map(|b| u16::from_ne_bytes(b.try_into().unwrap()))
            .collect();

        let answer: Vec<u16> = vec![2, 6];
        assert_eq!(answer, decoded);
    }
}
