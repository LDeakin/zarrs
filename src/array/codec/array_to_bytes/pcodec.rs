//! The `pcodec` array to bytes codec.
//!
//! [Pcodec](https://github.com/mwlon/pcodec) (or Pco, pronounced "pico") losslessly compresses and decompresses numerical sequences with high compression ratio and fast speed.
//!
//! <div class="warning">
//! This codec is experimental and is incompatible with other Zarr V3 implementations.
//! </div>
//!
//! This codec requires the `pcodec` feature, which is disabled by default.
//!
//! See [`PcodecCodecConfigurationV1`] for example `JSON` metadata.

mod pcodec_codec;
mod pcodec_partial_decoder;

pub use crate::metadata::v3::codec::pcodec::{
    PcodecCodecConfiguration, PcodecCodecConfigurationV1, PcodecCompressionLevel,
    PcodecDeltaEncodingOrder,
};

pub use pcodec_codec::PcodecCodec;

use crate::{
    array::codec::{Codec, CodecPlugin},
    config::global_config,
    metadata::v3::{codec::pcodec, MetadataV3},
    plugin::{PluginCreateError, PluginMetadataInvalidError},
};

pub use pcodec::IDENTIFIER;

// Register the codec.
inventory::submit! {
    CodecPlugin::new(IDENTIFIER, is_name_pcodec, create_codec_pcodec)
}

fn is_name_pcodec(name: &str) -> bool {
    name.eq(IDENTIFIER)
        || name
            == global_config()
                .experimental_codec_names()
                .get(IDENTIFIER)
                .expect("experimental codec identifier in global map")
}

pub(crate) fn create_codec_pcodec(metadata: &MetadataV3) -> Result<Codec, PluginCreateError> {
    let configuration = metadata
        .to_configuration()
        .map_err(|_| PluginMetadataInvalidError::new(IDENTIFIER, "codec", metadata.clone()))?;
    let codec = Box::new(PcodecCodec::new_with_configuration(&configuration));
    Ok(Codec::ArrayToBytes(codec))
}

#[cfg(test)]
mod tests {
    use std::{num::NonZeroU64, sync::Arc};

    use crate::{
        array::{
            codec::{ArrayToBytesCodecTraits, CodecOptions},
            transmute_to_bytes_vec, ArrayBytes, ChunkRepresentation, ChunkShape, DataType,
            FillValue,
        },
        array_subset::ArraySubset,
    };

    use super::*;

    const JSON_VALID: &str = r#"{
        "level": 8,
        "delta_encoding_order": 2,
        "mode_spec": "auto",
        "equal_pages_up_to": 262144
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
        let size = chunk_representation.num_elements_usize()
            * chunk_representation.data_type().fixed_size().unwrap();
        let bytes: Vec<u8> = (0..size).map(|s| s as u8).collect();
        let bytes: ArrayBytes = bytes.into();

        let max_encoded_size = codec.compute_encoded_size(&chunk_representation)?;
        let encoded = codec.encode(
            bytes.clone(),
            &chunk_representation,
            &CodecOptions::default(),
        )?;
        assert!((encoded.len() as u64) <= max_encoded_size.size().unwrap());
        let decoded = codec
            .decode(encoded, &chunk_representation, &CodecOptions::default())
            .unwrap();
        assert_eq!(bytes, decoded);
        Ok(())
    }

    #[test]
    fn codec_pcodec_round_trip_u16() {
        codec_pcodec_round_trip_impl(
            &PcodecCodec::new_with_configuration(&serde_json::from_str(JSON_VALID).unwrap()),
            DataType::UInt16,
            FillValue::from(0u16),
        )
        .unwrap();
    }

    #[test]
    fn codec_pcodec_round_trip_u32() {
        codec_pcodec_round_trip_impl(
            &PcodecCodec::new_with_configuration(&serde_json::from_str(JSON_VALID).unwrap()),
            DataType::UInt32,
            FillValue::from(0u32),
        )
        .unwrap();
    }

    #[test]
    fn codec_pcodec_round_trip_u64() {
        codec_pcodec_round_trip_impl(
            &PcodecCodec::new_with_configuration(&serde_json::from_str(JSON_VALID).unwrap()),
            DataType::UInt64,
            FillValue::from(0u64),
        )
        .unwrap();
    }

    #[test]
    fn codec_pcodec_round_trip_i16() {
        codec_pcodec_round_trip_impl(
            &PcodecCodec::new_with_configuration(&serde_json::from_str(JSON_VALID).unwrap()),
            DataType::Int16,
            FillValue::from(0i16),
        )
        .unwrap();
    }

    #[test]
    fn codec_pcodec_round_trip_i32() {
        codec_pcodec_round_trip_impl(
            &PcodecCodec::new_with_configuration(&serde_json::from_str(JSON_VALID).unwrap()),
            DataType::Int32,
            FillValue::from(0i32),
        )
        .unwrap();
    }

    #[test]
    fn codec_pcodec_round_trip_i64() {
        codec_pcodec_round_trip_impl(
            &PcodecCodec::new_with_configuration(&serde_json::from_str(JSON_VALID).unwrap()),
            DataType::Int64,
            FillValue::from(0i64),
        )
        .unwrap();
    }

    #[test]
    fn codec_pcodec_round_trip_f16() {
        codec_pcodec_round_trip_impl(
            &PcodecCodec::new_with_configuration(&serde_json::from_str(JSON_VALID).unwrap()),
            DataType::Float16,
            FillValue::from(half::f16::from_f32(0.0)),
        )
        .unwrap();
    }

    #[test]
    fn codec_pcodec_round_trip_f32() {
        codec_pcodec_round_trip_impl(
            &PcodecCodec::new_with_configuration(&serde_json::from_str(JSON_VALID).unwrap()),
            DataType::Float32,
            FillValue::from(0f32),
        )
        .unwrap();
    }

    #[test]
    fn codec_pcodec_round_trip_f64() {
        codec_pcodec_round_trip_impl(
            &PcodecCodec::new_with_configuration(&serde_json::from_str(JSON_VALID).unwrap()),
            DataType::Float64,
            FillValue::from(0f64),
        )
        .unwrap();
    }

    #[test]
    fn codec_pcodec_round_trip_complex64() {
        codec_pcodec_round_trip_impl(
            &PcodecCodec::new_with_configuration(&serde_json::from_str(JSON_VALID).unwrap()),
            DataType::Complex64,
            FillValue::from(num::complex::Complex32::new(0f32, 0f32)),
        )
        .unwrap();
    }

    #[test]
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
        let bytes: ArrayBytes = bytes.into();

        let codec = PcodecCodec::new_with_configuration(&serde_json::from_str(JSON_VALID).unwrap());

        let encoded = codec
            .encode(
                bytes.clone(),
                &chunk_representation,
                &CodecOptions::default(),
            )
            .unwrap();
        let decoded_regions = [ArraySubset::new_with_ranges(&[1..3, 0..1])];
        let input_handle = Arc::new(std::io::Cursor::new(encoded));
        let partial_decoder = codec
            .partial_decoder(
                input_handle,
                &chunk_representation,
                &CodecOptions::default(),
            )
            .unwrap();
        let decoded_partial_chunk = partial_decoder
            .partial_decode_opt(&decoded_regions, &CodecOptions::default())
            .unwrap();

        let decoded_partial_chunk: Vec<u8> = decoded_partial_chunk
            .into_iter()
            .map(|bytes| bytes.into_fixed().unwrap().into_owned())
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
        let bytes: ArrayBytes = bytes.into();

        let codec = PcodecCodec::new_with_configuration(&serde_json::from_str(JSON_VALID).unwrap());

        let encoded = codec
            .encode(
                bytes.clone(),
                &chunk_representation,
                &CodecOptions::default(),
            )
            .unwrap();
        let decoded_regions = [ArraySubset::new_with_ranges(&[1..3, 0..1])];
        let input_handle = Arc::new(std::io::Cursor::new(encoded));
        let partial_decoder = codec
            .async_partial_decoder(
                input_handle,
                &chunk_representation,
                &CodecOptions::default(),
            )
            .await
            .unwrap();
        let decoded_partial_chunk = partial_decoder
            .partial_decode_opt(&decoded_regions, &CodecOptions::default())
            .await
            .unwrap();

        let decoded_partial_chunk: Vec<u8> = decoded_partial_chunk
            .into_iter()
            .map(|bytes| bytes.into_fixed().unwrap().into_owned())
            .flatten()
            .collect::<Vec<_>>()
            .chunks(std::mem::size_of::<u8>())
            .map(|b| u8::from_ne_bytes(b.try_into().unwrap()))
            .collect();
        let answer: Vec<u32> = vec![4, 8];
        assert_eq!(transmute_to_bytes_vec(answer), decoded_partial_chunk);
    }
}
