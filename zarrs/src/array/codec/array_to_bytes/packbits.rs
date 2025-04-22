//! The `packbits` array to packbits codec (Extension).
//!
//! Packs together values with non-byte-aligned sizes.
//!
//! ### Specification
//! - <https://github.com/zarr-developers/zarr-extensions/blob/8a28c319023598d40b9a5b5a0dae0a446d497520/codecs/packbits/README.md>
//!
//! ### Codec `name` Aliases (Zarr V3)
//! - `packbits`
//!
//! ### Codec `id` Aliases (Zarr V2)
//! - `packbits`
//!
//! ### Codec `configuration` Example - [`PackBitsCodecConfiguration`]:
//! ```rust
//! # let JSON = r#"
//! {
//!     "padding_encoding": "first_byte",
//!     "first_bit": null,
//!     "last_bit": null
//! }
//! # "#;
//! # use zarrs_metadata::codec::packbits::PackBitsCodecConfiguration;
//! # serde_json::from_str::<PackBitsCodecConfiguration>(JSON).unwrap();
//! ```

mod packbits_codec;
mod packbits_partial_decoder;

use std::sync::Arc;

pub use crate::metadata::codec::packbits::{
    PackBitsCodecConfiguration, PackBitsCodecConfigurationV1,
};
use crate::{array::codec::CodecError, metadata::codec::PACKBITS};

use num::Integer;
pub use packbits_codec::PackBitsCodec;
use zarrs_data_type::DataType;

use crate::{
    array::codec::{Codec, CodecPlugin},
    metadata::v3::MetadataV3,
    plugin::{PluginCreateError, PluginMetadataInvalidError},
};

// Register the codec.
inventory::submit! {
    CodecPlugin::new(PACKBITS, is_identifier_packbits, create_codec_packbits)
}

fn is_identifier_packbits(identifier: &str) -> bool {
    identifier == PACKBITS
}

pub(crate) fn create_codec_packbits(metadata: &MetadataV3) -> Result<Codec, PluginCreateError> {
    let configuration: PackBitsCodecConfiguration = metadata
        .to_configuration()
        .map_err(|_| PluginMetadataInvalidError::new(PACKBITS, "codec", metadata.clone()))?;
    let codec = Arc::new(PackBitsCodec::new_with_configuration(&configuration)?);
    Ok(Codec::ArrayToBytes(codec))
}

struct DataTypeExtensionPackBitsCodecComponents {
    pub component_size_bits: u64,
    pub num_components: u64,
    pub sign_extension: bool,
}

fn pack_bits_components(
    data_type: &DataType,
) -> Result<DataTypeExtensionPackBitsCodecComponents, CodecError> {
    match data_type {
        DataType::Bool => Ok(DataTypeExtensionPackBitsCodecComponents {
            component_size_bits: 1,
            num_components: 1,
            sign_extension: false,
        }),
        DataType::UInt8 => Ok(DataTypeExtensionPackBitsCodecComponents {
            component_size_bits: 8,
            num_components: 1,
            sign_extension: false,
        }),
        DataType::Int8 => Ok(DataTypeExtensionPackBitsCodecComponents {
            component_size_bits: 8,
            num_components: 1,
            sign_extension: true,
        }),
        DataType::UInt16 | DataType::Float16 | DataType::BFloat16 => {
            Ok(DataTypeExtensionPackBitsCodecComponents {
                component_size_bits: 16,
                num_components: 1,
                sign_extension: false,
            })
        }
        DataType::Int16 => Ok(DataTypeExtensionPackBitsCodecComponents {
            component_size_bits: 16,
            num_components: 1,
            sign_extension: true,
        }),
        DataType::UInt32 | DataType::Float32 => Ok(DataTypeExtensionPackBitsCodecComponents {
            component_size_bits: 32,
            num_components: 1,
            sign_extension: false,
        }),
        DataType::Int32 => Ok(DataTypeExtensionPackBitsCodecComponents {
            component_size_bits: 32,
            num_components: 1,
            sign_extension: true,
        }),
        DataType::UInt64 | DataType::Float64 => Ok(DataTypeExtensionPackBitsCodecComponents {
            component_size_bits: 64,
            num_components: 1,
            sign_extension: false,
        }),
        DataType::Int64 => Ok(DataTypeExtensionPackBitsCodecComponents {
            component_size_bits: 64,
            num_components: 1,
            sign_extension: true,
        }),
        DataType::Complex64 => Ok(DataTypeExtensionPackBitsCodecComponents {
            component_size_bits: 32,
            num_components: 2,
            sign_extension: false,
        }),
        DataType::Complex128 => Ok(DataTypeExtensionPackBitsCodecComponents {
            component_size_bits: 64,
            num_components: 2,
            sign_extension: false,
        }),
        DataType::Extension(ext) => {
            let packbits = ext.codec_packbits()?;
            Ok(DataTypeExtensionPackBitsCodecComponents {
                component_size_bits: packbits.component_size_bits(),
                num_components: packbits.num_components(),
                sign_extension: packbits.sign_extension(),
            })
        }
        _ => Err(CodecError::UnsupportedDataType(
            data_type.clone(),
            PACKBITS.to_string(),
        )),
    }
}

fn div_rem_8bit(bit: u64, element_size_bits: u64) -> (u64, u8) {
    let (element, element_bit) = bit.div_rem(&element_size_bits);
    let element_size_bits_padded = 8 * element_size_bits.div_ceil(8);
    let byte = (element * element_size_bits_padded + element_bit) / 8;
    let byte_bit = (element_bit % 8) as u8;
    (byte, byte_bit)
}

#[cfg(test)]
mod tests {
    use std::{num::NonZeroU64, sync::Arc};

    use num::Integer;
    use zarrs_data_type::{DataType, FillValue};
    use zarrs_metadata::codec::packbits::PackBitsPaddingEncoding;

    use crate::{
        array::{
            codec::{ArrayToBytesCodecTraits, BytesCodec, CodecOptions},
            element::{Element, ElementOwned},
            ChunkRepresentation,
        },
        array_subset::ArraySubset,
    };

    #[test]
    fn div_rem_8bit() {
        use super::div_rem_8bit;

        assert_eq!(div_rem_8bit(0, 1), (0, 0));
        assert_eq!(div_rem_8bit(1, 1), (1, 0));
        assert_eq!(div_rem_8bit(2, 1), (2, 0));

        assert_eq!(div_rem_8bit(0, 3), (0, 0));
        assert_eq!(div_rem_8bit(1, 3), (0, 1));
        assert_eq!(div_rem_8bit(2, 3), (0, 2));
        assert_eq!(div_rem_8bit(3, 3), (1, 0));
        assert_eq!(div_rem_8bit(4, 3), (1, 1));
        assert_eq!(div_rem_8bit(5, 3), (1, 2));

        assert_eq!(div_rem_8bit(0, 12), (0, 0));
        assert_eq!(div_rem_8bit(7, 12), (0, 7));
        assert_eq!(div_rem_8bit(8, 12), (1, 0));
        assert_eq!(div_rem_8bit(9, 12), (1, 1));
        assert_eq!(div_rem_8bit(10, 12), (1, 2));
        assert_eq!(div_rem_8bit(11, 12), (1, 3));
        assert_eq!(div_rem_8bit(12, 12), (2, 0));
        assert_eq!(div_rem_8bit(13, 12), (2, 1));
    }

    #[test]
    fn codec_packbits_bool() -> Result<(), Box<dyn std::error::Error>> {
        for encoding in [
            PackBitsPaddingEncoding::None,
            PackBitsPaddingEncoding::FirstByte,
            PackBitsPaddingEncoding::LastByte,
        ] {
            let codec = Arc::new(super::PackBitsCodec::new(encoding, None, None).unwrap());
            let data_type = DataType::Bool;
            let fill_value = FillValue::from(false);

            let chunk_shape = vec![NonZeroU64::new(8).unwrap(), NonZeroU64::new(5).unwrap()];
            let chunk_representation =
                ChunkRepresentation::new(chunk_shape, data_type.clone(), fill_value).unwrap();
            let elements: Vec<bool> = (0..40).map(|i| i % 3 == 0).collect();
            let bytes = bool::into_array_bytes(&data_type, &elements)?.into_owned();
            // T F F T F
            // F T F F T
            // F F T F F
            // T F F T F
            // ...

            // Encoding
            let encoded = codec.encode(
                bytes.clone(),
                &chunk_representation,
                &CodecOptions::default(),
            )?;
            assert!((encoded.len() as u64) <= (40 * 1).div_ceil(&8) + 1);

            // Decoding
            let decoded = codec
                .decode(
                    encoded.clone(),
                    &chunk_representation,
                    &CodecOptions::default(),
                )
                .unwrap();
            assert_eq!(bytes, decoded);

            // Partial decoding
            let decoded_regions = [ArraySubset::new_with_ranges(&[1..4, 1..4])];
            let input_handle = Arc::new(std::io::Cursor::new(encoded));
            let partial_decoder = codec
                .partial_decoder(
                    input_handle,
                    &chunk_representation,
                    &CodecOptions::default(),
                )
                .unwrap();
            let decoded_partial_chunk = partial_decoder
                .partial_decode(&decoded_regions, &CodecOptions::default())
                .unwrap()
                .pop()
                .unwrap();
            let decoded_partial_chunk =
                bool::from_array_bytes(&data_type, decoded_partial_chunk).unwrap();
            let answer: Vec<bool> =
                vec![true, false, false, false, true, false, false, false, true];
            assert_eq!(answer, decoded_partial_chunk);
        }
        Ok(())
    }

    #[test]
    fn codec_packbits_float32() -> Result<(), Box<dyn std::error::Error>> {
        for encoding in [
            PackBitsPaddingEncoding::None,
            PackBitsPaddingEncoding::FirstByte,
            PackBitsPaddingEncoding::LastByte,
        ] {
            let codec = Arc::new(super::PackBitsCodec::new(encoding, None, None).unwrap());
            let data_type = DataType::Float32;
            let fill_value = FillValue::from(0.0f32);

            let chunk_shape = vec![NonZeroU64::new(8).unwrap(), NonZeroU64::new(5).unwrap()];
            let chunk_representation =
                ChunkRepresentation::new(chunk_shape, data_type.clone(), fill_value).unwrap();
            let elements: Vec<f32> = (0..40).map(|i| i as f32).collect();
            let bytes = f32::into_array_bytes(&data_type, &elements)?.into_owned();

            // Encoding
            let encoded = codec.encode(
                bytes.clone(),
                &chunk_representation,
                &CodecOptions::default(),
            )?;
            assert!((encoded.len() as u64) <= (40 * 32).div_ceil(&8) + 1);

            // Decoding
            let decoded = codec
                .decode(
                    encoded.clone(),
                    &chunk_representation,
                    &CodecOptions::default(),
                )
                .unwrap();
            assert_eq!(bytes, decoded);

            // Check it matches little endian bytes
            let decoded = BytesCodec::little()
                .decode(
                    encoded.clone(),
                    &chunk_representation,
                    &CodecOptions::default(),
                )
                .unwrap();
            assert_eq!(bytes, decoded);
        }
        Ok(())
    }

    #[test]
    fn codec_packbits_int16() -> Result<(), Box<dyn std::error::Error>> {
        for last_bit in 11..15 {
            for first_bit in 0..4 {
                for encoding in [
                    PackBitsPaddingEncoding::None,
                    PackBitsPaddingEncoding::FirstByte,
                    PackBitsPaddingEncoding::LastByte,
                ] {
                    let codec = Arc::new(
                        super::PackBitsCodec::new(encoding, Some(first_bit), Some(last_bit))
                            .unwrap(),
                    );
                    let data_type = DataType::Int16;
                    let fill_value = FillValue::from(0i16);

                    let chunk_shape =
                        vec![NonZeroU64::new(8).unwrap(), NonZeroU64::new(5).unwrap()];
                    let chunk_representation =
                        ChunkRepresentation::new(chunk_shape, data_type.clone(), fill_value)
                            .unwrap();
                    let elements: Vec<i16> = (-20..20).map(|i| (i as i16) << first_bit).collect();
                    let bytes = i16::into_array_bytes(&data_type, &elements)?.into_owned();

                    // Encoding
                    let encoded = codec.encode(
                        bytes.clone(),
                        &chunk_representation,
                        &CodecOptions::default(),
                    )?;
                    assert!(
                        (encoded.len() as u64) <= (40 * (last_bit - first_bit + 1)).div_ceil(8) + 1
                    );

                    // Decoding
                    let decoded = codec
                        .decode(
                            encoded.clone(),
                            &chunk_representation,
                            &CodecOptions::default(),
                        )
                        .unwrap();
                    assert_eq!(elements, i16::from_array_bytes(&data_type, decoded)?);
                }
            }
        }
        Ok(())
    }
}
