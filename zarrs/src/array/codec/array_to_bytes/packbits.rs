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
//!     "padding_encoding": "start_byte"
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

fn element_size_bits(data_type: &DataType) -> Result<u8, CodecError> {
    match data_type {
        DataType::Bool => Ok(1),
        DataType::Extension(ext) => Ok(ext.codec_packbits()?.size_bits()),
        _ => Err(CodecError::UnsupportedDataType(
            data_type.clone(),
            PACKBITS.to_string(),
        )),
    }
}

fn elements_size_bytes(data_type: &DataType, num_elements: u64) -> Result<u64, CodecError> {
    let element_size_bits = element_size_bits(data_type)?;
    Ok((num_elements * u64::from(element_size_bits)).div_ceil(8))
}

fn div_rem_8bit(bit: usize, element_size_bits: usize) -> (usize, usize) {
    let (element, element_bit) = bit.div_rem(&element_size_bits);
    let element_size_bits_padded = 8 * element_size_bits.div_ceil(8);
    let byte = (element * element_size_bits_padded + element_bit) / 8;
    let byte_bit = element_bit % 8;
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
            codec::{ArrayToBytesCodecTraits, CodecOptions},
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
    fn codec_packbits() -> Result<(), Box<dyn std::error::Error>> {
        for encoding in [
            PackBitsPaddingEncoding::None,
            PackBitsPaddingEncoding::StartByte,
            PackBitsPaddingEncoding::EndByte,
        ] {
            let codec = Arc::new(super::PackBitsCodec::new(encoding));
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
            assert!((encoded.len() as u64) <= 40.div_ceil(&8) + 1);

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
}
