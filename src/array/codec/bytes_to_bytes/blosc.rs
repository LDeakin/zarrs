//! The blosc `bytes->bytes` codec.
//!
//! It uses the [blosc](https://www.blosc.org/) container format.
//!
//! See <https://zarr-specs.readthedocs.io/en/latest/v3/codecs/blosc/v1.0.html>.

// NOTE: Zarr implementations MAY provide users an option to choose a shuffle mode automatically based on the typesize or other information, but MUST record in the metadata the mode that is chosen.

mod blosc_codec;
mod blosc_configuration;
mod blosc_partial_decoder;

pub use blosc_codec::BloscCodec;
pub use blosc_configuration::{BloscCodecConfiguration, BloscCodecConfigurationV1};

fn decompress_bytes(bytes: &[u8]) -> Result<Vec<u8>, blosc::BloscError> {
    unsafe {
        // NOTE:
        //  There is limited validation of the blosc encoded data
        //  See [Blosc issue #229](https://github.com/Blosc/c-blosc/issues/229)
        //  This can panic with capacity overflow with invalid data
        blosc::decompress_bytes(bytes)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        array::{
            codec::BytesToBytesCodecTraits, ArrayRepresentation, BytesRepresentation, DataType,
            FillValue,
        },
        array_subset::ArraySubset,
        byte_range::ByteRange,
    };

    use super::*;

    const JSON_VALID1: &'static str = r#"
{
    "cname": "lz4",
    "clevel": 5,
    "shuffle": "shuffle",
    "typesize": 4,
    "blocksize": 0
}"#;

    const JSON_VALID2: &'static str = r#"
{
    "cname": "lz4",
    "clevel": 4,
    "shuffle": "bitshuffle",
    "typesize": 4,
    "blocksize": 0
}"#;

    #[test]
    fn codec_blosc_round_trip1() {
        let elements: Vec<u16> = (0..32).collect();
        let bytes = safe_transmute::transmute_to_bytes(&elements).to_vec();
        let bytes_representation = BytesRepresentation::KnownSize(bytes.len());

        let codec_configuration: BloscCodecConfiguration =
            serde_json::from_str(JSON_VALID1).unwrap();
        let codec = BloscCodec::new_with_configuration(&codec_configuration).unwrap();

        let encoded = codec.encode(bytes.clone()).unwrap();
        let decoded = codec
            .decode(encoded.clone(), &bytes_representation)
            .unwrap();
        assert_eq!(bytes, decoded);
    }

    #[test]
    fn codec_blosc_round_trip2() {
        let elements: Vec<u16> = (0..32).collect();
        let bytes = safe_transmute::transmute_to_bytes(&elements).to_vec();
        let bytes_representation = BytesRepresentation::KnownSize(bytes.len());

        let codec_configuration: BloscCodecConfiguration =
            serde_json::from_str(JSON_VALID2).unwrap();
        let codec = BloscCodec::new_with_configuration(&codec_configuration).unwrap();

        let encoded = codec.encode(bytes.clone()).unwrap();
        let decoded = codec
            .decode(encoded.clone(), &bytes_representation)
            .unwrap();
        assert_eq!(bytes, decoded);
    }

    #[test]
    fn codec_blosc_partial_decode() {
        let array_representation =
            ArrayRepresentation::new(vec![2, 2, 2], DataType::UInt16, FillValue::from(0u16))
                .unwrap();
        let bytes_representation = BytesRepresentation::KnownSize(array_representation.size());

        let elements: Vec<u16> = (0..array_representation.num_elements() as u16).collect();
        let bytes = safe_transmute::transmute_to_bytes(&elements).to_vec();

        let codec_configuration: BloscCodecConfiguration =
            serde_json::from_str(JSON_VALID2).unwrap();
        let codec = BloscCodec::new_with_configuration(&codec_configuration).unwrap();

        let encoded = codec.encode(bytes.clone()).unwrap();
        let decoded_regions: Vec<ByteRange> =
            ArraySubset::new_with_start_shape(vec![0, 1, 0], vec![2, 1, 1])
                .unwrap()
                .byte_ranges(
                    array_representation.shape(),
                    array_representation.element_size(),
                )
                .unwrap();
        let input_handle = Box::new(std::io::Cursor::new(encoded));
        let partial_decoder = codec.partial_decoder(input_handle);
        let decoded = partial_decoder
            .partial_decode(&bytes_representation, &decoded_regions)
            .unwrap();

        let decoded: Vec<u16> = decoded
            .into_iter()
            .flatten()
            .collect::<Vec<_>>()
            .chunks(std::mem::size_of::<u16>())
            .map(|b| u16::from_ne_bytes(b.try_into().unwrap()))
            .collect();

        let answer: Vec<u16> = vec![2, 6];
        assert_eq!(answer, decoded);
    }
}
