/// Zarr V2 group metadata.
pub mod group;

/// Zarr V2 array metadata.
pub mod array;

/// Zarr V2 codec metadata.
pub mod codec {
    /// `bitround` codec metadata.
    pub mod bitround;
    /// `blosc` codec metadata.
    pub mod blosc;
    /// `bz2` codec metadata.
    pub mod bz2;
    /// `gzip` codec metadata.
    pub mod gzip;
    /// `zfpy` codec metadata.
    pub mod zfpy;
    /// `zstd` codec metadata.
    pub mod zstd;
}

pub use array::ArrayMetadataV2;
pub use group::GroupMetadataV2;

mod metadata;
pub use metadata::MetadataV2;

#[cfg(test)]
mod tests {
    use array::{data_type_metadata_v2_to_endianness, data_type_metadata_v2_to_v3_data_type};

    use super::*;
    use crate::{
        array::{chunk_key_encoding::ChunkKeySeparator, ChunkShape, DataType, Endianness},
        metadata::{
            array_metadata_v2_to_v3,
            v3::codec::{
                blosc::{self, BloscCodecConfigurationV1},
                transpose::{self, TransposeCodecConfigurationV1},
            },
        },
    };

    #[test]
    fn array_v2_config() -> Result<(), Box<dyn std::error::Error>> {
        let json = r#"
            {
                "chunks": [
                    1000,
                    1000
                ],
                "compressor": {
                    "id": "blosc",
                    "cname": "lz4",
                    "clevel": 5,
                    "shuffle": 1
                },
                "dtype": "<f8",
                "fill_value": "NaN",
                "filters": [
                    {"id": "delta", "dtype": "<f8", "astype": "<f4"}
                ],
                "order": "F",
                "shape": [
                    10000,
                    10000
                ],
                "zarr_format": 2
            }"#;
        let array_metadata_v2: crate::array::ArrayMetadataV2 = serde_json::from_str(&json).unwrap();
        assert_eq!(
            array_metadata_v2.chunks,
            ChunkShape::try_from(vec![1000, 1000]).unwrap()
        );
        assert_eq!(array_metadata_v2.shape, vec![10000, 10000]);
        assert_eq!(
            array_metadata_v2.dimension_separator,
            ChunkKeySeparator::Dot
        );
        assert_eq!(
            data_type_metadata_v2_to_v3_data_type(&array_metadata_v2.dtype)?,
            DataType::Float64
        );
        assert_eq!(
            data_type_metadata_v2_to_endianness(&array_metadata_v2.dtype)?,
            Some(Endianness::Little),
        );
        println!("{array_metadata_v2:?}");

        let array_metadata_v3 = array_metadata_v2_to_v3(&array_metadata_v2)?;
        println!("{array_metadata_v3:?}");

        let first_codec = array_metadata_v3.codecs.first().unwrap();
        assert_eq!(first_codec.name(), transpose::IDENTIFIER);
        let configuration = first_codec
            .to_configuration::<TransposeCodecConfigurationV1>()
            .unwrap();
        assert_eq!(configuration.order.0, vec![1, 0]);

        let last_codec = array_metadata_v3.codecs.last().unwrap();
        assert_eq!(last_codec.name(), blosc::IDENTIFIER);
        let configuration = last_codec
            .to_configuration::<BloscCodecConfigurationV1>()
            .unwrap();
        assert_eq!(configuration.typesize, Some(8));

        Ok(())
    }
}
