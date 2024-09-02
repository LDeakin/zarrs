/// Zarr V2 group metadata.
pub mod group;

/// Zarr V2 array metadata.
pub mod array;

pub use array::ArrayMetadataV2;
pub use group::GroupMetadataV2;

mod metadata;
pub use metadata::MetadataV2;

/// V2 node metadata ([`ArrayMetadataV2`] or [`GroupMetadataV2`]).
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub enum NodeMetadataV2 {
    /// Array metadata.
    Array(ArrayMetadataV2),
    /// Group metadata.
    Group(GroupMetadataV2),
}

#[cfg(test)]
mod tests {
    use array::data_type_metadata_v2_to_endianness;

    use super::*;

    use crate::{
        v2_to_v3::{array_metadata_v2_to_v3, data_type_metadata_v2_to_v3_data_type},
        v3::array::codec::{
            blosc::{self, BloscCodecConfigurationV1},
            transpose::{self, TransposeCodecConfigurationV1},
        },
        ChunkKeySeparator, ChunkShape, Endianness,
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
        let array_metadata_v2: crate::v2::ArrayMetadataV2 = serde_json::from_str(&json).unwrap();
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
            data_type_metadata_v2_to_v3_data_type(&array_metadata_v2.dtype)?.name(),
            "float64"
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
