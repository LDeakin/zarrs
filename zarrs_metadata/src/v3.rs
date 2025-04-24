/// Zarr V3 group metadata.
pub mod group;

/// Zarr V3 array metadata.
pub mod array;

pub use array::ArrayMetadataV3;
pub use group::GroupMetadataV3;

mod metadata;
pub use metadata::{
    AdditionalField, AdditionalFields, ConfigurationInvalidError, MetadataConfiguration,
    MetadataConfigurationSerialize, MetadataV3, UnsupportedAdditionalFieldError,
};

/// Zarr V3 node metadata ([`ArrayMetadataV3`] or [`GroupMetadataV3`]).
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
#[allow(clippy::large_enum_variant)]
pub enum NodeMetadataV3 {
    /// Array metadata.
    Array(ArrayMetadataV3),
    /// Group metadata.
    Group(GroupMetadataV3),
}
