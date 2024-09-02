/// Zarr V3 group metadata.
pub mod group;

/// Zarr V3 array metadata.
pub mod array;

pub use array::ArrayMetadataV3;
pub use group::GroupMetadataV3;

mod metadata;
pub use metadata::{
    AdditionalFields, ConfigurationInvalidError, MetadataV3, UnsupportedAdditionalFieldError,
};

/// V3 node metadata ([`ArrayMetadataV3`] or [`GroupMetadataV3`]).
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub enum NodeMetadataV3 {
    /// Array metadata.
    Array(ArrayMetadataV3),
    /// Group metadata.
    Group(GroupMetadataV3),
}
