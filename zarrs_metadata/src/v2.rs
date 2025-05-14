/// Zarr V2 group metadata.
pub mod group;

/// Zarr V2 array metadata.
pub mod array;

pub use array::ArrayMetadataV2;
pub use group::GroupMetadataV2;

mod metadata;
pub use metadata::MetadataV2;

/// Zarr V2 node metadata ([`ArrayMetadataV2`] or [`GroupMetadataV2`]).
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
#[allow(clippy::large_enum_variant)]
pub enum NodeMetadataV2 {
    /// Array metadata.
    Array(ArrayMetadataV2),
    /// Group metadata.
    Group(GroupMetadataV2),
}
