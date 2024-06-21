//! Zarr group metadata.
//!
//! See <https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html#group-metadata>.

use derive_more::{Display, From};
use serde::{Deserialize, Serialize};

/// A wrapper to handle various versions of Zarr group metadata.
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug, Display, From)]
#[serde(untagged)]
pub enum GroupMetadata {
    /// Zarr Version 3.0.
    V3(super::v3::GroupMetadataV3),
}

impl TryFrom<&str> for GroupMetadata {
    type Error = serde_json::Error;
    fn try_from(metadata_json: &str) -> Result<Self, Self::Error> {
        serde_json::from_str::<Self>(metadata_json)
    }
}