//! Zarr array metadata.
//!
//! See <https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html#array-metadata>.

use derive_more::{Display, From};
use serde::{Deserialize, Serialize};

/// A wrapper to handle various versions of Zarr array metadata.
#[derive(Deserialize, Serialize, Clone, PartialEq, Debug, Display, From)]
#[serde(untagged)]
pub enum ArrayMetadata {
    /// Zarr Version 3.0.
    V3(super::v3::ArrayMetadataV3),
}

impl TryFrom<&str> for ArrayMetadata {
    type Error = serde_json::Error;
    fn try_from(metadata_json: &str) -> Result<Self, Self::Error> {
        serde_json::from_str::<Self>(metadata_json)
    }
}
