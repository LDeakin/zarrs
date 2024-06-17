//! Zarr array metadata.
//!
//! See <https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html#array-metadata>.

mod v3;

pub use v3::ArrayMetadataV3;

use derive_more::{Display, From};
use serde::{Deserialize, Serialize};

use super::{ArrayShape, DimensionName};

/// Zarr array metadata.
#[derive(Serialize, Deserialize, Clone, PartialEq, Debug, Display, From)]
#[serde(untagged)]
pub enum ArrayMetadata {
    /// Version 3.0.
    V3(ArrayMetadataV3),
}

impl TryFrom<&str> for ArrayMetadata {
    type Error = serde_json::Error;
    fn try_from(metadata_json: &str) -> Result<Self, Self::Error> {
        serde_json::from_str::<Self>(metadata_json)
    }
}
