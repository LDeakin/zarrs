use derive_more::{Display, From};
use serde::{Deserialize, Serialize};

use crate::v3::AdditionalFields;

/// Zarr V2 group metadata.
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug, Display, From)]
#[display("{}", serde_json::to_string(self).unwrap_or_default())]
pub struct GroupMetadataV2 {
    /// An integer defining the version of the storage specification to which the group adheres. Must be `2`.
    pub zarr_format: monostate::MustBe!(2u64),
    /// Optional user metadata.
    #[serde(default, flatten)]
    pub attributes: serde_json::Map<String, serde_json::Value>,
    /// Additional fields.
    ///
    /// These are not part of Zarr V2, but are retained for compatibility/flexibility.
    #[serde(default, flatten)]
    pub additional_fields: AdditionalFields,
}

impl Default for GroupMetadataV2 {
    fn default() -> Self {
        Self::new()
    }
}

impl GroupMetadataV2 {
    /// Create Zarr V2 group metadata.
    #[must_use]
    pub fn new() -> Self {
        Self {
            zarr_format: monostate::MustBe!(2u64),
            attributes: serde_json::Map::new(),
            additional_fields: AdditionalFields::default(),
        }
    }

    /// Serialize the metadata as a pretty-printed String of JSON.
    #[allow(clippy::missing_panics_doc)]
    #[must_use]
    pub fn to_string_pretty(&self) -> String {
        serde_json::to_string_pretty(self).expect("array metadata is valid JSON")
    }

    /// Set the user attributes.
    #[must_use]
    pub fn with_attributes(
        mut self,
        attributes: serde_json::Map<String, serde_json::Value>,
    ) -> Self {
        self.attributes = attributes;
        self
    }

    /// Set the additional fields.
    #[must_use]
    pub fn with_additional_fields(mut self, additional_fields: AdditionalFields) -> Self {
        self.additional_fields = additional_fields;
        self
    }
}
