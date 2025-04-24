use std::collections::HashMap;

use derive_more::Display;
use serde::{Deserialize, Serialize};

use crate::NodeMetadata;

use super::{AdditionalFields, MetadataV3};

/// Zarr group metadata (storage specification v3).
///
/// See <https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html#group-metadata>.
///
/// An example `JSON` document for an explicit Zarr V3 group:
/// ```json
/// {
///     "zarr_format": 3,
///     "node_type": "group",
///     "attributes": {
///         "spam": "ham",
///         "eggs": 42,
///     }
/// }
#[non_exhaustive]
#[derive(Serialize, Deserialize, Clone, Debug, Display)]
#[display("{}", serde_json::to_string(self).unwrap_or_default())]
pub struct GroupMetadataV3 {
    /// An integer defining the version of the storage specification to which the group adheres. Must be `3`.
    pub zarr_format: monostate::MustBe!(3u64),
    /// A string defining the type of hierarchy node element, must be `group` here.
    pub node_type: monostate::MustBe!("group"),
    /// Optional user metadata.
    #[serde(default, skip_serializing_if = "serde_json::Map::is_empty")]
    pub attributes: serde_json::Map<String, serde_json::Value>,
    /// Extension definitions (Zarr 3.1, [ZEP0009](https://zarr.dev/zeps/draft/ZEP0009.html)).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub extensions: Vec<MetadataV3>,
    /// Consolidated metadata.
    // FIXME: Deprecate in the next breaking release (but support deserialisation to `extensions`)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consolidated_metadata: Option<ConsolidatedMetadata>,
    /// Additional fields.
    #[serde(flatten)]
    pub additional_fields: AdditionalFields,
}

impl std::cmp::PartialEq for GroupMetadataV3 {
    fn eq(&self, other: &Self) -> bool {
        self.attributes == other.attributes
            // && self.consolidated_metadata == other.consolidated_metadata
            && self.additional_fields == other.additional_fields
    }
}

impl Eq for GroupMetadataV3 {}

impl Default for GroupMetadataV3 {
    fn default() -> Self {
        Self::new()
    }
}

impl GroupMetadataV3 {
    /// Create Zarr V3 group metadata.
    #[must_use]
    pub fn new() -> Self {
        Self {
            zarr_format: monostate::MustBe!(3u64),
            node_type: monostate::MustBe!("group"),
            attributes: serde_json::Map::new(),
            extensions: Vec::default(),
            additional_fields: AdditionalFields::default(),
            consolidated_metadata: None,
        }
    }

    /// Serialize the metadata as a pretty-printed String of JSON.
    #[allow(clippy::missing_panics_doc)]
    #[must_use]
    pub fn to_string_pretty(&self) -> String {
        serde_json::to_string_pretty(self).expect("group metadata is valid JSON")
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

    /// Set the extension definitions.
    #[must_use]
    pub fn with_extensions(mut self, extensions: Vec<MetadataV3>) -> Self {
        self.extensions = extensions;
        self
    }

    /// Set the consolidated metadata.
    #[must_use]
    pub fn with_consolidated_metadata(
        mut self,
        consolidated_metadata: Option<ConsolidatedMetadata>,
    ) -> Self {
        self.consolidated_metadata = consolidated_metadata;
        self
    }
}

/// Consolidated metadata of a Zarr hierarchy.
#[derive(Serialize, Deserialize, Clone, PartialEq, Debug, Display)]
#[display("{}", serde_json::to_string(self).unwrap_or_default())]
pub struct ConsolidatedMetadata {
    /// A mapping from node path to Group or Array [`NodeMetadata`] object.
    pub metadata: ConsolidatedMetadataMetadata,
    /// The kind of the consolidated metadata. Must be `'inline'`. Reserved for future use.
    pub kind: ConsolidatedMetadataKind,
    /// The boolean literal `false`. Indicates that the field is not required to load the Zarr hierarchy.
    pub must_understand: monostate::MustBe!(false),
}

/// The `metadata` field of `consolidated_metadata` in [`GroupMetadataV3`].
pub type ConsolidatedMetadataMetadata = HashMap<String, NodeMetadata>;

impl Default for ConsolidatedMetadata {
    fn default() -> Self {
        Self {
            metadata: HashMap::default(),
            kind: ConsolidatedMetadataKind::Inline,
            must_understand: monostate::MustBe!(false),
        }
    }
}

/// The "kind" of consolidated metadata.
#[non_exhaustive]
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug, Display)]
pub enum ConsolidatedMetadataKind {
    /// Indicates that consolidated metadata is stored inline in the root `zarr.json` object.
    #[serde(rename = "inline")]
    Inline,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn group_metadata_consolidated() {
        let group_metadata = serde_json::from_str::<GroupMetadataV3>(
            r#"{
            "zarr_format": 3,
            "node_type": "group",
            "attributes": {
                "spam": "ham",
                "eggs": 42
            },
            "consolidated_metadata": {
                "metadata": {
                    "/subgroup": {
                        "zarr_format": 3,
                        "node_type": "group",
                        "attributes": {
                            "consolidated": "attributes"
                        }
                    }
                },
                "kind": "inline",
                "must_understand": false
            }
        }"#,
        )
        .unwrap();
        assert_eq!(
            group_metadata
                .consolidated_metadata
                .unwrap()
                .metadata
                .get("/subgroup")
                .unwrap(),
            &serde_json::from_str::<NodeMetadata>(
                r#"{
                    "zarr_format": 3,
                    "node_type": "group",
                    "attributes": {
                        "consolidated": "attributes"
                    }
                }"#
            )
            .unwrap()
        );
    }
}
