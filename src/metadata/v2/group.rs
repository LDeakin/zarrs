use derive_more::{Display, From};
use serde::{Deserialize, Serialize};

use crate::metadata::AdditionalFields;

/// Zarr V2 group metadata.
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug, Display, From)]
#[display(fmt = "{}", "serde_json::to_string(self).unwrap_or_default()")]
pub struct GroupMetadataV2 {
    /// Optional user metadata.
    #[serde(default, flatten)]
    pub attributes: serde_json::Map<String, serde_json::Value>,
    /// Additional fields.
    ///
    /// These are not part of Zarr V2, but are retrained for compatibility/flexibility.
    #[serde(default, flatten)]
    pub additional_fields: AdditionalFields,
}
