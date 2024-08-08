use derive_more::{Display, From};
use serde::{Deserialize, Serialize};

use crate::metadata::AdditionalFields;

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
