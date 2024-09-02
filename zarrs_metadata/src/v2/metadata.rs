use serde::{Deserialize, Serialize};

/// Metadata with a id and optional configuration.
///
/// Can be deserialised from a JSON string or name/configuration map.
/// For example:
/// ```json
/// {
///     "id": "blosc",
///     "cname": "lz4",
///     "clevel": 5,
///     "shuffle": 1
/// }
/// ```
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug)]
pub struct MetadataV2 {
    id: String,
    #[serde(flatten)]
    configuration: serde_json::Map<String, serde_json::Value>,
}

impl MetadataV2 {
    /// Return the "id" key.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Return the configuration, which includes all fields excluding the "id".
    #[must_use]
    pub fn configuration(&self) -> &serde_json::Map<String, serde_json::Value> {
        &self.configuration
    }
}
