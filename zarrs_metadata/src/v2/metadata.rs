use serde::{Deserialize, Serialize};

use crate::Configuration;

/// Metadata with an `id` and optional flattened `configuration`.
///
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
    configuration: Configuration,
}

impl MetadataV2 {
    /// Return the value of the `id` field.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Mutate the value of the `id` field.
    pub fn set_id(&mut self, id: String) -> &mut Self {
        self.id = id;
        self
    }

    /// Return the configuration, which includes all fields excluding the `id`.
    #[must_use]
    pub fn configuration(&self) -> &Configuration {
        &self.configuration
    }
}
