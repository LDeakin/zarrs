use serde::{Deserialize, Serialize};

use derive_more::Display;

use crate::ChunkKeySeparator;

/// A `default` chunk key encoding configuration.
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug, Display)]
#[serde(deny_unknown_fields)]
#[display("{}", serde_json::to_string(self).unwrap_or_default())]
pub struct DefaultChunkKeyEncodingConfiguration {
    /// The chunk key separator.
    #[serde(default = "default_separator")]
    pub separator: ChunkKeySeparator,
}

const fn default_separator() -> ChunkKeySeparator {
    ChunkKeySeparator::Slash
}
