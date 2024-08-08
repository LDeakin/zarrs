use serde::{Deserialize, Serialize};

use derive_more::Display;

use crate::metadata::v3::ChunkKeySeparator;

/// The identifier for the `v2` chunk key encoding.
pub const IDENTIFIER: &str = "v2";

/// A `v2` chunk key encoding configuration.
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug, Display)]
#[serde(deny_unknown_fields)]
#[display("{}", serde_json::to_string(self).unwrap_or_default())]
pub struct V2ChunkKeyEncodingConfiguration {
    /// The chunk key separator.
    #[serde(default = "v2_separator")]
    pub separator: ChunkKeySeparator,
}

const fn v2_separator() -> ChunkKeySeparator {
    ChunkKeySeparator::Dot
}
