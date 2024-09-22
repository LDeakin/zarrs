use std::path::PathBuf;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug)]
#[serde(deny_unknown_fields)]
pub struct ChunkManifestJsonConfiguration {
    /// The manifest path.
    pub manifest: PathBuf,
}
