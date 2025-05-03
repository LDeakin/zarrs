use std::collections::HashMap;

use crate::{ExtensionAliases, ExtensionTypeChunkGrid, ZarrVersion3};

/// Aliases for Zarr V3 *chunk grid* extensions.
pub type ExtensionAliasesChunkGridV3 = ExtensionAliases<ZarrVersion3, ExtensionTypeChunkGrid>;

impl Default for ExtensionAliasesChunkGridV3 {
    #[rustfmt::skip]
    fn default() -> Self {
        Self::new(
            // The default serialised `name`s
            HashMap::from([]),
            // `name` aliases (string match)
            HashMap::from([]),
            // `name` aliases (regex match)
            Vec::from([])
        )
    }
}
