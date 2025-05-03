use std::collections::HashMap;

use crate::{ExtensionAliases, ExtensionTypeChunkKeyEncoding, ZarrVersion3};

/// Aliases for Zarr V3 *chunk key encoding* extensions.
pub type ExtensionAliasesChunkKeyEncodingV3 =
    ExtensionAliases<ZarrVersion3, ExtensionTypeChunkKeyEncoding>;

impl Default for ExtensionAliasesChunkKeyEncodingV3 {
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
