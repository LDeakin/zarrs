use std::collections::HashMap;

use crate::{ExtensionAliases, ExtensionTypeStorageTransformer, ZarrVersion3};

/// Aliases for Zarr V3 *storage transformer* extensions.
pub type ExtensionAliasesStorageTransformerV3 =
    ExtensionAliases<ZarrVersion3, ExtensionTypeStorageTransformer>;

impl Default for ExtensionAliasesStorageTransformerV3 {
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
