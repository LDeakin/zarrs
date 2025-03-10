use std::{borrow::Cow, collections::HashMap, marker::PhantomData};

use crate::{ExtensionType, ExtensionTypeCodec};

/// A mapping from unique extension identifiers to an extension `name`.
pub type ExtensionNameMap = HashMap<&'static str, Cow<'static, str>>;

/// A mapping from extension `name` aliases to a unique extension identifier.
///
/// Extension identifiers are an implementation detail and may not match the extension `name`.
pub type ExtensionAliasMap = HashMap<Cow<'static, str>, &'static str>;

/// The maps that `zarrs` uses for serialising and deserialising extensions (e.g. codecs).
#[derive(Debug, Default)]
pub struct ExtensionMaps<T: ExtensionType> {
    extension_type: PhantomData<T>,
    /// The extension default `name` map.
    pub default_names: ExtensionNameMap,
    /// The extension `name` alias map for Zarr V3 data.
    pub aliases_v3: ExtensionAliasMap,
    /// The extension `name` alias map for Zarr V2 data.
    pub aliases_v2: ExtensionAliasMap,
}

impl<T: ExtensionType> ExtensionMaps<T> {
    /// Create a new [`ExtensionMaps`].
    #[must_use]
    pub fn new(
        default_names: ExtensionNameMap,
        aliases_v3: ExtensionAliasMap,
        aliases_v2: ExtensionAliasMap,
    ) -> Self {
        Self {
            extension_type: PhantomData,
            default_names,
            aliases_v3,
            aliases_v2,
        }
    }
}

/// Extension maps for the codec extension type.
pub type ExtensionMapsCodec = ExtensionMaps<ExtensionTypeCodec>;

// TODO: Data type extension maps
