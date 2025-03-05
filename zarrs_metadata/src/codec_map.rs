use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
};

/// The codec mapping.
pub type CodecMap = HashMap<Cow<'static, str>, CodecName>;

/// The name and compatible aliases of a codec.
#[derive(Debug)]
pub struct CodecName {
    /// The codec name that will be serialised in array metadata.
    pub name: Cow<'static, str>,
    /// Aliases for the codec.
    pub aliases: HashSet<Cow<'static, str>>,
    /// Zarr V2 aliases for the codec.
    pub aliases_v2: HashSet<Cow<'static, str>>,
}

impl CodecName {
    /// The codec name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Zarr V3 aliases for the codec.
    #[must_use]
    pub fn aliases(&self) -> &HashSet<Cow<'static, str>> {
        &self.aliases
    }

    /// Zarr V2 aliases for the codec.
    #[must_use]
    pub fn aliases_v2(&self) -> &HashSet<Cow<'static, str>> {
        &self.aliases_v2
    }

    /// Check if a `name` matches the codec name or any of its aliases.
    #[must_use]
    pub fn contains(&self, name: &str) -> bool {
        name == self.name || self.aliases.contains(name)
    }

    /// Check if a `name` matches the codec name or any of its Zarr V2 aliases.
    #[must_use]
    pub fn contains_v2(&self, name: &str) -> bool {
        name == self.name || self.aliases_v2.contains(name)
    }
}
