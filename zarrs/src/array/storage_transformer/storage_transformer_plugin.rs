use crate::{metadata::v3::MetadataV3, node::NodePath, plugin::PluginCreateError};

use super::StorageTransformer;

/// A storage transformer plugin.
pub struct StorageTransformerPlugin {
    /// the identifier of the plugin.
    identifier: &'static str,
    /// Tests if the name is a match for this plugin.
    match_name_fn: fn(name: &str) -> bool,
    /// Create an implementation of this plugin from metadata.
    create_fn:
        fn(metadata: &MetadataV3, path: &NodePath) -> Result<StorageTransformer, PluginCreateError>,
}
inventory::collect!(StorageTransformerPlugin);

impl StorageTransformerPlugin {
    /// Create a new plugin for registration.
    pub const fn new(
        identifier: &'static str,
        match_name_fn: fn(name: &str) -> bool,
        create_fn: fn(
            metadata: &MetadataV3,
            path: &NodePath,
        ) -> Result<StorageTransformer, PluginCreateError>,
    ) -> Self {
        Self {
            identifier,
            match_name_fn,
            create_fn,
        }
    }

    /// Create a storage transformer plugin from `metadata` relative to `path`.
    ///
    /// # Errors
    ///
    /// Returns a [`PluginCreateError`] if plugin creation fails due to either:
    ///  - metadata name being unregistered,
    ///  - or the configuration is invalid.
    pub fn create(
        &self,
        metadata: &MetadataV3,
        path: &NodePath,
    ) -> Result<StorageTransformer, PluginCreateError> {
        (self.create_fn)(metadata, path)
    }

    /// Returns true if this plugin is associated with `name`.
    #[must_use]
    pub fn match_name(&self, name: &str) -> bool {
        (self.match_name_fn)(name)
    }

    /// Returns the identifier of the plugin.
    #[must_use]
    pub const fn identifier(&self) -> &'static str {
        self.identifier
    }
}
