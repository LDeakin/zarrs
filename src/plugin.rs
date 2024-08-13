//! [Zarr V3 extension points](https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html#extension-points) utilities.
//!
//! A [`Plugin`] creates objects from [`MetadataV3`] (consisting of a name and optional configuration).
//! It is used to implement [Zarr extension points](https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html#extension-points), such as [chunk grids][`crate::array::chunk_grid`], [chunk key encodings](`crate::array::chunk_key_encoding`), [codecs](`crate::array::codec`), and [storage transformers](`crate::storage::storage_transformer`).
//!
//! [Data types](`crate::array::data_type`) are not currently supported as an extension point.
//!
//! Plugins are registered at compile time using the [inventory] crate.
//! At runtime, a name matching function is applied to identify which registered plugin is associated with the metadata.
//! If a match is found, the plugin is created from the metadata.

use thiserror::Error;

use crate::metadata::v3::MetadataV3;

/// A plugin.
pub struct Plugin<TPlugin> {
    /// the identifier of the plugin.
    identifier: &'static str,
    /// Tests if the name is a match for this plugin.
    match_name_fn: fn(name: &str) -> bool,
    /// Create an implementation of this plugin from metadata.
    create_fn: fn(metadata: &MetadataV3) -> Result<TPlugin, PluginCreateError>,
}

/// An invalid plugin metadata error.
#[derive(Debug, Error)]
#[error("{plugin_type} {identifier} is unsupported with metadata: {metadata}")]
pub struct PluginMetadataInvalidError {
    identifier: &'static str,
    plugin_type: &'static str,
    metadata: Box<MetadataV3>,
}

impl PluginMetadataInvalidError {
    /// Create a new [`PluginMetadataInvalidError`].
    #[must_use]
    pub fn new(identifier: &'static str, plugin_type: &'static str, metadata: MetadataV3) -> Self {
        Self {
            identifier,
            plugin_type,
            metadata: Box::new(metadata),
        }
    }
}

/// A plugin creation error.
#[derive(Error, Debug)]
#[allow(missing_docs)]
pub enum PluginCreateError {
    /// An unsupported plugin.
    #[error("{plugin_type} {name} is not supported")]
    Unsupported { name: String, plugin_type: String },
    /// Invalid metadata.
    #[error(transparent)]
    MetadataInvalid(#[from] PluginMetadataInvalidError),
    /// Other
    #[error("{_0}")]
    Other(String),
}

impl From<&str> for PluginCreateError {
    fn from(err_string: &str) -> Self {
        Self::Other(err_string.to_string())
    }
}

impl From<String> for PluginCreateError {
    fn from(err_string: String) -> Self {
        Self::Other(err_string)
    }
}

impl<TPlugin> Plugin<TPlugin> {
    /// Create a new plugin for registration.
    pub const fn new(
        identifier: &'static str,
        match_name_fn: fn(name: &str) -> bool,
        create_fn: fn(metadata: &MetadataV3) -> Result<TPlugin, PluginCreateError>,
    ) -> Self {
        Self {
            identifier,
            match_name_fn,
            create_fn,
        }
    }

    /// Create a `TPlugin` plugin from `metadata`.
    ///
    /// # Errors
    ///
    /// Returns a [`PluginCreateError`] if plugin creation fails due to either:
    ///  - metadata name being unregistered,
    ///  - or the configuration is invalid.
    pub fn create(&self, metadata: &MetadataV3) -> Result<TPlugin, PluginCreateError> {
        (self.create_fn)(metadata)
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
