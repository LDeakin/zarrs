//! Plugin utilities for supporting [Zarr extension points](https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html#extension-points).
//!
//! A [`Plugin`] creates objects from [`Metadata`] (consisting of a name and optional configuration).
//! It is used to implement [Zarr extension points](https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html#extension-points), such as [chunk grids][`crate::array::chunk_grid`], [chunk key encodings](`crate::array::chunk_key_encoding`), [codecs](`crate::array::codec`), and [storage transformers](`crate::storage::storage_transformer`).
//!
//! [Data types](`crate::array::data_type`) are not currently supported as an extension point.
//!
//! Plugins are registered at compile time using the [inventory] crate.
//! At runtime, a name matching function is applied to identify which registered plugin is associated with the metadata.
//! If a match is found, the plugin is created from the metadata.

use thiserror::Error;

use crate::metadata::{ConfigurationInvalidError, Metadata};

/// A plugin.

pub struct Plugin<TPlugin> {
    /// the identifier of the plugin.
    identifier: &'static str,
    /// Tests if the name is a match for this plugin.
    match_name_fn: fn(name: &str) -> bool,
    /// Create an implementation of this plugin from metadata.
    create_fn: fn(metadata: &Metadata) -> Result<TPlugin, PluginCreateError>,
}

/// A plugin creation error.
#[derive(Error, Debug)]
#[allow(missing_docs)]
pub enum PluginCreateError {
    /// An unsupported plugin.
    #[error("{name:?} is not supported")]
    Unsupported { name: String },
    /// Invalid metadata.
    #[error("{identifier} is unsupported, metadata: {metadata:?}")]
    MetadataInvalid {
        identifier: &'static str,
        metadata: Metadata,
    },
    /// Invalid configuration
    #[error(transparent)]
    ConfigurationInvalidError(#[from] ConfigurationInvalidError),
    /// Other
    #[error("{error_str}")]
    Other { error_str: String },
}

impl<TPlugin> Plugin<TPlugin> {
    /// Create a new plugin for registration.
    pub const fn new(
        identifier: &'static str,
        match_name_fn: fn(name: &str) -> bool,
        create_fn: fn(metadata: &Metadata) -> Result<TPlugin, PluginCreateError>,
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
    pub fn create(&self, metadata: &Metadata) -> Result<TPlugin, PluginCreateError> {
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
