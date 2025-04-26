//! The plugin API for the [`zarrs`](https://crates.io/crates/zarrs) Rust crate.
//!
//! A [`Plugin`] creates concrete implementations of [Zarr V3 extension points](https://zarr-specs.readthedocs.io/en/latest/v3/core/indexhtml#extension-points) from inputs.
//! Extension points include chunk grids, chunk key encodings, codecs, data types, and storage transformers.
//!
//! In `zarrs`, plugins are registered at compile time using the [`inventory`](https://docs.rs/inventory/latest/inventory/) crate.
//! At runtime, a name matching function is applied to identify which registered plugin is associated with the metadata.
//! If a match is found, the plugin is created from the metadata and other relevant inputs.

use thiserror::Error;

pub use zarrs_metadata::v3::{MetadataConfiguration, MetadataV3};

/// A plugin.
pub struct Plugin<TPlugin, TInputs> {
    /// the identifier of the plugin.
    identifier: &'static str,
    /// Tests if the name is a match for this plugin.
    match_name_fn: fn(name: &str) -> bool,
    /// Create an implementation of this plugin from metadata.
    create_fn: fn(inputs: &TInputs) -> Result<TPlugin, PluginCreateError>,
}

/// An unsupported plugin error
#[derive(Debug, Error)]
#[error("{plugin_type} {name} is not supported")]
pub struct PluginUnsupportedError {
    name: String,
    configuration: Option<MetadataConfiguration>,
    plugin_type: String,
}

impl PluginUnsupportedError {
    /// Create a new [`PluginUnsupportedError`].
    #[must_use]
    pub fn new(
        name: String,
        configuration: Option<MetadataConfiguration>,
        plugin_type: String,
    ) -> Self {
        Self {
            name,
            configuration,
            plugin_type,
        }
    }
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
    #[error(transparent)]
    Unsupported(#[from] PluginUnsupportedError),
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

impl<TPlugin, TInputs> Plugin<TPlugin, TInputs> {
    /// Create a new plugin for registration.
    pub const fn new(
        identifier: &'static str,
        match_name_fn: fn(name: &str) -> bool,
        create_fn: fn(inputs: &TInputs) -> Result<TPlugin, PluginCreateError>,
    ) -> Self {
        Self {
            identifier,
            match_name_fn,
            create_fn,
        }
    }

    /// Create a `TPlugin` plugin from `inputs`.
    ///
    /// # Errors
    ///
    /// Returns a [`PluginCreateError`] if plugin creation fails due to either:
    ///  - metadata name being unregistered,
    ///  - or the configuration is invalid, or
    ///  - some other reason specific to the plugin.
    pub fn create(&self, inputs: &TInputs) -> Result<TPlugin, PluginCreateError> {
        (self.create_fn)(inputs)
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

#[cfg(test)]
mod tests {
    use super::*;

    struct TestPlugin;

    fn is_test(name: &str) -> bool {
        name == "test"
    }

    fn create_test(input: &MetadataV3) -> Result<TestPlugin, PluginCreateError> {
        if input.name() == "test" {
            Ok(TestPlugin)
        } else {
            Err(PluginCreateError::from("invalid name".to_string()))
        }
    }

    #[test]
    fn plugin() {
        let plugin = Plugin::new("test", is_test, create_test);
        assert!(!plugin.match_name("fail"));
        assert!(plugin.match_name("test"));
        assert_eq!(plugin.identifier(), "test");
        assert!(plugin.create(&MetadataV3::new("test")).is_ok());
        assert!(plugin.create(&MetadataV3::new("fail")).is_err());
    }
}
