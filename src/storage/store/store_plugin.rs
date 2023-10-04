//! Store plugin support.

use thiserror::Error;

/// A store plugin.
pub struct StorePlugin<T> {
    /// the uri scheme of the plugin.
    scheme: &'static str,
    /// Creates the store from its url.
    create_fn: fn(uri: &str) -> Result<T, StorePluginCreateError>,
}

/// A store plugin creation error.
#[derive(Error, Debug)]
#[allow(missing_docs)]
pub enum StorePluginCreateError {
    /// Error parsing URI.
    #[error(transparent)]
    ParseError(#[from] url::ParseError),
    /// Unsupported URI scheme.
    #[error("unsupported uri scheme: {0}")]
    UnsupportedScheme(String),
    /// Other error.
    #[error("{0}")]
    Other(String),
}

impl From<&str> for StorePluginCreateError {
    fn from(err: &str) -> Self {
        Self::Other(err.to_string())
    }
}

impl From<String> for StorePluginCreateError {
    fn from(err: String) -> Self {
        Self::Other(err)
    }
}

impl<T> StorePlugin<T> {
    /// Create a new plugin.
    pub const fn new(
        scheme: &'static str,
        create_fn: fn(uri: &str) -> Result<T, StorePluginCreateError>,
    ) -> Self {
        Self { scheme, create_fn }
    }

    /// Create the plugin.
    ///
    /// # Errors
    ///
    /// Returns a [`StorePluginCreateError`] if plugin creation fails due to either:
    ///  - metadata name being unregistered,
    ///  - or the configuration is invalid.
    pub fn create(&self, url: &str) -> Result<T, StorePluginCreateError> {
        (self.create_fn)(url)
    }

    /// Returns the uri scheme of the plugin.
    #[must_use]
    pub fn uri_scheme(&self) -> &str {
        self.scheme
    }
}
