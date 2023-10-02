use derive_more::Display;
use std::path::Path;
use thiserror::Error;

use crate::node::NodePath;

/// A Zarr abstract store prefix.
///
/// See <https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html#abstract-store-interface>.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Display)]
pub struct StorePrefix(String);

/// An invalid store prefix.
#[derive(Debug, Error)]
#[error("invalid store prefix {0}")]
pub struct StorePrefixError(String);

impl StorePrefixError {
    /// Create a new invalid store prefix.
    #[must_use]
    pub fn new(prefix: String) -> Self {
        StorePrefixError(prefix)
    }
}

/// A list of [`StorePrefix`].
pub type StorePrefixes = Vec<StorePrefix>;

impl StorePrefix {
    /// Create a new Zarr Prefix from `prefix`.
    ///
    /// # Errors
    ///
    /// Returns [`StorePrefixError`] if `prefix` is not valid according to [`StorePrefix::validate`()].
    pub fn new(prefix: &str) -> Result<StorePrefix, StorePrefixError> {
        if Self::validate(prefix) {
            Ok(StorePrefix(prefix.to_string()))
        } else {
            Err(StorePrefixError(prefix.to_string()))
        }
    }

    /// Create a new Zarr Prefix from `prefix`.
    ///
    /// # Safety
    ///
    /// `prefix` is not validated, so this can result in an invalid store prefix.
    #[must_use]
    pub unsafe fn new_unchecked(prefix: &str) -> StorePrefix {
        debug_assert!(Self::validate(prefix));
        StorePrefix(prefix.to_string())
    }

    /// The root prefix.
    #[must_use]
    pub fn root() -> StorePrefix {
        StorePrefix(String::new())
    }

    /// Extracts a string slice containing the Prefix `String`.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Validates a prefix according to the following rules from the specification:
    /// - a prefix is a string containing only characters that are valid for use in keys,
    /// - and ending with a trailing / character.
    #[must_use]
    pub fn validate(prefix: &str) -> bool {
        prefix.is_empty() || (prefix.ends_with('/') && !prefix.starts_with('/'))
    }

    /// Returns the prefix of the parent, it if has one.
    #[must_use]
    pub fn parent(&self) -> Option<StorePrefix> {
        Path::new(&self.0).parent().map(|parent| {
            let parent = parent.to_str().unwrap_or_default();
            if parent.is_empty() {
                unsafe { StorePrefix::new_unchecked("") }
            } else {
                unsafe { StorePrefix::new_unchecked(&(parent.to_string() + "/")) }
            }
        })
    }
}

impl TryFrom<&str> for StorePrefix {
    type Error = StorePrefixError;

    fn try_from(prefix: &str) -> Result<StorePrefix, StorePrefixError> {
        StorePrefix::new(prefix)
    }
}

impl TryFrom<&NodePath> for StorePrefix {
    type Error = StorePrefixError;

    fn try_from(path: &NodePath) -> Result<StorePrefix, StorePrefixError> {
        let path = path.as_str();
        if path.eq("/") {
            StorePrefix::new("")
        } else {
            StorePrefix::new(&(path.strip_prefix('/').unwrap_or(path).to_string() + "/"))
        }
    }
}
