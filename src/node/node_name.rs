use thiserror::Error;

use crate::storage::store::StorePrefix;

/// A Zarr hierarchy node name.
///
/// See
/// - <https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html#name>, and
/// - <https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html#node-names>.
#[derive(Debug, PartialEq, Eq)]
pub struct NodeName(String);

/// An invalid node name.
#[derive(Debug, Error)]
#[error("invalid node name {0}")]
pub struct NodeNameError(String);

impl NodeName {
    /// Create a new Zarr node name from `name`.
    ///
    /// # Errors
    ///
    /// Returns [`NodeNameError`] if `name` is not valid according to [`NodeName::validate`()].
    pub fn new(name: &str) -> Result<Self, NodeNameError> {
        if Self::validate(name) {
            Ok(Self(name.to_string()))
        } else {
            Err(NodeNameError(name.to_string()))
        }
    }

    /// Create a new Zarr node name from `name`.
    ///
    /// # Safety
    ///
    /// `name` is not validated, so this can result in an invalid node name.
    #[must_use]
    pub unsafe fn new_unchecked(name: &str) -> Self {
        Self(name.to_string())
    }

    /// The root node.
    #[must_use]
    pub const fn root() -> Self {
        Self(String::new())
    }

    /// Extracts a string slice containing the node name `String`.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Validates a node name according to the following rules from the specification:
    /// - The root node does not have a name and is the empty string "". Otherwise,
    /// - must not be the empty string (""),
    /// - must not include the character "/",
    /// - must not be a string composed only of period characters, e.g. "." or "..", and
    /// - must not start with the reserved prefix "__".
    #[must_use]
    pub fn validate(node_name: &str) -> bool {
        node_name.is_empty()
            || (!node_name.contains('/')
                && !node_name.starts_with("__")
                && !node_name.replace('.', "").is_empty())
    }

    /// Indicates if a node has the root node name ("").
    #[must_use]
    pub fn is_root(&self) -> bool {
        self.0.is_empty()
    }
}

impl From<&StorePrefix> for NodeName {
    fn from(prefix: &StorePrefix) -> Self {
        let name = prefix
            .as_str()
            .strip_suffix('/')
            .expect("a store prefix must end with /")
            .split('/')
            .last()
            .expect("an empty string to split returns a single \"\" element")
            .to_string();
        Self(name)
    }
}
