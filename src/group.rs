//! Zarr groups.
//!
//! A Zarr group is a node in a Zarr hierarchy.
//! It can have associated metadata and may have child nodes (groups or [`arrays`](crate::array)).
//! See <https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html#group>.
//!
//! Use [`GroupBuilder`] to setup a new group, or use [`Group::new`] to read and/or write an existing group.
//!
//! A group can optionally store attributes in metadata in an accompanying `zarr.json` file. For example:
//! ```json
//! {
//!     "zarr_format": 3,
//!     "node_type": "group",
//!     "attributes": {
//!         "spam": "ham",
//!         "eggs": 42,
//!     }
//! }
//! ```
//! See <https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html#group-metadata> for more information on group metadata.

mod group_builder;
mod group_metadata;

use std::sync::Arc;

use derive_more::Display;
use thiserror::Error;

use crate::{
    metadata::{AdditionalFields, UnsupportedAdditionalFieldError},
    node::{NodePath, NodePathError},
    storage::{
        meta_key, ReadableStorageTraits, StorageError, StorageHandle, WritableStorageTraits,
    },
};

pub use self::{
    group_builder::GroupBuilder,
    group_metadata::{GroupMetadata, GroupMetadataV3},
};

/// A group.
#[derive(Clone, Debug, Display)]
#[display(
    fmt = "path {path} metadata {}",
    "serde_json::to_string(metadata).unwrap_or_default()"
)]
pub struct Group<TStorage: ?Sized> {
    /// The storage.
    #[allow(dead_code)]
    storage: Arc<TStorage>,
    /// The path of the group in the store.
    #[allow(dead_code)]
    path: NodePath,
    /// The metadata.
    metadata: GroupMetadataV3,
}

impl<TStorage: ?Sized> Group<TStorage> {
    /// Create a group in `storage` at `path` with `metadata`.
    /// This does **not** write to the store, use [`store_metadata`](Group<WritableStorageTraits>::store_metadata) to write `metadata` to `storage`.
    ///
    /// # Errors
    ///
    /// Returns [`GroupCreateError`] if any metadata is invalid.
    pub fn new_with_metadata(
        storage: Arc<TStorage>,
        path: &str,
        metadata: GroupMetadata,
    ) -> Result<Self, GroupCreateError> {
        let path = NodePath::new(path)?;
        let GroupMetadata::V3(metadata) = metadata;
        validate_group_metadata(&metadata)?;
        Ok(Self {
            storage,
            path,
            metadata,
        })
    }

    /// Get path.
    #[must_use]
    pub fn path(&self) -> &NodePath {
        &self.path
    }

    /// Get attributes.
    #[must_use]
    pub fn attributes(&self) -> &serde_json::Map<String, serde_json::Value> {
        &self.metadata.attributes
    }

    /// Get additional fields.
    #[must_use]
    pub fn additional_fields(&self) -> &AdditionalFields {
        &self.metadata.additional_fields
    }

    /// Get metadata.
    #[must_use]
    pub fn metadata(&self) -> GroupMetadata {
        self.metadata.clone().into()
    }

    /// Mutably borrow the group attributes.
    #[must_use]
    pub fn attributes_mut(&mut self) -> &mut serde_json::Map<String, serde_json::Value> {
        &mut self.metadata.attributes
    }

    /// Mutably borrow the additional fields.
    #[must_use]
    pub fn additional_fields_mut(&mut self) -> &mut AdditionalFields {
        &mut self.metadata.additional_fields
    }
}

impl<TStorage: ?Sized + ReadableStorageTraits> Group<TStorage> {
    /// Create a group in `storage` at `path`. The metadata is read from the store.
    ///
    /// # Errors
    ///
    /// Returns [`GroupCreateError`] if there is a storage error or any metadata is invalid.
    pub fn new(storage: Arc<TStorage>, path: &str) -> Result<Self, GroupCreateError> {
        let node_path = path.try_into()?;
        let metadata: GroupMetadata = match storage.get(&meta_key(&node_path))? {
            Some(metadata) => serde_json::from_slice(&metadata)?,
            None => GroupMetadataV3::default().into(),
        };
        Self::new_with_metadata(storage, path, metadata)
    }
}

/// A group creation error.
#[derive(Debug, Error)]
pub enum GroupCreateError {
    /// Invalid zarr format.
    #[error("invalid zarr format {0}, expected 3")]
    InvalidZarrFormat(usize),
    /// Invalid node type.
    #[error("invalid zarr format {0}, expected group")]
    InvalidNodeType(String),
    /// An invalid node path
    #[error(transparent)]
    NodePathError(#[from] NodePathError),
    /// Unsupported additional field.
    #[error(transparent)]
    UnsupportedAdditionalFieldError(UnsupportedAdditionalFieldError),
    /// Storage error.
    #[error(transparent)]
    StorageError(#[from] StorageError),
    /// An error deserializing the metadata.
    #[error(transparent)]
    MetadataDeserializationError(#[from] serde_json::Error),
    /// An error parsing the metadata.
    #[error("{0}")]
    Metadata(String),
}

fn validate_group_metadata(metadata: &GroupMetadataV3) -> Result<(), GroupCreateError> {
    if !metadata.validate_format() {
        Err(GroupCreateError::InvalidZarrFormat(metadata.zarr_format))
    } else if !metadata.validate_node_type() {
        Err(GroupCreateError::InvalidNodeType(
            metadata.node_type.clone(),
        ))
    } else {
        metadata
            .additional_fields
            .validate()
            .map_err(GroupCreateError::UnsupportedAdditionalFieldError)
    }
}

impl<TStorage: ?Sized + ReadableStorageTraits> Group<TStorage> {}

impl<TStorage: ?Sized + WritableStorageTraits> Group<TStorage> {
    /// Store metadata.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] if there is an underlying store error.
    pub fn store_metadata(&self) -> Result<(), StorageError> {
        let storage_handle = StorageHandle::new(&*self.storage);
        crate::storage::create_group(&storage_handle, self.path(), &self.metadata())
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::store::MemoryStore;

    use super::*;

    const JSON_VALID1: &'static str = r#"{
    "zarr_format": 3,
    "node_type": "group",
    "attributes": {
        "spam": "ham",
        "eggs": 42
    }
}"#;

    const JSON_VALID2: &'static str = r#"{
    "zarr_format": 3,
    "node_type": "group",
    "attributes": {
        "spam": "ham",
        "eggs": 42
    },
    "unknown": {
        "must_understand": false
    }
}"#;

    const JSON_INVALID_ADDITIONAL_FIELD: &'static str = r#"{
    "zarr_format": 3,
    "node_type": "group",
    "attributes": {
      "spam": "ham",
      "eggs": 42
    },
    "unknown": "fail"
}"#;

    const JSON_INVALID_FORMAT: &'static str = r#"{
    "zarr_format": 2,
    "node_type": "group",
    "attributes": {
        "spam": "ham",
        "eggs": 42
    }
}"#;

    #[test]
    fn group_metadata1() {
        let group_metadata: GroupMetadata = serde_json::from_str(JSON_VALID1).unwrap();
        let store = MemoryStore::default();
        Group::new_with_metadata(store.into(), "/", group_metadata).unwrap();
    }

    #[test]
    fn group_metadata2() {
        let group_metadata: GroupMetadata = serde_json::from_str(JSON_VALID2).unwrap();
        let store = MemoryStore::default();
        Group::new_with_metadata(store.into(), "/", group_metadata).unwrap();
    }

    #[test]
    fn group_metadata_invalid_format() {
        let group_metadata: GroupMetadata = serde_json::from_str(JSON_INVALID_FORMAT).unwrap();
        print!("{group_metadata:?}");
        let store = MemoryStore::default();
        let group_metadata = Group::new_with_metadata(store.into(), "/", group_metadata);
        assert!(group_metadata.is_err());
    }

    #[test]
    fn group_metadata_invalid_additional_field() {
        let group_metadata: GroupMetadata =
            serde_json::from_str(JSON_INVALID_ADDITIONAL_FIELD).unwrap();
        print!("{group_metadata:?}");
        let store = MemoryStore::default();
        let group_metadata = Group::new_with_metadata(store.into(), "/", group_metadata);
        assert!(group_metadata.is_err());
    }

    #[test]
    fn group_metadata_write_read() {
        let store = std::sync::Arc::new(MemoryStore::new());
        let group_path = "/group";
        let group = GroupBuilder::new()
            .build(store.clone(), group_path)
            .unwrap();
        group.store_metadata().unwrap();
        let metadata = Group::new(store.clone(), group_path).unwrap().metadata();
        assert_eq!(metadata, group.metadata());
    }
}
