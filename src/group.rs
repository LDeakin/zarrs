//! Zarr groups.
//!
//! A Zarr group is a node in a Zarr hierarchy.
//! It can have associated attributes and may have child nodes (groups or [`arrays`](crate::array)).
//! See <https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html#group>.
//!
//! Use [`GroupBuilder`] to setup a new group, or use [`Group::open`] to read and/or write an existing group.
//!
//! ## Group Metadata
//! Group metadata **must be explicitly stored** with [`store_metadata`](Group::store_metadata) or [`store_metadata_opt`](Group::store_metadata_opt) if a group is newly created or its metadata has been mutated.
//! Support for implicit groups was removed from Zarr V3 after provisional acceptance.
//!
//! Below is an example of a `zarr.json` file for a group:
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
mod group_metadata_options;

use std::sync::Arc;

use derive_more::Display;
use thiserror::Error;

use crate::{
    metadata::{
        group_metadata_v2_to_v3, v3::UnsupportedAdditionalFieldError, AdditionalFields,
        GroupMetadataV2, MetadataConvertVersion, MetadataEraseVersion, MetadataRetrieveVersion,
    },
    node::{NodePath, NodePathError},
    storage::{
        meta_key, meta_key_v2_attributes, meta_key_v2_group, ReadableStorageTraits, StorageError,
        StorageHandle, WritableStorageTraits,
    },
};

#[cfg(feature = "async")]
use crate::storage::{AsyncReadableStorageTraits, AsyncWritableStorageTraits};

pub use self::group_builder::GroupBuilder;
pub use crate::metadata::{v3::GroupMetadataV3, GroupMetadata};
pub use group_metadata_options::GroupMetadataOptions;

/// A group.
#[derive(Clone, Debug, Display)]
#[display(
    "group at {path} with metadata {}",
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
    metadata: GroupMetadata,
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
        Ok(Self {
            storage,
            path,
            metadata,
        })
    }

    /// Get path.
    #[must_use]
    pub const fn path(&self) -> &NodePath {
        &self.path
    }

    /// Get attributes.
    #[must_use]
    pub const fn attributes(&self) -> &serde_json::Map<String, serde_json::Value> {
        match &self.metadata {
            GroupMetadata::V3(metadata) => &metadata.attributes,
            GroupMetadata::V2(metadata) => &metadata.attributes,
        }
    }

    /// Mutably borrow the group attributes.
    #[must_use]
    pub fn attributes_mut(&mut self) -> &mut serde_json::Map<String, serde_json::Value> {
        match &mut self.metadata {
            GroupMetadata::V3(metadata) => &mut metadata.attributes,
            GroupMetadata::V2(metadata) => &mut metadata.attributes,
        }
    }

    /// Get additional fields.
    #[must_use]
    pub const fn additional_fields(&self) -> &AdditionalFields {
        match &self.metadata {
            GroupMetadata::V3(metadata) => &metadata.additional_fields,
            GroupMetadata::V2(metadata) => &metadata.additional_fields,
        }
    }

    /// Mutably borrow the additional fields.
    #[must_use]
    pub fn additional_fields_mut(&mut self) -> &mut AdditionalFields {
        match &mut self.metadata {
            GroupMetadata::V3(metadata) => &mut metadata.additional_fields,
            GroupMetadata::V2(metadata) => &mut metadata.additional_fields,
        }
    }
    /// Return the underlying group metadata.
    #[must_use]
    pub fn metadata(&self) -> &GroupMetadata {
        &self.metadata
    }

    /// Return a new [`GroupMetadata`] with [`GroupMetadataOptions`] applied.
    ///
    /// This method is used internally by [`Group::store_metadata`] and [`Group::store_metadata_opt`].
    #[must_use]
    pub fn metadata_opt(&self, options: &GroupMetadataOptions) -> GroupMetadata {
        use GroupMetadata as GM;
        use MetadataConvertVersion as V;
        let metadata = self.metadata.clone();

        match (metadata, options.metadata_convert_version()) {
            (GM::V3(metadata), V::Default | V::V3) => GM::V3(metadata),
            (GM::V2(metadata), V::Default) => GM::V2(metadata),
            (GM::V2(metadata), V::V3) => GM::V3(group_metadata_v2_to_v3(&metadata)),
        }
    }
}

impl<TStorage: ?Sized + ReadableStorageTraits> Group<TStorage> {
    /// Open a group in `storage` at `path` with [`MetadataRetrieveVersion`].
    /// The metadata is read from the store.
    ///
    /// # Errors
    /// Returns [`GroupCreateError`] if there is a storage error or any metadata is invalid.
    #[deprecated(since = "0.15.0", note = "please use `open` instead")]
    pub fn new(storage: Arc<TStorage>, path: &str) -> Result<Self, GroupCreateError> {
        Self::open(storage, path)
    }

    /// Open a group in `storage` at `path` with [`MetadataRetrieveVersion`].
    /// The metadata is read from the store.
    ///
    /// # Errors
    /// Returns [`GroupCreateError`] if there is a storage error or any metadata is invalid.
    pub fn open(storage: Arc<TStorage>, path: &str) -> Result<Self, GroupCreateError> {
        Self::open_opt(storage, path, &MetadataRetrieveVersion::Default)
    }

    /// Open a group in `storage` at `path` with non-default [`MetadataRetrieveVersion`].
    /// The metadata is read from the store.
    ///
    /// # Errors
    /// Returns [`GroupCreateError`] if there is a storage error or any metadata is invalid.
    pub fn open_opt(
        storage: Arc<TStorage>,
        path: &str,
        version: &MetadataRetrieveVersion,
    ) -> Result<Self, GroupCreateError> {
        let node_path = path.try_into()?;

        if let MetadataRetrieveVersion::Default | MetadataRetrieveVersion::V3 = version {
            // Try Zarr V3
            let key_v3 = meta_key(&node_path);
            if let Some(metadata) = storage.get(&key_v3)? {
                let metadata: GroupMetadataV3 = serde_json::from_slice(&metadata)
                    .map_err(|err| StorageError::InvalidMetadata(key_v3, err.to_string()))?;
                return Self::new_with_metadata(storage, path, GroupMetadata::V3(metadata));
            }
        }

        if let MetadataRetrieveVersion::Default | MetadataRetrieveVersion::V2 = version {
            // Try Zarr V2
            let key_v2 = meta_key_v2_group(&node_path);
            if let Some(metadata) = storage.get(&key_v2)? {
                let mut metadata: GroupMetadataV2 = serde_json::from_slice(&metadata)
                    .map_err(|err| StorageError::InvalidMetadata(key_v2, err.to_string()))?;
                let attributes_key = meta_key_v2_attributes(&node_path);
                let attributes = storage.get(&attributes_key)?;
                if let Some(attributes) = attributes {
                    metadata.attributes = serde_json::from_slice(&attributes).map_err(|err| {
                        StorageError::InvalidMetadata(attributes_key, err.to_string())
                    })?;
                }
                return Self::new_with_metadata(storage, path, GroupMetadata::V2(metadata));
            }
        }

        // No metadata has been found
        Err(GroupCreateError::MissingMetadata)
    }
}

#[cfg(feature = "async")]
impl<TStorage: ?Sized + AsyncReadableStorageTraits> Group<TStorage> {
    /// Async variant of [`new`](Group::open).
    #[deprecated(since = "0.15.0", note = "please use `async_open` instead")]
    #[allow(clippy::missing_errors_doc)]
    pub async fn async_new(storage: Arc<TStorage>, path: &str) -> Result<Self, GroupCreateError> {
        Self::async_open(storage, path).await
    }

    /// Async variant of [`open`](Group::open).
    #[allow(clippy::missing_errors_doc)]
    pub async fn async_open(storage: Arc<TStorage>, path: &str) -> Result<Self, GroupCreateError> {
        Self::async_open_opt(storage, path, &MetadataRetrieveVersion::Default).await
    }

    /// Async variant of [`open_opt`](Group::open_opt).
    #[allow(clippy::missing_errors_doc)]
    pub async fn async_open_opt(
        storage: Arc<TStorage>,
        path: &str,
        version: &MetadataRetrieveVersion,
    ) -> Result<Self, GroupCreateError> {
        let node_path = path.try_into()?;

        if let MetadataRetrieveVersion::Default | MetadataRetrieveVersion::V3 = version {
            // Try Zarr V3
            let key_v3 = meta_key(&node_path);
            if let Some(metadata) = storage.get(&key_v3).await? {
                let metadata: GroupMetadataV3 = serde_json::from_slice(&metadata)
                    .map_err(|err| StorageError::InvalidMetadata(key_v3, err.to_string()))?;
                return Self::new_with_metadata(storage, path, GroupMetadata::V3(metadata));
            }
        }

        if let MetadataRetrieveVersion::Default | MetadataRetrieveVersion::V2 = version {
            // Try Zarr V2
            let key_v2 = meta_key_v2_group(&node_path);
            if let Some(metadata) = storage.get(&key_v2).await? {
                let mut metadata: GroupMetadataV2 = serde_json::from_slice(&metadata)
                    .map_err(|err| StorageError::InvalidMetadata(key_v2, err.to_string()))?;
                let attributes_key = meta_key_v2_attributes(&node_path);
                let attributes = storage.get(&attributes_key).await?;
                if let Some(attributes) = attributes {
                    metadata.attributes = serde_json::from_slice(&attributes).map_err(|err| {
                        StorageError::InvalidMetadata(attributes_key, err.to_string())
                    })?;
                }
                return Self::new_with_metadata(storage, path, GroupMetadata::V2(metadata));
            }
        }

        // No metadata has been found
        Err(GroupCreateError::MissingMetadata)
    }
}

/// A group creation error.
#[derive(Debug, Error)]
pub enum GroupCreateError {
    /// An invalid node path
    #[error(transparent)]
    NodePathError(#[from] NodePathError),
    /// Unsupported additional field.
    #[error(transparent)]
    UnsupportedAdditionalFieldError(UnsupportedAdditionalFieldError),
    /// Storage error.
    #[error(transparent)]
    StorageError(#[from] StorageError),
    /// Missing metadata.
    #[error("group metadata is missing")]
    MissingMetadata,
}

impl<TStorage: ?Sized + ReadableStorageTraits> Group<TStorage> {}

impl<TStorage: ?Sized + WritableStorageTraits> Group<TStorage> {
    /// Store metadata with default [`GroupMetadataOptions`].
    ///
    /// # Errors
    /// Returns [`StorageError`] if there is an underlying store error.
    pub fn store_metadata(&self) -> Result<(), StorageError> {
        self.store_metadata_opt(&GroupMetadataOptions::default())
    }

    /// Store metadata with non-default [`GroupMetadataOptions`].
    ///
    /// # Errors
    /// Returns [`StorageError`] if there is an underlying store error.
    pub fn store_metadata_opt(&self, options: &GroupMetadataOptions) -> Result<(), StorageError> {
        let storage_handle = Arc::new(StorageHandle::new(self.storage.clone()));

        // Get the metadata with options applied and store
        let metadata = self.metadata_opt(options);
        crate::storage::create_group(&*storage_handle, self.path(), &metadata)
    }

    /// Erase the metadata with default [`MetadataEraseVersion`] options.
    ///
    /// Succeeds if the metadata does not exist.
    ///
    /// # Errors
    /// Returns a [`StorageError`] if there is an underlying store error.
    pub fn erase_metadata(&self) -> Result<(), StorageError> {
        self.erase_metadata_opt(&MetadataEraseVersion::default())
    }

    /// Erase the metadata with non-default [`MetadataEraseVersion`] options.
    ///
    /// Succeeds if the metadata does not exist.
    ///
    /// # Errors
    /// Returns a [`StorageError`] if there is an underlying store error.
    pub fn erase_metadata_opt(&self, options: &MetadataEraseVersion) -> Result<(), StorageError> {
        let storage_handle = StorageHandle::new(self.storage.clone());
        match options {
            MetadataEraseVersion::Default => match self.metadata {
                GroupMetadata::V3(_) => storage_handle.erase(&meta_key(self.path())),
                GroupMetadata::V2(_) => {
                    storage_handle.erase(&meta_key_v2_group(self.path()))?;
                    storage_handle.erase(&meta_key_v2_attributes(self.path()))
                }
            },
            MetadataEraseVersion::All => {
                storage_handle.erase(&meta_key(self.path()))?;
                storage_handle.erase(&meta_key_v2_group(self.path()))?;
                storage_handle.erase(&meta_key_v2_attributes(self.path()))
            }
            MetadataEraseVersion::V3 => storage_handle.erase(&meta_key(self.path())),
            MetadataEraseVersion::V2 => {
                storage_handle.erase(&meta_key_v2_group(self.path()))?;
                storage_handle.erase(&meta_key_v2_attributes(self.path()))
            }
        }
    }
}

#[cfg(feature = "async")]
impl<TStorage: ?Sized + AsyncWritableStorageTraits> Group<TStorage> {
    /// Async variant of [`store_metadata`](Group::store_metadata).
    #[allow(clippy::missing_errors_doc)]
    pub async fn async_store_metadata(&self) -> Result<(), StorageError> {
        self.async_store_metadata_opt(&GroupMetadataOptions::default())
            .await
    }

    /// Async variant of [`store_metadata_opt`](Group::store_metadata_opt).
    #[allow(clippy::missing_errors_doc)]
    pub async fn async_store_metadata_opt(
        &self,
        options: &GroupMetadataOptions,
    ) -> Result<(), StorageError> {
        let storage_handle = StorageHandle::new(self.storage.clone());

        // Get the metadata with options applied and store
        let metadata = self.metadata_opt(options);
        crate::storage::async_create_group(&storage_handle, self.path(), &metadata).await
    }

    /// Async variant of [`erase_metadata`](Group::erase_metadata).
    #[allow(clippy::missing_errors_doc)]
    pub async fn async_erase_metadata(&self) -> Result<(), StorageError> {
        self.async_erase_metadata_opt(&MetadataEraseVersion::default())
            .await
    }

    /// Async variant of [`erase_metadata_opt`](Group::erase_metadata_opt).
    #[allow(clippy::missing_errors_doc)]
    pub async fn async_erase_metadata_opt(
        &self,
        options: &MetadataEraseVersion,
    ) -> Result<(), StorageError> {
        let storage_handle = StorageHandle::new(self.storage.clone());
        match options {
            MetadataEraseVersion::Default => match self.metadata {
                GroupMetadata::V3(_) => storage_handle.erase(&meta_key(self.path())).await,
                GroupMetadata::V2(_) => {
                    storage_handle
                        .erase(&meta_key_v2_group(self.path()))
                        .await?;
                    storage_handle
                        .erase(&meta_key_v2_attributes(self.path()))
                        .await
                }
            },
            MetadataEraseVersion::All => {
                storage_handle.erase(&meta_key(self.path())).await?;
                storage_handle
                    .erase(&meta_key_v2_group(self.path()))
                    .await?;
                storage_handle
                    .erase(&meta_key_v2_attributes(self.path()))
                    .await
            }
            MetadataEraseVersion::V3 => storage_handle.erase(&meta_key(self.path())).await,
            MetadataEraseVersion::V2 => {
                storage_handle
                    .erase(&meta_key_v2_group(self.path()))
                    .await?;
                storage_handle
                    .erase(&meta_key_v2_attributes(self.path()))
                    .await
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::{store::MemoryStore, StoreKey};

    use super::*;

    const JSON_VALID1: &str = r#"{
    "zarr_format": 3,
    "node_type": "group",
    "attributes": {
        "spam": "ham",
        "eggs": 42
    }
}"#;

    #[test]
    fn group_metadata_v3_1() {
        let group_metadata: GroupMetadataV3 = serde_json::from_str(JSON_VALID1).unwrap();
        let store = MemoryStore::default();
        Group::new_with_metadata(store.into(), "/", GroupMetadata::V3(group_metadata)).unwrap();
    }

    #[test]
    fn group_metadata_v3_2() {
        let group_metadata: GroupMetadataV3 = serde_json::from_str(
            r#"{
            "zarr_format": 3,
            "node_type": "group",
            "attributes": {
                "spam": "ham",
                "eggs": 42
            },
            "unknown": {
                "must_understand": false
            }
        }"#,
        )
        .unwrap();
        let store = MemoryStore::default();
        Group::new_with_metadata(store.into(), "/", GroupMetadata::V3(group_metadata)).unwrap();
    }

    #[test]
    fn group_metadata_v3_invalid_format() {
        let group_metadata = serde_json::from_str::<GroupMetadataV3>(
            r#"{
            "zarr_format": 2,
            "node_type": "group",
            "attributes": {
                "spam": "ham",
                "eggs": 42
            }
        }"#,
        );
        assert!(group_metadata.is_err());
    }

    #[test]
    fn group_metadata_invalid_type() {
        let group_metadata = serde_json::from_str::<GroupMetadata>(
            r#"{
            "zarr_format": 3,
            "node_type": "array",
            "attributes": {
                "spam": "ham",
                "eggs": 42
            }
        }"#,
        );
        assert!(group_metadata.is_err());
    }

    #[test]
    fn group_metadata_invalid_additional_field() {
        let group_metadata = serde_json::from_str::<GroupMetadata>(
            r#"{
                "zarr_format": 3,
                "node_type": "group",
                "attributes": {
                  "spam": "ham",
                  "eggs": 42
                },
                "unknown": "fail"
            }"#,
        );
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

        let group_copy = Group::open(store, group_path).unwrap();
        assert_eq!(group_copy.metadata(), group.metadata());
        let group_metadata_str = group.metadata().to_string();
        println!("{}", group_metadata_str);
        assert!(
            group_metadata_str == r#"{"node_type":"group","zarr_format":3}"#
                || group_metadata_str == r#"{"zarr_format":3,"node_type":"group"}"#
        );
        // assert_eq!(
        //     group.to_string(),
        //     r#"group at /group with metadata {"node_type":"group","zarr_format":3}"#
        // );
    }

    #[test]
    fn group_metadata_invalid_path() {
        let group_metadata: GroupMetadata = serde_json::from_str(JSON_VALID1).unwrap();
        let store = MemoryStore::default();
        assert_eq!(
            Group::new_with_metadata(store.into(), "abc", group_metadata)
                .unwrap_err()
                .to_string(),
            "invalid node path abc"
        );
    }

    #[test]
    fn group_invalid_path() {
        let store: std::sync::Arc<MemoryStore> = std::sync::Arc::new(MemoryStore::new());
        assert_eq!(
            Group::open(store, "abc").unwrap_err().to_string(),
            "invalid node path abc"
        );
    }

    #[test]
    fn group_invalid_metadata() {
        let store: std::sync::Arc<MemoryStore> = std::sync::Arc::new(MemoryStore::new());
        store
            .set(&StoreKey::new("zarr.json").unwrap(), vec![0].into())
            .unwrap();
        assert_eq!(
            Group::open(store, "/").unwrap_err().to_string(),
            "error parsing metadata for zarr.json: expected value at line 1 column 1"
        );
    }

    #[cfg(feature = "async")]
    #[tokio::test]
    async fn group_metadata_write_read_async() {
        let store = std::sync::Arc::new(crate::storage::store::AsyncObjectStore::new(
            object_store::memory::InMemory::new(),
        ));
        let group_path = "/group";
        let group = GroupBuilder::new()
            .build(store.clone(), group_path)
            .unwrap();
        group.async_store_metadata().await.unwrap();

        let group_copy = Group::async_open(store, group_path).await.unwrap();
        assert_eq!(group_copy.metadata(), group.metadata());
    }

    /// Implicit group support is removed since implicit groups were removed from the Zarr V3 spec
    #[test]
    fn group_implicit() {
        let store = std::sync::Arc::new(MemoryStore::new());
        let group_path = "/group";
        assert!(Group::open(store, group_path).is_err());
    }
}
