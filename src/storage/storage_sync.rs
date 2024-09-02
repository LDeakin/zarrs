use std::sync::Arc;

use itertools::Itertools;

use crate::{
    array::{ArrayMetadata, ArrayMetadataV2},
    group::GroupMetadata,
    metadata::{v2::GroupMetadataV2, v3::NodeMetadataV3},
    node::{Node, NodeMetadata, NodePath},
};

use super::{
    byte_range::ByteRange, data_key, meta_key_v2_array, meta_key_v2_attributes, meta_key_v2_group,
    meta_key_v3, Bytes, MaybeBytes, StorageError, StoreKey, StoreKeyRange, StoreKeyStartValue,
    StoreKeys, StoreKeysPrefixes, StorePrefix, StorePrefixes,
};

/// Readable storage traits.
pub trait ReadableStorageTraits: Send + Sync {
    /// Retrieve the value (bytes) associated with a given [`StoreKey`].
    ///
    /// Returns [`None`] if the key is not found.
    ///
    /// # Errors
    /// Returns a [`StorageError`] if there is an underlying storage error.
    fn get(&self, key: &StoreKey) -> Result<MaybeBytes, StorageError> {
        Ok(self
            .get_partial_values_key(key, &[ByteRange::FromStart(0, None)])?
            .map(|mut v| v.remove(0)))
    }

    /// Retrieve partial bytes from a list of byte ranges for a store key.
    ///
    /// Returns [`None`] if the key is not found.
    ///
    /// # Errors
    /// Returns a [`StorageError`] if there is an underlying storage error.
    fn get_partial_values_key(
        &self,
        key: &StoreKey,
        byte_ranges: &[ByteRange],
    ) -> Result<Option<Vec<Bytes>>, StorageError>;

    /// Retrieve partial bytes from a list of [`StoreKeyRange`].
    ///
    /// # Parameters
    /// * `key_ranges`: ordered set of ([`StoreKey`], [`ByteRange`]) pairs. A key may occur multiple times with different ranges.
    ///
    /// # Output
    /// A a list of values in the order of the `key_ranges`. It will be [`None`] for missing keys.
    ///
    /// # Errors
    /// Returns a [`StorageError`] if there is an underlying storage error.
    fn get_partial_values(
        &self,
        key_ranges: &[StoreKeyRange],
    ) -> Result<Vec<MaybeBytes>, StorageError> {
        self.get_partial_values_batched_by_key(key_ranges)
    }

    /// Return the size in bytes of the value at `key`.
    ///
    /// Returns [`None`] if the key is not found.
    ///
    /// # Errors
    /// Returns a [`StorageError`] if there is an underlying storage error.
    fn size_key(&self, key: &StoreKey) -> Result<Option<u64>, StorageError>;

    /// A utility method with the same input and output as [`get_partial_values`](ReadableStorageTraits::get_partial_values) that internally calls [`get_partial_values_key`](ReadableStorageTraits::get_partial_values_key) with byte ranges grouped by key.
    ///
    /// Readable storage can use this function in the implementation of [`get_partial_values`](ReadableStorageTraits::get_partial_values) if that is optimal.
    ///
    /// # Errors
    /// Returns a [`StorageError`] if there is an underlying storage error.
    fn get_partial_values_batched_by_key(
        &self,
        key_ranges: &[StoreKeyRange],
    ) -> Result<Vec<MaybeBytes>, StorageError> {
        let mut out: Vec<MaybeBytes> = Vec::with_capacity(key_ranges.len());
        let mut last_key = None;
        let mut byte_ranges_key = Vec::new();
        for key_range in key_ranges {
            if last_key.is_none() {
                last_key = Some(&key_range.key);
            }
            let last_key_val = last_key.unwrap();

            if key_range.key != *last_key_val {
                // Found a new key, so do a batched get of the byte ranges of the last key
                let bytes = (self.get_partial_values_key(last_key.unwrap(), &byte_ranges_key)?)
                    .map_or_else(
                        || vec![None; byte_ranges_key.len()],
                        |partial_values| partial_values.into_iter().map(Some).collect(),
                    );
                out.extend(bytes);
                last_key = Some(&key_range.key);
                byte_ranges_key.clear();
            }

            byte_ranges_key.push(key_range.byte_range);
        }

        if !byte_ranges_key.is_empty() {
            // Get the byte ranges of the last key
            let bytes = (self.get_partial_values_key(last_key.unwrap(), &byte_ranges_key)?)
                .map_or_else(
                    || vec![None; byte_ranges_key.len()],
                    |partial_values| partial_values.into_iter().map(Some).collect(),
                );
            out.extend(bytes);
        }

        Ok(out)
    }
}

/// Listable storage traits.
pub trait ListableStorageTraits: Send + Sync {
    /// Retrieve all [`StoreKeys`] in the store.
    ///
    /// # Errors
    /// Returns a [`StorageError`] if there is an underlying error with the store.
    fn list(&self) -> Result<StoreKeys, StorageError>;

    /// Retrieve all [`StoreKeys`] with a given [`StorePrefix`].
    ///
    /// # Errors
    /// Returns a [`StorageError`] if the prefix is not a directory or there is an underlying error with the store.
    fn list_prefix(&self, prefix: &StorePrefix) -> Result<StoreKeys, StorageError>;

    /// Retrieve all [`StoreKeys`] and [`StorePrefix`] which are direct children of [`StorePrefix`].
    ///
    /// # Errors
    /// Returns a [`StorageError`] if the prefix is not a directory or there is an underlying error with the store.
    ///
    fn list_dir(&self, prefix: &StorePrefix) -> Result<StoreKeysPrefixes, StorageError>;

    /// Return the size in bytes of all keys under `prefix`.
    ///
    /// # Errors
    /// Returns a `StorageError` if the store does not support `size()` or there is an underlying error with the store.
    fn size_prefix(&self, prefix: &StorePrefix) -> Result<u64, StorageError>;

    /// Return the total size in bytes of the storage.
    ///
    /// # Errors
    /// Returns a `StorageError` if the store does not support `size()` or there is an underlying error with the store.
    fn size(&self) -> Result<u64, StorageError> {
        self.size_prefix(&StorePrefix::root())
    }
}

/// Set partial values for a store.
///
/// This method reads entire values, updates them, and replaces them.
/// Stores can use this internally if they do not support updating/appending without replacement.
///
/// # Errors
/// Returns a [`StorageError`] if an underlying store operation fails.
///
/// # Panics
/// Panics if a key ends beyond `usize::MAX`.
pub fn store_set_partial_values<T: ReadableWritableStorageTraits>(
    store: &T,
    key_start_values: &[StoreKeyStartValue],
    // truncate: bool,
) -> Result<(), StorageError> {
    // Group by key
    key_start_values
        .iter()
        .chunk_by(|key_start_value| &key_start_value.key)
        .into_iter()
        .map(|(key, group)| (key.clone(), group.into_iter().cloned().collect::<Vec<_>>()))
        .try_for_each(|(key, group)| {
            // Lock the store key
            // let mutex = store.mutex(&key)?;
            // let _lock = mutex.lock();

            // Read the store key
            let bytes = store.get(&key)?.unwrap_or_default();

            // Convert to a mutable vector of the required length
            let end_max =
                usize::try_from(group.iter().map(StoreKeyStartValue::end).max().unwrap()).unwrap();
            let mut bytes = if bytes.len() < end_max {
                // Expand the store key if needed
                let mut vec = Vec::with_capacity(end_max);
                vec.extend_from_slice(&bytes);
                vec.resize_with(end_max, Default::default);
                vec
            // } else if truncate {
            //     let mut bytes = bytes.to_vec();
            //     bytes.truncate(end_max);
            //     bytes
            } else {
                bytes.to_vec()
            };

            // Update the store key
            for key_start_value in group {
                let start: usize = key_start_value.start.try_into().unwrap();
                let end: usize = key_start_value.end().try_into().unwrap();
                bytes[start..end].copy_from_slice(key_start_value.value);
            }

            // Write the store key
            store.set(&key, Bytes::from(bytes))
        })?;
    Ok(())
}

/// Writable storage traits.
pub trait WritableStorageTraits: Send + Sync {
    /// Store bytes at a [`StoreKey`].
    ///
    /// # Errors
    /// Returns a [`StorageError`] on failure to store.
    fn set(&self, key: &StoreKey, value: Bytes) -> Result<(), StorageError>;

    /// Store bytes according to a list of [`StoreKeyStartValue`].
    ///
    /// # Errors
    /// Returns a [`StorageError`] on failure to store.
    fn set_partial_values(
        &self,
        key_start_values: &[StoreKeyStartValue],
    ) -> Result<(), StorageError>;

    /// Erase a [`StoreKey`].
    ///
    /// # Errors
    /// Returns a [`StorageError`] if there is an underlying storage error.
    fn erase(&self, key: &StoreKey) -> Result<(), StorageError>;

    /// Erase a list of [`StoreKey`].
    ///
    /// # Errors
    /// Returns a [`StorageError`] if there is an underlying storage error.
    fn erase_values(&self, keys: &[StoreKey]) -> Result<(), StorageError> {
        keys.iter().try_for_each(|key| self.erase(key))?;
        Ok(())
    }

    /// Erase all [`StoreKey`] under [`StorePrefix`].
    ///
    /// # Errors
    /// Returns a [`StorageError`] is the prefix is not in the store, or the erase otherwise fails.
    fn erase_prefix(&self, prefix: &StorePrefix) -> Result<(), StorageError>;
}

/// A supertrait of [`ReadableStorageTraits`] and [`WritableStorageTraits`].
pub trait ReadableWritableStorageTraits: ReadableStorageTraits + WritableStorageTraits {
    // /// Returns the mutex for the store value at `key`.
    // ///
    // /// # Errors
    // /// Returns a [`StorageError`] if the mutex cannot be retrieved.
    // fn mutex(&self, key: &StoreKey) -> Result<StoreKeyMutex, StorageError>;
}

impl<T> ReadableWritableStorageTraits for T where T: ReadableStorageTraits + WritableStorageTraits {}

/// A supertrait of [`ReadableStorageTraits`] and [`ListableStorageTraits`].
pub trait ReadableListableStorageTraits: ReadableStorageTraits + ListableStorageTraits {}

impl<T> ReadableListableStorageTraits for T where T: ReadableStorageTraits + ListableStorageTraits {}

/// A supertrait of [`ReadableWritableStorageTraits`] and [`ListableStorageTraits`].
pub trait ReadableWritableListableStorageTraits:
    ReadableWritableStorageTraits + ListableStorageTraits
{
}

impl<T> ReadableWritableListableStorageTraits for T where
    T: ReadableWritableStorageTraits + ListableStorageTraits
{
}

fn get_metadata_v3<TStorage: ?Sized + ReadableStorageTraits + ListableStorageTraits>(
    storage: &Arc<TStorage>,
    prefix: &StorePrefix,
) -> Result<Option<NodeMetadata>, StorageError> {
    let key: StoreKey = meta_key_v3(&prefix.try_into()?);
    match storage.get(&key)? {
        Some(metadata) => {
            let metadata: NodeMetadataV3 = serde_json::from_slice(&metadata)
                .map_err(|err| StorageError::InvalidMetadata(key, err.to_string()))?;
            Ok(Some(match metadata {
                NodeMetadataV3::Array(array) => NodeMetadata::Array(ArrayMetadata::V3(array)),
                NodeMetadataV3::Group(group) => NodeMetadata::Group(GroupMetadata::V3(group)),
            }))
        }
        None => Ok(None),
    }
}

fn get_metadata_v2<TStorage: ?Sized + ReadableStorageTraits + ListableStorageTraits>(
    storage: &Arc<TStorage>,
    prefix: &StorePrefix,
) -> Result<Option<NodeMetadata>, StorageError> {
    let node_path = prefix.try_into()?;
    let attributes_key = meta_key_v2_attributes(&node_path);

    // Try array
    let key_array: StoreKey = meta_key_v2_array(&node_path);
    if let Some(metadata) = storage.get(&key_array)? {
        let mut metadata: ArrayMetadataV2 = serde_json::from_slice(&metadata)
            .map_err(|err| StorageError::InvalidMetadata(key_array, err.to_string()))?;
        let attributes = storage.get(&attributes_key)?;
        if let Some(attributes) = attributes {
            let attributes: serde_json::Map<String, serde_json::Value> =
                serde_json::from_slice(&attributes).map_err(|err| {
                    StorageError::InvalidMetadata(attributes_key, err.to_string())
                })?;
            metadata.attributes = attributes;
        }
        return Ok(Some(NodeMetadata::Array(ArrayMetadata::V2(metadata))));
    }

    // Try group
    let key_group: StoreKey = meta_key_v2_group(&node_path);
    if let Some(metadata) = storage.get(&key_group)? {
        let mut metadata: GroupMetadataV2 = serde_json::from_slice(&metadata)
            .map_err(|err| StorageError::InvalidMetadata(key_group, err.to_string()))?;
        let attributes = storage.get(&attributes_key)?;
        if let Some(attributes) = attributes {
            let attributes: serde_json::Map<String, serde_json::Value> =
                serde_json::from_slice(&attributes).map_err(|err| {
                    StorageError::InvalidMetadata(attributes_key, err.to_string())
                })?;
            metadata.attributes = attributes;
        }
        return Ok(Some(NodeMetadata::Group(GroupMetadata::V2(metadata))));
    }

    Ok(None)
}

/// Get the child nodes.
///
/// # Errors
/// Returns a [`StorageError`] if there is an underlying error with the store.
pub fn get_child_nodes<TStorage: ?Sized + ReadableStorageTraits + ListableStorageTraits>(
    storage: &Arc<TStorage>,
    path: &NodePath,
) -> Result<Vec<Node>, StorageError> {
    let prefixes = discover_children(storage, path)?;
    let mut nodes: Vec<Node> = Vec::new();
    for prefix in &prefixes {
        let mut child_metadata = get_metadata_v3(storage, prefix)?;
        if child_metadata.is_none() {
            child_metadata = get_metadata_v2(storage, prefix)?;
        }
        let Some(child_metadata) = child_metadata else {
            return Err(StorageError::MissingMetadata(prefix.clone()));
        };

        let path: NodePath = prefix.try_into()?;
        let children = match child_metadata {
            NodeMetadata::Array(_) => Vec::default(),
            NodeMetadata::Group(_) => get_child_nodes(storage, &path)?,
        };
        nodes.push(Node::new_with_metadata(path, child_metadata, children));
    }
    Ok(nodes)
}

/// Retrieve byte ranges from a chunk.
///
/// Returns [`None`] where keys are not found.
///
/// # Errors
/// Returns a [`StorageError`] if there is an underlying error with the store.
pub fn retrieve_partial_values(
    storage: &dyn ReadableStorageTraits,
    array_path: &NodePath,
    chunk_key: &StoreKey,
    bytes_ranges: &[ByteRange],
) -> Result<Vec<MaybeBytes>, StorageError> {
    let key = data_key(array_path, chunk_key);
    let key_ranges: Vec<StoreKeyRange> = bytes_ranges
        .iter()
        .map(|byte_range| StoreKeyRange::new(key.clone(), *byte_range))
        .collect();
    storage.get_partial_values(&key_ranges)
}

/// Discover the children of a node.
///
/// # Errors
/// Returns a [`StorageError`] if there is an underlying error with the store.
pub fn discover_children<TStorage: ?Sized + ReadableStorageTraits + ListableStorageTraits>(
    storage: &Arc<TStorage>,
    path: &NodePath,
) -> Result<StorePrefixes, StorageError> {
    let prefix: StorePrefix = path.try_into()?;
    let children: Result<Vec<_>, _> = storage
        .list_dir(&prefix)?
        .prefixes()
        .iter()
        .filter(|v| !v.as_str().starts_with("__"))
        .map(|v| StorePrefix::new(v.as_str()))
        .collect();
    Ok(children?)
}

/// Discover all nodes.
///
/// # Errors
/// Returns a [`StorageError`] if there is an underlying error with the store.
///
pub fn discover_nodes(storage: &dyn ListableStorageTraits) -> Result<StoreKeys, StorageError> {
    storage.list_prefix(&"".try_into()?)
}

/// Erase a node (group or array) and all of its children.
///
/// Succeeds if the node does not exist.
///
/// # Errors
/// Returns a [`StorageError`] if there is an underlying error with the store.
pub fn erase_node(
    storage: &dyn WritableStorageTraits,
    path: &NodePath,
) -> Result<(), StorageError> {
    let prefix = path.try_into()?;
    storage.erase_prefix(&prefix)
}

/// Check if a node exists.
///
/// # Errors
/// Returns a [`StorageError`] if there is an underlying error with the store.
pub fn node_exists<TStorage: ?Sized + ReadableStorageTraits + ListableStorageTraits>(
    storage: &Arc<TStorage>,
    path: &NodePath,
) -> Result<bool, StorageError> {
    Ok(storage.get(&meta_key_v3(path))?.is_some()
        || storage.get(&meta_key_v2_array(path))?.is_some()
        || storage.get(&meta_key_v2_group(path))?.is_some())
}

/// Check if a node exists.
///
/// # Errors
/// Returns a [`StorageError`] if there is an underlying error with the store.
pub fn node_exists_listable<TStorage: ?Sized + ListableStorageTraits>(
    storage: &Arc<TStorage>,
    path: &NodePath,
) -> Result<bool, StorageError> {
    let prefix: StorePrefix = path.try_into()?;
    storage.list_prefix(&prefix).map(|keys| {
        keys.contains(&meta_key_v3(path))
            | keys.contains(&meta_key_v2_array(path))
            | keys.contains(&meta_key_v2_group(path))
    })
}
