use std::sync::Arc;

use itertools::Itertools;

use crate::{
    array::{ArrayMetadata, ChunkKeyEncoding},
    byte_range::ByteRange,
    group::{GroupMetadata, GroupMetadataV3},
    node::{Node, NodeMetadata, NodePath},
};

use super::{
    data_key, meta_key, meta_key_v2_array, meta_key_v2_attributes, meta_key_v2_group, Bytes,
    MaybeBytes, StorageError, StoreKey, StoreKeyRange, StoreKeyStartValue, StoreKeys,
    StoreKeysPrefixes, StorePrefix, StorePrefixes,
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
        let key = meta_key(&prefix.try_into()?);
        let child_metadata = match storage.get(&key)? {
            Some(child_metadata) => {
                let metadata: NodeMetadata = serde_json::from_slice(&child_metadata)
                    .map_err(|err| StorageError::InvalidMetadata(key, err.to_string()))?;
                metadata
            }
            None => NodeMetadata::Group(GroupMetadataV3::default().into()),
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

/// Create a group.
///
/// # Errors
/// Returns a [`StorageError`] if there is an underlying error with the store.
pub fn create_group(
    storage: &dyn WritableStorageTraits,
    path: &NodePath,
    group: &GroupMetadata,
) -> Result<(), StorageError> {
    match group {
        GroupMetadata::V3(group) => {
            let key = meta_key(path);
            let json = serde_json::to_vec_pretty(group)
                .map_err(|err| StorageError::InvalidMetadata(key.clone(), err.to_string()))?;
            storage.set(&meta_key(path), json.into())
        }
        GroupMetadata::V2(group) => {
            let mut group = group.clone();

            if !group.attributes.is_empty() {
                // Store .zgroup
                let key = meta_key_v2_attributes(path);
                let json = serde_json::to_vec_pretty(&group.attributes)
                    .map_err(|err| StorageError::InvalidMetadata(key.clone(), err.to_string()))?;
                storage.set(&key, json.into())?;

                group.attributes = serde_json::Map::default();
            }

            // Store .zarray
            let key = meta_key_v2_group(path);
            let json = serde_json::to_vec_pretty(&group)
                .map_err(|err| StorageError::InvalidMetadata(key.clone(), err.to_string()))?;
            storage.set(&key, json.into())?;
            Ok(())
        }
    }
}

/// Create an array.
///
/// # Errors
/// Returns a [`StorageError`] if there is an underlying error with the store.
pub fn create_array(
    storage: &dyn WritableStorageTraits,
    path: &NodePath,
    array: &ArrayMetadata,
) -> Result<(), StorageError> {
    match array {
        ArrayMetadata::V3(array) => {
            let key = meta_key(path);
            let json = serde_json::to_vec_pretty(array)
                .map_err(|err| StorageError::InvalidMetadata(key.clone(), err.to_string()))?;
            storage.set(&key, json.into())
        }
        ArrayMetadata::V2(array) => {
            let mut array = array.clone();

            if !array.attributes.is_empty() {
                // Store .zattrs
                let key = meta_key_v2_attributes(path);
                let json = serde_json::to_vec_pretty(&array.attributes)
                    .map_err(|err| StorageError::InvalidMetadata(key.clone(), err.to_string()))?;
                storage.set(&meta_key_v2_attributes(path), json.into())?;

                array.attributes = serde_json::Map::default();
            }

            // Store .zarray
            let key = meta_key_v2_array(path);
            let json = serde_json::to_vec_pretty(&array)
                .map_err(|err| StorageError::InvalidMetadata(key.clone(), err.to_string()))?;
            storage.set(&key, json.into())
        }
    }
}

/// Store a chunk.
///
/// # Errors
/// Returns a [`StorageError`] if there is an underlying error with the store.
pub fn store_chunk(
    storage: &dyn WritableStorageTraits,
    array_path: &NodePath,
    chunk_grid_indices: &[u64],
    chunk_key_encoding: &ChunkKeyEncoding,
    chunk_serialised: Bytes,
) -> Result<(), StorageError> {
    storage.set(
        &data_key(array_path, chunk_grid_indices, chunk_key_encoding),
        chunk_serialised,
    )?;
    Ok(())
}

/// Retrieve a chunk.
///
/// # Errors
/// Returns a [`StorageError`] if there is an underlying error with the store.
pub fn retrieve_chunk(
    storage: &dyn ReadableStorageTraits,
    array_path: &NodePath,
    chunk_grid_indices: &[u64],
    chunk_key_encoding: &ChunkKeyEncoding,
) -> Result<MaybeBytes, StorageError> {
    storage.get(&data_key(
        array_path,
        chunk_grid_indices,
        chunk_key_encoding,
    ))
}

/// Erase a chunk.
///
/// Succeeds if the chunk does not exist.
///
/// # Errors
/// Returns a [`StorageError`] if there is an underlying error with the store.
pub fn erase_chunk(
    storage: &dyn WritableStorageTraits,
    array_path: &NodePath,
    chunk_grid_indices: &[u64],
    chunk_key_encoding: &ChunkKeyEncoding,
) -> Result<(), StorageError> {
    storage.erase(&data_key(
        array_path,
        chunk_grid_indices,
        chunk_key_encoding,
    ))
}

/// Erase metadata.
///
/// Succeeds if the metadata does not exist.
///
/// # Errors
/// Returns a [`StorageError`] if there is an underlying error with the store.
pub fn erase_metadata(
    storage: &dyn WritableStorageTraits,
    array_path: &NodePath,
) -> Result<(), StorageError> {
    storage.erase(&meta_key(array_path))
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
    chunk_grid_indices: &[u64],
    chunk_key_encoding: &ChunkKeyEncoding,
    bytes_ranges: &[ByteRange],
) -> Result<Vec<MaybeBytes>, StorageError> {
    let key = data_key(array_path, chunk_grid_indices, chunk_key_encoding);
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
    Ok(storage
        .get(&meta_key(path))
        .map_or(storage.list_dir(&path.try_into()?).is_ok(), |_| true))
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
    prefix.parent().map_or_else(
        || Ok(false),
        |parent| {
            storage.list_dir(&parent).map(|keys_prefixes| {
                !keys_prefixes.keys().is_empty() || !keys_prefixes.prefixes().is_empty()
            })
        },
    )
}
