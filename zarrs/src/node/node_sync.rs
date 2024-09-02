use std::sync::Arc;

use crate::{
    array::{ArrayMetadata, ArrayMetadataV2},
    group::GroupMetadata,
    metadata::{v2::GroupMetadataV2, v3::NodeMetadataV3},
    storage::{
        discover_children, ListableStorageTraits, ReadableStorageTraits, StorageError, StoreKey,
        StorePrefix,
    },
};

use super::{
    meta_key_v2_array, meta_key_v2_attributes, meta_key_v2_group, meta_key_v3, Node, NodeMetadata,
    NodePath, NodePathError,
};

fn get_metadata_v3<TStorage: ?Sized + ReadableStorageTraits + ListableStorageTraits>(
    storage: &Arc<TStorage>,
    prefix: &StorePrefix,
) -> Result<Option<NodeMetadata>, StorageError> {
    let key: StoreKey = meta_key_v3(
        &prefix
            .try_into()
            .map_err(|err: NodePathError| StorageError::Other(err.to_string()))?,
    );
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
    let node_path = prefix
        .try_into()
        .map_err(|err: NodePathError| StorageError::Other(err.to_string()))?;
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
    let prefix: StorePrefix = path.try_into()?;
    let prefixes = discover_children(storage, &prefix)?;
    let mut nodes: Vec<Node> = Vec::new();
    for prefix in &prefixes {
        let mut child_metadata = get_metadata_v3(storage, prefix)?;
        if child_metadata.is_none() {
            child_metadata = get_metadata_v2(storage, prefix)?;
        }
        let Some(child_metadata) = child_metadata else {
            return Err(StorageError::MissingMetadata(prefix.clone()));
        };

        let path: NodePath = prefix
            .try_into()
            .map_err(|err: NodePathError| StorageError::Other(err.to_string()))?;
        let children = match child_metadata {
            NodeMetadata::Array(_) => Vec::default(),
            NodeMetadata::Group(_) => get_child_nodes(storage, &path)?,
        };
        nodes.push(Node::new_with_metadata(path, child_metadata, children));
    }
    Ok(nodes)
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
