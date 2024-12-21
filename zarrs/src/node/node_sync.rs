use std::sync::Arc;

use crate::{
    config::MetadataRetrieveVersion,
    storage::{
        discover_children, ListableStorageTraits, ReadableStorageTraits, StorageError, StorePrefix,
    },
};

use super::{
    meta_key_v2_array, meta_key_v2_group, meta_key_v3, Node, NodeMetadata, NodePath, NodePathError,
};

/// Get the child nodes.
///
/// # Errors
/// Returns a [`StorageError`] if there is an underlying error with the store.
// FIXME: Change to NodeCreateError in the next breaking release
pub fn get_child_nodes<TStorage: ?Sized + ReadableStorageTraits + ListableStorageTraits>(
    storage: &Arc<TStorage>,
    path: &NodePath,
) -> Result<Vec<Node>, StorageError> {
    let prefix: StorePrefix = path.try_into()?;
    let prefixes = discover_children(storage, &prefix)?;
    let mut nodes: Vec<Node> = Vec::new();
    for prefix in &prefixes {
        let path: NodePath = prefix
            .try_into()
            .map_err(|err: NodePathError| StorageError::Other(err.to_string()))?;
        let child_metadata = Node::get_metadata(storage, &path, &MetadataRetrieveVersion::Default)?;

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

/// Get the direct child nodes.
///
/// Unlike [`get_child_nodes`], this does not fully resolve the node hierarchy and the nodes returned will not have any children.
///
/// # Errors
/// Returns a [`StorageError`] if there is an underlying error with the store.
// FIXME: Change to NodeCreateError in the next breaking release
pub fn get_direct_child_nodes<TStorage: ?Sized + ReadableStorageTraits + ListableStorageTraits>(
    storage: &Arc<TStorage>,
    path: &NodePath,
) -> Result<Vec<Node>, StorageError> {
    let prefix: StorePrefix = path.try_into()?;
    let prefixes = discover_children(storage, &prefix)?;
    let mut nodes: Vec<Node> = Vec::new();
    for prefix in &prefixes {
        let path: NodePath = prefix
            .try_into()
            .map_err(|err: NodePathError| StorageError::Other(err.to_string()))?;
        let child_metadata = Node::get_metadata(storage, &path, &MetadataRetrieveVersion::Default)?;

        let path: NodePath = prefix
            .try_into()
            .map_err(|err: NodePathError| StorageError::Other(err.to_string()))?;
        nodes.push(Node::new_with_metadata(path, child_metadata, vec![]));
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
