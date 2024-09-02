use std::sync::Arc;

use async_recursion::async_recursion;

use crate::{
    array::{ArrayMetadata, ArrayMetadataV2},
    group::GroupMetadata,
    metadata::{v2::GroupMetadataV2, v3::NodeMetadataV3},
    storage::{
        async_discover_children, AsyncListableStorageTraits, AsyncReadableStorageTraits,
        StorageError, StoreKey, StorePrefix,
    },
};

use super::{
    meta_key_v2_array, meta_key_v2_attributes, meta_key_v2_group, meta_key_v3, Node, NodeMetadata,
    NodePath, NodePathError,
};

async fn get_metadata_v3<
    TStorage: ?Sized + AsyncReadableStorageTraits + AsyncListableStorageTraits,
>(
    storage: &Arc<TStorage>,
    prefix: &StorePrefix,
) -> Result<Option<NodeMetadata>, StorageError> {
    let key: StoreKey = meta_key_v3(
        &prefix
            .try_into()
            .map_err(|err: NodePathError| StorageError::Other(err.to_string()))?,
    );
    match storage.get(&key).await? {
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

async fn get_metadata_v2<
    TStorage: ?Sized + AsyncReadableStorageTraits + AsyncListableStorageTraits,
>(
    storage: &Arc<TStorage>,
    prefix: &StorePrefix,
) -> Result<Option<NodeMetadata>, StorageError> {
    let node_path = prefix
        .try_into()
        .map_err(|err: NodePathError| StorageError::Other(err.to_string()))?;
    let attributes_key = meta_key_v2_attributes(&node_path);

    // Try array
    let key_array: StoreKey = meta_key_v2_array(&node_path);
    if let Some(metadata) = storage.get(&key_array).await? {
        let mut metadata: ArrayMetadataV2 = serde_json::from_slice(&metadata)
            .map_err(|err| StorageError::InvalidMetadata(key_array, err.to_string()))?;
        let attributes = storage.get(&attributes_key).await?;
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
    if let Some(metadata) = storage.get(&key_group).await? {
        let mut metadata: GroupMetadataV2 = serde_json::from_slice(&metadata)
            .map_err(|err| StorageError::InvalidMetadata(key_group, err.to_string()))?;
        let attributes = storage.get(&attributes_key).await?;
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

/// Asynchronously get the child nodes.
///
/// # Errors
/// Returns a [`StorageError`] if there is an underlying error with the store.
#[async_recursion]
pub async fn async_get_child_nodes<TStorage>(
    storage: &Arc<TStorage>,
    path: &NodePath,
) -> Result<Vec<Node>, StorageError>
where
    TStorage: ?Sized + AsyncReadableStorageTraits + AsyncListableStorageTraits,
{
    let prefix: StorePrefix = path.try_into()?;
    let prefixes = async_discover_children(storage, &prefix).await?;
    let mut nodes: Vec<Node> = Vec::new();
    // TODO: Asynchronously get metadata of all prefixes
    for prefix in &prefixes {
        let mut child_metadata = get_metadata_v3(storage, prefix).await?;
        if child_metadata.is_none() {
            child_metadata = get_metadata_v2(storage, prefix).await?;
        }
        let Some(child_metadata) = child_metadata else {
            return Err(StorageError::MissingMetadata(prefix.clone()));
        };

        let path: NodePath = prefix
            .try_into()
            .map_err(|err: NodePathError| StorageError::Other(err.to_string()))?;
        let children = match child_metadata {
            NodeMetadata::Array(_) => Vec::default(),
            NodeMetadata::Group(_) => async_get_child_nodes(storage, &path).await?,
        };
        nodes.push(Node::new_with_metadata(path, child_metadata, children));
    }
    Ok(nodes)
}

/// Asynchronously check if a node exists.
///
/// # Errors
/// Returns a [`StorageError`] if there is an underlying error with the store.
pub async fn async_node_exists<
    TStorage: ?Sized + AsyncReadableStorageTraits + AsyncListableStorageTraits,
>(
    storage: &Arc<TStorage>,
    path: &NodePath,
) -> Result<bool, StorageError> {
    Ok(storage.get(&meta_key_v3(path)).await?.is_some()
        || storage.get(&meta_key_v2_array(path)).await?.is_some()
        || storage.get(&meta_key_v2_group(path)).await?.is_some())
}

/// Asynchronously check if a node exists.
///
/// # Errors
/// Returns a [`StorageError`] if there is an underlying error with the store.
pub async fn async_node_exists_listable<TStorage: ?Sized + AsyncListableStorageTraits>(
    storage: &Arc<TStorage>,
    path: &NodePath,
) -> Result<bool, StorageError> {
    let prefix: StorePrefix = path.try_into()?;
    storage.list_prefix(&prefix).await.map(|keys| {
        keys.contains(&meta_key_v3(path))
            | keys.contains(&meta_key_v2_array(path))
            | keys.contains(&meta_key_v2_group(path))
    })
}
