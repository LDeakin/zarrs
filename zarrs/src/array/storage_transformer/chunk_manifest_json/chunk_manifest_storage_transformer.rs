use std::sync::Arc;

use crate::metadata::v3::MetadataV3;

use crate::storage::{
    byte_range::ByteRange, Bytes, ListableStorage, ReadableStorage, ReadableStorageTraits,
    StorageError, StoreKey, WritableStorage,
};
use crate::{array::storage_transformer::StorageTransformerExtension, node::NodePath};

#[cfg(feature = "async")]
use crate::storage::{
    AsyncListableStorage, AsyncReadableStorage, AsyncReadableStorageTraits, AsyncWritableStorage,
};

use super::chunk_manifest::ChunkManifestValue;
use super::{ChunkManifest, ChunkManifestJsonConfiguration, IDENTIFIER};

#[derive(Debug)]
pub struct ChunkManifestJsonStorageTransformer {
    configuration: ChunkManifestJsonConfiguration,
    path: NodePath,
    chunk_manifest: std::sync::Mutex<Option<Arc<ChunkManifest>>>,
    #[cfg(feature = "async")]
    async_chunk_manifest: futures::lock::Mutex<Option<Arc<ChunkManifest>>>,
}

/// If the path starts with "./", strip it
fn strip_root_prefix(path: &str) -> String {
    path.strip_prefix("/").unwrap_or(path).to_string()
}

impl ChunkManifestJsonStorageTransformer {
    #[must_use]
    pub fn new(configuration: ChunkManifestJsonConfiguration, path: &NodePath) -> Self {
        Self {
            configuration,
            path: path.clone(),
            chunk_manifest: std::sync::Mutex::new(None),
            #[cfg(feature = "async")]
            async_chunk_manifest: futures::lock::Mutex::new(None),
        }
    }

    fn initialise(&self, storage: &ReadableStorage) -> Result<(), StorageError> {
        let mut out = self.chunk_manifest.lock().unwrap();
        if out.is_none() {
            let path = strip_root_prefix(
                &(format!(
                    "{}/{}",
                    self.path.as_str().to_string(),
                    self.configuration.manifest.to_string_lossy()
                )),
            );

            let key = StoreKey::new(path)?;
            let value = storage
                .get(&key)
                .map_err(|err| StorageError::Other(err.to_string()))?;
            let chunk_manifest = if let Some(value) = value {
                let value = core::str::from_utf8(value.as_ref())
                    .map_err(|err| StorageError::Other(err.to_string()))?;
                Arc::new(
                    serde_json::from_str(value)
                        .map_err(|err| StorageError::Other(err.to_string()))?,
                )
            } else {
                return Err(StorageError::Other(
                    "missing chunk manifest file".to_string(),
                ));
            };
            *out = Some(chunk_manifest);
        }
        Ok(())
    }

    #[cfg(feature = "async")]
    async fn async_initialise(&self, storage: AsyncReadableStorage) -> Result<(), StorageError> {
        let mut out = self.async_chunk_manifest.lock().await;
        if out.is_none() {
            let path = strip_root_prefix(
                &(format!(
                    "{}/{}",
                    self.path.as_str().to_string(),
                    self.configuration.manifest.to_string_lossy()
                )),
            );

            let key = StoreKey::new(path)?;
            let value = storage
                .get(&key)
                .await
                .map_err(|err| StorageError::Other(err.to_string()))?;
            let chunk_manifest = if let Some(value) = value {
                let value = core::str::from_utf8(value.as_ref())
                    .map_err(|err| StorageError::Other(err.to_string()))?;
                Arc::new(
                    serde_json::from_str(value)
                        .map_err(|err| StorageError::Other(err.to_string()))?,
                )
            } else {
                return Err(StorageError::Other(
                    "missing chunk manifest file".to_string(),
                ));
            };
            *out = Some(chunk_manifest);
        }
        Ok(())
    }

    fn create_transformer<TStorage: ?Sized>(
        &self,
        storage: Arc<TStorage>,
    ) -> Arc<ChunkManifestJsonImplStorageTransformer<TStorage>> {
        Arc::new(ChunkManifestJsonImplStorageTransformer {
            storage,
            path: self.path.clone(),
            chunk_manifest: self
                .chunk_manifest
                .lock()
                .unwrap()
                .as_ref()
                .expect("storage transformer must have initialise called")
                .clone(),
        })
    }
}

#[cfg_attr(feature = "async", async_trait::async_trait)]
impl StorageTransformerExtension for ChunkManifestJsonStorageTransformer {
    fn create_metadata(&self) -> MetadataV3 {
        MetadataV3::new_with_serializable_configuration(IDENTIFIER, &self.configuration).unwrap()
    }

    fn create_readable_transformer(
        self: Arc<Self>,
        storage: ReadableStorage,
    ) -> Result<ReadableStorage, StorageError> {
        self.initialise(&storage)?;
        let result: ReadableStorage = self.create_transformer(storage);
        Ok(result)
    }

    fn create_writable_transformer(
        self: Arc<Self>,
        _storage: WritableStorage,
    ) -> Result<WritableStorage, StorageError> {
        Err(StorageError::Unsupported(
            "chunk-manifest-json does not support writing".to_string(),
        ))
    }

    fn create_listable_transformer(
        self: Arc<Self>,
        _storage: ListableStorage,
    ) -> Result<ListableStorage, StorageError> {
        Err(StorageError::Unsupported(
            "chunk-manifest-json does not support listing".to_string(),
        ))
    }

    #[cfg(feature = "async")]
    async fn create_async_readable_transformer(
        self: Arc<Self>,
        storage: AsyncReadableStorage,
    ) -> Result<AsyncReadableStorage, StorageError> {
        self.async_initialise(storage.clone()).await?;
        let result: AsyncReadableStorage = self.create_transformer(storage);
        Ok(result)
    }

    #[cfg(feature = "async")]
    async fn create_async_writable_transformer(
        self: Arc<Self>,
        _storage: AsyncWritableStorage,
    ) -> Result<AsyncWritableStorage, StorageError> {
        Err(StorageError::Unsupported(
            "chunk-manifest-json does not support writing".to_string(),
        ))
    }

    #[cfg(feature = "async")]
    async fn create_async_listable_transformer(
        self: Arc<Self>,
        _storage: AsyncListableStorage,
    ) -> Result<AsyncListableStorage, StorageError> {
        Err(StorageError::Unsupported(
            "chunk-manifest-json storage transformer does not support listing".to_string(),
        ))
    }
}

#[derive(Debug)]
struct ChunkManifestJsonImplStorageTransformer<TStorage: ?Sized> {
    storage: Arc<TStorage>,
    path: NodePath,
    chunk_manifest: Arc<ChunkManifest>,
}

impl<TStorage: ?Sized> ChunkManifestJsonImplStorageTransformer<TStorage> {
    fn key_to_manifest_value(&self, key: &StoreKey) -> Option<&ChunkManifestValue> {
        let root = strip_root_prefix(self.path.as_str());
        let relative_key = key
            .as_str()
            .strip_prefix(&root)
            .expect("key should be relative to root")
            .strip_prefix('/')
            .expect("no leading / in chunks keys");
        self.chunk_manifest.get(relative_key)
    }
}

impl<TStorage: ?Sized + ReadableStorageTraits> ReadableStorageTraits
    for ChunkManifestJsonImplStorageTransformer<TStorage>
{
    fn get_partial_values_key(
        &self,
        key: &StoreKey,
        byte_ranges: &[crate::byte_range::ByteRange],
    ) -> Result<Option<Vec<Bytes>>, StorageError> {
        let manifest_value = self.key_to_manifest_value(key);
        if let Some(manifest_value) = manifest_value {
            let key = StoreKey::new(manifest_value.path.to_string_lossy()).unwrap(); // FIXME
            let bytes_ranges_offset = byte_ranges
                .iter()
                .map(|byte_range| match byte_range {
                    ByteRange::FromStart(offset, None) => ByteRange::FromStart(
                        manifest_value.offset + offset,
                        Some(manifest_value.length),
                    ),
                    ByteRange::FromStart(offset, Some(length)) => {
                        ByteRange::FromStart(manifest_value.offset + offset, Some(*length))
                    }
                    ByteRange::FromEnd(offset, None) => ByteRange::FromStart(
                        manifest_value.offset,
                        Some(manifest_value.length - *offset),
                    ),
                    ByteRange::FromEnd(offset, Some(length)) => ByteRange::FromEnd(
                        manifest_value.offset + manifest_value.length - offset - *length,
                        Some(*length),
                    ),
                })
                .collect::<Vec<_>>();
            self.storage
                .get_partial_values_key(&key, &bytes_ranges_offset)
        } else {
            Ok(None)
        }
    }

    fn size_key(&self, key: &StoreKey) -> Result<Option<u64>, StorageError> {
        let manifest_value = self.key_to_manifest_value(key);
        if let Some(manifest_value) = manifest_value {
            Ok(Some(manifest_value.length))
        } else {
            Ok(None)
        }
    }
}

#[cfg(feature = "async")]
#[async_trait::async_trait]
impl<TStorage: ?Sized + AsyncReadableStorageTraits> AsyncReadableStorageTraits
    for ChunkManifestJsonImplStorageTransformer<TStorage>
{
    async fn get_partial_values_key(
        &self,
        key: &StoreKey,
        byte_ranges: &[crate::byte_range::ByteRange],
    ) -> Result<Option<Vec<Bytes>>, StorageError> {
        let manifest_value = self.key_to_manifest_value(key);
        if let Some(manifest_value) = manifest_value {
            let key = StoreKey::new(manifest_value.path.to_string_lossy())?;
            let bytes_ranges_offset = byte_ranges
                .iter()
                .map(|byte_range| match byte_range {
                    ByteRange::FromStart(offset, None) => ByteRange::FromStart(
                        manifest_value.offset + offset,
                        Some(manifest_value.length),
                    ),
                    ByteRange::FromStart(offset, Some(length)) => {
                        ByteRange::FromStart(manifest_value.offset + offset, Some(*length))
                    }
                    ByteRange::FromEnd(offset, None) => ByteRange::FromStart(
                        manifest_value.offset,
                        Some(manifest_value.length - *offset),
                    ),
                    ByteRange::FromEnd(offset, Some(length)) => ByteRange::FromEnd(
                        manifest_value.offset + manifest_value.length - offset - *length,
                        Some(*length),
                    ),
                })
                .collect::<Vec<_>>();
            self.storage
                .get_partial_values_key(&key, &bytes_ranges_offset)
                .await
        } else {
            Ok(None)
        }
    }

    async fn size_key(&self, key: &StoreKey) -> Result<Option<u64>, StorageError> {
        let manifest_value = self.key_to_manifest_value(key);
        if let Some(manifest_value) = manifest_value {
            Ok(Some(manifest_value.length))
        } else {
            Ok(None)
        }
    }
}
