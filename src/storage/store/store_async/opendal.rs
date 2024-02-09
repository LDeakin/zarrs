use std::{future::IntoFuture, sync::Arc};

use opendal::Operator;

use crate::{
    array::MaybeBytes,
    byte_range::{ByteRange, InvalidByteRangeError},
    storage::{
        store_lock::{AsyncDefaultStoreLocks, AsyncStoreKeyMutex, AsyncStoreLocks},
        AsyncListableStorageTraits, AsyncReadableStorageTraits, AsyncReadableWritableStorageTraits,
        AsyncWritableStorageTraits, StorageError, StoreKey, StoreKeyRange, StoreKeyStartValue,
        StoreKeys, StoreKeysPrefixes, StorePrefix,
    },
};

/// An asynchronous store backed by an [`Operator`].
pub struct AsyncOpendalStore {
    operator: Operator,
    locks: AsyncStoreLocks,
}

impl AsyncOpendalStore {
    /// Create a new [`AsyncOpendalStore`].
    #[must_use]
    pub fn new(operator: Operator) -> Self {
        Self::new_with_locks(operator, Arc::new(AsyncDefaultStoreLocks::default()))
    }

    /// Create a new [`AsyncOpendalStore`] with non-default store locks.
    #[must_use]
    pub fn new_with_locks(operator: Operator, store_locks: AsyncStoreLocks) -> Self {
        Self {
            operator,
            locks: store_locks,
        }
    }
}

/// Map [`opendal::ErrorKind::NotFound`] to None, pass through other errors
fn handle_result<T>(result: Result<T, opendal::Error>) -> Result<Option<T>, StorageError> {
    match result {
        Ok(result) => Ok(Some(result)),
        Err(err) => {
            if err.kind() == opendal::ErrorKind::NotFound {
                Ok(None)
            } else {
                Err(err.into())
            }
        }
    }
}

#[async_trait::async_trait]
impl AsyncReadableStorageTraits for AsyncOpendalStore {
    async fn get(&self, key: &StoreKey) -> Result<MaybeBytes, StorageError> {
        handle_result(self.operator.read(key.as_str()).await)
    }

    async fn get_partial_values_key(
        &self,
        key: &StoreKey,
        byte_ranges: &[ByteRange],
    ) -> Result<Option<Vec<Vec<u8>>>, StorageError> {
        use futures::FutureExt;
        // FIXME: Does opendal offer a better way of retrieving multiple byte ranges?
        // FIXME: Coalesce like object_store?
        if byte_ranges
            .iter()
            .all(|byte_range| matches!(byte_range, ByteRange::FromEnd(_, _)))
        {
            let futures = byte_ranges
                .iter()
                .map(|byte_range| {
                    self.operator
                        .read_with(key.as_str())
                        .range(byte_range.offset()..)
                        .into_future()
                })
                .collect::<Vec<_>>();
            handle_result(futures::future::try_join_all(futures).await)
        } else {
            let size = self
                .size_key(key)
                .await?
                .ok_or(StorageError::UnknownKeySize(key.clone()))?;
            let futures = byte_ranges
                .iter()
                .map(|byte_range| {
                    let start = byte_range.start(size);
                    let end = byte_range.end(size);
                    self.operator
                        .read_with(key.as_str())
                        .range(start..end)
                        .into_future()
                        .map(move |bytes| match bytes {
                            Ok(bytes) => {
                                if (end - start) == bytes.len() as u64 {
                                    Ok(bytes)
                                } else {
                                    Err(InvalidByteRangeError::new(*byte_range, bytes.len() as u64)
                                        .into())
                                }
                            }
                            Err(err) => Err(StorageError::from(err.to_string())),
                        })
                })
                .collect::<Vec<_>>();
            futures::future::try_join_all(futures).await.map(Some)
        }
    }

    async fn get_partial_values(
        &self,
        key_ranges: &[StoreKeyRange],
    ) -> Result<Vec<MaybeBytes>, StorageError> {
        self.get_partial_values_batched_by_key(key_ranges).await
    }

    async fn size_prefix(&self, prefix: &StorePrefix) -> Result<u64, StorageError> {
        let list = self
            .operator
            .list_with(prefix.as_str())
            .recursive(true)
            .metakey(opendal::Metakey::ContentLength)
            .await?;
        let size = list
            .into_iter()
            .map(|entry| entry.metadata().content_length())
            .sum::<u64>();
        Ok(size)
    }

    async fn size_key(&self, key: &StoreKey) -> Result<Option<u64>, StorageError> {
        Ok(handle_result(self.operator.stat(key.as_str()).await)?
            .map(|metadata| metadata.content_length()))
    }

    async fn size(&self) -> Result<u64, StorageError> {
        self.size_prefix(&StorePrefix::root()).await
    }
}

#[async_trait::async_trait]
impl AsyncWritableStorageTraits for AsyncOpendalStore {
    async fn set(&self, key: &StoreKey, value: bytes::Bytes) -> Result<(), StorageError> {
        Ok(self.operator.write(key.as_str(), value).await?)
    }

    async fn set_partial_values(
        &self,
        key_start_values: &[StoreKeyStartValue],
    ) -> Result<(), StorageError> {
        crate::storage::async_store_set_partial_values(self, key_start_values).await
    }

    async fn erase(&self, key: &StoreKey) -> Result<(), StorageError> {
        self.operator.remove(vec![key.to_string()]).await?;
        Ok(())
    }

    async fn erase_prefix(&self, prefix: &StorePrefix) -> Result<(), StorageError> {
        self.operator.remove_all(prefix.as_str()).await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl AsyncReadableWritableStorageTraits for AsyncOpendalStore {
    async fn mutex(&self, key: &StoreKey) -> Result<AsyncStoreKeyMutex, StorageError> {
        Ok(self.locks.mutex(key).await)
    }
}

#[async_trait::async_trait]
impl AsyncListableStorageTraits for AsyncOpendalStore {
    async fn list(&self) -> Result<StoreKeys, StorageError> {
        self.list_prefix(&StorePrefix::root()).await
    }

    async fn list_prefix(&self, prefix: &StorePrefix) -> Result<StoreKeys, StorageError> {
        let mut list = self
            .operator
            .list_with(prefix.as_str())
            .recursive(true)
            .await?
            .into_iter()
            .filter_map(|entry| {
                if entry.metadata().mode() == opendal::EntryMode::FILE {
                    Some(StoreKey::try_from(entry.path()))
                } else {
                    None
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        list.sort();
        Ok(list)
    }

    async fn list_dir(&self, prefix: &StorePrefix) -> Result<StoreKeysPrefixes, StorageError> {
        let entries = self
            .operator
            .list_with(prefix.as_str())
            .recursive(false)
            .await?;
        let mut prefixes = Vec::<StorePrefix>::with_capacity(entries.len());
        let mut keys = Vec::<StoreKey>::with_capacity(entries.len());
        for entry in entries {
            match entry.metadata().mode() {
                opendal::EntryMode::FILE => {
                    keys.push(StoreKey::try_from(entry.path())?);
                }
                opendal::EntryMode::DIR => {
                    prefixes.push(StorePrefix::try_from(entry.path())?);
                }
                opendal::EntryMode::Unknown => {}
            }
        }
        keys.sort();
        prefixes.sort();
        Ok(StoreKeysPrefixes { keys, prefixes })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;

    #[tokio::test]
    async fn memory() -> Result<(), Box<dyn Error>> {
        let builder = opendal::services::Memory::default();
        let op = Operator::new(builder)?.finish();
        let store = AsyncOpendalStore::new(op);
        super::super::test_util::store_write(&store).await?;
        super::super::test_util::store_read(&store).await?;
        super::super::test_util::store_list(&store).await?;
        Ok(())
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)]
    async fn filesystem() -> Result<(), Box<dyn Error>> {
        let path = tempfile::TempDir::new()?;
        let mut builder = opendal::services::Fs::default();
        builder.root(&path.path().to_string_lossy());
        let op = Operator::new(builder)?.finish();
        let store = AsyncOpendalStore::new(op);
        super::super::test_util::store_write(&store).await?;
        super::super::test_util::store_read(&store).await?;
        super::super::test_util::store_list(&store).await?;
        Ok(())
    }
}
