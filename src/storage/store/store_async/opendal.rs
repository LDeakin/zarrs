use opendal::Operator;

use crate::{
    byte_range::{ByteRange, InvalidByteRangeError},
    storage::{
        AsyncBytes, AsyncListableStorageTraits, AsyncReadableStorageTraits,
        AsyncReadableWritableStorageTraits, AsyncWritableStorageTraits, MaybeAsyncBytes,
        StorageError, StoreKey, StoreKeyStartValue, StoreKeys, StoreKeysPrefixes, StorePrefix,
    },
};

/// An asynchronous store backed by an [`opendal::Operator`].
pub struct AsyncOpendalStore {
    operator: Operator,
    // locks: AsyncStoreLocks,
}

impl AsyncOpendalStore {
    /// Create a new [`AsyncOpendalStore`].
    #[must_use]
    pub fn new(operator: Operator) -> Self {
        Self { operator }
        // Self::new_with_locks(operator, Arc::new(AsyncDefaultStoreLocks::default()))
    }

    // /// Create a new [`AsyncOpendalStore`] with non-default store locks.
    // #[must_use]
    // pub fn new_with_locks(operator: Operator, store_locks: AsyncStoreLocks) -> Self {
    //     Self {
    //         operator,
    //         locks: store_locks,
    //     }
    // }
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
    async fn get(&self, key: &StoreKey) -> Result<MaybeAsyncBytes, StorageError> {
        handle_result(
            self.operator
                .read(key.as_str())
                .await
                .map(|buf| buf.to_bytes()),
        )
    }

    async fn get_partial_values_key(
        &self,
        key: &StoreKey,
        byte_ranges: &[ByteRange],
    ) -> Result<Option<Vec<AsyncBytes>>, StorageError> {
        // TODO: Get OpenDAL to return an error if byte range is OOB instead of panic, then don't need to query size
        let (size, reader) = futures::join!(self.size_key(key), self.operator.reader(key.as_str()));
        if let (Some(size), Some(reader)) = (size?, handle_result(reader)?) {
            let mut byte_ranges_fetch = Vec::with_capacity(byte_ranges.len());
            for byte_range in byte_ranges {
                let byte_range_opendal = byte_range.to_range(size);
                if byte_range_opendal.end > size {
                    return Err(InvalidByteRangeError::new(*byte_range, size).into());
                }
                byte_ranges_fetch.push(byte_range_opendal);
            }
            Ok(Some(
                reader
                    .fetch(byte_ranges_fetch)
                    .await?
                    .into_iter()
                    .map(|buf| buf.to_bytes())
                    .collect(),
            ))
        } else {
            Ok(None)
        }
    }

    async fn size_key(&self, key: &StoreKey) -> Result<Option<u64>, StorageError> {
        Ok(handle_result(self.operator.stat(key.as_str()).await)?
            .map(|metadata| metadata.content_length()))
    }
}

#[async_trait::async_trait]
impl AsyncWritableStorageTraits for AsyncOpendalStore {
    async fn set(&self, key: &StoreKey, value: AsyncBytes) -> Result<(), StorageError> {
        Ok(self.operator.write(key.as_str(), value).await?)
    }

    async fn set_partial_values(
        &self,
        key_start_values: &[StoreKeyStartValue],
    ) -> Result<(), StorageError> {
        crate::storage::async_store_set_partial_values(self, key_start_values).await
    }

    async fn erase(&self, key: &StoreKey) -> Result<(), StorageError> {
        Ok(self.operator.remove(vec![key.to_string()]).await?)
    }

    async fn erase_prefix(&self, prefix: &StorePrefix) -> Result<(), StorageError> {
        Ok(self.operator.remove_all(prefix.as_str()).await?)
    }
}

#[async_trait::async_trait]
impl AsyncReadableWritableStorageTraits for AsyncOpendalStore {
    // async fn mutex(&self, key: &StoreKey) -> Result<AsyncStoreKeyMutex, StorageError> {
    //     Ok(self.locks.mutex(key).await)
    // }
}

#[async_trait::async_trait]
impl AsyncListableStorageTraits for AsyncOpendalStore {
    async fn list(&self) -> Result<StoreKeys, StorageError> {
        self.list_prefix(&StorePrefix::root()).await
    }

    async fn list_prefix(&self, prefix: &StorePrefix) -> Result<StoreKeys, StorageError> {
        handle_result(
            self.operator
                .list_with(prefix.as_str())
                .recursive(true)
                .await,
        )?
        .map_or_else(
            || Ok(vec![]),
            |list_with_prefix| {
                let mut list = list_with_prefix
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
            },
        )
    }

    async fn list_dir(&self, prefix: &StorePrefix) -> Result<StoreKeysPrefixes, StorageError> {
        handle_result(
            self.operator
                .list_with(prefix.as_str())
                .recursive(false)
                .await,
        )?
        .map_or_else(
            || {
                Ok(StoreKeysPrefixes {
                    keys: vec![],
                    prefixes: vec![],
                })
            },
            |entries| {
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
            },
        )
    }

    async fn size_prefix(&self, prefix: &StorePrefix) -> Result<u64, StorageError> {
        handle_result(
            self.operator
                .list_with(prefix.as_str())
                .recursive(true)
                .metakey(opendal::Metakey::ContentLength)
                .await,
        )?
        .map_or_else(
            || Ok(0),
            |list| {
                let size = list
                    .into_iter()
                    .map(|entry| entry.metadata().content_length())
                    .sum::<u64>();
                Ok(size)
            },
        )
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
        let builder = opendal::services::Fs::default().root(&path.path().to_string_lossy());
        let op = Operator::new(builder)?.finish();
        let store = AsyncOpendalStore::new(op);
        super::super::test_util::store_write(&store).await?;
        super::super::test_util::store_read(&store).await?;
        super::super::test_util::store_list(&store).await?;
        Ok(())
    }
}
