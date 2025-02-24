use opendal::Operator;

use zarrs_storage::{
    byte_range::{ByteRange, InvalidByteRangeError},
    AsyncBytes, AsyncListableStorageTraits, AsyncReadableStorageTraits, AsyncWritableStorageTraits,
    MaybeAsyncBytes, StorageError, StoreKey, StoreKeyOffsetValue, StoreKeys, StoreKeysPrefixes,
    StorePrefix,
};

use crate::{handle_result, handle_result_notfound};

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
    }
}

#[async_trait::async_trait]
impl AsyncReadableStorageTraits for AsyncOpendalStore {
    async fn get(&self, key: &StoreKey) -> Result<MaybeAsyncBytes, StorageError> {
        handle_result_notfound(
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
        if let (Some(size), Some(reader)) = (size?, handle_result_notfound(reader)?) {
            let mut byte_ranges_fetch = Vec::with_capacity(byte_ranges.len());
            for byte_range in byte_ranges {
                let byte_range_opendal = byte_range.to_range(size);
                if byte_range_opendal.end > size {
                    return Err(InvalidByteRangeError::new(*byte_range, size).into());
                }
                byte_ranges_fetch.push(byte_range_opendal);
            }
            Ok(Some(
                handle_result(reader.fetch(byte_ranges_fetch).await)?
                    .into_iter()
                    .map(|buf| buf.to_bytes())
                    .collect(),
            ))
        } else {
            Ok(None)
        }
    }

    async fn size_key(&self, key: &StoreKey) -> Result<Option<u64>, StorageError> {
        Ok(
            handle_result_notfound(self.operator.stat(key.as_str()).await)?
                .map(|metadata| metadata.content_length()),
        )
    }
}

#[async_trait::async_trait]
impl AsyncWritableStorageTraits for AsyncOpendalStore {
    async fn set(&self, key: &StoreKey, value: AsyncBytes) -> Result<(), StorageError> {
        handle_result(self.operator.write(key.as_str(), value).await)?;
        Ok(())
    }

    async fn set_partial_values(
        &self,
        key_offset_values: &[StoreKeyOffsetValue],
    ) -> Result<(), StorageError> {
        zarrs_storage::async_store_set_partial_values(self, key_offset_values).await
    }

    async fn erase(&self, key: &StoreKey) -> Result<(), StorageError> {
        handle_result(self.operator.delete(key.as_str()).await)
    }

    async fn erase_prefix(&self, prefix: &StorePrefix) -> Result<(), StorageError> {
        handle_result(self.operator.remove_all(prefix.as_str()).await)
    }
}

#[async_trait::async_trait]
impl AsyncListableStorageTraits for AsyncOpendalStore {
    async fn list(&self) -> Result<StoreKeys, StorageError> {
        self.list_prefix(&StorePrefix::root()).await
    }

    async fn list_prefix(&self, prefix: &StorePrefix) -> Result<StoreKeys, StorageError> {
        handle_result_notfound(
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
        handle_result_notfound(
            self.operator
                .list_with(prefix.as_str())
                .recursive(false)
                .await,
        )?
        .map_or_else(
            || Ok(StoreKeysPrefixes::new(vec![], vec![])),
            |entries| {
                let mut prefixes = Vec::<StorePrefix>::with_capacity(entries.len());
                let mut keys = Vec::<StoreKey>::with_capacity(entries.len());
                for entry in entries {
                    match entry.metadata().mode() {
                        opendal::EntryMode::FILE => {
                            keys.push(StoreKey::try_from(entry.path())?);
                        }
                        opendal::EntryMode::DIR => {
                            let prefix_entry = StorePrefix::try_from(entry.path())?;
                            if &prefix_entry != prefix {
                                prefixes.push(StorePrefix::try_from(entry.path())?);
                            }
                        }
                        opendal::EntryMode::Unknown => {}
                    }
                }
                keys.sort();
                prefixes.sort();
                Ok(StoreKeysPrefixes::new(keys, prefixes))
            },
        )
    }

    async fn size_prefix(&self, prefix: &StorePrefix) -> Result<u64, StorageError> {
        let Some(files) = handle_result_notfound(
            self.operator
                .list_with(prefix.as_str())
                .recursive(true)
                .await,
        )?
        else {
            return Ok(0);
        };

        if self
            .operator
            .info()
            .full_capability()
            .list_has_content_length
        {
            let size = files
                .into_iter()
                .filter_map(|entry| {
                    if entry.metadata().is_file() {
                        Some(entry.metadata().content_length())
                    } else {
                        None
                    }
                })
                .sum::<u64>();
            Ok(size)
        } else {
            // TODO: concurrent
            let mut size = 0;
            for entry in files {
                let meta = handle_result(self.operator.stat(entry.path()).await)?;
                if meta.is_file() {
                    size += meta.content_length();
                }
            }
            Ok(size)
        }
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
        zarrs_storage::store_test::async_store_write(&store).await?;
        zarrs_storage::store_test::async_store_read(&store).await?;
        zarrs_storage::store_test::async_store_list(&store).await?;
        Ok(())
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)]
    async fn filesystem() -> Result<(), Box<dyn Error>> {
        let path = tempfile::TempDir::new()?;
        let builder = opendal::services::Fs::default().root(&path.path().to_string_lossy());
        let op = Operator::new(builder)?.finish();
        let store = AsyncOpendalStore::new(op);
        zarrs_storage::store_test::async_store_write(&store).await?;
        zarrs_storage::store_test::async_store_read(&store).await?;
        zarrs_storage::store_test::async_store_list(&store).await?;
        Ok(())
    }
}
