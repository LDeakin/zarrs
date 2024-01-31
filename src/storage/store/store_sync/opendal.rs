use std::sync::Arc;

use opendal::BlockingOperator;

use crate::{
    array::MaybeBytes,
    byte_range::ByteRange,
    storage::{
        store_lock::{DefaultStoreLocks, StoreKeyMutex, StoreLocks},
        ListableStorageTraits, ReadableStorageTraits, ReadableWritableStorageTraits, StorageError,
        StoreKey, StoreKeyRange, StoreKeyStartValue, StoreKeys, StoreKeysPrefixes, StorePrefix,
        WritableStorageTraits,
    },
};

/// An asynchronous store backed by a [`BlockingOperator`].
pub struct OpendalStore {
    operator: BlockingOperator,
    locks: StoreLocks,
}

impl OpendalStore {
    /// Create a new [`OpendalStore`].
    #[must_use]
    pub fn new(operator: BlockingOperator) -> Self {
        Self::new_with_locks(operator, Arc::new(DefaultStoreLocks::default()))
    }

    /// Create a new [`OpendalStore`] with non-default store locks.
    #[must_use]
    pub fn new_with_locks(operator: BlockingOperator, store_locks: StoreLocks) -> Self {
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
impl ReadableStorageTraits for OpendalStore {
    fn get(&self, key: &StoreKey) -> Result<MaybeBytes, StorageError> {
        handle_result(self.operator.read(key.as_str()))
    }

    fn get_partial_values_key(
        &self,
        key: &StoreKey,
        byte_ranges: &[ByteRange],
    ) -> Result<Option<Vec<Vec<u8>>>, StorageError> {
        // FIXME: Does opendal offer a better way of retrieving multiple byte ranges?
        // FIXME: Coalesce like object_store?
        if byte_ranges
            .iter()
            .all(|byte_range| matches!(byte_range, ByteRange::FromEnd(_, _)))
        {
            let bytes = byte_ranges
                .iter()
                .map(|byte_range| {
                    self.operator
                        .read_with(key.as_str())
                        .range(byte_range.offset()..)
                        .call()
                })
                .collect::<Result<Vec<_>, _>>();
            handle_result(bytes)
        } else {
            let size = self
                .size_key(key)?
                .ok_or(StorageError::UnknownKeySize(key.clone()))?;
            let bytes = byte_ranges
                .iter()
                .map(|byte_range| {
                    let start = byte_range.start(size);
                    let end = byte_range.end(size);
                    match self
                        .operator
                        .read_with(key.as_str())
                        .range(start..end)
                        .call()
                    {
                        Ok(bytes) => {
                            if (end - start) == bytes.len() as u64 {
                                Ok(bytes)
                            } else {
                                Err(opendal::Error::new(
                                    opendal::ErrorKind::InvalidInput,
                                    "InvalidByteRangeError",
                                ))
                            }
                        }
                        Err(err) => Err(err),
                    }
                })
                .collect::<Result<Vec<_>, _>>();
            handle_result(bytes)
        }
    }

    fn get_partial_values(
        &self,
        key_ranges: &[StoreKeyRange],
    ) -> Result<Vec<MaybeBytes>, StorageError> {
        self.get_partial_values_batched_by_key(key_ranges)
    }

    fn size_prefix(&self, prefix: &StorePrefix) -> Result<u64, StorageError> {
        let list = self
            .operator
            .list_with(prefix.as_str())
            .recursive(true)
            .metakey(opendal::Metakey::ContentLength)
            .call()?;
        let size = list
            .into_iter()
            .map(|entry| entry.metadata().content_length())
            .sum::<u64>();
        Ok(size)
    }

    fn size_key(&self, key: &StoreKey) -> Result<Option<u64>, StorageError> {
        Ok(handle_result(self.operator.stat(key.as_str()))?
            .map(|metadata| metadata.content_length()))
    }

    fn size(&self) -> Result<u64, StorageError> {
        self.size_prefix(&StorePrefix::root())
    }
}

#[async_trait::async_trait]
impl WritableStorageTraits for OpendalStore {
    fn set(&self, key: &StoreKey, value: &[u8]) -> Result<(), StorageError> {
        // FIXME: Can this copy be avoided?
        let bytes = bytes::Bytes::copy_from_slice(value);
        Ok(self.operator.write(key.as_str(), bytes)?)
    }

    fn set_partial_values(
        &self,
        key_start_values: &[StoreKeyStartValue],
    ) -> Result<(), StorageError> {
        crate::storage::store_set_partial_values(self, key_start_values)
    }

    fn erase(&self, key: &StoreKey) -> Result<(), StorageError> {
        self.operator.remove(vec![key.to_string()])?;
        Ok(())
    }

    fn erase_prefix(&self, prefix: &StorePrefix) -> Result<(), StorageError> {
        self.operator.remove_all(prefix.as_str())?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl ReadableWritableStorageTraits for OpendalStore {
    fn mutex(&self, key: &StoreKey) -> Result<StoreKeyMutex, StorageError> {
        Ok(self.locks.mutex(key))
    }
}

#[async_trait::async_trait]
impl ListableStorageTraits for OpendalStore {
    fn list(&self) -> Result<StoreKeys, StorageError> {
        self.list_prefix(&StorePrefix::root())
    }

    fn list_prefix(&self, prefix: &StorePrefix) -> Result<StoreKeys, StorageError> {
        let mut list = self
            .operator
            .list_with(prefix.as_str())
            .recursive(true)
            .call()?
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

    fn list_dir(&self, prefix: &StorePrefix) -> Result<StoreKeysPrefixes, StorageError> {
        let entries = self
            .operator
            .list_with(prefix.as_str())
            .recursive(false)
            .call()?;
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
    use opendal::Operator;

    use super::*;
    use std::error::Error;

    #[test]
    fn memory() -> Result<(), Box<dyn Error>> {
        let builder = opendal::services::Memory::default();
        let op = Operator::new(builder)?.finish().blocking();
        let store = OpendalStore::new(op);
        super::super::test_util::store_write(&store)?;
        super::super::test_util::store_read(&store)?;
        super::super::test_util::store_list(&store)?;
        Ok(())
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn filesystem() -> Result<(), Box<dyn Error>> {
        let path = tempfile::TempDir::new()?;
        let mut builder = opendal::services::Fs::default();
        builder.root(&path.path().to_string_lossy());
        let op = Operator::new(builder)?.finish().blocking();
        let store = OpendalStore::new(op);
        super::super::test_util::store_write(&store)?;
        super::super::test_util::store_read(&store)?;
        super::super::test_util::store_list(&store)?;
        Ok(())
    }
}
