use opendal::BlockingOperator;

use zarrs_storage::{
    byte_range::{ByteRange, InvalidByteRangeError},
    Bytes, ListableStorageTraits, MaybeBytes, ReadableStorageTraits, StorageError, StoreKey,
    StoreKeyOffsetValue, StoreKeys, StoreKeysPrefixes, StorePrefix, WritableStorageTraits,
};

use crate::{handle_result, handle_result_notfound};

/// An asynchronous store backed by an [`opendal::BlockingOperator`].
pub struct OpendalStore {
    operator: BlockingOperator,
    // locks: StoreLocks,
}

impl OpendalStore {
    /// Create a new [`OpendalStore`].
    #[must_use]
    pub fn new(operator: BlockingOperator) -> Self {
        Self { operator }
    }
}

#[async_trait::async_trait]
impl ReadableStorageTraits for OpendalStore {
    fn get(&self, key: &StoreKey) -> Result<MaybeBytes, StorageError> {
        handle_result_notfound(self.operator.read(key.as_str()).map(|buf| buf.to_bytes()))
    }

    fn get_partial_values_key(
        &self,
        key: &StoreKey,
        byte_ranges: &[ByteRange],
    ) -> Result<Option<Vec<Bytes>>, StorageError> {
        // TODO: Get OpenDAL to return an error if byte range is OOB instead of panic
        let size = self.size_key(key)?;
        if let Some(size) = size {
            let reader = handle_result(self.operator.reader(key.as_str()))?;
            let mut bytes = Vec::with_capacity(byte_ranges.len());
            for byte_range in byte_ranges {
                let byte_range_opendal = byte_range.to_range(size);
                if byte_range_opendal.end > size {
                    return Err(InvalidByteRangeError::new(*byte_range, size).into());
                }
                bytes.push(handle_result(reader.read(byte_range_opendal))?.to_bytes());
            }
            Ok(Some(bytes))
        } else {
            Ok(None)
        }
    }

    fn size_key(&self, key: &StoreKey) -> Result<Option<u64>, StorageError> {
        Ok(handle_result_notfound(self.operator.stat(key.as_str()))?
            .map(|metadata| metadata.content_length()))
    }
}

#[async_trait::async_trait]
impl WritableStorageTraits for OpendalStore {
    fn set(&self, key: &StoreKey, value: Bytes) -> Result<(), StorageError> {
        handle_result(self.operator.write(key.as_str(), value))
    }

    fn set_partial_values(
        &self,
        key_offset_values: &[StoreKeyOffsetValue],
    ) -> Result<(), StorageError> {
        zarrs_storage::store_set_partial_values(self, key_offset_values)
    }

    fn erase(&self, key: &StoreKey) -> Result<(), StorageError> {
        handle_result(self.operator.remove(vec![key.to_string()]))
    }

    fn erase_prefix(&self, prefix: &StorePrefix) -> Result<(), StorageError> {
        handle_result(self.operator.remove_all(prefix.as_str()))
    }
}

#[async_trait::async_trait]
impl ListableStorageTraits for OpendalStore {
    fn list(&self) -> Result<StoreKeys, StorageError> {
        self.list_prefix(&StorePrefix::root())
    }

    fn list_prefix(&self, prefix: &StorePrefix) -> Result<StoreKeys, StorageError> {
        handle_result_notfound(
            self.operator
                .list_with(prefix.as_str())
                .recursive(true)
                .call(),
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

    fn list_dir(&self, prefix: &StorePrefix) -> Result<StoreKeysPrefixes, StorageError> {
        handle_result_notfound(
            self.operator
                .list_with(prefix.as_str())
                .recursive(false)
                .call(),
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
                                prefixes.push(prefix_entry);
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

    fn size_prefix(&self, prefix: &StorePrefix) -> Result<u64, StorageError> {
        handle_result_notfound(
            self.operator
                .list_with(prefix.as_str())
                .recursive(true)
                .metakey(opendal::Metakey::ContentLength)
                .call(),
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
    use opendal::Operator;

    use super::*;
    use std::error::Error;

    #[test]
    fn memory() -> Result<(), Box<dyn Error>> {
        let builder = opendal::services::Memory::default();
        let op = Operator::new(builder)?.finish().blocking();
        let store = OpendalStore::new(op);
        zarrs_storage::store_test::store_write(&store)?;
        zarrs_storage::store_test::store_read(&store)?;
        zarrs_storage::store_test::store_list(&store)?;
        Ok(())
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn filesystem() -> Result<(), Box<dyn Error>> {
        let path = tempfile::TempDir::new()?;
        let builder = opendal::services::Fs::default().root(&path.path().to_string_lossy());
        let op = Operator::new(builder)?.finish().blocking();
        let store = OpendalStore::new(op);
        zarrs_storage::store_test::store_write(&store)?;
        zarrs_storage::store_test::store_read(&store)?;
        zarrs_storage::store_test::store_list(&store)?;
        Ok(())
    }
}
