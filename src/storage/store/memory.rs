//! An in-memory store.

use parking_lot::RwLock;

use crate::{
    array::MaybeBytes,
    byte_range::{ByteOffset, ByteRange},
    storage::{
        ListableStorageTraits, ReadableStorageTraits, ReadableWritableStorageTraits, StorageError,
        StoreKeyRange, StoreKeyStartValue, StoreKeysPrefixes, WritableStorageTraits,
    },
};

use std::collections::{BTreeMap, BTreeSet};

use super::{
    ReadableStoreExtension, StoreExtension, StoreKey, StoreKeys, StorePrefix,
    WritableStoreExtension,
};

/// An in-memory store.
#[derive(Debug)]
pub struct MemoryStore {
    data_map: RwLock<BTreeMap<StoreKey, RwLock<Vec<u8>>>>,
}

impl MemoryStore {
    /// Create a new memory store at a given `base_directory`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            data_map: RwLock::new(BTreeMap::default()),
        }
    }
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

impl StoreExtension for MemoryStore {}

impl ReadableStoreExtension for MemoryStore {}

impl WritableStoreExtension for MemoryStore {}

impl MemoryStore {
    fn set_impl(&self, key: &StoreKey, value: &[u8], offset: Option<ByteOffset>, _truncate: bool) {
        let mut data_map_read = self.data_map.read();
        if !data_map_read.contains_key(key) {
            drop(data_map_read);
            let mut data_map_write = self.data_map.write();
            data_map_write.entry(key.clone()).or_default();
            drop(data_map_write);
            data_map_read = self.data_map.read();
        }
        let mut data = data_map_read.get(key).unwrap().write();

        let offset = offset.unwrap_or(0);

        if offset == 0 && data.is_empty() {
            // fast path
            *data = value.to_vec();
        } else {
            let length = usize::try_from(offset + value.len() as u64).unwrap();
            if data.len() < length {
                data.resize(length, 0);
            }
            let offset = usize::try_from(offset).unwrap();
            data[offset..offset + value.len()].copy_from_slice(value);
        }
    }
}

impl ReadableStorageTraits for MemoryStore {
    fn get(&self, key: &StoreKey) -> Result<MaybeBytes, StorageError> {
        let data_map = self.data_map.read();
        if let Some(data) = data_map.get(key) {
            let data = data.read();
            Ok(Some(data.clone()))
        } else {
            Ok(None)
        }
    }

    fn get_partial_values_key(
        &self,
        key: &StoreKey,
        byte_ranges: &[ByteRange],
    ) -> Result<Option<Vec<Vec<u8>>>, StorageError> {
        let data_map = self.data_map.read();
        if let Some(data) = data_map.get(key) {
            let data = data.read();
            let mut out = Vec::with_capacity(byte_ranges.len());
            for byte_range in byte_ranges {
                let start = usize::try_from(byte_range.start(data.len() as u64)).unwrap();
                let end = usize::try_from(byte_range.end(data.len() as u64)).unwrap();
                let bytes = data[start..end].to_vec();
                out.push(bytes);
            }
            Ok(Some(out))
        } else {
            Ok(None)
        }
    }

    fn get_partial_values(
        &self,
        key_ranges: &[StoreKeyRange],
    ) -> Result<Vec<MaybeBytes>, StorageError> {
        self.get_partial_values_batched_by_key(key_ranges)
    }

    fn size(&self) -> Result<u64, StorageError> {
        let mut out: u64 = 0;
        let data_map = self.data_map.read();
        for values in data_map.values() {
            out += values.read().len() as u64;
        }
        Ok(out)
    }

    fn size_key(&self, key: &StoreKey) -> Result<Option<u64>, StorageError> {
        let data_map = self.data_map.read();
        if let Some(entry) = data_map.get(key) {
            Ok(Some(entry.read().len() as u64))
        } else {
            Ok(None)
        }
    }
}

impl WritableStorageTraits for MemoryStore {
    fn set(&self, key: &StoreKey, value: &[u8]) -> Result<(), StorageError> {
        MemoryStore::set_impl(self, key, value, None, true);
        Ok(())
    }

    fn set_partial_values(
        &self,
        key_start_values: &[StoreKeyStartValue],
    ) -> Result<(), StorageError> {
        for key_start_value in key_start_values {
            MemoryStore::set_impl(
                self,
                &key_start_value.key,
                key_start_value.value,
                Some(key_start_value.start),
                false,
            );
        }
        Ok(())
    }

    fn erase(&self, key: &StoreKey) -> Result<bool, StorageError> {
        let mut data_map = self.data_map.write();
        Ok(data_map.remove(key).is_some())
    }

    fn erase_prefix(&self, prefix: &StorePrefix) -> Result<bool, StorageError> {
        let mut data_map = self.data_map.write();
        let keys: Vec<StoreKey> = data_map.keys().cloned().collect();
        let mut any_deletions = false;
        for key in keys {
            if key.has_prefix(prefix) {
                data_map.remove(&key);
                any_deletions = true;
            }
        }
        Ok(any_deletions)
    }
}

impl ListableStorageTraits for MemoryStore {
    fn list(&self) -> Result<StoreKeys, StorageError> {
        let data_map = self.data_map.read();
        Ok(data_map.keys().cloned().collect())
    }

    fn list_prefix(&self, prefix: &StorePrefix) -> Result<StoreKeys, StorageError> {
        let data_map = self.data_map.read();
        Ok(data_map
            .keys()
            .filter(|&key| key.has_prefix(prefix))
            .cloned()
            .collect())
    }

    fn list_dir(&self, prefix: &StorePrefix) -> Result<StoreKeysPrefixes, StorageError> {
        let mut keys: StoreKeys = vec![];
        let mut prefixes: BTreeSet<StorePrefix> = BTreeSet::default();
        let data_map = self.data_map.read();
        for key in data_map.keys() {
            if key.has_prefix(prefix) {
                let key_strip = key.as_str().strip_prefix(prefix.as_str()).unwrap();
                let key_strip = key_strip.strip_prefix('/').unwrap_or(key_strip);
                let components: Vec<_> = key_strip.split('/').collect();
                if components.len() > 1 {
                    prefixes.insert(StorePrefix::new(
                        &(prefix.as_str().to_string() + components[0] + "/"),
                    )?);
                } else if let Some(parent) = key.parent() {
                    if parent.eq(prefix) {
                        keys.push(key.clone());
                    }
                }
            }
        }
        let prefixes: Vec<StorePrefix> = prefixes.iter().cloned().collect();
        Ok(StoreKeysPrefixes { keys, prefixes })
    }
}

impl ReadableWritableStorageTraits for MemoryStore {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;

    #[test]
    fn memory_set() -> Result<(), Box<dyn Error>> {
        let store = MemoryStore::new();
        let key = "a/b".try_into()?;
        store.set(&key, &[0, 1, 2])?;
        assert_eq!(store.get(&key)?.unwrap(), &[0, 1, 2]);
        store.set_partial_values(&[StoreKeyStartValue::new(key.clone(), 1, &[3, 4])])?;
        assert_eq!(store.get(&key)?.unwrap(), &[0, 3, 4]);
        Ok(())
    }

    #[test]
    fn memory_list() -> Result<(), Box<dyn Error>> {
        let store = MemoryStore::new();

        store.set(&"a/b".try_into()?, &[])?;
        store.set(&"a/c".try_into()?, &[])?;
        store.set(&"a/d/e".try_into()?, &[])?;
        store.set(&"a/d/f".try_into()?, &[])?;
        store.erase(&"a/d/e".try_into()?)?;
        assert_eq!(
            store.list()?,
            &["a/b".try_into()?, "a/c".try_into()?, "a/d/f".try_into()?]
        );
        assert_eq!(
            store.list_prefix(&"a/".try_into()?)?,
            &["a/b".try_into()?, "a/c".try_into()?, "a/d/f".try_into()?]
        );
        assert_eq!(
            store.list_prefix(&"a/d/".try_into()?)?,
            &["a/d/f".try_into()?]
        );
        assert_eq!(
            store.list_prefix(&"".try_into()?)?,
            &["a/b".try_into()?, "a/c".try_into()?, "a/d/f".try_into()?]
        );
        Ok(())
    }

    #[test]
    fn memory_list_dir() -> Result<(), Box<dyn Error>> {
        let store = MemoryStore::new();
        store.set(&"a/b".try_into()?, &[])?;
        store.set(&"a/c".try_into()?, &[])?;
        store.set(&"a/d/e".try_into()?, &[])?;
        store.set(&"a/f/g".try_into()?, &[])?;
        store.set(&"a/f/h".try_into()?, &[])?;
        store.set(&"b/c/d".try_into()?, &[])?;

        let list_dir = store.list_dir(&"a/".try_into()?)?;

        assert_eq!(list_dir.keys(), &["a/b".try_into()?, "a/c".try_into()?,]);
        assert_eq!(
            list_dir.prefixes(),
            &["a/d/".try_into()?, "a/f/".try_into()?,]
        );
        Ok(())
    }
}
