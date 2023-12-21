//! An in-memory store.

use parking_lot::RwLock;
use std::sync::Mutex;

use crate::{
    array::MaybeBytes,
    byte_range::{ByteOffset, ByteRange},
    storage::{
        ListableStorageTraits, ReadableStorageTraits, StorageError, StoreKey, StoreKeyRange,
        StoreKeyStartValue, StoreKeys, StoreKeysPrefixes, StorePrefix, WritableStorageTraits,
    },
};

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

/// An in-memory store.
#[derive(Debug)]
pub struct MemoryStore {
    data_map: Mutex<BTreeMap<StoreKey, Arc<RwLock<Vec<u8>>>>>,
}

impl MemoryStore {
    /// Create a new memory store at a given `base_directory`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            data_map: Mutex::default(),
        }
    }
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryStore {
    fn set_impl(&self, key: &StoreKey, value: &[u8], offset: Option<ByteOffset>, _truncate: bool) {
        let mut data_map = self.data_map.lock().unwrap();
        let data = data_map
            .entry(key.clone())
            .or_insert_with(|| Arc::new(RwLock::default()))
            .clone();
        drop(data_map);
        let mut data = data.write();

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
        let data_map = self.data_map.lock().unwrap();
        let data = data_map.get(key);
        if let Some(data) = data {
            let data = data.clone();
            drop(data_map);
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
        let data_map = self.data_map.lock().unwrap();
        let data = data_map.get(key);
        if let Some(data) = data {
            let data = data.clone();
            drop(data_map);
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

    fn size_prefix(&self, prefix: &StorePrefix) -> Result<u64, StorageError> {
        let mut size = 0;
        for key in self.list_prefix(prefix)? {
            if let Some(size_key) = self.size_key(&key)? {
                size += size_key;
            }
        }
        Ok(size)
    }

    fn size_key(&self, key: &StoreKey) -> Result<Option<u64>, StorageError> {
        let data_map = self.data_map.lock().unwrap();
        data_map
            .get(key)
            .map_or_else(|| Ok(None), |entry| Ok(Some(entry.read().len() as u64)))
    }
}

impl WritableStorageTraits for MemoryStore {
    fn set(&self, key: &StoreKey, value: &[u8]) -> Result<(), StorageError> {
        Self::set_impl(self, key, value, None, true);
        Ok(())
    }

    fn set_partial_values(
        &self,
        key_start_values: &[StoreKeyStartValue],
    ) -> Result<(), StorageError> {
        for key_start_value in key_start_values {
            Self::set_impl(
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
        let mut data_map = self.data_map.lock().unwrap();
        Ok(data_map.remove(key).is_some())
    }

    fn erase_prefix(&self, prefix: &StorePrefix) -> Result<bool, StorageError> {
        let mut data_map = self.data_map.lock().unwrap();
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
        let data_map = self.data_map.lock().unwrap();
        Ok(data_map.keys().cloned().collect())
    }

    fn list_prefix(&self, prefix: &StorePrefix) -> Result<StoreKeys, StorageError> {
        let data_map = self.data_map.lock().unwrap();
        Ok(data_map
            .keys()
            .filter(|&key| key.has_prefix(prefix))
            .cloned()
            .collect())
    }

    fn list_dir(&self, prefix: &StorePrefix) -> Result<StoreKeysPrefixes, StorageError> {
        let mut keys: StoreKeys = vec![];
        let mut prefixes: BTreeSet<StorePrefix> = BTreeSet::default();
        let data_map = self.data_map.lock().unwrap();
        for key in data_map.keys() {
            if key.has_prefix(prefix) {
                let key_strip = key.as_str().strip_prefix(prefix.as_str()).unwrap();
                let key_strip = key_strip.strip_prefix('/').unwrap_or(key_strip);
                let components: Vec<_> = key_strip.split('/').collect();
                if components.len() > 1 {
                    prefixes.insert(StorePrefix::new(
                        prefix.as_str().to_string() + components[0] + "/",
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

        assert_eq!(
            store
                .get_partial_values_key(&key, &[ByteRange::FromStart(1, None)])?
                .unwrap()
                .first()
                .unwrap(),
            &[3, 4]
        );

        assert!(store
            .get_partial_values_key(&"a/b/c".try_into()?, &[ByteRange::FromStart(1, None)])?
            .is_none());

        assert_eq!(
            store
                .get_partial_values(&[StoreKeyRange::new(
                    key.clone(),
                    ByteRange::FromStart(1, None)
                )])?
                .first()
                .unwrap()
                .as_ref()
                .unwrap(),
            &[3, 4]
        );
        Ok(())
    }

    #[test]
    fn memory_list() -> Result<(), Box<dyn Error>> {
        let store = MemoryStore::new();

        store.set(&"a/b".try_into()?, &[0])?;
        store.set(&"a/c".try_into()?, &[0, 0])?;
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

        assert_eq!(store.list_prefix(&"b/".try_into()?)?, &[]);

        assert_eq!(store.size()?, 3);
        assert_eq!(store.size_prefix(&"a/".try_into().unwrap())?, 3);
        assert_eq!(store.size_key(&"a/b".try_into().unwrap())?, Some(1));
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

        let list_dir = store.list_dir(&StorePrefix::root())?;
        assert_eq!(list_dir.prefixes(), &["a/".try_into()?, "b/".try_into()?,]);

        let list_dir = store.list_dir(&"a/".try_into()?)?;

        assert_eq!(list_dir.keys(), &["a/b".try_into()?, "a/c".try_into()?,]);
        assert_eq!(
            list_dir.prefixes(),
            &["a/d/".try_into()?, "a/f/".try_into()?,]
        );

        store.erase_prefix(&"b/".try_into()?)?;
        let list_dir = store.list_dir(&StorePrefix::root())?;
        assert_eq!(list_dir.prefixes(), &["a/".try_into()?,]);

        Ok(())
    }
}
