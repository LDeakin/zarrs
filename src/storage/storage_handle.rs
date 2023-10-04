use super::{ListableStorageTraits, ReadableStorageTraits, WritableStorageTraits};

/// A storage handle.
///
/// This is a handle to borrowed storage which can be owned and cloned, even if the storage it references is unsized.
#[derive(Clone)]
pub struct StorageHandle<'a, TStorage: ?Sized>(&'a TStorage);

impl<'a, TStorage: ?Sized> StorageHandle<'a, TStorage> {
    /// Create a new storage handle.
    pub fn new(storage: &'a TStorage) -> Self {
        Self(storage)
    }
}

impl<TStorage: ?Sized + ReadableStorageTraits> ReadableStorageTraits
    for StorageHandle<'_, TStorage>
{
    fn get(&self, key: &super::StoreKey) -> Result<Vec<u8>, super::StorageError> {
        self.0.get(key)
    }

    fn get_partial_values(
        &self,
        key_ranges: &[super::StoreKeyRange],
    ) -> Vec<Result<Vec<u8>, super::StorageError>> {
        self.0.get_partial_values(key_ranges)
    }

    fn size(&self) -> Result<u64, super::StorageError> {
        self.0.size()
    }

    fn size_key(&self, key: &super::StoreKey) -> Result<u64, super::StorageError> {
        self.0.size_key(key)
    }
}

impl<TStorage: ?Sized + ListableStorageTraits> ListableStorageTraits
    for StorageHandle<'_, TStorage>
{
    fn list(&self) -> Result<super::StoreKeys, super::StorageError> {
        self.0.list()
    }

    fn list_prefix(
        &self,
        prefix: &super::StorePrefix,
    ) -> Result<super::StoreKeys, super::StorageError> {
        self.0.list_prefix(prefix)
    }

    fn list_dir(
        &self,
        prefix: &super::StorePrefix,
    ) -> Result<super::StoreKeysPrefixes, super::StorageError> {
        self.0.list_dir(prefix)
    }
}

impl<TStorage: ?Sized + WritableStorageTraits> WritableStorageTraits
    for StorageHandle<'_, TStorage>
{
    fn set(&self, key: &super::StoreKey, value: &[u8]) -> Result<(), super::StorageError> {
        self.0.set(key, value)
    }

    fn set_partial_values(
        &self,
        key_start_values: &[super::StoreKeyStartValue],
    ) -> Result<(), super::StorageError> {
        self.0.set_partial_values(key_start_values)
    }

    fn erase(&self, key: &super::StoreKey) -> Result<(), super::StorageError> {
        self.0.erase(key)
    }

    fn erase_values(&self, keys: &[super::StoreKey]) -> Result<(), super::StorageError> {
        self.0.erase_values(keys)
    }

    fn erase_prefix(&self, prefix: &super::StorePrefix) -> Result<(), super::StorageError> {
        self.0.erase_prefix(prefix)
    }
}
