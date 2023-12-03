//! An in-memory store.

use crate::{object_store_impl, storage::StorageError};

/// An in-memory store.
#[derive(Debug)]
pub struct AsyncMemoryStore {
    object_store: object_store::memory::InMemory,
}

impl AsyncMemoryStore {
    /// Create a new memory store at a given `base_directory`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            object_store: object_store::memory::InMemory::new(),
        }
    }
}

impl Default for AsyncMemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

object_store_impl!(AsyncMemoryStore, object_store);

#[cfg(test)]
mod tests {
    use crate::storage::{
        AsyncListableStorageTraits, AsyncReadableStorageTraits, AsyncWritableStorageTraits,
        StoreKeyStartValue,
    };

    use super::*;
    use std::error::Error;

    #[tokio::test]
    async fn memory_set() -> Result<(), Box<dyn Error>> {
        let store = AsyncMemoryStore::new();
        let key = "a/b".try_into()?;
        store.set(&key, &[0, 1, 2]).await?;
        assert_eq!(store.get(&key).await?.unwrap(), &[0, 1, 2]);
        store
            .set_partial_values(&[StoreKeyStartValue::new(key.clone(), 1, &[3, 4])])
            .await?;
        assert_eq!(store.get(&key).await?.unwrap(), &[0, 3, 4]);
        Ok(())
    }

    #[tokio::test]
    async fn memory_list() -> Result<(), Box<dyn Error>> {
        let store = AsyncMemoryStore::new();

        store.set(&"a/b".try_into()?, &[]).await?;
        store.set(&"a/c".try_into()?, &[]).await?;
        store.set(&"a/d/e".try_into()?, &[]).await?;
        store.set(&"a/d/f".try_into()?, &[]).await?;
        store.erase(&"a/d/e".try_into()?).await?;
        assert_eq!(
            store.list().await?,
            &["a/b".try_into()?, "a/c".try_into()?, "a/d/f".try_into()?]
        );
        assert_eq!(
            store.list_prefix(&"a/".try_into()?).await?,
            &["a/b".try_into()?, "a/c".try_into()?, "a/d/f".try_into()?]
        );
        assert_eq!(
            store.list_prefix(&"a/d/".try_into()?).await?,
            &["a/d/f".try_into()?]
        );
        assert_eq!(
            store.list_prefix(&"".try_into()?).await?,
            &["a/b".try_into()?, "a/c".try_into()?, "a/d/f".try_into()?]
        );
        Ok(())
    }

    #[tokio::test]
    async fn memory_list_dir() -> Result<(), Box<dyn Error>> {
        let store = AsyncMemoryStore::new();
        store.set(&"a/b".try_into()?, &[]).await?;
        store.set(&"a/c".try_into()?, &[]).await?;
        store.set(&"a/d/e".try_into()?, &[]).await?;
        store.set(&"a/f/g".try_into()?, &[]).await?;
        store.set(&"a/f/h".try_into()?, &[]).await?;
        store.set(&"b/c/d".try_into()?, &[]).await?;

        let list_dir = store.list_dir(&"a/".try_into()?).await?;

        assert_eq!(list_dir.keys(), &["a/b".try_into()?, "a/c".try_into()?,]);
        assert_eq!(
            list_dir.prefixes(),
            &["a/d/".try_into()?, "a/f/".try_into()?,]
        );
        Ok(())
    }
}
