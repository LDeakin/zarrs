//! A filesystem store.
//!
//! See <https://zarr-specs.readthedocs.io/en/latest/v3/stores/filesystem/v1.0.html>.

use crate::{
    array::MaybeBytes,
    byte_range::{ByteOffset, ByteRange},
    storage::{
        ListableStorageTraits, ReadableListableStorageTraits, ReadableStorageTraits,
        ReadableWritableStorageTraits, StorageError, StoreKeyRange, StoreKeyStartValue,
        StoreKeysPrefixes,
    },
};

use super::{
    ListableStoreExtension, ReadableStoreExtension, ReadableWritableStoreExtension, StoreExtension,
    StoreKey, StoreKeyError, StoreKeys, StorePrefix, StorePrefixes, WritableStorageTraits,
    WritableStoreExtension,
};

use parking_lot::RwLock;
use thiserror::Error;
use walkdir::WalkDir;

use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

// // Register the store.
// inventory::submit! {
//     ReadableStorePlugin::new("file", |uri| Ok(Arc::new(create_store_filesystem(uri)?)))
// }
// inventory::submit! {
//     WritableStorePlugin::new("file", |uri| Ok(Arc::new(create_store_filesystem(uri)?)))
// }
// inventory::submit! {
//     ListableStorePlugin::new("file", |uri| Ok(Arc::new(create_store_filesystem(uri)?)))
// }
// inventory::submit! {
//     ReadableWritableStorePlugin::new("file", |uri| Ok(Arc::new(create_store_filesystem(uri)?)))
// }

// #[allow(clippy::similar_names)]
// fn create_store_filesystem(uri: &str) -> Result<FilesystemStore, StorePluginCreateError> {
//     let url = url::Url::parse(uri)?;
//     let path = std::path::PathBuf::from(url.path());
//     FilesystemStore::new(path).map_err(|e| StorePluginCreateError::Other(e.to_string()))
// }

/// A file system store.
///
/// See <https://zarr-specs.readthedocs.io/en/latest/v3/stores/filesystem/v1.0.html>.
#[derive(Debug)]
pub struct FilesystemStore {
    base_path: PathBuf,
    sort: bool,
    readonly: bool,
    files: Mutex<HashMap<StoreKey, Arc<RwLock<()>>>>,
}

impl ReadableStoreExtension for FilesystemStore {}

impl WritableStoreExtension for FilesystemStore {}

impl ListableStoreExtension for FilesystemStore {}

impl ReadableWritableStoreExtension for FilesystemStore {}

impl StoreExtension for FilesystemStore {}

impl FilesystemStore {
    /// Create a new file system store at a given `base_path`.
    ///
    /// # Errors
    ///
    /// Returns a [`FilesystemStoreCreateError`] if `base_directory`:
    ///   - is not valid, or
    ///   - it points to an existing file rather than a directory.
    pub fn new<P: AsRef<Path>>(base_path: P) -> Result<Self, FilesystemStoreCreateError> {
        let base_path = base_path.as_ref().to_path_buf();
        if base_path.to_str().is_none() {
            return Err(FilesystemStoreCreateError::InvalidBasePath(base_path));
        }

        let readonly = if base_path.exists() {
            // the path already exists, check if it is read only
            let md = std::fs::metadata(&base_path).map_err(FilesystemStoreCreateError::IOError)?;
            md.permissions().readonly()
        } else {
            // the path does not exist, so try and create it. If this succeeds, the filesystem is not read only
            std::fs::create_dir_all(&base_path).map_err(FilesystemStoreCreateError::IOError)?;
            std::fs::remove_dir(&base_path)?;
            false
        };

        Ok(Self {
            base_path,
            sort: false,
            readonly,
            files: Mutex::default(),
        })
    }

    /// Makes the store sort directories/files when walking.
    #[must_use]
    pub fn sorted(mut self) -> Self {
        self.sort = true;
        self
    }

    /// Maps a [`StoreKey`] to a filesystem [`PathBuf`].
    ///
    /// If key is empty `""` then this is the top level file/directory
    #[must_use]
    pub fn key_to_fspath(&self, key: &StoreKey) -> PathBuf {
        let mut path = self.base_path.clone();
        if !key.as_str().is_empty() {
            path.push(key.as_str().strip_prefix('/').unwrap_or(key.as_str()));
        }
        path
    }

    /// Maps a filesystem [`PathBuf`] to a [`StoreKey`].
    fn fspath_to_key(&self, path: &std::path::Path) -> Result<StoreKey, StoreKeyError> {
        let path = pathdiff::diff_paths(path, &self.base_path).ok_or(StoreKeyError::from(
            path.to_str().unwrap_or_default().to_string(),
        ))?;
        let path_str = path.to_string_lossy();
        StoreKey::new(&path_str)
    }

    /// Maps a store [`StorePrefix`] to a filesystem [`PathBuf`].
    #[must_use]
    pub fn prefix_to_fs_path(&self, prefix: &StorePrefix) -> PathBuf {
        let mut path = self.base_path.clone();
        path.push(prefix.as_str());
        path
    }

    fn get_file_mutex(&self, key: &StoreKey) -> Arc<RwLock<()>> {
        let mut files = self.files.lock().unwrap();
        let file = files
            .entry(key.clone())
            .or_insert_with(|| Arc::new(RwLock::default()))
            .clone();
        drop(files);
        file
    }

    fn set_impl(
        &self,
        key: &StoreKey,
        value: &[u8],
        offset: Option<ByteOffset>,
        truncate: bool,
    ) -> Result<(), StorageError> {
        let file = self.get_file_mutex(key);
        let _lock = file.write();

        // Create directories
        let key_path = self.key_to_fspath(key);
        if let Some(parent) = key_path.parent() {
            if !parent.exists() {
                std::fs::create_dir_all(parent)?;
            }
        }

        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(truncate)
            .open(key_path)?;

        // Write
        if let Some(offset) = offset {
            file.seek(SeekFrom::Start(offset))?;
        }
        file.write_all(value)?;

        Ok(())
    }
}

impl ReadableStorageTraits for FilesystemStore {
    fn get(&self, key: &StoreKey) -> Result<MaybeBytes, StorageError> {
        Ok(self
            .get_partial_values_key(key, &[ByteRange::FromStart(0, None)])?
            .map(|mut v| v.remove(0)))
    }

    fn get_partial_values_key(
        &self,
        key: &StoreKey,
        byte_ranges: &[ByteRange],
    ) -> Result<Option<Vec<Vec<u8>>>, StorageError> {
        let file = self.get_file_mutex(key);
        let _lock = file.read();

        let mut file = match File::open(self.key_to_fspath(key)) {
            Ok(file) => file,
            Err(err) => {
                if err.kind() == std::io::ErrorKind::NotFound {
                    return Ok(None);
                }
                return Err(err.into());
            }
        };

        let mut out = Vec::with_capacity(byte_ranges.len());
        for byte_range in byte_ranges {
            let bytes = {
                // Seek
                match byte_range {
                    ByteRange::FromStart(offset, _) => file.seek(SeekFrom::Start(*offset)),
                    ByteRange::FromEnd(_, None) => file.seek(SeekFrom::Start(0u64)),
                    ByteRange::FromEnd(offset, Some(length)) => {
                        file.seek(SeekFrom::End(-(i64::try_from(*offset + *length).unwrap())))
                    }
                }?;

                // Read
                match byte_range {
                    ByteRange::FromStart(_, None) | ByteRange::FromEnd(_, None) => {
                        let mut buffer = Vec::new();
                        file.read_to_end(&mut buffer)?;
                        buffer
                    }
                    ByteRange::FromStart(_, Some(length)) | ByteRange::FromEnd(_, Some(length)) => {
                        let length = usize::try_from(*length).unwrap();
                        let mut buffer = vec![0; length];
                        file.read_exact(&mut buffer)?;
                        buffer
                    }
                }
            };
            out.push(bytes);
        }

        Ok(Some(out))
    }

    fn get_partial_values(
        &self,
        key_ranges: &[StoreKeyRange],
    ) -> Result<Vec<MaybeBytes>, StorageError> {
        self.get_partial_values_batched_by_key(key_ranges)
    }

    fn size(&self) -> Result<u64, StorageError> {
        Ok(WalkDir::new(&self.base_path)
            .into_iter()
            .filter_map(std::result::Result::ok)
            .filter_map(|v| {
                if v.path().is_file() {
                    Some(std::fs::metadata(v.path()).unwrap().len())
                } else {
                    None
                }
            })
            .sum())
    }

    fn size_key(&self, key: &StoreKey) -> Result<Option<u64>, StorageError> {
        let key_path = self.key_to_fspath(key);
        if let Ok(metadata) = std::fs::metadata(key_path) {
            Ok(Some(metadata.len()))
        } else {
            Ok(None)
        }
    }
}

impl WritableStorageTraits for FilesystemStore {
    fn set(&self, key: &StoreKey, value: &[u8]) -> Result<(), StorageError> {
        if self.readonly {
            Err(StorageError::ReadOnly)
        } else {
            Self::set_impl(self, key, value, None, true)
        }
    }

    fn set_partial_values(
        &self,
        key_start_values: &[StoreKeyStartValue],
    ) -> Result<(), StorageError> {
        if self.readonly {
            return Err(StorageError::ReadOnly);
        }

        for key_start_value in key_start_values {
            Self::set_impl(
                self,
                &key_start_value.key,
                key_start_value.value,
                Some(key_start_value.start),
                false,
            )?;
        }
        Ok(())
    }

    fn erase(&self, key: &StoreKey) -> Result<bool, StorageError> {
        if self.readonly {
            return Err(StorageError::ReadOnly);
        }

        let file = self.get_file_mutex(key);
        let _lock = file.write();

        let key_path = self.key_to_fspath(key);
        Ok(std::fs::remove_file(key_path).is_ok())
    }

    fn erase_prefix(&self, prefix: &StorePrefix) -> Result<bool, StorageError> {
        if self.readonly {
            return Err(StorageError::ReadOnly);
        }

        let _lock = self.files.lock(); // lock all operations

        let prefix_path = self.prefix_to_fs_path(prefix);
        let result = std::fs::remove_dir_all(prefix_path);
        if let Err(err) = result {
            match err.kind() {
                std::io::ErrorKind::NotFound => Ok(false),
                _ => Err(err.into()),
            }
        } else {
            Ok(true)
        }
    }
}

impl ListableStorageTraits for FilesystemStore {
    fn list(&self) -> Result<StoreKeys, StorageError> {
        Ok(WalkDir::new(&self.base_path)
            .sort_by_file_name()
            .into_iter()
            .filter_map(std::result::Result::ok)
            .filter(|v| v.path().is_file())
            .filter_map(|v| self.fspath_to_key(v.path()).ok())
            .collect())
    }

    fn list_prefix(&self, prefix: &StorePrefix) -> Result<StoreKeys, StorageError> {
        let key: StoreKey = prefix.into();
        Ok(WalkDir::new(self.key_to_fspath(&key))
            .sort_by_file_name()
            .into_iter()
            .filter_map(std::result::Result::ok)
            .filter(|v| v.path().is_file())
            .filter_map(|v| self.fspath_to_key(v.path()).ok())
            .collect())
    }

    fn list_dir(&self, prefix: &StorePrefix) -> Result<StoreKeysPrefixes, StorageError> {
        let prefix_path = self.prefix_to_fs_path(prefix);
        let mut keys: StoreKeys = vec![];
        let mut prefixes: StorePrefixes = vec![];
        let dir = std::fs::read_dir(prefix_path);
        if let Ok(dir) = dir {
            for entry in dir {
                let entry = entry?;
                let fs_path = entry.path();
                let path = fs_path.file_name().unwrap();
                if fs_path.is_dir() {
                    prefixes.push(StorePrefix::new(
                        &(prefix.as_str().to_string() + path.to_str().unwrap() + "/"),
                    )?);
                } else {
                    keys.push(StoreKey::new(
                        &(prefix.as_str().to_owned() + path.to_str().unwrap()),
                    )?);
                }
            }
        }
        if self.sort {
            keys.sort();
            prefixes.sort();
        }

        Ok(StoreKeysPrefixes { keys, prefixes })
    }
}

impl ReadableWritableStorageTraits for FilesystemStore {}

impl ReadableListableStorageTraits for FilesystemStore {}

/// A filesystem store creation error.
#[derive(Debug, Error)]
pub enum FilesystemStoreCreateError {
    /// An IO error.
    #[error(transparent)]
    IOError(#[from] std::io::Error),
    /// The path is not valid on this system.
    #[error("base path {0} is not valid")]
    InvalidBasePath(PathBuf),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;

    #[test]
    fn filesystem_set() -> Result<(), Box<dyn Error>> {
        let path = tempfile::TempDir::new()?;
        let store = FilesystemStore::new(path.path())?;
        let key = "a/b".try_into()?;
        store.set(&key, &[0, 1, 2])?;
        assert_eq!(store.get(&key)?.unwrap(), &[0, 1, 2]);
        store.set_partial_values(&[StoreKeyStartValue::new(key.clone(), 1, &[3, 4])])?;
        assert_eq!(store.get(&key)?.unwrap(), &[0, 3, 4]);
        Ok(())
    }

    #[test]
    fn filesystem_list() -> Result<(), Box<dyn Error>> {
        let path = tempfile::TempDir::new()?;
        let store = FilesystemStore::new(path.path())?;

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

        assert!(crate::storage::node_exists(&store, &"/a/b".try_into()?)?);
        assert!(crate::storage::node_exists_listable(
            &store,
            &"/a/b".try_into()?
        )?);

        Ok(())
    }

    #[test]
    fn filesystem_list_dir() -> Result<(), Box<dyn Error>> {
        let path = tempfile::TempDir::new()?;
        let store = FilesystemStore::new(path.path())?.sorted();
        store.set(&"a/b".try_into()?, &[])?;
        store.set(&"a/c".try_into()?, &[])?;
        store.set(&"a/d/e".try_into()?, &[])?;
        store.set(&"a/f/g".try_into()?, &[])?;
        store.set(&"a/f/h".try_into()?, &[])?;
        store.set(&"b/c/d".try_into()?, &[])?;

        let list_dir = store.list_dir(&StorePrefix::new("a/")?)?;

        assert_eq!(list_dir.keys(), &["a/b".try_into()?, "a/c".try_into()?,]);
        assert_eq!(
            list_dir.prefixes(),
            &["a/d/".try_into()?, "a/f/".try_into()?,]
        );
        Ok(())
    }
}
