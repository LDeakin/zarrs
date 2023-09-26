//! A filesystem store.
//!
//! See <https://zarr-specs.readthedocs.io/en/latest/v3/stores/filesystem/v1.0.html>.

use crate::{
    byte_range::{ByteOffset, ByteRange, InvalidByteRangeError},
    storage::{
        ListableStorageTraits, ReadableStorageTraits, ReadableWritableStorageTraits, StorageError,
        StoreKeyRange, StoreKeyStartValue, StoreKeysPrefixes,
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
    sync::Mutex,
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
    base_directory: PathBuf,
    sort: bool,
    readonly: bool,
    files: RwLock<HashMap<StoreKey, Mutex<()>>>,
}

impl ReadableStoreExtension for FilesystemStore {}

impl WritableStoreExtension for FilesystemStore {}

impl ListableStoreExtension for FilesystemStore {}

impl ReadableWritableStoreExtension for FilesystemStore {}

impl StoreExtension for FilesystemStore {
    fn uri_scheme(&self) -> Option<&'static str> {
        Some("file")
    }
}

impl FilesystemStore {
    /// Create a new file system store at a given `base_directory`.
    /// The base directory will be created if it does not exist.
    ///
    /// # Errors
    ///
    /// Returns a [`FilesystemStoreCreateError`] if `base_directory`:
    ///   - is not valid, or
    ///   - it points to an existing file rather than a directory.
    pub fn new<P: AsRef<Path>>(
        base_directory: P,
    ) -> Result<FilesystemStore, FilesystemStoreCreateError> {
        let mut base_directory = base_directory.as_ref().to_path_buf();
        if !base_directory.ends_with("/") {
            base_directory.push("");
        }

        if base_directory.to_str().is_none() {
            return Err(FilesystemStoreCreateError::InvalidBaseDirectory(
                base_directory,
            ));
        }
        if base_directory.is_file() {
            return Err(FilesystemStoreCreateError::ExistingFile(base_directory));
        }
        let readonly = if base_directory.is_dir() {
            // the directory already exists, check if it is read only
            let md =
                std::fs::metadata(&base_directory).map_err(FilesystemStoreCreateError::IOError)?;
            md.permissions().readonly()
        } else {
            // base directory does not exist, so create it. If this succeeds, the filesystem is not read only
            std::fs::create_dir_all(&base_directory)
                .map_err(FilesystemStoreCreateError::IOError)?;
            false
        };
        Ok(FilesystemStore {
            base_directory,
            sort: false,
            readonly,
            files: RwLock::new(HashMap::new()),
        })
    }

    /// Makes the store sort directories/files when walking.
    #[must_use]
    pub fn sorted(mut self) -> Self {
        self.sort = true;
        self
    }

    /// Maps a [`StoreKey`] to a filesystem [`PathBuf`].
    #[must_use]
    pub fn key_to_fspath(&self, key: &StoreKey) -> PathBuf {
        let path = PathBuf::from(self.base_directory_as_str().to_owned() + "/" + key.as_str());
        path
    }

    /// Maps a filesystem [`PathBuf`] to a [`StoreKey`].
    fn fspath_to_key(&self, path: &std::path::Path) -> Result<StoreKey, StoreKeyError> {
        let path = pathdiff::diff_paths(path, &self.base_directory)
            .ok_or(StoreKeyError::from(
                path.to_str().unwrap_or_default().to_string(),
            ))?
            .to_str()
            .ok_or(StoreKeyError::from(
                path.to_str().unwrap_or_default().to_string(),
            ))?
            .to_owned();
        StoreKey::new(&path)
    }

    /// Maps a store [`StorePrefix`] to a filesystem [`PathBuf`].
    #[must_use]
    pub fn prefix_to_fs_path(&self, prefix: &StorePrefix) -> PathBuf {
        let path = PathBuf::from(self.base_directory_as_str().to_owned() + prefix.as_str());
        path
    }

    fn base_directory_as_str(&self) -> &str {
        let Some(base_directory) = self.base_directory.to_str() else {
            unreachable!() // checked in new()
        };
        base_directory
    }

    fn get_impl(&self, key: &StoreKey, byte_range: &ByteRange) -> Result<Vec<u8>, StorageError> {
        let mut files = self.files.write();
        let _lock = files.entry(key.clone()).or_default();
        let mut file = File::open(self.key_to_fspath(key)).map_err(|err| match err.kind() {
            std::io::ErrorKind::NotFound => StorageError::KeyNotFound(key.clone()),
            _ => err.into(),
        })?;

        let buffer = {
            // Seek
            match byte_range {
                ByteRange::FromStart(offset, _) => file.seek(SeekFrom::Start(*offset as u64)),
                ByteRange::FromEnd(_, None) => file.seek(SeekFrom::Start(0u64)),
                ByteRange::FromEnd(offset, Some(length)) => file.seek(SeekFrom::End(
                    -(i64::try_from(*offset + *length).map_err(|_| InvalidByteRangeError)?),
                )),
            }?;

            // Read
            match byte_range {
                ByteRange::FromStart(_, None) | ByteRange::FromEnd(_, None) => {
                    let mut buffer = Vec::new();
                    file.read_to_end(&mut buffer)?;
                    buffer
                }
                ByteRange::FromStart(_, Some(length)) | ByteRange::FromEnd(_, Some(length)) => {
                    let mut buffer = vec![0; *length];
                    file.read_exact(&mut buffer)?;
                    buffer
                }
            }
        };

        Ok(buffer)
    }

    fn set_impl(
        &self,
        key: &StoreKey,
        value: &[u8],
        offset: Option<ByteOffset>,
        truncate: bool,
    ) -> Result<(), StorageError> {
        let key_path = self.key_to_fspath(key);

        let mut files = self.files.write();
        let _lock = files.entry(key.clone()).or_default();

        // Create directories
        if let Some(parent) = key_path.parent() {
            if !parent.exists() {
                std::fs::create_dir_all(parent)?;
            }
        }

        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(truncate)
            .open(key_path.clone())
            .map_err(|err| match err.kind() {
                std::io::ErrorKind::NotFound => StorageError::KeyNotFound(key.clone()),
                _ => err.into(),
            })?;

        // Write
        if offset.is_some() {
            file.seek(SeekFrom::Start(offset.unwrap() as u64))?;
        }
        file.write_all(value)?;

        Ok(())
    }
}

impl ReadableStorageTraits for FilesystemStore {
    fn get(&self, key: &StoreKey) -> Result<Vec<u8>, StorageError> {
        self.get_impl(key, &ByteRange::FromStart(0, None))
    }

    fn get_partial_values(
        &self,
        key_ranges: &[StoreKeyRange],
    ) -> Vec<Result<Vec<u8>, StorageError>> {
        let mut out = Vec::with_capacity(key_ranges.len());
        for key_range in key_ranges {
            out.push(self.get_impl(&key_range.key, &key_range.byte_range));
        }
        out
    }

    fn size(&self) -> usize {
        WalkDir::new(&self.base_directory)
            .into_iter()
            .filter_map(std::result::Result::ok)
            .filter_map(|v| {
                if v.path().is_file() {
                    Some(usize::try_from(std::fs::metadata(v.path()).unwrap().len()).unwrap())
                } else {
                    None
                }
            })
            .sum()
    }
}

impl WritableStorageTraits for FilesystemStore {
    fn set(&self, key: &StoreKey, value: &[u8]) -> Result<(), StorageError> {
        if self.readonly {
            Err(StorageError::ReadOnly)
        } else {
            FilesystemStore::set_impl(self, key, value, None, true)
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
            FilesystemStore::set_impl(
                self,
                &key_start_value.key,
                key_start_value.value,
                Some(key_start_value.start),
                false,
            )?;
        }
        Ok(())
    }
    fn erase(&self, key: &StoreKey) -> Result<(), StorageError> {
        if self.readonly {
            return Err(StorageError::ReadOnly);
        }

        let key_path = self.key_to_fspath(key);
        Ok(std::fs::remove_file(key_path)?)
    }
    fn erase_values(&self, keys: &[StoreKey]) -> Result<(), StorageError> {
        if self.readonly {
            return Err(StorageError::ReadOnly);
        }

        for key in keys {
            self.erase(key)?;
        }
        Ok(())
    }
    fn erase_prefix(&self, prefix: &StorePrefix) -> Result<(), StorageError> {
        if self.readonly {
            return Err(StorageError::ReadOnly);
        }

        let prefix_path = self.prefix_to_fs_path(prefix);
        Ok(std::fs::remove_dir(prefix_path)?)
    }
}

impl ListableStorageTraits for FilesystemStore {
    fn list(&self) -> Result<StoreKeys, StorageError> {
        Ok(WalkDir::new(&self.base_directory)
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

/// A filesystem store creation error.
#[derive(Debug, Error)]
pub enum FilesystemStoreCreateError {
    /// An IO error.
    #[error(transparent)]
    IOError(#[from] std::io::Error),
    /// Base directory is an existing file.
    #[error("{0} is an existing file")]
    ExistingFile(PathBuf),
    /// The path is not valid on this system.
    #[error("base directory {0} is not valid")]
    InvalidBaseDirectory(PathBuf),
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
        assert_eq!(store.get(&key)?, &[0, 1, 2]);
        store.set_partial_values(&[StoreKeyStartValue::new(key.clone(), 1, &[3, 4])])?;
        assert_eq!(store.get(&key)?, &[0, 3, 4]);
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
