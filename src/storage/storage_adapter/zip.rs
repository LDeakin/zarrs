//! A zip storage adapter.

use crate::{
    array::codec::extract_byte_ranges_read,
    byte_range::ByteRange,
    storage::{
        storage_value_io::StorageValueIO, Bytes, ListableStorageTraits, ReadableStorageTraits,
        StorageError, StoreKey, StoreKeys, StoreKeysPrefixes, StorePrefix, StorePrefixes,
    },
};

use itertools::Itertools;
use parking_lot::Mutex;
use thiserror::Error;
use zip::{result::ZipError, ZipArchive};

use std::{path::PathBuf, sync::Arc};

/// A zip storage adapter.
pub struct ZipStorageAdapter<TStorage: ?Sized> {
    size: u64,
    zip_archive: Mutex<ZipArchive<StorageValueIO<TStorage>>>,
    zip_path: PathBuf,
}

impl<TStorage: ?Sized + ReadableStorageTraits> ZipStorageAdapter<TStorage> {
    /// Create a new zip storage adapter.
    ///
    /// # Errors
    ///
    /// Returns a [`ZipStorageAdapterCreateError`] if the root path of the store is not a valid zip file.
    pub fn new(
        storage: Arc<TStorage>,
        key: StoreKey,
    ) -> Result<Self, ZipStorageAdapterCreateError> {
        Self::new_with_path(storage, key, "")
    }

    /// Create a new zip storage adapter to `path` within the zip file.
    ///
    /// # Errors
    ///
    /// Returns a [`ZipStorageAdapterCreateError`] if the root path of the store is not a valid zip file.
    pub fn new_with_path<T: Into<PathBuf>>(
        storage: Arc<TStorage>,
        key: StoreKey,
        path: T,
    ) -> Result<Self, ZipStorageAdapterCreateError> {
        let zip_path = path.into();
        let size = storage
            .size_key(&key)?
            .ok_or::<ZipStorageAdapterCreateError>(
                StorageError::UnknownKeySize(key.clone()).into(),
            )?;
        let storage_io = StorageValueIO::new(storage, key, size);
        let zip_archive = Mutex::new(
            ZipArchive::new(storage_io)
                .map_err(|err| ZipStorageAdapterCreateError::ZipError(err.to_string()))?,
        );
        Ok(Self {
            size,
            zip_archive,
            zip_path,
        })
    }

    fn get_impl(
        &self,
        key: &StoreKey,
        byte_ranges: &[ByteRange],
    ) -> Result<Option<Vec<Bytes>>, StorageError> {
        let mut zip_archive = self.zip_archive.lock();
        let mut zip_name = self.zip_path.clone();
        zip_name.push(key.as_str());

        let mut file = {
            let zip_file = zip_archive.by_name(&zip_name.to_string_lossy());
            match zip_file {
                Ok(zip_file) => zip_file,
                Err(err) => match err {
                    ZipError::FileNotFound => return Ok(None),
                    _ => return Err(StorageError::Other(err.to_string())),
                },
            }
        };
        let size = file.size();

        let out = extract_byte_ranges_read(&mut file, size, byte_ranges)?
            .into_iter()
            .map(Bytes::from)
            .collect();
        Ok(Some(out))
    }

    fn zip_file_strip_prefix<'a>(&self, name: &'a str) -> Option<&'a str> {
        name.strip_prefix(self.zip_path.to_str().unwrap())
            .filter(|&name| !name.is_empty())
    }
}

impl<TStorage: ?Sized + ReadableStorageTraits> ReadableStorageTraits
    for ZipStorageAdapter<TStorage>
{
    fn get_partial_values_key(
        &self,
        key: &StoreKey,
        byte_ranges: &[ByteRange],
    ) -> Result<Option<Vec<Bytes>>, StorageError> {
        self.get_impl(key, byte_ranges)
    }

    fn size_key(&self, key: &StoreKey) -> Result<Option<u64>, StorageError> {
        let mut zip_archive = self.zip_archive.lock();
        let file = zip_archive.by_name(key.as_str());
        match file {
            Ok(file) => Ok(Some(file.compressed_size())),
            Err(err) => match err {
                ZipError::FileNotFound => Ok(None),
                _ => Err(StorageError::Other(err.to_string())),
            },
        }
    }
}

impl<TStorage: ?Sized + ReadableStorageTraits> ListableStorageTraits
    for ZipStorageAdapter<TStorage>
{
    fn list(&self) -> Result<StoreKeys, StorageError> {
        Ok(self
            .zip_archive
            .lock()
            .file_names()
            .filter_map(|name| self.zip_file_strip_prefix(name))
            .filter_map(|v| StoreKey::try_from(v).ok())
            .sorted()
            .collect())
    }

    fn list_prefix(&self, prefix: &StorePrefix) -> Result<StoreKeys, StorageError> {
        let mut zip_archive = self.zip_archive.lock();
        let file_names: Vec<String> = zip_archive
            .file_names()
            .filter_map(|name| self.zip_file_strip_prefix(name))
            .map(std::string::ToString::to_string)
            .collect();
        Ok(file_names
            .into_iter()
            .filter_map(|name| {
                if name.starts_with(prefix.as_str()) {
                    let mut zip_name = self.zip_path.clone();
                    zip_name.push(&name);
                    if let Ok(file) = zip_archive.by_name(&zip_name.to_string_lossy()) {
                        if file.is_file() {
                            let name = name.strip_suffix('/').unwrap_or(&name);
                            if let Ok(store_key) = StoreKey::try_from(name) {
                                return Some(store_key);
                            }
                        }
                    }
                }
                None
            })
            .sorted()
            .collect())
    }

    fn list_dir(&self, prefix: &StorePrefix) -> Result<StoreKeysPrefixes, StorageError> {
        let zip_archive = self.zip_archive.lock();
        let mut keys: StoreKeys = vec![];
        let mut prefixes: StorePrefixes = vec![];
        for name in zip_archive
            .file_names()
            .filter_map(|name| self.zip_file_strip_prefix(name))
        {
            if name.starts_with(prefix.as_str()) {
                if name.ends_with('/') {
                    if let Ok(store_prefix) = StorePrefix::try_from(name) {
                        if let Some(parent) = store_prefix.parent() {
                            if &parent == prefix {
                                prefixes.push(store_prefix);
                            }
                        }
                    }
                } else if let Ok(store_key) = StoreKey::try_from(name) {
                    let parent = store_key.parent();
                    if &parent == prefix {
                        keys.push(store_key);
                    }
                }
            }
        }
        keys.sort();
        prefixes.sort();

        Ok(StoreKeysPrefixes { keys, prefixes })
    }

    fn size(&self) -> Result<u64, StorageError> {
        Ok(self.size)
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
}

/// A zip store creation error.
#[derive(Debug, Error)]
pub enum ZipStorageAdapterCreateError {
    /// An IO error.
    #[error(transparent)]
    IOError(#[from] std::io::Error),
    /// An existing directory.
    #[error("{0} is an existing directory, not a zip file")]
    ExistingDir(PathBuf),
    /// A zip error.
    #[error("{0}")]
    ZipError(String),
    /// A storage error.
    #[error(transparent)]
    StorageError(#[from] StorageError),
}

#[cfg(test)]
mod tests {
    use walkdir::WalkDir;

    use crate::storage::{store::FilesystemStore, WritableStorageTraits};

    use super::*;
    use std::{
        error::Error,
        fs::File,
        io::{Read, Write},
        path::Path,
    };

    // https://github.com/zip-rs/zip/blob/master/examples/write_dir.rs
    fn zip_dir(
        it: &mut dyn Iterator<Item = walkdir::DirEntry>,
        prefix: &str,
        writer: File,
        method: zip::CompressionMethod,
    ) -> zip::result::ZipResult<()> {
        let mut zip = zip::ZipWriter::new(writer);
        let options = zip::write::SimpleFileOptions::default().compression_method(method);
        let mut buffer = Vec::new();
        for entry in it {
            let path = entry.path();
            let name = path.strip_prefix(Path::new(prefix)).unwrap();
            if path.is_file() {
                #[allow(deprecated)]
                zip.start_file_from_path(name, options)?;
                let mut f = File::open(path)?;
                f.read_to_end(&mut buffer)?;
                zip.write_all(&buffer)?;
                buffer.clear();
            } else if !name.as_os_str().is_empty() {
                #[allow(deprecated)]
                zip.add_directory_from_path(name, options)?;
            }
        }
        zip.finish()?;
        Result::Ok(())
    }

    fn zip_write(path: &Path) -> Result<(), Box<dyn Error>> {
        let tmp_path = tempfile::TempDir::new()?;
        let tmp_path = tmp_path.path();
        let store = FilesystemStore::new(tmp_path)?.sorted();
        store.set(&"a/b".try_into()?, vec![0, 1, 2, 3].into())?;
        store.set(&"a/c".try_into()?, vec![].into())?;
        store.set(&"a/d/e".try_into()?, vec![].into())?;
        store.set(&"a/f/g".try_into()?, vec![].into())?;
        store.set(&"a/f/h".try_into()?, vec![].into())?;
        store.set(&"b/c/d".try_into()?, vec![].into())?;
        store.set(&"c".try_into()?, vec![].into())?;

        let walkdir = WalkDir::new(tmp_path);

        let file = File::create(path).unwrap();
        zip_dir(
            &mut walkdir.into_iter().filter_map(std::result::Result::ok),
            tmp_path.to_str().unwrap(),
            file,
            zip::CompressionMethod::Stored,
        )?;

        Ok(())
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn zip_root() -> Result<(), Box<dyn Error>> {
        let path = tempfile::TempDir::new()?;
        let mut path = path.path().to_path_buf();
        let store = FilesystemStore::new(path.clone())?;

        path.push("test.zip");
        zip_write(&path)?;

        let store = Arc::new(ZipStorageAdapter::new(
            store.into(),
            StoreKey::new("test.zip")?,
        )?);

        assert_eq!(
            store.list()?,
            &[
                "a/b".try_into()?,
                "a/c".try_into()?,
                "a/d/e".try_into()?,
                "a/f/g".try_into()?,
                "a/f/h".try_into()?,
                "b/c/d".try_into()?,
                "c".try_into()?,
            ]
        );
        assert_eq!(
            store.list_prefix(&"a/".try_into()?)?,
            &[
                "a/b".try_into()?,
                "a/c".try_into()?,
                "a/d/e".try_into()?,
                "a/f/g".try_into()?,
                "a/f/h".try_into()?,
            ]
        );
        assert_eq!(
            store.list_prefix(&"a/d/".try_into()?)?,
            &["a/d/e".try_into()?]
        );
        assert_eq!(
            store.list_prefix(&"".try_into()?)?,
            &[
                "a/b".try_into()?,
                "a/c".try_into()?,
                "a/d/e".try_into()?,
                "a/f/g".try_into()?,
                "a/f/h".try_into()?,
                "b/c/d".try_into()?,
                "c".try_into()?,
            ]
        );

        let list = store.list_dir(&"a/".try_into()?)?;
        assert_eq!(list.keys(), &["a/b".try_into()?, "a/c".try_into()?]);
        assert_eq!(list.prefixes(), &["a/d/".try_into()?, "a/f/".try_into()?,]);

        assert!(crate::storage::node_exists(&store, &"/a/b".try_into()?)?);
        assert!(crate::storage::node_exists_listable(
            &store,
            &"/a/b".try_into()?
        )?);

        assert_eq!(store.get(&"a/b".try_into()?)?.unwrap(), vec![0, 1, 2, 3]);
        assert_eq!(
            store.get(&"a/c".try_into()?)?.unwrap(),
            Vec::<u8>::new().as_slice()
        );

        Ok(())
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn zip_path() -> Result<(), Box<dyn Error>> {
        let path = tempfile::TempDir::new()?;
        let mut path = path.path().to_path_buf();
        let store = FilesystemStore::new(path.clone())?;
        path.push("test.zip");
        zip_write(&path)?;

        let store = Arc::new(ZipStorageAdapter::new_with_path(
            store.into(),
            StoreKey::new("test.zip")?,
            "a/",
        )?);

        assert_eq!(
            store.list()?,
            &[
                "b".try_into()?,
                "c".try_into()?,
                "d/e".try_into()?,
                "f/g".try_into()?,
                "f/h".try_into()?,
            ]
        );
        assert_eq!(store.list_prefix(&"a/".try_into()?)?, &[]);
        assert_eq!(store.list_prefix(&"d/".try_into()?)?, &["d/e".try_into()?]);
        assert_eq!(
            store.list_prefix(&"".try_into()?)?,
            &[
                "b".try_into()?,
                "c".try_into()?,
                "d/e".try_into()?,
                "f/g".try_into()?,
                "f/h".try_into()?,
            ]
        );

        let list = store.list_dir(&"".try_into()?)?;
        assert_eq!(list.keys(), &["b".try_into()?, "c".try_into()?]);
        assert_eq!(list.prefixes(), &["d/".try_into()?, "f/".try_into()?,]);

        assert!(crate::storage::node_exists(&store, &"/b".try_into()?)?);
        assert!(crate::storage::node_exists_listable(
            &store,
            &"/b".try_into()?
        )?);

        assert_eq!(store.get(&"b".try_into()?)?.unwrap(), vec![0, 1, 2, 3]);
        // assert_eq!(store.get(&"c".try_into()?)?, Vec::<u8>::new().as_slice());

        Ok(())
    }
}
