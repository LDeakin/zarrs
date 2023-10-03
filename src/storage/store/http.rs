//! A HTTP store.

use crate::{
    byte_range::ByteRange,
    storage::{ReadableStorageTraits, StorageError, StoreKeyRange},
};

use super::{ReadableStoreExtension, StoreExtension, StoreKey};

use reqwest::{
    header::{HeaderValue, CONTENT_LENGTH, RANGE},
    Url,
};
use std::str::FromStr;
use thiserror::Error;

/// A HTTP store.
#[derive(Debug)]
pub struct HTTPStore {
    base_url: Url,
}

impl ReadableStoreExtension for HTTPStore {}

impl StoreExtension for HTTPStore {}

impl From<reqwest::Error> for StorageError {
    fn from(err: reqwest::Error) -> Self {
        Self::Other(err.to_string())
    }
}

impl From<url::ParseError> for StorageError {
    fn from(err: url::ParseError) -> Self {
        Self::Other(err.to_string())
    }
}

impl HTTPStore {
    /// Create a new HTTP store at a given `base_url`.
    ///
    /// # Errors
    ///
    /// Returns a [`HTTPStoreCreateError`] if `base_url` is not a valid URL.
    pub fn new(base_url: &str) -> Result<HTTPStore, HTTPStoreCreateError> {
        let base_url = Url::from_str(base_url)
            .map_err(|_| HTTPStoreCreateError::InvalidBaseURL(base_url.into()))?;
        Ok(HTTPStore { base_url })
    }

    /// Maps a [`StoreKey`] to a HTTP [`Url`].
    ///
    /// # Errors
    ///
    /// Returns an error if the URL is invalid.
    pub fn key_to_url(&self, key: &StoreKey) -> Result<Url, url::ParseError> {
        let mut url = self.base_url.as_str().to_string();
        if !key.as_str().is_empty() {
            url += &("/".to_string() + key.as_str().strip_prefix('/').unwrap_or(key.as_str()));
        }
        Url::parse(&url)
    }

    fn get_impl(&self, key: &StoreKey, byte_range: &ByteRange) -> Result<Vec<u8>, StorageError> {
        let url = self.key_to_url(key)?;
        let client = reqwest::blocking::Client::new();
        let size = self.size_key(key)?;
        let range = HeaderValue::from_str(&format!(
            "bytes={}-{}",
            byte_range.start(size),
            byte_range.end(size) - 1
        ))
        .unwrap();
        let response = client.get(url).header(RANGE, range).send()?;
        Ok(response.bytes()?.to_vec())
    }
}

impl ReadableStorageTraits for HTTPStore {
    fn get(&self, key: &StoreKey) -> Result<Vec<u8>, StorageError> {
        let url = self.key_to_url(key)?;
        let client = reqwest::blocking::Client::new();
        let response = client.get(url).send()?;
        Ok(response.bytes()?.to_vec())
    }

    fn get_partial_values(
        &self,
        key_ranges: &[StoreKeyRange],
    ) -> Vec<Result<Vec<u8>, StorageError>> {
        // TODO: Batch multiple byte ranges for a single key into a single request
        let mut out = Vec::with_capacity(key_ranges.len());
        for key_range in key_ranges {
            out.push(self.get_impl(&key_range.key, &key_range.byte_range));
        }
        out
    }

    fn size(&self) -> Result<u64, StorageError> {
        Err(StorageError::Unsupported(
            "size() not supported for HTTP store".into(),
        ))
    }

    fn size_key(&self, key: &StoreKey) -> Result<u64, StorageError> {
        let url = self.key_to_url(key)?;
        let client = reqwest::blocking::Client::new();
        let response = client.head(url).send()?;
        let length = response
            .headers()
            .get(CONTENT_LENGTH)
            .and_then(|header_value| header_value.to_str().ok())
            .and_then(|header_str| u64::from_str(header_str).ok())
            .ok_or(StorageError::from("content length response is invalid"))?;
        Ok(length)
    }
}

/// A HTTP store creation error.
#[derive(Debug, Error)]
pub enum HTTPStoreCreateError {
    /// An IO error.
    #[error(transparent)]
    IOError(#[from] std::io::Error),
    /// The url is not valid.
    #[error("base url {0} is not valid")]
    InvalidBaseURL(String),
}

#[cfg(test)]
mod tests {
    use crate::{
        array::{Array, DataType},
        node::NodePath,
        storage::meta_key,
    };

    use super::*;

    const HTTP_TEST_PATH: &'static str =
        "https://raw.githubusercontent.com/LDeakin/zarrs/main/tests/data/hierarchy.zarr";
    const ARRAY_PATH: &'static str = "/a/baz";

    #[test]
    fn http_store_size() {
        let store = HTTPStore::new(HTTP_TEST_PATH).unwrap();
        let len = store
            .size_key(&meta_key(&NodePath::new(ARRAY_PATH).unwrap()))
            .unwrap();
        assert_eq!(len, 691);
    }

    #[test]
    fn http_store_get() {
        let store = HTTPStore::new(HTTP_TEST_PATH).unwrap();
        let metadata = store
            .get(&meta_key(&NodePath::new(ARRAY_PATH).unwrap()))
            .unwrap();
        let metadata: crate::array::ArrayMetadataV3 = serde_json::from_slice(&metadata).unwrap();
        assert_eq!(metadata.data_type.name(), "float64");
    }

    #[test]
    fn http_store_array() {
        let store = HTTPStore::new(HTTP_TEST_PATH).unwrap();
        let array = Array::new(store.into(), ARRAY_PATH).unwrap();
        assert_eq!(array.data_type(), &DataType::Float64);
    }
}
