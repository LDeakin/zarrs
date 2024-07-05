//! A synchronous HTTP store.

#![allow(deprecated)]

use crate::{
    byte_range::ByteRange,
    storage::{Bytes, MaybeBytes, ReadableStorageTraits, StorageError, StoreKey},
};

use itertools::Itertools;
use reqwest::{
    header::{HeaderValue, CONTENT_LENGTH, RANGE},
    StatusCode, Url,
};
use std::str::FromStr;
use thiserror::Error;

/// A synchronous HTTP store.
#[deprecated(
    since = "0.15.0",
    note = "Use an opendal or object_store HTTP store with AsyncToSyncStorageAdapter instead, or write a new sync HTTP store."
)]
#[derive(Debug)]
pub struct HTTPStore {
    base_url: Url,
    batch_range_requests: bool,
    client: reqwest::blocking::Client,
}

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
    pub fn new(base_url: &str) -> Result<Self, HTTPStoreCreateError> {
        let base_url = Url::from_str(base_url)
            .map_err(|_| HTTPStoreCreateError::InvalidBaseURL(base_url.into()))?;
        let client = reqwest::blocking::Client::new();
        Ok(Self {
            base_url,
            batch_range_requests: true,
            client,
        })
    }

    /// Set whether to batch range requests.
    ///
    /// Defaults to true.
    /// Some servers do not fully support multipart ranges and might return an entire resource given such a request.
    /// It may be preferable to disable batched range requests in this case, so that each range request is a single part range.
    pub fn set_batch_range_requests(&mut self, batch_range_requests: bool) {
        self.batch_range_requests = batch_range_requests;
    }

    /// Maps a [`StoreKey`] to a HTTP [`Url`].
    ///
    /// # Errors
    ///
    /// Returns an error if the URL is invalid.
    pub fn key_to_url(&self, key: &StoreKey) -> Result<Url, url::ParseError> {
        let mut url = self.base_url.as_str().to_string();
        if !key.as_str().is_empty() {
            url +=
                ("/".to_string() + key.as_str().strip_prefix('/').unwrap_or(key.as_str())).as_str();
        }
        Url::parse(&url)
    }
}

impl ReadableStorageTraits for HTTPStore {
    fn get(&self, key: &StoreKey) -> Result<MaybeBytes, StorageError> {
        let url = self.key_to_url(key)?;
        let response = self.client.get(url).send()?;
        match response.status() {
            StatusCode::OK => Ok(Some(response.bytes()?)),
            StatusCode::NOT_FOUND => Ok(None),
            _ => Err(StorageError::from(format!(
                "http unexpected status code: {}",
                response.status()
            ))),
        }
    }

    fn get_partial_values_key(
        &self,
        key: &StoreKey,
        byte_ranges: &[ByteRange],
    ) -> Result<Option<Vec<Bytes>>, StorageError> {
        let url = self.key_to_url(key)?;
        let Some(size) = self.size_key(key)? else {
            return Ok(None);
        };
        let bytes_strs = byte_ranges
            .iter()
            .map(|byte_range| format!("{}-{}", byte_range.start(size), byte_range.end(size) - 1))
            .join(", ");

        let range = HeaderValue::from_str(&format!("bytes={bytes_strs}")).unwrap();
        let response = self.client.get(url).header(RANGE, range).send()?;

        match response.status() {
            StatusCode::NOT_FOUND => Err(StorageError::from("the http server returned a NOT FOUND status for the byte range request, but returned a non zero size for CONTENT_LENGTH")),
            StatusCode::PARTIAL_CONTENT => {
                // TODO: Gracefully handle a response from the server which does not include all requested by ranges
                let mut bytes = response.bytes()?;
                if bytes.len() as u64
                    == byte_ranges
                        .iter()
                        .map(|byte_range| byte_range.length(size))
                        .sum::<u64>()
                {
                    let mut out = Vec::with_capacity(byte_ranges.len());
                    for byte_range in byte_ranges {
                        let bytes_range =
                            bytes.split_to(usize::try_from(byte_range.length(size)).unwrap());
                        out.push(bytes_range);
                    }
                    Ok(Some(out))
                } else {
                    Err(StorageError::from(
                        "http partial content response did not include all requested byte ranges",
                    ))
                }
            }
            StatusCode::OK => {
                // Received all bytes
                let bytes = response.bytes()?;
                let mut out = Vec::with_capacity(byte_ranges.len());
                for byte_range in byte_ranges {
                    let start = usize::try_from(byte_range.start(size)).unwrap();
                    let end = usize::try_from(byte_range.end(size)).unwrap();
                    out.push(bytes.slice(start..end));
                }
                Ok(Some(out))
            }
            _ => Err(StorageError::from(format!(
                "the http server responded with status {} for the byte range request",
                response.status()
            ))),
        }
    }

    fn size_key(&self, key: &StoreKey) -> Result<Option<u64>, StorageError> {
        let url = self.key_to_url(key)?;
        let response = self.client.head(url).send()?;
        match response.status() {
            StatusCode::OK => {
                let length = response
                    .headers()
                    .get(CONTENT_LENGTH)
                    .and_then(|header_value| header_value.to_str().ok())
                    .and_then(|header_str| u64::from_str(header_str).ok())
                    .ok_or_else(|| StorageError::from("content length response is invalid"))?;
                Ok(Some(length))
            }
            StatusCode::NOT_FOUND => Ok(None),
            _ => Err(StorageError::from(format!(
                "http size_key has status code {}",
                response.status()
            ))),
        }
    }
}

/// A HTTP store creation error.
#[derive(Debug, Error)]
pub enum HTTPStoreCreateError {
    /// An IO error.
    #[error(transparent)]
    IOError(#[from] std::io::Error),
    /// The URL is not valid.
    #[error("base URL {0} is not valid")]
    InvalidBaseURL(String),
}

#[cfg(test)]
mod tests {
    use crate::{
        array::{Array, DataType},
        array_subset::ArraySubset,
        node::NodePath,
        storage::meta_key,
    };

    use super::*;

    const HTTP_TEST_PATH_REF: &str =
        "https://raw.githubusercontent.com/LDeakin/zarrs/main/tests/data/hierarchy.zarr";
    const ARRAY_PATH_REF: &str = "/a/baz";

    #[test]
    #[cfg_attr(miri, ignore)]
    fn http_store_size() {
        let store = HTTPStore::new(HTTP_TEST_PATH_REF).unwrap();
        let len = store
            .size_key(&meta_key(&NodePath::new(ARRAY_PATH_REF).unwrap()))
            .unwrap();
        assert_eq!(len.unwrap(), 691);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn http_store_get() {
        let store = HTTPStore::new(HTTP_TEST_PATH_REF).unwrap();
        let metadata = store
            .get(&meta_key(&NodePath::new(ARRAY_PATH_REF).unwrap()))
            .unwrap()
            .unwrap();
        let metadata: crate::array::ArrayMetadataV3 = serde_json::from_slice(&metadata).unwrap();
        assert_eq!(metadata.data_type.name(), "float64");
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn http_store_array() {
        let store = HTTPStore::new(HTTP_TEST_PATH_REF).unwrap();
        let array = Array::open(store.into(), ARRAY_PATH_REF).unwrap();
        assert_eq!(array.data_type(), &DataType::Float64);
    }

    #[cfg(feature = "gzip")]
    #[test]
    #[cfg_attr(miri, ignore)]
    fn http_store_array_get() {
        const HTTP_TEST_PATH: &str =
            "https://raw.githubusercontent.com/LDeakin/zarrs/main/tests/data/array_write_read.zarr";
        const ARRAY_PATH: &str = "/group/array";

        let store = HTTPStore::new(HTTP_TEST_PATH).unwrap();
        let array = Array::open(store.into(), ARRAY_PATH).unwrap();
        assert_eq!(array.data_type(), &DataType::Float32);

        // Read the central 4x2 subset of the array
        let subset_4x2 = ArraySubset::new_with_ranges(&[2..6, 3..5]); // the center 4x2 region
        let data_4x2 = array
            .retrieve_array_subset_elements::<f32>(&subset_4x2)
            .unwrap();
        assert!(data_4x2[0].is_nan());
        assert_eq!(data_4x2[1], 0.1);
        assert!(data_4x2[2].is_nan());
        assert_eq!(data_4x2[3], -3.4);
        assert_eq!(data_4x2[4], -4.3);
        assert_eq!(data_4x2[5], -4.4);
        assert_eq!(data_4x2[6], -5.3);
        assert_eq!(data_4x2[7], -5.4);

        // let data = array.retrieve_array_subset_ndarray::<f32>(&ArraySubset::new_with_shape(array.shape().to_vec())).unwrap();
        // println!("{data:?}");
    }

    #[cfg(all(feature = "sharding", feature = "gzip", feature = "crc32c"))]
    #[test]
    #[cfg_attr(miri, ignore)]
    fn http_store_sharded_array_get() {
        const HTTP_TEST_PATH_SHARDED: &str =
            "https://raw.githubusercontent.com/LDeakin/zarrs/main/tests/data/sharded_array_write_read.zarr";
        const ARRAY_PATH_SHARDED: &str = "/group/array";

        let store = HTTPStore::new(HTTP_TEST_PATH_SHARDED).unwrap();
        let array = Array::open(store.into(), ARRAY_PATH_SHARDED).unwrap();
        assert_eq!(array.data_type(), &DataType::UInt16);

        // Read the central 4x2 subset of the array
        let subset_4x2 = ArraySubset::new_with_ranges(&[2..6, 3..5]); // the center 4x2 region
        let data_4x2 = array
            .retrieve_array_subset_elements::<u16>(&subset_4x2)
            .unwrap();
        assert_eq!(data_4x2, [19, 20, 27, 28, 35, 36, 43, 44]);

        // let data = array.retrieve_array_subset_ndarray::<u16>(&ArraySubset::new_with_shape(array.shape().to_vec())).unwrap();
        // println!("{data:?}");
    }
}
