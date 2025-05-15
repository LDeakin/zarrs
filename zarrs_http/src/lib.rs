//! A synchronous `http` store for the [`zarrs`](https://docs.rs/zarrs/latest/zarrs/index.html) crate.
//!
//! ```rust
//! # use std::sync::Arc;
//! use zarrs_storage::ReadableStorage;
//! use zarrs_http::HTTPStore;
//!
//! let http_store: ReadableStorage = Arc::new(HTTPStore::new("http://...")?);
//! # Ok::<_, Box<dyn std::error::Error>>(())
//! ```
//!
//! ## Licence
//! `zarrs_http` is licensed under either of
//! - the Apache License, Version 2.0 [LICENSE-APACHE](https://docs.rs/crate/zarrs_http/latest/source/LICENCE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0> or
//! - the MIT license [LICENSE-MIT](https://docs.rs/crate/zarrs_http/latest/source/LICENCE-MIT) or <http://opensource.org/licenses/MIT>, at your option.

use zarrs_storage::{
    byte_range::ByteRange, Bytes, MaybeBytes, ReadableStorageTraits, StorageError, StoreKey,
};

use itertools::Itertools;
use reqwest::{
    header::{HeaderValue, CONTENT_LENGTH, RANGE},
    StatusCode, Url,
};
use std::str::FromStr;
use thiserror::Error;

/// A synchronous HTTP store.
#[derive(Debug)]
pub struct HTTPStore {
    base_url: Url,
    batch_range_requests: bool,
    client: reqwest::blocking::Client,
}

#[allow(clippy::needless_pass_by_value)]
fn handle_reqwest_error(err: reqwest::Error) -> StorageError {
    StorageError::Other(err.to_string())
}

fn handle_url_error(err: url::ParseError) -> StorageError {
    StorageError::Other(err.to_string())
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
        let url = self.key_to_url(key).map_err(handle_url_error)?;
        let response = self.client.get(url).send().map_err(handle_reqwest_error)?;
        match response.status() {
            StatusCode::OK => Ok(Some(response.bytes().map_err(handle_reqwest_error)?)),
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
        let url = self.key_to_url(key).map_err(handle_url_error)?;
        let Some(size) = self.size_key(key)? else {
            return Ok(None);
        };
        let bytes_strs = byte_ranges
            .iter()
            .map(|byte_range| format!("{}-{}", byte_range.start(size), byte_range.end(size) - 1))
            .join(", ");

        let range = HeaderValue::from_str(&format!("bytes={bytes_strs}")).unwrap();
        let response = self
            .client
            .get(url)
            .header(RANGE, range)
            .send()
            .map_err(handle_reqwest_error)?;

        match response.status() {
            StatusCode::NOT_FOUND => Err(StorageError::from("the http server returned a NOT FOUND status for the byte range request, but returned a non zero size for CONTENT_LENGTH")),
            StatusCode::PARTIAL_CONTENT => {
                // TODO: Gracefully handle a response from the server which does not include all requested by ranges
                let mut bytes = response.bytes().map_err(handle_reqwest_error)?;
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
                let bytes = response.bytes().map_err(handle_reqwest_error)?;
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
        let url = self.key_to_url(key).map_err(handle_url_error)?;
        let response = self.client.head(url).send().map_err(handle_reqwest_error)?;
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
    use super::*;
    use std::error::Error;

    const HTTP_TEST_PATH_REF: &str =
        "https://raw.githubusercontent.com/zarrs/zarrs/main/zarrs/tests/data/store";

    #[ignore]
    #[test]
    #[cfg_attr(miri, ignore)]
    fn http_store() -> Result<(), Box<dyn Error>> {
        let mut store = HTTPStore::new(HTTP_TEST_PATH_REF).unwrap();
        zarrs_storage::store_test::store_read(&store)?;
        store.set_batch_range_requests(false);
        zarrs_storage::store_test::store_read(&store)?;
        Ok(())
    }

    #[ignore]
    #[test]
    #[cfg_attr(miri, ignore)]
    fn http_store_bad_url() {
        assert!(HTTPStore::new("invalid").is_err());
    }

    #[ignore]
    #[test]
    #[cfg_attr(miri, ignore)]
    fn http_store_bad_request() -> Result<(), Box<dyn Error>> {
        let store = HTTPStore::new("https://raw.githubusercontent.com/bad").unwrap();
        assert!(store.get(&"zarr.json".try_into().unwrap()).is_err());
        assert!(store
            .get_partial_values_key(
                &"zarr.json".try_into().unwrap(),
                &[ByteRange::FromStart(0, None)]
            )
            .is_err());
        assert!(store.size_key(&"zarr.json".try_into().unwrap()).is_err());
        Ok(())
    }
}
