//! [`opendal`] store support for the [`zarrs`](https://docs.rs/zarrs/latest/zarrs/index.html) crate.
//!
//! ```
//! # use std::sync::Arc;
//! use zarrs_storage::AsyncReadableStorage;
//! use zarrs_opendal::AsyncOpendalStore;
//!
//! let builder = opendal::services::Http::default().endpoint("http://...");
//! let operator = opendal::Operator::new(builder)?.finish();
//! let store: AsyncReadableStorage = Arc::new(AsyncOpendalStore::new(operator));
//! # Ok::<_, Box<dyn std::error::Error>>(())
//! ```
//!
//! ## Version Compatibility Matrix
//!
#![doc = include_str!("../doc/version_compatibility_matrix.md")]
//!
//! ## Licence
//! `zarrs_opendal` is licensed under either of
//! - the Apache License, Version 2.0 [LICENSE-APACHE](https://docs.rs/crate/zarrs_opendal/latest/source/LICENCE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0> or
//! - the MIT license [LICENSE-MIT](https://docs.rs/crate/zarrs_opendal/latest/source/LICENCE-MIT) or <http://opensource.org/licenses/MIT>, at your option.

mod r#async;
mod sync;

pub use r#async::AsyncOpendalStore;
pub use sync::OpendalStore;

pub use opendal;

use zarrs_storage::StorageError;

/// Map [`opendal::ErrorKind::NotFound`] to None, pass through other errors
fn handle_result_notfound<T>(result: Result<T, opendal::Error>) -> Result<Option<T>, StorageError> {
    match result {
        Ok(result) => Ok(Some(result)),
        Err(err) => {
            if err.kind() == opendal::ErrorKind::NotFound {
                Ok(None)
            } else {
                Err(StorageError::Other(err.to_string()))
            }
        }
    }
}

fn handle_result<T>(result: Result<T, opendal::Error>) -> Result<T, StorageError> {
    result.map_err(|err| StorageError::Other(err.to_string()))
}
