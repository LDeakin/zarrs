mod r#async;
mod sync;

pub use r#async::AsyncOpendalStore;
pub use sync::OpendalStore;

pub use opendal::{BlockingOperator, Operator};

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
