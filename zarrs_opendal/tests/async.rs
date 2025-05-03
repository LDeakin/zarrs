#![allow(missing_docs)]

use opendal::Operator;
use zarrs_opendal::AsyncOpendalStore;
use zarrs_storage::{
    storage_adapter::{
        async_to_sync::{AsyncToSyncBlockOn, AsyncToSyncStorageAdapter},
        performance_metrics::PerformanceMetricsStorageAdapter,
        usage_log::UsageLogStorageAdapter,
    },
    StorageHandle,
};

use std::{error::Error, sync::Arc};

struct TokioBlockOn(tokio::runtime::Runtime);

impl AsyncToSyncBlockOn for TokioBlockOn {
    fn block_on<F: core::future::Future>(&self, future: F) -> F::Output {
        self.0.block_on(future)
    }
}

#[tokio::test]
async fn memory() -> Result<(), Box<dyn Error>> {
    let builder = opendal::services::Memory::default();
    let op = Operator::new(builder)?.finish();
    let store = AsyncOpendalStore::new(op);
    zarrs_storage::store_test::async_store_write(&store).await?;
    zarrs_storage::store_test::async_store_read(&store).await?;
    zarrs_storage::store_test::async_store_list(&store).await?;
    Ok(())
}

#[tokio::test]
async fn memory_async_storage_adapters() -> Result<(), Box<dyn Error>> {
    let builder = opendal::services::Memory::default();
    let op = Operator::new(builder)?.finish();
    let store = Arc::new(AsyncOpendalStore::new(op));
    let store = Arc::new(StorageHandle::new(store));
    let store = Arc::new(PerformanceMetricsStorageAdapter::new(store));
    let log_writer = Arc::new(std::sync::Mutex::new(
        // std::io::BufWriter::new(
        std::io::stdout(),
        //    )
    ));
    let store = Arc::new(UsageLogStorageAdapter::new(store, log_writer, || {
        "".to_string()
    }));
    zarrs_storage::store_test::async_store_write(&store).await?;
    zarrs_storage::store_test::async_store_read(&store).await?;
    zarrs_storage::store_test::async_store_list(&store).await?;
    Ok(())
}

#[test]
fn memory_sync() -> Result<(), Box<dyn Error>> {
    let builder = opendal::services::Memory::default();
    let op = Operator::new(builder)?.finish();
    let store = Arc::new(AsyncOpendalStore::new(op));
    let store =
        AsyncToSyncStorageAdapter::new(store, TokioBlockOn(tokio::runtime::Runtime::new()?));
    zarrs_storage::store_test::store_write(&store)?;
    zarrs_storage::store_test::store_read(&store)?;
    zarrs_storage::store_test::store_list(&store)?;
    Ok(())
}

#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn filesystem() -> Result<(), Box<dyn Error>> {
    let path = tempfile::TempDir::new()?;
    let builder = opendal::services::Fs::default().root(&path.path().to_string_lossy());
    let op = Operator::new(builder)?.finish();
    let store = AsyncOpendalStore::new(op);
    zarrs_storage::store_test::async_store_write(&store).await?;
    zarrs_storage::store_test::async_store_read(&store).await?;
    zarrs_storage::store_test::async_store_list(&store).await?;
    Ok(())
}
