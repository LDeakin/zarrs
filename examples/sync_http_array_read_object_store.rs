use zarrs::storage::storage_adapter::async_to_sync::{
    AsyncToSyncBlockOn, AsyncToSyncStorageAdapter,
};

struct TokioBlockOn(tokio::runtime::Handle);

impl AsyncToSyncBlockOn for TokioBlockOn {
    fn block_on<F: core::future::Future>(&self, future: F) -> F::Output {
        self.0.block_on(future)
    }
}

fn http_array_read() -> Result<(), Box<dyn std::error::Error>> {
    use std::sync::Arc;
    use zarrs::{
        array::Array,
        array_subset::ArraySubset,
        storage::{
            storage_transformer::{StorageTransformerExtension, UsageLogStorageTransformer},
            store,
        },
    };

    const HTTP_URL: &str =
        "https://raw.githubusercontent.com/LDeakin/zarrs/main/tests/data/array_write_read.zarr";
    const ARRAY_PATH: &str = "/group/array";

    // Create a HTTP store
    // The object_store HTTP store uses reqwest under the hood, which uses tokio
    let runtime = tokio::runtime::Runtime::new()?;
    let block_on = TokioBlockOn(runtime.handle().clone());
    let async_store = Arc::new(store::AsyncObjectStore::new(
        object_store::http::HttpBuilder::new()
            .with_url(HTTP_URL)
            .build()?,
    ));
    let mut store: zarrs::storage::ReadableStorage =
        Arc::new(AsyncToSyncStorageAdapter::new(async_store, block_on));

    if let Some(arg1) = std::env::args().collect::<Vec<_>>().get(1) {
        if arg1 == "--usage-log" {
            let log_writer = Arc::new(std::sync::Mutex::new(
                // std::io::BufWriter::new(
                std::io::stdout(),
                //    )
            ));
            let usage_log = Arc::new(UsageLogStorageTransformer::new(log_writer, || {
                chrono::Utc::now().format("[%T%.3f] ").to_string()
            }));
            store = usage_log.clone().create_readable_transformer(store);
        }
    }

    // Init the existing array, reading metadata
    let array = Array::open(store, ARRAY_PATH)?;

    println!(
        "The array metadata is:\n{}\n",
        serde_json::to_string_pretty(&array.metadata()).unwrap()
    );

    // Read the whole array
    let subset_all = ArraySubset::new_with_shape(array.shape().to_vec());
    let data_all = array.retrieve_array_subset_ndarray::<f32>(&subset_all)?;
    println!("The whole array is:\n{data_all}\n");

    // Read a chunk back from the store
    let chunk_indices = vec![1, 0];
    let data_chunk = array.retrieve_chunk_ndarray::<f32>(&chunk_indices)?;
    println!("Chunk [1,0] is:\n{data_chunk}\n");

    // Read the central 4x2 subset of the array
    let subset_4x2 = ArraySubset::new_with_ranges(&[2..6, 3..5]); // the center 4x2 region
    let data_4x2 = array.retrieve_array_subset_ndarray::<f32>(&subset_4x2)?;
    println!("The middle 4x2 subset is:\n{data_4x2}\n");

    Ok(())
}

fn main() {
    if let Err(err) = http_array_read() {
        println!("{:?}", err);
    }
}
