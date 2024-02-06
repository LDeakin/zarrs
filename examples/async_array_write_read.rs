async fn async_array_write_read() -> Result<(), Box<dyn std::error::Error>> {
    use futures::{stream::FuturesUnordered, StreamExt};
    use std::sync::Arc;
    use zarrs::{
        array::{DataType, FillValue, ZARR_NAN_F32},
        array_subset::ArraySubset,
        node::Node,
        storage::store,
    };

    // Create a store
    // let path = tempfile::TempDir::new()?;
    // let store = Arc::new(store::AsyncFilesystemStore::new(path.path())?);
    // let store = Arc::new(store::AsyncFilesystemStore::new(
    //     "tests/data/array_write_read.zarr",
    // )?);
    let store = Arc::new(store::AsyncObjectStore::new(
        object_store::memory::InMemory::new(),
    ));

    // Create a group
    let group_path = "/group";
    let mut group = zarrs::group::GroupBuilder::new().build(store.clone(), group_path)?;

    // Update group metadata
    group
        .attributes_mut()
        .insert("foo".into(), serde_json::Value::String("bar".into()));

    // Write group metadata to store
    group.async_store_metadata().await?;

    println!(
        "The group metadata is:\n{}\n",
        serde_json::to_string_pretty(&group.metadata())?
    );

    // Create an array
    let array_path = "/group/array";
    let array = zarrs::array::ArrayBuilder::new(
        vec![8, 8], // array shape
        DataType::Float32,
        vec![4, 4].try_into()?, // regular chunk shape
        FillValue::from(ZARR_NAN_F32),
    )
    // .bytes_to_bytes_codecs(vec![]) // uncompressed
    .dimension_names(Some(vec!["y".into(), "x".into()]))
    // .storage_transformers(vec![].into())
    .build(store.clone(), array_path)?;

    // Write array metadata to store
    array.async_store_metadata().await?;

    println!(
        "The array metadata is:\n{}\n",
        serde_json::to_string_pretty(&array.metadata())?
    );

    // Write some chunks
    let subsets = (0..2)
        .map(|i| {
            let chunk_indices: Vec<u64> = vec![0, i];
            array
                .chunk_grid()
                .subset(&chunk_indices, array.shape())?
                .ok_or_else(|| {
                    zarrs::array::ArrayError::InvalidChunkGridIndicesError(chunk_indices.to_vec())
                })
                .map(|chunk_subset| (i, chunk_indices, chunk_subset))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut futures = subsets
        .iter()
        .map(|(i, chunk_indices, chunk_subset)| {
            array.async_store_chunk_elements(
                &chunk_indices,
                vec![*i as f32 * 0.1; chunk_subset.num_elements() as usize],
            )
        })
        .collect::<FuturesUnordered<_>>();
    while let Some(item) = futures.next().await {
        item?;
    }

    let subset_all = ArraySubset::new_with_shape(array.shape().to_vec());
    let data_all = array
        .async_retrieve_array_subset_ndarray::<f32>(&subset_all)
        .await?;
    println!("async_store_chunk [0, 0] and [0, 1]:\n{data_all:+4.1}\n");

    // Store multiple chunks
    array
        .async_store_chunks_elements::<f32>(
            &ArraySubset::new_with_ranges(&[1..2, 0..2]),
            vec![
                //
                1.0, 1.0, 1.0, 1.0, 1.1, 1.1, 1.1, 1.1, 1.0, 1.0, 1.0, 1.0, 1.1, 1.1, 1.1, 1.1,
                //
                1.0, 1.0, 1.0, 1.0, 1.1, 1.1, 1.1, 1.1, 1.0, 1.0, 1.0, 1.0, 1.1, 1.1, 1.1, 1.1,
            ],
        )
        .await?;
    let data_all = array
        .async_retrieve_array_subset_ndarray::<f32>(&subset_all)
        .await?;
    println!("async_store_chunks [1..2, 0..2]:\n{data_all:+4.1}\n");

    // Write a subset spanning multiple chunks, including updating chunks already written
    array
        .async_store_array_subset_elements::<f32>(
            &ArraySubset::new_with_ranges(&[3..6, 3..6]),
            vec![-3.3, -3.4, -3.5, -4.3, -4.4, -4.5, -5.3, -5.4, -5.5],
        )
        .await?;
    let data_all = array
        .async_retrieve_array_subset_ndarray::<f32>(&subset_all)
        .await?;
    println!("async_store_array_subset [3..6, 3..6]:\n{data_all:+4.1}\n");

    // Store array subset
    array
        .async_store_array_subset_elements::<f32>(
            &ArraySubset::new_with_ranges(&[0..8, 6..7]),
            vec![-0.6, -1.6, -2.6, -3.6, -4.6, -5.6, -6.6, -7.6],
        )
        .await?;
    let data_all = array
        .async_retrieve_array_subset_ndarray::<f32>(&subset_all)
        .await?;
    println!("async_store_array_subset [0..8, 6..7]:\n{data_all:+4.1}\n");

    // Store chunk subset
    array
        .async_store_chunk_subset_elements::<f32>(
            // chunk indices
            &[1, 1],
            // subset within chunk
            &ArraySubset::new_with_ranges(&[3..4, 0..4]),
            vec![-7.4, -7.5, -7.6, -7.7],
        )
        .await?;
    let data_all = array
        .async_retrieve_array_subset_ndarray::<f32>(&subset_all)
        .await?;
    println!("async_store_chunk_subset [3..4, 0..4] of chunk [1, 1]:\n{data_all:+4.1}\n");

    // Erase a chunk
    array.async_erase_chunk(&[0, 0]).await?;
    let data_all = array
        .async_retrieve_array_subset_ndarray::<f32>(&subset_all)
        .await?;
    println!("async_erase_chunk [0, 0]:\n{data_all:+4.1}\n");

    // Read a chunk
    let chunk_indices = vec![0, 1];
    let data_chunk = array
        .async_retrieve_chunk_ndarray::<f32>(&chunk_indices)
        .await?;
    println!("async_retrieve_chunk [0, 1]:\n{data_chunk:+4.1}\n");

    // Read chunks
    let chunks = ArraySubset::new_with_ranges(&[0..2, 1..2]);
    let data_chunks = array.async_retrieve_chunks_ndarray::<f32>(&chunks).await?;
    println!("async_retrieve_chunks [0..2, 1..2]:\n{data_chunks:+4.1}\n");

    // Retrieve an array subset
    let subset = ArraySubset::new_with_ranges(&[2..6, 3..5]); // the center 4x2 region
    let data_subset = array
        .async_retrieve_array_subset_ndarray::<f32>(&subset)
        .await?;
    println!("async_retrieve_array_subset [2..6, 3..5]:\n{data_subset:+4.1}\n");

    // Show the hierarchy
    let node = Node::async_new(&*store, "/").await.unwrap();
    let tree = node.hierarchy_tree();
    println!("hierarchy_tree:\n{}", tree);

    Ok(())
}

#[tokio::main]
async fn main() {
    if let Err(err) = async_array_write_read().await {
        println!("{:?}", err);
    }
}
