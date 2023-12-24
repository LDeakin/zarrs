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
    let store = Arc::new(store::AsyncMemoryStore::default());

    // Create a group and write metadata to filesystem
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
        serde_json::to_string_pretty(&group.metadata()).unwrap()
    );

    // Create an array
    let array_path = "/group/array";
    let array = zarrs::array::ArrayBuilder::new(
        vec![8, 8], // array shape
        DataType::Float32,
        vec![4, 4].into(), // regular chunk shape
        FillValue::from(ZARR_NAN_F32),
    )
    // .bytes_to_bytes_codecs(vec![]) // uncompressed
    .dimension_names(Some(vec!["y".into(), "x".into()]))
    // .storage_transformers(vec![].into())
    .build(store.clone(), array_path)?;

    // Write array metadata to store
    array.async_store_metadata().await?;

    // Write some chunks (in parallel)
    let subsets = (0..2)
        .map(|i| {
            let chunk_indices: Vec<u64> = vec![i, 0];
            if let Some(chunk_subset) = array.chunk_grid().subset(&chunk_indices, array.shape())? {
                Ok((i, chunk_indices, chunk_subset))
            } else {
                Err(zarrs::array::ArrayError::InvalidChunkGridIndicesError(
                    chunk_indices.to_vec(),
                ))
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut futures = subsets
        .iter()
        .map(|(i, chunk_indices, chunk_subset)| {
            array.async_store_chunk_elements(
                &chunk_indices,
                vec![*i as f32; chunk_subset.num_elements() as usize],
            )
        })
        .collect::<FuturesUnordered<_>>();
    while let Some(item) = futures.next().await {
        item?;
    }

    println!(
        "The array metadata is:\n{}\n",
        serde_json::to_string_pretty(&array.metadata()).unwrap()
    );

    // Write a subset spanning multiple chunks, including updating chunks already written
    array
        .async_store_array_subset_elements::<f32>(
            &ArraySubset::new_with_ranges(&[3..6, 3..6]),
            vec![0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9],
        )
        .await?;

    // Store elements directly, in this case set the 7th column to 123.0
    array
        .async_store_array_subset_elements::<f32>(
            &ArraySubset::new_with_ranges(&[0..8, 6..7]),
            vec![123.0; 8],
        )
        .await?;

    // Store elements directly in a chunk, in this case set the last row of the bottom right chunk
    array
        .async_store_chunk_subset_elements::<f32>(
            // chunk indices
            &[1, 1],
            // subset within chunk
            &ArraySubset::new_with_ranges(&[3..4, 0..4]),
            vec![-4.0; 4],
        )
        .await?;

    // Erase a chunk
    array.async_erase_chunk(&[0, 1]).await?;

    // Read the whole array
    let subset_all = ArraySubset::new_with_shape(array.shape().to_vec());
    let data_all = array
        .async_retrieve_array_subset_ndarray::<f32>(&subset_all)
        .await?;
    println!("The whole array is:\n{:?}\n", data_all);

    // Read a chunk back from the store
    let chunk_indices = vec![1, 0];
    let data_chunk = array
        .async_retrieve_chunk_ndarray::<f32>(&chunk_indices)
        .await?;
    println!("Chunk [1,0] is:\n{data_chunk:?}\n");

    // Read the central 4x2 subset of the array
    let subset_4x2 = ArraySubset::new_with_ranges(&[2..6, 3..5]); // the center 4x2 region
    let data_4x2 = array
        .async_retrieve_array_subset_ndarray::<f32>(&subset_4x2)
        .await?;
    println!("The middle 4x2 subset is:\n{:?}\n", data_4x2);

    // Show the hierarchy
    let node = Node::async_new_with_store(&*store, "/").await.unwrap();
    let tree = node.hierarchy_tree();
    println!("The zarr hierarchy tree is:\n{}", tree);

    Ok(())
}

#[tokio::main]
async fn main() {
    if let Err(err) = async_array_write_read().await {
        println!("{}", err);
    }
}
