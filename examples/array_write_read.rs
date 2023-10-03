#[cfg(feature = "ndarray")]
fn array_write_read() -> Result<(), Box<dyn std::error::Error>> {
    use rayon::prelude::{IntoParallelIterator, ParallelIterator};
    use zarrs::{
        array::{chunk_grid::ChunkGridTraits, DataType},
        array::{codec, FillValue},
        array_subset::ArraySubset,
        node::Node,
        storage::store,
    };

    // Create a store
    // let path = tempfile::TempDir::new()?;
    // let store = Arc::new(store::FilesystemStore::new(path.path())?);
    let store = std::sync::Arc::new(store::MemoryStore::default());

    // Create a group and write metadata to filesystem
    let group_path = "/group";
    let mut group = zarrs::group::GroupBuilder::new().build(store.clone(), group_path)?;

    // Update group metadata
    group
        .attributes_mut()
        .insert("foo".into(), serde_json::Value::String("bar".into()));

    // Write group metadata to store
    group.store_metadata()?;

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
        FillValue::from(f32::NAN),
    )
    .bytes_to_bytes_codecs(vec![
        #[cfg(feature = "gzip")]
        Box::new(codec::GzipCodec::new(5)?),
    ])
    .dimension_names(vec!["y".into(), "x".into()])
    .storage_transformers(vec![])
    .build(store.clone(), array_path)?;

    // Write array metadata to store
    array.store_metadata()?;

    // Write some chunks (in parallel)
    (0..2)
        .into_par_iter()
        .map(|i| {
            let chunk_grid: &Box<dyn ChunkGridTraits> = array.chunk_grid();
            let chunk_indices: Vec<u64> = vec![i, 0];
            let chunk_subset: ArraySubset = chunk_grid.subset(&chunk_indices, &array.shape())?;
            array.store_chunk_elements(
                &chunk_indices,
                &vec![i as f32; chunk_subset.num_elements() as usize],
            )
            // let chunk_shape = chunk_grid.chunk_shape(&chunk_indices, &array.shape())?;
            // let chunk_array = ndarray::ArrayD::<f32>::from_elem(chunk_shape.clone(), i as f32);
            // array.store_chunk_ndarray(&chunk_indices, &chunk_array.view())
        })
        .collect::<Vec<_>>();

    println!(
        "The array metadata is:\n{}\n",
        serde_json::to_string_pretty(&array.metadata()).unwrap()
    );

    // Write a subset spanning multiple chunks, including updating chunks already written
    array.store_array_subset_elements::<f32>(
        &ArraySubset::new_with_start_shape(vec![3, 3], vec![3, 3]).unwrap(),
        &vec![0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9],
    )?;

    // Store elements directly, in this case set the 7th column to 123.0
    array.store_array_subset_elements::<f32>(
        &ArraySubset::new_with_start_shape(vec![0, 6], vec![8, 1])?,
        &vec![123.0; 8],
    )?;

    // Store elements directly in a chunk, in this case set the last row of the bottom right chunk
    array.store_chunk_subset_elements::<f32>(
        // chunk indices
        &[1, 1],
        // subset within chunk
        &ArraySubset::new_with_start_shape(vec![3, 0], vec![1, 4])?,
        &vec![-4.0; 4],
    )?;

    // Read the whole array
    let subset_all = ArraySubset::new_with_start_shape(vec![0, 0], array.shape().to_vec())?;
    let data_all = array.retrieve_array_subset_ndarray::<f32>(&subset_all)?;
    println!("The whole array is:\n{:?}\n", data_all);

    // Read a chunk back from the store
    let chunk_indices = vec![1, 0];
    let data_chunk = array.retrieve_chunk_ndarray::<f32>(&chunk_indices)?;
    println!("Chunk [1,0] is:\n{data_chunk:?}\n");

    // Read the central 2x2 subset of the array
    let subset_2x2 = ArraySubset::new_with_start_shape(vec![3, 3], vec![2, 2])?; // the center 2x2 region
    let data_2x2 = array.retrieve_array_subset_ndarray::<f32>(&subset_2x2)?;
    println!("The middle 2x2 subset is:\n{:?}\n", data_2x2);

    // Show the hierarchy
    let node = Node::new_with_store(&*store, "/").unwrap();
    let tree = node.hierarchy_tree();
    println!("The zarr hierarchy tree is:\n{}", tree);

    Ok(())
}

#[cfg(not(feature = "ndarray"))]
fn array_write_read() -> Result<(), Box<dyn std::error::Error>> {
    panic!("the array_write_read example requires the ndarray feature")
}

fn main() {
    if let Err(err) = array_write_read() {
        println!("{}", err);
    }
}
