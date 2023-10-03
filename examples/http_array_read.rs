#[cfg(all(feature = "ndarray", feature = "gzip"))]
fn http_array_read() -> Result<(), Box<dyn std::error::Error>> {
    use std::sync::Arc;
    use zarrs::{array::Array, array_subset::ArraySubset, storage::store};

    const HTTP_URL: &'static str =
        "https://raw.githubusercontent.com/LDeakin/zarrs/main/tests/data/array_write_read.zarr";
    const ARRAY_PATH: &'static str = "/group/array";

    // Create a HTTP store
    let store = Arc::new(store::HTTPStore::new(HTTP_URL)?);

    // Init the existing array, reading metadata
    let array = Array::new(store, ARRAY_PATH)?;

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

    Ok(())
}

#[cfg(any(not(feature = "ndarray"), not(feature = "gzip")))]
fn http_array_read() -> Result<(), Box<dyn std::error::Error>> {
    panic!("the http_array_read example requires the ndarray and gzip feature")
}

fn main() {
    if let Err(err) = http_array_read() {
        println!("{}", err);
    }
}
