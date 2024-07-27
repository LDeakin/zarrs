# zar<u>*rs*</u>

[![Latest Version](https://img.shields.io/crates/v/zarrs.svg)](https://crates.io/crates/zarrs)
[![zarrs documentation](https://docs.rs/zarrs/badge.svg)](https://docs.rs/zarrs)
![msrv](https://img.shields.io/crates/msrv/zarrs)
[![downloads](https://img.shields.io/crates/d/zarrs)](https://crates.io/crates/zarrs)
[![build](https://github.com/LDeakin/zarrs/actions/workflows/ci.yml/badge.svg)](https://github.com/LDeakin/zarrs/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/LDeakin/zarrs/graph/badge.svg?token=OBKJQNAZPP)](https://codecov.io/gh/LDeakin/zarrs)

`zarrs` is a Rust library for the [Zarr](https://zarr.dev) storage format for multidimensional arrays and metadata. It supports:
 - [Zarr V3](https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html), and
 - (New in 0.15) A [V3 compatible subset](https://docs.rs/zarrs/latest/zarrs/#implementation-status) of [Zarr V2](https://zarr-specs.readthedocs.io/en/latest/v2/v2.0.html).

A changelog can be found [here](https://github.com/LDeakin/zarrs/blob/main/CHANGELOG.md).
Correctness issues with past versions are [detailed here](https://github.com/LDeakin/zarrs/blob/main/doc/correctness_issues.md).

Developed at the [Department of Materials Physics](https://physics.anu.edu.au/research/mp/), Australian National University, Canberra, Australia.

## Getting Started
- Review the [implementation status](https://docs.rs/zarrs/latest/zarrs/#implementation-status).
- View the [examples](https://github.com/LDeakin/zarrs/tree/main/examples).
- Read the [documentation](https://docs.rs/zarrs/latest/zarrs/). [`array::Array`](https://docs.rs/zarrs/latest/zarrs/array/struct.Array.html), [`storage`](https://docs.rs/zarrs/latest/zarrs/storage/index.html), and [`metadata`](https://docs.rs/zarrs/latest/zarrs/metadata/index.html) are good places to start.
- Check out [zarrs_tools](https://github.com/LDeakin/zarrs_tools) for various tools built upon this crate. Includes:
  - A reencoder that can change codecs, chunk shape, convert Zarr V2 to V3, etc.
  - Create an [OME-Zarr](https://ngff.openmicroscopy.org/latest/) hierarchy from a Zarr array.
  - Transform arrays: crop, rescale, downsample, gradient magnitude, gaussian, noise filtering, etc.
  - Benchmarking tools and performance benchmarks of `zarrs`.

## Implementation Status
| [Zarr Enhancement Proposal]             | Status                     | Zarrs        |
| --------------------------------------- | -------------------------- | ------------ |
| [ZEP0001]: Zarr specification version 3 | Accepted                   | Full support |
| [ZEP0002]: Sharding codec               | Accepted                   | Full support |
| Draft [ZEP0003]: Variable chunking      | [zarr-developers #52]      | Full support |
| Draft ZEP0007: Strings                  | [zarr-developers/zeps #47] | Prototype    |

[Zarr Enhancement Proposal]: https://zarr.dev/zeps/
[ZEP0001]: https://zarr.dev/zeps/accepted/ZEP0001.html
[ZEP0002]: https://zarr.dev/zeps/accepted/ZEP0002.html
[ZEP0003]: https://zarr.dev/zeps/draft/ZEP0003.html

[zarr-developers #52]: https://github.com/orgs/zarr-developers/discussions/52
[zarr-developers/zeps #47]: https://github.com/zarr-developers/zeps/pull/47#issuecomment-1710505141

## Example
```rust
use zarrs::array::{ArrayBuilder, DataType, FillValue, ZARR_NAN_F32};
use zarrs::array::codec::GzipCodec; // requires gzip feature
use zarrs::array_subset::ArraySubset;
use zarrs::storage::{ReadableWritableListableStorage, store::FilesystemStore};

// Create a filesystem store
let store_path: PathBuf = "/path/to/store".into();
let store: ReadableWritableListableStorage =
    Arc::new(FilesystemStore::new(&store_path)?);

// Create a new V3 array using the array builder
let array = ArrayBuilder::new(
    vec![3, 4], // array shape
    DataType::Float32,
    vec![2, 2].try_into()?, // regular chunk shape (non-zero elements)
    FillValue::from(ZARR_NAN_F32),
)
.bytes_to_bytes_codecs(vec![
    Box::new(GzipCodec::new(5)?),
])
.dimension_names(["y", "x"].into())
.attributes(serde_json::json!({"Zarr V3": "is great"}).as_object().unwrap().clone())
.build(store.clone(), "/group/array")?; // /path/to/store/group/array

// Store the array metadata
array.store_metadata()?;
println!("{}", serde_json::to_string_pretty(array.metadata())?);
// {
//     "zarr_format": 3,
//     "node_type": "array",
//     ...
// }

// Perform some operations on the chunks
array.store_chunk_elements::<f32>(
    &[0, 1], // chunk index
    &[0.2, 0.3, 1.2, 1.3]
)?;
array.store_array_subset_ndarray::<f32, _>(
    &[1, 1], // array index
    ndarray::array![[-1.1, -1.2], [-2.1, -2.2]]
)?;
array.erase_chunk(&[1, 1])?;

// Retrieve all array elements as an ndarray
let array_subset_all = ArraySubset::new_with_shape(array.shape().to_vec());
let array_ndarray = array.retrieve_array_subset_ndarray::<f32>(&array_subset_all)?;
println!("{array_ndarray:4}");
// [[ NaN,  NaN,  0.2,  0.3],
//  [ NaN, -1.1, -1.2,  1.3],
//  [ NaN, -2.1,  NaN,  NaN]]
```

## `zarrs` Ecosystem
- [zarrs_tools](https://github.com/LDeakin/zarrs_tools): Various tools for creating and manipulating Zarr V3 data.
- [zarrs_ffi](https://github.com/LDeakin/zarrs_ffi): A subset of `zarrs` exposed as a C API.

## Licence
`zarrs` is licensed under either of
 - the Apache License, Version 2.0 [LICENSE-APACHE](./LICENCE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0> or
 - the MIT license [LICENSE-MIT](./LICENCE-MIT) or <http://opensource.org/licenses/MIT>, at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.
