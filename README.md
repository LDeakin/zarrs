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
- Review the [implementation status](https://docs.rs/zarrs/latest/zarrs/#implementation-status), [array support](https://docs.rs/zarrs/latest/zarrs/#array-support), and [storage support](https://docs.rs/zarrs/latest/zarrs/#storage-support).
- View the [examples](https://github.com/LDeakin/zarrs/tree/main/examples) and [the example below](#example).
- Read the [documentation](https://docs.rs/zarrs/latest/zarrs/). [`array::Array`](https://docs.rs/zarrs/latest/zarrs/array/struct.Array.html) is a good place to start.
- Check out the [`zarrs` ecosystem](#zarrs-ecosystem).

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
use zarrs::group::GroupBuilder;
use zarrs::array::{ArrayBuilder, DataType, FillValue, ZARR_NAN_F32};
use zarrs::array::codec::GzipCodec; // requires gzip feature
use zarrs::array_subset::ArraySubset;
use zarrs::storage::ReadableWritableListableStorage;
use zarrs_filesystem::FilesystemStore;

// Create a filesystem store
let store_path: PathBuf = "/path/to/hierarchy.zarr".into();
let store: ReadableWritableListableStorage =
    Arc::new(FilesystemStore::new(&store_path)?);

// Write the root group metadata
GroupBuilder::new()
    .build(store.clone(), "/")?
    // .attributes(...)
    .store_metadata()?;

// Create a new V3 array using the array builder
let array = ArrayBuilder::new(
    vec![3, 4], // array shape
    DataType::Float32,
    vec![2, 2].try_into()?, // regular chunk shape (non-zero elements)
    FillValue::from(ZARR_NAN_F32),
)
.bytes_to_bytes_codecs(vec![
    Arc::new(GzipCodec::new(5)?),
])
.dimension_names(["y", "x"].into())
.attributes(serde_json::json!({"Zarr V3": "is great"}).as_object().unwrap().clone())
.build(store.clone(), "/array")?; // /path/to/hierarchy.zarr/array

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

| Crate | Description | Version | Docs |
| ----- | ----------- | ------- | ---- |
| **Core** |
| `zarrs` | The core library for manipulating Zarr hierarchies | [![zarrs_ver]](https://crates.io/crates/zarrs) | [![zarrs_doc]](https://docs.rs/zarrs) |
| `zarrs_metadata` | Zarr metadata support | [![zarrs_metadata_ver]](https://crates.io/crates/zarrs_metadata) | [![zarrs_metadata_doc]](https://docs.rs/zarrs_metadata) |
| `zarrs_storage` | The storage API for `zarrs` | [![zarrs_storage_ver]](https://crates.io/crates/zarrs_storage) | [![zarrs_storage_doc]](https://docs.rs/zarrs_storage) |
| **Stores** |
| `zarrs_filesystem` | A filesystem store | [![zarrs_filesystem_ver]](https://crates.io/crates/zarrs_filesystem) | [![zarrs_filesystem_doc]](https://docs.rs/zarrs_filesystem) |
| `zarrs_http` | A synchronous http store | [![zarrs_http_ver]](https://crates.io/crates/zarrs_http) | [![zarrs_http_doc]](https://docs.rs/zarrs_http) |
| `zarrs_object_store` | [`object_store`](https://docs.rs/object_store/latest/object_store/) store support | [![zarrs_object_store_ver]](https://crates.io/crates/zarrs_object_store) | [![zarrs_object_store_doc]](https://docs.rs/zarrs_object_store) |
| `zarrs_opendal` | [`opendal`](https://docs.rs/opendal/latest/opendal/) store support | [![zarrs_opendal_ver]](https://crates.io/crates/zarrs_opendal) | [![zarrs_opendal_doc]](https://docs.rs/zarrs_opendal) |
| `zarrs_zip` | A storage adapter for zip files | [![zarrs_zip_ver]](https://crates.io/crates/zarrs_zip) | [![zarrs_zip_doc]](https://docs.rs/zarrs_zip) |
| **Bindings** |
| [zarrs_ffi] | A subset of `zarrs` exposed as a C/C++ API | [![zarrs_ffi_ver]](https://crates.io/crates/zarrs_ffi) | [![zarrs_ffi_doc]](https://docs.rs/zarrs_ffi) |

[zarrs_ver]: https://img.shields.io/crates/v/zarrs.svg
[zarrs_doc]: https://docs.rs/zarrs/badge.svg
[zarrs_metadata_ver]: https://img.shields.io/crates/v/zarrs_metadata.svg
[zarrs_metadata_doc]: https://docs.rs/zarrs_metadata/badge.svg
[zarrs_storage_ver]: https://img.shields.io/crates/v/zarrs_storage.svg
[zarrs_storage_doc]: https://docs.rs/zarrs_storage/badge.svg
[zarrs_filesystem_ver]: https://img.shields.io/crates/v/zarrs_filesystem.svg
[zarrs_filesystem_doc]: https://docs.rs/zarrs_filesystem/badge.svg
[zarrs_http_ver]: https://img.shields.io/crates/v/zarrs_http.svg
[zarrs_http_doc]: https://docs.rs/zarrs_http/badge.svg
[zarrs_object_store_ver]: https://img.shields.io/crates/v/zarrs_object_store.svg
[zarrs_object_store_doc]: https://docs.rs/zarrs_object_store/badge.svg
[zarrs_opendal_ver]: https://img.shields.io/crates/v/zarrs_opendal.svg
[zarrs_opendal_doc]: https://docs.rs/zarrs_opendal/badge.svg
[zarrs_zip_ver]: https://img.shields.io/crates/v/zarrs_zip.svg
[zarrs_zip_doc]: https://docs.rs/zarrs_zip/badge.svg
[zarrs_ffi]: https://github.com/LDeakin/zarrs_ffi
[zarrs_ffi_ver]: https://img.shields.io/crates/v/zarrs_ffi.svg
[zarrs_ffi_doc]: https://docs.rs/zarrs_ffi/badge.svg

#### [zarrs_tools] [![zarrs_tools_ver]](https://crates.io/crates/zarrs_tools) [![zarrs_tools_doc]](https://docs.rs/zarrs_tools)

[zarrs_tools]: https://github.com/LDeakin/zarrs_tools
[zarrs_tools_ver]: https://img.shields.io/crates/v/zarrs_tools.svg
[zarrs_tools_doc]: https://docs.rs/zarrs_tools/badge.svg

  - A reencoder that can change codecs, chunk shape, convert Zarr V2 to V3, etc.
  - Create an [OME-Zarr](https://ngff.openmicroscopy.org/latest/) hierarchy from a Zarr array.
  - Transform arrays: crop, rescale, downsample, gradient magnitude, gaussian, noise filtering, etc.
  - Benchmarking tools and performance benchmarks of `zarrs`.

## Licence
`zarrs` is licensed under either of
 - the Apache License, Version 2.0 [LICENSE-APACHE](./LICENCE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0> or
 - the MIT license [LICENSE-MIT](./LICENCE-MIT) or <http://opensource.org/licenses/MIT>, at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.
