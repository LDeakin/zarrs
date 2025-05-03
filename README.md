# zar<ins>rs</ins>

[![Latest Version](https://img.shields.io/crates/v/zarrs.svg)](https://crates.io/crates/zarrs)
[![zarrs documentation](https://docs.rs/zarrs/badge.svg)][documentation]
![msrv](https://img.shields.io/crates/msrv/zarrs)
[![downloads](https://img.shields.io/crates/d/zarrs)](https://crates.io/crates/zarrs)
[![build](https://github.com/LDeakin/zarrs/actions/workflows/ci.yml/badge.svg)](https://github.com/LDeakin/zarrs/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/LDeakin/zarrs/graph/badge.svg?component=zarrs)](https://codecov.io/gh/LDeakin/zarrs)
[![DOI](https://zenodo.org/badge/695021547.svg)](https://zenodo.org/badge/latestdoi/695021547)

`zarrs` is a Rust library for the [Zarr] storage format for multidimensional arrays and metadata.

> [!TIP]
> If you are a Python user, check out [`zarrs-python`].
> It includes a high-performance codec pipeline for the reference [`zarr-python`] implementation.

`zarrs` supports [Zarr V3] and a V3 compatible subset of [Zarr V2].
It is fully up-to-date and conformant with the Zarr 3.1 specification with support for:
- all *core extensions* (data types, codecs, chunk grids, chunk key encodings, storage transformers),
- all accepted [Zarr Enhancement Proposals (ZEPs)](https://zarr.dev/zeps/) and several draft ZEPs:
  - ZEP 0003: Variable chunking
  - ZEP 0007: Strings
  - ZEP 0009: Zarr Extension Naming
- various registered extensions from [zarr-developers/zarr-extensions/](https://github.com/zarr-developers/zarr-extensions/),
- experimental codecs and data types intended for future registration, and
- user-defined custom extensions and stores.

A changelog can be found [here][CHANGELOG].
Correctness issues with past versions are [detailed here][correctness_issues].

Developed at the [Department of Materials Physics, Australian National University, Canberra, Australia].

## Getting Started
- Review the [implementation status] which summarises zarr version support, array support (codecs, data types, etc.) and storage support.
- Read [The `zarrs` Book].
- View the [examples] and [the example below](#example).
- Read the [documentation].
- Check out the [`zarrs` ecosystem](#zarrs-ecosystem).

## Example
```rust
use zarrs::group::GroupBuilder;
use zarrs::array::{ArrayBuilder, DataType, FillValue, ZARR_NAN_F32};
use zarrs::array::codec::GzipCodec; // requires gzip feature
use zarrs::array_subset::ArraySubset;
use zarrs::storage::ReadableWritableListableStorage;
use zarrs::filesystem::FilesystemStore; // requires filesystem feature

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
println!("{}", array.metadata().to_string_pretty());
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
    &[1, 1], // array index (start of subset)
    ndarray::array![[-1.1, -1.2], [-2.1, -2.2]]
)?;
array.erase_chunk(&[1, 1])?;

// Retrieve all array elements as an ndarray
let array_ndarray = array.retrieve_array_subset_ndarray::<f32>(&array.subset_all())?;
println!("{array_ndarray:4}");
// [[ NaN,  NaN,  0.2,  0.3],
//  [ NaN, -1.1, -1.2,  1.3],
//  [ NaN, -2.1,  NaN,  NaN]]
```

## `zarrs` Ecosystem

The Zarr specification is inherently unstable.
It is under active development and new extensions are continually being introduced.

The `zarrs` crate has been split into multiple crates to:
- allow external implementations of stores and extensions points to target a relatively stable API compatible with a range of `zarrs` versions,
- enable automatic backporting of metadata compatibility fixes and changes due to standardisation,
- stay up-to-date with unstable public dependencies (e.g. `opendal`, `object_store`, `icechunk`, etc) without impacting the release cycle of `zarrs`, and
- improve compilation times.

### Core
- [`zarrs`]: The core library for manipulating Zarr hierarchies.
- [`zarrs_metadata`]: Zarr metadata support (re-exported as `zarrs::metadata`).
- [`zarrs_data_type`]: The data type extension API for `zarrs` (re-exported in `zarrs::array::data_type`).
- [`zarrs_storage`]: The storage API for `zarrs` (re-exported as `zarrs::storage`).
- [`zarrs_plugin`]: The plugin API for `zarrs` (re-exported as `zarrs::plugin`).
- [`zarrs_registry`]: The Zarr extension point registry for `zarrs` (re-exported as `zarrs::registry`).

### Stores
- [`zarrs_filesystem`]: A filesystem store (re-exported as `zarrs::filesystem`).
- [`zarrs_object_store`]: [`object_store`] store support.
- [`zarrs_opendal`]: [`opendal`] store support.
- [`zarrs_http`]: A synchronous http store.
- [`zarrs_zip`]: A storage adapter for zip files.
- [`zarrs_icechunk`]: [`icechunk`] store support.
  - `git`-like version control for Zarr hierachies.
  - Read "virtual Zarr datacubes" of archival formats (e.g., [`netCDF4`](https://www.unidata.ucar.edu/software/netcdf/), [`HDF5`](https://www.hdfgroup.org/solutions/hdf5/), etc.) created by [`VirtualiZarr`](https://github.com/zarr-developers/VirtualiZarr) and backed by [`icechunk`].

### Bindings
- [`zarrs-python`]: A high-performance codec pipeline for [`zarr-python`].
- [`zarrs_ffi`]: A subset of `zarrs` exposed as a C/C++ API.

### Zarr Metadata Conventions
- [`ome_zarr_metadata`]: A library for OME-Zarr (previously OME-NGFF) metadata.

### Tools
- [`zarrs_tools`]: Various tools for creating and manipulating Zarr V3 data with the `zarrs` rust crate
  - A reencoder that can change codecs, chunk shape, convert Zarr V2 to V3, etc.
  - Create an [OME-Zarr] hierarchy from a Zarr array.
  - Transform arrays: crop, rescale, downsample, gradient magnitude, gaussian, noise filtering, etc.
  - Benchmarking tools and performance benchmarks of `zarrs`.

## Licence
`zarrs` is licensed under either of
 - the Apache License, Version 2.0 [LICENSE-APACHE](./LICENCE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0> or
 - the MIT license [LICENSE-MIT](./LICENCE-MIT) or <http://opensource.org/licenses/MIT>, at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.

[CHANGELOG]: https://github.com/LDeakin/zarrs/blob/main/CHANGELOG.md
[correctness_issues]: https://github.com/LDeakin/zarrs/blob/main/doc/correctness_issues.md
[implementation status]: https://docs.rs/zarrs/latest/zarrs/#implementation-status
[examples]: https://github.com/LDeakin/zarrs/tree/main/zarrs/examples
[documentation]: https://docs.rs/zarrs/latest/zarrs/
[The `zarrs` Book]: https://book.zarrs.dev

[`zarrs`]: https://github.com/LDeakin/zarrs/tree/main/zarrs
[`zarrs_data_type`]: https://github.com/LDeakin/zarrs/tree/main/zarrs_data_type
[`zarrs_metadata`]: https://github.com/LDeakin/zarrs/tree/main/zarrs_metadata
[`zarrs_plugin`]: https://github.com/LDeakin/zarrs/tree/main/zarrs_plugin
[`zarrs_registry`]: https://docs.rs/zarrs_plugin/latest/zarrs_registry/
[`zarrs_storage`]: https://github.com/LDeakin/zarrs/tree/main/zarrs_storage
[`zarrs_filesystem`]: https://github.com/LDeakin/zarrs/tree/main/zarrs_filesystem
[`zarrs_http`]: https://github.com/LDeakin/zarrs/tree/main/zarrs_http
[`zarrs_object_store`]: https://github.com/LDeakin/zarrs/tree/main/zarrs_object_store
[`zarrs_opendal`]: https://github.com/LDeakin/zarrs/tree/main/zarrs_opendal
[`zarrs_zip`]: https://github.com/LDeakin/zarrs/tree/main/zarrs_zip
[`zarrs_icechunk`]: https://github.com/LDeakin/zarrs_icechunk
[`zarrs_ffi`]: https://github.com/LDeakin/zarrs_ffi
[`zarrs-python`]: https://github.com/ilan-gold/zarrs-python
[`zarr-python`]: https://github.com/zarr-developers/zarr-python
[`zarrs_tools`]: https://github.com/LDeakin/zarrs_tools
[`ome_zarr_metadata`]: https://github.com/LDeakin/rust_ome_zarr_metadata
[`object_store`]: https://github.com/apache/arrow-rs/tree/main/object_store
[`opendal`]: https://github.com/apache/OpenDAL
[`icechunk`]: https://github.com/earth-mover/icechunk

[Zarr]: https://zarr.dev
[Zarr V3]: https://zarr-specs.readthedocs.io/en/latest/v3/core/index.html
[Zarr V2]: https://zarr-specs.readthedocs.io/en/latest/v2/v2.0.html
[OME-Zarr]: https://ngff.openmicroscopy.org/latest/

[Department of Materials Physics, Australian National University, Canberra, Australia]: https://physics.anu.edu.au/research/mp/
