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
| `zarrs_ffi` | A subset of `zarrs` exposed as a C/C++ API | [![zarrs_ffi_ver]](https://crates.io/crates/zarrs_ffi) | [![zarrs_ffi_doc]](https://docs.rs/zarrs_ffi) |

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
[zarrs_ffi_ver]: https://img.shields.io/crates/v/zarrs_ffi.svg
[zarrs_ffi_doc]: https://docs.rs/zarrs_ffi/badge.svg
[zarrs_tools_ver]: https://img.shields.io/crates/v/zarrs_tools.svg
[zarrs_tools_doc]: https://docs.rs/zarrs_tools/badge.svg

#### `zarrs_tools` [![zarrs_tools_ver]](https://crates.io/crates/zarrs_tools) [![zarrs_tools_doc]](https://docs.rs/zarrs_tools)

[zarrs_tools_ver]: https://img.shields.io/crates/v/zarrs_tools.svg
[zarrs_tools_doc]: https://docs.rs/zarrs_tools/badge.svg

  - A reencoder that can change codecs, chunk shape, convert Zarr V2 to V3, etc.
  - Create an [OME-Zarr](https://ngff.openmicroscopy.org/latest/) hierarchy from a Zarr array.
  - Transform arrays: crop, rescale, downsample, gradient magnitude, gaussian, noise filtering, etc.
  - Benchmarking tools and performance benchmarks of `zarrs`.
