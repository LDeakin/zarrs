| Crate                                                                                         | Docs / Description                                                                                                              |
| --------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------- |
| **Core**                                                                                      |                                                                                                                                 |
| [![zarrs_ver]](https://crates.io/crates/zarrs) `zarrs`                                        | [![docs]](https://docs.rs/zarrs) The core library for manipulating Zarr hierarchies                                             |
| [![zarrs_metadata_ver]](https://crates.io/crates/zarrs_metadata) `zarrs_metadata`             | [![docs]](https://docs.rs/zarrs_metadata) Zarr metadata support                                                                 |
| [![zarrs_storage_ver]](https://crates.io/crates/zarrs_storage) `zarrs_storage`                | [![docs]](https://docs.rs/zarrs_storage) The storage API for `zarrs`                                                            |
| **Stores**                                                                                    |                                                                                                                                 |
| [![zarrs_filesystem_ver]](https://crates.io/crates/zarrs_filesystem) `zarrs_filesystem`       | [![docs]](https://docs.rs/zarrs_filesystem) A filesystem store                                                                  |
| [![zarrs_object_store_ver]](https://crates.io/crates/zarrs_object_store) `zarrs_object_store` | [![docs]](https://docs.rs/zarrs_object_store) [`object_store`](https://docs.rs/object_store/latest/object_store/) store support |
| [![zarrs_opendal_ver]](https://crates.io/crates/zarrs_opendal) `zarrs_opendal`                | [![docs]](https://docs.rs/zarrs_opendal) [`opendal`](https://docs.rs/opendal/latest/opendal/) store support                     |
| [![zarrs_http_ver]](https://crates.io/crates/zarrs_http) `zarrs_http`                         | [![docs]](https://docs.rs/zarrs_http) A synchronous http store                                                                  |
| [![zarrs_zip_ver]](https://crates.io/crates/zarrs_zip) `zarrs_zip`                            | [![docs]](https://docs.rs/zarrs_zip) A storage adapter for zip files                                                            |
| [![zarrs_icechunk_ver]](https://crates.io/crates/zarrs_icechunk) [zarrs_icechunk]             | [![docs]](https://docs.rs/zarrs_icechunk) [`icechunk`](https://docs.rs/icechunk/latest/icechunk/) store support                 |
| **Bindings**                                                                                  |                                                                                                                                 |
| [![zarrs_python_ver]](https://pypi.org/project/zarrs/) [zarrs-python]                         | [![docs]](https://zarrs-python.readthedocs.io/en/latest/) A codec pipeline for [zarr-python]                                  |
| [![zarrs_ffi_ver]](https://crates.io/crates/zarrs_ffi) [zarrs_ffi]                            | [![docs]](https://docs.rs/zarrs_ffi) A subset of `zarrs` exposed as a C/C++ API                                                 |
| **Zarr Metadata Conventions**                                                                 |                                                                                                                                 |
| [![ome_zarr_metadata_ver]](https://crates.io/crates/ome_zarr_metadata) [ome_zarr_metadata]    | [![docs]](https://docs.rs/ome_zarr_metadata)  A library for OME-Zarr (previously OME-NGFF) metadata                             |

[docs]: https://img.shields.io/badge/docs-brightgreen
[zarrs_ver]: https://img.shields.io/crates/v/zarrs
[zarrs_metadata_ver]: https://img.shields.io/crates/v/zarrs_metadata
[zarrs_storage_ver]: https://img.shields.io/crates/v/zarrs_storage
[zarrs_filesystem_ver]: https://img.shields.io/crates/v/zarrs_filesystem
[zarrs_http_ver]: https://img.shields.io/crates/v/zarrs_http
[zarrs_object_store_ver]: https://img.shields.io/crates/v/zarrs_object_store
[zarrs_opendal_ver]: https://img.shields.io/crates/v/zarrs_opendal
[zarrs_zip_ver]: https://img.shields.io/crates/v/zarrs_zip
[zarrs_icechunk_ver]: https://img.shields.io/crates/v/zarrs_icechunk
[zarrs_icechunk]: https://github.com/LDeakin/zarrs_icechunk
[zarrs_ffi_ver]: https://img.shields.io/crates/v/zarrs_ffi
[zarrs_ffi]: https://github.com/LDeakin/zarrs_ffi
[zarrs_tools_ver]: https://img.shields.io/crates/v/zarrs_tools
[zarrs_python_ver]: https://img.shields.io/pypi/v/zarrs
[zarrs-python]: https://github.com/ilan-gold/zarrs-python
[zarr-python]: https://github.com/zarr-developers/zarr-python
[ome_zarr_metadata_ver]: https://img.shields.io/crates/v/ome_zarr_metadata
[ome_zarr_metadata]: https://github.com/LDeakin/rust_ome_zarr_metadata

#### [zarrs_tools]
[![zarrs_tools_ver]](https://crates.io/crates/zarrs_tools) [![zarrs_tools_doc]](https://docs.rs/zarrs_tools)

[zarrs_tools]: https://github.com/LDeakin/zarrs_tools
[zarrs_tools_ver]: https://img.shields.io/crates/v/zarrs_tools.svg
[zarrs_tools_doc]: https://docs.rs/zarrs_tools/badge.svg

  - A reencoder that can change codecs, chunk shape, convert Zarr V2 to V3, etc.
  - Create an [OME-Zarr](https://ngff.openmicroscopy.org/latest/) hierarchy from a Zarr array.
  - Transform arrays: crop, rescale, downsample, gradient magnitude, gaussian, noise filtering, etc.
  - Benchmarking tools and performance benchmarks of `zarrs`.
