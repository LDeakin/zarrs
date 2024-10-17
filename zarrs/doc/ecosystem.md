| Crate                | Description                                                                       | Version                                                                  | Docs                                                            |
| -------------------- | --------------------------------------------------------------------------------- | ------------------------------------------------------------------------ | --------------------------------------------------------------- |
| **Core**             |
| `zarrs`              | The core library for manipulating Zarr hierarchies                                | [![zarrs_ver]](https://crates.io/crates/zarrs)                           | [![zarrs_doc]](https://docs.rs/zarrs)                           |
| `zarrs_metadata`     | Zarr metadata support                                                             | [![zarrs_metadata_ver]](https://crates.io/crates/zarrs_metadata)         | [![zarrs_metadata_doc]](https://docs.rs/zarrs_metadata)         |
| `zarrs_storage`      | The storage API for `zarrs`                                                       | [![zarrs_storage_ver]](https://crates.io/crates/zarrs_storage)           | [![zarrs_storage_doc]](https://docs.rs/zarrs_storage)           |
| **Stores**           |
| `zarrs_filesystem`   | A filesystem store                                                                | [![zarrs_filesystem_ver]](https://crates.io/crates/zarrs_filesystem)     | [![zarrs_filesystem_doc]](https://docs.rs/zarrs_filesystem)     |
| `zarrs_object_store` | [`object_store`](https://docs.rs/object_store/latest/object_store/) store support | [![zarrs_object_store_ver]](https://crates.io/crates/zarrs_object_store) | [![zarrs_object_store_doc]](https://docs.rs/zarrs_object_store) |
| `zarrs_opendal`      | [`opendal`](https://docs.rs/opendal/latest/opendal/) store support                | [![zarrs_opendal_ver]](https://crates.io/crates/zarrs_opendal)           | [![zarrs_opendal_doc]](https://docs.rs/zarrs_opendal)           |
| `zarrs_http`         | A synchronous http store                                                          | [![zarrs_http_ver]](https://crates.io/crates/zarrs_http)                 | [![zarrs_http_doc]](https://docs.rs/zarrs_http)                 |
| `zarrs_zip`          | A storage adapter for zip files                                                   | [![zarrs_zip_ver]](https://crates.io/crates/zarrs_zip)                   | [![zarrs_zip_doc]](https://docs.rs/zarrs_zip)                   |
| `zarrs_icechunk`     | [`icechunk`](https://docs.rs/icechunk/latest/icechunk/) store support             | [![zarrs_icechunk_ver]](https://crates.io/crates/zarrs_icechunk)         | [![zarrs_icechunk_doc]](https://docs.rs/zarrs_icechunk)         |
| **Bindings**         |
| [zarrs_ffi]          | A subset of `zarrs` exposed as a C/C++ API                                        | [![zarrs_ffi_ver]](https://crates.io/crates/zarrs_ffi)                   | [![zarrs_ffi_doc]](https://docs.rs/zarrs_ffi)                   |

[zarrs_ver]: https://img.shields.io/crates/v/zarrs?label=
[zarrs_doc]: https://img.shields.io/docsrs/zarrs?label=
[zarrs_metadata_ver]: https://img.shields.io/crates/v/zarrs_metadata?label=
[zarrs_metadata_doc]: https://img.shields.io/docsrs/zarrs_metadata?label=
[zarrs_storage_ver]: https://img.shields.io/crates/v/zarrs_storage?label=
[zarrs_storage_doc]: https://img.shields.io/docsrs/zarrs_storage?label=
[zarrs_filesystem_ver]: https://img.shields.io/crates/v/zarrs_filesystem?label=
[zarrs_filesystem_doc]: https://img.shields.io/docsrs/zarrs_filesystem?label=
[zarrs_http_ver]: https://img.shields.io/crates/v/zarrs_http?label=
[zarrs_http_doc]: https://img.shields.io/docsrs/zarrs_http?label=
[zarrs_object_store_ver]: https://img.shields.io/crates/v/zarrs_object_store?label=
[zarrs_object_store_doc]: https://img.shields.io/docsrs/zarrs_object_store?label=
[zarrs_opendal_ver]: https://img.shields.io/crates/v/zarrs_opendal?label=
[zarrs_opendal_doc]: https://img.shields.io/docsrs/zarrs_opendal?label=
[zarrs_zip_ver]: https://img.shields.io/crates/v/zarrs_zip?label=
[zarrs_zip_doc]: https://img.shields.io/docsrs/zarrs_zip?label=
[zarrs_icechunk_ver]: https://img.shields.io/crates/v/zarrs_icechunk?label=
[zarrs_icechunk_doc]: https://img.shields.io/docsrs/zarrs_icechunk?label=
[zarrs_ffi_ver]: https://img.shields.io/crates/v/zarrs_ffi?label=
[zarrs_ffi_doc]: https://img.shields.io/docsrs/zarrs_ffi?label=
[zarrs_ffi]: https://github.com/LDeakin/zarrs_ffi
[zarrs_tools_ver]: https://img.shields.io/crates/v/zarrs_tools
[zarrs_tools_doc]: https://img.shields.io/docsrs/zarrs_tools

#### [zarrs_tools]
[![zarrs_tools_ver]](https://crates.io/crates/zarrs_tools) [![zarrs_tools_doc]](https://docs.rs/zarrs_tools)

[zarrs_tools]: https://github.com/LDeakin/zarrs_tools
[zarrs_tools_ver]: https://img.shields.io/crates/v/zarrs_tools.svg
[zarrs_tools_doc]: https://docs.rs/zarrs_tools/badge.svg

  - A reencoder that can change codecs, chunk shape, convert Zarr V2 to V3, etc.
  - Create an [OME-Zarr](https://ngff.openmicroscopy.org/latest/) hierarchy from a Zarr array.
  - Transform arrays: crop, rescale, downsample, gradient magnitude, gaussian, noise filtering, etc.
  - Benchmarking tools and performance benchmarks of `zarrs`.
