#### Core
- [`zarrs`]: The core library for manipulating Zarr hierarchies.
- [`zarrs_data_type`]: Zarr data types (re-exported as `zarrs::data_type`).
- [`zarrs_metadata`]: Zarr metadata support (re-exported as `zarrs::metadata`).
- [`zarrs_plugin`]: The plugin API for `zarrs` (re-exported as `zarrs::plugin`).
- [`zarrs_storage`]: The storage API for `zarrs` (re-exported as `zarrs::storage`).

#### Stores
- [`zarrs_filesystem`]: A filesystem store (re-exported as `zarrs::filesystem`).
- [`zarrs_object_store`]: [`object_store`] store support.
- [`zarrs_opendal`]: [`opendal`] store support.
- [`zarrs_http`]: A synchronous http store.
- [`zarrs_zip`]: A storage adapter for zip files.
- [`zarrs_icechunk`]: [`icechunk`] store support.

#### Bindings
- [`zarrs-python`]: A high-performance codec pipeline for [`zarr-python`].
- [`zarrs_ffi`]: A subset of `zarrs` exposed as a C/C++ API.

#### Zarr Metadata Conventions
- [`ome_zarr_metadata`]: A library for OME-Zarr (previously OME-NGFF) metadata.

#### Tools
- [`zarrs_tools`]: Various tools for creating and manipulating Zarr V3 data with the zarrs rust crate
  - A reencoder that can change codecs, chunk shape, convert Zarr V2 to V3, etc.
  - Create an [OME-Zarr] hierarchy from a Zarr array.
  - Transform arrays: crop, rescale, downsample, gradient magnitude, gaussian, noise filtering, etc.
  - Benchmarking tools and performance benchmarks of `zarrs`.

[`zarrs`]: https://docs.rs/zarrs/latest/zarrs/
[`zarrs_data_type`]: https://docs.rs/zarrs_data_type/latest/zarrs_data_type/
[`zarrs_metadata`]: https://docs.rs/zarrs_metadata/latest/zarrs_metadata/
[`zarrs_plugin`]: https://docs.rs/zarrs_plugin/latest/zarrs_plugin/
[`zarrs_storage`]: https://docs.rs/zarrs_storage/latest/zarrs_storage/
[`zarrs_filesystem`]: https://docs.rs/zarrs_filesystem/latest/zarrs_filesystem/
[`zarrs_http`]: https://docs.rs/zarrs_http/latest/zarrs_http/
[`zarrs_object_store`]: https://docs.rs/zarrs_object_store/latest/zarrs_object_store/
[`zarrs_opendal`]: https://docs.rs/zarrs_opendal/latest/zarrs_opendal/
[`zarrs_zip`]: https://docs.rs/zarrs_zip/latest/zarrs_zip/
[`zarrs_icechunk`]: https://docs.rs/zarrs_icechunk/latest/zarrs_icechunk/
[`zarrs_ffi`]: https://docs.rs/zarrs_ffi/latest/zarrs_ffi/
[`zarrs-python`]: https://github.com/ilan-gold/zarrs-python
[`zarr-python`]: https://github.com/zarr-developers/zarr-python
[`zarrs_tools`]: https://docs.rs/zarrs_tools/latest/zarrs_tools/
[`ome_zarr_metadata`]: https://docs.rs/ome_zarr_metadata/latest/ome_zarr_metadata/
[`object_store`]: https://docs.rs/object_store/latest/object_store/
[`opendal`]: https://docs.rs/opendal/latest/opendal/
[`icechunk`]: https://github.com/earth-mover/icechunk

[OME-Zarr]: https://ngff.openmicroscopy.org/latest/
