# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Add support for a `must_understand` field to `MetadataV3` (ZEP0009)
  - Extensions can now be parsed in more than just the additional fields of array/group metadata (e.g. codecs)
  - Automatically skip unsupported codecs/storage transformers with `"must_understand": false`
- Add `ExtensionAliases[Codec,DataType]{V2,V3}` and `Extension{Name,Alias}Map` for extension `name` overriding and aliasing
- Implement `From<T> for MetadataConfiguration` for all codec configuration enums
- Implement `Copy` for `ZstdCompressionLevel`
- Add `zfpy` codec metadata (unmerged from `zfp`)
- Add `MetadataConfigurationSerialize` trait
- Add `ExtensionType` marker trait and `ExtensionType{DataType,ChunkGrid,ChunkKeyEncoding,Codec,StorageTranformer}`
- Add `{Array,Group,Node}Metadata::to_string_pretty()`
- Add `{Array,Group}Metadata{V2,V3}::to_string_pretty()`
- Add `zlib` codec metadata
- Add `shuffle` codec metadata
- Add `MetadataV3::set_name()`
- Add `MetadataV2::set_id()`
- Add `ZarrVersion` marker trait and `ZarrVersion{2,3}`
- Add re-exports for `v3::array::{chunk_grid,chunk_key_encoding,codec,data_type}` at the crate root

### Changed
- **Breaking**: Refactor `FillValueMetadataV3` to support arbitrary fill value metadata (for ZEP0009)
- **Breaking**: Mark versioned codec metadata as non-exhaustive
- **Breaking**: `{array,codec}_metadata_v2_to_v3` have an additional `ExtensionAliasesCodec` parameter
  - `zarrs` has a default codec map accessible via `zarrs::config::global_config().codec_aliases()`
- **Breaking**: Remove `write_header` from `zfp` codec configuration
- Bump `half` to 2.3.1
- Bump `thiserror` to 2.0.12
- **Breaking**: `DataType::from_metadata()` now takes an owned `MetadataV3` instead of a reference
- **Breaking**: `MetadataV3::new[_with_{configuration,serializable_configuration}]` now take a `String` name instead of `&str`
- **Breaking**: Move `v3::array::data_type::DataTypeSize` to the crate root
- **Breaking**: Rename `v2_to_v3::array_metadata_fill_value_v2_to_v3` to `fill_value_metadata_v2_to_v3`
- **Breaking**: Rename `v2_to_v3::data_type_metadata_v2_to_v3_data_type` to `data_type_metadata_v2_to_v3`

### Removed
- **Breaking**: Remove `fill_value::{HexString,FillValueFloat,FillValueFloatStringNonFinite}`
- **Breaking**: Remove all functions in `v3::array::fill_value`
- **Breaking**: Remove all `FillValueMetadataV3::try_as_*()` methods
- **Breaking**: Remove `DataTypeMetadataV3`
- **Breaking**: Remove the `v2::array::codec` module which just contained re-exports of V3 codecs
- **Breaking**: Remove `v2_to_v3::DataTypeMetadataV2UnsupportedDataTypeError`
- **Breaking**: Remove the `Metadata` alias in the crate root (deprecated since 0.17)

## [0.3.6] - 2025-03-02

### Added
- Make `FillValueFloat::to_float` public

### Changed
- Bump `derive_more` to 0.2.0

## [0.3.5] - 2025-02-18

### Fixed
- Ensure that Zarr V2 array metadata with empty `filters` is serialised as `null` instead of `[]`

## [0.3.4] - 2025-02-13

### Added
- Add `UnsupportedAdditionalFieldError::new`

### Fixed
- Make `AdditionalField` public and permit any JSON type (not just objects)

## [0.3.3] - 2025-02-06

### Fixed
- Permit string compression levels in `zstd` codec metadata (for `zarr-python` compatibility)
- Use `bytes` codec with native endianness if unset for a Zarr V2 array

## [0.3.2] - 2025-02-04

### Added
- Derive `Copy` for `ArrayMetadataV2Order`
- Add `codec_metadata_v2_to_v3`

### Fixed
- Interpret a `0` fill value as `""` for Zarr V2 string arrays (for `zarr-python` compatibility) ([#140] by [@zqfang])

[#140]: https://github.com/LDeakin/zarrs/pull/140

## [0.3.1] - 2025-01-29

### Fixed
- Interpret a `null` fill value as `""` for Zarr V2 string arrays (for `zarr-python` compatibility)

## [0.3.0] - 2025-01-10

### Added
- Add `v3::group::{ConsolidatedMetadata,ConsolidatedMetadataMetadata,ConsolidatedMetadataKind}`
- Add `GroupMetadataV3::consolidated_metadata` field
- Add `GroupMetadataV3::with_consolidated_metadata` field
- Add `fletcher32` codec metadata
- Add numcodecs zfpy configuration support to `ZfpCodecConfiguration` for decoding V3 arrays that use `numcodecs.zfpy`

### Changed
- **Breaking**: Rename `DataTypeMetadataV3::Binary` to `Bytes` for compatibility with `zarr-python`
- **Breaking**: Revise `PcodecCodecConfiguration` to match `numcodecs`:
  - Adds `delta_spec: PcodecDeltaSpecConfiguration` and `paging_spec: PcodecPagingSpecConfiguration`
  - Removes `PcodecModeSpecConfiguration::{TryFloatMult,TryFloatQuant,TryIntMult}`
- **Breaking**: Refactor `ZfpyCodecConfigurationNumcodecs` and `ZfpyCodecConfigurationMode` to validate on deserialisation
  - `codec_zfpy_v2_numcodecs_to_v3` is now infallible

### Removed
- **Breaking**: Remove the `v3::array::codec::vlen_v2` module and all associated types
- **Breaking**: Remove `Reversible` support from `zfpy` codec metadata

### Fixed
- Deny unknown fields in `PcodecCodecConfigurationV1`

## [0.2.0] - 2024-11-15

### Added
- Add `GroupMetadataV2` constructors
- Add `ArrayMetadataV2` constructors
- Implement `From<{&str,String}>` for `DataTypeMetadataV2`
- Add `v2::array::codec::vlen_{array,bytes,utf8}` modules
- Add support for Zarr V2 string fill values

### Changed
- **Breaking**: Mark `GroupMetadataV3` and `ArrayMetadataV3` as non-exhaustive
- **Breaking**: Bump MSRV to 1.77 (21 March, 2024)
- Refactor `GroupMetadataV3` constructors
  - **Breaking**: `GroupMetadataV3::new()` is now parameter free in favor of `with_` methods
  - Add `GroupMetadataV3::with_{attributes,additional_fields}()`
- Refactor `ArrayMetadataV3` constructors
  - **Breaking**: `ArrayMetadataV3::new()` takes fewer parameters in favor of `with_` methods
  - Add `ArrayMetadataV3::with_{attributes,additional_fields,chunk_key_encoding,dimension_names,storage_transformers}`

## [0.1.0] - 2024-09-02

### Added
- Initial release
- Split from the `metadata` module of `zarrs` 0.17.0-dev

[unreleased]: https://github.com/LDeakin/zarrs/compare/zarrs_metadata-v0.3.6...HEAD
[0.3.6]: https://github.com/LDeakin/zarrs/releases/tag/zarrs_metadata-v0.3.6
[0.3.5]: https://github.com/LDeakin/zarrs/releases/tag/zarrs_metadata-v0.3.5
[0.3.4]: https://github.com/LDeakin/zarrs/releases/tag/zarrs_metadata-v0.3.4
[0.3.3]: https://github.com/LDeakin/zarrs/releases/tag/zarrs_metadata-v0.3.3
[0.3.2]: https://github.com/LDeakin/zarrs/releases/tag/zarrs_metadata-v0.3.2
[0.3.1]: https://github.com/LDeakin/zarrs/releases/tag/zarrs_metadata-v0.3.1
[0.3.0]: https://github.com/LDeakin/zarrs/releases/tag/zarrs_metadata-v0.3.0
[0.2.0]: https://github.com/LDeakin/zarrs/releases/tag/zarrs_metadata-v0.2.0
[0.1.0]: https://github.com/LDeakin/zarrs/releases/tag/zarrs_metadata-v0.1.0

[@zqfang]: https://github.com/zqfang
