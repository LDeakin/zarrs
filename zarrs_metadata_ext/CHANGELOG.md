# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- `data_type::Numpy{DateTime64,TimeDelta64}DataTypeConfiguration`

## [0.1.0] - 2025-05-16

### Added
- Split from the `zarrs_metadata` module of `zarrs_metadata` 0.4.0

### Changed
- rename `ArrayMetadataV2ToV3ConversionError` to `ArrayMetadataV2ToV3Error`
- rename `InvalidPermutationError` to `TransposeOrderError`
- change the suffix of experimental codec configurations from V1 to V0 (`gdeflate`, `squeeze`, `vlen`, `vlen_v2`)

[unreleased]: https://github.com/zarrs/zarrs/compare/zarrs_metadata_ext-v0.1.0...HEAD
[0.1.0]: https://github.com/LDeakin/zarrs/releases/tag/zarrs_metadata_ext-v0.1.0
