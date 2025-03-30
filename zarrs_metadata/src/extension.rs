//! Zarr extension point support.
//!
//! This module currently provides functionality to manage extension point aliases.
//!
//! Zarr V3 extension points include data types, codecs, chunk grids, chunk key encodings, and storage transformers.
//! Additionally, [`ZEP0009`] introduces a new `extension` metadata field for arbitrary array and group extensions.
//!
//! Zarr V2 extension points are limited to data types and codecs.
//!
//! ### Extension Aliasing
//!
//! An extension point `name` (or `id` in Zarr V2) may change over time as the Zarr specification and extensions evolve.
//! For example, the `bytes` codec was originally called `endian` in the provisionally accepted Zarr V3 specification.
//! Aliasing of `name`s permits for seamless backward compatibility and interoperability between different Zarr versions, Zarr implementations, and extension versions (where compatible).
//!
//! [`zarrs_metadata`](`crate`) defines a *unique extension identifier* for each known extension in each extension point.
//! Known aliases for an extension can be mapped to their unique extension identifier, and this can subsequently be mapped to a default `name` for metadata serialisation.
//! Extension alias maps can be mutated to support custom extensions or to override the defaults.
//!
//! *Unique extension identifiers* are an implementation detail and may not match extension `name`s.
//! However, they are publicly exposed to permit manipulation of aliases and default names.
//!
//! ### Extension Name Conventions
//! Prior to [`ZEP0009`], a Zarr V3 extension point `name` was encouraged to be a unique URI pointing to a specification of the extension.
//! [`ZEP0009`] revises conventions for extension point naming:
//! - private or experimental extensions must use a *namespaced name* (e.g. `numcodecs.bitround`, `zarrs.vlen`, etc.), and
//! - extensions registered  at [`zarr-extensions`] can use a *raw name*, such as `bfloat16`.
//!
//! ### Alias Maps
//! This crate provides [`Default`] alias maps for Zarr V2 and V3 extension points.
//! The alias maps define the list of known compatible extension point aliases and a *default* `name` for each extension point.
//! The default `name` is what will be serialised when creating a new array or group, unless overriden.
//! Alias maps are currently limited to codecs and data types, but they may be extended to other extension types in the future.
//!
//! In the [`zarrs`] crate, the extension point aliases can be configured to support custom extensions or to override the defaults, such as if [`zarrs_metadata`](`crate`) is lagging [`zarr-extensions`].
//! This crate will be continually updated to include new compatible extension point aliases as they are developed in other implementations and registered at [`zarr-extensions`].
//!
//! [`ZEP0009`]: https://zarr.dev/zeps/draft/ZEP0009.html
//! [`zarrs`]: https://docs.rs/zarrs/latest/zarrs/index.html
//! [`zarr-extensions`]: https://github.com/zarr-developers/zarr-extensions

mod extension_type;
pub use extension_type::{
    ExtensionType, ExtensionTypeChunkGrid, ExtensionTypeChunkKeyEncoding, ExtensionTypeCodec,
    ExtensionTypeDataType, ExtensionTypeStorageTransformer,
};

mod extension_aliases;
pub use extension_aliases::{
    ExtensionAliasMapRegex, ExtensionAliasMapString, ExtensionAliases, ExtensionNameMap,
};

mod extension_aliases_codec;
pub use extension_aliases_codec::{ExtensionAliasesCodecV2, ExtensionAliasesCodecV3};

mod extension_aliases_data_type;
pub use extension_aliases_data_type::{ExtensionAliasesDataTypeV2, ExtensionAliasesDataTypeV3};
