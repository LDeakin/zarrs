//! [Zarr](https://zarr-specs.readthedocs.io/) data types for the [`zarrs`](https://docs.rs/zarrs/latest/zarrs/index.html) crate.

mod data_type_extension;
mod data_type_extension_bytes_codec;
mod data_type_extension_packbits_codec;
mod data_type_plugin;
mod fill_value;

pub use data_type_extension::{DataTypeExtension, DataTypeExtensionError};
pub use data_type_extension_bytes_codec::{
    DataTypeExtensionBytesCodec, DataTypeExtensionBytesCodecError,
};
pub use data_type_extension_packbits_codec::DataTypeExtensionPackBitsCodec;
pub use data_type_plugin::DataTypePlugin;
pub use fill_value::{FillValue, IncompatibleFillValueError, IncompatibleFillValueMetadataError};
