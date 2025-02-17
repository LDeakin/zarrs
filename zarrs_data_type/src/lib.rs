//! [Zarr](https://zarr-specs.readthedocs.io/) data types for the [`zarrs`](https://docs.rs/zarrs/latest/zarrs/index.html) crate.

mod data_type;
mod fill_value;

pub use data_type::{
    DataType, IncompatibleFillValueError, IncompatibleFillValueMetadataError,
    UnsupportedDataTypeError,
};
pub use fill_value::FillValue;
