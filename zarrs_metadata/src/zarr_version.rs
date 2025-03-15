use std::fmt::Debug;

/// Marker trait for zarr versions.
pub trait ZarrVersion: Debug + Default {}

/// Zarr Version 3.
#[derive(Debug, Default)]
pub struct ZarrVersion3;

/// Zarr Version 2.
#[derive(Debug, Default)]
pub struct ZarrVersion2;

impl ZarrVersion for ZarrVersion3 {}
impl ZarrVersion for ZarrVersion2 {}
