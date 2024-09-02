//! Storage adapters.
//!
//! Storage adapters can be layered on stores.

#[cfg(feature = "zip")]
pub mod zip;

#[cfg(feature = "async")]
pub mod async_to_sync;
