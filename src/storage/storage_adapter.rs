//! Storage adapters. Includes a [zip](ZipStorageAdapter) implementation.
//!
//! An adapter is a nested resource using a specified protocol they can be chained with a an absolute resource location (e.g. a filesystem store).

#[cfg(feature = "zip")]
mod zip;
#[cfg(feature = "zip")]
pub use self::zip::{ZipStorageAdapter, ZipStorageAdapterCreateError};
