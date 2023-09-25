//! Bytes to bytes codecs.

#[cfg(feature = "blosc")]
pub mod blosc;
#[cfg(feature = "crc32c")]
pub mod crc32c;
#[cfg(feature = "gzip")]
pub mod gzip;
#[cfg(feature = "zstd")]
pub mod zstd;
