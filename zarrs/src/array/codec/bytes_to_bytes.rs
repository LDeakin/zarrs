//! Bytes to bytes codecs.

#[cfg(feature = "blosc")]
pub mod blosc;
#[cfg(feature = "bz2")]
pub mod bz2;
#[cfg(feature = "crc32c")]
pub mod crc32c;
#[cfg(feature = "fletcher32")]
pub mod fletcher32;
#[cfg(feature = "gdeflate")]
pub mod gdeflate;
#[cfg(feature = "gzip")]
pub mod gzip;
#[cfg(feature = "zstd")]
pub mod zstd;

#[cfg(test)]
pub mod test_unbounded;

#[cfg(any(feature = "crc32c", feature = "fletcher32"))]
mod strip_suffix_partial_decoder;
