//! Array to bytes codecs.

pub mod bytes;
pub mod codec_chain;

#[cfg(feature = "sharding")]
pub mod sharding;

#[cfg(feature = "zfp")]
pub mod zfp;
