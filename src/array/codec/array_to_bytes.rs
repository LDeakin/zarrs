//! Array to bytes codecs.

pub mod bytes;
pub mod codec_chain;
pub mod vlen;
pub mod vlen_interleaved;

#[cfg(feature = "pcodec")]
pub mod pcodec;
#[cfg(feature = "sharding")]
pub mod sharding;
#[cfg(feature = "zfp")]
pub mod zfp;
