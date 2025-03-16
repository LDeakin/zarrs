//! Array to array codecs.

#[cfg(feature = "bitround")]
pub mod bitround;
pub mod fixedscaleoffset;
#[cfg(feature = "transpose")]
pub mod transpose;
