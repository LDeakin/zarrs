//! The `vlen-utf8` array to bytes codec (Experimental).
//!
//! <div class="warning">
//! This codec is experimental and may be incompatible with other Zarr V3 implementations.
//! </div>
//!
//! ### Compatible Implementations
//! This codec is fully compatible with the `vlen-utf8` codec in `zarr-python`.
//!
//! ### Specification
//! - <https://github.com/zarr-developers/zarr-extensions/tree/zarr-python-exts/codecs/vlen-utf8>
//!
//! ### Codec `name` Aliases (Zarr V3)
//! - `vlen-utf8`
//! - `https://codec.zarrs.dev/array_to_bytes/vlen_utf8`
//!
//! ### Codec `id` Aliases (Zarr V2)
//! - `vlen-utf8`
//!
//! ### Codec `configuration` Example - [`VlenUtf8CodecConfiguration`]:
//! ```json
//! {}
//! ```

use crate::array::codec::array_to_bytes::vlen_v2::vlen_v2_macros;

pub use crate::metadata::codec::vlen_utf8::{
    VlenUtf8CodecConfiguration, VlenUtf8CodecConfigurationV1,
};

vlen_v2_macros::vlen_v2_module!(vlen_utf8, vlen_utf8_codec, VlenUtf8Codec);
