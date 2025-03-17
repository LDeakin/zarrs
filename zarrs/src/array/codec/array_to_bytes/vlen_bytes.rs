//! The `vlen-bytes` array to bytes codec (Experimental).
//!
//! <div class="warning">
//! This codec is experimental and may be incompatible with other Zarr V3 implementations.
//! </div>
//!
//! ### Compatible Implementations
//! This codec is fully compatible with the `vlen-bytes` codec in `zarr-python`.
//!
//! ### Specification:
//! - <https://github.com/zarr-developers/zarr-extensions/tree/zarr-python-exts/codecs/vlen-bytes>
//!
//! ### Codec `name` Aliases (Zarr V3)
//! - `vlen-bytes`
//! - `https://codec.zarrs.dev/array_to_bytes/vlen_bytes`
//!
//! ### Codec `id` Aliases (Zarr V2)
//! - `vlen-bytes`
//!
//! ### Codec `configuration` Example - [`VlenBytesCodecConfiguration`]:
//! ```json
//! {}
//! ```

use crate::array::codec::array_to_bytes::vlen_v2::vlen_v2_macros;

pub use crate::metadata::codec::vlen_bytes::{
    VlenBytesCodecConfiguration, VlenBytesCodecConfigurationV1,
};

vlen_v2_macros::vlen_v2_module!(vlen_bytes, vlen_bytes_codec, VlenBytesCodec, VLEN_BYTES);
