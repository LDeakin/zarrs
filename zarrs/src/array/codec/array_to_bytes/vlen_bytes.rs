//! The `vlen-bytes` array to bytes codec.

use crate::array::codec::array_to_bytes::vlen_v2::vlen_v2_macros;

vlen_v2_macros::vlen_v2_module!(vlen_bytes, vlen_bytes_codec, VlenBytesCodec);
