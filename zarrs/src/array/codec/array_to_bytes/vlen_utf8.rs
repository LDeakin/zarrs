//! The `vlen-utf8` array to bytes codec.

use crate::array::codec::array_to_bytes::vlen_v2::vlen_v2_macros;

vlen_v2_macros::vlen_v2_module!(vlen_utf8, vlen_utf8_codec, VlenUtf8Codec);
