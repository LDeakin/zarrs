//! The `vlen-array` array to bytes codec.

use crate::array::codec::array_to_bytes::vlen_v2::vlen_v2_macros;

vlen_v2_macros::vlen_v2_module!(vlen_array, vlen_array_codec, VlenArrayCodec);
