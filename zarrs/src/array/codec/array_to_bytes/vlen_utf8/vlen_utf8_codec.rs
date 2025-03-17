use zarrs_metadata::codec::VLEN_UTF8;

use crate::array::codec::array_to_bytes::vlen_v2::vlen_v2_macros;

vlen_v2_macros::vlen_v2_codec!(VlenUtf8Codec, VLEN_UTF8);
