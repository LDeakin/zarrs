use zarrs_metadata::codec::VLEN_BYTES;

use crate::array::codec::array_to_bytes::vlen_v2::vlen_v2_macros;

vlen_v2_macros::vlen_v2_codec!(VlenBytesCodec, VLEN_BYTES);
