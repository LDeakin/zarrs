//! The `vlen-bytes` array to bytes codec.

mod vlen_bytes_codec;

use std::sync::Arc;

pub use vlen_bytes::IDENTIFIER;

pub use vlen_bytes_codec::VlenBytesCodec;

use crate::{
    array::codec::{Codec, CodecPlugin},
    metadata::v2::array::codec::vlen_bytes,
    metadata::v3::MetadataV3,
    plugin::{PluginCreateError, PluginMetadataInvalidError},
};

// Register the codec.
inventory::submit! {
    CodecPlugin::new(IDENTIFIER, is_name_vlen_bytes, create_codec_vlen_bytes)
}

fn is_name_vlen_bytes(name: &str) -> bool {
    name.eq(IDENTIFIER)
}

pub(crate) fn create_codec_vlen_bytes(metadata: &MetadataV3) -> Result<Codec, PluginCreateError> {
    if metadata.configuration_is_none_or_empty() {
        let codec = Arc::new(VlenBytesCodec::new());
        Ok(Codec::ArrayToBytes(codec))
    } else {
        Err(PluginMetadataInvalidError::new(IDENTIFIER, "codec", metadata.clone()).into())
    }
}
