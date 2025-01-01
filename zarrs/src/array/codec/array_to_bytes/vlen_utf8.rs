//! The `vlen-utf8` array to bytes codec.

mod vlen_utf8_codec;

use std::sync::Arc;

pub use vlen_utf8::IDENTIFIER;

pub use vlen_utf8_codec::VlenUtf8Codec;

use crate::{
    array::codec::{Codec, CodecPlugin},
    metadata::v2::array::codec::vlen_utf8,
    metadata::v3::MetadataV3,
    plugin::{PluginCreateError, PluginMetadataInvalidError},
};

// Register the codec.
inventory::submit! {
    CodecPlugin::new(IDENTIFIER, is_name_vlen_utf8, create_codec_vlen_utf8)
}

fn is_name_vlen_utf8(name: &str) -> bool {
    name.eq(IDENTIFIER)
}

pub(crate) fn create_codec_vlen_utf8(metadata: &MetadataV3) -> Result<Codec, PluginCreateError> {
    if metadata.configuration_is_none_or_empty() {
        let codec = Arc::new(VlenUtf8Codec::new());
        Ok(Codec::ArrayToBytes(codec))
    } else {
        Err(PluginMetadataInvalidError::new(IDENTIFIER, "codec", metadata.clone()).into())
    }
}
