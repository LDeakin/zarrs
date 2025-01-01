//! The `vlen-array` array to bytes codec.

mod vlen_array_codec;

use std::sync::Arc;

pub use vlen_array::IDENTIFIER;

pub use vlen_array_codec::VlenArrayCodec;

use crate::{
    array::codec::{Codec, CodecPlugin},
    metadata::v2::array::codec::vlen_array,
    metadata::v3::MetadataV3,
    plugin::{PluginCreateError, PluginMetadataInvalidError},
};

// Register the codec.
inventory::submit! {
    CodecPlugin::new(IDENTIFIER, is_name_vlen_array, create_codec_vlen_array)
}

fn is_name_vlen_array(name: &str) -> bool {
    name.eq(IDENTIFIER)
}

pub(crate) fn create_codec_vlen_array(metadata: &MetadataV3) -> Result<Codec, PluginCreateError> {
    if metadata.configuration_is_none_or_empty() {
        let codec = Arc::new(VlenArrayCodec::new());
        Ok(Codec::ArrayToBytes(codec))
    } else {
        Err(PluginMetadataInvalidError::new(IDENTIFIER, "codec", metadata.clone()).into())
    }
}
