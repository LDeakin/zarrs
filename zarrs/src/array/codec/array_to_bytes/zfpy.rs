//! The `zfpy` array to bytes codec.
//!
//! [zfp](https://zfp.io/) is a compressed number format for 1D to 4D arrays of 32/64-bit floating point or integer data.
//! 8/16-bit integer types are supported through promotion to 32-bit in accordance with the [zfp utility functions](https://zfp.readthedocs.io/en/release1.0.1/low-level-api.html#utility-functions).
//!
//! This codec requires the `zfp` feature, which is disabled by default.
//!
//! See [`ZfpyCodecConfigurationNumcodecs`] for example `JSON` metadata.

use std::sync::Arc;

pub use crate::metadata::codec::zfpy::ZfpyCodecConfigurationNumcodecs;
use crate::{
    array::codec::{Codec, CodecPlugin},
    config::global_config,
    metadata::codec::zfpy,
};

use zarrs_plugin::{MetadataV3, PluginCreateError, PluginMetadataInvalidError};
pub use zfpy::IDENTIFIER;

use super::zfp::ZfpCodec;

// Register the codec.
inventory::submit! {
    CodecPlugin::new(IDENTIFIER, is_name_zfpy, create_codec_zfpy)
}

fn is_name_zfpy(name: &str) -> bool {
    global_config()
        .codec_map()
        .get(IDENTIFIER)
        .is_some_and(|map| map.contains(name))
}

pub(crate) fn create_codec_zfpy(metadata: &MetadataV3) -> Result<Codec, PluginCreateError> {
    let configuration: ZfpyCodecConfigurationNumcodecs = metadata
        .to_configuration()
        .map_err(|_| PluginMetadataInvalidError::new(IDENTIFIER, "codec", metadata.clone()))?;
    let codec = Arc::new(ZfpCodec::new_zfpy(&configuration));
    Ok(Codec::ArrayToBytes(codec))
}
