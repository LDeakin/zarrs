//! The `zfpy` array to bytes codec.
//!
//! [zfp](https://zfp.io/) is a compressed number format for 1D to 4D arrays of 32/64-bit floating point or integer data.
//! 8/16-bit integer types are supported through promotion to 32-bit in accordance with the [zfp utility functions](https://zfp.readthedocs.io/en/release1.0.1/low-level-api.html#utility-functions).
//!
//! This codec requires the `zfp` feature, which is disabled by default.
//!
//! ### Compatible Implementations
//! This codec is fully compatible with the `numcodecs.zfpy` codec in `zarr-python`.
//!
//! ### Specification
//! - <https://github.com/zarr-developers/zarr-extensions/tree/numcodecs/codecs/numcodecs.zfpy>
//! - <https://codec.zarrs.dev/array_to_bytes/zfpy>
//!
//! ### Codec `name` Aliases (Zarr V3)
//! - `numcodecs.zfpy`
//! - `https://codec.zarrs.dev/array_to_bytes/zfpy`
//!
//! ### Codec `id` Aliases (Zarr V2)
//! - `zfp`
//!
//! ### Codec `configuration` Example - [`ZfpyCodecConfiguration`]:
//! #### Encode in fixed rate mode with 10.5 compressed bits per value
//! ```rust
//! # let JSON = r#"
//! {
//!     "mode": 2,
//!     "rate": 10.5
//! }
//! # "#;
//! # use zarrs_metadata::codec::zfpy::ZfpyCodecConfiguration;
//! # let configuration: ZfpyCodecConfiguration = serde_json::from_str(JSON).unwrap();
//! ```
//!
//! #### Encode in fixed precision mode with 19 uncompressed bits per value
//! ```rust
//! # let JSON = r#"
//! {
//!     "mode": 3,
//!     "precision": 19
//! }
//! # "#;
//! # use zarrs_metadata::codec::zfpy::ZfpyCodecConfiguration;
//! # let configuration: ZfpyCodecConfiguration = serde_json::from_str(JSON).unwrap();
//! ```
//!
//! #### Encode in fixed accuracy mode with a tolerance of 0.05
//! ```rust
//! # let JSON = r#"
//! {
//!     "mode": 4,
//!     "tolerance": 0.05
//! }
//! # "#;
//! # use zarrs_metadata::codec::zfpy::ZfpyCodecConfiguration;
//! # let configuration: ZfpyCodecConfiguration = serde_json::from_str(JSON).unwrap();
//! ```

use std::sync::Arc;

pub use crate::metadata::codec::zfpy::{ZfpyCodecConfiguration, ZfpyCodecConfigurationNumcodecs};
use crate::{
    array::codec::{Codec, CodecPlugin},
    metadata::codec::ZFPY,
};

use zarrs_plugin::{MetadataV3, PluginCreateError, PluginMetadataInvalidError};

use super::zfp::ZfpCodec;

// Register the codec.
inventory::submit! {
    CodecPlugin::new(ZFPY, is_identifier_zfpy, create_codec_zfpy)
}

fn is_identifier_zfpy(identifier: &str) -> bool {
    identifier == ZFPY
}

pub(crate) fn create_codec_zfpy(metadata: &MetadataV3) -> Result<Codec, PluginCreateError> {
    let configuration: ZfpyCodecConfiguration = metadata
        .to_configuration()
        .map_err(|_| PluginMetadataInvalidError::new(ZFPY, "codec", metadata.clone()))?;
    let codec = Arc::new(ZfpCodec::new_with_configuration_zfpy(&configuration)?);
    Ok(Codec::ArrayToBytes(codec))
}
