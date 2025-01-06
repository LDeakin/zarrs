use derive_more::derive::{Display, From};
use serde::{Deserialize, Serialize};

pub use crate::v3::array::codec::zstd::ZstdCodecConfigurationV1;

use crate::v3::array::codec::zstd::{ZstdCodecConfiguration, ZstdCompressionLevel};

type ZstdCodecConfigurationNumCodecs0_13 = ZstdCodecConfigurationV1;

/// A wrapper to handle various versions of `zstd` codec configuration parameters.
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug, Display, From)]
#[serde(untagged)]
pub enum ZstdCodecConfigurationNumCodecs {
    /// `numcodecs` version 0.13.
    V0_13(ZstdCodecConfigurationNumCodecs0_13),
    /// `numcodecs` version 0.1.
    V0_1(ZstdCodecConfigurationNumCodecs0_1),
}

/// Configuration parameters for the `zstd` codec (`numcodecs` version 0.1).
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug, Display)]
#[serde(deny_unknown_fields)]
#[display("{}", serde_json::to_string(self).unwrap_or_default())]
pub struct ZstdCodecConfigurationNumCodecs0_1 {
    /// The compression level.
    pub level: ZstdCompressionLevel,
}

/// Convert [`ZstdCodecConfigurationNumCodecs`] to [`ZstdCodecConfiguration`].
#[must_use]
pub fn codec_zstd_v2_numcodecs_to_v3(
    zstd: &ZstdCodecConfigurationNumCodecs,
) -> ZstdCodecConfiguration {
    match zstd {
        ZstdCodecConfigurationNumCodecs::V0_13(zstd) => ZstdCodecConfiguration::V1(zstd.clone()),
        ZstdCodecConfigurationNumCodecs::V0_1(zstd) => {
            ZstdCodecConfiguration::V1(ZstdCodecConfigurationV1::new(zstd.level.clone(), false))
        }
    }
}
