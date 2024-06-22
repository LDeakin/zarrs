use derive_more::Display;
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};

use crate::{
    array::data_type::DataType,
    metadata::v3::codec::blosc::{
        BloscCodecConfiguration, BloscCodecConfigurationV1, BloscCompressionLevel, BloscCompressor,
        BloscShuffleMode,
    },
};

/// Configuration parameters for the `blosc` codec (numcodecs).
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug, Display)]
#[serde(deny_unknown_fields)]
#[display(fmt = "{}", "serde_json::to_string(self).unwrap_or_default()")]
pub struct BloscCodecConfigurationNumcodecs {
    /// The compressor.
    pub cname: BloscCompressor,
    /// The compression level.
    pub clevel: BloscCompressionLevel,
    /// The shuffle mode.
    pub shuffle: BloscShuffleModeNumCodecs,
    /// The compression block size. Automatically determined if 0.
    #[serde(default)]
    pub blocksize: usize,
}

/// Blosc shuffle modes (numcodecs).
#[derive(Serialize_repr, Deserialize_repr, Clone, Eq, PartialEq, Debug, Display)]
#[repr(i8)]
pub enum BloscShuffleModeNumCodecs {
    /// No shuffling.
    NoShuffle = 0,
    /// Byte-wise shuffling.
    Shuffle = 1,
    /// Bit-wise shuffling.
    BitShuffle = 2,
    /// Bit-wise shuffling will be used for buffers with itemsize 1, and byte-wise shuffling will be used otherwise.
    AutoShuffle = -1,
}

/// Convert [`BloscCodecConfigurationNumcodecs`] to [`BloscCodecConfiguration`].
#[must_use]
pub fn codec_blosc_v2_numcodecs_to_v3(
    blosc: &BloscCodecConfigurationNumcodecs,
    data_type: &DataType,
) -> BloscCodecConfiguration {
    BloscCodecConfiguration::V1(BloscCodecConfigurationV1 {
        cname: blosc.cname,
        clevel: blosc.clevel,
        shuffle: match blosc.shuffle {
            BloscShuffleModeNumCodecs::NoShuffle => BloscShuffleMode::NoShuffle,
            BloscShuffleModeNumCodecs::Shuffle => BloscShuffleMode::Shuffle,
            BloscShuffleModeNumCodecs::BitShuffle => BloscShuffleMode::BitShuffle,
            BloscShuffleModeNumCodecs::AutoShuffle => {
                if data_type.size() == 1 {
                    BloscShuffleMode::BitShuffle
                } else {
                    BloscShuffleMode::Shuffle
                }
            }
        },
        typesize: Some(data_type.size()),
        blocksize: blosc.blocksize,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codec_blosc_v2_numcodecs() {
        serde_json::from_str::<BloscCodecConfigurationNumcodecs>(
            r#"
        {
            "cname": "lz4",
            "clevel": 5,
            "shuffle": 2,
            "blocksize": 0
        }"#,
        )
        .unwrap();
    }
}
