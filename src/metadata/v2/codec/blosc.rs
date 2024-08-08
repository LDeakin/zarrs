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
#[display("{}", serde_json::to_string(self).unwrap_or_default())]
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
    let (shuffle, typesize) = match (&blosc.shuffle, data_type.fixed_size()) {
        (BloscShuffleModeNumCodecs::NoShuffle, _) => (BloscShuffleMode::NoShuffle, None),
        // Fixed
        (BloscShuffleModeNumCodecs::Shuffle, Some(data_type_size)) => {
            (BloscShuffleMode::Shuffle, Some(data_type_size))
        }
        (BloscShuffleModeNumCodecs::BitShuffle, Some(data_type_size)) => {
            (BloscShuffleMode::BitShuffle, Some(data_type_size))
        }
        (BloscShuffleModeNumCodecs::AutoShuffle, Some(data_type_size)) => {
            if data_type_size == 1 {
                (BloscShuffleMode::BitShuffle, Some(data_type_size))
            } else {
                (BloscShuffleMode::Shuffle, Some(data_type_size))
            }
        }
        // Variable
        (
            BloscShuffleModeNumCodecs::Shuffle
            | BloscShuffleModeNumCodecs::BitShuffle
            | BloscShuffleModeNumCodecs::AutoShuffle,
            None,
        ) => {
            // FIXME: Check blosc auto behaviour with variable sized data type
            //        Currently defaulting to "bitshuffle"
            //        What do other implementations do for a variable sized data type?
            //        May need to make this function fallible
            (BloscShuffleMode::NoShuffle, None)
        }
    };

    BloscCodecConfiguration::V1(BloscCodecConfigurationV1 {
        cname: blosc.cname,
        clevel: blosc.clevel,
        shuffle,
        typesize,
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
