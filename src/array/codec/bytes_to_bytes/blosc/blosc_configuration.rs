use derive_more::{Display, From};
use serde::{Deserialize, Serialize};

/// A wrapper to handle various versions of `blosc` codec configuration parameters.
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug, Display, From)]
#[serde(untagged)]
pub enum BloscCodecConfiguration {
    /// Version 1.0.
    V1(BloscCodecConfigurationV1),
}

/// Configuration parameters for the `blosc` codec (version 1.0).
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug, Display)]
#[serde(
    deny_unknown_fields,
    try_from = "BloscCodecConfigurationV1Intermediate",
    into = "BloscCodecConfigurationV1Intermediate"
)]
#[display(fmt = "{}", "serde_json::to_string(self).unwrap_or_default()")]
pub struct BloscCodecConfigurationV1 {
    /// The compressor.
    pub compressor: blosc::Compressor,
    /// The compression level.
    pub clevel: blosc::Clevel,
    /// The shuffle mode.
    pub shuffle: blosc::ShuffleMode,
    /// The type size in bytes.
    pub typesize: usize,
    /// The compression block size. Automatically determined if [`None`].
    pub blocksize: Option<usize>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
struct BloscCodecConfigurationV1Intermediate {
    #[serde(alias = "cname")]
    compressor: BloscCompressorIntermediate,
    clevel: u32,
    shuffle: ShuffleModeIntermediate,
    typesize: usize,
    blocksize: usize,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "lowercase")]
enum BloscCompressorIntermediate {
    BloscLZ,
    LZ4,
    LZ4HC,
    Snappy,
    Zlib,
    Zstd,
}

/// The type of shuffling to perform, if any, prior to compression.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "lowercase")]
enum ShuffleModeIntermediate {
    NoShuffle,
    Shuffle,
    BitShuffle,
}

impl From<BloscCodecConfigurationV1> for BloscCodecConfigurationV1Intermediate {
    fn from(value: BloscCodecConfigurationV1) -> Self {
        let compressor: BloscCompressorIntermediate = match value.compressor {
            blosc::Compressor::BloscLZ => BloscCompressorIntermediate::BloscLZ,
            blosc::Compressor::LZ4 => BloscCompressorIntermediate::LZ4,
            blosc::Compressor::LZ4HC => BloscCompressorIntermediate::LZ4HC,
            blosc::Compressor::Snappy => BloscCompressorIntermediate::Snappy,
            blosc::Compressor::Zlib => BloscCompressorIntermediate::Zlib,
            blosc::Compressor::Zstd => BloscCompressorIntermediate::Zstd,
            blosc::Compressor::Invalid => unreachable!(),
        };

        let clevel = match value.clevel {
            blosc::Clevel::None => 0,
            blosc::Clevel::L1 => 1,
            blosc::Clevel::L2 => 2,
            blosc::Clevel::L3 => 3,
            blosc::Clevel::L4 => 4,
            blosc::Clevel::L5 => 5,
            blosc::Clevel::L6 => 6,
            blosc::Clevel::L7 => 7,
            blosc::Clevel::L8 => 8,
            blosc::Clevel::L9 => 9,
        };

        let shuffle = match value.shuffle {
            blosc::ShuffleMode::None => ShuffleModeIntermediate::NoShuffle,
            blosc::ShuffleMode::Bit => ShuffleModeIntermediate::BitShuffle,
            blosc::ShuffleMode::Byte => ShuffleModeIntermediate::Shuffle,
        };

        BloscCodecConfigurationV1Intermediate {
            compressor,
            clevel,
            shuffle,
            typesize: value.typesize,
            blocksize: value.blocksize.unwrap_or_default(),
        }
    }
}

impl TryFrom<BloscCodecConfigurationV1Intermediate> for BloscCodecConfigurationV1 {
    type Error = std::io::Error;

    fn try_from(value: BloscCodecConfigurationV1Intermediate) -> Result<Self, Self::Error> {
        let compressor: blosc::Compressor = match value.compressor {
            BloscCompressorIntermediate::BloscLZ => blosc::Compressor::BloscLZ,
            BloscCompressorIntermediate::LZ4 => blosc::Compressor::LZ4,
            BloscCompressorIntermediate::LZ4HC => blosc::Compressor::LZ4HC,
            BloscCompressorIntermediate::Snappy => blosc::Compressor::Snappy,
            BloscCompressorIntermediate::Zlib => blosc::Compressor::Zlib,
            BloscCompressorIntermediate::Zstd => blosc::Compressor::Zstd,
        };

        let clevel = match value.clevel {
            0 => blosc::Clevel::None,
            1 => blosc::Clevel::L1,
            2 => blosc::Clevel::L2,
            3 => blosc::Clevel::L3,
            4 => blosc::Clevel::L4,
            5 => blosc::Clevel::L5,
            6 => blosc::Clevel::L6,
            7 => blosc::Clevel::L7,
            8 => blosc::Clevel::L8,
            9 => blosc::Clevel::L9,
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "invalid blosc clevel",
                ))
            }
        };

        let shuffle = match value.shuffle {
            ShuffleModeIntermediate::NoShuffle => blosc::ShuffleMode::None,
            ShuffleModeIntermediate::BitShuffle => blosc::ShuffleMode::Bit,
            ShuffleModeIntermediate::Shuffle => blosc::ShuffleMode::Byte,
        };

        let blocksize = if value.blocksize == 0 {
            None
        } else {
            Some(value.blocksize)
        };

        Ok(BloscCodecConfigurationV1 {
            compressor,
            clevel,
            shuffle,
            typesize: value.typesize,
            blocksize,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codec_blosc_valid1() {
        serde_json::from_str::<BloscCodecConfiguration>(
            r#"
        {
            "cname": "lz4",
            "clevel": 5,
            "shuffle": "shuffle",
            "typesize": 4,
            "blocksize": 0
        }"#,
        )
        .unwrap();
    }

    #[test]
    fn codec_blosc_valid2() {
        serde_json::from_str::<BloscCodecConfiguration>(
            r#"
        {
            "cname": "lz4",
            "clevel": 4,
            "shuffle": "bitshuffle",
            "typesize": 4,
            "blocksize": 0
        }"#,
        )
        .unwrap();
    }

    #[test]
    fn codec_blosc_invalid_clevel() {
        let json = r#"
    {
        "cname": "lz4",
        "clevel": 10,
        "shuffle": "shuffle",
        "typesize": 4,
        "blocksize": 0
    }"#;
        let codec_configuration = serde_json::from_str::<BloscCodecConfiguration>(json);
        assert!(codec_configuration.is_err())
    }

    #[test]
    fn codec_blosc_invalid_cname() {
        let json = r#"
    {
        "cname": "",
        "clevel": 1,
        "shuffle": "shuffle",
        "typesize": 4,
        "blocksize": 0
    }"#;
        let codec_configuration = serde_json::from_str::<BloscCodecConfiguration>(json);
        assert!(codec_configuration.is_err())
    }

    #[test]
    fn codec_blosc_invalid_shuffle() {
        let json = r#"
    {
        "cname": "lz4",
        "clevel": 1,
        "shuffle": "",
        "typesize": 4,
        "blocksize": 0
    }"#;
        let codec_configuration = serde_json::from_str::<BloscCodecConfiguration>(json);
        assert!(codec_configuration.is_err())
    }
}
