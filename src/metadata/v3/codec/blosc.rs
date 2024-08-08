use derive_more::{Display, From};
use serde::{Deserialize, Serialize};

/// The identifier for the `blosc` codec.
pub const IDENTIFIER: &str = "blosc";

/// An integer from 0 to 9 controlling the compression level
///
/// A level of 1 is the fastest compression method and produces the least compressions, while 9 is slowest and produces the most compression.
/// Compression is turned off when the compression level is 0.
#[derive(Serialize, Copy, Clone, Debug, Eq, PartialEq)]
pub struct BloscCompressionLevel(u8);

impl From<BloscCompressionLevel> for u8 {
    fn from(val: BloscCompressionLevel) -> Self {
        val.0
    }
}

impl TryFrom<u8> for BloscCompressionLevel {
    type Error = u8;
    fn try_from(level: u8) -> Result<Self, Self::Error> {
        if level <= 9 {
            Ok(Self(level))
        } else {
            Err(level)
        }
    }
}

impl<'de> serde::Deserialize<'de> for BloscCompressionLevel {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let level = u8::deserialize(d)?;
        if level <= 9 {
            Ok(Self(level))
        } else {
            Err(serde::de::Error::custom("clevel must be between 0 and 9"))
        }
    }
}

/// The `blosc` shuffle mode.
#[derive(Serialize, Deserialize, Copy, Clone, Debug, Eq, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
#[repr(u32)]
pub enum BloscShuffleMode {
    /// No shuffling.
    #[default]
    NoShuffle = 0, // blosc_sys::BLOSC_NOSHUFFLE,
    /// Byte-wise shuffling.
    Shuffle = 1, // blosc_sys::BLOSC_SHUFFLE,
    /// Bit-wise shuffling.
    BitShuffle = 2, // blosc_sys::BLOSC_BITSHUFFLE,
}

/// The `blosc` compressor.
///
/// See <https://www.blosc.org/pages/>.
#[derive(Serialize, Deserialize, Copy, Clone, Debug, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum BloscCompressor {
    /// [BloscLZ](https://github.com/Blosc/c-blosc/blob/master/blosc/blosclz.h): blosc default compressor, heavily based on [FastLZ](http://fastlz.org/).
    BloscLZ,
    /// [LZ4](http://fastcompression.blogspot.com/p/lz4.html): a compact, very popular and fast compressor.
    LZ4,
    /// [LZ4HC](http://fastcompression.blogspot.com/p/lz4.html): a tweaked version of LZ4, produces better compression ratios at the expense of speed.
    LZ4HC,
    /// [Snappy](https://code.google.com/p/snappy): a popular compressor used in many places.
    Snappy,
    /// [Zlib](http://www.zlib.net/): a classic; somewhat slower than the previous ones, but achieving better compression ratios.
    Zlib,
    /// [Zstd](http://www.zstd.net/): an extremely well balanced codec; it provides the best compression ratios among the others above, and at reasonably fast speed.
    Zstd,
}

#[cfg(feature = "blosc")]
impl BloscCompressor {
    pub(crate) const fn as_cstr(&self) -> *const u8 {
        match self {
            Self::BloscLZ => blosc_sys::BLOSC_BLOSCLZ_COMPNAME.as_ptr(),
            Self::LZ4 => blosc_sys::BLOSC_LZ4_COMPNAME.as_ptr(),
            Self::LZ4HC => blosc_sys::BLOSC_LZ4HC_COMPNAME.as_ptr(),
            Self::Snappy => blosc_sys::BLOSC_SNAPPY_COMPNAME.as_ptr(),
            Self::Zlib => blosc_sys::BLOSC_ZLIB_COMPNAME.as_ptr(),
            Self::Zstd => blosc_sys::BLOSC_ZSTD_COMPNAME.as_ptr(),
        }
    }
}

/// A wrapper to handle various versions of `blosc` codec configuration parameters.
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug, Display, From)]
#[serde(untagged)]
pub enum BloscCodecConfiguration {
    /// Version 1.0.
    V1(BloscCodecConfigurationV1),
}

/// Configuration parameters for the `blosc` codec (version 1.0).
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug, Display)]
#[serde(deny_unknown_fields)]
#[display("{}", serde_json::to_string(self).unwrap_or_default())]
pub struct BloscCodecConfigurationV1 {
    /// The compressor.
    pub cname: BloscCompressor,
    /// The compression level.
    pub clevel: BloscCompressionLevel,
    /// The shuffle mode.
    ///
    /// Defaults to noshuffle if unspecified.
    #[serde(default)]
    pub shuffle: BloscShuffleMode,
    /// The type size in bytes.
    ///
    /// Required unless shuffle is "noshuffle", in which case the value is ignored.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub typesize: Option<usize>,
    /// The compression block size. Automatically determined if 0.
    pub blocksize: usize,
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
    fn codec_blosc_invalid_no_typesize() {
        serde_json::from_str::<BloscCodecConfiguration>(
            r#"
        {
            "cname": "lz4",
            "clevel": 4,
            "shuffle": "bitshuffle",
            "blocksize": 0
        }"#,
        )
        .unwrap();
    }

    #[test]
    fn codec_blosc_valid_no_shuffle() {
        serde_json::from_str::<BloscCodecConfiguration>(
            r#"
        {
            "cname": "lz4",
            "clevel": 4,
            "blocksize": 0
        }"#,
        )
        .unwrap();
    }

    #[test]
    fn codec_blosc_valid_no_typesize() {
        serde_json::from_str::<BloscCodecConfiguration>(
            r#"
        {
            "cname": "lz4",
            "clevel": 4,
            "shuffle": "shuffle",
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
        assert!(codec_configuration.is_err());
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
        assert!(codec_configuration.is_err());
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
        assert!(codec_configuration.is_err());
    }
}
