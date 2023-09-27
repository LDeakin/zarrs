use derive_more::{Display, From};
use serde::{Deserialize, Serialize};

use super::{BloscCompressionLevel, BloscCompressor, BloscShuffleMode};

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
#[display(fmt = "{}", "serde_json::to_string(self).unwrap_or_default()")]
pub struct BloscCodecConfigurationV1 {
    /// The compressor.
    pub cname: BloscCompressor,
    /// The compression level.
    pub clevel: BloscCompressionLevel,
    /// The shuffle mode.
    pub shuffle: BloscShuffleMode,
    /// The type size in bytes.
    pub typesize: usize,
    /// The compression block size. Automatically determined if [`None`].
    pub blocksize: Option<usize>,
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
