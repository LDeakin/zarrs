use std::ffi::c_char;

use blosc_sys::blosc_get_complib_info;

use crate::{
    array::{
        codec::{
            BytesPartialDecoderTraits, BytesToBytesCodecTraits, Codec, CodecError, CodecPlugin,
            CodecTraits,
        },
        BytesRepresentation,
    },
    metadata::Metadata,
    plugin::PluginCreateError,
};

use super::{
    blosc_partial_decoder, compress_bytes, decompress_bytes, BloscCodecConfiguration,
    BloscCodecConfigurationV1, BloscCompressionLevel, BloscCompressor, BloscError,
    BloscShuffleMode,
};

const IDENTIFIER: &str = "blosc";

// Register the codec.
inventory::submit! {
    CodecPlugin::new(IDENTIFIER, is_name_blosc, create_codec_blosc)
}

fn is_name_blosc(name: &str) -> bool {
    name.eq(IDENTIFIER)
}

fn create_codec_blosc(metadata: &Metadata) -> Result<Codec, PluginCreateError> {
    let configuration: BloscCodecConfiguration = metadata.to_configuration()?;
    let codec = Box::new(BloscCodec::new_with_configuration(&configuration)?);
    Ok(Codec::BytesToBytes(codec))
}

/// A `blosc` codec implementation.
#[derive(Clone, Debug)]
pub struct BloscCodec {
    configuration: BloscCodecConfigurationV1,
}

impl BloscCodec {
    /// Create a new `blosc` codec.
    ///
    /// The block size is chosen automatically if `blocksize` is none.
    /// `typesize` must be a positive integer if shuffling is enabled.
    ///
    /// # Errors
    ///
    /// Returns [`PluginCreateError`] if
    ///  - the compressor is not supported, or
    ///  - the typesize has not been specified and shuffling is enabled.
    pub fn new(
        cname: BloscCompressor,
        clevel: BloscCompressionLevel,
        blocksize: Option<usize>,
        shuffle_mode: BloscShuffleMode,
        typesize: Option<usize>,
    ) -> Result<Self, PluginCreateError> {
        if shuffle_mode != BloscShuffleMode::NoShuffle && typesize.is_none() {
            return Err(PluginCreateError::Other {
                error_str: "typesize is a positive integer required if shuffle mode is not none."
                    .into(),
            });
        }

        // Check that the compressor is available
        let support = unsafe {
            blosc_get_complib_info(
                cname.as_cstr().cast::<c_char>(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            )
        };
        if support < 0 {
            return Err(PluginCreateError::Other {
                error_str: format!("compressor {cname:?} is not supported."),
            });
        }

        let configuration = BloscCodecConfigurationV1 {
            cname,
            clevel,
            blocksize,
            shuffle: shuffle_mode,
            typesize: typesize.unwrap_or_default(),
        };

        Ok(BloscCodec { configuration })
    }

    /// Create a new `blosc` codec from configuration.
    ///
    /// # Errors
    ///
    /// Returns [`PluginCreateError`] if the configuration is not supported.
    pub fn new_with_configuration(
        configuration: &BloscCodecConfiguration,
    ) -> Result<Self, PluginCreateError> {
        let BloscCodecConfiguration::V1(configuration) = configuration;
        Self::new(
            configuration.cname,
            configuration.clevel,
            configuration.blocksize,
            configuration.shuffle,
            Some(configuration.typesize),
        )
    }
}

impl CodecTraits for BloscCodec {
    fn create_metadata(&self) -> Option<Metadata> {
        Some(
            Metadata::new_with_serializable_configuration(IDENTIFIER, &self.configuration).unwrap(),
        )
    }

    fn partial_decoder_should_cache_input(&self) -> bool {
        false
    }

    fn partial_decoder_decodes_all(&self) -> bool {
        true
    }
}

impl BytesToBytesCodecTraits for BloscCodec {
    fn encode(&self, decoded_value: Vec<u8>) -> Result<Vec<u8>, CodecError> {
        compress_bytes(
            &decoded_value,
            self.configuration.clevel,
            self.configuration.shuffle,
            self.configuration.typesize,
            self.configuration.cname,
            self.configuration.blocksize.unwrap_or(0),
        )
        .map_err(|err: BloscError| CodecError::Other(err.to_string()))
    }

    fn decode(
        &self,
        encoded_value: Vec<u8>,
        _decoded_representation: &BytesRepresentation,
    ) -> Result<Vec<u8>, CodecError> {
        decompress_bytes(&encoded_value).map_err(|e| CodecError::Other(e.to_string()))
    }

    fn partial_decoder<'a>(
        &'a self,
        input_handle: Box<dyn BytesPartialDecoderTraits + 'a>,
    ) -> Box<dyn BytesPartialDecoderTraits + 'a> {
        Box::new(blosc_partial_decoder::BloscPartialDecoder::new(
            input_handle,
        ))
    }

    fn compute_encoded_size(
        &self,
        _bytes_representation: &BytesRepresentation,
    ) -> BytesRepresentation {
        BytesRepresentation::VariableSize
    }
}
