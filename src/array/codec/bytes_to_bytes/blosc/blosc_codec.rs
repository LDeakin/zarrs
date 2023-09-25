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
    blosc_partial_decoder, decompress_bytes, BloscCodecConfiguration, BloscCodecConfigurationV1,
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
    ctx: blosc::Context,
    // configuration is mostly just the fields in ctx, which are private
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
        compressor: blosc::Compressor,
        clevel: blosc::Clevel,
        blocksize: Option<usize>,
        shuffle_mode: blosc::ShuffleMode,
        typesize: Option<usize>,
    ) -> Result<Self, PluginCreateError> {
        if shuffle_mode != blosc::ShuffleMode::None && typesize.is_none() {
            return Err(PluginCreateError::Other {
                error_str: "typesize is a positive integer required if shuffle mode is not none."
                    .into(),
            });
        }

        let ctx = blosc::Context::new()
            .compressor(compressor)
            .map_err(|_| PluginCreateError::Other {
                error_str: format!("blosc compressor {compressor:?} is not available"),
            })?
            .blocksize(blocksize)
            .clevel(clevel)
            .shuffle(shuffle_mode)
            .typesize(typesize);

        let configuration = BloscCodecConfigurationV1 {
            compressor,
            clevel,
            blocksize,
            shuffle: shuffle_mode,
            typesize: typesize.unwrap_or_default(),
        };

        Ok(BloscCodec { ctx, configuration })
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
            configuration.compressor,
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
        Ok(self.ctx.compress(&decoded_value).into())
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
