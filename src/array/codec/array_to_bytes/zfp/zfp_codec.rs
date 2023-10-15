use zfp_sys::{
    zfp_compress, zfp_stream_maximum_size, zfp_stream_rewind, zfp_stream_set_bit_stream,
};

use crate::{
    array::{
        codec::{
            ArrayCodecTraits, ArrayPartialDecoderTraits, ArrayToBytesCodecTraits,
            BytesPartialDecoderTraits, Codec, CodecError, CodecPlugin, CodecTraits,
        },
        ArrayRepresentation, BytesRepresentation,
    },
    metadata::Metadata,
    plugin::PluginCreateError,
};

use super::{
    zarr_data_type_to_zfp_data_type,
    zfp_bitstream::ZfpBitstream,
    zfp_configuration::{
        ZfpFixedAccuracyConfiguration, ZfpFixedPrecisionConfiguration, ZfpFixedRateConfiguration,
    },
    zfp_decode,
    zfp_field::ZfpField,
    zfp_partial_decoder,
    zfp_stream::ZfpStream,
    ZfpCodecConfiguration, ZfpCodecConfigurationV1, ZfpExpertParams, ZfpMode,
};

const IDENTIFIER: &str = "zfp";

// Register the codec.
inventory::submit! {
    CodecPlugin::new(IDENTIFIER, is_name_zfp, create_codec_zfp)
}

fn is_name_zfp(name: &str) -> bool {
    name.eq(IDENTIFIER)
}

fn create_codec_zfp(metadata: &Metadata) -> Result<Codec, PluginCreateError> {
    let configuration: ZfpCodecConfiguration = metadata.to_configuration()?;
    let codec: Box<ZfpCodec> = Box::new(ZfpCodec::new_with_configuration(&configuration));
    Ok(Codec::ArrayToBytes(codec))
}

/// A Zfp codec implementation.
#[derive(Clone, Copy, Debug)]
pub struct ZfpCodec {
    mode: ZfpMode,
}

impl ZfpCodec {
    /// Create a new `Zfp` codec in expert mode.
    #[must_use]
    pub fn new_expert(expert_params: ZfpExpertParams) -> Self {
        Self {
            mode: ZfpMode::Expert(expert_params),
        }
    }

    /// Create a new `Zfp` codec in fixed rate mode.
    #[must_use]
    pub fn new_fixed_rate(rate: f64) -> Self {
        Self {
            mode: ZfpMode::FixedRate(rate),
        }
    }

    /// Create a new `Zfp` codec in fixed precision mode.
    #[must_use]
    pub fn new_fixed_precision(precision: u32) -> Self {
        Self {
            mode: ZfpMode::FixedPrecision(precision),
        }
    }

    /// Create a new `Zfp` codec in fixed accuracy mode.
    #[must_use]
    pub fn new_fixed_accuracy(tolerance: f64) -> Self {
        Self {
            mode: ZfpMode::FixedAccuracy(tolerance),
        }
    }

    /// Create a new `Zfp` codec in reversible mode.
    #[must_use]
    pub fn new_reversible() -> Self {
        Self {
            mode: ZfpMode::Reversible,
        }
    }

    /// Create a new `Zfp` codec from configuration.
    #[must_use]
    pub fn new_with_configuration(configuration: &ZfpCodecConfiguration) -> Self {
        type V1 = ZfpCodecConfigurationV1;
        let ZfpCodecConfiguration::V1(configuration) = configuration;
        match configuration {
            V1::Expert(cfg) => Self::new_expert(*cfg),
            V1::FixedRate(cfg) => Self::new_fixed_rate(cfg.rate),
            V1::FixedPrecision(cfg) => Self::new_fixed_precision(cfg.precision),
            V1::FixedAccuracy(cfg) => Self::new_fixed_accuracy(cfg.tolerance),
            V1::Reversible => Self::new_reversible(),
        }
    }
}

impl CodecTraits for ZfpCodec {
    fn create_metadata(&self) -> Option<Metadata> {
        let configuration = match self.mode {
            ZfpMode::Expert(expert) => ZfpCodecConfigurationV1::Expert(expert),
            ZfpMode::FixedRate(rate) => {
                ZfpCodecConfigurationV1::FixedRate(ZfpFixedRateConfiguration { rate })
            }
            ZfpMode::FixedPrecision(precision) => {
                ZfpCodecConfigurationV1::FixedPrecision(ZfpFixedPrecisionConfiguration {
                    precision,
                })
            }
            ZfpMode::FixedAccuracy(tolerance) => {
                ZfpCodecConfigurationV1::FixedAccuracy(ZfpFixedAccuracyConfiguration { tolerance })
            }
            ZfpMode::Reversible => ZfpCodecConfigurationV1::Reversible,
        };
        Some(Metadata::new_with_serializable_configuration(IDENTIFIER, &configuration).unwrap())
    }

    fn partial_decoder_should_cache_input(&self) -> bool {
        false
    }

    fn partial_decoder_decodes_all(&self) -> bool {
        true
    }
}

impl ArrayCodecTraits for ZfpCodec {
    fn encode(
        &self,
        mut decoded_value: Vec<u8>,
        decoded_representation: &ArrayRepresentation,
    ) -> Result<Vec<u8>, CodecError> {
        let Some(zfp_type) = zarr_data_type_to_zfp_data_type(decoded_representation.data_type())
        else {
            return Err(CodecError::from(
                "data type {} is unsupported for zfp codec",
            ));
        };
        let Some(field) = ZfpField::new(
            &mut decoded_value,
            zfp_type,
            &decoded_representation
                .shape()
                .iter()
                .map(|u| usize::try_from(*u).unwrap())
                .collect::<Vec<usize>>(),
        ) else {
            return Err(CodecError::from("failed to create zfp field"));
        };
        let Some(zfp) = ZfpStream::new(&self.mode, zfp_type) else {
            return Err(CodecError::from("failed to create zfp stream"));
        };

        let bufsize = unsafe { zfp_stream_maximum_size(zfp.as_zfp_stream(), field.as_zfp_field()) };
        let mut encoded_value: Vec<u8> = vec![0; bufsize];

        let Some(stream) = ZfpBitstream::new(&mut encoded_value) else {
            return Err(CodecError::from("failed to create zfp field"));
        };
        unsafe {
            zfp_stream_set_bit_stream(zfp.as_zfp_stream(), stream.as_bitstream());
            zfp_stream_rewind(zfp.as_zfp_stream()); // needed?
        }

        // Compress array
        let size = unsafe { zfp_compress(zfp.as_zfp_stream(), field.as_zfp_field()) };

        if size == 0 {
            Err(CodecError::from("zfp compression failed"))
        } else {
            Ok(encoded_value)
        }
    }

    fn decode(
        &self,
        encoded_value: Vec<u8>,
        decoded_representation: &ArrayRepresentation,
    ) -> Result<Vec<u8>, CodecError> {
        zfp_decode(&self.mode, encoded_value, decoded_representation)
    }
}

impl ArrayToBytesCodecTraits for ZfpCodec {
    fn partial_decoder<'a>(
        &'a self,
        input_handle: Box<dyn BytesPartialDecoderTraits + 'a>,
    ) -> Box<dyn ArrayPartialDecoderTraits + 'a> {
        Box::new(zfp_partial_decoder::ZfpPartialDecoder::new(
            input_handle,
            &self.mode,
        ))
    }

    fn compute_encoded_size(
        &self,
        _decoded_representation: &ArrayRepresentation,
    ) -> BytesRepresentation {
        BytesRepresentation::VariableSize
    }
}