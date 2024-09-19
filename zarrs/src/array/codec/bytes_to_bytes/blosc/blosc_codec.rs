use std::{borrow::Cow, ffi::c_char, sync::Arc};

use blosc_sys::{blosc_get_complib_info, BLOSC_MAX_OVERHEAD};

use crate::{
    array::{
        codec::{
            BytesPartialDecoderTraits, BytesToBytesCodecTraits, CodecError, CodecOptions,
            CodecTraits, RecommendedConcurrency,
        },
        ArrayMetadataOptions, BytesRepresentation, RawBytes,
    },
    metadata::v3::MetadataV3,
    plugin::PluginCreateError,
};

#[cfg(feature = "async")]
use crate::array::codec::AsyncBytesPartialDecoderTraits;

use super::{
    blosc_compress_bytes, blosc_decompress_bytes, blosc_partial_decoder, blosc_validate,
    compressor_as_cstr, BloscCodecConfiguration, BloscCodecConfigurationV1, BloscCompressionLevel,
    BloscCompressor, BloscError, BloscShuffleMode, IDENTIFIER,
};

/// A `blosc` codec implementation.
#[derive(Clone, Debug)]
pub struct BloscCodec {
    cname: BloscCompressor,
    clevel: BloscCompressionLevel,
    blocksize: usize,
    shuffle_mode: Option<BloscShuffleMode>,
    typesize: Option<usize>,
}

impl BloscCodec {
    /// Create a new `blosc` codec.
    ///
    /// The block size is chosen automatically if `blocksize` is none or zero.
    /// `typesize` must be a positive integer if shuffling is enabled.
    ///
    /// # Errors
    ///
    /// Returns [`PluginCreateError`] if
    ///  - the compressor is not supported, or
    ///  - `typesize` is [`None`] and shuffling is enabled.
    pub fn new(
        cname: BloscCompressor,
        clevel: BloscCompressionLevel,
        blocksize: Option<usize>,
        shuffle_mode: BloscShuffleMode,
        typesize: Option<usize>,
    ) -> Result<Self, PluginCreateError> {
        if shuffle_mode != BloscShuffleMode::NoShuffle
            && (typesize.is_none() || typesize == Some(0))
        {
            return Err(PluginCreateError::from(
                "typesize is a positive integer required if shuffling is enabled.",
            ));
        }

        // Check that the compressor is available
        let support = unsafe {
            blosc_get_complib_info(
                compressor_as_cstr(cname).cast::<c_char>(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            )
        };
        if support < 0 {
            return Err(PluginCreateError::from(format!(
                "compressor {cname:?} is not supported."
            )));
        }

        Ok(Self {
            cname,
            clevel,
            blocksize: blocksize.unwrap_or_default(),
            shuffle_mode: Some(shuffle_mode),
            typesize,
        })
    }

    /// Create a new `blosc` codec from configuration.
    ///
    /// # Errors
    ///
    /// Returns [`PluginCreateError`] if the configuration is not supported.
    pub fn new_with_configuration(
        configuration: &BloscCodecConfiguration,
    ) -> Result<Self, PluginCreateError> {
        match configuration {
            BloscCodecConfiguration::V1(configuration) => Self::new(
                configuration.cname,
                configuration.clevel,
                Some(configuration.blocksize),
                configuration.shuffle,
                configuration.typesize,
            ),
        }
    }

    fn do_encode(&self, decoded_value: &[u8], n_threads: usize) -> Result<Vec<u8>, CodecError> {
        let typesize = self.typesize.unwrap_or_default();
        blosc_compress_bytes(
            decoded_value,
            self.clevel,
            self.shuffle_mode.unwrap_or(if typesize > 0 {
                BloscShuffleMode::BitShuffle
            } else {
                BloscShuffleMode::NoShuffle
            }),
            typesize,
            self.cname,
            self.blocksize,
            n_threads,
        )
        .map_err(|err: BloscError| CodecError::Other(err.to_string()))
    }

    fn do_decode(encoded_value: &[u8], n_threads: usize) -> Result<Vec<u8>, CodecError> {
        blosc_validate(encoded_value).map_or_else(
            || Err(CodecError::from("blosc encoded value is invalid")),
            |destsize| {
                blosc_decompress_bytes(encoded_value, destsize, n_threads)
                    .map_err(|e| CodecError::from(e.to_string()))
            },
        )
    }
}

impl CodecTraits for BloscCodec {
    fn create_metadata_opt(&self, _options: &ArrayMetadataOptions) -> Option<MetadataV3> {
        let configuration = BloscCodecConfigurationV1 {
            cname: self.cname,
            clevel: self.clevel,
            shuffle: self.shuffle_mode.unwrap_or_else(|| {
                if self.typesize.unwrap_or_default() > 0 {
                    BloscShuffleMode::BitShuffle
                } else {
                    BloscShuffleMode::NoShuffle
                }
            }),
            typesize: self.typesize,
            blocksize: self.blocksize,
        };
        Some(MetadataV3::new_with_serializable_configuration(IDENTIFIER, &configuration).unwrap())
    }

    fn partial_decoder_should_cache_input(&self) -> bool {
        false
    }

    fn partial_decoder_decodes_all(&self) -> bool {
        true
    }
}

#[cfg_attr(feature = "async", async_trait::async_trait)]
impl BytesToBytesCodecTraits for BloscCodec {
    fn recommended_concurrency(
        &self,
        _decoded_representation: &BytesRepresentation,
    ) -> Result<RecommendedConcurrency, CodecError> {
        // TODO: Dependent on the block size, recommended concurrency could be > 1
        Ok(RecommendedConcurrency::new_maximum(1))
    }

    fn encode<'a>(
        &self,
        decoded_value: RawBytes<'a>,
        _options: &CodecOptions,
    ) -> Result<RawBytes<'a>, CodecError> {
        // let n_threads = std::cmp::min(
        //     options.concurrent_limit(),
        //     std::thread::available_parallelism().unwrap(),
        // )
        // .get();
        let n_threads = 1;
        Ok(Cow::Owned(self.do_encode(&decoded_value, n_threads)?))
    }

    fn decode<'a>(
        &self,
        encoded_value: RawBytes<'a>,
        _decoded_representation: &BytesRepresentation,
        _options: &CodecOptions,
    ) -> Result<RawBytes<'a>, CodecError> {
        // let n_threads = std::cmp::min(
        //     options.concurrent_limit(),
        //     std::thread::available_parallelism().unwrap(),
        // )
        // .get();
        let n_threads = 1;
        Ok(Cow::Owned(Self::do_decode(&encoded_value, n_threads)?))
    }

    fn partial_decoder(
        self: Arc<Self>,
        input_handle: Arc<dyn BytesPartialDecoderTraits>,
        _decoded_representation: &BytesRepresentation,
        _parallel: &CodecOptions,
    ) -> Result<Arc<dyn BytesPartialDecoderTraits>, CodecError> {
        Ok(Arc::new(blosc_partial_decoder::BloscPartialDecoder::new(
            input_handle,
        )))
    }

    #[cfg(feature = "async")]
    async fn async_partial_decoder(
        self: Arc<Self>,
        input_handle: Arc<dyn AsyncBytesPartialDecoderTraits>,
        _decoded_representation: &BytesRepresentation,
        _parallel: &CodecOptions,
    ) -> Result<Arc<dyn AsyncBytesPartialDecoderTraits>, CodecError> {
        Ok(Arc::new(
            blosc_partial_decoder::AsyncBloscPartialDecoder::new(input_handle),
        ))
    }

    fn compute_encoded_size(
        &self,
        decoded_representation: &BytesRepresentation,
    ) -> BytesRepresentation {
        decoded_representation
            .size()
            .map_or(BytesRepresentation::UnboundedSize, |size| {
                BytesRepresentation::BoundedSize(size + u64::from(BLOSC_MAX_OVERHEAD))
            })
    }
}
