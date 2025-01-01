use std::sync::Arc;

use zarrs_metadata::v3::MetadataV3;

use crate::array::{
    codec::{
        array_to_bytes::vlen_v2::VlenV2Codec, ArrayPartialDecoderTraits, ArrayPartialEncoderTraits,
        ArrayToBytesCodecTraits, BytesPartialDecoderTraits, BytesPartialEncoderTraits, CodecError,
        CodecOptions, CodecTraits,
    },
    ArrayBytes, ArrayCodecTraits, ArrayMetadataOptions, BytesRepresentation, ChunkRepresentation,
    RawBytes, RecommendedConcurrency,
};

#[cfg(feature = "async")]
use crate::array::codec::{AsyncArrayPartialDecoderTraits, AsyncBytesPartialDecoderTraits};

/// The `vlen-utf8` codec implementation.
#[derive(Debug, Clone)]
pub struct VlenUtf8Codec {
    inner: Arc<VlenV2Codec>,
}

impl VlenUtf8Codec {
    /// Create a new `vlen-utf8` codec.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(VlenV2Codec::new("vlen-utf8".to_string())),
        }
    }
}

impl Default for VlenUtf8Codec {
    fn default() -> Self {
        Self::new()
    }
}

impl CodecTraits for VlenUtf8Codec {
    fn create_metadata_opt(&self, options: &ArrayMetadataOptions) -> Option<MetadataV3> {
        self.inner.create_metadata_opt(options)
    }

    fn partial_decoder_should_cache_input(&self) -> bool {
        self.inner.partial_decoder_should_cache_input()
    }

    fn partial_decoder_decodes_all(&self) -> bool {
        self.inner.partial_decoder_decodes_all()
    }
}

impl ArrayCodecTraits for VlenUtf8Codec {
    fn recommended_concurrency(
        &self,
        decoded_representation: &ChunkRepresentation,
    ) -> Result<RecommendedConcurrency, CodecError> {
        self.inner.recommended_concurrency(decoded_representation)
    }
}

#[cfg_attr(feature = "async", async_trait::async_trait)]
impl ArrayToBytesCodecTraits for VlenUtf8Codec {
    fn dynamic(self: Arc<Self>) -> Arc<dyn ArrayToBytesCodecTraits> {
        self as Arc<dyn ArrayToBytesCodecTraits>
    }

    fn encode<'a>(
        &self,
        bytes: ArrayBytes<'a>,
        decoded_representation: &ChunkRepresentation,
        options: &CodecOptions,
    ) -> Result<RawBytes<'a>, CodecError> {
        self.inner.encode(bytes, decoded_representation, options)
    }

    fn decode<'a>(
        &self,
        bytes: RawBytes<'a>,
        decoded_representation: &ChunkRepresentation,
        options: &CodecOptions,
    ) -> Result<ArrayBytes<'a>, CodecError> {
        self.inner.decode(bytes, decoded_representation, options)
    }

    fn partial_decoder(
        self: Arc<Self>,
        input_handle: Arc<dyn BytesPartialDecoderTraits>,
        decoded_representation: &ChunkRepresentation,
        options: &CodecOptions,
    ) -> Result<Arc<dyn ArrayPartialDecoderTraits>, CodecError> {
        self.inner
            .clone()
            .partial_decoder(input_handle, decoded_representation, options)
    }

    fn partial_encoder(
        self: Arc<Self>,
        input_handle: Arc<dyn BytesPartialDecoderTraits>,
        output_handle: Arc<dyn BytesPartialEncoderTraits>,
        decoded_representation: &ChunkRepresentation,
        options: &CodecOptions,
    ) -> Result<Arc<dyn ArrayPartialEncoderTraits>, CodecError> {
        self.inner.clone().partial_encoder(
            input_handle,
            output_handle,
            decoded_representation,
            options,
        )
    }

    #[cfg(feature = "async")]
    async fn async_partial_decoder(
        self: Arc<Self>,
        input_handle: Arc<dyn AsyncBytesPartialDecoderTraits>,
        decoded_representation: &ChunkRepresentation,
        options: &CodecOptions,
    ) -> Result<Arc<dyn AsyncArrayPartialDecoderTraits>, CodecError> {
        self.inner
            .clone()
            .async_partial_decoder(input_handle, decoded_representation, options)
            .await
    }

    fn compute_encoded_size(
        &self,
        decoded_representation: &ChunkRepresentation,
    ) -> Result<BytesRepresentation, CodecError> {
        self.inner.compute_encoded_size(decoded_representation)
    }
}
