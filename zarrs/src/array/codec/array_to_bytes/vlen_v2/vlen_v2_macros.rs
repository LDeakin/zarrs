macro_rules! vlen_v2_module {
    ($module:ident, $module_codec:ident, $struct:ident) => {
        mod $module_codec;

        use std::sync::Arc;

        pub use $module::IDENTIFIER;

        pub use $module_codec::$struct;

        use crate::{
            array::codec::{Codec, CodecPlugin},
            metadata::v2::array::codec::$module,
            metadata::v3::MetadataV3,
            plugin::{PluginCreateError, PluginMetadataInvalidError},
        };

        // Register the codec.
        inventory::submit! {
            CodecPlugin::new(IDENTIFIER, is_name, create_codec)
        }

        fn is_name(name: &str) -> bool {
            name.eq(IDENTIFIER)
        }

        fn create_codec(metadata: &MetadataV3) -> Result<Codec, PluginCreateError> {
            if metadata.configuration_is_none_or_empty() {
                let codec = Arc::new($struct::new());
                Ok(Codec::ArrayToBytes(codec))
            } else {
                Err(PluginMetadataInvalidError::new(IDENTIFIER, "codec", metadata.clone()).into())
            }
        }
    };
}

macro_rules! vlen_v2_codec {
    ($struct:ident,$identifier:expr) => {
        use std::sync::Arc;

        use zarrs_metadata::v3::MetadataV3;

        use crate::array::{
            codec::{
                array_to_bytes::vlen_v2::VlenV2Codec, ArrayPartialDecoderTraits,
                ArrayPartialEncoderTraits, ArrayToBytesCodecTraits, BytesPartialDecoderTraits,
                BytesPartialEncoderTraits, CodecError, CodecMetadataOptions, CodecOptions,
                CodecTraits,
            },
            ArrayBytes, ArrayCodecTraits, BytesRepresentation, ChunkRepresentation, RawBytes,
            RecommendedConcurrency,
        };

        #[cfg(feature = "async")]
        use crate::array::codec::{AsyncArrayPartialDecoderTraits, AsyncBytesPartialDecoderTraits};

        #[doc = concat!("The `", $identifier, "` codec implementation.")]
        #[derive(Debug, Clone)]
        pub struct $struct {
            inner: Arc<VlenV2Codec>,
        }

        impl $struct {
            #[doc = concat!("Create a new `", $identifier, "` codec.")]
            #[must_use]
            pub fn new() -> Self {
                Self {
                    inner: Arc::new(VlenV2Codec::new($identifier.to_string())),
                }
            }
        }

        impl Default for $struct {
            fn default() -> Self {
                Self::new()
            }
        }

        impl CodecTraits for $struct {
            fn create_metadata_opt(&self, options: &CodecMetadataOptions) -> Option<MetadataV3> {
                self.inner.create_metadata_opt(options)
            }

            fn partial_decoder_should_cache_input(&self) -> bool {
                self.inner.partial_decoder_should_cache_input()
            }

            fn partial_decoder_decodes_all(&self) -> bool {
                self.inner.partial_decoder_decodes_all()
            }
        }

        impl ArrayCodecTraits for $struct {
            fn recommended_concurrency(
                &self,
                decoded_representation: &ChunkRepresentation,
            ) -> Result<RecommendedConcurrency, CodecError> {
                self.inner.recommended_concurrency(decoded_representation)
            }
        }

        #[cfg_attr(feature = "async", async_trait::async_trait)]
        impl ArrayToBytesCodecTraits for $struct {
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
    };
}

pub(crate) use vlen_v2_codec;
pub(crate) use vlen_v2_module;
