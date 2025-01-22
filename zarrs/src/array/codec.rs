//! Zarr codecs.
//!
//! Array chunks can be encoded using a sequence of codecs, each of which specifies a bidirectional transform (an encode transform and a decode transform).
//! A codec can map array to an array, an array to bytes, or bytes to bytes.
//! A codec may support partial decoding to extract a byte range or array subset without needing to decode the entire input.
//!
//! A [`CodecChain`] represents a codec sequence consisting of any number of array to array and bytes to bytes codecs, and one array to bytes codec.
//! A codec chain is itself an array to bytes codec.
//! A cache may be inserted into a codec chain to optimise partial decoding where appropriate.
//!
//! See <https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html#id18>.

pub mod array_to_array;
pub mod array_to_bytes;
pub mod bytes_to_bytes;
pub mod metadata_options;
pub mod options;

use derive_more::derive::Display;
pub use metadata_options::CodecMetadataOptions;
pub use options::{CodecOptions, CodecOptionsBuilder};

// Array to array
#[cfg(feature = "bitround")]
pub use array_to_array::bitround::{
    BitroundCodec, BitroundCodecConfiguration, BitroundCodecConfigurationV1,
};
#[cfg(feature = "transpose")]
pub use array_to_array::transpose::{
    TransposeCodec, TransposeCodecConfiguration, TransposeCodecConfigurationV1,
};

// Array to bytes
pub use array_to_bytes::bytes::{BytesCodec, BytesCodecConfiguration, BytesCodecConfigurationV1};
pub use array_to_bytes::codec_chain::CodecChain;
#[cfg(feature = "pcodec")]
pub use array_to_bytes::pcodec::{
    PcodecCodec, PcodecCodecConfiguration, PcodecCodecConfigurationV1,
};
#[cfg(feature = "sharding")]
pub use array_to_bytes::sharding::{
    ShardingCodec, ShardingCodecConfiguration, ShardingCodecConfigurationV1,
};
#[cfg(feature = "zfp")]
pub use array_to_bytes::zfp::{ZfpCodec, ZfpCodecConfiguration, ZfpCodecConfigurationV1};

// Bytes to bytes
#[cfg(feature = "blosc")]
pub use bytes_to_bytes::blosc::{BloscCodec, BloscCodecConfiguration, BloscCodecConfigurationV1};
#[cfg(feature = "bz2")]
pub use bytes_to_bytes::bz2::{Bz2Codec, Bz2CodecConfiguration, Bz2CodecConfigurationV1};
#[cfg(feature = "crc32c")]
pub use bytes_to_bytes::crc32c::{
    Crc32cCodec, Crc32cCodecConfiguration, Crc32cCodecConfigurationV1,
};
#[cfg(feature = "gzip")]
pub use bytes_to_bytes::gzip::{GzipCodec, GzipCodecConfiguration, GzipCodecConfigurationV1};
#[cfg(feature = "zstd")]
pub use bytes_to_bytes::zstd::{ZstdCodec, ZstdCodecConfiguration, ZstdCodecConfigurationV1};

use thiserror::Error;

mod array_partial_decoder_cache;
mod bytes_partial_decoder_cache;
pub(crate) use array_partial_decoder_cache::ArrayPartialDecoderCache;
pub(crate) use bytes_partial_decoder_cache::BytesPartialDecoderCache;

mod byte_interval_partial_decoder;
pub use byte_interval_partial_decoder::ByteIntervalPartialDecoder;

#[cfg(feature = "async")]
pub use byte_interval_partial_decoder::AsyncByteIntervalPartialDecoder;

mod array_partial_encoder_default;
pub use array_partial_encoder_default::ArrayPartialEncoderDefault;

mod array_to_array_partial_encoder_default;
pub use array_to_array_partial_encoder_default::ArrayToArrayPartialEncoderDefault;

mod bytes_partial_encoder_default;
pub use bytes_partial_encoder_default::BytesPartialEncoderDefault;
use zarrs_metadata::ArrayShape;

use crate::storage::{StoreKeyOffsetValue, WritableStorage};
use crate::{
    array_subset::{ArraySubset, IncompatibleArraySubsetAndShapeError},
    byte_range::{extract_byte_ranges_read_seek, ByteOffset, ByteRange, InvalidByteRangeError},
    metadata::v3::MetadataV3,
    plugin::{Plugin, PluginCreateError},
    storage::{ReadableStorage, StorageError, StoreKey},
};

#[cfg(feature = "async")]
use crate::storage::AsyncReadableStorage;

use std::any::Any;
use std::borrow::Cow;
use std::sync::Arc;

use super::{
    array_bytes::RawBytesOffsetsCreateError, concurrency::RecommendedConcurrency, ArrayBytes,
    ArrayBytesFixedDisjointView, BytesRepresentation, ChunkRepresentation, ChunkShape, DataType,
    RawBytes,
};

/// A codec plugin.
pub type CodecPlugin = Plugin<Codec>;
inventory::collect!(CodecPlugin);

/// A generic array to array, array to bytes, or bytes to bytes codec.
#[derive(Debug)]
pub enum Codec {
    /// An array to array codec.
    ArrayToArray(Arc<dyn ArrayToArrayCodecTraits>),
    /// An array to bytes codec.
    ArrayToBytes(Arc<dyn ArrayToBytesCodecTraits>),
    /// A bytes to bytes codec.
    BytesToBytes(Arc<dyn BytesToBytesCodecTraits>),
}

impl Codec {
    /// Create a codec from metadata.
    ///
    /// # Errors
    /// Returns [`PluginCreateError`] if the metadata is invalid or not associated with a registered codec plugin.
    pub fn from_metadata(metadata: &MetadataV3) -> Result<Self, PluginCreateError> {
        for plugin in inventory::iter::<CodecPlugin> {
            if plugin.match_name(metadata.name()) {
                return plugin.create(metadata);
            }
        }
        #[cfg(miri)]
        {
            // Inventory does not work in miri, so manually handle all known codecs
            match metadata.name() {
                #[cfg(feature = "transpose")]
                array_to_array::transpose::IDENTIFIER => {
                    return array_to_array::transpose::create_codec_transpose(metadata);
                }
                #[cfg(feature = "bitround")]
                array_to_array::bitround::IDENTIFIER => {
                    return array_to_array::bitround::create_codec_bitround(metadata);
                }
                array_to_bytes::bytes::IDENTIFIER => {
                    return array_to_bytes::bytes::create_codec_bytes(metadata);
                }
                #[cfg(feature = "pcodec")]
                array_to_bytes::pcodec::IDENTIFIER => {
                    return array_to_bytes::pcodec::create_codec_pcodec(metadata);
                }
                #[cfg(feature = "sharding")]
                array_to_bytes::sharding::IDENTIFIER => {
                    return array_to_bytes::sharding::create_codec_sharding(metadata);
                }
                #[cfg(feature = "zfp")]
                array_to_bytes::zfp::IDENTIFIER => {
                    return array_to_bytes::zfp::create_codec_zfp(metadata);
                }
                array_to_bytes::vlen::IDENTIFIER => {
                    return array_to_bytes::vlen::create_codec_vlen(metadata);
                }
                array_to_bytes::vlen_v2::IDENTIFIER => {
                    return array_to_bytes::vlen_v2::create_codec_vlen_v2(metadata);
                }
                #[cfg(feature = "blosc")]
                bytes_to_bytes::blosc::IDENTIFIER => {
                    return bytes_to_bytes::blosc::create_codec_blosc(metadata);
                }
                #[cfg(feature = "bz2")]
                bytes_to_bytes::bz2::IDENTIFIER => {
                    return bytes_to_bytes::bz2::create_codec_bz2(metadata);
                }
                #[cfg(feature = "crc32c")]
                bytes_to_bytes::crc32c::IDENTIFIER => {
                    return bytes_to_bytes::crc32c::create_codec_crc32c(metadata);
                }
                #[cfg(feature = "gdeflate")]
                bytes_to_bytes::gdeflate::IDENTIFIER => {
                    return bytes_to_bytes::gdeflate::create_codec_gdeflate(metadata);
                }
                #[cfg(feature = "gzip")]
                bytes_to_bytes::gzip::IDENTIFIER => {
                    return bytes_to_bytes::gzip::create_codec_gzip(metadata);
                }
                #[cfg(feature = "zstd")]
                bytes_to_bytes::zstd::IDENTIFIER => {
                    return bytes_to_bytes::zstd::create_codec_zstd(metadata);
                }
                _ => {}
            }
        }
        Err(PluginCreateError::Unsupported {
            name: metadata.name().to_string(),
            plugin_type: "codec".to_string(),
        })
    }
}

/// Codec traits.
pub trait CodecTraits: Send + Sync {
    /// Create metadata.
    ///
    /// A hidden codec (e.g. a cache) will return [`None`], since it will not have any associated metadata.
    fn create_metadata_opt(&self, options: &CodecMetadataOptions) -> Option<MetadataV3>;

    /// Create metadata with default options.
    ///
    /// A hidden codec (e.g. a cache) will return [`None`], since it will not have any associated metadata.
    fn create_metadata(&self) -> Option<MetadataV3> {
        self.create_metadata_opt(&CodecMetadataOptions::default())
    }

    /// Indicates if the input to a codecs partial decoder should be cached for optimal performance.
    /// If true, a cache may be inserted *before* it in a [`CodecChain`] partial decoder.
    fn partial_decoder_should_cache_input(&self) -> bool;

    /// Indicates if a partial decoder decodes all bytes from its input handle and its output should be cached for optimal performance.
    /// If true, a cache will be inserted at some point *after* it in a [`CodecChain`] partial decoder.
    fn partial_decoder_decodes_all(&self) -> bool;
}

/// Traits for both array to array and array to bytes codecs.
pub trait ArrayCodecTraits: CodecTraits {
    /// Return the recommended concurrency for the requested decoded representation.
    ///
    /// # Errors
    /// Returns [`CodecError`] if the decoded representation is not valid for the codec.
    fn recommended_concurrency(
        &self,
        decoded_representation: &ChunkRepresentation,
    ) -> Result<RecommendedConcurrency, CodecError>;

    /// Return the partial decode granularity.
    ///
    /// This represents the shape of the smallest subset of a chunk that can be efficiently decoded if the chunk were subdivided into a regular grid.
    /// For most codecs, this is just the shape of the chunk.
    /// It is the shape of the "inner chunks" for the sharding codec.
    fn partial_decode_granularity(
        &self,
        decoded_representation: &ChunkRepresentation,
    ) -> ChunkShape {
        decoded_representation.shape().into()
    }
}

/// Partial bytes decoder traits.
pub trait BytesPartialDecoderTraits: Any + Send + Sync {
    /// Partially decode bytes.
    ///
    /// Returns [`None`] if partial decoding of the input handle returns [`None`].
    ///
    /// # Errors
    /// Returns [`CodecError`] if a codec fails or a byte range is invalid.
    fn partial_decode(
        &self,
        decoded_regions: &[ByteRange],
        options: &CodecOptions,
    ) -> Result<Option<Vec<RawBytes<'_>>>, CodecError>;

    /// Partially decode bytes and concatenate.
    ///
    /// Returns [`None`] if partial decoding of the input handle returns [`None`].
    ///
    /// Codecs can manually implement this method with a preallocated array to reduce internal allocations.
    ///
    /// # Errors
    /// Returns [`CodecError`] if a codec fails or a byte range is invalid.
    fn partial_decode_concat(
        &self,
        decoded_regions: &[ByteRange],
        options: &CodecOptions,
    ) -> Result<Option<RawBytes<'_>>, CodecError> {
        Ok(self
            .partial_decode(decoded_regions, options)?
            .map(|vecs| Cow::Owned(vecs.concat())))
    }

    /// Decode all bytes.
    ///
    /// Returns [`None`] if partial decoding of the input handle returns [`None`].
    ///
    /// # Errors
    /// Returns [`CodecError`] if a codec fails.
    fn decode(&self, options: &CodecOptions) -> Result<Option<RawBytes<'_>>, CodecError> {
        Ok(self
            .partial_decode(&[ByteRange::FromStart(0, None)], options)?
            .map(|mut v| v.remove(0)))
    }
}

#[cfg(feature = "async")]
/// Asynchronous partial bytes decoder traits.
#[async_trait::async_trait]
pub trait AsyncBytesPartialDecoderTraits: Any + Send + Sync {
    /// Partially decode bytes.
    ///
    /// Returns [`None`] if partial decoding of the input handle returns [`None`].
    ///
    /// # Errors
    /// Returns [`CodecError`] if a codec fails or a byte range is invalid.
    async fn partial_decode(
        &self,
        decoded_regions: &[ByteRange],
        options: &CodecOptions,
    ) -> Result<Option<Vec<RawBytes<'_>>>, CodecError>;

    /// Partially decode bytes and concatenate.
    ///
    /// Returns [`None`] if partial decoding of the input handle returns [`None`].
    ///
    /// # Errors
    /// Returns [`CodecError`] if a codec fails or a byte range is invalid.
    async fn partial_decode_concat(
        &self,
        decoded_regions: &[ByteRange],
        options: &CodecOptions,
    ) -> Result<Option<RawBytes<'_>>, CodecError> {
        Ok(self
            .partial_decode(decoded_regions, options)
            .await?
            .map(|vecs| Cow::Owned(vecs.concat())))
    }

    /// Decode all bytes.
    ///
    /// Returns [`None`] if partial decoding of the input handle returns [`None`].
    ///
    /// # Errors
    /// Returns [`CodecError`] if a codec fails.
    async fn decode(&self, options: &CodecOptions) -> Result<Option<RawBytes<'_>>, CodecError> {
        Ok(self
            .partial_decode(&[ByteRange::FromStart(0, None)], options)
            .await?
            .map(|mut v| v.remove(0)))
    }
}

/// Partial array decoder traits.
pub trait ArrayPartialDecoderTraits: Any + Send + Sync {
    /// Return the data type of the partial decoder.
    fn data_type(&self) -> &DataType;

    /// Partially decode a chunk.
    ///
    /// If the inner `input_handle` is a bytes decoder and partial decoding returns [`None`], then the array subsets have the fill value.
    ///
    /// # Errors
    /// Returns [`CodecError`] if a codec fails or an array subset is invalid.
    fn partial_decode(
        &self,
        array_subsets: &[ArraySubset],
        options: &CodecOptions,
    ) -> Result<Vec<ArrayBytes<'_>>, CodecError>;

    /// Partially decode into a preallocated output.
    ///
    /// This method is intended for internal use by Array.
    /// It currently only works for fixed length data types.
    ///
    /// The `array_subset` shape and dimensionality does not need to match `output_subset`, but the number of elements must match.
    /// Extracted elements from the `array_subset` are written to the subset of the output in C order.
    ///
    /// # Errors
    /// Returns [`CodecError`] if a codec fails or the number of elements in `array_subset` does not match the number of elements in `output_view`,
    fn partial_decode_into(
        &self,
        array_subset: &ArraySubset,
        output_view: &mut ArrayBytesFixedDisjointView<'_>,
        options: &CodecOptions,
    ) -> Result<(), CodecError> {
        if array_subset.num_elements() != output_view.num_elements() {
            return Err(InvalidNumberOfElementsError::new(
                array_subset.num_elements(),
                output_view.num_elements(),
            )
            .into());
        }

        let decoded_value = self
            .partial_decode(&[array_subset.clone()], options)?
            .remove(0);
        if let ArrayBytes::Fixed(decoded_value) = decoded_value {
            output_view.copy_from_slice(&decoded_value)?;
            Ok(())
        } else {
            Err(CodecError::ExpectedFixedLengthBytes)
        }
    }
}

/// Partial array encoder traits.
pub trait ArrayPartialEncoderTraits: Any + Send + Sync {
    /// Erase the chunk.
    ///
    /// # Errors
    /// Returns an error if there is an underlying store error.
    fn erase(&self) -> Result<(), CodecError>;

    /// Partially encode a chunk.
    ///
    /// # Errors
    /// Returns [`CodecError`] if a codec fails or an array subset is invalid.
    fn partial_encode(
        &self,
        subsets_and_bytes: &[(&ArraySubset, ArrayBytes<'_>)],
        options: &CodecOptions,
    ) -> Result<(), CodecError>;
}

/// Partial bytes encoder traits.
pub trait BytesPartialEncoderTraits: Any + Send + Sync {
    /// Erase the chunk.
    ///
    /// # Errors
    /// Returns an error if there is an underlying store error.
    fn erase(&self) -> Result<(), CodecError>;

    /// Partially encode a chunk.
    ///
    /// # Errors
    /// Returns [`CodecError`] if a codec fails or an array subset is invalid.
    fn partial_encode(
        &self,
        offsets_and_bytes: &[(ByteOffset, crate::array::RawBytes<'_>)],
        options: &CodecOptions,
    ) -> Result<(), CodecError>;
}

#[cfg(feature = "async")]
/// Asynchronous partial array decoder traits.
#[async_trait::async_trait]
pub trait AsyncArrayPartialDecoderTraits: Any + Send + Sync {
    /// Return the data type of the partial decoder.
    fn data_type(&self) -> &DataType;

    /// Partially decode a chunk.
    ///
    /// # Errors
    /// Returns [`CodecError`] if a codec fails, array subset is invalid, or the array subset shape does not match array view subset shape.
    async fn partial_decode(
        &self,
        array_subsets: &[ArraySubset],
        options: &CodecOptions,
    ) -> Result<Vec<ArrayBytes<'_>>, CodecError>;

    /// Async variant of [`ArrayPartialDecoderTraits::partial_decode_into`].
    #[allow(clippy::missing_safety_doc)]
    async fn partial_decode_into(
        &self,
        array_subset: &ArraySubset,
        output_view: &mut ArrayBytesFixedDisjointView<'_>,
        options: &CodecOptions,
    ) -> Result<(), CodecError> {
        if array_subset.num_elements() != output_view.num_elements() {
            return Err(InvalidNumberOfElementsError::new(
                output_view.num_elements(),
                array_subset.num_elements(),
            )
            .into());
        }
        let decoded_value = self
            .partial_decode(&[array_subset.clone()], options)
            .await?
            .remove(0);
        if let ArrayBytes::Fixed(decoded_value) = decoded_value {
            output_view.copy_from_slice(&decoded_value)?;
            Ok(())
        } else {
            Err(CodecError::ExpectedFixedLengthBytes)
        }
    }
}

/// A [`ReadableStorage`] store value partial decoder.
pub struct StoragePartialDecoder {
    storage: ReadableStorage,
    key: StoreKey,
}

impl StoragePartialDecoder {
    /// Create a new storage partial decoder.
    pub fn new(storage: ReadableStorage, key: StoreKey) -> Self {
        Self { storage, key }
    }
}

impl BytesPartialDecoderTraits for StoragePartialDecoder {
    fn partial_decode(
        &self,
        decoded_regions: &[ByteRange],
        _options: &CodecOptions,
    ) -> Result<Option<Vec<RawBytes<'_>>>, CodecError> {
        Ok(self
            .storage
            .get_partial_values_key(&self.key, decoded_regions)?
            .map(|vec_bytes| {
                vec_bytes
                    .into_iter()
                    .map(|bytes| Cow::Owned(bytes.to_vec()))
                    .collect()
            }))
    }
}

#[cfg(feature = "async")]
/// An [`AsyncReadableStorage`] store value partial decoder.
pub struct AsyncStoragePartialDecoder {
    storage: AsyncReadableStorage,
    key: StoreKey,
}

#[cfg(feature = "async")]
impl AsyncStoragePartialDecoder {
    /// Create a new storage partial decoder.
    pub fn new(storage: AsyncReadableStorage, key: StoreKey) -> Self {
        Self { storage, key }
    }
}

#[cfg(feature = "async")]
#[async_trait::async_trait]
impl AsyncBytesPartialDecoderTraits for AsyncStoragePartialDecoder {
    async fn partial_decode(
        &self,
        decoded_regions: &[ByteRange],
        _options: &CodecOptions,
    ) -> Result<Option<Vec<RawBytes<'_>>>, CodecError> {
        Ok(self
            .storage
            .get_partial_values_key(&self.key, decoded_regions)
            .await?
            .map(|vec_bytes| {
                vec_bytes
                    .into_iter()
                    .map(|bytes| Cow::Owned(bytes.to_vec()))
                    .collect()
            }))
    }
}

/// A [`WritableStorage`] store value partial encoder.
pub struct StoragePartialEncoder {
    storage: WritableStorage,
    key: StoreKey,
}

impl StoragePartialEncoder {
    /// Create a new storage partial encoder.
    pub fn new(storage: WritableStorage, key: StoreKey) -> Self {
        Self { storage, key }
    }
}

impl BytesPartialEncoderTraits for StoragePartialEncoder {
    fn erase(&self) -> Result<(), CodecError> {
        Ok(self.storage.erase(&self.key)?)
    }

    fn partial_encode(
        &self,
        offsets_and_bytes: &[(ByteOffset, crate::array::RawBytes<'_>)],
        _options: &CodecOptions,
    ) -> Result<(), CodecError> {
        let key_offset_values = offsets_and_bytes
            .iter()
            .map(|(offset, bytes)| StoreKeyOffsetValue::new(self.key.clone(), *offset, bytes))
            .collect::<Vec<_>>();
        Ok(self.storage.set_partial_values(&key_offset_values)?)
    }
}

/// Traits for array to array codecs.
#[cfg_attr(feature = "async", async_trait::async_trait)]
pub trait ArrayToArrayCodecTraits: ArrayCodecTraits + core::fmt::Debug {
    /// Return a dynamic version of the codec.
    fn dynamic(self: Arc<Self>) -> Arc<dyn ArrayToArrayCodecTraits>;

    /// Returns the size of the encoded representation given a size of the decoded representation.
    ///
    /// # Errors
    ///
    /// Returns a [`CodecError`] if the decoded representation is not supported by this codec.
    fn compute_encoded_size(
        &self,
        decoded_representation: &ChunkRepresentation,
    ) -> Result<ChunkRepresentation, CodecError>;

    /// Returns the size of the decoded representation given a size of the encoded representation.
    ///
    /// # Errors
    ///
    /// Returns a [`CodecError`] if the encoded representation is not supported by this codec.
    fn compute_decoded_shape(
        &self,
        encoded_representation: ChunkShape,
    ) -> Result<ChunkShape, CodecError>;

    /// Encode a chunk.
    ///
    /// # Errors
    /// Returns [`CodecError`] if a codec fails or `bytes` is incompatible with `decoded_representation`.
    fn encode<'a>(
        &self,
        bytes: ArrayBytes<'a>,
        decoded_representation: &ChunkRepresentation,
        options: &CodecOptions,
    ) -> Result<ArrayBytes<'a>, CodecError>;

    /// Decode a chunk.
    ///
    /// # Errors
    /// Returns [`CodecError`] if a codec fails or the decoded output is incompatible with `decoded_representation`.
    fn decode<'a>(
        &self,
        bytes: ArrayBytes<'a>,
        decoded_representation: &ChunkRepresentation,
        options: &CodecOptions,
    ) -> Result<ArrayBytes<'a>, CodecError>;

    /// Initialise a partial decoder.
    ///
    /// # Errors
    /// Returns a [`CodecError`] if initialisation fails.
    fn partial_decoder(
        self: Arc<Self>,
        input_handle: Arc<dyn ArrayPartialDecoderTraits>,
        decoded_representation: &ChunkRepresentation,
        options: &CodecOptions,
    ) -> Result<Arc<dyn ArrayPartialDecoderTraits>, CodecError>;

    /// Initialise a partial encoder.
    ///
    /// # Errors
    /// Returns a [`CodecError`] if initialisation fails.
    fn partial_encoder(
        self: Arc<Self>,
        input_handle: Arc<dyn ArrayPartialDecoderTraits>,
        output_handle: Arc<dyn ArrayPartialEncoderTraits>,
        decoded_representation: &ChunkRepresentation,
        options: &CodecOptions,
    ) -> Result<Arc<dyn ArrayPartialEncoderTraits>, CodecError>;

    #[cfg(feature = "async")]
    /// Initialise an asynchronous partial decoder.
    ///
    /// # Errors
    /// Returns a [`CodecError`] if initialisation fails.
    async fn async_partial_decoder(
        self: Arc<Self>,
        input_handle: Arc<dyn AsyncArrayPartialDecoderTraits>,
        decoded_representation: &ChunkRepresentation,
        options: &CodecOptions,
    ) -> Result<Arc<dyn AsyncArrayPartialDecoderTraits>, CodecError>;

    // TODO: async_partial_encoder
}

/// Traits for array to bytes codecs.
#[cfg_attr(feature = "async", async_trait::async_trait)]
pub trait ArrayToBytesCodecTraits: ArrayCodecTraits + core::fmt::Debug {
    /// Return a dynamic version of the codec.
    fn dynamic(self: Arc<Self>) -> Arc<dyn ArrayToBytesCodecTraits>;

    /// Returns the size of the encoded representation given a size of the decoded representation.
    ///
    /// # Errors
    /// Returns a [`CodecError`] if the decoded representation is not supported by this codec.
    fn compute_encoded_size(
        &self,
        decoded_representation: &ChunkRepresentation,
    ) -> Result<BytesRepresentation, CodecError>;

    /// Encode a chunk.
    ///
    /// # Errors
    /// Returns [`CodecError`] if a codec fails or `bytes` is incompatible with `decoded_representation`.
    fn encode<'a>(
        &self,
        bytes: ArrayBytes<'a>,
        decoded_representation: &ChunkRepresentation,
        options: &CodecOptions,
    ) -> Result<RawBytes<'a>, CodecError>;

    /// Decode a chunk.
    ///
    /// # Errors
    /// Returns [`CodecError`] if a codec fails or the decoded output is incompatible with `decoded_representation`.
    fn decode<'a>(
        &self,
        bytes: RawBytes<'a>,
        decoded_representation: &ChunkRepresentation,
        options: &CodecOptions,
    ) -> Result<ArrayBytes<'a>, CodecError>;

    /// Decode into a subset of a preallocated output.
    ///
    /// This method is intended for internal use by Array.
    /// It currently only works for fixed length data types.
    ///
    /// The decoded representation shape and dimensionality does not need to match `output_subset`, but the number of elements must match.
    /// Chunk elements are written to the subset of the output in C order.
    ///
    /// # Errors
    /// Returns [`CodecError`] if a codec fails or the number of elements in `decoded_representation` does not match the number of elements in `output_view`,
    fn decode_into(
        &self,
        bytes: RawBytes<'_>,
        decoded_representation: &ChunkRepresentation,
        output_view: &mut ArrayBytesFixedDisjointView<'_>,
        options: &CodecOptions,
    ) -> Result<(), CodecError> {
        if decoded_representation.num_elements() != output_view.num_elements() {
            return Err(InvalidNumberOfElementsError::new(
                output_view.num_elements(),
                decoded_representation.num_elements(),
            )
            .into());
        }
        let decoded_value = self.decode(bytes, decoded_representation, options)?;
        if let ArrayBytes::Fixed(decoded_value) = decoded_value {
            output_view.copy_from_slice(&decoded_value)?;
        } else {
            return Err(CodecError::ExpectedFixedLengthBytes);
        }
        Ok(())
    }

    /// Initialise a partial decoder.
    ///
    /// # Errors
    /// Returns a [`CodecError`] if initialisation fails.
    fn partial_decoder(
        self: Arc<Self>,
        input_handle: Arc<dyn BytesPartialDecoderTraits>,
        decoded_representation: &ChunkRepresentation,
        options: &CodecOptions,
    ) -> Result<Arc<dyn ArrayPartialDecoderTraits>, CodecError>;

    /// Initialise a partial encoder.
    ///
    /// # Errors
    /// Returns a [`CodecError`] if initialisation fails.
    fn partial_encoder(
        self: Arc<Self>,
        input_handle: Arc<dyn BytesPartialDecoderTraits>,
        output_handle: Arc<dyn BytesPartialEncoderTraits>,
        decoded_representation: &ChunkRepresentation,
        _options: &CodecOptions,
    ) -> Result<Arc<dyn ArrayPartialEncoderTraits>, CodecError>;

    #[cfg(feature = "async")]
    /// Initialise an asynchronous partial decoder.
    ///
    /// # Errors
    /// Returns a [`CodecError`] if initialisation fails.
    async fn async_partial_decoder(
        self: Arc<Self>,
        mut input_handle: Arc<dyn AsyncBytesPartialDecoderTraits>,
        decoded_representation: &ChunkRepresentation,
        options: &CodecOptions,
    ) -> Result<Arc<dyn AsyncArrayPartialDecoderTraits>, CodecError>;

    // TODO: Async partial encoder
}

/// Traits for bytes to bytes codecs.
#[cfg_attr(feature = "async", async_trait::async_trait)]
pub trait BytesToBytesCodecTraits: CodecTraits + core::fmt::Debug {
    /// Return a dynamic version of the codec.
    fn dynamic(self: Arc<Self>) -> Arc<dyn BytesToBytesCodecTraits>;

    /// Return the maximum internal concurrency supported for the requested decoded representation.
    ///
    /// # Errors
    /// Returns [`CodecError`] if the decoded representation is not valid for the codec.
    fn recommended_concurrency(
        &self,
        decoded_representation: &BytesRepresentation,
    ) -> Result<RecommendedConcurrency, CodecError>;

    /// Returns the size of the encoded representation given a size of the decoded representation.
    fn compute_encoded_size(
        &self,
        decoded_representation: &BytesRepresentation,
    ) -> BytesRepresentation;

    /// Encode chunk bytes.
    ///
    /// # Errors
    /// Returns [`CodecError`] if a codec fails.
    fn encode<'a>(
        &self,
        decoded_value: RawBytes<'a>,
        options: &CodecOptions,
    ) -> Result<RawBytes<'a>, CodecError>;

    /// Decode chunk bytes.
    //
    /// # Errors
    /// Returns [`CodecError`] if a codec fails.
    fn decode<'a>(
        &self,
        encoded_value: RawBytes<'a>,
        decoded_representation: &BytesRepresentation,
        options: &CodecOptions,
    ) -> Result<RawBytes<'a>, CodecError>;

    /// Initialises a partial decoder.
    ///
    /// # Errors
    /// Returns a [`CodecError`] if initialisation fails.
    fn partial_decoder(
        self: Arc<Self>,
        input_handle: Arc<dyn BytesPartialDecoderTraits>,
        decoded_representation: &BytesRepresentation,
        options: &CodecOptions,
    ) -> Result<Arc<dyn BytesPartialDecoderTraits>, CodecError>;

    /// Initialise a partial encoder.
    ///
    /// # Errors
    /// Returns a [`CodecError`] if initialisation fails.
    fn partial_encoder(
        self: Arc<Self>,
        input_handle: Arc<dyn BytesPartialDecoderTraits>,
        output_handle: Arc<dyn BytesPartialEncoderTraits>,
        decoded_representation: &BytesRepresentation,
        options: &CodecOptions,
    ) -> Result<Arc<dyn BytesPartialEncoderTraits>, CodecError>;

    #[cfg(feature = "async")]
    /// Initialises an asynchronous partial decoder.
    ///
    /// # Errors
    /// Returns a [`CodecError`] if initialisation fails.
    async fn async_partial_decoder(
        self: Arc<Self>,
        input_handle: Arc<dyn AsyncBytesPartialDecoderTraits>,
        decoded_representation: &BytesRepresentation,
        options: &CodecOptions,
    ) -> Result<Arc<dyn AsyncBytesPartialDecoderTraits>, CodecError>;

    // TODO: Async partial encoder
}

impl BytesPartialDecoderTraits for std::io::Cursor<&'static [u8]> {
    fn partial_decode(
        &self,
        decoded_regions: &[ByteRange],
        _parallel: &CodecOptions,
    ) -> Result<Option<Vec<RawBytes<'_>>>, CodecError> {
        Ok(Some(
            extract_byte_ranges_read_seek(&mut self.clone(), decoded_regions)?
                .into_iter()
                .map(Cow::Owned)
                .collect(),
        ))
    }
}

impl BytesPartialDecoderTraits for std::io::Cursor<RawBytes<'static>> {
    fn partial_decode(
        &self,
        decoded_regions: &[ByteRange],
        _parallel: &CodecOptions,
    ) -> Result<Option<Vec<RawBytes<'_>>>, CodecError> {
        Ok(Some(
            extract_byte_ranges_read_seek(&mut self.clone(), decoded_regions)?
                .into_iter()
                .map(Cow::Owned)
                .collect(),
        ))
    }
}

impl BytesPartialDecoderTraits for std::io::Cursor<Vec<u8>> {
    fn partial_decode(
        &self,
        decoded_regions: &[ByteRange],
        _parallel: &CodecOptions,
    ) -> Result<Option<Vec<RawBytes<'_>>>, CodecError> {
        Ok(Some(
            extract_byte_ranges_read_seek(&mut self.clone(), decoded_regions)?
                .into_iter()
                .map(Cow::Owned)
                .collect(),
        ))
    }
}

#[cfg(feature = "async")]
#[async_trait::async_trait]
impl AsyncBytesPartialDecoderTraits for std::io::Cursor<&'static [u8]> {
    async fn partial_decode(
        &self,
        decoded_regions: &[ByteRange],
        _parallel: &CodecOptions,
    ) -> Result<Option<Vec<RawBytes<'_>>>, CodecError> {
        Ok(Some(
            extract_byte_ranges_read_seek(&mut self.clone(), decoded_regions)?
                .into_iter()
                .map(Cow::Owned)
                .collect(),
        ))
    }
}

#[cfg(feature = "async")]
#[async_trait::async_trait]
impl AsyncBytesPartialDecoderTraits for std::io::Cursor<RawBytes<'static>> {
    async fn partial_decode(
        &self,
        decoded_regions: &[ByteRange],
        _parallel: &CodecOptions,
    ) -> Result<Option<Vec<RawBytes<'_>>>, CodecError> {
        Ok(Some(
            extract_byte_ranges_read_seek(&mut self.clone(), decoded_regions)?
                .into_iter()
                .map(Cow::Owned)
                .collect(),
        ))
    }
}

#[cfg(feature = "async")]
#[async_trait::async_trait]
impl AsyncBytesPartialDecoderTraits for std::io::Cursor<Vec<u8>> {
    async fn partial_decode(
        &self,
        decoded_regions: &[ByteRange],
        _parallel: &CodecOptions,
    ) -> Result<Option<Vec<RawBytes<'_>>>, CodecError> {
        Ok(Some(
            extract_byte_ranges_read_seek(&mut self.clone(), decoded_regions)?
                .into_iter()
                .map(Cow::Owned)
                .collect(),
        ))
    }
}

/// An error indicating the length of bytes does not match the expected length.
#[derive(Debug, Error, Display)]
#[display("Invalid bytes len {len}, expected {expected_len}")]
pub struct InvalidBytesLengthError {
    len: usize,
    expected_len: usize,
}

impl InvalidBytesLengthError {
    /// Create a new [`InvalidBytesLengthError`].
    #[must_use]
    pub fn new(len: usize, expected_len: usize) -> Self {
        Self { len, expected_len }
    }
}

/// An error indicating the shape is not compatible with the expected number of elements.
#[derive(Debug, Error, Display)]
#[display("Invalid shape {shape:?} for number of elements {expected_num_elements}")]
pub struct InvalidArrayShapeError {
    shape: ArrayShape,
    expected_num_elements: usize,
}

impl InvalidArrayShapeError {
    /// Create a new [`InvalidArrayShapeError`].
    #[must_use]
    pub fn new(shape: ArrayShape, expected_num_elements: usize) -> Self {
        Self {
            shape,
            expected_num_elements,
        }
    }
}

/// An error indicating the length of elements does not match the expected length.
#[derive(Debug, Error, Display)]
#[display("Invalid number of elements {num}, expected {expected}")]
pub struct InvalidNumberOfElementsError {
    num: u64,
    expected: u64,
}

impl InvalidNumberOfElementsError {
    /// Create a new [`InvalidNumberOfElementsError`].
    #[must_use]
    pub fn new(num: u64, expected: u64) -> Self {
        Self { num, expected }
    }
}

/// An array subset is out of bounds.
#[derive(Debug, Error, Display)]
#[display("Subset {subset} is out of bounds of {must_be_within}")]
pub struct SubsetOutOfBoundsError {
    subset: ArraySubset,
    must_be_within: ArraySubset,
}

impl SubsetOutOfBoundsError {
    /// Create a new [`InvalidNumberOfElementsError`].
    #[must_use]
    pub fn new(subset: ArraySubset, must_be_within: ArraySubset) -> Self {
        Self {
            subset,
            must_be_within,
        }
    }
}

/// A codec error.
#[derive(Debug, Error)]
pub enum CodecError {
    /// An IO error.
    #[error(transparent)]
    IOError(#[from] std::io::Error),
    /// An invalid byte range was requested.
    #[error(transparent)]
    InvalidByteRangeError(#[from] InvalidByteRangeError),
    /// An invalid array subset was requested.
    #[error(transparent)]
    InvalidArraySubsetError(#[from] IncompatibleArraySubsetAndShapeError),
    /// An invalid array subset was requested with the wrong dimensionality.
    #[error("the array subset {_0} has the wrong dimensionality, expected {_1}")]
    InvalidArraySubsetDimensionalityError(ArraySubset, usize),
    /// The decoded size of a chunk did not match what was expected.
    #[error("the size of a decoded chunk is {}, expected {}", _0.len, _0.expected_len)]
    UnexpectedChunkDecodedSize(#[from] InvalidBytesLengthError),
    /// An embedded checksum does not match the decoded value.
    #[error("the checksum is invalid")]
    InvalidChecksum,
    /// A store error.
    #[error(transparent)]
    StorageError(#[from] StorageError),
    /// Unsupported data type
    #[error("Unsupported data type {0} for codec {1}")]
    UnsupportedDataType(DataType, String),
    /// Offsets are not [`None`] with a fixed length data type.
    #[error("Offsets are invalid or are not compatible with the data type (e.g. fixed-sized data types)")]
    InvalidOffsets,
    /// Other
    #[error("{_0}")]
    Other(String),
    /// Invalid variable sized array offsets.
    #[error("Invalid variable sized array offsets")]
    InvalidVariableSizedArrayOffsets,
    /// Expected fixed length bytes.
    #[error("Expected fixed length array bytes")]
    ExpectedFixedLengthBytes,
    /// Expected variable length bytes.
    #[error("Expected variable length array bytes")]
    ExpectedVariableLengthBytes,
    /// Invalid array shape.
    #[error(transparent)]
    InvalidArrayShape(#[from] InvalidArrayShapeError),
    /// Invalid number of elements.
    #[error(transparent)]
    InvalidNumberOfElements(#[from] InvalidNumberOfElementsError),
    /// Subset out of bounds.
    #[error(transparent)]
    SubsetOutOfBounds(#[from] SubsetOutOfBoundsError),
    /// Invalid byte offsets for variable length data.
    #[error(transparent)]
    RawBytesOffsetsCreate(#[from] RawBytesOffsetsCreateError),
}

impl From<&str> for CodecError {
    fn from(err: &str) -> Self {
        Self::Other(err.to_string())
    }
}

impl From<String> for CodecError {
    fn from(err: String) -> Self {
        Self::Other(err)
    }
}
