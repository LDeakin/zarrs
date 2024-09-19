//! Zarr codecs.
//!
//! Array chunks can be encoded using a sequence of codecs, each of which specifies a bidirectional transform (an encode transform and a decode transform).
//! A codec can map array to an array, an array to bytes, or bytes to bytes.
//! A codec may support partial decoding to extract a byte range or array subset without needing to decode the entire input.
//!
//! A [`CodecChain`] represents a codec sequence consisting of any number of array to array and bytes to bytes codecs, and one array to bytes codec.
//! A codec chain is itself an array to bytes codec.
//! A [`ArrayPartialDecoderCache`] or [`BytesPartialDecoderCache`] may be inserted into a codec chain to optimise partial decoding where appropriate.
//!
//! See <https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html#id18>.

pub mod array_to_array;
pub mod array_to_bytes;
pub mod bytes_to_bytes;
pub mod options;

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
pub use array_partial_decoder_cache::ArrayPartialDecoderCache;
pub use bytes_partial_decoder_cache::BytesPartialDecoderCache;

mod byte_interval_partial_decoder;
pub use byte_interval_partial_decoder::ByteIntervalPartialDecoder;

#[cfg(feature = "async")]
pub use byte_interval_partial_decoder::AsyncByteIntervalPartialDecoder;

use crate::{
    array_subset::{ArraySubset, IncompatibleArraySubsetAndShapeError},
    byte_range::{extract_byte_ranges_read_seek, ByteRange, InvalidByteRangeError},
    metadata::v3::MetadataV3,
    plugin::{Plugin, PluginCreateError},
    storage::{ReadableStorage, StorageError, StoreKey},
};

#[cfg(feature = "async")]
use crate::storage::AsyncReadableStorage;

use std::borrow::Cow;
use std::sync::Arc;

use super::array_bytes::update_bytes_flen;
use super::{
    concurrency::RecommendedConcurrency, ArrayMetadataOptions, BytesRepresentation,
    ChunkRepresentation, ChunkShape, DataType,
};
use super::{ArrayBytes, RawBytes};

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
    fn create_metadata_opt(&self, options: &ArrayMetadataOptions) -> Option<MetadataV3>;

    /// Create metadata with default options.
    ///
    /// A hidden codec (e.g. a cache) will return [`None`], since it will not have any associated metadata.
    fn create_metadata(&self) -> Option<MetadataV3> {
        self.create_metadata_opt(&ArrayMetadataOptions::default())
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
pub trait BytesPartialDecoderTraits: Send + Sync {
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
pub trait AsyncBytesPartialDecoderTraits: Send + Sync {
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
pub trait ArrayPartialDecoderTraits: Send + Sync {
    /// Return the data type of the partial decoder.
    fn data_type(&self) -> &DataType;

    /// Partially decode a chunk with default codec options.
    ///
    /// If the inner `input_handle` is a bytes decoder and partial decoding returns [`None`], then the array subsets have the fill value.
    /// Use [`partial_decode_opt`](ArrayPartialDecoderTraits::partial_decode_opt) to control codec options.
    ///
    /// # Errors
    /// Returns [`CodecError`] if a codec fails or an array subset is invalid.
    fn partial_decode(
        &self,
        array_subsets: &[ArraySubset],
    ) -> Result<Vec<ArrayBytes<'_>>, CodecError> {
        self.partial_decode_opt(array_subsets, &CodecOptions::default())
    }

    /// Explicit options version of [`partial_decode`](ArrayPartialDecoderTraits::partial_decode).
    #[allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]
    fn partial_decode_opt(
        &self,
        array_subsets: &[ArraySubset],
        options: &CodecOptions,
    ) -> Result<Vec<ArrayBytes<'_>>, CodecError>;

    /// Partially decode into a preallocated output.
    ///
    /// This method is intended for internal use by Array.
    /// It currently only works for fixed length data types.
    ///
    /// # Errors
    /// Returns [`CodecError`] if a codec fails or an array subset is invalid.
    ///
    /// # Safety
    /// The caller must ensure that:
    ///  - `output` holds enough space for the preallocated bytes of an array with shape `output_shape` of the appropriate data type,
    ///  - `output_subset` is within the bounds of `output_shape`, and
    ///  - `array_subset` has the same shape as the `output_subset`.
    unsafe fn partial_decode_into(
        &self,
        array_subset: &ArraySubset,
        output: &mut [u8],
        output_shape: &[u64],
        output_subset: &ArraySubset,
        options: &CodecOptions,
    ) -> Result<(), CodecError> {
        debug_assert!(output_subset.inbounds(output_shape));
        debug_assert_eq!(array_subset.shape(), output_subset.shape());
        let decoded_value = self
            .partial_decode_opt(&[array_subset.clone()], options)?
            .remove(0);
        if let ArrayBytes::Fixed(decoded_value) = decoded_value {
            update_bytes_flen(
                output,
                output_shape,
                &decoded_value,
                output_subset,
                self.data_type().fixed_size().unwrap(),
            );
            Ok(())
        } else {
            Err(CodecError::ExpectedFixedLengthBytes)
        }
    }
}

#[cfg(feature = "async")]
/// Asynchronous partial array decoder traits.
#[async_trait::async_trait]
pub trait AsyncArrayPartialDecoderTraits: Send + Sync {
    /// Return the data type of the partial decoder.
    fn data_type(&self) -> &DataType;

    /// Partially decode a chunk with default codec options.
    ///
    /// # Errors
    /// Returns [`CodecError`] if a codec fails, array subset is invalid, or the array subset shape does not match array view subset shape.
    async fn partial_decode(
        &self,
        array_subsets: &[ArraySubset],
    ) -> Result<Vec<ArrayBytes<'_>>, CodecError> {
        self.partial_decode_opt(array_subsets, &CodecOptions::default())
            .await
    }

    /// Explicit options variant of [`partial_decode`](AsyncArrayPartialDecoderTraits::partial_decode).
    async fn partial_decode_opt(
        &self,
        array_subsets: &[ArraySubset],
        options: &CodecOptions,
    ) -> Result<Vec<ArrayBytes<'_>>, CodecError>;

    /// Async variant of [`ArrayPartialDecoderTraits::partial_decode_into`].
    #[allow(clippy::missing_safety_doc)]
    async unsafe fn partial_decode_into(
        &self,
        array_subset: &ArraySubset,
        output: &mut [u8],
        output_shape: &[u64],
        output_subset: &ArraySubset,
        options: &CodecOptions,
    ) -> Result<(), CodecError> {
        debug_assert!(output_subset.inbounds(output_shape));
        debug_assert_eq!(array_subset.shape(), output_subset.shape());
        let decoded_value = self
            .partial_decode_opt(&[array_subset.clone()], options)
            .await?
            .remove(0);
        if let ArrayBytes::Fixed(decoded_value) = decoded_value {
            update_bytes_flen(
                output,
                output_shape,
                &decoded_value,
                output_subset,
                self.data_type().fixed_size().unwrap(),
            );
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

/// Traits for array to array codecs.
#[cfg_attr(feature = "async", async_trait::async_trait)]
pub trait ArrayToArrayCodecTraits: ArrayCodecTraits + core::fmt::Debug {
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
}

/// Traits for array to bytes codecs.
#[cfg_attr(feature = "async", async_trait::async_trait)]
pub trait ArrayToBytesCodecTraits: ArrayCodecTraits + core::fmt::Debug {
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
    /// # Errors
    /// Returns [`CodecError`] if a codec fails or the decoded output is incompatible with `decoded_representation`.
    ///
    /// # Safety
    /// The caller must ensure that:
    ///  - `output` holds enough space for the preallocated bytes of an array with shape `output_shape` of the appropriate data type,
    ///  - `output_subset` is within the bounds of `output_shape`, and
    ///  - `array_subset` has the same shape as the `output_subset`.
    unsafe fn decode_into(
        &self,
        bytes: RawBytes<'_>,
        decoded_representation: &ChunkRepresentation,
        output: &mut [u8],
        output_shape: &[u64],
        output_subset: &ArraySubset,
        options: &CodecOptions,
    ) -> Result<(), CodecError> {
        debug_assert!(output_subset.inbounds(output_shape));
        debug_assert_eq!(decoded_representation.shape_u64(), output_subset.shape());
        let decoded_value = self.decode(bytes, decoded_representation, options)?;
        if let ArrayBytes::Fixed(decoded_value) = decoded_value {
            update_bytes_flen(
                output,
                output_shape,
                &decoded_value,
                output_subset,
                decoded_representation.data_type().fixed_size().unwrap(),
            );
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
}

/// Traits for bytes to bytes codecs.
#[cfg_attr(feature = "async", async_trait::async_trait)]
pub trait BytesToBytesCodecTraits: CodecTraits + core::fmt::Debug {
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
}

impl BytesPartialDecoderTraits for std::io::Cursor<&[u8]> {
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

impl BytesPartialDecoderTraits for std::io::Cursor<RawBytes<'_>> {
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
impl AsyncBytesPartialDecoderTraits for std::io::Cursor<&[u8]> {
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
impl AsyncBytesPartialDecoderTraits for std::io::Cursor<RawBytes<'_>> {
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
    #[error("the size of a decoded chunk is {_0}, expected {_1}")]
    UnexpectedChunkDecodedSize(usize, u64),
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
