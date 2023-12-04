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
#[cfg(feature = "sharding")]
pub use array_to_bytes::sharding::{
    ShardingCodec, ShardingCodecConfiguration, ShardingCodecConfigurationV1,
};
#[cfg(feature = "zfp")]
pub use array_to_bytes::zfp::{ZfpCodec, ZfpCodecConfiguration, ZfpCodecConfigurationV1};
pub use array_to_bytes::{
    bytes::{BytesCodec, BytesCodecConfiguration, BytesCodecConfigurationV1},
    codec_chain::CodecChain,
};

// Bytes to bytes
#[cfg(feature = "blosc")]
pub use bytes_to_bytes::blosc::{BloscCodec, BloscCodecConfiguration, BloscCodecConfigurationV1};
#[cfg(feature = "crc32c")]
pub use bytes_to_bytes::crc32c::{
    Crc32cCodec, Crc32cCodecConfiguration, Crc32cCodecConfigurationV1,
};
#[cfg(feature = "gzip")]
pub use bytes_to_bytes::gzip::{GzipCodec, GzipCodecConfiguration, GzipCodecConfigurationV1};
#[cfg(feature = "zstd")]
pub use bytes_to_bytes::zstd::{ZstdCodec, ZstdCodecConfiguration, ZstdCodecConfigurationV1};

use itertools::Itertools;
use thiserror::Error;

mod partial_decoder_cache;
pub use partial_decoder_cache::{ArrayPartialDecoderCache, BytesPartialDecoderCache};

mod byte_interval_partial_decoder;
pub use byte_interval_partial_decoder::ByteIntervalPartialDecoder;

#[cfg(feature = "async")]
pub use byte_interval_partial_decoder::AsyncByteIntervalPartialDecoder;

use crate::{
    array_subset::{ArraySubset, InvalidArraySubsetError},
    byte_range::{ByteOffset, ByteRange, InvalidByteRangeError},
    metadata::Metadata,
    plugin::{Plugin, PluginCreateError},
    storage::{ReadableStorage, StorageError, StoreKey},
};

#[cfg(feature = "async")]
use crate::storage::AsyncReadableStorage;

use std::{
    collections::{BTreeMap, BTreeSet},
    io::{Read, Seek, SeekFrom},
};

use super::{ArrayRepresentation, BytesRepresentation, DataType, MaybeBytes};

/// A codec plugin.
pub type CodecPlugin = Plugin<Codec>;
inventory::collect!(CodecPlugin);

/// A generic array to array, array to bytes, or bytes to bytes codec.
#[derive(Debug)]
pub enum Codec {
    /// An array to array codec.
    ArrayToArray(Box<dyn ArrayToArrayCodecTraits>),
    /// An array to bytes codec.
    ArrayToBytes(Box<dyn ArrayToBytesCodecTraits>),
    /// A bytes to bytes codec.
    BytesToBytes(Box<dyn BytesToBytesCodecTraits>),
}

impl Codec {
    /// Create a codec from metadata.
    ///
    /// # Errors
    ///
    /// Returns [`PluginCreateError`] if the metadata is invalid or not associated with a registered codec plugin.
    pub fn from_metadata(metadata: &Metadata) -> Result<Self, PluginCreateError> {
        for plugin in inventory::iter::<CodecPlugin> {
            if plugin.match_name(metadata.name()) {
                return plugin.create(metadata);
            }
        }
        Err(PluginCreateError::Unsupported {
            name: metadata.name().to_string(),
        })
    }
}

/// Codec traits.
pub trait CodecTraits: Send + Sync {
    /// Create metadata.
    ///
    /// A hidden codec (e.g. a cache) will return [`None`], since it will not have any associated metadata.
    fn create_metadata(&self) -> Option<Metadata>;

    /// Indicates if the input to a codecs partial decoder should be cached for optimal performance.
    /// If true, a cache may be inserted *before* it in a [`CodecChain`] partial decoder.
    fn partial_decoder_should_cache_input(&self) -> bool;

    /// Indicates if a partial decoder decodes all bytes from its input handle and its output should be cached for optimal performance.
    /// If true, a cache will be inserted at some point *after* it in a [`CodecChain`] partial decoder.
    fn partial_decoder_decodes_all(&self) -> bool;
}

/// Traits for both array to array and array to bytes codecs.
#[cfg_attr(feature = "async", async_trait::async_trait)]
pub trait ArrayCodecTraits: CodecTraits {
    /// Encode array with optional parallelism.
    ///
    /// # Errors
    /// Returns [`CodecError`] if a codec fails or `decoded_value` is incompatible with `decoded_representation`.
    fn encode_opt(
        &self,
        decoded_value: Vec<u8>,
        decoded_representation: &ArrayRepresentation,
        parallel: bool,
    ) -> Result<Vec<u8>, CodecError>;

    #[cfg(feature = "async")]
    /// Asynchronously encode array with optional parallelism.
    ///
    /// The default implementation calls [`encode_opt`](ArrayCodecTraits::encode_opt) with parallelism disabled.
    ///
    /// # Errors
    /// Returns [`CodecError`] if a codec fails or the decoded output is incompatible with `decoded_representation`.
    async fn async_encode_opt(
        &self,
        decoded_value: Vec<u8>,
        decoded_representation: &ArrayRepresentation,
        _parallel: bool,
    ) -> Result<Vec<u8>, CodecError> {
        self.encode_opt(decoded_value, decoded_representation, false)
    }

    /// Decode array with optional parallelism.
    ///
    /// # Errors
    /// Returns [`CodecError`] if a codec fails or the decoded output is incompatible with `decoded_representation`.
    fn decode_opt(
        &self,
        encoded_value: Vec<u8>,
        decoded_representation: &ArrayRepresentation,
        parallel: bool,
    ) -> Result<Vec<u8>, CodecError>;

    #[cfg(feature = "async")]
    /// Asynchronously decode array with optional parallelism.
    ///
    /// The default implementation calls [`decode_opt`](ArrayCodecTraits::decode_opt) with parallelism disabled.
    ///
    /// # Errors
    /// Returns [`CodecError`] if a codec fails or the decoded output is incompatible with `decoded_representation`.
    async fn async_decode_opt(
        &self,
        encoded_value: Vec<u8>,
        decoded_representation: &ArrayRepresentation,
        _parallel: bool,
    ) -> Result<Vec<u8>, CodecError> {
        self.decode_opt(encoded_value, decoded_representation, false)
    }

    /// Encode array.
    ///
    /// # Errors
    /// Returns [`CodecError`] if a codec fails or `decoded_value` is incompatible with `decoded_representation`.
    fn encode(
        &self,
        decoded_value: Vec<u8>,
        decoded_representation: &ArrayRepresentation,
    ) -> Result<Vec<u8>, CodecError> {
        self.encode_opt(decoded_value, decoded_representation, false)
    }

    /// Encode array using multithreading (if supported).
    ///
    /// # Errors
    /// Returns [`CodecError`] if a codec fails or `decoded_value` is incompatible with `decoded_representation`.
    fn par_encode(
        &self,
        decoded_value: Vec<u8>,
        decoded_representation: &ArrayRepresentation,
    ) -> Result<Vec<u8>, CodecError> {
        self.encode_opt(decoded_value, decoded_representation, true)
    }

    /// Decode array.
    ///
    /// # Errors
    /// Returns [`CodecError`] if a codec fails or the decoded output is incompatible with `decoded_representation`.
    fn decode(
        &self,
        encoded_value: Vec<u8>,
        decoded_representation: &ArrayRepresentation,
    ) -> Result<Vec<u8>, CodecError> {
        self.decode_opt(encoded_value, decoded_representation, false)
    }

    /// Decode array using multithreading (if supported).
    ///
    /// # Errors
    /// Returns [`CodecError`] if a codec fails or the decoded output is incompatible with `decoded_representation`.
    fn par_decode(
        &self,
        encoded_value: Vec<u8>,
        decoded_representation: &ArrayRepresentation,
    ) -> Result<Vec<u8>, CodecError> {
        self.decode_opt(encoded_value, decoded_representation, true)
    }
}

/// Partial bytes decoder traits.
pub trait BytesPartialDecoderTraits: Send + Sync {
    /// Partially decode bytes with optional parallelism.
    ///
    /// Returns [`None`] if partial decoding of the input handle returns [`None`].
    ///
    /// # Errors
    /// Returns [`CodecError`] if a codec fails or a byte range is invalid.
    fn partial_decode_opt(
        &self,
        decoded_regions: &[ByteRange],
        parallel: bool,
    ) -> Result<Option<Vec<Vec<u8>>>, CodecError>;

    /// Decode all bytes with optional parallelism.
    ///
    /// Returns [`None`] if partial decoding of the input handle returns [`None`].
    ///
    /// # Errors
    /// Returns [`CodecError`] if a codec fails.
    fn decode_opt(&self, parallel: bool) -> Result<MaybeBytes, CodecError> {
        Ok(self
            .partial_decode_opt(&[ByteRange::FromStart(0, None)], parallel)?
            .map(|mut v| v.remove(0)))
    }

    /// Partially decode bytes.
    ///
    /// Returns [`None`] if partial decoding of the input handle returns [`None`].
    ///
    /// # Errors
    /// Returns [`CodecError`] if a codec fails or a byte range is invalid.
    fn partial_decode(
        &self,
        byte_ranges: &[ByteRange],
    ) -> Result<Option<Vec<Vec<u8>>>, CodecError> {
        self.partial_decode_opt(byte_ranges, false)
    }

    /// Partially decode bytes using multithreading if applicable.
    ///
    /// Returns [`None`] if partial decoding of the input handle returns [`None`].
    ///
    /// # Errors
    /// Returns [`CodecError`] if a codec fails or a byte range is invalid.
    fn par_partial_decode(
        &self,
        byte_ranges: &[ByteRange],
    ) -> Result<Option<Vec<Vec<u8>>>, CodecError> {
        self.partial_decode_opt(byte_ranges, true)
    }

    /// Decode all bytes.
    ///
    /// Returns [`None`] if decoding of the input handle returns [`None`].
    ///
    /// # Errors
    /// Returns [`CodecError`] if a codec fails.
    fn decode(&self) -> Result<MaybeBytes, CodecError> {
        self.decode_opt(false)
    }

    /// Decode all bytes using multithreading (if supported).
    ///
    /// Returns [`None`] if decoding of the input handle returns [`None`].
    ///
    /// # Errors
    /// Returns [`CodecError`] if a codec fails.
    fn par_decode(&self) -> Result<MaybeBytes, CodecError> {
        self.decode_opt(true)
    }
}

#[cfg(feature = "async")]
/// Asynchronous partial bytes decoder traits.
#[cfg_attr(feature = "async", async_trait::async_trait)]
pub trait AsyncBytesPartialDecoderTraits: Send + Sync {
    /// Partially decode bytes with optional parallelism.
    ///
    /// Returns [`None`] if partial decoding of the input handle returns [`None`].
    ///
    /// # Errors
    /// Returns [`CodecError`] if a codec fails or a byte range is invalid.
    async fn partial_decode_opt(
        &self,
        decoded_regions: &[ByteRange],
        parallel: bool,
    ) -> Result<Option<Vec<Vec<u8>>>, CodecError>;

    /// Decode all bytes with optional parallelism.
    ///
    /// Returns [`None`] if partial decoding of the input handle returns [`None`].
    ///
    /// # Errors
    /// Returns [`CodecError`] if a codec fails.
    async fn decode_opt(&self, parallel: bool) -> Result<MaybeBytes, CodecError> {
        Ok(self
            .partial_decode_opt(&[ByteRange::FromStart(0, None)], parallel)
            .await?
            .map(|mut v| v.remove(0)))
    }

    /// Partially decode bytes.
    ///
    /// Returns [`None`] if partial decoding of the input handle returns [`None`].
    ///
    /// # Errors
    /// Returns [`CodecError`] if a codec fails or a byte range is invalid.
    async fn partial_decode(
        &self,
        byte_ranges: &[ByteRange],
    ) -> Result<Option<Vec<Vec<u8>>>, CodecError> {
        self.partial_decode_opt(byte_ranges, false).await
    }

    /// Partially decode bytes using multithreading if applicable.
    ///
    /// Returns [`None`] if partial decoding of the input handle returns [`None`].
    ///
    /// # Errors
    /// Returns [`CodecError`] if a codec fails or a byte range is invalid.
    async fn par_partial_decode(
        &self,
        byte_ranges: &[ByteRange],
    ) -> Result<Option<Vec<Vec<u8>>>, CodecError> {
        self.partial_decode_opt(byte_ranges, true).await
    }

    /// Decode all bytes.
    ///
    /// Returns [`None`] if decoding of the input handle returns [`None`].
    ///
    /// # Errors
    /// Returns [`CodecError`] if a codec fails.
    async fn decode(&self) -> Result<MaybeBytes, CodecError> {
        self.decode_opt(false).await
    }

    /// Decode all bytes using multithreading (if supported).
    ///
    /// Returns [`None`] if decoding of the input handle returns [`None`].
    ///
    /// # Errors
    /// Returns [`CodecError`] if a codec fails.
    async fn par_decode(&self) -> Result<MaybeBytes, CodecError> {
        self.decode_opt(true).await
    }
}

/// Partial array decoder traits.
pub trait ArrayPartialDecoderTraits: Send + Sync {
    /// Partially decode an array.
    ///
    /// If the inner `input_handle` is a bytes decoder and partial decoding returns [`None`], then the array subsets have the fill value.
    ///
    /// # Errors
    /// Returns [`CodecError`] if a codec fails or an array subset is invalid.
    fn partial_decode_opt(
        &self,
        array_subsets: &[ArraySubset],
        parallel: bool,
    ) -> Result<Vec<Vec<u8>>, CodecError>;

    /// Partially decode an array.
    ///
    /// If the inner `input_handle` is a bytes decoder and partial decoding returns [`None`], then the array subsets have the fill value.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError`] if a codec fails or an array subset is invalid.
    fn partial_decode(&self, array_subsets: &[ArraySubset]) -> Result<Vec<Vec<u8>>, CodecError> {
        self.partial_decode_opt(array_subsets, false)
    }

    /// Partially decode an array using multithreading (if supported).
    ///
    /// If the inner `input_handle` is a bytes decoder and partial decoding returns [`None`], then the array subsets have the fill value.
    ///
    /// # Errors
    /// Returns [`CodecError`] if a codec fails or an array subset is invalid.
    fn par_partial_decode(
        &self,
        array_subsets: &[ArraySubset],
    ) -> Result<Vec<Vec<u8>>, CodecError> {
        self.partial_decode_opt(array_subsets, true)
    }
}

#[cfg(feature = "async")]
/// Asynchronous partial array decoder traits.
#[cfg_attr(feature = "async", async_trait::async_trait)]
pub trait AsyncArrayPartialDecoderTraits: Send + Sync {
    /// Partially decode an array.
    ///
    /// If the inner `input_handle` is a bytes decoder and partial decoding returns [`None`], then the array subsets have the fill value.
    ///
    /// # Errors
    /// Returns [`CodecError`] if a codec fails or an array subset is invalid.
    async fn partial_decode_opt(
        &self,
        array_subsets: &[ArraySubset],
        parallel: bool,
    ) -> Result<Vec<Vec<u8>>, CodecError>;

    /// Partially decode an array.
    ///
    /// If the inner `input_handle` is a bytes decoder and partial decoding returns [`None`], then the array subsets have the fill value.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError`] if a codec fails or an array subset is invalid.
    async fn partial_decode(
        &self,
        array_subsets: &[ArraySubset],
    ) -> Result<Vec<Vec<u8>>, CodecError> {
        self.partial_decode_opt(array_subsets, false).await
    }

    /// Partially decode an array using multithreading (if supported).
    ///
    /// If the inner `input_handle` is a bytes decoder and partial decoding returns [`None`], then the array subsets have the fill value.
    ///
    /// # Errors
    /// Returns [`CodecError`] if a codec fails or an array subset is invalid.
    async fn par_partial_decode(
        &self,
        array_subsets: &[ArraySubset],
    ) -> Result<Vec<Vec<u8>>, CodecError> {
        self.partial_decode_opt(array_subsets, true).await
    }
}

/// A [`ReadableStorage`] partial decoder.
pub struct StoragePartialDecoder<'a> {
    storage: ReadableStorage<'a>,
    key: StoreKey,
}

impl<'a> StoragePartialDecoder<'a> {
    /// Create a new storage partial decoder.
    pub fn new(storage: ReadableStorage<'a>, key: StoreKey) -> Self {
        Self { storage, key }
    }
}

impl BytesPartialDecoderTraits for StoragePartialDecoder<'_> {
    fn partial_decode_opt(
        &self,
        decoded_regions: &[ByteRange],
        _parallel: bool,
    ) -> Result<Option<Vec<Vec<u8>>>, CodecError> {
        Ok(self
            .storage
            .get_partial_values_key(&self.key, decoded_regions)?)
    }
}

#[cfg(feature = "async")]
/// A [`ReadableStorage`] partial decoder.
pub struct AsyncStoragePartialDecoder<'a> {
    storage: AsyncReadableStorage<'a>,
    key: StoreKey,
}

#[cfg(feature = "async")]
impl<'a> AsyncStoragePartialDecoder<'a> {
    /// Create a new storage partial decoder.
    pub fn new(storage: AsyncReadableStorage<'a>, key: StoreKey) -> Self {
        Self { storage, key }
    }
}

#[cfg(feature = "async")]
#[cfg_attr(feature = "async", async_trait::async_trait)]
impl AsyncBytesPartialDecoderTraits for AsyncStoragePartialDecoder<'_> {
    async fn partial_decode_opt(
        &self,
        decoded_regions: &[ByteRange],
        _parallel: bool,
    ) -> Result<Option<Vec<Vec<u8>>>, CodecError> {
        Ok(self
            .storage
            .get_partial_values_key(&self.key, decoded_regions)
            .await?)
    }
}

/// Traits for array to array codecs.
#[cfg_attr(feature = "async", async_trait::async_trait)]
pub trait ArrayToArrayCodecTraits:
    ArrayCodecTraits + dyn_clone::DynClone + core::fmt::Debug
{
    /// Initialise a partial decoder with optional parallelism.
    ///
    /// `parallel` only affects parallelism on initialisation, which is irrelevant for most codecs.
    /// Parallel partial decoding support is independent of how the partial decoder is initialised.
    ///
    /// # Errors
    /// Returns a [`CodecError`] if initialisation fails.
    fn partial_decoder_opt<'a>(
        &'a self,
        input_handle: Box<dyn ArrayPartialDecoderTraits + 'a>,
        decoded_representation: &ArrayRepresentation,
        parallel: bool,
    ) -> Result<Box<dyn ArrayPartialDecoderTraits + 'a>, CodecError>;

    #[cfg(feature = "async")]
    /// Initialise an asynchronous partial decoder with optional parallelism.
    ///
    /// `parallel` only affects parallelism on initialisation, which is irrelevant for most codecs.
    /// Parallel partial decoding support is independent of how the partial decoder is initialised.
    ///
    /// # Errors
    /// Returns a [`CodecError`] if initialisation fails.
    async fn async_partial_decoder_opt<'a>(
        &'a self,
        input_handle: Box<dyn AsyncArrayPartialDecoderTraits + 'a>,
        decoded_representation: &ArrayRepresentation,
        parallel: bool,
    ) -> Result<Box<dyn AsyncArrayPartialDecoderTraits + 'a>, CodecError>;

    /// Returns the size of the encoded representation given a size of the decoded representation.
    ///
    /// # Errors
    ///
    /// Returns a [`CodecError`] if the decoded representation is not supported by this codec.
    fn compute_encoded_size(
        &self,
        decoded_representation: &ArrayRepresentation,
    ) -> Result<ArrayRepresentation, CodecError>;

    /// Initialise a partial decoder.
    ///
    /// # Errors
    /// Returns a [`CodecError`] if initialisation fails.
    fn partial_decoder<'a>(
        &'a self,
        input_handle: Box<dyn ArrayPartialDecoderTraits + 'a>,
        decoded_representation: &ArrayRepresentation,
    ) -> Result<Box<dyn ArrayPartialDecoderTraits + 'a>, CodecError> {
        self.partial_decoder_opt(input_handle, decoded_representation, false)
    }

    /// Initialise a partial decoder with multithreading (where supported).
    ///
    /// # Errors
    /// Returns a [`CodecError`] if initialisation fails.
    fn par_partial_decoder<'a>(
        &'a self,
        input_handle: Box<dyn ArrayPartialDecoderTraits + 'a>,
        decoded_representation: &ArrayRepresentation,
    ) -> Result<Box<dyn ArrayPartialDecoderTraits + 'a>, CodecError> {
        self.partial_decoder_opt(input_handle, decoded_representation, true)
    }

    #[cfg(feature = "async")]
    /// Initialise an asynchronous partial decoder.
    ///
    /// # Errors
    /// Returns a [`CodecError`] if initialisation fails.
    async fn async_partial_decoder<'a>(
        &'a self,
        input_handle: Box<dyn AsyncArrayPartialDecoderTraits + 'a>,
        decoded_representation: &ArrayRepresentation,
    ) -> Result<Box<dyn AsyncArrayPartialDecoderTraits + 'a>, CodecError> {
        self.async_partial_decoder_opt(input_handle, decoded_representation, false)
            .await
    }

    #[cfg(feature = "async")]
    /// Initialise an asynchronous partial decoder with multithreading (where supported).
    ///
    /// # Errors
    /// Returns a [`CodecError`] if initialisation fails.
    async fn async_par_partial_decoder<'a>(
        &'a self,
        input_handle: Box<dyn AsyncArrayPartialDecoderTraits + 'a>,
        decoded_representation: &ArrayRepresentation,
    ) -> Result<Box<dyn AsyncArrayPartialDecoderTraits + 'a>, CodecError> {
        self.async_partial_decoder_opt(input_handle, decoded_representation, true)
            .await
    }
}

dyn_clone::clone_trait_object!(ArrayToArrayCodecTraits);

/// Traits for array to bytes codecs.
#[cfg_attr(feature = "async", async_trait::async_trait)]
pub trait ArrayToBytesCodecTraits:
    ArrayCodecTraits + dyn_clone::DynClone + core::fmt::Debug
{
    /// Initialise a partial decoder with optional parallelism.
    ///
    /// # Errors
    /// Returns a [`CodecError`] if initialisation fails.
    fn partial_decoder_opt<'a>(
        &'a self,
        input_handle: Box<dyn BytesPartialDecoderTraits + 'a>,
        decoded_representation: &ArrayRepresentation,
        parallel: bool,
    ) -> Result<Box<dyn ArrayPartialDecoderTraits + 'a>, CodecError>;

    #[cfg(feature = "async")]
    /// Initialise an asynchronous partial decoder with optional parallelism.
    ///
    /// # Errors
    /// Returns a [`CodecError`] if initialisation fails.
    async fn async_partial_decoder_opt<'a>(
        &'a self,
        mut input_handle: Box<dyn AsyncBytesPartialDecoderTraits + 'a>,
        decoded_representation: &ArrayRepresentation,
        parallel: bool,
    ) -> Result<Box<dyn AsyncArrayPartialDecoderTraits + 'a>, CodecError>;

    /// Returns the size of the encoded representation given a size of the decoded representation.
    ///
    /// # Errors
    ///
    /// Returns a [`CodecError`] if the decoded representation is not supported by this codec.
    fn compute_encoded_size(
        &self,
        decoded_representation: &ArrayRepresentation,
    ) -> Result<BytesRepresentation, CodecError>;

    /// Initialise a partial decoder.
    ///
    /// # Errors
    /// Returns a [`CodecError`] if initialisation fails.
    fn partial_decoder<'a>(
        &'a self,
        input_handle: Box<dyn BytesPartialDecoderTraits + 'a>,
        decoded_representation: &ArrayRepresentation,
    ) -> Result<Box<dyn ArrayPartialDecoderTraits + 'a>, CodecError> {
        self.partial_decoder_opt(input_handle, decoded_representation, false)
    }

    /// Initialise a partial decoder with multithreading (where supported).
    ///
    /// # Errors
    /// Returns a [`CodecError`] if initialisation fails.
    fn par_partial_decoder<'a>(
        &'a self,
        input_handle: Box<dyn BytesPartialDecoderTraits + 'a>,
        decoded_representation: &ArrayRepresentation,
    ) -> Result<Box<dyn ArrayPartialDecoderTraits + 'a>, CodecError> {
        self.partial_decoder_opt(input_handle, decoded_representation, true)
    }

    #[cfg(feature = "async")]
    /// Initialise an asynchronous partial decoder.
    ///
    /// # Errors
    /// Returns a [`CodecError`] if initialisation fails.
    async fn async_partial_decoder<'a>(
        &'a self,
        input_handle: Box<dyn AsyncBytesPartialDecoderTraits + 'a>,
        decoded_representation: &ArrayRepresentation,
    ) -> Result<Box<dyn AsyncArrayPartialDecoderTraits + 'a>, CodecError> {
        self.async_partial_decoder_opt(input_handle, decoded_representation, false)
            .await
    }

    #[cfg(feature = "async")]
    /// Initialise an asynchronous partial decoder with multithreading (where supported).
    ///
    /// # Errors
    /// Returns a [`CodecError`] if initialisation fails.
    async fn async_par_partial_decoder<'a>(
        &'a self,
        input_handle: Box<dyn AsyncBytesPartialDecoderTraits + 'a>,
        decoded_representation: &ArrayRepresentation,
    ) -> Result<Box<dyn AsyncArrayPartialDecoderTraits + 'a>, CodecError> {
        self.async_partial_decoder_opt(input_handle, decoded_representation, true)
            .await
    }
}

dyn_clone::clone_trait_object!(ArrayToBytesCodecTraits);

/// Traits for bytes to bytes codecs.
#[cfg_attr(feature = "async", async_trait::async_trait)]
pub trait BytesToBytesCodecTraits: CodecTraits + dyn_clone::DynClone + core::fmt::Debug {
    /// Encode bytes with optional parallelism.
    ///
    /// # Errors
    /// Returns [`CodecError`] if a codec fails.
    fn encode_opt(&self, decoded_value: Vec<u8>, parallel: bool) -> Result<Vec<u8>, CodecError>;

    #[cfg(feature = "async")]
    /// Asynchronously encode bytes with optional parallelism.
    ///
    /// The default implementation calls [`encode_opt`](BytesToBytesCodecTraits::encode_opt) with parallelism disabled.
    ///
    /// # Errors
    /// Returns [`CodecError`] if a codec fails.
    async fn async_encode_opt(
        &self,
        decoded_value: Vec<u8>,
        _parallel: bool,
    ) -> Result<Vec<u8>, CodecError> {
        self.encode_opt(decoded_value, false)
    }

    /// Decode bytes with optional parallelism.
    //
    /// # Errors
    /// Returns [`CodecError`] if a codec fails.
    fn decode_opt(
        &self,
        encoded_value: Vec<u8>,
        decoded_representation: &BytesRepresentation,
        parallel: bool,
    ) -> Result<Vec<u8>, CodecError>;

    #[cfg(feature = "async")]
    /// Asynchronously decode bytes with optional parallelism.
    ///
    /// The default implementation calls [`decode_opt`](BytesToBytesCodecTraits::decode_opt) with parallelism disabled.
    ///
    /// # Errors
    /// Returns [`CodecError`] if a codec fails.
    async fn async_decode_opt(
        &self,
        encoded_value: Vec<u8>,
        decoded_representation: &BytesRepresentation,
        _parallel: bool,
    ) -> Result<Vec<u8>, CodecError> {
        self.decode_opt(encoded_value, decoded_representation, false)
    }

    /// Initialises a partial decoder with optional parallelism.
    ///
    /// # Errors
    /// Returns a [`CodecError`] if initialisation fails.
    fn partial_decoder_opt<'a>(
        &'a self,
        input_handle: Box<dyn BytesPartialDecoderTraits + 'a>,
        decoded_representation: &BytesRepresentation,
        parallel: bool,
    ) -> Result<Box<dyn BytesPartialDecoderTraits + 'a>, CodecError>;

    #[cfg(feature = "async")]
    /// Initialises an asynchronous partial decoder with optional parallelism.
    ///
    /// # Errors
    /// Returns a [`CodecError`] if initialisation fails.
    async fn async_partial_decoder_opt<'a>(
        &'a self,
        input_handle: Box<dyn AsyncBytesPartialDecoderTraits + 'a>,
        decoded_representation: &BytesRepresentation,
        parallel: bool,
    ) -> Result<Box<dyn AsyncBytesPartialDecoderTraits + 'a>, CodecError>;

    /// Returns the size of the encoded representation given a size of the decoded representation.
    fn compute_encoded_size(
        &self,
        decoded_representation: &BytesRepresentation,
    ) -> BytesRepresentation;

    /// Encode bytes.
    ///
    /// # Errors
    /// Returns [`CodecError`] if a codec fails.
    fn encode(&self, decoded_value: Vec<u8>) -> Result<Vec<u8>, CodecError> {
        self.encode_opt(decoded_value, false)
    }

    /// Encode bytes using using multithreading (if supported).
    ///
    /// # Errors
    /// Returns [`CodecError`] if a codec fails.
    fn par_encode(&self, decoded_value: Vec<u8>) -> Result<Vec<u8>, CodecError> {
        self.encode_opt(decoded_value, true)
    }

    /// Decode bytes.
    ///
    /// # Errors
    /// Returns [`CodecError`] if a codec fails.
    fn decode(
        &self,
        encoded_value: Vec<u8>,
        decoded_representation: &BytesRepresentation,
    ) -> Result<Vec<u8>, CodecError> {
        self.decode_opt(encoded_value, decoded_representation, false)
    }

    /// Decode bytes using using multithreading (if supported).
    ///
    /// # Errors
    /// Returns [`CodecError`] if a codec fails.
    fn par_decode(
        &self,
        encoded_value: Vec<u8>,
        decoded_representation: &BytesRepresentation,
    ) -> Result<Vec<u8>, CodecError> {
        self.decode_opt(encoded_value, decoded_representation, true)
    }

    /// Initialises a partial decoder.
    ///
    /// # Errors
    /// Returns a [`CodecError`] if initialisation fails.
    fn partial_decoder<'a>(
        &'a self,
        input_handle: Box<dyn BytesPartialDecoderTraits + 'a>,
        decoded_representation: &BytesRepresentation,
    ) -> Result<Box<dyn BytesPartialDecoderTraits + 'a>, CodecError> {
        self.partial_decoder_opt(input_handle, decoded_representation, false)
    }

    /// Initialise a partial decoder with multithreading (where supported).
    ///
    /// # Errors
    /// Returns a [`CodecError`] if initialisation fails.
    fn par_partial_decoder<'a>(
        &'a self,
        input_handle: Box<dyn BytesPartialDecoderTraits + 'a>,
        decoded_representation: &BytesRepresentation,
    ) -> Result<Box<dyn BytesPartialDecoderTraits + 'a>, CodecError> {
        self.partial_decoder_opt(input_handle, decoded_representation, true)
    }

    #[cfg(feature = "async")]
    /// Initialises an asynchronous partial decoder.
    ///
    /// # Errors
    /// Returns a [`CodecError`] if initialisation fails.
    async fn async_partial_decoder<'a>(
        &'a self,
        input_handle: Box<dyn AsyncBytesPartialDecoderTraits + 'a>,
        decoded_representation: &BytesRepresentation,
    ) -> Result<Box<dyn AsyncBytesPartialDecoderTraits + 'a>, CodecError> {
        self.async_partial_decoder_opt(input_handle, decoded_representation, false)
            .await
    }

    #[cfg(feature = "async")]
    /// Initialise an asynchronous partial decoder with multithreading (where supported).
    ///
    /// # Errors
    /// Returns a [`CodecError`] if initialisation fails.
    async fn async_par_partial_decoder<'a>(
        &'a self,
        input_handle: Box<dyn AsyncBytesPartialDecoderTraits + 'a>,
        decoded_representation: &BytesRepresentation,
    ) -> Result<Box<dyn AsyncBytesPartialDecoderTraits + 'a>, CodecError> {
        self.async_partial_decoder_opt(input_handle, decoded_representation, true)
            .await
    }
}

dyn_clone::clone_trait_object!(BytesToBytesCodecTraits);

impl BytesPartialDecoderTraits for std::io::Cursor<&[u8]> {
    fn partial_decode_opt(
        &self,
        decoded_regions: &[ByteRange],
        _parallel: bool,
    ) -> Result<Option<Vec<Vec<u8>>>, CodecError> {
        Ok(Some(extract_byte_ranges_read_seek(
            &mut self.clone(),
            decoded_regions,
        )?))
    }
}

impl BytesPartialDecoderTraits for std::io::Cursor<Vec<u8>> {
    fn partial_decode_opt(
        &self,
        decoded_regions: &[ByteRange],
        _parallel: bool,
    ) -> Result<Option<Vec<Vec<u8>>>, CodecError> {
        Ok(Some(extract_byte_ranges_read_seek(
            &mut self.clone(),
            decoded_regions,
        )?))
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
    InvalidArraySubsetError(#[from] InvalidArraySubsetError),
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
    /// Other
    #[error("{_0}")]
    Other(String),
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

/// Extract byte ranges from bytes implementing [`Read`] and [`Seek`].
///
/// # Errors
///
/// Returns a [`std::io::Error`] if there is an error reading or seeking from `bytes`.
/// This can occur if the byte range is out-of-bounds of the `bytes`.
///
/// # Panics
///
/// Panics if a byte has length exceeding [`usize::MAX`].
pub fn extract_byte_ranges_read_seek<T: Read + Seek>(
    bytes: &mut T,
    byte_ranges: &[ByteRange],
) -> std::io::Result<Vec<Vec<u8>>> {
    let len: u64 = bytes.seek(SeekFrom::End(0))?;
    let mut out = Vec::with_capacity(byte_ranges.len());
    for byte_range in byte_ranges {
        let data: Vec<u8> = match byte_range {
            ByteRange::FromStart(offset, None) => {
                bytes.seek(SeekFrom::Start(*offset))?;
                let length = usize::try_from(len).unwrap();
                let mut data = vec![0; length];
                bytes.read_exact(&mut data)?;
                data
            }
            ByteRange::FromStart(offset, Some(length)) => {
                bytes.seek(SeekFrom::Start(*offset))?;
                let length = usize::try_from(*length).unwrap();
                let mut data = vec![0; length];
                bytes.read_exact(&mut data)?;
                data
            }
            ByteRange::FromEnd(offset, None) => {
                bytes.seek(SeekFrom::Start(0))?;
                let length = usize::try_from(len - offset).unwrap();
                let mut data = vec![0; length];
                bytes.read_exact(&mut data)?;
                data
            }
            ByteRange::FromEnd(offset, Some(length)) => {
                bytes.seek(SeekFrom::End(-i64::try_from(*offset + *length).unwrap()))?;
                let length = usize::try_from(*length).unwrap();
                let mut data = vec![0; length];
                bytes.read_exact(&mut data)?;
                data
            }
        };
        out.push(data);
    }
    Ok(out)
}

/// Extract byte ranges from bytes implementing [`Read`].
///
/// # Errors
///
/// Returns a [`std::io::Error`] if there is an error reading from `bytes`.
/// This can occur if the byte range is out-of-bounds of the `bytes`.
///
/// # Panics
///
/// Panics if a byte has length exceeding [`usize::MAX`].
pub fn extract_byte_ranges_read<T: Read>(
    bytes: &mut T,
    size: u64,
    byte_ranges: &[ByteRange],
) -> std::io::Result<Vec<Vec<u8>>> {
    // Could this be cleaner/more efficient?

    // Allocate output and find the endpoints of the "segments" of bytes which must be read
    let mut out = Vec::with_capacity(byte_ranges.len());
    let mut segments_endpoints = BTreeSet::<u64>::new();
    for byte_range in byte_ranges {
        out.push(vec![0; usize::try_from(byte_range.length(size)).unwrap()]);
        segments_endpoints.insert(byte_range.start(size));
        segments_endpoints.insert(byte_range.end(size));
    }

    // Find the overlapping part of each byte range with each segment
    //                 SEGMENT start     , end        OUTPUT index, offset
    let mut overlap: BTreeMap<(ByteOffset, ByteOffset), Vec<(usize, ByteOffset)>> = BTreeMap::new();
    for (byte_range_index, byte_range) in byte_ranges.iter().enumerate() {
        let byte_range_start = byte_range.start(size);
        let range = segments_endpoints.range((
            std::ops::Bound::Included(byte_range_start),
            std::ops::Bound::Included(byte_range.end(size)),
        ));
        for (segment_start, segment_end) in range.tuple_windows() {
            let byte_range_offset = *segment_start - byte_range_start;
            overlap
                .entry((*segment_start, *segment_end))
                .or_default()
                .push((byte_range_index, byte_range_offset));
        }
    }

    let mut bytes_offset = 0u64;
    for ((segment_start, segment_end), outputs) in overlap {
        // Go to the start of the segment
        if segment_start > bytes_offset {
            std::io::copy(
                &mut bytes.take(segment_start - bytes_offset),
                &mut std::io::sink(),
            )
            .unwrap();
        }

        let segment_length = segment_end - segment_start;
        if outputs.is_empty() {
            // No byte ranges are associated with this segment, so just read it to sink
            std::io::copy(&mut bytes.take(segment_length), &mut std::io::sink()).unwrap();
        } else {
            // Populate all byte ranges in this segment with data
            let segment_length_usize = usize::try_from(segment_length).unwrap();
            let mut segment_bytes = vec![0; segment_length_usize];
            bytes.take(segment_length).read_exact(&mut segment_bytes)?;
            for (byte_range_index, byte_range_offset) in outputs {
                let byte_range_offset = usize::try_from(byte_range_offset).unwrap();
                out[byte_range_index][byte_range_offset..byte_range_offset + segment_length_usize]
                    .copy_from_slice(&segment_bytes);
            }
        }

        // Offset is now the end of the segment
        bytes_offset = segment_end;
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_array_subset_iterator1() {
        let array_shape = vec![2, 2];
        let array_subset = ArraySubset::new_with_start_shape(vec![0, 0], vec![2, 1]).unwrap();
        let mut iter = array_subset.iter_contiguous_indices(&array_shape).unwrap();

        assert_eq!(iter.next().unwrap(), (vec![0, 0], 1));
        assert_eq!(iter.next().unwrap(), (vec![1, 0], 1));
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_array_subset_iterator2() {
        let array_shape = vec![2, 2];
        let array_subset = ArraySubset::new_with_start_shape(vec![1, 0], vec![1, 2]).unwrap();
        let mut iter = array_subset.iter_contiguous_indices(&array_shape).unwrap();

        assert_eq!(iter.next().unwrap(), (vec![1, 0], 2));
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_array_subset_iterator3() {
        let array_shape = vec![2, 2];
        let array_subset = ArraySubset::new_with_start_shape(vec![0, 0], vec![2, 2]).unwrap();
        let mut iter = array_subset.iter_contiguous_indices(&array_shape).unwrap();

        assert_eq!(iter.next().unwrap(), (vec![0, 0], 4));
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_array_subset_iterator4() {
        let array_shape = vec![2, 2, 2, 3];
        let array_subset =
            ArraySubset::new_with_start_shape(vec![0, 0, 0, 0], vec![2, 1, 2, 3]).unwrap();
        let mut iter = array_subset.iter_contiguous_indices(&array_shape).unwrap();

        assert_eq!(iter.next().unwrap(), (vec![0, 0, 0, 0], 6));
        assert_eq!(iter.next().unwrap(), (vec![1, 0, 0, 0], 6));
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_array_subset_iterator5() {
        let array_shape = vec![2, 2, 3];
        let array_subset = ArraySubset::new_with_start_shape(vec![0, 0, 1], vec![2, 2, 2]).unwrap();
        let mut iter = array_subset.iter_contiguous_indices(&array_shape).unwrap();

        assert_eq!(iter.next().unwrap(), (vec![0, 0, 1], 2));
        assert_eq!(iter.next().unwrap(), (vec![0, 1, 1], 2));
        assert_eq!(iter.next().unwrap(), (vec![1, 0, 1], 2));
        assert_eq!(iter.next().unwrap(), (vec![1, 1, 1], 2));
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_extract_byte_ranges_read() {
        let data: Vec<u8> = (0..10).collect();
        let size = data.len() as u64;
        let mut read = std::io::Cursor::new(data);
        let byte_ranges = vec![
            ByteRange::FromStart(3, Some(3)),
            ByteRange::FromStart(4, Some(1)),
            ByteRange::FromStart(1, Some(1)),
            ByteRange::FromEnd(1, Some(5)),
        ];
        let out = extract_byte_ranges_read(&mut read, size, &byte_ranges).unwrap();
        assert_eq!(
            out,
            vec![vec![3, 4, 5], vec![4], vec![1], vec![4, 5, 6, 7, 8]]
        );
    }
}
