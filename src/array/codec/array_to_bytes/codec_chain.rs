//! An `array->bytes` codec formed by joining an `array->array` sequence, `array->bytes`, and `bytes->bytes` sequence of codecs.

use crate::{
    array::{
        codec::{
            partial_decoder_cache::{ArrayPartialDecoderCache, BytesPartialDecoderCache},
            ArrayCodecTraits, ArrayPartialDecoderTraits, ArrayToArrayCodecTraits,
            ArrayToBytesCodecTraits, BytesPartialDecoderTraits, BytesToBytesCodecTraits, Codec,
            CodecError, CodecTraits,
        },
        ArrayRepresentation, BytesRepresentation,
    },
    metadata::Metadata,
    plugin::PluginCreateError,
};

#[cfg(feature = "async")]
use crate::array::codec::{AsyncArrayPartialDecoderTraits, AsyncBytesPartialDecoderTraits};

/// A codec chain is a sequence of `array->array`, a `bytes->bytes`, and a sequence of `array->bytes` codecs.
///
/// A codec chain partial decoder may insert a cache: [`ArrayPartialDecoderCache`] or [`BytesPartialDecoderCache`].
/// For example, the output of the blosc/gzip codecs should be cached since they read and decode an entire chunk.
/// If decoding (i.e. going backwards through a codec chain), then a cache may be inserted
///    - following the last codec with [`partial_decoder_decodes_all`](crate::array::codec::CodecTraits::partial_decoder_decodes_all) true, or
///    - preceding the first codec with [`partial_decoder_should_cache_input`](crate::array::codec::CodecTraits::partial_decoder_should_cache_input), whichever is further.
#[derive(Debug, Clone)]
pub struct CodecChain {
    array_to_array: Vec<Box<dyn ArrayToArrayCodecTraits>>,
    array_to_bytes: Box<dyn ArrayToBytesCodecTraits>,
    bytes_to_bytes: Vec<Box<dyn BytesToBytesCodecTraits>>,
    // decoded_representations_array: Vec<ArrayRepresentation>,
    cache_index: Option<usize>, // for partial decoders
}

impl CodecChain {
    /// Create a new codec chain.
    #[must_use]
    pub fn new(
        array_to_array: Vec<Box<dyn ArrayToArrayCodecTraits>>,
        array_to_bytes: Box<dyn ArrayToBytesCodecTraits>,
        bytes_to_bytes: Vec<Box<dyn BytesToBytesCodecTraits>>,
        // decoded_representations_array: Vec<ArrayRepresentation>,
    ) -> Self {
        let mut cache_index_must = None;
        let mut cache_index_should = None;
        let mut codec_index = 0;
        for codec in bytes_to_bytes.iter().rev() {
            if cache_index_should.is_none() && codec.partial_decoder_should_cache_input() {
                cache_index_should = Some(codec_index);
            }
            if codec.partial_decoder_decodes_all() {
                cache_index_must = Some(codec_index + 1);
            }
            codec_index += 1;
        }

        if cache_index_should.is_none() && array_to_bytes.partial_decoder_should_cache_input() {
            cache_index_should = Some(codec_index);
        }
        if array_to_bytes.partial_decoder_decodes_all() {
            cache_index_must = Some(codec_index + 1);
        }
        codec_index += 1;

        for codec in array_to_array.iter().rev() {
            if cache_index_should.is_none() && codec.partial_decoder_should_cache_input() {
                cache_index_should = Some(codec_index);
            }
            if codec.partial_decoder_decodes_all() {
                cache_index_must = Some(codec_index + 1);
            }
            codec_index += 1;
        }

        let cache_index = if let (Some(cache_index_must), Some(cache_index_should)) =
            (cache_index_must, cache_index_should)
        {
            Some(std::cmp::max(cache_index_must, cache_index_should))
        } else if cache_index_must.is_some() {
            cache_index_must
        } else if cache_index_should.is_some() {
            cache_index_should
        } else {
            None
        };

        Self {
            array_to_array,
            array_to_bytes,
            bytes_to_bytes,
            cache_index,
        }
    }

    /// Create a new codec chain from a list of metadata.
    ///
    /// # Errors
    ///
    /// Returns a [`PluginCreateError`] if:
    ///  - a codec could not be created,
    ///  - no array to bytes codec is supplied, or
    ///  - more than one array to bytes codec is supplied.
    pub fn from_metadata(metadatas: &[Metadata]) -> Result<Self, PluginCreateError> {
        let mut array_to_array: Vec<Box<dyn ArrayToArrayCodecTraits>> = vec![];
        let mut array_to_bytes: Option<Box<dyn ArrayToBytesCodecTraits>> = None;
        let mut bytes_to_bytes: Vec<Box<dyn BytesToBytesCodecTraits>> = vec![];
        for metadata in metadatas {
            let codec = Codec::from_metadata(metadata)?;
            match codec {
                Codec::ArrayToArray(codec) => {
                    array_to_array.push(codec);
                }
                Codec::ArrayToBytes(codec) => {
                    if array_to_bytes.is_none() {
                        array_to_bytes = Some(codec);
                    } else {
                        return Err(PluginCreateError::Other {
                            error_str: "multiple array to bytes codecs".to_string(),
                        });
                    }
                }
                Codec::BytesToBytes(codec) => {
                    bytes_to_bytes.push(codec);
                }
            }
        }

        array_to_bytes.map_or_else(
            || {
                Err(PluginCreateError::Other {
                    error_str: "missing array to bytes codec".to_string(),
                })
            },
            |array_to_bytes| Ok(Self::new(array_to_array, array_to_bytes, bytes_to_bytes)),
        )
    }

    /// Create codec chain metadata.
    #[must_use]
    pub fn create_metadatas(&self) -> Vec<Metadata> {
        let mut metadatas =
            Vec::with_capacity(self.array_to_array.len() + 1 + self.bytes_to_bytes.len());
        for codec in &self.array_to_array {
            if let Some(metadata) = codec.create_metadata() {
                metadatas.push(metadata);
            }
        }
        if let Some(metadata) = self.array_to_bytes.create_metadata() {
            metadatas.push(metadata);
        }
        for codec in &self.bytes_to_bytes {
            if let Some(metadata) = codec.create_metadata() {
                metadatas.push(metadata);
            }
        }
        metadatas
    }

    /// Get the array to array codecs
    #[must_use]
    pub fn array_to_array_codecs(&self) -> &[Box<dyn ArrayToArrayCodecTraits>] {
        &self.array_to_array
    }

    /// Get the array to bytes codec
    #[allow(clippy::borrowed_box)]
    #[must_use]
    pub fn array_to_bytes_codec(&self) -> &Box<dyn ArrayToBytesCodecTraits> {
        &self.array_to_bytes
    }

    /// Get the bytes to bytes codecs
    #[must_use]
    pub fn bytes_to_bytes_codecs(&self) -> &[Box<dyn BytesToBytesCodecTraits>] {
        &self.bytes_to_bytes
    }

    fn get_array_representations(
        &self,
        decoded_representation: ArrayRepresentation,
    ) -> Result<Vec<ArrayRepresentation>, CodecError> {
        let mut array_representations = Vec::with_capacity(self.array_to_array.len() + 1);
        array_representations.push(decoded_representation);
        for codec in &self.array_to_array {
            array_representations
                .push(codec.compute_encoded_size(array_representations.last().unwrap())?);
        }
        Ok(array_representations)
    }

    fn get_bytes_representations(
        &self,
        array_representation_last: &ArrayRepresentation,
    ) -> Result<Vec<BytesRepresentation>, CodecError> {
        let mut bytes_representations = Vec::with_capacity(self.bytes_to_bytes.len() + 1);
        bytes_representations.push(
            self.array_to_bytes
                .compute_encoded_size(array_representation_last)?,
        );
        for codec in &self.bytes_to_bytes {
            bytes_representations
                .push(codec.compute_encoded_size(bytes_representations.last().unwrap()));
        }
        Ok(bytes_representations)
    }
}

impl CodecTraits for CodecChain {
    fn create_metadata(&self) -> Option<Metadata> {
        // A codec chain cannot does not have standard metadata.
        // However, usage of the codec chain is explicit in [Array] and it will call create_configurations()
        // from CodecChain::create_metadatas().
        None
    }

    fn partial_decoder_should_cache_input(&self) -> bool {
        false
    }

    fn partial_decoder_decodes_all(&self) -> bool {
        false
    }
}

#[cfg_attr(feature = "async", async_trait::async_trait)]
impl ArrayToBytesCodecTraits for CodecChain {
    fn partial_decoder_opt<'a>(
        &'a self,
        mut input_handle: Box<dyn BytesPartialDecoderTraits + 'a>,
        decoded_representation: &ArrayRepresentation,
        parallel: bool,
    ) -> Result<Box<dyn ArrayPartialDecoderTraits + 'a>, CodecError> {
        let array_representations =
            self.get_array_representations(decoded_representation.clone())?;
        let bytes_representations =
            self.get_bytes_representations(array_representations.last().unwrap())?;

        let mut codec_index = 0;
        for (codec, bytes_representation) in std::iter::zip(
            self.bytes_to_bytes.iter().rev(),
            bytes_representations.iter().rev().skip(1),
        ) {
            if Some(codec_index) == self.cache_index {
                input_handle = Box::new(BytesPartialDecoderCache::new(&*input_handle, parallel)?);
            }
            codec_index += 1;
            input_handle =
                codec.partial_decoder_opt(input_handle, bytes_representation, parallel)?;
        }

        if Some(codec_index) == self.cache_index {
            input_handle = Box::new(BytesPartialDecoderCache::new(&*input_handle, parallel)?);
        };

        let mut input_handle = {
            let array_representation = array_representations.last().unwrap();
            let codec = &self.array_to_bytes;
            codec_index += 1;
            codec.partial_decoder_opt(input_handle, array_representation, parallel)?
        };

        for (codec, array_representation) in std::iter::zip(
            self.array_to_array.iter().rev(),
            array_representations.iter().rev().skip(1),
        ) {
            if Some(codec_index) == self.cache_index {
                input_handle = Box::new(ArrayPartialDecoderCache::new(
                    &*input_handle,
                    array_representation.clone(),
                    parallel,
                )?);
            }
            codec_index += 1;
            input_handle =
                codec.partial_decoder_opt(input_handle, array_representation, parallel)?;
        }

        if Some(codec_index) == self.cache_index {
            input_handle = Box::new(ArrayPartialDecoderCache::new(
                &*input_handle,
                array_representations.first().unwrap().clone(),
                parallel,
            )?);
        }

        Ok(input_handle)
    }

    #[cfg(feature = "async")]
    async fn async_partial_decoder_opt<'a>(
        &'a self,
        mut input_handle: Box<dyn AsyncBytesPartialDecoderTraits + 'a>,
        decoded_representation: &ArrayRepresentation,
        parallel: bool,
    ) -> Result<Box<dyn AsyncArrayPartialDecoderTraits + 'a>, CodecError> {
        let array_representations =
            self.get_array_representations(decoded_representation.clone())?;
        let bytes_representations =
            self.get_bytes_representations(array_representations.last().unwrap())?;

        let mut codec_index = 0;
        for (codec, bytes_representation) in std::iter::zip(
            self.bytes_to_bytes.iter().rev(),
            bytes_representations.iter().rev().skip(1),
        ) {
            if Some(codec_index) == self.cache_index {
                input_handle =
                    Box::new(BytesPartialDecoderCache::async_new(&*input_handle, parallel).await?);
            }
            codec_index += 1;
            input_handle = codec
                .async_partial_decoder_opt(input_handle, bytes_representation, parallel)
                .await?;
        }

        if Some(codec_index) == self.cache_index {
            input_handle =
                Box::new(BytesPartialDecoderCache::async_new(&*input_handle, parallel).await?);
        };

        let mut input_handle = {
            let array_representation = array_representations.last().unwrap();
            let codec = &self.array_to_bytes;
            codec_index += 1;
            codec
                .async_partial_decoder_opt(input_handle, array_representation, parallel)
                .await?
        };

        for (codec, array_representation) in std::iter::zip(
            self.array_to_array.iter().rev(),
            array_representations.iter().rev().skip(1),
        ) {
            if Some(codec_index) == self.cache_index {
                input_handle = Box::new(
                    ArrayPartialDecoderCache::async_new(
                        &*input_handle,
                        array_representation.clone(),
                        parallel,
                    )
                    .await?,
                );
            }
            codec_index += 1;
            input_handle = codec
                .async_partial_decoder_opt(input_handle, array_representation, parallel)
                .await?;
        }

        if Some(codec_index) == self.cache_index {
            input_handle = Box::new(
                ArrayPartialDecoderCache::async_new(
                    &*input_handle,
                    array_representations.first().unwrap().clone(),
                    parallel,
                )
                .await?,
            );
        }

        Ok(input_handle)
    }

    fn compute_encoded_size(
        &self,
        decoded_representation: &ArrayRepresentation,
    ) -> Result<BytesRepresentation, CodecError> {
        let mut decoded_representation = decoded_representation.clone();
        for codec in &self.array_to_array {
            decoded_representation = codec.compute_encoded_size(&decoded_representation)?;
        }

        let mut bytes_representation = self
            .array_to_bytes
            .compute_encoded_size(&decoded_representation)?;

        for codec in &self.bytes_to_bytes {
            bytes_representation = codec.compute_encoded_size(&bytes_representation);
        }

        Ok(bytes_representation)
    }
}

#[cfg_attr(feature = "async", async_trait::async_trait)]
impl ArrayCodecTraits for CodecChain {
    fn encode_opt(
        &self,
        decoded_value: Vec<u8>,
        decoded_representation: &ArrayRepresentation,
        parallel: bool,
    ) -> Result<Vec<u8>, CodecError> {
        if decoded_value.len() as u64 != decoded_representation.size() {
            return Err(CodecError::UnexpectedChunkDecodedSize(
                decoded_value.len(),
                decoded_representation.size(),
            ));
        }

        let mut decoded_representation = decoded_representation.clone();

        let mut value = decoded_value;
        // array->array
        for codec in &self.array_to_array {
            value = codec.encode_opt(value, &decoded_representation, parallel)?;
            decoded_representation = codec.compute_encoded_size(&decoded_representation)?;
        }

        // array->bytes
        value = self
            .array_to_bytes
            .encode_opt(value, &decoded_representation, parallel)?;
        let mut decoded_representation = self
            .array_to_bytes
            .compute_encoded_size(&decoded_representation)?;

        // bytes->bytes
        for codec in &self.bytes_to_bytes {
            value = codec.encode_opt(value, parallel)?;
            decoded_representation = codec.compute_encoded_size(&decoded_representation);
        }

        Ok(value)
    }

    fn decode_opt(
        &self,
        mut encoded_value: Vec<u8>,
        decoded_representation: &ArrayRepresentation,
        parallel: bool,
    ) -> Result<Vec<u8>, CodecError> {
        let array_representations =
            self.get_array_representations(decoded_representation.clone())?;
        let bytes_representations =
            self.get_bytes_representations(array_representations.last().unwrap())?;

        // bytes->bytes
        for (codec, bytes_representation) in std::iter::zip(
            self.bytes_to_bytes.iter().rev(),
            bytes_representations.iter().rev().skip(1),
        ) {
            encoded_value = codec.decode_opt(encoded_value, bytes_representation, parallel)?;
        }

        // bytes->array
        encoded_value = self.array_to_bytes.decode_opt(
            encoded_value,
            array_representations.last().unwrap(),
            parallel,
        )?;

        // array->array
        for (codec, array_representation) in std::iter::zip(
            self.array_to_array.iter().rev(),
            array_representations.iter().rev().skip(1),
        ) {
            encoded_value = codec.decode_opt(encoded_value, array_representation, parallel)?;
        }

        if encoded_value.len() as u64 != decoded_representation.size() {
            return Err(CodecError::UnexpectedChunkDecodedSize(
                encoded_value.len(),
                decoded_representation.size(),
            ));
        }

        Ok(encoded_value)
    }

    #[cfg(feature = "async")]
    async fn async_encode_opt(
        &self,
        decoded_value: Vec<u8>,
        decoded_representation: &ArrayRepresentation,
        parallel: bool,
    ) -> Result<Vec<u8>, CodecError> {
        if decoded_value.len() as u64 != decoded_representation.size() {
            return Err(CodecError::UnexpectedChunkDecodedSize(
                decoded_value.len(),
                decoded_representation.size(),
            ));
        }

        let mut decoded_representation = decoded_representation.clone();

        let mut value = decoded_value;
        // array->array
        for codec in &self.array_to_array {
            value = codec
                .async_encode_opt(value, &decoded_representation, parallel)
                .await?;
            decoded_representation = codec.compute_encoded_size(&decoded_representation)?;
        }

        // array->bytes
        value = self
            .array_to_bytes
            .async_encode_opt(value, &decoded_representation, parallel)
            .await?;
        let mut decoded_representation = self
            .array_to_bytes
            .compute_encoded_size(&decoded_representation)?;

        // bytes->bytes
        for codec in &self.bytes_to_bytes {
            value = codec.async_encode_opt(value, parallel).await?;
            decoded_representation = codec.compute_encoded_size(&decoded_representation);
        }

        Ok(value)
    }

    #[cfg(feature = "async")]
    async fn async_decode_opt(
        &self,
        mut encoded_value: Vec<u8>,
        decoded_representation: &ArrayRepresentation,
        parallel: bool,
    ) -> Result<Vec<u8>, CodecError> {
        let array_representations =
            self.get_array_representations(decoded_representation.clone())?;
        let bytes_representations =
            self.get_bytes_representations(array_representations.last().unwrap())?;

        // bytes->bytes
        for (codec, bytes_representation) in std::iter::zip(
            self.bytes_to_bytes.iter().rev(),
            bytes_representations.iter().rev().skip(1),
        ) {
            encoded_value = codec
                .async_decode_opt(encoded_value, bytes_representation, parallel)
                .await?;
        }

        // bytes->array
        encoded_value = self
            .array_to_bytes
            .async_decode_opt(
                encoded_value,
                array_representations.last().unwrap(),
                parallel,
            )
            .await?;

        // array->array
        for (codec, array_representation) in std::iter::zip(
            self.array_to_array.iter().rev(),
            array_representations.iter().rev().skip(1),
        ) {
            encoded_value = codec
                .async_decode_opt(encoded_value, array_representation, parallel)
                .await?;
        }

        if encoded_value.len() as u64 != decoded_representation.size() {
            return Err(CodecError::UnexpectedChunkDecodedSize(
                encoded_value.len(),
                decoded_representation.size(),
            ));
        }

        Ok(encoded_value)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        array::{DataType, FillValue},
        array_subset::ArraySubset,
    };

    use super::*;

    #[cfg(feature = "transpose")]
    const JSON_TRANSPOSE1: &str = r#"{
    "name": "transpose",
    "configuration": {
      "order": [0, 2, 1]
    }
}"#;

    #[cfg(feature = "transpose")]
    const JSON_TRANSPOSE2: &str = r#"{
    "name": "transpose",
    "configuration": {
        "order": [2, 0, 1]
    }
}"#;

    #[cfg(feature = "blosc")]
    const JSON_BLOSC: &str = r#"{
    "name": "blosc",
    "configuration": {
        "cname": "lz4",
        "clevel": 5,
        "shuffle": "shuffle",
        "typesize": 2,
        "blocksize": 0
    }
}"#;

    #[cfg(feature = "gzip")]
    const JSON_GZIP: &str = r#"{
    "name": "gzip",
    "configuration": {
        "level": 1
    }
}"#;

    #[cfg(feature = "zstd")]
    const JSON_ZSTD: &str = r#"{
    "name": "zstd",
    "configuration": {
        "level": 1,
        "checksum": false
    }
}"#;

    const JSON_BYTES: &str = r#"{
    "name": "bytes",
    "configuration": {
        "endian": "big"
    }
}"#;

    #[cfg(feature = "crc32c")]
    const JSON_CRC32C: &str = r#"{ 
    "name": "crc32c"
}"#;

    #[test]
    fn codec_chain_round_trip() {
        let array_representation =
            ArrayRepresentation::new(vec![2, 3, 4], DataType::UInt16, FillValue::from(0u16))
                .unwrap();
        let elements: Vec<u16> = (0..array_representation.num_elements() as u16).collect();
        let bytes = safe_transmute::transmute_to_bytes(&elements).to_vec();

        let codec_configurations: Vec<Metadata> = vec![
            #[cfg(feature = "transpose")]
            serde_json::from_str(JSON_TRANSPOSE1).unwrap(),
            #[cfg(feature = "transpose")]
            serde_json::from_str(JSON_TRANSPOSE2).unwrap(),
            serde_json::from_str(JSON_BYTES).unwrap(),
            #[cfg(feature = "blosc")]
            serde_json::from_str(JSON_BLOSC).unwrap(),
            #[cfg(feature = "gzip")]
            serde_json::from_str(JSON_GZIP).unwrap(),
            #[cfg(feature = "zstd")]
            serde_json::from_str(JSON_ZSTD).unwrap(),
            #[cfg(feature = "crc32c")]
            serde_json::from_str(JSON_CRC32C).unwrap(),
        ];
        println!("{codec_configurations:?}");
        let not_just_bytes = codec_configurations.len() > 1;
        let codec = CodecChain::from_metadata(&codec_configurations).unwrap();

        let encoded = codec.encode(bytes.clone(), &array_representation).unwrap();
        let decoded = codec
            .decode(encoded.clone(), &array_representation)
            .unwrap();
        if not_just_bytes {
            assert_ne!(encoded, decoded);
        }
        assert_eq!(bytes, decoded);

        let encoded = codec
            .par_encode(bytes.clone(), &array_representation)
            .unwrap();
        let decoded = codec
            .par_decode(encoded.clone(), &array_representation)
            .unwrap();
        if not_just_bytes {
            assert_ne!(encoded, decoded);
        }
        assert_eq!(bytes, decoded);

        // println!("{} {}", encoded_chunk.len(), decoded_chunk.len());
    }

    #[test]
    fn codec_chain_round_trip_partial() {
        let array_representation =
            ArrayRepresentation::new(vec![2, 2, 2], DataType::UInt16, FillValue::from(0u16))
                .unwrap();
        let elements: Vec<u16> = (0..array_representation.num_elements() as u16).collect();
        let bytes = safe_transmute::transmute_to_bytes(&elements).to_vec();

        let codec_configurations: Vec<Metadata> = vec![
            #[cfg(feature = "transpose")]
            serde_json::from_str(JSON_TRANSPOSE1).unwrap(),
            #[cfg(feature = "transpose")]
            serde_json::from_str(JSON_TRANSPOSE2).unwrap(),
            serde_json::from_str(JSON_BYTES).unwrap(),
            #[cfg(feature = "blosc")]
            serde_json::from_str(JSON_BLOSC).unwrap(),
            #[cfg(feature = "gzip")]
            serde_json::from_str(JSON_GZIP).unwrap(),
            #[cfg(feature = "zstd")]
            serde_json::from_str(JSON_ZSTD).unwrap(),
            #[cfg(feature = "crc32c")]
            serde_json::from_str(JSON_CRC32C).unwrap(),
        ];
        println!("{codec_configurations:?}");
        let codec = CodecChain::from_metadata(&codec_configurations).unwrap();

        let encoded = codec.encode(bytes, &array_representation).unwrap();
        let decoded_regions =
            [ArraySubset::new_with_start_shape(vec![0, 1, 0], vec![2, 1, 1]).unwrap()];
        let input_handle = Box::new(std::io::Cursor::new(encoded));
        let partial_decoder = codec
            .partial_decoder(input_handle, &array_representation)
            .unwrap();
        let decoded_partial_chunk = partial_decoder.partial_decode(&decoded_regions).unwrap();

        let decoded_partial_chunk: Vec<u16> = decoded_partial_chunk
            .into_iter()
            .flatten()
            .collect::<Vec<_>>()
            .chunks(std::mem::size_of::<u16>())
            .map(|b| u16::from_ne_bytes(b.try_into().unwrap()))
            .collect();
        println!("decoded_partial_chunk {decoded_partial_chunk:?}");
        let answer: Vec<u16> = vec![2, 6];
        assert_eq!(answer, decoded_partial_chunk);

        // println!("{} {}", encoded_chunk.len(), decoded_chunk.len());
    }
}
