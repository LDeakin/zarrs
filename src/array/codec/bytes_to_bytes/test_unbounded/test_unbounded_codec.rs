use crate::{
    array::{
        codec::{BytesPartialDecoderTraits, BytesToBytesCodecTraits, CodecError, CodecTraits},
        BytesRepresentation,
    },
    metadata::Metadata,
};

#[cfg(feature = "async")]
use crate::array::codec::AsyncBytesPartialDecoderTraits;

use super::test_unbounded_partial_decoder;

/// A `test_unbounded` codec implementation.
#[derive(Clone, Debug)]
pub struct TestUnboundedCodec {}

impl TestUnboundedCodec {
    /// Create a new `test_unbounded` codec.
    ///
    /// # Errors
    /// Returns [`TestUnboundedCompressionLevelError`] if `compression_level` is not valid.
    pub fn new() -> Self {
        Self {}
    }
}

impl CodecTraits for TestUnboundedCodec {
    fn create_metadata(&self) -> Option<Metadata> {
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
impl BytesToBytesCodecTraits for TestUnboundedCodec {
    fn encode_opt(&self, decoded_value: Vec<u8>, _parallel: bool) -> Result<Vec<u8>, CodecError> {
        Ok(decoded_value)
    }

    fn decode_opt(
        &self,
        encoded_value: Vec<u8>,
        _decoded_representation: &BytesRepresentation,
        _parallel: bool,
    ) -> Result<Vec<u8>, CodecError> {
        Ok(encoded_value)
    }

    fn partial_decoder_opt<'a>(
        &self,
        r: Box<dyn BytesPartialDecoderTraits + 'a>,
        _decoded_representation: &BytesRepresentation,
        _parallel: bool,
    ) -> Result<Box<dyn BytesPartialDecoderTraits + 'a>, CodecError> {
        Ok(Box::new(
            test_unbounded_partial_decoder::TestUnboundedPartialDecoder::new(r),
        ))
    }

    #[cfg(feature = "async")]
    async fn async_partial_decoder_opt<'a>(
        &'a self,
        r: Box<dyn AsyncBytesPartialDecoderTraits + 'a>,
        _decoded_representation: &BytesRepresentation,
        _parallel: bool,
    ) -> Result<Box<dyn AsyncBytesPartialDecoderTraits + 'a>, CodecError> {
        Ok(Box::new(
            test_unbounded_partial_decoder::AsyncTestUnboundedPartialDecoder::new(r),
        ))
    }

    fn compute_encoded_size(
        &self,
        _decoded_representation: &BytesRepresentation,
    ) -> BytesRepresentation {
        BytesRepresentation::UnboundedSize
    }
}
