use std::borrow::Cow;

use crate::{
    array::{
        codec::{ArrayPartialDecoderTraits, CodecError, CodecOptions},
        DataType,
    },
    array_subset::ArraySubset,
};

#[cfg(feature = "async")]
use crate::array::codec::AsyncArrayPartialDecoderTraits;

use super::{round_bytes, IDENTIFIER};

/// Partial decoder for the `bitround` codec.
pub struct BitroundPartialDecoder<'a> {
    input_handle: Box<dyn ArrayPartialDecoderTraits + 'a>,
    data_type: DataType,
    keepbits: u32,
}

impl<'a> BitroundPartialDecoder<'a> {
    /// Create a new partial decoder for the `bitround` codec.
    pub fn new(
        input_handle: Box<dyn ArrayPartialDecoderTraits + 'a>,
        data_type: &DataType,
        keepbits: u32,
    ) -> Result<Self, CodecError> {
        match data_type {
            DataType::Float16
            | DataType::BFloat16
            | DataType::UInt16
            | DataType::Int16
            | DataType::Float32
            | DataType::Complex64
            | DataType::UInt32
            | DataType::Int32
            | DataType::Float64
            | DataType::Complex128
            | DataType::UInt64
            | DataType::Int64 => Ok(Self {
                input_handle,
                data_type: data_type.clone(),
                keepbits,
            }),
            _ => Err(CodecError::UnsupportedDataType(
                data_type.clone(),
                IDENTIFIER.to_string(),
            )),
        }
    }
}

impl ArrayPartialDecoderTraits for BitroundPartialDecoder<'_> {
    fn element_size(&self) -> usize {
        self.data_type.size()
    }

    fn partial_decode_opt(
        &self,
        array_subsets: &[ArraySubset],
        options: &CodecOptions,
    ) -> Result<Vec<Cow<'_, [u8]>>, CodecError> {
        let mut bytes = self
            .input_handle
            .partial_decode_opt(array_subsets, options)?;

        for bytes in &mut bytes {
            round_bytes(bytes.to_mut(), &self.data_type, self.keepbits)?;
        }

        Ok(bytes)
    }
}

#[cfg(feature = "async")]
/// Asynchronous partial decoder for the `bitround` codec.
pub struct AsyncBitroundPartialDecoder<'a> {
    input_handle: Box<dyn AsyncArrayPartialDecoderTraits + 'a>,
    data_type: DataType,
    keepbits: u32,
}

#[cfg(feature = "async")]
impl<'a> AsyncBitroundPartialDecoder<'a> {
    /// Create a new partial decoder for the `bitround` codec.
    pub fn new(
        input_handle: Box<dyn AsyncArrayPartialDecoderTraits + 'a>,
        data_type: &DataType,
        keepbits: u32,
    ) -> Result<Self, CodecError> {
        match data_type {
            DataType::Float16
            | DataType::BFloat16
            | DataType::UInt16
            | DataType::Int16
            | DataType::Float32
            | DataType::Complex64
            | DataType::UInt32
            | DataType::Int32
            | DataType::Float64
            | DataType::Complex128
            | DataType::UInt64
            | DataType::Int64 => Ok(Self {
                input_handle,
                data_type: data_type.clone(),
                keepbits,
            }),
            _ => Err(CodecError::UnsupportedDataType(
                data_type.clone(),
                IDENTIFIER.to_string(),
            )),
        }
    }
}

#[cfg(feature = "async")]
#[async_trait::async_trait]
impl AsyncArrayPartialDecoderTraits for AsyncBitroundPartialDecoder<'_> {
    fn element_size(&self) -> usize {
        self.data_type.size()
    }

    async fn partial_decode_opt(
        &self,
        array_subsets: &[ArraySubset],
        options: &CodecOptions,
    ) -> Result<Vec<Cow<'_, [u8]>>, CodecError> {
        let mut bytes = self
            .input_handle
            .partial_decode_opt(array_subsets, options)
            .await?;

        for bytes in &mut bytes {
            round_bytes(bytes.to_mut(), &self.data_type, self.keepbits)?;
        }

        Ok(bytes)
    }
}
