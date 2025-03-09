use std::{ffi::c_void, sync::Arc};

use derive_more::Display;
use dlpark::{ffi::Device, ShapeAndStrides, ToTensor};
use thiserror::Error;
use zarrs_data_type::DataType;

use super::{ChunkRepresentation, RawBytes};

mod array_dlpack_ext_async;
mod array_dlpack_ext_sync;

pub use array_dlpack_ext_async::AsyncArrayDlPackExt;
pub use array_dlpack_ext_sync::ArrayDlPackExt;

/// [`RawBytes`] for use in a [`dlpark::ManagerCtx`].
pub struct RawBytesDlPack {
    bytes: Arc<RawBytes<'static>>,
    dtype: dlpark::ffi::DataType,
    shape: Vec<i64>,
}

/// Errors related to [`[Async]ArrayDlPackExt`](ArrayDlPackExt) methods.
#[derive(Clone, Debug, Error, Display)]
#[non_exhaustive]
pub enum ArrayDlPackExtError {
    /// The Zarr data type is not supported by `DLPack`.
    UnsupportedDataType,
}

impl RawBytesDlPack {
    /// Create a new [`RawBytesDlPack`].
    ///
    /// # Errors
    /// Returns [`ArrayDlPackExtError::UnsupportedDataType`] if the data type is not supported.
    ///
    /// # Panics
    /// Panics if an element in the shape cannot be encoded in a `i64`.
    pub fn new(
        bytes: Arc<RawBytes<'static>>,
        representation: &ChunkRepresentation,
    ) -> Result<Self, ArrayDlPackExtError> {
        let dtype = match representation.data_type() {
            DataType::Bool => dlpark::ffi::DataType::BOOL,
            DataType::Int8 => dlpark::ffi::DataType::I8,
            DataType::Int16 => dlpark::ffi::DataType::I16,
            DataType::Int32 => dlpark::ffi::DataType::I32,
            DataType::Int64 => dlpark::ffi::DataType::I64,
            DataType::UInt8 => dlpark::ffi::DataType::U8,
            DataType::UInt16 => dlpark::ffi::DataType::U16,
            DataType::UInt32 => dlpark::ffi::DataType::U32,
            DataType::UInt64 => dlpark::ffi::DataType::U64,
            DataType::Float16 => dlpark::ffi::DataType::F16,
            DataType::Float32 => dlpark::ffi::DataType::F32,
            DataType::Float64 => dlpark::ffi::DataType::F64,
            DataType::BFloat16 => dlpark::ffi::DataType::BF16,
            // TODO: Support extension data types with fallback?
            _ => Err(ArrayDlPackExtError::UnsupportedDataType)?,
        };
        let shape = representation
            .shape()
            .iter()
            .map(|s| i64::try_from(s.get()).unwrap())
            .collect();
        Ok(Self {
            bytes,
            dtype,
            shape,
        })
    }
}

impl ToTensor for RawBytesDlPack {
    fn data_ptr(&self) -> *mut c_void {
        self.bytes.as_ptr().cast::<c_void>().cast_mut()
    }

    fn byte_offset(&self) -> u64 {
        0
    }

    fn device(&self) -> Device {
        Device::CPU
    }

    fn dtype(&self) -> dlpark::ffi::DataType {
        self.dtype
    }

    fn shape_and_strides(&self) -> ShapeAndStrides {
        ShapeAndStrides::new_contiguous(&self.shape)
    }
}

#[cfg(test)]
mod tests {
    use dlpark::{IntoDLPack, ManagedTensor};
    use zarrs_data_type::{DataType, FillValue};
    use zarrs_storage::store::MemoryStore;

    use crate::{
        array::{codec::CodecOptions, ArrayBuilder, ArrayDlPackExt},
        array_subset::ArraySubset,
    };

    #[test]
    fn array_dlpack_ext_sync() {
        let store = MemoryStore::new();
        let array = ArrayBuilder::new(
            vec![4, 4],
            DataType::Float32,
            vec![2, 2].try_into().unwrap(),
            FillValue::from(-1.0f32),
        )
        .build(store.into(), "/")
        .unwrap();
        array
            .store_chunk_elements::<f32>(&[0, 0], &[0.0, 1.0, 2.0, 3.0])
            .unwrap();
        let tensor = array
            .retrieve_chunks_dlpack(
                &ArraySubset::new_with_shape(vec![1, 2]),
                &CodecOptions::default(),
            )
            .unwrap();

        assert_eq!(
            ManagedTensor::new(tensor.into_dlpack()).as_slice::<f32>(),
            &[0.0, 1.0, -1.0, -1.0, 2.0, 3.0, -1.0, -1.0]
        );
    }
}
