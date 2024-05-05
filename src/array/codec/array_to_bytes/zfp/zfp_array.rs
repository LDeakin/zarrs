#[derive(Debug)]
pub enum ZfpArray {
    Int32(Vec<i32>),
    Int64(Vec<i64>),
    Float(Vec<f32>),
    Double(Vec<f64>),
}

impl ZfpArray {
    pub fn len(&self) -> usize {
        match self {
            ZfpArray::Int32(v) => v.len(),
            ZfpArray::Int64(v) => v.len(),
            ZfpArray::Float(v) => v.len(),
            ZfpArray::Double(v) => v.len(),
        }
    }

    pub fn zfp_type(&self) -> zfp_sys::zfp_type {
        match self {
            ZfpArray::Int32(_) => zfp_sys::zfp_type_zfp_type_int32,
            ZfpArray::Int64(_) => zfp_sys::zfp_type_zfp_type_int64,
            ZfpArray::Float(_) => zfp_sys::zfp_type_zfp_type_float,
            ZfpArray::Double(_) => zfp_sys::zfp_type_zfp_type_double,
        }
    }

    // pub fn as_ptr(&self) -> *const std::ffi::c_void {
    //     match self {
    //         ZfpArray::Int32(v) => v.as_ptr().cast::<std::ffi::c_void>(),
    //         ZfpArray::Int64(v) => v.as_ptr().cast::<std::ffi::c_void>(),
    //         ZfpArray::Float(v) => v.as_ptr().cast::<std::ffi::c_void>(),
    //         ZfpArray::Double(v) => v.as_ptr().cast::<std::ffi::c_void>(),
    //     }
    // }

    pub fn as_mut_ptr(&mut self) -> *mut std::ffi::c_void {
        match self {
            ZfpArray::Int32(v) => v.as_mut_ptr().cast::<std::ffi::c_void>(),
            ZfpArray::Int64(v) => v.as_mut_ptr().cast::<std::ffi::c_void>(),
            ZfpArray::Float(v) => v.as_mut_ptr().cast::<std::ffi::c_void>(),
            ZfpArray::Double(v) => v.as_mut_ptr().cast::<std::ffi::c_void>(),
        }
    }
}
