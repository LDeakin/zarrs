use std::ptr::NonNull;
use zfp_sys::{
    zfp_field, zfp_field_1d, zfp_field_2d, zfp_field_3d, zfp_field_4d, zfp_field_free, zfp_type,
    zfp_type_zfp_type_double, zfp_type_zfp_type_float, zfp_type_zfp_type_int32,
    zfp_type_zfp_type_int64,
};

/// A `zfp` field.
#[derive(Debug)]
pub struct ZfpField(NonNull<zfp_field>);

impl Drop for ZfpField {
    fn drop(&mut self) {
        unsafe {
            zfp_field_free(self.0.as_ptr());
        }
    }
}

#[allow(non_upper_case_globals)]
const fn zfp_type_to_size(zfp_type_: zfp_type) -> Option<usize> {
    match zfp_type_ {
        zfp_type_zfp_type_int32 | zfp_type_zfp_type_float => Some(4),
        zfp_type_zfp_type_int64 | zfp_type_zfp_type_double => Some(8),
        _ => None,
    }
}

impl ZfpField {
    pub fn new(data: &mut [u8], zfp_type_: zfp_type, shape: &[usize]) -> Option<Self> {
        match shape.len() {
            1 => Self::new_1d(data, zfp_type_, shape[0]),
            2 => Self::new_2d(data, zfp_type_, shape[1], shape[0]),
            3 => Self::new_3d(data, zfp_type_, shape[2], shape[1], shape[0]),
            4 => Self::new_4d(data, zfp_type_, shape[3], shape[2], shape[1], shape[0]),
            _ => None,
        }
    }

    pub fn new_1d(data: &mut [u8], zfp_type_: zfp_type, nx: usize) -> Option<Self> {
        if let Some(size) = zfp_type_to_size(zfp_type_) {
            if size * nx != data.len() {
                return None;
            }
        } else {
            return None;
        }
        let pointer = data.as_mut_ptr().cast::<std::ffi::c_void>();
        let field = unsafe { zfp_field_1d(pointer, zfp_type_, nx) };
        NonNull::new(field).map(Self)
    }

    pub fn new_2d(data: &mut [u8], zfp_type_: zfp_type, nx: usize, ny: usize) -> Option<Self> {
        if let Some(size) = zfp_type_to_size(zfp_type_) {
            if size * nx * ny != data.len() {
                return None;
            }
        } else {
            return None;
        }
        let pointer = data.as_mut_ptr().cast::<std::ffi::c_void>();
        let field = unsafe { zfp_field_2d(pointer, zfp_type_, nx, ny) };
        NonNull::new(field).map(Self)
    }

    pub fn new_3d(
        data: &mut [u8],
        zfp_type_: zfp_type,
        nx: usize,
        ny: usize,
        nz: usize,
    ) -> Option<Self> {
        if let Some(size) = zfp_type_to_size(zfp_type_) {
            if size * nx * ny * nz != data.len() {
                return None;
            }
        } else {
            return None;
        }
        let pointer = data.as_mut_ptr().cast::<std::ffi::c_void>();
        let field = unsafe { zfp_field_3d(pointer, zfp_type_, nx, ny, nz) };
        NonNull::new(field).map(Self)
    }

    pub fn new_4d(
        data: &mut [u8],
        zfp_type_: zfp_type,
        nx: usize,
        ny: usize,
        nz: usize,
        nw: usize,
    ) -> Option<Self> {
        if let Some(size) = zfp_type_to_size(zfp_type_) {
            if size * nx * ny * nz * nw != data.len() {
                return None;
            }
        } else {
            return None;
        }
        let pointer = data.as_mut_ptr().cast::<std::ffi::c_void>();
        let field = unsafe { zfp_field_4d(pointer, zfp_type_, nx, ny, nz, nw) };
        NonNull::new(field).map(Self)
    }

    pub const fn as_zfp_field(&self) -> *mut zfp_field {
        self.0.as_ptr()
    }
}
