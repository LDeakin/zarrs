use std::ptr::NonNull;
use zfp_sys::{
    zfp_field, zfp_field_1d, zfp_field_2d, zfp_field_3d, zfp_field_4d, zfp_field_free, zfp_type,
};

/// A zfp field.
#[derive(Debug)]
pub struct ZfpField(NonNull<zfp_field>);

impl Drop for ZfpField {
    fn drop(&mut self) {
        unsafe {
            zfp_field_free(self.0.as_ptr());
        }
    }
}

impl ZfpField {
    pub fn new(data: &mut [u8], zfp_type: zfp_type, shape: &[usize]) -> Option<Self> {
        match shape.len() {
            1 => ZfpField::new_1d(data, zfp_type, shape[0]),
            2 => ZfpField::new_2d(data, zfp_type, shape[1], shape[0]),
            3 => ZfpField::new_3d(data, zfp_type, shape[2], shape[1], shape[0]),
            4 => ZfpField::new_4d(data, zfp_type, shape[3], shape[2], shape[1], shape[0]),
            _ => None,
        }
    }

    pub fn new_1d(data: &mut [u8], zfp_type: zfp_type, nx: usize) -> Option<Self> {
        // FIXME: Validate size of data
        let pointer = data.as_ptr() as *mut std::ffi::c_void;
        let field = unsafe { zfp_field_1d(pointer, zfp_type, nx) };
        NonNull::new(field).map(Self)
    }

    pub fn new_2d(data: &mut [u8], zfp_type: zfp_type, nx: usize, ny: usize) -> Option<Self> {
        // FIXME: This is a flaw with the zfp library, this should be const
        // FIXME: Validate size of data
        let pointer = data.as_ptr() as *mut std::ffi::c_void;
        let field = unsafe { zfp_field_2d(pointer, zfp_type, nx, ny) };
        NonNull::new(field).map(Self)
    }

    pub fn new_3d(
        data: &mut [u8],
        zfp_type: zfp_type,
        nx: usize,
        ny: usize,
        nz: usize,
    ) -> Option<Self> {
        // FIXME: Validate size of data
        let pointer = data.as_ptr() as *mut std::ffi::c_void;
        let field = unsafe { zfp_field_3d(pointer, zfp_type, nx, ny, nz) };
        NonNull::new(field).map(Self)
    }

    pub fn new_4d(
        data: &mut [u8],
        zfp_type: zfp_type,
        nx: usize,
        ny: usize,
        nz: usize,
        nw: usize,
    ) -> Option<Self> {
        // FIXME: Validate size of data
        let pointer = data.as_ptr() as *mut std::ffi::c_void;
        let field = unsafe { zfp_field_4d(pointer, zfp_type, nx, ny, nz, nw) };
        NonNull::new(field).map(Self)
    }

    pub fn as_zfp_field(&self) -> *mut zfp_field {
        self.0.as_ptr()
    }
}
