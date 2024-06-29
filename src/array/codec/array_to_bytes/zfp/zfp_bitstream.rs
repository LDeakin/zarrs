use std::ptr::NonNull;

use zfp_sys::{bitstream, stream_close, stream_open};

/// A `zfp` bitstream.
pub(super) struct ZfpBitstream(NonNull<bitstream>);

impl Drop for ZfpBitstream {
    fn drop(&mut self) {
        unsafe {
            stream_close(self.0.as_ptr());
        }
    }
}

impl ZfpBitstream {
    pub fn new(buffer: &mut [u8]) -> Option<Self> {
        let stream =
            unsafe { stream_open(buffer.as_mut_ptr().cast::<std::ffi::c_void>(), buffer.len()) };
        NonNull::new(stream).map(Self)
    }

    pub const fn as_bitstream(&self) -> *mut bitstream {
        self.0.as_ptr()
    }
}
