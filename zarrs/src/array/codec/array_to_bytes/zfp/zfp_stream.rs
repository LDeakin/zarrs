use std::ptr::NonNull;

use zfp_sys::{
    zfp_stream, zfp_stream_close, zfp_stream_open, zfp_stream_set_accuracy, zfp_stream_set_params,
    zfp_stream_set_precision, zfp_stream_set_rate, zfp_stream_set_reversible, zfp_type,
    zfp_type_zfp_type_double, zfp_type_zfp_type_float,
};

use super::ZfpMode;

/// A `zfp` stream.
pub(super) struct ZfpStream(NonNull<zfp_stream>);

impl Drop for ZfpStream {
    fn drop(&mut self) {
        unsafe {
            zfp_stream_close(self.0.as_ptr());
        }
    }
}

impl ZfpStream {
    pub fn new(mode: &ZfpMode, type_: zfp_type) -> Option<Self> {
        let zfp = unsafe { zfp_stream_open(std::ptr::null_mut()) };
        match mode {
            ZfpMode::Expert {
                minbits,
                maxbits,
                maxprec,
                minexp,
            } => {
                unsafe { zfp_stream_set_params(zfp, *minbits, *maxbits, *maxprec, *minexp) };
            }
            ZfpMode::FixedRate { rate } => {
                unsafe { zfp_stream_set_rate(zfp, *rate, type_, 3, 0) };
            }
            ZfpMode::FixedPrecision { precision } => unsafe {
                zfp_stream_set_precision(zfp, *precision);
            },
            ZfpMode::FixedAccuracy { tolerance } => {
                if type_ == zfp_type_zfp_type_float || type_ == zfp_type_zfp_type_double {
                    unsafe { zfp_stream_set_accuracy(zfp, *tolerance) };
                } else {
                    return None;
                }
            }
            ZfpMode::Reversible => {
                unsafe { zfp_stream_set_reversible(zfp) };
            }
        };
        NonNull::new(zfp).map(Self)
    }

    pub const fn as_zfp_stream(&self) -> *mut zfp_stream {
        self.0.as_ptr()
    }
}
