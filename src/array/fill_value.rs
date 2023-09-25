//! Zarr fill values.
//!
//! See <https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html#fill-value>.

/// The fill value of the Zarr array.
///
/// Provides an element value to use for uninitialised portions of the Zarr array.
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct FillValue(Vec<u8>);

impl core::fmt::Display for FillValue {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

impl From<Vec<u8>> for FillValue {
    fn from(value: Vec<u8>) -> Self {
        FillValue(value)
    }
}

impl From<bool> for FillValue {
    fn from(value: bool) -> Self {
        FillValue(vec![u8::from(value)])
    }
}

impl From<u8> for FillValue {
    fn from(value: u8) -> Self {
        FillValue(value.to_ne_bytes().to_vec())
    }
}

impl From<u16> for FillValue {
    fn from(value: u16) -> Self {
        FillValue(value.to_ne_bytes().to_vec())
    }
}

impl From<u32> for FillValue {
    fn from(value: u32) -> Self {
        FillValue(value.to_ne_bytes().to_vec())
    }
}

impl From<u64> for FillValue {
    fn from(value: u64) -> Self {
        FillValue(value.to_ne_bytes().to_vec())
    }
}

impl From<i8> for FillValue {
    fn from(value: i8) -> Self {
        FillValue(value.to_ne_bytes().to_vec())
    }
}

impl From<i16> for FillValue {
    fn from(value: i16) -> Self {
        FillValue(value.to_ne_bytes().to_vec())
    }
}

impl From<i32> for FillValue {
    fn from(value: i32) -> Self {
        FillValue(value.to_ne_bytes().to_vec())
    }
}

impl From<i64> for FillValue {
    fn from(value: i64) -> Self {
        FillValue(value.to_ne_bytes().to_vec())
    }
}

#[cfg(feature = "float16")]
impl From<half::f16> for FillValue {
    fn from(value: half::f16) -> Self {
        FillValue(value.to_ne_bytes().to_vec())
    }
}

#[cfg(feature = "bfloat16")]
impl From<half::bf16> for FillValue {
    fn from(value: half::bf16) -> Self {
        FillValue(value.to_ne_bytes().to_vec())
    }
}

impl From<f32> for FillValue {
    fn from(value: f32) -> Self {
        FillValue(value.to_ne_bytes().to_vec())
    }
}

impl From<f64> for FillValue {
    fn from(value: f64) -> Self {
        FillValue(value.to_ne_bytes().to_vec())
    }
}

impl From<num::complex::Complex32> for FillValue {
    fn from(value: num::complex::Complex32) -> Self {
        let mut bytes = Vec::with_capacity(std::mem::size_of::<num::complex::Complex32>());
        bytes.extend(value.re.to_ne_bytes());
        bytes.extend(value.im.to_ne_bytes());
        FillValue(bytes)
    }
}

impl From<num::complex::Complex64> for FillValue {
    fn from(value: num::complex::Complex64) -> Self {
        let mut bytes = Vec::with_capacity(std::mem::size_of::<num::complex::Complex64>());
        bytes.extend(value.re.to_ne_bytes());
        bytes.extend(value.im.to_ne_bytes());
        FillValue(bytes)
    }
}

impl FillValue {
    /// Create a new fill value composed of `bytes`.
    #[must_use]
    pub fn new(bytes: Vec<u8>) -> FillValue {
        FillValue(bytes)
    }

    /// Returns the size in bytes of the fill value.
    #[must_use]
    pub fn size(&self) -> usize {
        self.0.len()
    }

    /// Return the byte representation of the fill value.
    #[must_use]
    pub fn as_ne_bytes(&self) -> &[u8] {
        &self.0
    }
}
