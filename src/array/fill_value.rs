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

impl From<&[u8]> for FillValue {
    fn from(value: &[u8]) -> Self {
        Self(value.to_vec())
    }
}

impl From<Vec<u8>> for FillValue {
    fn from(value: Vec<u8>) -> Self {
        Self(value)
    }
}

impl From<bool> for FillValue {
    fn from(value: bool) -> Self {
        Self(vec![u8::from(value)])
    }
}

impl From<u8> for FillValue {
    fn from(value: u8) -> Self {
        Self(value.to_ne_bytes().to_vec())
    }
}

impl From<u16> for FillValue {
    fn from(value: u16) -> Self {
        Self(value.to_ne_bytes().to_vec())
    }
}

impl From<u32> for FillValue {
    fn from(value: u32) -> Self {
        Self(value.to_ne_bytes().to_vec())
    }
}

impl From<u64> for FillValue {
    fn from(value: u64) -> Self {
        Self(value.to_ne_bytes().to_vec())
    }
}

impl From<i8> for FillValue {
    fn from(value: i8) -> Self {
        Self(value.to_ne_bytes().to_vec())
    }
}

impl From<i16> for FillValue {
    fn from(value: i16) -> Self {
        Self(value.to_ne_bytes().to_vec())
    }
}

impl From<i32> for FillValue {
    fn from(value: i32) -> Self {
        Self(value.to_ne_bytes().to_vec())
    }
}

impl From<i64> for FillValue {
    fn from(value: i64) -> Self {
        Self(value.to_ne_bytes().to_vec())
    }
}

impl From<half::f16> for FillValue {
    fn from(value: half::f16) -> Self {
        Self(value.to_ne_bytes().to_vec())
    }
}

impl From<half::bf16> for FillValue {
    fn from(value: half::bf16) -> Self {
        Self(value.to_ne_bytes().to_vec())
    }
}

impl From<f32> for FillValue {
    fn from(value: f32) -> Self {
        Self(value.to_ne_bytes().to_vec())
    }
}

impl From<f64> for FillValue {
    fn from(value: f64) -> Self {
        Self(value.to_ne_bytes().to_vec())
    }
}

impl From<num::complex::Complex32> for FillValue {
    fn from(value: num::complex::Complex32) -> Self {
        let mut bytes = Vec::with_capacity(std::mem::size_of::<num::complex::Complex32>());
        bytes.extend(value.re.to_ne_bytes());
        bytes.extend(value.im.to_ne_bytes());
        Self(bytes)
    }
}

impl From<num::complex::Complex64> for FillValue {
    fn from(value: num::complex::Complex64) -> Self {
        let mut bytes = Vec::with_capacity(std::mem::size_of::<num::complex::Complex64>());
        bytes.extend(value.re.to_ne_bytes());
        bytes.extend(value.im.to_ne_bytes());
        Self(bytes)
    }
}

impl FillValue {
    /// Create a new fill value composed of `bytes`.
    #[must_use]
    pub fn new(bytes: Vec<u8>) -> Self {
        Self(bytes)
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

    /// Check if the bytes are equal to a sequence of the fill value.
    #[allow(clippy::missing_panics_doc)]
    #[must_use]
    pub fn equals_all(&self, bytes: &[u8]) -> bool {
        match self.0.len() {
            1 => {
                let fill_value = self.0[0];
                let fill_value_128 = u128::from_ne_bytes([self.0[0]; 16]);
                let (prefix, aligned, suffix) = unsafe { bytes.align_to::<u128>() };
                prefix.iter().all(|x| x == &fill_value)
                    && suffix.iter().all(|x| x == &fill_value)
                    && aligned.iter().all(|x| x == &fill_value_128)
            }
            2 => {
                let fill_value_128 = u128::from_ne_bytes(self.0[..2].repeat(8).try_into().unwrap());
                let (prefix, aligned, suffix) = unsafe { bytes.align_to::<u128>() };
                prefix.chunks_exact(2).all(|x| x == self.0)
                    && suffix.chunks_exact(2).all(|x| x == self.0)
                    && aligned.iter().all(|x| x == &fill_value_128)
            }
            4 => {
                let fill_value_128 = u128::from_ne_bytes(self.0[..4].repeat(4).try_into().unwrap());
                let (prefix, aligned, suffix) = unsafe { bytes.align_to::<u128>() };
                prefix.chunks_exact(4).all(|x| x == self.0)
                    && suffix.chunks_exact(4).all(|x| x == self.0)
                    && aligned.iter().all(|x| x == &fill_value_128)
            }
            8 => {
                let fill_value_128 = u128::from_ne_bytes(self.0[..8].repeat(2).try_into().unwrap());
                let (prefix, aligned, suffix) = unsafe { bytes.align_to::<u128>() };
                prefix.chunks_exact(8).all(|x| x == self.0)
                    && suffix.chunks_exact(8).all(|x| x == self.0)
                    && aligned.iter().all(|x| x == &fill_value_128)
            }
            16 => {
                let fill_value_128 = u128::from_ne_bytes(self.0[..16].try_into().unwrap());
                let (prefix, aligned, suffix) = unsafe { bytes.align_to::<u128>() };
                prefix.chunks_exact(16).all(|x| x == self.0)
                    && suffix.chunks_exact(16).all(|x| x == self.0)
                    && aligned.iter().all(|x| x == &fill_value_128)
            }
            _ => bytes
                .chunks_exact(bytes.len())
                .all(|element| element == self.0),
        }
    }
}
