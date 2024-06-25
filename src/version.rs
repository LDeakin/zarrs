//! `zarrs` version information.

/// The `zarrs` major version.
#[must_use]
pub fn version_major() -> u32 {
    const VERSION_MAJOR: &str = env!("CARGO_PKG_VERSION_MAJOR");
    let major: u32 = VERSION_MAJOR.parse::<u32>().unwrap_or_default();
    major
}

/// The `zarrs` minor version.
#[must_use]
pub fn version_minor() -> u32 {
    const VERSION_MINOR: &str = env!("CARGO_PKG_VERSION_MINOR");
    let minor: u32 = VERSION_MINOR.parse::<u32>().unwrap_or_default();
    minor
}

/// The `zarrs` patch version.
#[must_use]
pub fn version_patch() -> u32 {
    const VERSION_PATCH: &str = env!("CARGO_PKG_VERSION_PATCH");
    let patch: u32 = VERSION_PATCH.parse::<u32>().unwrap_or_default();
    patch
}

/// A [`u32`] representation of the `zarrs` version.
///
/// Encoded as
/// ```rust
/// # use zarrs::version::version_major;
/// # use zarrs::version::version_minor;
/// # use zarrs::version::version_patch;
/// let version =
///     (version_major() << 22) | (version_minor() << 12) | version_patch();
/// ```
/// The major/minor/patch version can then be decoded as
/// ```rust
/// # use zarrs::version::version_major;
/// # use zarrs::version::version_minor;
/// # use zarrs::version::version_patch;
/// # let version = (version_major() << 22) | (version_minor() << 12) | version_patch();
/// # assert!(version == zarrs::version::version());
/// let version_major = (version as u32 >> 22) & 0x7F;
/// # assert!(version_major == zarrs::version::version_major());
/// let version_minor = (version as u32 >> 12) & 0x3FF;
/// # assert!(version_minor == zarrs::version::version_minor());
/// let version_patch = (version as u32) & 0xFFF;
/// # assert!(version_patch == zarrs::version::version_patch());
/// ```
#[must_use]
pub fn version() -> u32 {
    (version_major() << 22) | (version_minor() << 12) | version_patch()
}
