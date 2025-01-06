//! `zarrs` version information.

include!(concat!(env!("OUT_DIR"), "/version.rs"));

/// The `zarrs` major version.
#[must_use]
pub const fn version_major() -> u32 {
    VERSION_MAJOR
}

/// The `zarrs` minor version.
#[must_use]
pub const fn version_minor() -> u32 {
    VERSION_MINOR
}

/// The `zarrs` patch version.
#[must_use]
pub const fn version_patch() -> u32 {
    VERSION_PATCH
}

/// The `zarrs` pre-release version.
#[must_use]
pub const fn version_pre() -> &'static str {
    env!("CARGO_PKG_VERSION_PRE")
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
pub const fn version() -> u32 {
    (version_major() << 22) | (version_minor() << 12) | version_patch()
}

/// A string representation of the `zarrs` version.
///
/// Matches the `CARGO_PKG_VERSION`.
#[must_use]
pub const fn version_str() -> &'static str {
    env!("CARGO_PKG_VERSION")
}
