use std::{ops::Deref, sync::Arc};

use zarrs_metadata::Configuration;

use super::{
    ArrayToArrayCodecTraits, ArrayToBytesCodecTraits, BytesToBytesCodecTraits,
    CodecMetadataOptions, CodecTraits,
};

/// A named codec.
#[derive(Debug)]
pub struct NamedCodec<T: CodecTraits + ?Sized> {
    name: String,
    codec: Arc<T>,
}

impl<T: CodecTraits + ?Sized> NamedCodec<T> {
    /// Create a new [`NamedCodec`].
    #[must_use]
    pub fn new(name: String, codec: Arc<T>) -> Self {
        Self { name, codec }
    }

    /// The name of the codec.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Create the codec configuration.
    ///
    /// See [`CodecTraits::configuration_opt`].
    #[must_use]
    pub fn configuration_opt(&self, options: &CodecMetadataOptions) -> Option<Configuration> {
        self.codec().configuration_opt(self.name(), options)
    }

    /// Create the codec configuration with default options.
    ///
    /// See [`CodecTraits::configuration`].
    #[must_use]
    pub fn configuration(&self) -> Option<Configuration> {
        self.codec().configuration(self.name())
    }

    /// The underlying codec.
    #[must_use]
    pub fn codec(&self) -> &Arc<T> {
        &self.codec
    }
}

impl<T: CodecTraits + ?Sized> Deref for NamedCodec<T> {
    type Target = Arc<T>;

    fn deref(&self) -> &Self::Target {
        &self.codec
    }
}

macro_rules! impl_named_codec {
    ($named_codec:ident, $codec_trait:ident) => {
        impl Clone for $named_codec {
            fn clone(&self) -> Self {
                Self {
                    name: self.name.clone(),
                    codec: self.codec.clone(),
                }
            }
        }

        impl<T: $codec_trait + 'static> From<Arc<T>> for $named_codec {
            fn from(codec: Arc<T>) -> Self {
                $named_codec::new(codec.default_name(), codec)
            }
        }

        impl From<Arc<dyn $codec_trait>> for $named_codec {
            fn from(codec: Arc<dyn $codec_trait>) -> Self {
                $named_codec::new(codec.default_name(), codec)
            }
        }
    };
}

/// A named array to array codec.
pub type NamedArrayToArrayCodec = NamedCodec<dyn ArrayToArrayCodecTraits>;
impl_named_codec!(NamedArrayToArrayCodec, ArrayToArrayCodecTraits);

/// A named array to bytes codec.
pub type NamedArrayToBytesCodec = NamedCodec<dyn ArrayToBytesCodecTraits>;
impl_named_codec!(NamedArrayToBytesCodec, ArrayToBytesCodecTraits);

/// A named bytes to bytes codec.
pub type NamedBytesToBytesCodec = NamedCodec<dyn BytesToBytesCodecTraits>;
impl_named_codec!(NamedBytesToBytesCodec, BytesToBytesCodecTraits);
