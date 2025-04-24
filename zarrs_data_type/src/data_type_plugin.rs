use zarrs_metadata::v3::MetadataV3;
use zarrs_plugin::{Plugin, PluginCreateError};

use crate::DataType;

/// A data type plugin.
#[derive(derive_more::Deref)]
pub struct DataTypePlugin(Plugin<DataType, MetadataV3>);
inventory::collect!(DataTypePlugin);

impl DataTypePlugin {
    /// Create a new [`DataTypePlugin`].
    pub const fn new(
        identifier: &'static str,
        match_name_fn: fn(name: &str) -> bool,
        create_fn: fn(metadata: &MetadataV3) -> Result<DataType, PluginCreateError>,
    ) -> Self {
        Self(Plugin::new(identifier, match_name_fn, create_fn))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zarrs_plugin::{PluginCreateError, PluginMetadataInvalidError};

    inventory::submit! {
        DataTypePlugin::new("test", is_test, create_test)
    }

    fn is_test(name: &str) -> bool {
        name == "test"
    }

    fn create_test(metadata: &MetadataV3) -> Result<DataType, PluginCreateError> {
        Err(PluginMetadataInvalidError::new("test", "codec", metadata.clone()).into())
    }

    #[test]
    fn data_type_plugin() {
        let mut found = false;
        for plugin in inventory::iter::<DataTypePlugin> {
            found |= plugin.match_name("test");
        }
        assert!(found);
    }
}
