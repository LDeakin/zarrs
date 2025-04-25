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
    use std::sync::Arc;

    use crate::{
        DataTypeExtension, FillValue, IncompatibleFillValueError,
        IncompatibleFillValueMetadataError,
    };

    use super::*;
    use zarrs_metadata::{v3::array::fill_value::FillValueMetadataV3, DataTypeSize};
    use zarrs_plugin::{MetadataConfiguration, PluginCreateError};

    inventory::submit! {
        DataTypePlugin::new("zarrs.test_void", is_test_void, create_test_void)
    }

    #[derive(Debug)]
    struct TestVoidDataType;

    impl DataTypeExtension for TestVoidDataType {
        fn name(&self) -> String {
            "zarrs.test_void".to_string()
        }

        fn size(&self) -> DataTypeSize {
            DataTypeSize::Fixed(0)
        }

        fn configuration(&self) -> MetadataConfiguration {
            MetadataConfiguration::default()
        }

        fn fill_value(
            &self,
            _fill_value_metadata: &FillValueMetadataV3,
        ) -> Result<FillValue, IncompatibleFillValueMetadataError> {
            Ok(FillValue::new(vec![]))
        }

        fn metadata_fill_value(
            &self,
            _fill_value: &FillValue,
        ) -> Result<FillValueMetadataV3, IncompatibleFillValueError> {
            Ok(FillValueMetadataV3::Null)
        }
    }

    fn is_test_void(name: &str) -> bool {
        name == "zarrs.test_void"
    }

    fn create_test_void(_metadata: &MetadataV3) -> Result<DataType, PluginCreateError> {
        Ok(DataType::Extension(Arc::new(TestVoidDataType)))
    }

    #[test]
    fn data_type_plugin() {
        let mut found = false;
        for plugin in inventory::iter::<DataTypePlugin> {
            if plugin.match_name("zarrs.test_void") {
                found = true;
                let data_type = plugin.create(&MetadataV3::new("zarrs.test_void")).unwrap();
                assert_eq!(data_type.name(), "zarrs.test_void");
                assert_eq!(data_type.size(), DataTypeSize::Fixed(0));
                assert!(data_type.metadata().configuration_is_none_or_empty());
                assert!(data_type
                    .fill_value_from_metadata(&FillValueMetadataV3::Null)
                    .is_ok());
                assert_eq!(
                    data_type
                        .metadata_fill_value(&FillValue::new(vec![]))
                        .unwrap(),
                    FillValueMetadataV3::Null
                );
                if let DataType::Extension(ext) = data_type {
                    assert!(ext.codec_bytes().is_err());
                    assert!(ext.codec_packbits().is_err());
                }
            }
        }
        assert!(found);
    }
}
