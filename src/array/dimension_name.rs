use derive_more::From;
use serde::{Deserialize, Serialize};

/// A dimension name.
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug, From)]
pub struct DimensionName(Option<String>);

impl Default for DimensionName {
    /// Create a dimension without a name.
    fn default() -> Self {
        Self(None)
    }
}

impl DimensionName {
    /// Create a new dimension with `name`. Use [`default`](DimensionName::default) to create a dimension with no name.
    #[must_use]
    pub fn new<T: Into<String>>(name: T) -> Self {
        Self(Some(name.into()))
    }

    /// Get the dimension name as a [`&str`]. Returns [`None`] is the dimension has no name.
    #[must_use]
    pub fn as_str(&self) -> Option<&str> {
        self.0.as_deref()
    }
}

// impl<T: Into<String>> From<T> for DimensionName {
//     fn from(name: T) -> Self {
//         Self::new(name)
//     }
// }

impl From<&str> for DimensionName {
    fn from(name: &str) -> Self {
        Self::new(name)
    }
}

impl From<String> for DimensionName {
    fn from(name: String) -> Self {
        Self::new(name)
    }
}

impl From<&String> for DimensionName {
    fn from(name: &String) -> Self {
        Self::new(name)
    }
}

#[cfg(test)]
mod tests {
    use crate::array::DimensionName;

    #[test]
    fn dimension_name() {
        let dimension_name: DimensionName = "x".into();
        assert!(dimension_name.as_str() == Some("x"));
    }

    #[test]
    fn dimension_default_is_none() {
        let dimension_name = DimensionName::default();
        assert!(dimension_name.as_str().is_none());
    }

    #[test]
    fn dimension_name_eq() {
        let dimension_name_x: DimensionName = "x".into();
        let dimension_name_y = DimensionName::new("y");
        assert_ne!(dimension_name_x, dimension_name_y);
        assert_eq!(dimension_name_x, dimension_name_x.clone());
    }
}
