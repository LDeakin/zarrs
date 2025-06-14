#![allow(missing_docs)]

use std::{error::Error, path::PathBuf, sync::Arc};

use zarrs::{array::Array, array_subset::ArraySubset, storage::StoreKey};
use zarrs_filesystem::FilesystemStore;
use zarrs_zip::ZipStorageAdapter;

#[test]
fn zarr_python_compat_zip_store() -> Result<(), Box<dyn Error>> {
    let path = PathBuf::from("tests/data/zarr_python_compat");
    let store = Arc::new(FilesystemStore::new(&path)?);
    let store = Arc::new(ZipStorageAdapter::new(store, StoreKey::new("zarr.zip")?)?);

    let array = Array::open(store, "/foo")?;
    assert_eq!(array.shape(), vec![100, 100]);
    let elements = array.retrieve_array_subset_elements::<u8>(&ArraySubset::new_with_shape(
        array.shape().to_vec(),
    ))?;
    assert_eq!(elements, vec![42u8; 100 * 100]);

    Ok(())
}

#[cfg(feature = "fletcher32")]
#[test]
fn zarr_python_compat_fletcher32_v2() -> Result<(), Box<dyn Error>> {
    // NOTE: could support numcodecs.zarr3.fletcher32, but would need to permit and ignore "id" field
    // zarrs::config::global_config_mut()
    //     .experimental_codec_names_mut()
    //     .entry("fletcher32".to_string())
    //     .and_modify(|e| *e = "numcodecs.fletcher32".to_string());

    let path = PathBuf::from("tests/data/zarr_python_compat/fletcher32.zarr");
    let store = Arc::new(FilesystemStore::new(&path)?);

    let array = Array::open(store, "/")?;
    assert_eq!(array.shape(), vec![100, 100]);
    let elements = array.retrieve_array_subset_elements::<u16>(&ArraySubset::new_with_shape(
        array.shape().to_vec(),
    ))?;
    assert_eq!(elements, (0..100 * 100).collect::<Vec<u16>>());

    Ok(())
}

#[test]
fn zarr_python_v2_compat_str_fv_0() -> Result<(), Box<dyn Error>> {
    let store = Arc::new(FilesystemStore::new(
        "tests/data/zarr_python_compat/str_v2_fv_0.zarr",
    )?);
    let array = zarrs::array::Array::open(store.clone(), "/")?;
    let subset_all = array.subset_all();
    let elements = array.retrieve_array_subset_elements::<String>(&subset_all)?;

    assert_eq!(elements, &["a", "bb", "", "", ""]);

    Ok(())
}

#[test]
fn zarr_python_v2_compat_str_fv_null() -> Result<(), Box<dyn Error>> {
    let store = Arc::new(FilesystemStore::new(
        "tests/data/zarr_python_compat/str_v2_fv_null.zarr",
    )?);
    let array = zarrs::array::Array::open(store.clone(), "/")?;
    let subset_all = array.subset_all();
    let elements = array.retrieve_array_subset_elements::<String>(&subset_all)?;

    assert_eq!(elements, &["a", "bb", "", "", ""]);

    Ok(())
}

#[cfg(feature = "jiff")]
#[test]
fn zarr_python_v3_numpy_datetime_read_jiff() -> Result<(), Box<dyn Error>> {
    use jiff::{Timestamp, TimestampRound, Unit};
    // https://github.com/BurntSushi/jiff/issues/1
    for (path, unit) in [
        // (
        //     "tests/data/zarr_python_compat/datetime64[Y].zarr",
        //     Unit::Year,
        // ),
        // (
        //     "tests/data/zarr_python_compat/datetime64[M].zarr",
        //     Unit::Month,
        // ),
        // (
        //     "tests/data/zarr_python_compat/datetime64[W].zarr",
        //     Unit::Week,
        // ),
        // (
        //     "tests/data/zarr_python_compat/datetime64[D].zarr",
        //     Unit::Day,
        // ),
        (
            "tests/data/zarr_python_compat/datetime64[h].zarr",
            Unit::Hour,
        ),
        (
            "tests/data/zarr_python_compat/datetime64[s].zarr",
            Unit::Second,
        ),
        (
            "tests/data/zarr_python_compat/datetime64[ms].zarr",
            Unit::Millisecond,
        ),
        (
            "tests/data/zarr_python_compat/datetime64[us].zarr",
            Unit::Microsecond,
        ),
        (
            "tests/data/zarr_python_compat/datetime64[ns].zarr",
            Unit::Nanosecond,
        ),
        (
            "tests/data/zarr_python_compat/datetime64[10ms].zarr",
            Unit::Millisecond,
        ),
        (
            "tests/data/zarr_python_compat/datetime64[10us].zarr",
            Unit::Microsecond,
        ),
    ] {
        let store = Arc::new(FilesystemStore::new(path)?);
        let array = zarrs::array::Array::open(store.clone(), "/")?;
        let subset_all = array.subset_all();
        let elements = array.retrieve_array_subset_elements::<jiff::Timestamp>(&subset_all)?;

        println!("{path:?}");
        println!("{elements:?}");
        let round = TimestampRound::new().smallest(unit);
        assert_eq!(
            elements,
            &[
                jiff::Timestamp::UNIX_EPOCH,
                jiff::Timestamp::MIN,
                "2005-02-03T00:00:00Z"
                    .parse::<Timestamp>()
                    .unwrap()
                    .round(round)
                    .unwrap(),
                "2005-02-03T04:05Z"
                    .parse::<Timestamp>()
                    .unwrap()
                    .round(round)
                    .unwrap(),
                "2005-02-03T04:05:06Z"
                    .parse::<Timestamp>()
                    .unwrap()
                    .round(round)
                    .unwrap(),
                jiff::Timestamp::MIN,
            ]
        );
    }

    Ok(())
}

#[cfg(feature = "jiff")]
#[test]
fn zarr_python_v3_numpy_datetime_write_jiff() -> Result<(), Box<dyn Error>> {
    use zarrs_metadata_ext::data_type::NumpyDateTime64DataTypeUnit;
    // Write to store, check equality

    for unit in [
        // NumpyDateTime64DataTypeUnit::Year,
        // NumpyDateTime64DataTypeUnit::Month,
        // NumpyDateTime64DataTypeUnit::Week,
        // NumpyDateTime64DataTypeUnit::Day,
        // NumpyDateTime64DataTypeUnit::Hour,
        NumpyDateTime64DataTypeUnit::Minute,
        NumpyDateTime64DataTypeUnit::Second,
        NumpyDateTime64DataTypeUnit::Millisecond,
        NumpyDateTime64DataTypeUnit::Microsecond,
        NumpyDateTime64DataTypeUnit::Nanosecond,
    ] {
        println!("{unit:?}");
        use jiff::{Timestamp, TimestampRound};
        use zarrs::array::ArrayBuilder;
        use zarrs_data_type::FillValue;
        use zarrs_storage::store::MemoryStore;

        let store = Arc::new(MemoryStore::new());
        let array = ArrayBuilder::new(
            vec![6],
            zarrs::array::DataType::NumpyDateTime64 {
                unit,
                scale_factor: 1.try_into().unwrap(),
            },
            vec![5].try_into().unwrap(),
            FillValue::from(i64::MIN),
        )
        .build(store.clone(), "/")?;

        let round = TimestampRound::new().smallest(unit.try_into().unwrap());
        let elements = [
            "2005-02-25T00:00:00Z"
                .parse::<Timestamp>()
                .unwrap()
                .round(round)
                .unwrap(),
            jiff::Timestamp::UNIX_EPOCH,
            jiff::Timestamp::MIN,
            "2005-02-01T00:00:00Z"
                .parse::<Timestamp>()
                .unwrap()
                .round(round)
                .unwrap(),
            "2005-02-25T03:00:00Z"
                .parse::<Timestamp>()
                .unwrap()
                .round(round)
                .unwrap(),
            jiff::Timestamp::MIN,
        ];

        array.store_array_subset_elements(&array.subset_all(), &elements)?;
        assert_eq!(
            array.retrieve_array_subset_elements::<jiff::Timestamp>(&array.subset_all())?,
            elements
        );
    }

    Ok(())
}
