#![allow(missing_docs)]

use opendal::Operator;
use zarrs_opendal::OpendalStore;

use std::error::Error;

#[test]
fn memory() -> Result<(), Box<dyn Error>> {
    let builder = opendal::services::Memory::default();
    let op = Operator::new(builder)?.finish().blocking();
    let store = OpendalStore::new(op);
    zarrs_storage::store_test::store_write(&store)?;
    zarrs_storage::store_test::store_read(&store)?;
    zarrs_storage::store_test::store_list(&store)?;
    Ok(())
}

#[test]
#[cfg_attr(miri, ignore)]
fn filesystem() -> Result<(), Box<dyn Error>> {
    let path = tempfile::TempDir::new()?;
    let builder = opendal::services::Fs::default().root(&path.path().to_string_lossy());
    let op = Operator::new(builder)?.finish().blocking();
    let store = OpendalStore::new(op);
    zarrs_storage::store_test::store_write(&store)?;
    zarrs_storage::store_test::store_read(&store)?;
    zarrs_storage::store_test::store_list(&store)?;
    Ok(())
}
