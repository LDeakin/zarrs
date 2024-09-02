use std::error::Error;

use crate::{
    byte_range::ByteRange, ListableStorageTraits, ReadableStorageTraits, StoreKeyRange,
    StoreKeyStartValue, StorePrefix, WritableStorageTraits,
};

#[cfg(feature = "async")]
use crate::{AsyncListableStorageTraits, AsyncReadableStorageTraits, AsyncWritableStorageTraits};

// Create a store with the following data
// - a/
//   - b [0, 1, 2, 3]
//   - c [0]
//   - d/
//     - e
//   - f/
//     - g
//     - h
// - i/
//   - j/
//     - k [0, 1]
pub fn store_write<T: WritableStorageTraits>(store: &T) -> Result<(), Box<dyn Error>> {
    store.erase_prefix(&StorePrefix::root())?;

    store.set(&"a/b".try_into()?, vec![255, 255, 255].into())?;
    store.set_partial_values(&[StoreKeyStartValue::new("a/b".try_into()?, 1, &[1, 2])])?;
    store.set_partial_values(&[StoreKeyStartValue::new("a/b".try_into()?, 3, &[3])])?;
    store.set_partial_values(&[StoreKeyStartValue::new("a/b".try_into()?, 0, &[0])])?;

    store.set(&"a/c".try_into()?, vec![0].into())?;
    store.set(&"a/d/e".try_into()?, vec![].into())?;
    store.set(&"a/f/g".try_into()?, vec![].into())?;
    store.set(&"a/f/h".try_into()?, vec![].into())?;
    store.set(&"i/j/k".try_into()?, vec![0, 1].into())?;

    store.set(&"erase".try_into()?, vec![].into())?;
    store.erase(&"erase".try_into()?)?;
    store.erase(&"erase".try_into()?)?; // succeeds

    store.set(&"erase_values_0".try_into()?, vec![].into())?;
    store.set(&"erase_values_1".try_into()?, vec![].into())?;
    store.erase_values(&["erase_values_0".try_into()?, "erase_values_1".try_into()?])?;

    store.set(&"erase_prefix/0".try_into()?, vec![].into())?;
    store.set(&"erase_prefix/1".try_into()?, vec![].into())?;
    store.erase_prefix(&"erase_prefix/".try_into()?)?;

    Ok(())
}

pub fn store_read<T: ReadableStorageTraits>(store: &T) -> Result<(), Box<dyn Error>> {
    assert!(store.get(&"notfound".try_into()?)?.is_none());
    assert!(store.size_key(&"notfound".try_into()?)?.is_none());
    assert_eq!(
        store.get(&"a/b".try_into()?)?,
        Some(vec![0, 1, 2, 3].into())
    );
    assert_eq!(store.size_key(&"a/b".try_into()?)?, Some(4));
    assert_eq!(store.size_key(&"a/c".try_into()?)?, Some(1));
    assert_eq!(store.size_key(&"i/j/k".try_into()?)?, Some(2));
    assert_eq!(
        store.get_partial_values_key(
            &"a/b".try_into()?,
            &[
                ByteRange::FromStart(1, Some(1)),
                ByteRange::FromEnd(0, Some(1))
            ]
        )?,
        Some(vec![vec![1].into(), vec![3].into()])
    );
    assert_eq!(
        store.get_partial_values(&[
            StoreKeyRange::new("a/b".try_into()?, ByteRange::FromStart(1, None)),
            StoreKeyRange::new("a/b".try_into()?, ByteRange::FromEnd(1, Some(2))),
            StoreKeyRange::new("i/j/k".try_into()?, ByteRange::FromStart(1, Some(1))),
        ])?,
        vec![
            Some(vec![1, 2, 3].into()),
            Some(vec![1, 2].into()),
            Some(vec![1].into())
        ]
    );
    assert!(store
        .get_partial_values(&[StoreKeyRange::new(
            "a/b".try_into()?,
            ByteRange::FromStart(1, Some(10))
        ),])
        .is_err());

    Ok(())
}

pub fn store_list<T: ListableStorageTraits>(store: &T) -> Result<(), Box<dyn Error>> {
    assert_eq!(store.size()?, 7);
    assert_eq!(store.size_prefix(&"a/".try_into()?)?, 5);
    assert_eq!(store.size_prefix(&"i/".try_into()?)?, 2);

    assert_eq!(
        store.list()?,
        &[
            "a/b".try_into()?,
            "a/c".try_into()?,
            "a/d/e".try_into()?,
            "a/f/g".try_into()?,
            "a/f/h".try_into()?,
            "i/j/k".try_into()?
        ]
    );

    assert_eq!(
        store.list_prefix(&"".try_into()?)?,
        &[
            "a/b".try_into()?,
            "a/c".try_into()?,
            "a/d/e".try_into()?,
            "a/f/g".try_into()?,
            "a/f/h".try_into()?,
            "i/j/k".try_into()?
        ]
    );

    assert_eq!(
        store.list_prefix(&"a/".try_into()?)?,
        &[
            "a/b".try_into()?,
            "a/c".try_into()?,
            "a/d/e".try_into()?,
            "a/f/g".try_into()?,
            "a/f/h".try_into()?
        ]
    );
    assert_eq!(
        store.list_prefix(&"i/".try_into()?)?,
        &["i/j/k".try_into()?]
    );

    {
        let list_dir = store.list_dir(&"a/".try_into()?)?;
        assert_eq!(list_dir.keys(), &["a/b".try_into()?, "a/c".try_into()?,]);
        assert_eq!(
            list_dir.prefixes(),
            &["a/d/".try_into()?, "a/f/".try_into()?,]
        );
    }
    Ok(())
}

#[cfg(feature = "async")]
// Create a store with the following data
// - a/
//   - b [0, 1, 2, 3]
//   - c [0]
//   - d/
//     - e
//   - f/
//     - g
//     - h
// - i/
//   - j/
//     - k [0, 1]
pub async fn async_store_write<T: AsyncWritableStorageTraits>(
    store: &T,
) -> Result<(), Box<dyn Error>> {
    store.erase_prefix(&StorePrefix::root()).await?;

    store
        .set(&"a/b".try_into()?, vec![255, 255, 255].into())
        .await?;
    store
        .set_partial_values(&[StoreKeyStartValue::new("a/b".try_into()?, 1, &[1, 2])])
        .await?;
    store
        .set_partial_values(&[StoreKeyStartValue::new("a/b".try_into()?, 3, &[3])])
        .await?;
    store
        .set_partial_values(&[StoreKeyStartValue::new("a/b".try_into()?, 0, &[0])])
        .await?;

    store.set(&"a/c".try_into()?, vec![0].into()).await?;
    store.set(&"a/d/e".try_into()?, vec![].into()).await?;
    store.set(&"a/f/g".try_into()?, vec![].into()).await?;
    store.set(&"a/f/h".try_into()?, vec![].into()).await?;
    store.set(&"i/j/k".try_into()?, vec![0, 1].into()).await?;

    store.set(&"erase".try_into()?, vec![].into()).await?;
    store.erase(&"erase".try_into()?).await?;
    store.erase(&"erase".try_into()?).await?; // succeeds

    store
        .set(&"erase_values_0".try_into()?, vec![].into())
        .await?;
    store
        .set(&"erase_values_1".try_into()?, vec![].into())
        .await?;
    store
        .erase_values(&["erase_values_0".try_into()?, "erase_values_1".try_into()?])
        .await?;

    store
        .set(&"erase_prefix/0".try_into()?, vec![].into())
        .await?;
    store
        .set(&"erase_prefix/1".try_into()?, vec![].into())
        .await?;
    store.erase_prefix(&"erase_prefix/".try_into()?).await?;

    Ok(())
}

#[cfg(feature = "async")]
pub async fn async_store_read<T: AsyncReadableStorageTraits>(
    store: &T,
) -> Result<(), Box<dyn Error>> {
    assert!(store.get(&"notfound".try_into()?).await?.is_none());
    assert!(store.size_key(&"notfound".try_into()?).await?.is_none());
    assert_eq!(
        store.get(&"a/b".try_into()?).await?,
        Some(vec![0, 1, 2, 3].into())
    );
    assert_eq!(store.size_key(&"a/b".try_into()?).await?, Some(4));
    assert_eq!(store.size_key(&"a/c".try_into()?).await?, Some(1));
    assert_eq!(store.size_key(&"i/j/k".try_into()?).await?, Some(2));
    assert_eq!(
        store
            .get_partial_values_key(
                &"a/b".try_into()?,
                &[
                    ByteRange::FromStart(1, Some(1)),
                    ByteRange::FromEnd(0, Some(1))
                ]
            )
            .await?,
        Some(vec![vec![1].into(), vec![3].into()])
    );
    assert_eq!(
        store
            .get_partial_values(&[
                StoreKeyRange::new("a/b".try_into()?, ByteRange::FromStart(1, None)),
                StoreKeyRange::new("a/b".try_into()?, ByteRange::FromEnd(1, Some(2))),
                StoreKeyRange::new("i/j/k".try_into()?, ByteRange::FromStart(1, Some(1))),
            ])
            .await?,
        vec![
            Some(vec![1, 2, 3].into()),
            Some(vec![1, 2].into()),
            Some(vec![1].into())
        ]
    );
    assert!(store
        .get_partial_values(&[StoreKeyRange::new(
            "a/b".try_into()?,
            ByteRange::FromStart(1, Some(10))
        ),])
        .await
        .is_err());

    Ok(())
}

#[cfg(feature = "async")]
pub async fn async_store_list<T: AsyncListableStorageTraits>(
    store: &T,
) -> Result<(), Box<dyn Error>> {
    assert_eq!(store.size().await?, 7);
    assert_eq!(store.size_prefix(&"a/".try_into()?).await?, 5);
    assert_eq!(store.size_prefix(&"i/".try_into()?).await?, 2);

    assert_eq!(
        store.list().await?,
        &[
            "a/b".try_into()?,
            "a/c".try_into()?,
            "a/d/e".try_into()?,
            "a/f/g".try_into()?,
            "a/f/h".try_into()?,
            "i/j/k".try_into()?
        ]
    );

    assert_eq!(
        store.list_prefix(&"".try_into()?).await?,
        &[
            "a/b".try_into()?,
            "a/c".try_into()?,
            "a/d/e".try_into()?,
            "a/f/g".try_into()?,
            "a/f/h".try_into()?,
            "i/j/k".try_into()?
        ]
    );

    assert_eq!(
        store.list_prefix(&"a/".try_into()?).await?,
        &[
            "a/b".try_into()?,
            "a/c".try_into()?,
            "a/d/e".try_into()?,
            "a/f/g".try_into()?,
            "a/f/h".try_into()?
        ]
    );
    assert_eq!(
        store.list_prefix(&"i/".try_into()?).await?,
        &["i/j/k".try_into()?]
    );

    {
        let list_dir = store.list_dir(&"a/".try_into()?).await?;
        assert_eq!(list_dir.keys(), &["a/b".try_into()?, "a/c".try_into()?,]);
        assert_eq!(
            list_dir.prefixes(),
            &["a/d/".try_into()?, "a/f/".try_into()?,]
        );
    }
    Ok(())
}
