#![cfg(feature = "filesystem")]

use std::sync::Arc;

use zarrs::{
    filesystem::FilesystemStore, group::Group, metadata::v3::group::ConsolidatedMetadata,
    node::Node,
};

#[test]
fn hierarchy_tree() {
    let store = Arc::new(
        FilesystemStore::new("./tests/data/hierarchy.zarr")
            .unwrap()
            .sorted(),
    );
    let node = Node::open(&store, "/").unwrap();
    let tree = node.hierarchy_tree();
    println!("{:?}", tree);
    assert_eq!(
        tree,
        "/
  a
    baz [10000, 1000] float64
    foo [10000, 1000] float64
  b
"
    );
}

#[test]
fn consolidated_metadata() {
    let store = Arc::new(
        FilesystemStore::new("./tests/data/hierarchy.zarr")
            .unwrap()
            .sorted(),
    );
    let node = Node::open(&store, "/").unwrap();
    let consolidated_metadata = node.consolidate_metadata().unwrap();
    println!("{:#?}", consolidated_metadata);

    for relative_path in ["a", "a/baz", "a/foo", "b"] {
        let consolidated = consolidated_metadata.get(relative_path).unwrap();
        let node_path = format!("/{}", relative_path);
        let actual = Node::open(&store, &node_path).unwrap();
        assert_eq!(consolidated, actual.metadata());
    }

    let mut group = Group::open(store.clone(), "/").unwrap();
    assert!(group.consolidated_metadata().is_none());
    group.set_consolidated_metadata(Some(ConsolidatedMetadata {
        metadata: consolidated_metadata,
        ..Default::default()
    }));
    assert!(group.consolidated_metadata().is_some());

    let node = Node::open(&store, "/a").unwrap();
    let consolidated_metadata = node.consolidate_metadata().unwrap();
    println!("{:#?}", consolidated_metadata);
    for relative_path in ["baz", "foo"] {
        let consolidated = consolidated_metadata.get(relative_path).unwrap();
        let node_path = format!("/a/{}", relative_path);
        let actual = Node::open(&store, &node_path).unwrap();
        assert_eq!(consolidated, actual.metadata());
    }
}
