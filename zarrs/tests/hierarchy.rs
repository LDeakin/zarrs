#![cfg(feature = "filesystem")]
#![allow(missing_docs)]

use std::sync::Arc;

use zarrs::{filesystem::FilesystemStore, group::Group, node::Node};
use zarrs_metadata_ext::group::consolidated_metadata::ConsolidatedMetadata;

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

#[test]
fn child_arrays() {
    let store = Arc::new(
        FilesystemStore::new("./tests/data/hierarchy.zarr")
            .unwrap()
            .sorted(),
    );

    // Two arrays in /a
    let group = Group::open(store.clone(), "/a").unwrap();
    let arrays = group.child_arrays().unwrap();
    let array_paths: Vec<_> = arrays.iter().map(|a| a.path().as_str()).collect();
    assert_eq!(array_paths, ["/a/baz", "/a/foo"]);

    // At root, there are no arrays
    let group = Group::open(store.clone(), "/").unwrap();
    let arrays = group.child_arrays().unwrap();
    assert!(arrays.is_empty());
}

#[test]
fn child_groups() {
    let store = Arc::new(
        FilesystemStore::new("./tests/data/hierarchy.zarr")
            .unwrap()
            .sorted(),
    );

    // At root, there are two groups: a and b
    let group = Group::open(store.clone(), "/").unwrap();
    let groups = group.child_groups().unwrap();
    let group_paths: Vec<_> = groups.iter().map(|g| g.path().as_str()).collect();
    assert_eq!(group_paths, ["/a", "/b"]);

    // In /a, there are no child groups (only arrays)
    let group = Group::open(store.clone(), "/a").unwrap();
    let groups = group.child_groups().unwrap();
    assert!(groups.is_empty());
}

#[test]
fn child_paths() {
    let store = Arc::new(
        FilesystemStore::new("./tests/data/hierarchy.zarr")
            .unwrap()
            .sorted(),
    );

    // At root, there are two child paths: a and b (both groups)
    let group = Group::open(store.clone(), "/").unwrap();
    let paths = group.child_paths().unwrap();
    let path_strings: Vec<_> = paths.iter().map(|p| p.as_str()).collect();
    assert_eq!(path_strings, ["/a", "/b"]);

    // In /a, there are two child paths: baz and foo (both arrays)
    let group = Group::open(store.clone(), "/a").unwrap();
    let paths = group.child_paths().unwrap();
    let path_strings: Vec<_> = paths.iter().map(|p| p.as_str()).collect();
    assert_eq!(path_strings, ["/a/baz", "/a/foo"]);
}

#[test]
fn child_group_paths() {
    let store = Arc::new(
        FilesystemStore::new("./tests/data/hierarchy.zarr")
            .unwrap()
            .sorted(),
    );

    // At root, there are two group paths: a and b
    let group = Group::open(store.clone(), "/").unwrap();
    let paths = group.child_group_paths().unwrap();
    let path_strings: Vec<_> = paths.iter().map(|p| p.as_str()).collect();
    assert_eq!(path_strings, ["/a", "/b"]);

    // In /a, there are no child group paths (only arrays)
    let group = Group::open(store.clone(), "/a").unwrap();
    let paths = group.child_group_paths().unwrap();
    assert!(paths.is_empty());
}

#[test]
fn child_array_paths() {
    let store = Arc::new(
        FilesystemStore::new("./tests/data/hierarchy.zarr")
            .unwrap()
            .sorted(),
    );

    // At root, there are no array paths (only groups)
    let group = Group::open(store.clone(), "/").unwrap();
    let paths = group.child_array_paths().unwrap();
    assert!(paths.is_empty());

    // In /a, there are two array paths: baz and foo
    let group = Group::open(store.clone(), "/a").unwrap();
    let paths = group.child_array_paths().unwrap();
    let path_strings: Vec<_> = paths.iter().map(|p| p.as_str()).collect();
    assert_eq!(path_strings, ["/a/baz", "/a/foo"]);
}
