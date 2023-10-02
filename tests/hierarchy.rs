use zarrs::{node::Node, storage::store::FilesystemStore};

#[test]
fn hierarchy_tree() {
    let store = FilesystemStore::new("./tests/data/hierarchy.zarr")
        .unwrap()
        .sorted();
    let node = Node::new_with_store(&store, "/").unwrap();
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
