use std::sync::Arc;

use zarrs::{node::Node, storage::store::FilesystemStore};

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
