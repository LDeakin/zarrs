## Building
```bash
cargo build --release
```

## Testing
```bash
# Must have no warnings/errors to pass CI
cargo build --all-features && \
cargo test --all-features && \
RUSTDOCFLAGS="--cfg docsrs" cargo +nightly doc --all-features && \
cargo fmt --all -- --check && \
cargo +nightly clippy --all-features -- -D warnings && \
cargo check && \
cargo check --no-default-features
```

```bash
# Additional checks
cargo +nightly clippy --all-features -- -D warnings -W clippy::nursery -A clippy::significant_drop_tightening -A clippy::significant_drop_in_scrutinee
# cargo clippy --all-features -- -D warnings -W clippy::unwrap_used -W clippy::expect_used
```

## Docs with Examples
```bash
cargo +nightly doc -Z unstable-options -Z rustdoc-scrape-examples --all-features
```

## Performance
```bash
# Set a baseline
cargo bench -- --save-baseline baseline
# Implement changes and compare against baseline
cargo bench -- --baseline baseline
```

## Coverage report (using [cargo-llvm-cov](https://crates.io/crates/cargo-llvm-cov))

Install `cargo-llvm-cov`
```bash
cargo +nightly install cargo-llvm-cov --locked
```

Generate a HTML report
```bash
cargo +nightly llvm-cov --all-features --doctests --html
open target/llvm-cov/html/index.html
```

Generate a coverage file for [Coverage Gutters](https://marketplace.visualstudio.com/items?itemName=ryanluker.vscode-coverage-gutters) in VSCode
```bash
cargo +nightly llvm-cov --all-features --doctests --lcov --output-path lcov.info
```

## [Miri](https://github.com/rust-lang/miri)
Tests which call foreign functions or access the filesystem are disabled.
The [inventory](https://crates.io/crates/inventory) crate does not work in miri, so there are workarounds in place for codecs, chunk key encodings, and chunk grids.
```bash
# `-Zmiri-ignore-leaks` is needed for multi-threaded programs... https://github.com/rust-lang/miri/issues/1371
MIRIFLAGS="-Zmiri-disable-isolation -Zmiri-permissive-provenance -Zmiri-ignore-leaks -Zmiri-tree-borrows" cargo +nightly miri test --all-features
```
