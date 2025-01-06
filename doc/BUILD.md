## Testing
```bash
make check
```
Runs:
- build (default features, all features, no default features)
- test
- clippy
- doc
- fmt (check)

Extra checks with `clippy::nursery` (can have false positives):
```bash
make check_extra
```

## Docs with Examples
```bash
make doc
```

## Performance
```bash
# Set a baseline
cargo bench -- --save-baseline baseline
# Implement changes and compare against baseline
cargo bench -- --baseline baseline
```

## Coverage (using [cargo-llvm-cov](https://crates.io/crates/cargo-llvm-cov))
Install `cargo-llvm-cov`
```bash
make coverage_install
```

Generate a HTML report
```bash
make coverage_report
```

Generate a coverage file for [Coverage Gutters](https://marketplace.visualstudio.com/items?itemName=ryanluker.vscode-coverage-gutters) in VSCode
```bash
make coverage_file
```

## [Miri](https://github.com/rust-lang/miri)
Tests that call foreign functions or access the filesystem are disabled.
The [inventory](https://crates.io/crates/inventory) crate does not work in miri, so there are workarounds in place for codecs, chunk key encodings, and chunk grids.
```bash
make miri
```
