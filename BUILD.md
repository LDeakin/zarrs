## Building
```bash
cargo build --release
```

## Testing
```bash
# Must have no warnings/errors to pass CI
cargo build --all-features && \
cargo test --all-features && \
cargo +nightly doc --all-features && \
cargo fmt --all -- --check && \
cargo clippy --all-features -- -D warnings && \
cargo check && \
cargo check --no-default-features
```

```bash
# Additional checks
cargo clippy --all-features -- -D warnings -W clippy::nursery -A clippy::significant_drop_tightening -A clippy::significant_drop_in_scrutinee
# cargo clippy --all-features -- -D warnings -W clippy::unwrap_used -W clippy::expect_used
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
cargo +stable install cargo-llvm-cov --locked
```

Generate a HTML report
```bash
cargo +nightly llvm-cov --doctests --html
open target/llvm-cov/html/index.html
```

Generate a coverage file for [Coverage Gutters](https://marketplace.visualstudio.com/items?itemName=ryanluker.vscode-coverage-gutters) in VSCode
```bash
cargo +nightly llvm-cov --doctests --lcov --output-path lcov.info
```
