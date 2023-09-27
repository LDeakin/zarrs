## Building
```bash
cargo build --release
```

## Testing
```bash
# Must have no warnings/errors to pass CI
cargo build && \
cargo test && \
cargo doc && \
cargo fmt --all -- --check && \
cargo clippy -- -D warnings && \
cargo check --no-default-features
```

## Coverage report
```bash
# on ubuntu..
# apt install llvm-14 jq
# cargo install rustfilt
./coverage.sh
```
