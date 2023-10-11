## Building
```bash
cargo build --release
```

## Testing
```bash
# Must have no warnings/errors to pass CI
cargo build --all-features && \
cargo test --all-features && \
cargo doc --all-features && \
cargo fmt --all -- --check && \
cargo clippy -- -D warnings && \
cargo check && \
cargo check --no-default-features
```

## Performance
```bash
# Set a baseline
cargo bench -- --save-baseline baseline
# Implement changes and compare against baseline
cargo bench -- --baseline baseline
```

## Coverage report
```bash
# on ubuntu..
# apt install llvm-14 jq
# cargo install rustfilt
./coverage.sh
```
