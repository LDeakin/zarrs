## Dependencies
This crate currently has the following C dependencies wrapped by `-sys` crates:
- Zstd [`zstd-sys`]: builds from source, and
- [Blosc](https://www.blosc.org/) [`blosc-sys`]: does **not** build from source
  - Blosc is available through most package managers on linux (e.g. `libblosc-dev` on Ubuntu).

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
