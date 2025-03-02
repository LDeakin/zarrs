TOOLCHAIN ?= nightly

all: build

build:
	cargo +$(TOOLCHAIN) build --all-features

test:
	cargo +$(TOOLCHAIN) test --all-features
	cargo +$(TOOLCHAIN) test --all-features --examples

doc: RUSTDOCFLAGS="-D warnings --cfg docsrs"
doc:
	cargo +$(TOOLCHAIN) doc -Z unstable-options -Z rustdoc-scrape-examples --all-features --no-deps

clippy:
	cargo +$(TOOLCHAIN) clippy --all-features -- -D warnings

check: build test clippy doc
	cargo +$(TOOLCHAIN) fmt --all -- --check
	cargo +$(TOOLCHAIN) check
	cargo +$(TOOLCHAIN) check --no-default-features

check_extra:
	cargo +$(TOOLCHAIN) clippy --all-features -- -D warnings -W clippy::nursery -A clippy::significant_drop_tightening -A clippy::significant_drop_in_scrutinee

# `-Zmiri-ignore-leaks` is needed for multi-threaded programs... https://github.com/rust-lang/miri/issues/1371
miri: MIRIFLAGS="-Zmiri-disable-isolation -Zmiri-ignore-leaks -Zmiri-tree-borrows"
miri:
	cargo +$(TOOLCHAIN) miri test -p zarrs --all-features

coverage_install:
	cargo install cargo-llvm-cov --locked

coverage_report:
	cargo +$(TOOLCHAIN) llvm-cov --all-features --doctests --html

coverage_file:
	cargo +$(TOOLCHAIN) llvm-cov --all-features --doctests --lcov --output-path lcov.info

fmt:
	cargo +$(TOOLCHAIN) fmt

clean:
	cargo clean

.PHONY: all build test doc clippy check fmt clean
