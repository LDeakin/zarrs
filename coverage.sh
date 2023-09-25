#!/bin/bash
shopt -s expand_aliases

alias llvm-cov=llvm-cov-14
alias llvm-profdata=llvm-profdata-14

# export RUSTFLAGS="-C instrument-coverage"
export RUSTFLAGS="-Zunstable-options -C instrument-coverage=except-unused-functions"
export RUSTDOCFLAGS="-C instrument-coverage -Z unstable-options --persist-doctests target/debug/doctestbins"
cargo +nightly test
tests=$( cargo +nightly test --no-run --message-format=json \
        | jq -r "select(.profile.test == true) | .filenames[]" \
        | grep -v dSYM - \
)
llvm-profdata merge -sparse default_*.profraw -o coverage.profdata
rm *.profraw
rm ../*.profraw || true # Sometimes these are created?

objects=$( for file in $tests; do printf "%s %s " -object $file; done )

llvm-cov report $objects \
  --ignore-filename-regex='(/tests/|rustc/)' \
  --use-color \
  --instr-profile=coverage.profdata \
  --ignore-filename-regex='/.cargo/registry'

llvm-cov show $objects \
  --ignore-filename-regex='(/tests/|rustc/)' \
  --use-color \
  --instr-profile=coverage.profdata \
  --ignore-filename-regex='/.cargo/registry' \
  --show-instantiations \
  --show-line-counts-or-regions \
  --Xdemangler=rustfilt \
  | less -R

rm coverage.profdata
