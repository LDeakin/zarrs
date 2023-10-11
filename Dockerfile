FROM rust:latest

RUN apt update
RUN apt install -y cmake clang-15

WORKDIR /usr/src/zarrs
COPY . .

RUN cargo build --all-features && \
    cargo test --all-features && \
    cargo doc --all-features && \
    cargo check && \
    cargo check --no-default-features
