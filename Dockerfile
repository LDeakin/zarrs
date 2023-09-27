FROM rust:latest

WORKDIR /usr/src/zarrs
COPY . .

RUN cargo build && cargo test && cargo doc
