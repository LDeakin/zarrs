FROM rust:latest

RUN apt update && apt install -y libblosc-dev

WORKDIR /usr/src/zarrs
COPY . .

RUN cargo build && cargo test && cargo doc
