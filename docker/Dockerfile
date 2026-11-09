FROM rust:1.99-slim

RUN rustup toolchain install nightly --profile minimal --component miri,rust-src \
    && cargo +nightly miri setup
