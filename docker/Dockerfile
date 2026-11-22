FROM rust:1.100-slim

RUN rustup component add clippy llvm-tools-preview \
    && cargo install cargo-llvm-cov --locked \
    && rustup toolchain install nightly --profile minimal --component miri,rust-src \
    && cargo +nightly miri setup
