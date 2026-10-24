FROM rust:1.94.1-bookworm AS build
# prost-build needs protoc; the rust image already carries the C toolchain.
RUN apt-get update \
    && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY Cargo.toml Cargo.lock build.rs ./
COPY proto ./proto
COPY src ./src
RUN cargo build --release --locked

FROM debian:bookworm-slim
COPY --from=build /build/target/release/selene /usr/local/bin/selene
ENTRYPOINT ["/usr/local/bin/selene"]
