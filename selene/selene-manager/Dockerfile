# Build from the Flare repository root:
#   docker build -f selene/selene-manager/Dockerfile -t selene .
FROM rust:1.94.1-bookworm AS build
# prost-build needs protoc; the rust image already carries the C toolchain.
RUN apt-get update \
    && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY eos/Cargo.toml ./eos/
COPY eos/src ./eos/src
COPY selene/selene/Cargo.toml selene/selene/Cargo.lock selene/selene/build.rs ./selene/selene/
COPY selene/selene/src ./selene/selene/src
COPY selene/selene/proto ./selene/selene/proto
COPY selene/selene-manager/Cargo.toml selene/selene-manager/Cargo.lock selene/selene-manager/build.rs ./selene/selene-manager/
COPY selene/selene-manager/proto ./selene/selene-manager/proto
COPY selene/selene-manager/src ./selene/selene-manager/src
WORKDIR /build/selene/selene-manager
RUN cargo build --release --locked

FROM debian:bookworm-slim
COPY --from=build /build/selene/selene-manager/target/release/selene /usr/local/bin/selene
ENTRYPOINT ["/usr/local/bin/selene"]
