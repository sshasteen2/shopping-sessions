# Scorers are linux/amd64 Intel Xeon.
FROM rust:bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends musl-tools \
    && rm -rf /var/lib/apt/lists/*
RUN rustup target add x86_64-unknown-linux-musl
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src/ src/
ENV RUSTFLAGS="-C target-cpu=x86-64-v3"
RUN cargo build --release --locked --target x86_64-unknown-linux-musl

FROM scratch
COPY --from=build /build/target/x86_64-unknown-linux-musl/release/shopping-sessions /shopping-sessions
USER 1000:1000
ENTRYPOINT ["/shopping-sessions"]
