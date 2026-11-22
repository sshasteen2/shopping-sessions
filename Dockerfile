# Scorers are linux/amd64 Intel Xeon.
FROM rust:bookworm AS build
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src/ src/
ENV RUSTFLAGS="-C target-cpu=x86-64-v3 -C llvm-args=-inline-threshold=1000"
RUN cargo build --release --locked

FROM debian:bookworm-slim
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY --from=build /build/target/release/shopping-sessions /usr/local/bin/shopping-sessions
USER 1000:1000
CMD ["/usr/local/bin/shopping-sessions"]
