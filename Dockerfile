# Scorers are linux/amd64 Intel Xeon. No FROM line pins a platform.
FROM rust:bookworm AS build
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src/ src/
RUN cargo build --release --locked

FROM debian:bookworm-slim
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY --from=build /build/target/release/shopping-sessions /usr/local/bin/shopping-sessions
USER 1000:1000
CMD ["/usr/local/bin/shopping-sessions"]
