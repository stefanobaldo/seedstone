# The server as a container image. Two stages: the first builds the binary
# from this tree with the lockfile it was tested with; the second is a
# distroless glibc base holding nothing but that binary. Building here rather
# than copying a binary in keeps the glibc the binary links against and the
# glibc the image ships the same one — Debian 12's on both sides.
#
# Both bases are pinned by the digest of their multi-architecture index, the
# rule the e2e lanes follow: a tag is a name its owner may move. The tag is
# kept beside each digest for a human reader.

FROM rust:1-bookworm@sha256:82150a52ec202c1b14d7817e14516c392bb7f5cfebd88f1ed531cb37ebd39922 AS builder
WORKDIR /src
COPY . .
RUN cargo build --release -p seedstone --locked

# `nonroot` runs the server as uid 65532. There is no shell in this image:
# `redis-cli` from any other container is how one talks to it.
FROM gcr.io/distroless/cc-debian12:nonroot@sha256:9dac0a79194e45a7da0158a9c6da57b217585af0786db3845d1f0ec1a0dd182f
COPY --from=builder /src/target/release/seedstone /usr/local/bin/seedstone
EXPOSE 6379
ENTRYPOINT ["/usr/local/bin/seedstone"]
