# runner-hive on top of a runner image, so jobs get that image's toolchains.
#
#   docker build -f contrib/Dockerfile --build-arg BASE=ghcr.io/actions/actions-runner:latest -t runner-hive .
#
# BASE is any image with a `runner` user (the official actions-runner image, or
# your own built on it). The daemon runs as root and jobs as `runner`
# (worker.run_as), so jobs cannot read the admin token or registration keys.
ARG BASE=ghcr.io/actions/actions-runner:latest

FROM rust:1-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends musl-tools \
    && rm -rf /var/lib/apt/lists/* \
    && rustup target add x86_64-unknown-linux-musl
WORKDIR /src
COPY . .
RUN cargo build --release --locked --target x86_64-unknown-linux-musl

FROM ${BASE}
USER root
# A job that can become root can read the daemon's admin token and every
# registration key, which defeats worker.run_as. The official runner image
# gives `runner` passwordless sudo through the sudo group; drop it unless
# KEEP_SUDO=1 (only for jobs you trust with that).
ARG KEEP_SUDO=0
RUN if [ "$KEEP_SUDO" != 1 ] && id -nG runner 2>/dev/null | grep -qw sudo; then \
        gpasswd -d runner sudo >/dev/null; \
    fi
COPY --from=build /src/target/x86_64-unknown-linux-musl/release/runner-hive /usr/local/bin/runner-hive
ENTRYPOINT ["/usr/local/bin/runner-hive", "--config", "/etc/runner-hive/hive.toml"]
CMD ["run"]
