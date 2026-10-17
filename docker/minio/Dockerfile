# MinIO built from its last tagged release. The upstream repository was archived
# in April 2026 and minio/minio is no longer published on Docker Hub, so the
# image is built here from source, pinned to the release tag and its commit.
FROM golang:1.24.8-bookworm AS build

ARG MINIO_TAG=RELEASE.2025-10-15T17-29-55Z
ARG MINIO_COMMIT=9e49d5e7a648f00e26f2246f4dc28e6b07f8c84a
WORKDIR /src
RUN set -eux; \
    git clone --depth 1 --branch "$MINIO_TAG" https://github.com/minio/minio.git .; \
    test "$(git rev-parse HEAD)" = "$MINIO_COMMIT"
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    set -eux; \
    CGO_ENABLED=0 go build -tags kqueue -trimpath \
        -ldflags "$(MINIO_RELEASE=RELEASE go run buildscripts/gen-ldflags.go "${MINIO_TAG#RELEASE.}")" \
        -o /out/minio .

FROM debian:bookworm-slim
# curl serves the health check and the one-shot bucket creation (curl --aws-sigv4).
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 1000 --home-dir /data minio \
    && mkdir -p /data && chown minio:minio /data
COPY --from=build /out/minio /usr/local/bin/minio
USER minio
EXPOSE 9000 9001
VOLUME ["/data"]
ENTRYPOINT ["minio"]
CMD ["server", "/data", "--console-address", ":9001"]
