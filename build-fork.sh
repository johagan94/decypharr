#!/bin/sh
# Build the decypharr fork binary with an embedded fork version string so the
# running binary self-reports which fork build it is (helps rollback/debug).
# Usage: ./build-fork.sh
set -e
cd "$(dirname "$0")"
VERSION="$(cat FORK_VERSION)"
echo "==> Building decypharr fork ${VERSION} (channel=fork)"
docker run --rm -v "$(pwd)":/app -w /app golang:1.25-alpine sh -c "
  apk add --no-cache gcc g++ musl-dev fuse-dev | tail -1 &&
  CGO_ENABLED=1 go build -trimpath \
    -ldflags=\"-w -s \
      -X github.com/sirrobot01/decypharr/pkg/version.Version=${VERSION} \
      -X github.com/sirrobot01/decypharr/pkg/version.Channel=fork\" \
    -o /app/decypharr_fork ."
echo "==> Built ./decypharr_fork  (${VERSION}-fork)"
