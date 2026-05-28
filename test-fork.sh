#!/bin/sh
# Run the Go test suite for the packages the fork modifies, inside the same
# alpine toolchain used for builds.
set -e
cd "$(dirname "$0")"
PKGS="${1:-./pkg/manager/... ./pkg/warden/...}"
echo "==> go test ${PKGS}"
docker run --rm -v "$(pwd)":/app -w /app golang:1.25-alpine sh -c "
  apk add --no-cache gcc g++ musl-dev fuse-dev | tail -1 &&
  CGO_ENABLED=1 go test -count=1 ${PKGS}"
