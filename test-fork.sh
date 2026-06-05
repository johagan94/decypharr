#!/bin/sh
# Run the fork test suite (race detector) with persistent Go caches.
# Usage: ./test-fork.sh [packages]   default: manager + warden + storage/hybrid
set -e
cd "$(dirname "$0")"
PKGS="${1:-./pkg/manager/... ./pkg/warden/... ./pkg/storage/hybrid/...}"
echo "==> go test -race ${PKGS}"
docker run --rm \
  -v "$(pwd)":/app -w /app \
  -v decypharr-gomod:/go/pkg/mod \
  -v decypharr-gobuild:/root/.cache/go-build \
  golang:1.25-alpine sh -c "
    apk add --no-cache gcc g++ musl-dev fuse-dev | tail -1 &&
    CGO_ENABLED=1 go test -race -count=1 ${PKGS}"
