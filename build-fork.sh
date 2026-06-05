#!/bin/sh
# Build the decypharr fork binary with an embedded fork version string.
# Persistent Go module + build caches (named docker volumes) so rebuilds do not
# re-download deps or recompile the world every time.
set -e
cd "$(dirname "$0")"
VERSION="$(cat FORK_VERSION)"
echo "==> Building decypharr fork ${VERSION} (channel=fork)"
LDFLAGS="-w -s -X github.com/sirrobot01/decypharr/pkg/version.Version=${VERSION} -X github.com/sirrobot01/decypharr/pkg/version.Channel=fork"
docker run --rm \
  -v "$(pwd)":/app -w /app \
  -v decypharr-gomod:/go/pkg/mod \
  -v decypharr-gobuild:/root/.cache/go-build \
  golang:1.26-alpine sh -c "
    apk add --no-cache gcc g++ musl-dev fuse-dev | tail -1 &&
    CGO_ENABLED=1 go build -trimpath -ldflags=\"${LDFLAGS}\" -o /app/decypharr_fork ."
echo "==> Built ./decypharr_fork  (${VERSION}-fork)"
# Keep the working tree SMB-writable (root-created files default to 0644/0755)
# so Edit/Write over the Z: share keep working without a manual chmod.
chmod -R a+rwX . 2>/dev/null || true
