#!/bin/sh
# Build a durable fork image (upstream base + our binary). Run build-fork.sh
# first so decypharr_fork exists. Then point the Unraid template Repository at
# the printed tag and recreate the container — survives future recreations.
set -e
cd "$(dirname "$0")"
VERSION="$(cat FORK_VERSION)"
if [ ! -f decypharr_fork ]; then
  echo "decypharr_fork not found — run ./build-fork.sh first" >&2
  exit 1
fi
TAG="local/decypharr:fork-${VERSION}"
echo "==> Building image ${TAG}"
docker build -f Dockerfile.fork -t "${TAG}" -t local/decypharr:fork-latest .
echo "==> Built ${TAG}  (also tagged local/decypharr:fork-latest)"
echo "    Unraid: set the decypharr template Repository to ${TAG} and recreate."
