#!/bin/sh
# Copy the freshly-built fork binary into the running decypharr container and
# restart it. Run build-fork.sh first.
set -e
cd "$(dirname "$0")"
VERSION="$(cat FORK_VERSION)"
echo "==> Deploying ${VERSION}-fork into container 'decypharr'"
docker cp ./decypharr_fork decypharr:/usr/bin/decypharr
docker restart decypharr
echo "==> Restarted. Verify:  curl -s http://localhost:8282/api/version"
