#!/usr/bin/env bash
# hollo-stream-proxy: Buildah build & push (multi-arch: amd64 + arm64)
#
# Usage:
#   ./build.sh                # version from package.json
#   VERSION=xxx ./build.sh    # override version
#
# Prerequisites:
#   buildah login nrt.vultrcr.com

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

VERSION="${VERSION:-$(jq -r '.version' ./package.json)}"
REGISTRY="nrt.vultrcr.com"
NAMESPACE="ntlab1"
IMAGE_NAME="hollo-stream-proxy"
FULL_IMAGE="${REGISTRY}/${NAMESPACE}/${IMAGE_NAME}:${VERSION}"
PLATFORMS=("linux/amd64" "linux/arm64")

echo "=== Cleaning up existing image/manifest ==="
buildah manifest rm "${FULL_IMAGE}" 2>/dev/null || true
buildah rmi "${FULL_IMAGE}" 2>/dev/null || true

echo "=== Creating manifest ${FULL_IMAGE} ==="
buildah manifest create "${FULL_IMAGE}"

for PLATFORM in "${PLATFORMS[@]}"; do
  echo "=== Building for ${PLATFORM} ==="
  buildah build --platform "${PLATFORM}" --manifest "${FULL_IMAGE}" .
done

echo "=== Pushing manifest ${FULL_IMAGE} (all platforms) ==="
buildah manifest push --all "${FULL_IMAGE}" "docker://${FULL_IMAGE}"

echo "=== Done: ${FULL_IMAGE} ==="
