#!/usr/bin/env bash
# Build the one image all three roles run from.
set -euo pipefail
source "$(dirname "$0")/env.sh"
require_docker

cd "${REPO_ROOT}"
docker build --tag "${IMAGE}" .

echo
echo "built ${IMAGE}"
docker image inspect "${IMAGE}" --format 'size: {{.Size}} bytes'
echo "(image size is item 7 of the spike: record it in docs/research/07-ray.md)"
