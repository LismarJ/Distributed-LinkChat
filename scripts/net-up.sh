#!/usr/bin/env bash
# Create the one network every container attaches to.
#
# This is what replaces Docker Compose for us: on a user-defined network,
# containers resolve each other by container name through Docker's built-in
# DNS, so a node only needs to be told the head's name.
set -euo pipefail
source "$(dirname "$0")/env.sh"
require_docker

if docker network inspect "${NETWORK}" >/dev/null 2>&1; then
  echo "network ${NETWORK} already exists"
else
  docker network create "${NETWORK}" >/dev/null
  echo "created network ${NETWORK}"
fi
