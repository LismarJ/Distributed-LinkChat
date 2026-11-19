#!/usr/bin/env bash
# Everything CI runs, so that CI never tells you something you could have seen.
set -euo pipefail
cd "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

run() {
  echo
  echo "=== $* ==="
  "$@"
}

if command -v uv >/dev/null 2>&1; then
  run uv run ruff check .
  run uv run ruff format --check .
  run uv run mypy
  run uv run pytest
else
  echo "uv is not installed; falling back to whatever is on PATH." >&2
  echo "Install it from https://docs.astral.sh/uv/ for reproducible runs." >&2
  run ruff check .
  run ruff format --check .
  run mypy
  run pytest
fi

echo
echo "all checks passed"
