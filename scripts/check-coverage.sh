#!/usr/bin/env bash
# Run unit tests with coverage and fail if total coverage is below the threshold.
# Go has no native fail-under, so we parse `go tool cover -func`.
set -euo pipefail

THRESHOLD="${COVERAGE_THRESHOLD:-80}"
PROFILE="${COVERAGE_PROFILE:-coverage.out}"

go test -coverprofile="${PROFILE}" -covermode=atomic ./...

total="$(go tool cover -func="${PROFILE}" | awk '/^total:/ {sub(/%/, "", $3); print $3}')"

echo "Total coverage: ${total}% (threshold: ${THRESHOLD}%)"

# Compare as floats.
if awk -v t="${total}" -v thr="${THRESHOLD}" 'BEGIN { exit (t+0 >= thr+0) ? 0 : 1 }'; then
  echo "Coverage gate passed."
else
  echo "Coverage gate FAILED: ${total}% < ${THRESHOLD}%" >&2
  exit 1
fi
