#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
namespace="${1:-hh-lake}"
env_file="$(mktemp)"
trap 'rm -f "$env_file"' EXIT
grep -E '^RUSTFS_(ACCESS|SECRET)_KEY=' .env > "$env_file"
kubectl create namespace "$namespace" --dry-run=client -o yaml | kubectl apply -f -
kubectl -n "$namespace" create secret generic hh-lake-s3 --from-env-file="$env_file" --dry-run=client -o yaml | kubectl apply -f -
