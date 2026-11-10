#!/usr/bin/env bash
set -uo pipefail
namespace="$1"
selector="$2"
seconds="$3"
echo "\$ kubectl -n $namespace get pods -l $selector -o wide -w --output-watch-events  (${seconds}s)"
kubectl -n "$namespace" get pods -l "$selector" -o wide -w --output-watch-events --request-timeout="${seconds}s" 2>/dev/null | while IFS= read -r line; do printf '%s  %s\n' "$(date -u +%H:%M:%S)" "$line"; done
