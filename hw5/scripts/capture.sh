#!/usr/bin/env bash
set -uo pipefail
namespace="$1"
job="$2"
run() { echo "\$ $*"; "$@" 2>&1; echo; }
run kubectl -n "$namespace" get job "$job" -o wide
run kubectl -n "$namespace" describe job "$job"
run kubectl -n "$namespace" get pods -l "batch.kubernetes.io/job-name=$job" -o wide
for pod in $(kubectl -n "$namespace" get pods -l "batch.kubernetes.io/job-name=$job" -o name); do
  run kubectl -n "$namespace" describe "$pod"
  run kubectl -n "$namespace" get "$pod" -o jsonpath='{range .status.containerStatuses[*]}{.name} reason={.state.terminated.reason} exitCode={.state.terminated.exitCode} started={.state.terminated.startedAt} finished={.state.terminated.finishedAt}{"\n"}{end}{range .status.conditions[*]}{.type}={.status} {.reason}{"\n"}{end}'
  run kubectl -n "$namespace" logs "$pod"
done
echo "\$ kubectl -n $namespace get events --sort-by=.lastTimestamp -o custom-columns=... | grep -E 'LAST|$job'"
kubectl -n "$namespace" get events --sort-by=.lastTimestamp -o custom-columns=LAST:.lastTimestamp,TYPE:.type,REASON:.reason,OBJECT:.involvedObject.name,MESSAGE:.message | grep -E "LAST|$job"
