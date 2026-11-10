#!/usr/bin/env bash
set -euo pipefail
size=847619384
run() { echo "\$ $*"; "$@"; echo; }
run kubectl -n hh-lake get sts,pod,svc,pvc -o wide
run kubectl -n hh-lake exec rustfs-0 -- du -sk /data/disk1 /data/disk2 /data/disk3 /data/disk4
echo "\$ bytes of raw/it_vacancies_2024.csv shards per disk"
total=0
for d in 1 2 3 4; do
  bytes=$(kubectl -n hh-lake exec rustfs-0 -- sh -c "find /data/disk$d/hh-lake/raw/it_vacancies_2024.csv -type f -name 'part.*' ! -name '*.meta' -exec cat {} + | wc -c")
  total=$((total + bytes))
  awk -v d="$d" -v b="$bytes" -v s="$size" 'BEGIN { printf "disk%s %d B  share %.4f of object\n", d, b, b / s }'
done
awk -v t="$total" -v s="$size" 'BEGIN { printf "total %d B  overhead %.4fx\n", t, t / s }'
