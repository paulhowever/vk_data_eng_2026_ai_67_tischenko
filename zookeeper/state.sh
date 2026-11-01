#!/usr/bin/env bash
set -u
run=${1:?usage: state.sh <run>}
root=/hhlake/$run
zkq() {
  echo "\$ zkCli.sh -server zk1:2181 $*"
  docker exec hh-zk1 zkCli.sh -server zk1:2181 "$@" 2>&1 | grep -v -e ' INFO ' -e '^WATCHER::' -e '^WatchedEvent' -e '^$'
  echo
}
zkq ls "$root"
zkq stat "$root/result"
zkq get "$root/result"
zkq stat "$root/epoch"
zkq get "$root/epoch"
zkq ls "$root/election"
zkq ls "$root/done"
