#!/usr/bin/env bash
set -u
cd "$(dirname "$0")"
OUT=../hw4/outputs
mkdir -p "$OUT"
NET=hh-zk_default
TMP=$(mktemp -d)

stamp() { python3 -c 'import datetime;print(datetime.datetime.now().strftime("%H:%M:%S.%f")[:-3])'; }
port() { echo $((2180 + ${1#zk})); }
now() { python3 -c 'import time;print(f"{time.time():.3f}")'; }
show() { echo "[$(stamp)] \$ $*"; "$@" 2>&1; echo; }
zkcli() {
  local s=$1; shift
  echo "[$(stamp)] \$ zkCli.sh -server $s:$(port $s) $*"
  docker exec "hh-$s" zkCli.sh -server "$s:$(port $s)" "$@" 2>&1 | grep -v -e ' INFO ' -e '^WATCHER::' -e '^WatchedEvent' -e '^$'
  echo
}
status() {
  for s in zk1 zk2 zk3; do
    state=$(docker inspect -f '{{.State.Status}}' "hh-$s")
    if [ "$state" != running ]; then
      echo "[$(stamp)] \$ docker inspect -f '{{.State.Status}}' hh-$s"
      echo "$state"
      echo
      continue
    fi
    echo "[$(stamp)] \$ docker exec hh-$s zkServer.sh status"
    docker exec "hh-$s" zkServer.sh status 2>&1 | grep -v -e '^ZooKeeper JMX' -e '^Using config'
    echo
  done
}
mode() { docker exec "hh-$1" zkServer.sh status 2>/dev/null | sed -n 's/^Mode: //p'; }
leader() { for s in "$@"; do [ "$(mode "$s")" = leader ] && echo "$s"; done; }
serving() { for s in "$@"; do [ -n "$(mode "$s")" ] || return 1; done; }
alone() {
  local s=$1; shift
  echo "[$(stamp)] \$ timeout 20 zkCli.sh -server $s:$(port $s) $*"
  docker exec "hh-$s" timeout 20 zkCli.sh -server "$s:$(port $s)" "$@" 2>&1 \
    | grep -v -e ' INFO ' -e '^$' | grep -e 'KeeperErrorCode' -e 'Created' -e 'written' | sort -u
  echo "код выхода zkCli: ${PIPESTATUS[0]}"
  echo
}
elected() { docker logs "hh-$1" 2>&1 | grep 'LEADER ELECTION TOOK' | tail -1 | sed -E 's/^([^ ]+ [^ ]+) .*QuorumPeer\[myid=([0-9]+)\].* - ([A-Z]+ - LEADER ELECTION TOOK.*)$/\1 (UTC) myid=\2 \3/'; }
ready() { serving zk1 zk2 zk3 && [ -n "$(leader zk1 zk2 zk3)" ]; }

tries=0
until ready; do
  tries=$((tries + 1))
  [ "$tries" -le 240 ] || { echo "ансамбль так и не начал обслуживать запросы" >&2; exit 1; }
  sleep 0.5
done
echo "ансамбль обслуживает запросы, лидер $(leader zk1 zk2 zk3)"

{
  echo "== Java локально"
  echo "[$(stamp)] \$ echo \$JAVA_HOME"
  echo "${JAVA_HOME:?}"
  echo
  show "${JAVA_HOME:?}/bin/java" -version
  show which java
  show java -version
  echo "== Docker"
  show docker version --format '{{.Server.Version}}'
  echo "== Java и ZooKeeper в образе zookeeper:3.9.6"
  show docker exec hh-zk1 bash -c 'echo JAVA_HOME=$JAVA_HOME; java -version'
  show docker exec hh-zk1 zkServer.sh version
} > "$OUT/stage1_java.txt"

{
  echo "== Конфигурации: строки server.N одинаковые, clientPort, порты кворума и dataDir у каждого свои"
  for s in zk1 zk2 zk3; do
    show cat "$s/conf/zoo.cfg"
    show cat "$s/data/myid"
  done
  show diff zk1/conf/zoo.cfg zk2/conf/zoo.cfg
  show diff zk1/conf/zoo.cfg zk3/conf/zoo.cfg
  echo "== Запуск: каждый контейнер выполняет штатную команду zkServer.sh start-foreground"
  show docker compose ps --format 'table {{.Name}}\t{{.Command}}\t{{.Status}}\t{{.Ports}}'
  show docker exec hh-zk1 sh -c 'ps -o pid,user,args -p 1 | sed -E "s/-cp [^ ]+/-cp .../"'
  echo "== Роли"
  status
  for s in zk1 zk2 zk3; do show sh -c "echo srvr | nc -w 2 127.0.0.1 $(port $s)"; done
} > "$OUT/stage2_roles.txt"

{
  first=$(leader zk1 zk2 zk3)
  [ -n "$first" ] || { echo "нет лидера перед остановкой" >&2; exit 1; }
  rest=$(for s in zk1 zk2 zk3; do [ "$s" != "$first" ] && echo "$s"; done | xargs)
  echo "== Останавливаю лидера $first, остаются $rest"
  t0=$(now)
  show docker compose stop "$first"
  until [ -n "$(leader $rest)" ]; do sleep 0.2; done
  t1=$(now)
  next=$(leader $rest)
  echo "zkServer.sh status показал нового лидера $next не позже чем через $(python3 -c "print(f'{$t1-$t0:.1f}')") с после начала docker compose stop"
  echo
  for s in $rest; do
    echo "[$(stamp)] \$ docker logs hh-$s | grep 'LEADER ELECTION TOOK' | tail -1"
    elected "$s"
    echo
  done
  show docker compose ps -a --format 'table {{.Name}}\t{{.Status}}'
  status
  a=${rest%% *}; b=${rest##* }
  zkcli "$a" create /minus1 "written-with-2-of-3"
  zkcli "$b" get /minus1
  zkcli "$b" ls /
  echo "$first" > "$TMP/first"
} > "$OUT/stage2_minus1.txt"

{
  first=$(cat "$TMP/first")
  rest=$(for s in zk1 zk2 zk3; do [ "$s" != "$first" ] && echo "$s"; done | xargs)
  second=$(leader $rest)
  last=$(for s in $rest; do [ "$s" != "$second" ] && echo "$s"; done)
  echo "== Останавливаю второй сервер $second (текущий лидер), остаётся один $last"
  show docker compose stop "$second"
  sleep 3
  show docker compose ps -a --format 'table {{.Name}}\t{{.Status}}'
  status
  show sh -c "echo srvr | nc -w 2 127.0.0.1 $(port $last)"
  show sh -c "echo ruok | nc -w 2 127.0.0.1 $(port $last); echo"
  alone "$last" get /minus1
  alone "$last" create /minus2 x
  echo "$first $second" > "$TMP/stopped"
} > "$OUT/stage2_minus2.txt"

{
  read -r first second < "$TMP/stopped"
  echo "== Запускаю оба остановленных сервера: $first $second"
  show docker compose start "$first" "$second"
  until serving zk1 zk2 zk3; do sleep 0.5; done
  show docker compose ps -a --format 'table {{.Name}}\t{{.Status}}'
  status
  zkcli "$first" get /minus1
  zkcli "$second" get /minus1
  zkcli zk1 delete /minus1
} > "$OUT/stage2_restart.txt"

{
  echo "== Эфемерный узел: окно 2 создаёт узел, окно 1 смотрит"
  zkcli zk1 create /demo
  fifo=$(mktemp -u)
  mkfifo "$fifo"
  log=$(mktemp)
  docker run --rm -i --name hh-zk-window2 --network "$NET" zookeeper:3.9.6 zkCli.sh -server zk2:2182 < "$fifo" > "$log" 2>&1 &
  exec 3> "$fifo"
  until grep -q 'SyncConnected' "$log"; do sleep 0.2; done
  for cmd in 'create -e /demo/eph "owned-by-window-2"' 'stat /demo/eph'; do
    echo "[$(stamp)] окно 2> $cmd"
    echo "$cmd" >&3
  done
  until grep -q 'ephemeralOwner' "$log"; do sleep 0.2; done
  echo
  echo "[$(stamp)] окно 2 (отдельный контейнер hh-zk-window2, интерактивный zkCli.sh -server zk2:2182):"
  grep -v -e ' INFO ' -e '^$' -e '^WATCHER::' -e '^WatchedEvent' -e '^Connecting' -e '^Welcome' -e '^JLine' "$log" | sed 's/^/    /'
  echo
  zkcli zk1 stat /demo/eph
  zkcli zk1 ls /demo
  sid=$(sed -n 's/^ *ephemeralOwner = //p' "$log" | head -1)
  echo "== Сессия окна 2: $sid, согласованный тайм-аут на сервере zk2 (4lw cons)"
  echo "[$(stamp)] \$ echo cons | nc -w 2 127.0.0.1 2182 | grep sid=$sid"
  echo cons | nc -w 2 127.0.0.1 2182 | grep "sid=$sid"
  echo
  show sh -c "echo mntr | nc -w 2 127.0.0.1 2181 | grep zk_ephemerals_count"
  echo "== Закрываю окно 2: docker kill (zkCli убит без закрытия сессии, как при закрытии окна терминала)"
  t0=$(now)
  show docker kill hh-zk-window2
  exec 3>&-
  rm -f "$fifo" "$log"
  polls=0
  until [ "$(echo mntr | nc -w 2 127.0.0.1 2181 | sed -n 's/^zk_ephemerals_count[[:space:]]*//p')" = 0 ]; do
    polls=$((polls + 1))
    sleep 0.1
  done
  t1=$(now)
  echo "zk_ephemerals_count на zk1 стал 0 не позже чем через $(python3 -c "print(f'{$t1-$t0:.1f}')") с после закрытия окна (опрос mntr раз в ~$(python3 -c "print(f'{($t1-$t0)/max($polls,1):.2f}')") с, опросов $polls)"
  echo
  zkcli zk1 ls /demo
  zkcli zk1 stat /demo/eph
  zkcli zk1 delete /demo
} > "$OUT/stage2_ephemeral.txt"

rm -rf "$TMP"
echo "готово: $OUT/stage1_java.txt stage2_*.txt"
