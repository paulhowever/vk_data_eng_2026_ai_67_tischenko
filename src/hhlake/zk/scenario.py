import argparse
import json
import math
import os
import re
import signal
import socket
import subprocess
import sys
import time
from datetime import datetime

from kazoo.exceptions import NoNodeError

from .. import config, s3
from ..mr import split
from . import common

LINE = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}) (\S+)\s+(.*)$")
KEY_EVENTS = ("старт", "стал лидером", "продолжаю с чекпоинта", "контрольные числа", "сверка с ДЗ 3", "файл ДЗ 3",
              "записал результат", "запись отклонена", "состояние сессии", "Session", "удалил", "удалён", "встал в очередь",
              "запасной", "заново", "SIGSTOP", "SIGCONT", "эпоха", "закрываю", "map закончен", "положил файл")


class Out:
    def __init__(self, path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.file = open(path, "a", encoding="utf-8")

    def __call__(self, message, at=None):
        moment = datetime.fromtimestamp(at or time.time())
        line = f"{moment.strftime(common.STAMP)}.{moment.microsecond // 1000:03d} scenario {message}"
        print(line, flush=True)
        self.file.write(line + "\n")
        self.file.flush()


def parse_args(argv=None):
    parser = argparse.ArgumentParser(prog="python -m hhlake.zk.scenario")
    parser.add_argument("--run")
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--chunk-mib", type=float, default=32)
    parser.add_argument("--pause", type=float, default=40)
    parser.add_argument("--pause-at", type=float, default=0.5)
    parser.add_argument("--names", nargs=2, default=["coord-A", "coord-B"])
    return parser.parse_args(argv)


def reset(zk, paths, run, out):
    if zk.exists(paths.root):
        zk.delete(paths.root, recursive=True)
    zk.ensure_path(paths.election)
    zk.ensure_path(paths.done)
    zk.create(paths.epoch, b"")
    zk.create(paths.result, b"")
    removed = s3.delete_prefix(common.tmp_prefix(run)) + s3.delete_prefix(common.result_prefix(run))
    out(f"сброс: {paths.root} пересоздан (epoch и result версии 0), удалено объектов S3 прошлых прогонов: {removed}")


def spawn(name, args, run):
    log = common.LOGS / run / f"{name}.log"
    sink = open(log, "a", encoding="utf-8")
    command = [sys.executable, "-m", "hhlake.zk.coord", "--run", run, "--name", name,
               "--workers", str(args.workers), "--chunk-mib", str(args.chunk_mib)]
    return subprocess.Popen(command, cwd=config.ROOT, stdout=sink, stderr=subprocess.STDOUT,
                            start_new_session=True, env=os.environ.copy())


def stop(procs):
    alive = [proc for proc in procs.values() if proc.poll() is None]
    for proc in alive:
        for sig in (signal.SIGCONT, signal.SIGTERM):
            try:
                os.killpg(proc.pid, sig)
            except ProcessLookupError:
                pass
    for proc in alive:
        try:
            proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            os.killpg(proc.pid, signal.SIGKILL)
            proc.wait()


def until(check, timeout, step=0.05):
    deadline = time.time() + timeout
    while time.time() < deadline:
        value = check()
        if value:
            return value
        time.sleep(step)
    raise TimeoutError("не дождался условия")


def epoch(zk, paths):
    data, stat = zk.get(paths.epoch)
    return stat.version, data.decode()


def read_log(path):
    events = []
    for line in path.read_text(encoding="utf-8").splitlines():
        match = LINE.match(line)
        if match:
            at = datetime.strptime(match[1], f"{common.STAMP}.%f").timestamp()
            events.append((at, match[2], match[3], line))
    return events


def first(events, who, text):
    return next((at for at, name, message, _ in events if name == who and text in message), None)


def four(port, command):
    with socket.create_connection(("127.0.0.1", port), timeout=5) as sock:
        sock.sendall(command.encode())
        chunks = []
        while chunk := sock.recv(65536):
            chunks.append(chunk)
    return b"".join(chunks).decode()


def show_sessions(out):
    for port in (2181, 2182, 2183):
        for line in four(port, "cons").splitlines():
            sid = re.search(r"sid=(0x[0-9a-f]+)", line)
            to = re.search(r"to=(\d+)", line)
            if sid and to:
                out(f"  сервер :{port} держит сессию {sid[1]}, согласованный тайм-аут {int(to[1]) / 1000:g} с")


def show_node(zk, path, out):
    try:
        data, stat = zk.get(path)
    except NoNodeError:
        out(f"{path}: нет узла")
        return None
    out(f"{path}: version={stat.version} mzxid=0x{stat.mzxid:x} data={data.decode() or '(пусто)'}")
    return stat


def main(argv=None):
    args = parse_args(argv)
    fencing = os.environ.get("FENCING", "0") == "1"
    run = args.run or ("stage4_fence" if fencing else "stage3_nofence")
    folder = common.LOGS / run
    folder.mkdir(parents=True, exist_ok=True)
    for old in folder.glob("*.log"):
        old.unlink()
    out = Out(folder / "scenario.log")
    paths = common.Paths(run)
    size = s3.object_size(config.RAW_KEY)
    total = len(split.plan(size, int(args.chunk_mib * 2**20)))
    stop_at = math.ceil(total * args.pause_at)
    out(f"сценарий {run}: FENCING={int(fencing)}, ZooKeeper {common.HOSTS}, тайм-аут сессии {common.TIMEOUT:g} с, "
        f"map-задач {total}, пауза лидера после {stop_at} готовых задач на {args.pause:g} с")
    zk = common.client()
    zk.start(timeout=30)
    reset(zk, paths, run, out)

    first_name, second_name = args.names
    procs = {}
    try:
        procs[first_name] = spawn(first_name, args, run)
        out(f"запущен {first_name}, pid {procs[first_name].pid}")
        until(lambda: epoch(zk, paths)[0] >= 1, 60)
        procs[second_name] = spawn(second_name, args, run)
        out(f"запущен {second_name}, pid {procs[second_name].pid}")
        version, leader = epoch(zk, paths)
        backup = second_name if leader == first_name else first_name
        out(f"лидер по {paths.epoch}: {leader} (эпоха {version}), запасной {backup}")
        until(lambda: len(zk.get_children(paths.election)) >= 2, 60)
        out("сессии на серверах ансамбля (4lw cons)")
        show_sessions(out)

        until(lambda: len(zk.get_children(paths.done)) >= stop_at, 300)
        done = len(zk.get_children(paths.done))
        os.killpg(procs[leader].pid, signal.SIGSTOP)
        stopped = time.time()
        out(f"SIGSTOP группе процессов {leader} (pgid {procs[leader].pid}, координатор и воркеры пула): "
            f"в чекпоинте {done} из {total}", stopped)

        until(lambda: epoch(zk, paths)[0] >= 2, 120, 0.02)
        seen = time.time()
        version, holder = epoch(zk, paths)
        out(f"{paths.epoch}: версия {version}, лидер {holder}; замечено через {seen - stopped:.2f} с после SIGSTOP")

        def backup_finished():
            return procs[backup].poll() is not None

        until(lambda: time.time() - stopped >= args.pause and backup_finished(), 600, 0.1)
        os.killpg(procs[leader].pid, signal.SIGCONT)
        resumed = time.time()
        out(f"SIGCONT группе процессов {leader}: пауза длилась {resumed - stopped:.1f} с, {backup} завершился "
            f"с кодом {procs[backup].returncode}", resumed)

        for name, proc in procs.items():
            proc.wait(timeout=600)
            out(f"{name} завершился с кодом {proc.returncode}")

        out("итог в ZooKeeper")
        stat = show_node(zk, paths.result, out)
        show_node(zk, paths.epoch, out)
        out(f"чекпоинт {paths.done}: {len(zk.get_children(paths.done))} узлов")
        keys = s3.list_keys(common.result_prefix(run))
        out(f"файлы результата в S3 ({config.s3_url(common.result_prefix(run))}): {len(keys)}")
        for key in keys:
            out(f"  {key}  sha256 {s3.sha256_object(key)}")

        events = sorted((e for path in (folder / f"{n}.log" for n in args.names) for e in read_log(path)), key=lambda e: e[0])
        elected = first(events, backup, "стал лидером")
        out(f"время смены лидера: от SIGSTOP {leader} до строки «стал лидером» у {backup}: {elected - stopped:.2f} с")
        writes = [line for _, _, message, line in events if "записал результат" in message]
        rejects = [line for _, _, message, line in events if "запись отклонена" in message]
        out(f"строк «записал результат»: {len(writes)}, строк «запись отклонена»: {len(rejects)}, "
            f"версия указателя {stat.version if stat else '-'}")
        for line in writes + rejects:
            out(f"  {line}")

        summary = {
            "run": run,
            "fencing": fencing,
            "map_tasks": total,
            "paused_after": done,
            "leader": leader,
            "backup": backup,
            "pause_s": round(resumed - stopped, 2),
            "failover_s": round(elected - stopped, 2),
            "failover_seen_by_scenario_s": round(seen - stopped, 2),
            "writes": len(writes),
            "rejects": len(rejects),
            "pointer_version": stat.version if stat else None,
            "pointer": json.loads(zk.get(paths.result)[0] or b"null"),
            "result_files": keys,
        }
        out(json.dumps(summary, ensure_ascii=False))
        removed = s3.delete_prefix(common.tmp_prefix(run))
        out(f"удалено промежуточных объектов shuffle: {removed} ({config.s3_url(common.tmp_prefix(run))}*)")

        print("\nхронология (все журналы, по времени)")
        merged = sorted(events + read_log(folder / "scenario.log"), key=lambda e: e[0])
        for _, _, message, line in merged:
            if not message.startswith(" ") and any(word in message for word in KEY_EVENTS):
                print(line)
    finally:
        stop(procs)
        zk.stop()
        zk.close()


if __name__ == "__main__":
    main()
