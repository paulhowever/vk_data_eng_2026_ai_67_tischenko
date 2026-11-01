import argparse
import hashlib
import json
import multiprocessing
import os
import queue
import time
import uuid
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed

from kazoo.exceptions import ConnectionLoss, NodeExistsError, NoNodeError, SessionExpiredError
from kazoo.protocol.states import EventType

from .. import config, s3
from ..mr import engine, jobs, split
from . import common
from .common import num


def parse_args(argv=None):
    parser = argparse.ArgumentParser(prog="python -m hhlake.zk.coord")
    parser.add_argument("--run", required=True)
    parser.add_argument("--name", required=True)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--chunk-mib", type=float, default=32)
    return parser.parse_args(argv)


def fencing_enabled():
    return os.environ.get("FENCING", "0") == "1"


def order(nodes):
    return sorted(nodes, key=lambda node: node[-10:])


def join(zk, paths, name, log):
    prefix = f"{uuid.uuid4().hex}-n_"
    while True:
        try:
            mine = zk.create(f"{paths.election}/{prefix}", name.encode(), ephemeral=True, sequence=True).rsplit("/", 1)[1]
            break
        except (ConnectionLoss, SessionExpiredError):
            found = [node for node in zk.retry(zk.get_children, paths.election) if node.startswith(prefix)]
            if found:
                mine = found[0]
                log.info(f"после обрыва связи нашёл свой узел по префиксу {prefix}")
                break
    log.info(f"встал в очередь выборов: {paths.election}/{mine}")
    return mine


def take_epoch(zk, paths, mine, name):
    def attempt():
        tx = zk.transaction()
        tx.check(f"{paths.election}/{mine}", 0)
        tx.set_data(paths.epoch, name.encode())
        return tx.commit()

    results = zk.retry(attempt)
    if isinstance(results[0], NoNodeError):
        return None
    errors = [r for r in results if isinstance(r, Exception)]
    if errors:
        raise errors[0]
    return results[1].version


def elect(zk, paths, name, log):
    mine = join(zk, paths, name, log)
    watching = None
    while True:
        nodes = order(zk.retry(zk.get_children, paths.election))
        if mine not in nodes:
            log.info(f"моего узла {mine} больше нет (сессия истекла), встаю в очередь заново")
            mine, watching = join(zk, paths, name, log), None
            continue
        if nodes[0] == mine:
            epoch = take_epoch(zk, paths, mine, name)
            if epoch is not None:
                return mine, epoch
            log.info(f"узел {mine} пропал до взятия эпохи, встаю в очередь заново")
            mine, watching = join(zk, paths, name, log), None
            continue
        ahead = nodes[nodes.index(mine) - 1]
        events = queue.Queue()
        try:
            owner, _ = zk.retry(zk.get, f"{paths.election}/{ahead}", watch=events.put)
        except NoNodeError:
            continue
        if ahead != watching:
            log.info(f"запасной: жду, впереди {ahead} ({owner.decode()}), слежу за его удалением")
            watching = ahead
        event = events.get()
        if event.type == EventType.DELETED:
            log.info(f"узел {ahead} удалён")


def checkpoint(zk, paths):
    done = {}
    for child in zk.retry(zk.get_children, paths.done):
        data, _ = zk.retry(zk.get, f"{paths.done}/{child}")
        entry = json.loads(data)
        done[entry["task"]] = entry
    return done


def pending(tasks, done):
    return [task for task in tasks if task.index not in done]


def entry_of(report, job, epoch, name):
    return {
        "task": report.index,
        "epoch": epoch,
        "by": name,
        "keys": [job.part_key(report.index, report.attempt, r) for r in report.parts],
        "records": report.records,
        "variant": report.variant,
        "with_skills": report.with_skills,
    }


def save(zk, paths, entry):
    try:
        zk.retry(zk.create, paths.task(entry["task"]), json.dumps(entry).encode())
        return True
    except NodeExistsError:
        return False


def combine(entries, fetch):
    totals = {common.K: Counter()}
    for entry in sorted(entries, key=lambda e: e["task"]):
        for key in entry["keys"]:
            jobs.merge_blob(fetch(key), totals)
    return totals[common.K]


def pointer(key, epoch, name, digest):
    return json.dumps({"key": key, "epoch": epoch, "by": name, "sha256": digest}).encode()


def publish(zk, paths, value, epoch, fencing):
    if not fencing:
        return True, zk.set(paths.result, value).version
    tx = zk.transaction()
    tx.check(paths.epoch, epoch)
    tx.set_data(paths.result, value)
    results = tx.commit()
    errors = [r for r in results if isinstance(r, Exception)]
    if errors:
        return False, type(errors[0]).__name__
    return True, results[1].version


def run_maps(zk, paths, job, todo, workers, epoch, name, total, done_before, log):
    context = multiprocessing.get_context("spawn")
    finished = done_before
    with ProcessPoolExecutor(max_workers=workers, mp_context=context) as pool:
        futures = {pool.submit(engine.run_map, job, task, 1): task for task in todo}
        for future in as_completed(futures):
            report = future.result()
            entry = entry_of(report, job, epoch, name)
            finished += 1
            if save(zk, paths, entry):
                log.info(f"чекпоинт: map {report.index:>2} готова за {report.seconds:.2f} с, "
                         f"{num(report.records)} записей [{finished} из {total}]")
            else:
                log.info(f"чекпоинт: map {report.index:>2} готова, но узел {paths.task(report.index)} "
                         f"уже создан другим лидером, не перезаписываю [{finished} из {total}]")


def verify(counter, entries, data, log):
    keys, total, top_key, top_count = jobs.CONTROL[common.K]
    (key, count), = jobs.top(counter)
    actual = (len(counter), sum(counter.values()), key, count)
    same = actual == (keys, total, top_key, top_count)
    log.info(f"контрольные числа k={common.K}: уникальных пар {num(actual[0])}, вхождений пар {num(actual[1])}, "
             f"топ-пара ({', '.join(key)}) {num(count)}")
    log.info(f"сверка с ДЗ 3 (jobs.CONTROL[{common.K}] = {num(keys)} / {num(total)} / ({', '.join(top_key)}) {num(top_count)}): "
             f"{'совпадает' if same else 'НЕ СОВПАДАЕТ'}")
    sums = {name: sum(e[name] for e in entries) for name in ("records", "variant", "with_skills")}
    expected = {name: jobs.EXPECTED[name] for name in sums}
    log.info(f"записей {num(sums['records'])}, вакансий варианта {num(sums['variant'])}, с навыками {num(sums['with_skills'])}: "
             f"{'совпадает' if sums == expected else 'НЕ СОВПАДАЕТ'} с ДЗ 3")
    digest = hashlib.sha256(data).hexdigest()
    remote = engine.object_sha256(common.BASELINE)
    local = s3.sha256_file(config.ROOT / common.BASELINE)
    log.info(f"sha256 k{common.K}.csv {digest}")
    log.info(f"файл ДЗ 3 {common.BASELINE}: sha256 в S3 {'совпадает' if remote == digest else 'НЕ СОВПАДАЕТ'}, "
             f"локальной копии {'совпадает' if local == digest else 'НЕ СОВПАДАЕТ'}")
    return digest, same and sums == expected and remote == digest == local


def lead(zk, args, paths, log, fencing):
    size = s3.object_size(config.RAW_KEY)
    tasks = [engine.MapTask(i, start, end) for i, (start, end) in enumerate(split.plan(size, int(args.chunk_mib * 2**20)))]
    began = time.time()
    _, epoch = elect(zk, paths, args.name, log)
    log.info(f"стал лидером, эпоха {epoch} (транзакция check своего узла + set {paths.epoch}, версия {epoch})")
    done = checkpoint(zk, paths)
    todo = pending(tasks, done)
    log.info(f"продолжаю с чекпоинта: готово {len(done)} из {len(tasks)}, осталось {len(todo)}")
    job = engine.Job(name=common.job_name(args.run, epoch), ks=(common.K,), regional=False,
                     reducers=common.REDUCERS, size=size)
    run_maps(zk, paths, job, todo, args.workers, epoch, args.name, len(tasks), len(done), log)
    entries = list(checkpoint(zk, paths).values())
    if len(entries) != len(tasks):
        raise RuntimeError(f"в чекпоинте {len(entries)} задач из {len(tasks)}")
    by = Counter(e["by"] for e in entries)
    log.info(f"map закончен: в чекпоинте {len(entries)} задач ({', '.join(f'{k}: {v}' for k, v in sorted(by.items()))}), "
             f"reduce по {common.REDUCERS} партициям")
    counter = combine(entries, s3.get_bytes)
    data = jobs.render_csv(counter, common.K, False).encode("utf-8")
    digest, ok = verify(counter, entries, data, log)
    if not ok:
        raise SystemExit("проверка не пройдена, результат не публикую")
    key = common.result_key(args.run, epoch)
    s3.put_bytes(key, data)
    log.info(f"положил файл {config.s3_url(key)} ({num(len(data))} Б)")
    zk.retry(zk.exists, paths.root)
    ok, outcome = publish(zk, paths, pointer(key, epoch, args.name, digest), epoch, fencing)
    if ok:
        log.info(f"записал результат: указатель {paths.result} -> {key}, версия указателя {outcome}, эпоха {epoch}")
    else:
        current = zk.retry(zk.exists, paths.epoch).version
        log.info(f"запись отклонена: {outcome} (check {paths.epoch} version={epoch}, а текущая эпоха {current})")
        s3.client().delete_object(Bucket=config.BUCKET, Key=key)
        log.info(f"удалил свой неопубликованный файл {config.s3_url(key)}")
    log.info(f"готово за {time.time() - began:.1f} с с момента старта")


def main(argv=None):
    args = parse_args(argv)
    fencing = fencing_enabled()
    log = common.open_log(args.run, args.name)
    paths = common.Paths(args.run)
    log.info(f"старт: pid {os.getpid()}, группа процессов {os.getpgrp()}, FENCING={int(fencing)}, "
             f"воркеров {args.workers}, чанк {args.chunk_mib:g} MiB, k={common.K}")
    zk = common.client()

    def watch_state(state):
        session = zk.client_id
        suffix = f", сессия 0x{session[0]:x}" if session and state == "CONNECTED" else ""
        log.info(f"состояние сессии ZooKeeper: {state}{suffix}")

    zk.add_listener(watch_state)
    zk.start(timeout=30)
    log.info(f"подключился к {common.HOSTS}, сессия 0x{zk.client_id[0]:x}, тайм-аут сессии {common.TIMEOUT:g} с")
    try:
        lead(zk, args, paths, log, fencing)
    finally:
        log.info("закрываю сессию ZooKeeper")
        zk.stop()
        zk.close()


if __name__ == "__main__":
    main()
