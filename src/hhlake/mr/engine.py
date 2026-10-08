import hashlib
import json
import multiprocessing
import os
import queue
import resource
import signal
import sys
import time
from array import array
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed
from concurrent.futures.process import BrokenProcessPool
from dataclasses import dataclass
from functools import partial

from botocore.exceptions import ClientError

from .. import config, s3
from ..segment import FIELDS
from . import jobs, split

MAX_ATTEMPTS = 3
CRASH_EXIT_CODE = 137
COLLATERAL = ("lost (not started)", "terminated (pool broken)")

_started = None
_ready_s = None


@dataclass(frozen=True)
class Job:
    name: str
    ks: tuple
    regional: bool
    reducers: int
    size: int
    key: str = config.RAW_KEY
    tail: int = split.TAIL
    fail_mapper: int | None = None
    fail_attempt: int = 1

    @property
    def prefix(self):
        return f"{config.TMP_MR}/{self.name}/"

    @property
    def manifest_key(self):
        return f"{self.prefix}_manifest.json"

    def part_key(self, task, attempt, reducer):
        return f"{self.prefix}map-{task}-a{attempt}/part-{reducer}"

    def crashes(self, task, attempt):
        return self.fail_mapper == task and self.fail_attempt == attempt


@dataclass(frozen=True)
class MapTask:
    index: int
    start: int
    end: int


@dataclass(frozen=True)
class ReduceTask:
    index: int


@dataclass
class MapReport:
    index: int
    attempt: int
    pid: int
    seconds: float
    first: int
    last: int
    ids: array
    variant: int
    with_skills: int
    region_variant: Counter
    region_with_skills: Counter
    emitted: dict
    combined: dict
    shuffle_bytes: dict
    parts: list
    get_requests: int
    get_bytes: int
    peak_rss_mib: float
    skill_sizes: Counter
    fetch_s: float
    parse_s: float
    combine_s: float
    serialize_s: float
    put_s: float
    worker_ready_s: float | None

    @property
    def records(self):
        return len(self.ids)

    def describe(self):
        return f"bytes [{self.first:,}, {self.last:,})  records {self.records:,}  GET {self.get_requests}"


@dataclass
class ReduceReport:
    index: int
    attempt: int
    pid: int
    seconds: float
    counts: dict
    reads: int
    read_bytes: int
    peak_rss_mib: float

    def describe(self):
        keys = sum(len(counter) for counter in self.counts.values())
        return f"parts {self.reads}  bytes {self.read_bytes:,}  keys {keys:,}"


@dataclass
class Attempt:
    stage: str
    index: int
    attempt: int
    status: str
    seconds: float
    pid: int | None
    exitcode: int | None
    charged: bool


@dataclass
class JobResult:
    job: Job
    tasks: list
    data_start: int
    header_requests: int
    header_bytes: int
    maps: list
    reduces: list
    counts: dict
    attempts: list
    time_map: float
    time_reduce: float
    tmp_removed: int


def peak_rss_mib():
    usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return usage / 2**20 if sys.platform == "darwin" else usage / 2**10


def _init_worker(started, opened_at):
    global _started, _ready_s
    _started = started
    _ready_s = time.time() - opened_at


def announce(stage, index, attempt):
    global _ready_s
    if _started is not None:
        _started.put((stage, index, attempt, os.getpid(), time.time()))
    ready, _ready_s = _ready_s, None
    return ready


class Timed:
    def __init__(self, iterable):
        self.iterator = iter(iterable)
        self.seconds = 0.0

    def __iter__(self):
        return self

    def __next__(self):
        began = time.perf_counter()
        try:
            return next(self.iterator)
        finally:
            self.seconds += time.perf_counter() - began


def run_map(job, task, attempt):
    ready = announce("map", task.index, attempt)
    began = time.perf_counter()
    chunk = split.read_split(partial(s3.get_range, job.key), job.size, task.start, task.end, job.tail)
    fetched = time.perf_counter()
    rows = Timed(split.rows(chunk.data))
    result = jobs.map_rows(rows, job.ks, job.regional)
    mapped = time.perf_counter()
    blobs, sizes = jobs.shuffle_blobs(result.counts, job.reducers)
    serialized = time.perf_counter()
    for reducer, blob in blobs.items():
        s3.put_bytes(job.part_key(task.index, attempt, reducer), blob)
        if job.crashes(task.index, attempt):
            os._exit(CRASH_EXIT_CODE)
    finished = time.perf_counter()
    return MapReport(
        index=task.index,
        attempt=attempt,
        pid=os.getpid(),
        seconds=time.perf_counter() - began,
        first=chunk.first,
        last=chunk.last,
        ids=result.ids,
        variant=result.variant,
        with_skills=result.with_skills,
        region_variant=result.region_variant,
        region_with_skills=result.region_with_skills,
        emitted=result.emitted,
        combined=result.combined,
        shuffle_bytes=sizes,
        parts=sorted(blobs),
        get_requests=chunk.requests,
        get_bytes=chunk.fetched,
        peak_rss_mib=peak_rss_mib(),
        skill_sizes=result.skill_sizes,
        fetch_s=fetched - began,
        parse_s=rows.seconds,
        combine_s=mapped - fetched - rows.seconds,
        serialize_s=serialized - mapped,
        put_s=finished - serialized,
        worker_ready_s=ready,
    )


def run_reduce(job, task, attempt):
    announce("reduce", task.index, attempt)
    began = time.perf_counter()
    manifest = json.loads(s3.get_text(job.manifest_key))
    totals = {k: Counter() for k in job.ks}
    reads = read_bytes = 0
    for entry in manifest["maps"]:
        if task.index in entry["parts"]:
            blob = s3.get_bytes(job.part_key(entry["task"], entry["attempt"], task.index))
            jobs.merge_blob(blob, totals)
            reads += 1
            read_bytes += len(blob)
    return ReduceReport(
        index=task.index,
        attempt=attempt,
        pid=os.getpid(),
        seconds=time.perf_counter() - began,
        counts=totals,
        reads=reads,
        read_bytes=read_bytes,
        peak_rss_mib=peak_rss_mib(),
    )


class Master:
    def __init__(self, workers, log=print):
        self.workers = workers
        self.log = log
        self.context = multiprocessing.get_context("spawn")
        self.pool = None
        self.started = None
        self.processes = {}
        self.attempts = Counter()
        self.failures = Counter()
        self.history = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def close(self):
        if self.pool is not None:
            self.pool.shutdown(wait=True, cancel_futures=True)
            self.pool = None
        if self.started is not None:
            self.started.close()
            self.started = None

    def _open(self):
        self.started = self.context.Queue()
        self.pool = ProcessPoolExecutor(
            max_workers=self.workers,
            mp_context=self.context,
            initializer=_init_worker,
            initargs=(self.started, time.time()),
        )
        self.processes = getattr(self.pool, "_processes", {})

    def _submit(self, stage, fn, job, pending):
        futures = {}
        for position, task in enumerate(pending):
            attempt = self.attempts[stage, task.index] + 1
            try:
                future = self.pool.submit(fn, job, task, attempt)
            except BrokenProcessPool:
                return futures, pending[position:]
            self.attempts[stage, task.index] = attempt
            futures[future] = (task, attempt)
        return futures, []

    def _drain(self):
        started = {}
        while True:
            try:
                stage, index, attempt, pid, at = self.started.get_nowait()
            except queue.Empty:
                return started
            started[stage, index, attempt] = (pid, at)

    def _record(self, stage, index, attempt, status, seconds, pid, exitcode, detail="", charged=False):
        self.history.append(Attempt(stage, index, attempt, status, seconds, pid, exitcode, charged))
        code = "" if exitcode is None else f"exit {exitcode}"
        self.log(
            f"{stage:<6} task {index:>3}  attempt {attempt}  {status:<24} {code:<8}"
            f" pid {pid or '-':>6} {seconds:7.2f} s  {detail}".rstrip(),
            flush=True,
        )

    def _report_failures(self, stage, failed, failed_at, broken):
        started = self._drain()
        outcomes = []
        for task, attempt, error in failed:
            pid, began = started.get((stage, task.index, attempt), (None, None))
            process = self.processes.get(pid)
            exitcode = process.exitcode if process is not None else None
            seconds = failed_at - began if began is not None else 0.0
            if error is not None:
                status = f"error {type(error).__name__}: {error}"
            elif pid is None:
                status = "lost (not started)"
            elif exitcode == -signal.SIGTERM:
                status = "terminated (pool broken)"
            else:
                status = "crashed"
            outcomes.append((task, attempt, status, seconds, pid, exitcode))
        culprits = {task.index for task, _, status, *_ in outcomes if status not in COLLATERAL}
        if broken and not culprits:
            culprits = {task.index for task, *_ in outcomes}
        for task, attempt, status, seconds, pid, exitcode in outcomes:
            charged = task.index in culprits
            detail = f"failure {self.failures[stage, task.index] + 1} of {MAX_ATTEMPTS}" if charged else "not charged"
            self._record(stage, task.index, attempt, status, seconds, pid, exitcode, detail, charged)
        return sorted(culprits)

    def _charge(self, stage, culprits):
        for index in culprits:
            self.failures[stage, index] += 1
            if self.failures[stage, index] >= MAX_ATTEMPTS:
                raise RuntimeError(f"{stage} task {index} failed {MAX_ATTEMPTS} times")

    def run(self, stage, fn, job, tasks):
        reports = {}
        pending = list(tasks)
        while pending:
            if self.pool is None:
                self._open()
            futures, deferred = self._submit(stage, fn, job, pending)
            failed = []
            broken = False
            for future in as_completed(futures):
                task, attempt = futures[future]
                try:
                    report = future.result()
                except BrokenProcessPool:
                    broken = True
                    failed.append((task, attempt, None))
                except Exception as error:
                    failed.append((task, attempt, error))
                else:
                    reports[task.index] = report
                    self._record(stage, task.index, attempt, "ok", report.seconds, report.pid, None, report.describe())
            failed_at = time.time()
            broken = broken or bool(deferred)
            if broken:
                self.pool.shutdown(wait=True)
                self.pool = None
            culprits = self._report_failures(stage, failed, failed_at, broken)
            if broken:
                self.started.close()
                self.started = None
                unfinished = len(failed) + len(deferred)
                self.log(f"{stage:<6} pool broken: new pool, resubmitting {unfinished} unfinished tasks", flush=True)
            self._charge(stage, culprits)
            pending = sorted([task for task, _, _ in failed] + deferred, key=lambda task: task.index)
        return [reports[task.index] for task in tasks]


def write_manifest(job, tasks, maps):
    manifest = {
        "job": job.name,
        "object": job.key,
        "size": job.size,
        "ks": list(job.ks),
        "regional": job.regional,
        "reducers": job.reducers,
        "maps": [
            {
                "task": report.index,
                "attempt": report.attempt,
                "range": [task.start, task.end],
                "aligned": [report.first, report.last],
                "parts": report.parts,
            }
            for task, report in zip(tasks, maps, strict=True)
        ],
    }
    s3.put_text(job.manifest_key, json.dumps(manifest, ensure_ascii=False, indent=1))


def execute(job, chunk, workers, keep_tmp=False, log=print):
    before = dict(s3.STATS)
    header, data_start = split.read_header(partial(s3.get_range, job.key), job.size)
    if header != FIELDS:
        raise ValueError(f"unexpected header in {job.key}: {header}")
    header_requests = s3.STATS["get_requests"] - before["get_requests"]
    header_bytes = s3.STATS["get_bytes"] - before["get_bytes"]
    tasks = [MapTask(i, start, end) for i, (start, end) in enumerate(split.plan(job.size, chunk))]
    if job.fail_mapper is not None and not 0 <= job.fail_mapper < len(tasks):
        raise ValueError(f"--fail-mapper {job.fail_mapper} is outside 0..{len(tasks) - 1}")
    s3.delete_prefix(job.prefix)
    with Master(workers, log) as master:
        began = time.perf_counter()
        maps = master.run("map", run_map, job, tasks)
        write_manifest(job, tasks, maps)
        mapped = time.perf_counter()
        reduces = master.run("reduce", run_reduce, job, [ReduceTask(r) for r in range(job.reducers)])
        reduced = time.perf_counter()
    counts = {k: Counter() for k in job.ks}
    for report in reduces:
        for k, counter in report.counts.items():
            counts[k].update(counter)
    tmp_removed = 0 if keep_tmp else s3.delete_prefix(job.prefix)
    return JobResult(
        job=job,
        tasks=tasks,
        data_start=data_start,
        header_requests=header_requests,
        header_bytes=header_bytes,
        maps=maps,
        reduces=reduces,
        counts=counts,
        attempts=master.history,
        time_map=mapped - began,
        time_reduce=reduced - mapped,
        tmp_removed=tmp_removed,
    )


def publish(key, text):
    data = text.encode("utf-8")
    s3.put_bytes(key, data)
    local = config.ROOT / key
    local.parent.mkdir(parents=True, exist_ok=True)
    local.write_bytes(data)
    return len(data), hashlib.sha256(data).hexdigest()


def object_sha256(key):
    try:
        return s3.sha256_object(key)
    except ClientError as error:
        if error.response.get("Error", {}).get("Code") in ("NoSuchKey", "404"):
            return None
        raise


def attempt_order(prefix):
    task, _, attempt = prefix.removeprefix("map-").partition("-a")
    return (int(task), int(attempt)) if task.isdigit() and attempt.isdigit() else (sys.maxsize, 0)


def tmp_inventory(job):
    manifest = json.loads(s3.get_text(job.manifest_key))
    accepted = {f"map-{entry['task']}-a{entry['attempt']}" for entry in manifest["maps"]}
    groups = {}
    keys = s3.list_keys(job.prefix)
    for key in keys:
        head, _, part = key.removeprefix(job.prefix).partition("/")
        if part:
            groups.setdefault(head, []).append(part)
    ordered = sorted(groups, key=attempt_order)
    return {
        "objects": len(keys),
        "manifest": manifest,
        "accepted": {head: sorted(groups[head]) for head in ordered if head in accepted},
        "orphans": {head: sorted(groups[head]) for head in ordered if head not in accepted},
    }
