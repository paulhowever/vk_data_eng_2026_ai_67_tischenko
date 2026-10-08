import argparse
import contextlib
import io
import json
import math
import os
import platform
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import matplotlib
import pandas as pd
import psutil
from botocore.exceptions import BotoCoreError, ClientError
from matplotlib.figure import Figure

from . import config, s3

MIB = 1 << 20
INTERVAL = 0.02
SCAN_EVERY = 5
CPU_WINDOW = 0.2
TIMELINE_POINTS = 1500
PROBE_OVERHEAD_MIB = 64
CONTAINER = "hh-lake-rustfs"
HEALTH_TIMEOUT = 120
LOCAL_DIR = config.LOCAL_RESULTS / "bench"
IMG_DIR = config.ROOT / "report" / "img"
TIME_BIN = Path("/usr/bin/time")

KS = ("1", "2", "3", "123")
WORKERS = (1, 2, 4, 8, 10)
CHUNKS = (8, 32, 128)
DEFAULT_WORKERS = 10
DEFAULT_CHUNK = 32

INK = "#0b0b0b"
INK_2 = "#52514e"
GRID = "#e4e3df"
BLUE, ORANGE, AQUA, YELLOW, MAGENTA, GREEN, VIOLET, RED = (
    "#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#4a3aa7", "#e34948",
)
SERIES = (BLUE, ORANGE, AQUA, YELLOW, MAGENTA, GREEN, VIOLET, RED)
BLUE_LIGHT = "#86b6ef"
ORANGE_LIGHT = "#f5b39a"
STYLE = {
    "font.family": "DejaVu Sans",
    "font.size": 9,
    "axes.titlesize": 10,
    "axes.labelcolor": INK_2,
    "axes.edgecolor": INK_2,
    "axes.spines.top": False,
    "axes.spines.right": False,
    "axes.axisbelow": True,
    "axes.grid": True,
    "axes.grid.axis": "y",
    "grid.color": GRID,
    "grid.linewidth": 0.8,
    "xtick.color": INK_2,
    "ytick.color": INK_2,
    "text.color": INK,
    "legend.frameon": False,
    "savefig.dpi": 200,
    "savefig.bbox": "tight",
}


@dataclass(frozen=True)
class Scenario:
    name: str
    label: str
    module: str
    args: tuple
    engine: str
    k: str = ""
    workers: int | None = None
    chunk_mib: int | None = None
    source: str = ""

    def argv(self):
        return [sys.executable, "-m", self.module, *self.args]

    def shell(self):
        return shlex.join(["python", "-m", self.module, *self.args])

    def flags(self):
        return {arg for arg in self.args if arg.startswith("--")}


def k_label(ks):
    return "+".join(ks)


def mapreduce(name, ks="123", workers=DEFAULT_WORKERS, chunk_mib=DEFAULT_CHUNK, label=None):
    args = ("--k", *ks, "--workers", str(workers), "--chunk-mib", str(chunk_mib), "--no-write")
    return Scenario(name, label or f"MapReduce k={k_label(ks)}", "hhlake.mr.run", args, "mapreduce",
                    k_label(ks), workers, chunk_mib, "csv")


def naive(name, ks="123"):
    args = ("--k", *ks, "--no-write")
    return Scenario(name, "MapReduce v1 (наивный)", "hhlake.mr.naive", args, "mapreduce_naive", k_label(ks), 1, None, "csv")


def duck(name, ks="123", source="csv", label=None, materialize=False):
    args = ("--k", *ks, "--source", source, *(("--materialize",) if materialize else ()), "--no-write")
    return Scenario(name, label or f"DuckDB k={k_label(ks)}", "hhlake.duck", args, "duckdb", k_label(ks), None, None, source)


def probe(name, label, mode, mib, children=0, parent_mib=32, exit_code=0):
    args = ("--mode", mode, "--mib", str(mib), "--children", str(children), "--parent-mib", str(parent_mib),
            "--hold", "1.0", "--exit-code", str(exit_code))
    return Scenario(name, label, "hhlake.memprobe", args, "probe")


SUITES = {
    "main": [mapreduce(f"mr_k{k}", k) for k in KS] + [duck(f"duck_k{k}", k) for k in KS]
            + [duck("duck_k123_1scan", label="DuckDB k=1+2+3, один проход", materialize=True)],
    "scaling": [mapreduce(f"mr_w{w}_c{DEFAULT_CHUNK}", workers=w, label=f"{w} воркеров, чанк {DEFAULT_CHUNK} МиБ")
                for w in WORKERS]
               + [mapreduce(f"mr_w{DEFAULT_WORKERS}_c{c}", chunk_mib=c, label=f"{DEFAULT_WORKERS} воркеров, чанк {c} МиБ")
                  for c in CHUNKS if c != DEFAULT_CHUNK],
    "bonus": [
        naive("mr_naive_k123"),
        mapreduce("mr_k123", label="MapReduce v2"),
        duck("duck_k123", label="DuckDB CSV"),
        duck("duck_parquet_k123", source="parquet", label="DuckDB Parquet"),
    ],
    "selftest": [
        probe("probe_single", "1 процесс, 256 МиБ", "single", 256),
        probe("probe_pool", "4 потомка × 128 МиБ", "pool", 128, children=4),
        probe("probe_chain", "3 потомка подряд × 192 МиБ", "chain", 192, children=3),
        probe("probe_nested", "потомок + внук × 96 МиБ", "nested", 96),
        probe("probe_fail", "выход с кодом 3", "single", 16, exit_code=3),
    ],
}
SUITE_ORDER = ("main", "scaling", "bonus")
PANELS = {
    "main": [("MapReduce", [f"mr_k{k}" for k in KS]), ("DuckDB", [f"duck_k{k}" for k in KS] + ["duck_k123_1scan"])],
    "scaling": [
        (f"Воркеры (чанк {DEFAULT_CHUNK} МиБ)", [f"mr_w{w}_c{DEFAULT_CHUNK}" for w in WORKERS]),
        (f"Чанк ({DEFAULT_WORKERS} воркеров)", [f"mr_w{DEFAULT_WORKERS}_c{c}" for c in CHUNKS]),
    ],
    "bonus": [("MapReduce v1 → v2", ["mr_naive_k123", "mr_k123"]), ("DuckDB CSV → Parquet", ["duck_k123", "duck_parquet_k123"])],
    "selftest": [("Синтетические пробы", [s.name for s in SUITES["selftest"]])],
}
SCENARIOS = {(suite, s.name): s for suite, items in SUITES.items() for s in items}
ORDER = {key: i for i, key in enumerate(SCENARIOS)}


@dataclass
class Measurement:
    wall_s: float
    exit_code: int
    timed_out: bool
    peak_rss: int
    peak_procs: int
    peak_cpu_pct: float
    cpu_s: float
    kernel_maxrss: int
    samples: int
    stdout: str
    stderr: str
    timeline: list


class TreeSampler(threading.Thread):
    def __init__(self, pid, interval=INTERVAL, timeout=None):
        super().__init__(daemon=True)
        try:
            self.root = psutil.Process(pid)
        except psutil.Error:
            self.root = None
        self.interval = interval
        self.timeout = timeout
        self.done = threading.Event()
        self.members = {pid: self.root} if self.root is not None else {}
        self.cpu_prev = {}
        self.timeline = []
        self.peak_rss = 0
        self.peak_procs = 0
        self.peak_cpu = 0.0
        self.timed_out = False

    def discover(self):
        with contextlib.suppress(psutil.Error):
            for child in self.root.children(recursive=True):
                self.members.setdefault(child.pid, child)
        self.members.setdefault(self.root.pid, self.root)

    def poll(self):
        rss = 0
        cpu = {}
        for pid, proc in list(self.members.items()):
            try:
                with proc.oneshot():
                    rss += proc.memory_info().rss
                    times = proc.cpu_times()
            except psutil.Error:
                del self.members[pid]
                continue
            cpu[pid] = times.user + times.system
        busy = sum(value - self.cpu_prev.get(pid, value) for pid, value in cpu.items())
        self.cpu_prev = cpu
        return rss, len(cpu), busy

    def kill_tree(self):
        if self.root is None:
            return
        with contextlib.suppress(psutil.Error):
            for child in self.root.children(recursive=True):
                with contextlib.suppress(psutil.Error):
                    child.kill()
        with contextlib.suppress(psutil.Error):
            self.root.kill()

    def run(self):
        if self.root is None:
            return
        start = time.perf_counter()
        window_start, window_busy = start, 0.0
        tick = 0
        while not self.done.is_set():
            if tick % SCAN_EVERY == 0:
                self.discover()
            rss, procs, busy = self.poll()
            now = time.perf_counter()
            self.timeline.append((now - start, rss, procs))
            self.peak_rss = max(self.peak_rss, rss)
            self.peak_procs = max(self.peak_procs, procs)
            window_busy += busy
            if now - window_start >= CPU_WINDOW:
                self.peak_cpu = max(self.peak_cpu, 100 * window_busy / (now - window_start))
                window_start, window_busy = now, 0.0
            if self.timeout and now - start > self.timeout and not self.timed_out:
                self.timed_out = True
                self.kill_tree()
            tick += 1
            self.done.wait(max(0.0, start + tick * self.interval - time.perf_counter()))


def maxrss_bytes(usage):
    return usage.ru_maxrss if sys.platform == "darwin" else usage.ru_maxrss * 1024


def measure(argv, timeout=None, interval=INTERVAL):
    with tempfile.TemporaryFile() as out, tempfile.TemporaryFile() as err:
        started = time.perf_counter()
        proc = subprocess.Popen(argv, cwd=config.ROOT, stdout=out, stderr=err, stdin=subprocess.DEVNULL)
        sampler = TreeSampler(proc.pid, interval, timeout)
        sampler.start()
        try:
            _, status, usage = os.wait4(proc.pid, 0)
        except BaseException:
            sampler.kill_tree()
            proc.wait()
            raise
        finally:
            sampler.done.set()
        wall = time.perf_counter() - started
        proc.returncode = os.waitstatus_to_exitcode(status)
        sampler.join()
        out.seek(0)
        err.seek(0)
        stdout = out.read().decode("utf-8", "replace")
        stderr = err.read().decode("utf-8", "replace")
    return Measurement(
        wall_s=wall,
        exit_code=proc.returncode,
        timed_out=sampler.timed_out,
        peak_rss=sampler.peak_rss,
        peak_procs=sampler.peak_procs,
        peak_cpu_pct=sampler.peak_cpu,
        cpu_s=usage.ru_utime + usage.ru_stime,
        kernel_maxrss=maxrss_bytes(usage),
        samples=len(sampler.timeline),
        stdout=stdout,
        stderr=stderr,
        timeline=sampler.timeline,
    )


def last_json(text):
    for line in reversed(text.splitlines()):
        line = line.strip()
        if not (line.startswith("{") and line.endswith("}")):
            continue
        with contextlib.suppress(json.JSONDecodeError):
            value = json.loads(line)
            if isinstance(value, dict):
                return value
    return None


def tail(text, lines=15):
    return "\n".join(text.strip().splitlines()[-lines:])


def utc_now():
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def mib(value):
    return round(value / MIB, 1)


def power():
    battery = getattr(psutil, "sensors_battery", lambda: None)()
    if battery is None:
        return None, None
    return bool(battery.power_plugged), round(battery.percent)


def power_text(on_ac, percent):
    if on_ac is None:
        return "power: no battery"
    return f"power: {'AC' if on_ac else 'BATTERY'}, battery {percent}%"


def run_record(session, suite, scenario, run, m, started_at, supply):
    job = last_json(m.stdout)
    on_ac, battery_pct = supply
    return {
        "session": session,
        "suite": suite,
        "scenario": scenario.name,
        "label": scenario.label,
        "engine": scenario.engine,
        "k": scenario.k,
        "workers": scenario.workers,
        "chunk_mib": scenario.chunk_mib,
        "source": scenario.source,
        "run": run,
        "cold": run == 0,
        "exit_code": m.exit_code,
        "timed_out": m.timed_out,
        "wall_s": round(m.wall_s, 4),
        "job_time_s": job.get("time_total") if job else None,
        "peak_rss_mib": mib(m.peak_rss),
        "peak_procs": m.peak_procs,
        "peak_cpu_pct": round(m.peak_cpu_pct, 1),
        "cpu_s": round(m.cpu_s, 3),
        "cpu_avg_pct": round(100 * m.cpu_s / m.wall_s, 1) if m.wall_s else None,
        "kernel_maxrss_mib": mib(m.kernel_maxrss),
        "samples": m.samples,
        "on_ac": on_ac,
        "battery_pct": battery_pct,
        "started_at": started_at,
        "job_json": json.dumps(job, ensure_ascii=False, separators=(",", ":")) if job else "",
    }


def timeline_records(suite, scenario, run, timeline):
    stride = max(1, math.ceil(len(timeline) / TIMELINE_POINTS))
    buckets = (timeline[i:i + stride] for i in range(0, len(timeline), stride))
    return [
        {"suite": suite, "scenario": scenario.name, "run": run, "t_s": round(t, 3), "rss_mib": mib(rss), "procs": procs}
        for t, rss, procs in (max(bucket, key=lambda sample: sample[1]) for bucket in buckets)
    ]


def describe_run(rec):
    tag = "cold  " if rec["cold"] else f"warm {rec['run']}"
    status = "timeout" if rec["timed_out"] else f"exit {rec['exit_code']}"
    supply = f"  ON BATTERY {rec['battery_pct']}%" if rec["on_ac"] is False else ""
    return (f"  {tag}  {rec['wall_s']:9.3f} s  peak RSS {rec['peak_rss_mib']:8.1f} MiB  procs {rec['peak_procs']:3d}  "
            f"cpu avg {rec['cpu_avg_pct']:6.1f}% peak {rec['peak_cpu_pct']:6.1f}%  "
            f"max single {rec['kernel_maxrss_mib']:7.1f} MiB  {status}{supply}")


def bench_scenario(session, suite, scenario, runs, timeout, pause, interval):
    records, timeline = [], []
    for run in range(runs + 1):
        started_at = utc_now()
        supply = power()
        m = measure(scenario.argv(), timeout, interval)
        rec = run_record(session, suite, scenario, run, m, started_at, supply)
        records.append(rec)
        print(describe_run(rec), flush=True)
        if m.exit_code != 0:
            print(tail(m.stderr or m.stdout), flush=True)
            if run == 0:
                print("  cold run failed, warm runs skipped", flush=True)
                break
        if run == 1:
            timeline = timeline_records(suite, scenario, run, m.timeline)
        time.sleep(pause)
    return records, timeline


def parse_size(text):
    match = re.match(r"\s*([\d.]+)\s*([kKMGT]?i?B)", text)
    if not match:
        return None
    units = {"B": 1, "kB": 1e3, "KB": 1e3, "KiB": 1 << 10, "MB": 1e6, "MiB": 1 << 20, "GB": 1e9, "GiB": 1 << 30,
             "TB": 1e12, "TiB": 1 << 40}
    return round(float(match.group(1)) * units.get(match.group(2), 1) / MIB, 1)


def docker_snapshot(session, suite, phase):
    if shutil.which("docker") is None:
        return None
    try:
        result = subprocess.run(
            ["docker", "stats", "--no-stream", "--format", "{{json .}}", CONTAINER],
            capture_output=True, text=True, timeout=60, check=True,
        )
        data = json.loads(result.stdout.strip().splitlines()[-1])
    except (subprocess.SubprocessError, OSError, json.JSONDecodeError, IndexError):
        print(f"  docker stats for {CONTAINER} unavailable", flush=True)
        return None
    record = {
        "session": session,
        "suite": suite,
        "phase": phase,
        "at": utc_now(),
        "mem_mib": parse_size(data.get("MemUsage", "")),
        "mem_usage": data.get("MemUsage"),
        "mem_perc": data.get("MemPerc"),
        "cpu_perc": data.get("CPUPerc"),
        "net_io": data.get("NetIO"),
        "block_io": data.get("BlockIO"),
        "pids": data.get("PIDs"),
    }
    print(f"  docker {CONTAINER} [{phase}]: mem {record['mem_usage']} ({record['mem_perc']}), cpu {record['cpu_perc']}, "
          f"net {record['net_io']}, pids {record['pids']}", flush=True)
    return record


def restart_storage():
    started = time.perf_counter()
    subprocess.run(["docker", "restart", CONTAINER], capture_output=True, text=True, timeout=HEALTH_TIMEOUT, check=True)
    while time.perf_counter() - started < HEALTH_TIMEOUT:
        with contextlib.suppress(OSError), urllib.request.urlopen(f"{config.ENDPOINT}/health", timeout=2) as response:
            if response.status == 200:
                print(f"  docker restart {CONTAINER}: healthy after {time.perf_counter() - started:.1f} s", flush=True)
                return
        time.sleep(0.5)
    raise SystemExit(f"{CONTAINER} is not healthy {HEALTH_TIMEOUT} s after restart")


def artifact(suite, name):
    return f"selftest_{name}" if suite == "selftest" else name


def read_frame(text):
    return pd.read_csv(io.StringIO(text), dtype={"k": str, "job_json": str})


def load_csv(name):
    try:
        return read_frame(s3.get_text(f"{config.RESULTS_BENCH}/{name}"))
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in ("NoSuchKey", "404"):
            return pd.DataFrame()
        raise
    except BotoCoreError as exc:
        local = LOCAL_DIR / name
        print(f"  S3 unavailable ({exc}); using local {local.name}" if local.exists() else f"  S3 unavailable ({exc})")
        return read_frame(local.read_text(encoding="utf-8")) if local.exists() else pd.DataFrame()


def store_csv(df, name):
    text = df.to_csv(index=False)
    LOCAL_DIR.mkdir(parents=True, exist_ok=True)
    (LOCAL_DIR / name).write_text(text, encoding="utf-8")
    try:
        s3.put_text(f"{config.RESULTS_BENCH}/{name}", text)
    except (BotoCoreError, ClientError) as exc:
        print(f"  S3 upload of {name} failed ({exc}); local copy kept")
        return False
    return True


def scenario_key(df):
    return df["suite"].astype(str) + "/" + df["scenario"].astype(str)


def merge(old, new, fresh):
    if old.empty:
        return new.reset_index(drop=True)
    return pd.concat([old[~scenario_key(old).isin(fresh)], new], ignore_index=True)


def ordered(df, *extra):
    if df.empty:
        return df
    rank = [ORDER.get((suite, name), len(ORDER)) for suite, name in zip(df["suite"], df["scenario"], strict=True)]
    return df.assign(_rank=rank).sort_values(["_rank", *extra], kind="stable").drop(columns="_rank").reset_index(drop=True)


def summarize(runs):
    if runs.empty:
        return pd.DataFrame()
    keys = ["suite", "scenario"]
    meta = runs.drop_duplicates(keys).set_index(keys)[["label", "engine", "k", "workers", "chunk_mib", "source"]]
    ok = runs[(~runs["cold"].astype(bool)) & (runs["exit_code"] == 0)]
    stats = ok.groupby(keys, sort=False).agg(
        n=("wall_s", "size"),
        time_mean_s=("wall_s", "mean"),
        time_std_s=("wall_s", "std"),
        time_min_s=("wall_s", "min"),
        time_max_s=("wall_s", "max"),
        rss_mean_mib=("peak_rss_mib", "mean"),
        rss_std_mib=("peak_rss_mib", "std"),
        rss_min_mib=("peak_rss_mib", "min"),
        rss_max_mib=("peak_rss_mib", "max"),
        cpu_avg_pct=("cpu_avg_pct", "mean"),
        peak_cpu_pct=("peak_cpu_pct", "mean"),
        max_single_rss_mib=("kernel_maxrss_mib", "mean"),
    )
    cold = runs[runs["cold"].astype(bool)].set_index(keys)[["wall_s", "peak_rss_mib"]]
    cold.columns = ["cold_time_s", "cold_rss_mib"]
    failed = runs[runs["exit_code"] != 0].groupby(keys).size().rename("failed")
    on_battery = runs["on_ac"].astype(str).eq("False") if "on_ac" in runs else runs["run"].lt(0)
    battery = runs[on_battery].groupby(keys).size().rename("battery_runs")
    summary = meta.join(stats).join(cold).join(failed).join(battery).reset_index()
    for column in ("n", "failed", "battery_runs"):
        summary[column] = summary[column].fillna(0).astype(int)
    time_cols = [c for c in summary.columns if c.startswith(("time_", "cold_time"))]
    other_cols = [c for c in summary.columns if c.endswith(("_mib", "_pct"))]
    summary[time_cols] = summary[time_cols].round(3)
    summary[other_cols] = summary[other_cols].round(1)
    return ordered(summary)


def fmt(value, spec):
    return "—" if value is None or (isinstance(value, float) and math.isnan(value)) else format(value, spec)


def print_summary(summary):
    print(f"\n{'scenario':<20}{'n':>3}  {'time, s: mean ± std [min … max]':<36}{'peak RSS, MiB: mean ± std [min … max]':<42}"
          f"{'cold s':>8}{'cold MiB':>10}{'cpu%':>8}{'fail':>6}{'batt':>6}")
    for row in summary.itertuples(index=False):
        time_text = (f"{fmt(row.time_mean_s, '.3f')} ± {fmt(row.time_std_s, '.3f')} "
                     f"[{fmt(row.time_min_s, '.3f')} … {fmt(row.time_max_s, '.3f')}]")
        rss_text = (f"{fmt(row.rss_mean_mib, '.1f')} ± {fmt(row.rss_std_mib, '.1f')} "
                    f"[{fmt(row.rss_min_mib, '.1f')} … {fmt(row.rss_max_mib, '.1f')}]")
        print(f"{row.scenario:<20}{row.n:>3}  {time_text:<36}{rss_text:<42}{fmt(row.cold_time_s, '.3f'):>8}"
              f"{fmt(row.cold_rss_mib, '.1f'):>10}{fmt(row.cpu_avg_pct, '.0f'):>8}{row.failed:>6}{row.battery_runs:>6}")


def preflight(scenarios):
    problems = []
    for module in dict.fromkeys(s.module for s in scenarios):
        flags = sorted(set().union(*(s.flags() for s in scenarios if s.module == module)))
        try:
            result = subprocess.run([sys.executable, "-m", module, "--help"], cwd=config.ROOT, capture_output=True,
                                    text=True, timeout=120)
        except subprocess.TimeoutExpired:
            problems.append(f"{module}: --help timed out")
            continue
        if result.returncode != 0:
            problems.append(f"{module}: --help exited with {result.returncode}\n{tail(result.stderr, 5)}")
            continue
        missing = [f for f in flags if not re.search(rf"(?<![\w-]){re.escape(f)}(?![\w-])", result.stdout)]
        if missing:
            problems.append(f"{module}: flags missing from --help: {', '.join(missing)}")
    return problems


def time_l_control(scenario, timeout, interval):
    if sys.platform != "darwin" or not TIME_BIN.exists():
        return
    m = measure([str(TIME_BIN), "-l", *scenario.argv()], timeout, interval)
    found = {
        key: int(match.group(1))
        for key, pattern in (("maxrss", r"(\d+)\s+maximum resident set size"), ("footprint", r"(\d+)\s+peak memory footprint"))
        if (match := re.search(pattern, m.stderr))
    }
    if "maxrss" not in found:
        print("  /usr/bin/time -l output not recognised", flush=True)
        return
    print(f"\ncontrol /usr/bin/time -l {scenario.shell()}")
    print(f"  /usr/bin/time: maximum resident set size {mib(found['maxrss']):.1f} MiB, "
          f"peak memory footprint {mib(found.get('footprint', 0)):.1f} MiB")
    print(f"  harness: tree peak {mib(m.peak_rss):.1f} MiB over {m.peak_procs} procs (includes /usr/bin/time itself), "
          f"wait4 max single {mib(m.kernel_maxrss):.1f} MiB, "
          f"tree − time -l = {mib(m.peak_rss - found['maxrss']):+.1f} MiB")


def selftest_checks(records, timeout, interval):
    print("\nselftest checks (warm runs; cold run for scenarios expected to fail)")
    verdicts = []
    for scenario in SUITES["selftest"]:
        rows = [r for r in records if r["scenario"] == scenario.name]
        if not rows:
            continue
        job = json.loads(rows[0]["job_json"]) if rows[0]["job_json"] else {}
        expected_exit = job.get("exit_code", 0)
        if expected_exit:
            ok = all(r["exit_code"] == expected_exit for r in rows) and bool(job)
            print(f"  {scenario.name:<14} exit codes {[r['exit_code'] for r in rows]} expected {expected_exit}, "
                  f"job json parsed: {bool(job)}  {'OK' if ok else 'FAIL'}")
            verdicts.append(ok)
            continue
        warm = [r for r in rows if not r["cold"]]
        payload = job.get("expected_payload_mib", 0)
        procs = max(r["peak_procs"] for r in warm)
        upper = payload + procs * PROBE_OVERHEAD_MIB
        ok = all(r["exit_code"] == 0 and payload <= r["peak_rss_mib"] <= upper for r in warm)
        peaks = ", ".join(f"{r['peak_rss_mib']:.0f}" for r in warm)
        mean_peak = sum(r["peak_rss_mib"] for r in warm) / len(warm)
        print(f"  {scenario.name:<14} payload {payload:5.0f} MiB  tree peak [{peaks}] MiB  procs {procs}  "
              f"overhead/proc {(mean_peak - payload) / procs:5.1f} MiB  "
              f"max single {warm[0]['kernel_maxrss_mib']:.0f} MiB  bound [{payload}, {upper}]  {'OK' if ok else 'FAIL'}")
        verdicts.append(ok)
    print(f"  selftest: {'PASS' if verdicts and all(verdicts) else 'FAIL'}")
    time_l_control(SUITES["selftest"][0], timeout, interval)


def bar_values(summary, names, column):
    data = summary.set_index("scenario")
    return [float(data[column].get(name, math.nan)) if name in data.index else math.nan for name in names]


def clean(values):
    return [0.0 if math.isnan(v) else v for v in values]


def time_text(value):
    return "" if math.isnan(value) else (f"{value:.2f} с" if value < 10 else f"{value:.1f} с")


def mem_text(value):
    return "" if math.isnan(value) else f"{value:.0f} МиБ"


def value_text(value, digits):
    return "" if math.isnan(value) else f"{value:.{digits}f}"


def ratio_text(a, b):
    return "" if math.isnan(a) or math.isnan(b) or b == 0 else f"×{a / b:.1f}"


def draw_bars(ax, xs, values, errors, colors, texts, width, label=None):
    bars = ax.bar(xs, clean(values), width, yerr=clean(errors), color=colors, edgecolor="white", linewidth=1,
                  capsize=3, error_kw={"ecolor": INK_2, "elinewidth": 1, "capthick": 1}, label=label)
    ax.bar_label(bars, labels=texts, padding=3, fontsize=7.5, color=INK_2)
    ax.margins(y=0.15)


MAIN_SERIES = (
    ("MapReduce (Python, 10 воркеров; k=1,2,3 за один проход)", BLUE, [f"mr_k{k}" for k in KS]),
    ("DuckDB (CSV из S3, отдельное чтение на каждый k)", ORANGE, [f"duck_k{k}" for k in KS]),
    ("DuckDB (CSV из S3 читается один раз в TEMP TABLE)", ORANGE_LIGHT, [None, None, None, "duck_k123_1scan"]),
)


def plot_main(summary, path):
    groups = ["k=1", "k=2", "k=3", "k=1,2,3"]
    width = 0.27
    slots = {}
    for g in range(len(groups)):
        members = [i for i, (_, _, names) in enumerate(MAIN_SERIES) if names[g] is not None]
        for j, i in enumerate(members):
            slots[i, g] = g + (j - (len(members) - 1) / 2) * width
    with matplotlib.rc_context(STYLE):
        fig = Figure(figsize=(11, 4.6), layout="constrained")
        axes = fig.subplots(1, 2)
        for ax, mean_col, std_col, title, ylabel, text in (
            (axes[0], "time_mean_s", "time_std_s", "Время выполнения, mean ± std (×N = MR / DuckDB)", "секунды",
             lambda v: value_text(v, 2)),
            (axes[1], "rss_mean_mib", "rss_std_mib", "Пик RSS дерева процессов, mean ± std (×N = MR / DuckDB)", "МиБ",
             lambda v: value_text(v, 0)),
        ):
            means = {}
            for i, (label, color, names) in enumerate(MAIN_SERIES):
                present = [(g, name) for g, name in enumerate(names) if name is not None]
                values = bar_values(summary, [name for _, name in present], mean_col)
                errors = bar_values(summary, [name for _, name in present], std_col)
                means.update({(i, g): v for (g, _), v in zip(present, values, strict=True)})
                draw_bars(ax, [slots[i, g] for g, _ in present], values, errors, color, [text(v) for v in values],
                          width, label)
            ticks = [f"{groups[g]}\n{ratio_text(means[0, g], means[2 if g == 3 else 1, g])}" for g in range(len(groups))]
            ax.set_xticks(range(len(groups)), ticks)
            ax.set_title(title, loc="left")
            ax.set_ylabel(ylabel)
        handles, labels = axes[0].get_legend_handles_labels()
        fig.legend(handles, labels, loc="outside lower center", ncol=len(MAIN_SERIES), fontsize=8)
        fig.suptitle("MapReduce vs DuckDB, вариант PHP: тёплые прогоны", x=0.01, ha="left", fontsize=11)
        fig.savefig(path)


def errorbar_line(ax, xs, means, stds, color, label=None):
    ax.errorbar(xs, clean(means), yerr=clean(stds), color=color, marker="o", markersize=5, linewidth=2, capsize=3,
                elinewidth=1, label=label)


def plot_scaling(summary, path):
    worker_names = [f"mr_w{w}_c{DEFAULT_CHUNK}" for w in WORKERS]
    chunk_names = [f"mr_w{DEFAULT_WORKERS}_c{c}" for c in CHUNKS]
    with matplotlib.rc_context(STYLE):
        fig = Figure(figsize=(11, 7.5), layout="constrained")
        axes = fig.subplots(2, 2)
        times = bar_values(summary, worker_names, "time_mean_s")
        errorbar_line(axes[0][0], WORKERS, times, bar_values(summary, worker_names, "time_std_s"), BLUE, "измерено")
        if not math.isnan(times[0]):
            axes[0][0].plot(WORKERS, [times[0] / w for w in WORKERS], color=INK_2, linestyle="--", linewidth=1,
                            label="идеал T(1)/w")
            for w, t in zip(WORKERS, times, strict=True):
                if not math.isnan(t):
                    axes[0][0].annotate(f"×{times[0] / t:.1f}", (w, t), textcoords="offset points", xytext=(6, 6),
                                        fontsize=7.5, color=INK_2)
        axes[0][0].set_title(f"Время vs число воркеров (чанк {DEFAULT_CHUNK} МиБ)", loc="left")
        axes[0][0].set_ylabel("секунды")
        axes[0][0].legend()
        errorbar_line(axes[0][1], WORKERS, bar_values(summary, worker_names, "rss_mean_mib"),
                      bar_values(summary, worker_names, "rss_std_mib"), BLUE)
        axes[0][1].set_title("Пик RSS дерева процессов vs воркеры", loc="left")
        axes[0][1].set_ylabel("МиБ")
        errorbar_line(axes[1][0], CHUNKS, bar_values(summary, chunk_names, "time_mean_s"),
                      bar_values(summary, chunk_names, "time_std_s"), AQUA)
        axes[1][0].set_title(f"Время vs размер чанка ({DEFAULT_WORKERS} воркеров)", loc="left")
        axes[1][0].set_ylabel("секунды")
        errorbar_line(axes[1][1], CHUNKS, bar_values(summary, chunk_names, "rss_mean_mib"),
                      bar_values(summary, chunk_names, "rss_std_mib"), AQUA)
        axes[1][1].set_title("Пик RSS дерева процессов vs размер чанка", loc="left")
        axes[1][1].set_ylabel("МиБ")
        for ax in axes[0]:
            ax.set_xticks(WORKERS)
            ax.set_xlabel("воркеры (процессы-мапперы)")
            ax.set_ylim(bottom=0)
        for ax in axes[1]:
            ax.set_xscale("log", base=2)
            ax.set_xticks(CHUNKS, [str(c) for c in CHUNKS])
            ax.set_xlabel("размер чанка, МиБ")
            ax.set_ylim(bottom=0)
        fig.suptitle("Масштабирование MapReduce k=1,2,3 за проход", x=0.01, ha="left", fontsize=11)
        fig.savefig(path)


def plot_bonus(summary, path):
    names = ["mr_naive_k123", "mr_k123", "duck_k123", "duck_parquet_k123"]
    ticks = ["MR v1\nнаивный", "MR v2\n{}", "DuckDB\nCSV", "DuckDB\nParquet\n{}"]
    xs = [0, 1, 2.5, 3.5]
    colors = [BLUE_LIGHT, BLUE, ORANGE_LIGHT, ORANGE]
    with matplotlib.rc_context(STYLE):
        fig = Figure(figsize=(11, 4.4), layout="constrained")
        axes = fig.subplots(1, 2)
        for ax, mean_col, std_col, title, ylabel, text in (
            (axes[0], "time_mean_s", "time_std_s", "Время, mean ± std (×N = до / после)", "секунды", time_text),
            (axes[1], "rss_mean_mib", "rss_std_mib", "Пик RSS дерева процессов (×N = до / после)", "МиБ", mem_text),
        ):
            values = bar_values(summary, names, mean_col)
            draw_bars(ax, xs, values, bar_values(summary, names, std_col), colors, [text(v) for v in values], 0.8)
            gains = ["", ratio_text(values[0], values[1]), "", ratio_text(values[2], values[3])]
            ax.set_xticks(xs, [tick.format(gain).strip() for tick, gain in zip(ticks, gains, strict=True)])
            ax.set_title(title, loc="left")
            ax.set_ylabel(ylabel)
        fig.suptitle("Бонус: оптимизация «до/после»", x=0.01, ha="left", fontsize=11)
        fig.savefig(path)


def plot_timeline(suite, timeline, path):
    panels = PANELS[suite]
    labels = {s.name: s.label for s in SUITES[suite]}
    with matplotlib.rc_context(STYLE):
        fig = Figure(figsize=(5.5 * len(panels), 4), layout="constrained")
        axes = fig.subplots(1, len(panels), squeeze=False)[0]
        for ax, (title, names) in zip(axes, panels, strict=True):
            for color, name in zip(SERIES, names, strict=False):
                part = timeline[timeline["scenario"] == name]
                if part.empty:
                    continue
                ax.plot(part["t_s"], part["rss_mib"], color=color, linewidth=1.6, label=labels.get(name, name))
            ax.set_title(title, loc="left")
            ax.set_xlabel("время от старта процесса, с")
            ax.set_ylabel("RSS дерева процессов, МиБ")
            ax.set_ylim(bottom=0)
            ax.grid(axis="x", color=GRID, linewidth=0.8)
            ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.16), ncol=2, fontsize=8)
        fig.suptitle(f"Профиль памяти во времени (первый тёплый прогон, опрос {int(INTERVAL * 1000)} мс)",
                     x=0.01, ha="left", fontsize=11)
        fig.savefig(path)


def plot_suite(suite, summary, timeline):
    IMG_DIR.mkdir(parents=True, exist_ok=True)
    paths = []
    part = summary[summary["suite"] == suite] if not summary.empty else summary
    if not part.empty:
        plotter = {"main": plot_main, "scaling": plot_scaling, "bonus": plot_bonus}.get(suite)
        if plotter:
            paths.append(IMG_DIR / f"bench_{suite}.png")
            plotter(part, paths[-1])
    lines = timeline[timeline["suite"] == suite] if not timeline.empty else timeline
    if not lines.empty:
        paths.append(IMG_DIR / f"bench_{suite}_timeline.png")
        plot_timeline(suite, lines, paths[-1])
    for path in paths:
        print(f"plot {path.relative_to(config.ROOT)}")


def persist(suite, records, timelines, dockers):
    fresh = {f"{suite}/{rec['scenario']}" for rec in records}
    runs = ordered(merge(load_csv(artifact(suite, "runs.csv")), pd.DataFrame(records), fresh), "run")
    timeline = ordered(merge(load_csv(artifact(suite, "timeline.csv")), pd.DataFrame(timelines), fresh), "t_s")
    summary = summarize(runs)
    tables = {artifact(suite, "runs.csv"): runs, artifact(suite, "summary.csv"): summary}
    if not timeline.empty:
        tables[artifact(suite, "timeline.csv")] = timeline
    if dockers:
        tables["docker_stats.csv"] = pd.concat([load_csv("docker_stats.csv"), pd.DataFrame(dockers)], ignore_index=True)
    uploaded = [name for name, df in tables.items() if store_csv(df, name)]
    print(f"\nsaved {', '.join(config.s3_url(f'{config.RESULTS_BENCH}/{name}') for name in uploaded) or 'nothing to S3'}")
    print(f"local copies in {LOCAL_DIR.relative_to(config.ROOT)}/")
    return summary, timeline


def machine():
    brand = platform.processor()
    if sys.platform == "darwin":
        with contextlib.suppress(OSError, subprocess.SubprocessError):
            brand = subprocess.run(["sysctl", "-n", "machdep.cpu.brand_string"], capture_output=True, text=True,
                                   check=True).stdout.strip()
    return (f"{brand}, {psutil.cpu_count(logical=False)} cores / {psutil.cpu_count()} threads, "
            f"RAM {psutil.virtual_memory().total / (1 << 30):.0f} GiB, {platform.system()} {platform.release()}, "
            f"Python {platform.python_version()}")


def run_suite(suite, scenarios, args, session):
    print(f"\n=== suite {suite}: {len(scenarios)} scenarios × (1 cold + {args.runs} warm), "
          f"RSS sampling every {args.interval * 1000:.0f} ms, "
          f"tree rescan every {args.interval * SCAN_EVERY * 1000:.0f} ms", flush=True)
    track_docker = suite != "selftest"
    dockers = [docker_snapshot(session, suite, "start")] if track_docker else []
    records, timelines = [], []
    for i, scenario in enumerate(scenarios):
        print(f"\n[{suite}] {scenario.name}: {scenario.shell()}", flush=True)
        if args.restart_storage and track_docker:
            restart_storage()
        rows, timeline = bench_scenario(session, suite, scenario, args.runs, args.timeout, args.pause, args.interval)
        records += rows
        timelines += timeline
        if track_docker:
            dockers.append(docker_snapshot(session, suite, "end" if i == len(scenarios) - 1 else f"after {scenario.name}"))
    if suite == "selftest":
        selftest_checks(records, args.timeout, args.interval)
    summary, timeline = persist(suite, records, timelines, [d for d in dockers if d])
    print_summary(summary[summary["suite"] == suite])
    plot_suite(suite, summary, timeline)


def select(suites, only):
    plan = {suite: [s for s in SUITES[suite] if not only or s.name in only] for suite in suites}
    unknown = only - {s.name for suite in suites for s in SUITES[suite]}
    return {suite: items for suite, items in plan.items() if items}, unknown


def parse_args():
    parser = argparse.ArgumentParser(
        prog="python -m hhlake.bench",
        description="Benchmark MapReduce and DuckDB jobs: wall time, peak RSS of the whole process tree, CPU",
    )
    parser.add_argument("--suite", choices=[*SUITES, "all"], default="main")
    parser.add_argument("--runs", type=int, default=5, help="warm runs per scenario (a cold run is added)")
    parser.add_argument("--only", default="", help="comma-separated scenario names")
    parser.add_argument("--timeout", type=float, default=3600, help="seconds per run before the tree is killed")
    parser.add_argument("--pause", type=float, default=1.0, help="seconds between runs")
    parser.add_argument("--interval", type=float, default=INTERVAL, help="RSS sampling period, seconds")
    parser.add_argument("--list", action="store_true", help="print scenarios and commands, run nothing")
    parser.add_argument("--plot-only", action="store_true", help="rebuild plots from results/bench in S3")
    parser.add_argument("--skip-preflight", action="store_true")
    parser.add_argument("--restart-storage", action="store_true",
                        help=f"docker restart {CONTAINER} before every scenario, so RustFS memory growth does not carry over")
    parser.add_argument("--allow-battery", action="store_true",
                        help="run on battery power (macOS throttles a laptop on low battery, so timings are not comparable)")
    args = parser.parse_args()
    if args.runs < 3:
        parser.error("--runs must be at least 3")
    return args


def main():
    args = parse_args()
    suites = SUITE_ORDER if args.suite == "all" else (args.suite,)
    only = {name.strip() for name in args.only.split(",") if name.strip()}
    plan, unknown = select(suites, only)
    if unknown or not plan:
        available = ", ".join(s.name for suite in suites for s in SUITES[suite])
        raise SystemExit(f"unknown scenarios: {', '.join(sorted(unknown)) or '—'}; available: {available}")
    if args.list:
        for suite, scenarios in plan.items():
            for scenario in scenarios:
                print(f"{suite:<9}{scenario.name:<20}{scenario.shell()}")
        return
    if args.plot_only:
        for suite in plan:
            summary = load_csv(artifact(suite, "summary.csv"))
            timeline = load_csv(artifact(suite, "timeline.csv"))
            plot_suite(suite, summary, timeline)
        return
    if not args.skip_preflight:
        problems = preflight([s for scenarios in plan.values() for s in scenarios])
        if problems:
            print("preflight failed:\n" + "\n".join(problems), file=sys.stderr)
            raise SystemExit(2)
    on_ac, percent = power()
    if on_ac is False and not args.allow_battery:
        raise SystemExit(f"{power_text(on_ac, percent)}: plug in the charger or pass --allow-battery")
    session = utc_now()
    print(f"bench session {session}")
    print(f"machine: {machine()}")
    print(power_text(on_ac, percent))
    print(f"S3: {config.ENDPOINT}/{config.BUCKET}, cwd {config.ROOT}")
    for suite, scenarios in plan.items():
        run_suite(suite, scenarios, args, session)


if __name__ == "__main__":
    main()
