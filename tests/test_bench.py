import sys

import pandas as pd

from hhlake import bench


def probe(*args):
    return [sys.executable, "-m", "hhlake.memprobe", "--hold", "0.6", *args]


def payload_bounds(m, payload_mib):
    peak = m.peak_rss / bench.MIB
    return payload_mib <= peak <= payload_mib + m.peak_procs * bench.PROBE_OVERHEAD_MIB


def test_tree_peak_sums_concurrent_children():
    m = bench.measure(probe("--mode", "pool", "--mib", "64", "--children", "3", "--parent-mib", "16"))
    assert m.exit_code == 0
    assert m.peak_procs >= 4
    assert payload_bounds(m, 16 + 3 * 64)
    assert m.kernel_maxrss < m.peak_rss
    assert bench.last_json(m.stdout)["expected_payload_mib"] == 16 + 3 * 64


def test_tree_peak_does_not_accumulate_finished_children():
    m = bench.measure(probe("--mode", "chain", "--mib", "96", "--children", "3"))
    assert m.exit_code == 0
    assert payload_bounds(m, 96)
    assert m.peak_rss / bench.MIB < 2 * 96


def test_grandchildren_are_counted():
    m = bench.measure(probe("--mode", "nested", "--mib", "64"))
    assert m.peak_procs >= 3
    assert payload_bounds(m, 2 * 64)


def test_exit_code_and_timeout():
    failed = bench.measure(probe("--mode", "single", "--mib", "8", "--exit-code", "3"))
    assert failed.exit_code == 3
    assert bench.last_json(failed.stdout)["exit_code"] == 3
    killed = bench.measure(probe("--mode", "pool", "--mib", "8", "--children", "2", "--hold", "30"), timeout=1.0)
    assert killed.timed_out
    assert killed.exit_code < 0
    assert killed.wall_s < 10


def test_last_json_takes_final_object_line():
    text = 'progress\n{"a": 1}\nmore\n{"b": 2}\nnot json {\n'
    assert bench.last_json(text) == {"b": 2}
    assert bench.last_json("no json here") is None


def test_parse_size_units():
    assert bench.parse_size("224.2MiB / 7.653GiB") == 224.2
    assert bench.parse_size("1.5GiB / 8GiB") == 1536.0
    assert bench.parse_size("512KiB") == 0.5


def test_summary_excludes_cold_and_failed_runs():
    base = {"suite": "main", "scenario": "mr_k1", "label": "", "engine": "mapreduce", "k": "1", "workers": 10,
            "chunk_mib": 32, "source": "csv", "cpu_avg_pct": 100.0, "peak_cpu_pct": 200.0, "kernel_maxrss_mib": 50.0}
    rows = [
        {**base, "run": 0, "cold": True, "exit_code": 0, "wall_s": 9.0, "peak_rss_mib": 900.0},
        {**base, "run": 1, "cold": False, "exit_code": 0, "wall_s": 2.0, "peak_rss_mib": 100.0},
        {**base, "run": 2, "cold": False, "exit_code": 0, "wall_s": 4.0, "peak_rss_mib": 300.0},
        {**base, "run": 3, "cold": False, "exit_code": 1, "wall_s": 0.5, "peak_rss_mib": 10.0},
    ]
    summary = bench.summarize(pd.DataFrame(rows)).iloc[0]
    assert summary["n"] == 2
    assert summary["failed"] == 1
    assert summary["time_mean_s"] == 3.0
    assert summary["time_min_s"] == 2.0
    assert summary["rss_max_mib"] == 300.0
    assert summary["cold_time_s"] == 9.0


def test_merge_replaces_only_rerun_scenarios():
    old = pd.DataFrame([{"suite": "main", "scenario": "mr_k1", "run": 1}, {"suite": "main", "scenario": "duck_k1", "run": 1}])
    new = pd.DataFrame([{"suite": "main", "scenario": "mr_k1", "run": 7}])
    merged = bench.merge(old, new, {"main/mr_k1"})
    assert sorted(zip(merged["scenario"], merged["run"], strict=True)) == [("duck_k1", 1), ("mr_k1", 7)]
