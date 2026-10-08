import multiprocessing
import os
import subprocess
import sys
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
from itertools import combinations, repeat

from hhlake.mr import jobs

SKILLS = [
    "php", "mysql", "git", "1с-битрикс", "php7+", "a,b", 'say "hi"', "tab\there", "line\nbreak", "sep\u2028x", "🐘",
]
KEYS = [combo for k in (1, 2, 3) for combo in combinations(sorted(SKILLS), k)]
KEYS += [("Москва", *combo) for combo in KEYS[:50]]
REDUCERS = 7


def spawn_partitions(keys, reducers):
    context = multiprocessing.get_context("spawn")
    with ProcessPoolExecutor(max_workers=3, mp_context=context) as pool:
        return list(pool.map(jobs.partition, keys, repeat(reducers), chunksize=16))


def child_output(code, seed):
    env = {**os.environ, "PYTHONHASHSEED": str(seed)}
    return subprocess.run([sys.executable, "-c", code], env=env, capture_output=True, text=True, check=True).stdout


def test_partition_is_stable_across_spawned_processes():
    local = [jobs.partition(key, REDUCERS) for key in KEYS]
    assert spawn_partitions(KEYS, REDUCERS) == local
    assert spawn_partitions(KEYS, REDUCERS) == local
    assert set(local) == set(range(REDUCERS))


def test_partition_ignores_hash_seed_unlike_builtin_hash():
    code = (
        "from itertools import combinations\n"
        "from hhlake.mr import jobs\n"
        "keys = list(combinations(['git', 'laravel', 'mysql', 'php', 'redis', 'Москва'], 2))\n"
        "print([jobs.partition(key, 7) for key in keys])\n"
        "print([hash(key) % 7 for key in keys])\n"
    )
    crc_first, hash_first = child_output(code, 1).splitlines()
    crc_second, hash_second = child_output(code, 2).splitlines()
    assert crc_first == crc_second
    assert hash_first != hash_second


def test_shuffle_round_trip_keeps_every_count():
    counts = {k: Counter() for k in (1, 2, 3, 4)}
    for index, key in enumerate(KEYS):
        counts[len(key)][key] = index + 1
    blobs, sizes = jobs.shuffle_blobs(counts, REDUCERS)
    assert sum(sizes.values()) == sum(len(blob) for blob in blobs.values())
    restored = {k: Counter() for k in counts}
    for reducer, blob in blobs.items():
        part = jobs.merge_blob(blob, {k: Counter() for k in counts})
        for k, counter in part.items():
            assert all(jobs.partition(key, REDUCERS) == reducer for key in counter)
            assert not set(counter) & set(restored[k])
            restored[k].update(counter)
    assert restored == counts


def test_map_combine_shuffle_reduce_equals_direct_count():
    rows = []
    for number, skills in enumerate(["PHP; MySQL;git", "php;Laravel; ;MYSQL", "", "php;php;Git", "Go;Rust"]):
        row = [""] * 23
        row[1] = str(number)
        row[5] = "Москва" if number % 2 else "Казань"
        row[10] = "Senior PHP developer" if number < 4 else "Go developer"
        row[11] = "Программист, разработчик"
        row[20] = skills
        rows.append(row)
    ks = (1, 2, 3)
    left = jobs.map_rows(rows[:2], ks, False)
    right = jobs.map_rows(rows[2:], ks, False)
    totals = {k: Counter() for k in ks}
    for result in (left, right):
        blobs, _ = jobs.shuffle_blobs(result.counts, 3)
        for blob in blobs.values():
            jobs.merge_blob(blob, totals)
    whole = jobs.map_rows(rows, ks, False)
    assert totals == whole.counts
    assert whole.variant == 4
    assert whole.with_skills == 3
    assert totals[1] == Counter({("php",): 3, ("mysql",): 2, ("git",): 2, ("laravel",): 1})
    assert sum(totals[2].values()) == whole.emitted[2] == 3 + 3 + 1
    regional = jobs.map_rows(rows, ks, True)
    assert {key[0] for key in regional.counts[1]} == {"Москва"}
