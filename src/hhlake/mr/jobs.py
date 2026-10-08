import csv
import heapq
import io
import json
import zlib
from array import array
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from itertools import combinations
from math import comb

from ..segment import COL, REGIONS, is_variant, normalize_skills

EXPECTED = {"records": 229_388, "variant": 5_382, "with_skills": 4_380}
CONTROL = {
    1: (955, 31_835, ("php",), 4_033),
    2: (15_921, 135_362, ("mysql", "php"), 2_096),
    3: (127_466, 472_071, ("git", "mysql", "php"), 1_030),
}
REGION_SET = frozenset(REGIONS)


@dataclass
class MapResult:
    ks: tuple
    regional: bool
    counts: dict = field(init=False)
    emitted: dict = field(init=False)
    ids: array = field(default_factory=lambda: array("Q"))
    variant: int = 0
    with_skills: int = 0
    region_variant: Counter = field(default_factory=Counter)
    region_with_skills: Counter = field(default_factory=Counter)
    skill_sizes: Counter = field(default_factory=Counter)

    def __post_init__(self):
        self.counts = {k: Counter() for k in self.ks}
        self.emitted = dict.fromkeys(self.ks, 0)

    @property
    def records(self):
        return len(self.ids)

    @property
    def combined(self):
        return {k: len(counter) for k, counter in self.counts.items()}


def map_rows(rows, ks, regional):
    result = MapResult(tuple(ks), regional)
    for row in rows:
        result.ids.append(int(row[COL["vacancy_id"]]))
        if not is_variant(row):
            continue
        result.variant += 1
        area = row[COL["area"]]
        in_region = area in REGION_SET
        if in_region:
            result.region_variant[area] += 1
        skills = normalize_skills(row[COL["key_skills"]])
        if not skills:
            continue
        result.with_skills += 1
        result.skill_sizes[len(skills)] += 1
        if in_region:
            result.region_with_skills[area] += 1
        elif regional:
            continue
        prefix = (area,) if regional else ()
        for k in ks:
            result.emitted[k] += comb(len(skills), k)
            result.counts[k].update(prefix + combo for combo in combinations(skills, k))
    return result


def partition(key, reducers):
    return zlib.crc32("\t".join(key).encode("utf-8")) % reducers


def shuffle_blobs(counts, reducers):
    lines = defaultdict(list)
    for k, counter in counts.items():
        for key, count in counter.items():
            lines[partition(key, reducers), k].append(json.dumps([k, count, *key], ensure_ascii=False))
    blobs = {}
    sizes = dict.fromkeys(counts, 0)
    for r in range(reducers):
        pieces = []
        for k in counts:
            if bucket := lines.get((r, k)):
                piece = ("\n".join(bucket) + "\n").encode("utf-8")
                sizes[k] += len(piece)
                pieces.append(piece)
        if pieces:
            blobs[r] = b"".join(pieces)
    return blobs, sizes


def merge_blob(blob, totals):
    for line in blob.decode("utf-8").split("\n"):
        if line:
            k, count, *key = json.loads(line)
            totals[k][tuple(key)] += count
    return totals


def header(k, regional):
    return (["area"] if regional else []) + [f"skill{i}" for i in range(1, k + 1)] + ["count"]


def ordered(counts, regional):
    if regional:
        return sorted(counts.items(), key=lambda item: (item[0][0], -item[1], item[0][1:]))
    return sorted(counts.items(), key=lambda item: (-item[1], item[0]))


def render_csv(counts, k, regional):
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(header(k, regional))
    writer.writerows([*key, count] for key, count in ordered(counts, regional))
    return buf.getvalue()


def output_key(prefix, k, regional):
    return f"{prefix}/{'regions_' if regional else ''}k{k}.csv"


def top(counts, n=1):
    return heapq.nsmallest(n, counts.items(), key=lambda item: (-item[1], item[0]))
