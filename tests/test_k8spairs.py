import csv
import importlib
import io
import random
import sys
from collections import Counter
from itertools import combinations, pairwise

import pytest

from hhlake import config
from hhlake.segment import FIELDS, normalize_skills

sys.path.insert(0, str(config.ROOT / "hw5" / "app"))
k8spairs = importlib.import_module("k8spairs")

SKILLS = ["PHP", "MySQL", "Git", "Laravel", "Docker", "Redis", "JavaScript", "  php "]


def vacancy(number, rng):
    vacancy_id = str(200_000_000 + number)
    row = dict.fromkeys(FIELDS, "")
    row.update(
        source="hh",
        vacancy_id=vacancy_id,
        status="active",
        snapshot_date="2024-09-03",
        area="Москва",
        name=rng.choice(["PHP-разработчик", "Backend PHP developer", "Java-разработчик"]),
        role=rng.choice(["Программист, разработчик", "Программист, разработчик", "Тестировщик"]),
        key_skills=";".join(rng.sample(SKILLS, rng.randint(0, 5))),
        description="описание, с запятой\nи переводом строки " * rng.randint(0, 30),
        url=f"https://hh.ru/vacancy/{vacancy_id}",
    )
    return [row[name] for name in FIELDS]


def dataset(count=400, seed=11):
    rng = random.Random(seed)
    rows = [vacancy(i, rng) for i in range(count)]
    buf = io.StringIO()
    csv.writer(buf, lineterminator="\n").writerows([FIELDS, *rows])
    return buf.getvalue().encode("utf-8"), rows


def expected_pairs(rows):
    counts = Counter()
    for row in rows:
        if row[FIELDS.index("role")] == "Программист, разработчик" and "php" in row[FIELDS.index("name")].lower():
            counts.update(combinations(normalize_skills(row[FIELDS.index("key_skills")]), 2))
    return counts


def fetch_from(data):
    return lambda start, end: data[start:end]


@pytest.mark.parametrize("parts", [1, 3, 8])
@pytest.mark.parametrize("chunk", [512, 4096, 1 << 20])
def test_parts_merge_to_single_pass(parts, chunk):
    data, rows = dataset()
    fetch = fetch_from(data)
    merged = Counter()
    tiles = []
    records = 0
    for index in range(parts):
        start, end = k8spairs.bounds(len(data), parts, index)
        counts, stats = k8spairs.map_range(fetch, len(data), start, end, chunk, log=lambda *a, **k: None)
        merged.update(counts)
        records += stats["records"]
        if stats["first"] is not None:
            tiles.append((stats["first"], stats["last"]))
    assert records == len(rows)
    assert merged == expected_pairs(rows)
    assert tiles[0][0] == data.index(b"\nhh,") + 1
    assert tiles[-1][1] == len(data)
    assert all(left[1] == right[0] for left, right in pairwise(tiles))


def test_bounds_cover_object():
    size, parts = 847_619_384, 8
    ranges = [k8spairs.bounds(size, parts, i) for i in range(parts)]
    assert ranges[0][0] == 0 and ranges[-1][1] == size
    assert all(left[1] == right[0] for left, right in pairwise(ranges))
